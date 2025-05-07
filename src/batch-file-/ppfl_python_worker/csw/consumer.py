import json
import logging
import os
import sys
import traceback

import pika
from dotenv import load_dotenv
from pymongo import MongoClient

from ppfl_python_worker.analysis.analyzer import compare_statistics
from ppfl_python_worker.csw.tasks import (
    process_candidate_search_message,
    publish_to_queue_b,
)

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT"))
QUEUE_A = os.environ.get("RABBITMQ_QUEUE")
MONGO_URI = os.environ.get("MONGO_URI")
MONGO_DBNAME = os.environ.get("MONGO_DBNAME")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION")
ROOT_FP_ID1 = os.environ.get("ROOT_FINGERPRINT_ID_1")
ROOT_FP_ID2 = os.environ.get("ROOT_FINGERPRINT_ID_2")

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


if not logger.handlers:
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


def extract_percentiles_from_doc(doc):
    """
    Given a Mongo document, attempt to extract the fingerprint statistics percentiles.
    This assumes the structure is:
      doc["rawFingerprintJson"]["fingerprint"]["recordSet"][0]["field"][0]["statistics"]["percentiles"]
    """
    try:
        raw = doc.get("RawFingerprintJson") or doc.get("rawFingerprintJson")
        if isinstance(raw, dict):
            fp = raw.get("fingerprint", {})
            recordset = fp.get("recordSet", [])
            if recordset and isinstance(recordset, list):
                first_record = recordset[0]
                fields = first_record.get("field", [])
                if fields and isinstance(fields, list):
                    statistics = fields[0].get("statistics", {})
                    percentiles = statistics.get("percentiles", {})
                    return percentiles
    except Exception as ex:
        logger.error(f"Error extracting percentiles: {ex}")
    return {}


def extract_statistics_from_doc(doc):
    """
    Given a Mongo document, attempt to extract all statistics including percentiles.
    This assumes the structure is:
      doc["rawFingerprintJson"]["fingerprint"]["recordSet"][0]["field"][0]["statistics"]
    """
    try:
        raw = doc.get("RawFingerprintJson") or doc.get("rawFingerprintJson")
        if isinstance(raw, dict):
            fp = raw.get("fingerprint", {})
            recordset = fp.get("recordSet", [])
            if recordset and isinstance(recordset, list):
                first_record = recordset[0]
                fields = first_record.get("field", [])
                if fields and isinstance(fields, list):
                    statistics = fields[0].get("statistics", {})
                    return statistics
    except Exception as ex:
        logger.error(f"Error extracting statistics: {ex}")
    return {}


def extract_field_metadata(doc):
    """
    Extract field name, description, data type from the fingerprint document.
    """
    try:
        raw = doc.get("RawFingerprintJson") or doc.get("rawFingerprintJson")
        if isinstance(raw, dict):
            fp = raw.get("fingerprint", {})
            recordset = fp.get("recordSet", [])
            if recordset and isinstance(recordset, list):
                first_record = recordset[0]
                fields = first_record.get("field", [])
                if fields and isinstance(fields, list):
                    field = fields[0]
                    field_id = field.get("@id", "")
                    logger.debug(
                        f"FIELD_ID_DEBUG: Extracted field ID: '{field_id}' from fingerprint"
                    )
                    return {
                        "name": field.get("name", "Unknown"),
                        "description": field.get("description", ""),
                        "dataType": field.get("dataType", ""),
                        "unit": field.get("unit", ""),
                        "fieldId": field_id,
                    }
    except Exception as ex:
        logger.error(f"Error extracting field metadata: {ex}")
    logger.debug("Failed to extract field metadata or field ID not found")
    return {}


def interpret_wasserstein_distance(distance):
    """
    Provide an interpretation of the Wasserstein distance value.
    """
    if distance < 5:
        return "Very similar statistical profiles"
    elif distance < 10:
        return "Moderately similar statistical profiles"
    elif distance < 20:
        return "Somewhat different statistical profiles"
    else:
        return "Very different statistical profiles"


def query_fingerprint_stats(fp_id, coll):
    """Retrieve fingerprint document by _id and extract its statistics."""
    doc = coll.find_one({"_id": fp_id})
    if not doc:
        logger.error(f"[Consumer] No document found for fingerprint ID {fp_id}")
        return {}, {}
    logger.debug(f"[Consumer] Found document for fingerprint ID {fp_id}")
    return extract_statistics_from_doc(doc), extract_field_metadata(doc)


def callback(ch, method, properties, body):
    """
    When a message arrives in queue A, parse the JSON, then:
      1. Extract candidate fingerprint IDs.
      2. For each candidate, retrieve its full document and extract its statistics.
      3. Retrieve the two root fingerprint documents (using ROOT_FP_ID1 and ROOT_FP_ID2).
      4. Use the statistics analyzer to compare candidate stats vs. each root stats.
      5. Aggregate the comparison (here, we take the minimum distance).
      6. Add the similarity metric to the result message.
      7. Pass the message to the Celery task and publish final results to queue B.
    """
    try:
        message_json = json.loads(body)
        logger.debug(f"[Consumer] Received message from {QUEUE_A}: {message_json}")
        fingerprints = []
        if "fingerprints" in message_json:
            fingerprints = message_json["fingerprints"]
            logger.debug(
                f"[Consumer] Found fingerprints at top level: {len(fingerprints)}"
            )
        elif "data" in message_json and isinstance(message_json["data"], dict):
            if "fingerprints" in message_json["data"]:
                fingerprints = message_json["data"].get("fingerprints", [])
                logger.debug(
                    f"[Consumer] Found fingerprints under 'data': {len(fingerprints)}"
                )

        candidate_fp_ids = [
            fp.get("fingerprintId") for fp in fingerprints if "fingerprintId" in fp
        ]
        logger.debug(f"[Consumer] Candidate fingerprint IDs: {candidate_fp_ids}")
        logger.debug(
            f"[Consumer] Connecting to MongoDB: {MONGO_URI.replace(os.environ.get('MONGO_PASSWORD', ''), '****')}"
        )
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DBNAME]
        coll = db[MONGO_COLLECTION]
        candidate_stats_list = []
        candidate_metadata = {}
        for fp_id in candidate_fp_ids:
            stats, metadata = query_fingerprint_stats(fp_id, coll)
            if stats:
                candidate_stats_list.append((fp_id, stats))
                candidate_metadata[fp_id] = metadata

        if not candidate_stats_list:
            logger.error("[Consumer] No candidate fingerprint statistics found.")
        else:
            root_stats = {}
            root_metadata = {}
            if ROOT_FP_ID1:
                stats, metadata = query_fingerprint_stats(ROOT_FP_ID1, coll)
                root_stats[ROOT_FP_ID1] = stats
                root_metadata[ROOT_FP_ID1] = metadata
            if ROOT_FP_ID2:
                stats, metadata = query_fingerprint_stats(ROOT_FP_ID2, coll)
                root_stats[ROOT_FP_ID2] = stats
                root_metadata[ROOT_FP_ID2] = metadata
            similarity_results = {}

            if not root_stats:
                logger.error(
                    "[Consumer] No root fingerprint statistics available from env variables."
                )
            else:
                all_distances = {}
                detailed_comparisons = {}

                for fp_id, cand_stats in candidate_stats_list:
                    distances = []
                    root_distances = {}
                    fp_comparisons = {}

                    for root_id, stats in root_stats.items():
                        if stats:
                            try:
                                candidate_field_id = candidate_metadata.get(
                                    fp_id, {}
                                ).get("fieldId", "")
                                root_field_id = root_metadata.get(root_id, {}).get(
                                    "fieldId", ""
                                )

                                logger.debug(
                                    f"Comparing fingerprints - Candidate ID: {fp_id}, Root ID: {root_id}"
                                )
                                logger.debug(
                                    f"Field IDs - Candidate: '{candidate_field_id}', Root: '{root_field_id}'"
                                )

                                distance = compare_statistics(
                                    cand_stats,
                                    stats,
                                    candidate_field_id=candidate_field_id,
                                    root_field_id=root_field_id,
                                )

                                if distance != float("inf"):
                                    logger.debug(
                                        f"Valid comparison result - Distance: {distance}"
                                    )
                                    distances.append(distance)
                                    root_distances[root_id] = distance

                                    fp_comparisons[root_id] = {
                                        "distance": distance,
                                        "interpretation": interpret_wasserstein_distance(
                                            distance
                                        ),
                                        "candidate_stats": {
                                            "min": cand_stats.get("min"),
                                            "max": cand_stats.get("max"),
                                            "mean": cand_stats.get("mean"),
                                            "median": cand_stats.get("median"),
                                            "stdDev": cand_stats.get("stdDev"),
                                            "percentiles": cand_stats.get(
                                                "percentiles", {}
                                            ),
                                        },
                                        "root_stats": {
                                            "min": stats.get("min"),
                                            "max": stats.get("max"),
                                            "mean": stats.get("mean"),
                                            "median": stats.get("median"),
                                            "stdDev": stats.get("stdDev"),
                                            "percentiles": stats.get("percentiles", {}),
                                        },
                                        "fieldIdMatch": {
                                            "candidateFieldId": candidate_field_id,
                                            "rootFieldId": root_field_id,
                                        },
                                    }
                                else:
                                    logger.debug(
                                        f"Skipping comparison between {fp_id} and {root_id} due to dissimilar field IDs"
                                    )
                            except Exception as e:
                                logger.error(
                                    f"Error comparing fingerprint {fp_id} with root {root_id}: {e}"
                                )

                    if distances:
                        min_distance = min(distances)
                        similarity_results[fp_id] = min_distance
                        all_distances[fp_id] = root_distances
                        detailed_comparisons[fp_id] = fp_comparisons
                    else:
                        similarity_results[fp_id] = None

                logger.debug(
                    f"[Consumer] Similarity results (Wasserstein distances): {similarity_results}"
                )

                closest_fingerprint = None
                farthest_fingerprint = None
                min_distance = float("inf")
                max_distance = float("-inf")
                closest_comparison = {}
                farthest_comparison = {}

                for fp_id, distance in similarity_results.items():
                    if distance is not None:
                        if distance < min_distance:
                            min_distance = distance
                            closest_fingerprint = fp_id
                            for root_id, root_distance in all_distances[fp_id].items():
                                if root_distance == distance:
                                    closest_comparison = {
                                        "rootFingerprint": root_id,
                                        "rootMetadata": root_metadata.get(root_id, {}),
                                        "candidateMetadata": candidate_metadata.get(
                                            fp_id, {}
                                        ),
                                        "comparison": detailed_comparisons[fp_id][
                                            root_id
                                        ],
                                    }
                                    break

                        if distance > max_distance:
                            max_distance = distance
                            farthest_fingerprint = fp_id
                            for root_id, root_distance in all_distances[fp_id].items():
                                if root_distance == distance:
                                    farthest_comparison = {
                                        "rootFingerprint": root_id,
                                        "rootMetadata": root_metadata.get(root_id, {}),
                                        "candidateMetadata": candidate_metadata.get(
                                            fp_id, {}
                                        ),
                                        "comparison": detailed_comparisons[fp_id][
                                            root_id
                                        ],
                                    }
                                    break

                logger.debug(
                    f"[Consumer] Closest fingerprint: {closest_fingerprint} with distance {min_distance}"
                )
                logger.debug(
                    f"[Consumer] Farthest fingerprint: {farthest_fingerprint} with distance {max_distance}"
                )

        final_result = {
            "experimentId": message_json.get("experimentId")
            or (
                message_json.get("data", {}).get("experimentId")
                if isinstance(message_json.get("data"), dict)
                else None
            ),
            "candidateCount": len(candidate_fp_ids),
            "closestFingerprint": closest_fingerprint
            if "closest_fingerprint" in locals()
            else None,
            "closestDistance": min_distance
            if "min_distance" in locals() and min_distance != float("inf")
            else None,
            "farthestFingerprint": farthest_fingerprint
            if "farthest_fingerprint" in locals()
            else None,
            "farthestDistance": max_distance
            if "max_distance" in locals() and max_distance != float("-inf")
            else None,
        }
        if "closest_comparison" in locals() and closest_comparison:
            final_result["closestInsights"] = {
                "fieldName": closest_comparison["candidateMetadata"].get("name"),
                "fieldType": closest_comparison["candidateMetadata"].get("dataType"),
                "interpretation": closest_comparison["comparison"]["interpretation"],
                "comparedTo": closest_comparison["rootMetadata"].get("name"),
                "keyDifferences": {
                    "meanDiff": abs(
                        closest_comparison["comparison"]["candidate_stats"].get(
                            "mean", 0
                        )
                        - closest_comparison["comparison"]["root_stats"].get("mean", 0)
                    ),
                    "medianDiff": abs(
                        closest_comparison["comparison"]["candidate_stats"].get(
                            "median", 0
                        )
                        - closest_comparison["comparison"]["root_stats"].get(
                            "median", 0
                        )
                    ),
                    "stdDevDiff": abs(
                        closest_comparison["comparison"]["candidate_stats"].get(
                            "stdDev", 0
                        )
                        - closest_comparison["comparison"]["root_stats"].get(
                            "stdDev", 0
                        )
                    ),
                },
            }

        if "farthest_comparison" in locals() and farthest_comparison:
            final_result["farthestInsights"] = {
                "fieldName": farthest_comparison["candidateMetadata"].get("name"),
                "fieldType": farthest_comparison["candidateMetadata"].get("dataType"),
                "interpretation": farthest_comparison["comparison"]["interpretation"],
                "comparedTo": farthest_comparison["rootMetadata"].get("name"),
                "keyDifferences": {
                    "meanDiff": abs(
                        farthest_comparison["comparison"]["candidate_stats"].get(
                            "mean", 0
                        )
                        - farthest_comparison["comparison"]["root_stats"].get("mean", 0)
                    ),
                    "medianDiff": abs(
                        farthest_comparison["comparison"]["candidate_stats"].get(
                            "median", 0
                        )
                        - farthest_comparison["comparison"]["root_stats"].get(
                            "median", 0
                        )
                    ),
                    "stdDevDiff": abs(
                        farthest_comparison["comparison"]["candidate_stats"].get(
                            "stdDev", 0
                        )
                        - farthest_comparison["comparison"]["root_stats"].get(
                            "stdDev", 0
                        )
                    ),
                },
            }
        task_result = process_candidate_search_message(
            message_json, publish_result=False
        )

        if isinstance(task_result, dict):
            if "verifiedFingerprints" in task_result:
                final_result["verifiedFingerprints"] = task_result[
                    "verifiedFingerprints"
                ]
            if "status" in task_result:
                final_result["status"] = task_result["status"]
        publish_to_queue_b(final_result)

        logger.debug(f"[Consumer] Final task result with insights: {final_result}")

        client.close()

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"[Consumer] Error processing message: {e}")
        traceback.print_exc()
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_A, durable=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_A, on_message_callback=callback)

    logger.debug(f"[Consumer] Waiting for messages in {QUEUE_A}...")
    channel.start_consuming()


if __name__ == "__main__":
    main()
