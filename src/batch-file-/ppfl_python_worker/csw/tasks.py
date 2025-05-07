import json
import logging
import os
import re
import uuid
from pathlib import Path
from typing import List, Union

import pika
from celery.utils.log import get_task_logger
from dotenv import load_dotenv
from pymongo import MongoClient

from ppfl_python_worker.csw.celery_app import app

# Set up the logger to use DEBUG level
logger = get_task_logger(__name__)
logger.setLevel(logging.DEBUG)

# Load environment variables from the .env file in the project root
project_root = Path(__file__).parent.parent.parent
load_dotenv(dotenv_path=os.path.join(project_root, ".env"))

# Debug: print environment variables (masking sensitive info)
logger.debug("=" * 80)
logger.debug("ENVIRONMENT VARIABLES:")
for key, value in os.environ.items():
    if key.startswith("MONGO_"):
        if "PASSWORD" in key or ("URI" in key and ":" in value):
            masked_value = value.replace(os.environ.get("MONGO_PASSWORD", ""), "****")
            logger.debug(f"{key}: {masked_value}")
        else:
            logger.debug(f"{key}: {value}")
logger.debug("=" * 80)

# Retrieve and validate MongoDB connection details from environment variables
MONGO_URI = os.environ.get("MONGO_URI")
if not MONGO_URI:
    logger.error("MONGO_URI environment variable is not set")
    raise ValueError("MONGO_URI environment variable is not set")

MONGO_DBNAME = os.environ.get("MONGO_DBNAME")
if not MONGO_DBNAME:
    logger.error("MONGO_DBNAME environment variable is not set")
    raise ValueError("MONGO_DBNAME environment variable is not set")

MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION")
if not MONGO_COLLECTION:
    logger.error("MONGO_COLLECTION environment variable is not set")
    raise ValueError("MONGO_COLLECTION environment variable is not set")

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))
QUEUE_B = os.environ.get("RABBITMQ_QUEUE_B")

logger.debug("=" * 80)
logger.debug("ATTEMPTING TO CONNECT TO MONGODB:")
logger.debug(
    f"MONGO_URI: {MONGO_URI.replace(os.environ.get('MONGO_PASSWORD', ''), '****')}"
)
logger.debug(f"MONGO_DBNAME: {MONGO_DBNAME}")
logger.debug(f"MONGO_COLLECTION: {MONGO_COLLECTION}")

try:
    test_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    # Force connection to verify the credentials work
    test_client.admin.command("ping")
    logger.debug("✅ MONGODB CONNECTION SUCCESSFUL!")

    # Test database and collection access
    db = test_client[MONGO_DBNAME]
    coll = db[MONGO_COLLECTION]
    count = coll.count_documents({})
    logger.debug(
        f"✅ SUCCESSFULLY ACCESSED COLLECTION! Found {count} documents in {MONGO_COLLECTION}"
    )

    test_client.close()
except Exception as e:
    logger.error(f"❌ MONGODB CONNECTION FAILED: {str(e)}")
    logger.error("Please check your MongoDB credentials and connection parameters")
logger.debug("=" * 80)


def ensure_uuid_string_format(fingerprint_id: Union[str, int]) -> str:
    """
    Ensure the fingerprint ID is properly formatted as a UUID-style string.
    """
    fp_id_str = str(fingerprint_id)
    uuid_pattern = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
    )
    if uuid_pattern.match(fp_id_str):
        return fp_id_str
    if re.match(r"^[0-9a-f]{32}$", fp_id_str, re.IGNORECASE):
        formatted = f"{fp_id_str[:8]}-{fp_id_str[8:12]}-{fp_id_str[12:16]}-{fp_id_str[16:20]}-{fp_id_str[20:]}"
        return formatted
    logger.warning(
        f"Fingerprint ID '{fp_id_str}' is not in UUID format. This may cause matching issues with MongoDB."
    )
    return fp_id_str


def format_fingerprint_ids(fingerprint_ids: List[Union[str, int]]) -> List[str]:
    formatted_ids = [ensure_uuid_string_format(fp_id) for fp_id in fingerprint_ids]
    # Log the conversion details for each fingerprint ID
    for i, (before, after) in enumerate(zip(fingerprint_ids, formatted_ids)):
        if str(before) != after:
            logger.debug(f"Formatted fingerprint ID {i}: '{before}' -> '{after}'")
    return formatted_ids


def get_mongo_client():
    return MongoClient(MONGO_URI)


@app.task
def process_candidate_search_message(message_json, publish_result=True):
    """
    Process a candidate search message from the queue.
    
    Args:
        message_json: The message to process
        publish_result: Whether to publish the result to queue B (default: True)
    """
    logger.debug("=" * 80)
    logger.debug("ENVIRONMENT VARIABLES:")
    # Log all environment variables except password
    for key, value in os.environ.items():
        if key.startswith("MONGO_") or key.startswith("RABBITMQ_"):
            if "PASSWORD" in key:
                logger.debug(f"{key}: ****")
            else:
                logger.debug(f"{key}: {value}")
    logger.debug("=" * 80)

    try:
        # If the message is a JSON string, parse it into a dict
        data = (
            message_json if isinstance(message_json, dict) else json.loads(message_json)
        )

        # Extract fingerprints from either the top-level or under 'data'
        fingerprints = []
        if "fingerprints" in data:
            fingerprints = data["fingerprints"]
            logger.debug(f"Found fingerprints at top level: {len(fingerprints)}")
        elif "data" in data and isinstance(data["data"], dict):
            if "fingerprints" in data["data"]:
                fingerprints = data["data"].get("fingerprints", [])
                logger.debug(
                    f"Found fingerprints under 'data' key: {len(fingerprints)}"
                )
            else:
                logger.debug(
                    f"No 'fingerprints' found under 'data'. Available keys: {list(data['data'].keys())}"
                )
        else:
            logger.debug("Could not find fingerprints in the message")

        logger.debug(f"Extracted fingerprints: {json.dumps(fingerprints, indent=2)}")

        # Extract experimentId from the message (either at the top level or within 'data')
        experimentId = data.get("experimentId")
        if not experimentId and "data" in data and isinstance(data["data"], dict):
            experimentId = data["data"].get("experimentId")

        if not experimentId:
            # Generate a random UUID if experimentId is not provided
            experimentId = str(uuid.uuid4())

        logger.debug(f"Using experimentId: {experimentId}")

        # Extract fingerprint IDs directly - no formatting needed
        fingerprint_ids = [
            fp["fingerprintId"] for fp in fingerprints if "fingerprintId" in fp
        ]
        logger.debug(f"Fingerprint IDs to check: {fingerprint_ids}")
        logger.debug(
            f"Types of fingerprint IDs: {[type(fid).__name__ for fid in fingerprint_ids]}"
        )

        # Connect to MongoDB
        client = get_mongo_client()
        db = client[MONGO_DBNAME]
        coll = db[MONGO_COLLECTION]

        # Get a sample document to understand the structure
        sample = list(coll.find().limit(1))
        if sample:
            logger.debug(
                f"Sample document ID: {sample[0].get('_id')}, Type: {type(sample[0].get('_id')).__name__}"
            )
            logger.debug(f"Sample document keys: {list(sample[0].keys())}")

        # Try direct lookup for each ID first
        for fp_id in fingerprint_ids:
            doc = coll.find_one({"_id": fp_id})
            logger.debug(
                f"Direct lookup for ID '{fp_id}': {'Found' if doc else 'Not found'}"
            )
            if doc:
                logger.debug(
                    f"Document ID: {doc.get('_id')}, Type: {type(doc.get('_id')).__name__}"
                )

        # Check if the fingerprints exist in MongoDB using $in query
        query_filter = {"_id": {"$in": fingerprint_ids}}
        logger.debug(f"MongoDB query filter: {json.dumps(query_filter)}")

        verified_count = coll.count_documents(query_filter)
        logger.debug(f"Verified fingerprints count: {verified_count}")

        # Try with string conversion (in case there's a type mismatch)
        string_ids = [str(fid) for fid in fingerprint_ids]
        query_filter_str = {"_id": {"$in": string_ids}}
        logger.debug(f"String query filter: {json.dumps(query_filter_str)}")
        verified_count_str = coll.count_documents(query_filter_str)
        logger.debug(f"Verified count with string conversion: {verified_count_str}")

        # Use the higher count between the two approaches
        verified_count = max(verified_count, verified_count_str)
        logger.debug(f"Final verified count: {verified_count}")

        # Determine overall status
        status = "verified" if verified_count > 0 else "partial"
        result = {
            "experimentId": experimentId,
            "verifiedFingerprints": verified_count,  # Using correct key name 'verifiedFingerprints'
            "status": status,
        }

        logger.debug(f"Final result: {json.dumps(result, indent=2)}")
        logger.debug("=" * 80)

        # Publish result to queue B only if publish_result is True
        if publish_result:
            publish_to_queue_b(result)
            
        client.close()
        return result

    except Exception as e:
        logger.error(f"[ERROR] Exception during processing: {str(e)}")
        raise


def publish_to_queue_b(message: dict):
    """
    Publish the result message to queue B using RabbitMQ.
    """
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
        )
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_B, durable=True)

        body = json.dumps(message)
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_B,
            body=body,
            properties=pika.BasicProperties(delivery_mode=2),
        )

        logger.debug(f"[Worker] Published result to {QUEUE_B}: {body}")
        connection.close()
    except Exception as e:
        logger.error(f"Error publishing to queue {QUEUE_B}: {str(e)}")
