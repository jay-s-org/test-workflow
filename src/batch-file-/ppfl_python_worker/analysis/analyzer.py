import logging

from scipy.stats import wasserstein_distance

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def extract_percentiles(stats: dict) -> list:
    """
    Extracts the p25, p50, and p75 percentiles from the provided statistics dictionary.
    If any of these keys are missing, a default value of 0 is used.

    :param stats: A dictionary expected to have keys 'p25', 'p50', and 'p75'
    :return: A list of three numeric values [p25, p50, p75]
    """
    p25 = stats.get("p25", 0)
    p50 = stats.get("p50", 0)
    p75 = stats.get("p75", 0)
    return [p25, p50, p75]


def extract_statistics(stats: dict) -> list:
    """
    Extracts all numeric statistics from the provided statistics dictionary.
    If any of these keys are missing, a default value of 0 is used.

    :param stats: A dictionary with statistics (min, max, mean, median, stdDev)
    :return: A list of numeric values representing the statistics
    """
    min_val = stats.get("min", 0)
    max_val = stats.get("max", 0)
    mean = stats.get("mean", 0)
    median = stats.get("median", 0)
    std_dev = stats.get("stdDev", 0)
    unique_count = stats.get("uniqueCount", 0)
    null_count = stats.get("nullCount", 0)
    percentiles = stats.get("percentiles", {})
    p25 = percentiles.get("p25", 0)
    p50 = percentiles.get("p50", 0)
    p75 = percentiles.get("p75", 0)

    return [
        min_val,
        max_val,
        mean,
        median,
        std_dev,
        unique_count,
        null_count,
        p25,
        p50,
        p75,
    ]


def compare_percentiles(candidate_stats: dict, root_stats: dict) -> float:
    """
    Compares two percentile distributions using the Wasserstein distance (Earth Mover's Distance).
    This is ideal for comparing the distribution of statistical values from different datasets.

    :param candidate_stats: Dictionary with candidate fingerprint percentiles.
    :param root_stats: Dictionary with root fingerprint percentiles.
    :return: A float representing the Wasserstein distance between the two distributions.
    """
    candidate_values = extract_percentiles(candidate_stats)
    root_values = extract_percentiles(root_stats)

    logger.debug("Candidate percentiles: %s", candidate_values)
    logger.debug("Root percentiles: %s", root_values)

    distance = wasserstein_distance(candidate_values, root_values)
    logger.debug("Computed Wasserstein distance for percentiles: %f", distance)
    return distance


def should_compare_fields(candidate_field_id: str, root_field_id: str) -> bool:
    """
    Determines if two field IDs are similar enough to warrant statistical comparison.
    This checks if the field IDs match exactly or if they refer to similar concepts.

    :param candidate_field_id: Field ID from the candidate fingerprint
    :param root_field_id: Field ID from the root fingerprint
    :return: Boolean indicating whether the fields should be compared
    """
    logger.debug(
        f"Comparing field IDs - Candidate: '{candidate_field_id}', Root: '{root_field_id}'"
    )

    if not candidate_field_id or not root_field_id:
        logger.debug("One or both field IDs are empty or None")
        return False

    if candidate_field_id == root_field_id:
        logger.debug(f"Field IDs match exactly: '{candidate_field_id}'")
        return True

    try:
        candidate_field_name = candidate_field_id.split("/")[-1].lower()
        root_field_name = root_field_id.split("/")[-1].lower()

        logger.debug(
            f"Extracted field names - Candidate: '{candidate_field_name}', Root: '{root_field_name}'"
        )

        if candidate_field_name == root_field_name:
            logger.debug(f"Field names match exactly: '{candidate_field_name}'")
            return True
        if (
            candidate_field_name in root_field_name
            or root_field_name in candidate_field_name
        ):
            logger.debug(
                f"Field names are similar: {candidate_field_name} and {root_field_name}"
            )
            return True
    except Exception as e:
        logger.error(f"Error comparing field IDs: {e}")

    logger.debug(f"Field IDs are not similar: {candidate_field_id} and {root_field_id}")
    return False


def compare_statistics(
    candidate_stats: dict,
    root_stats: dict,
    candidate_field_id: str = None,
    root_field_id: str = None,
) -> float:
    """
    Compares all statistics between two distributions using the Wasserstein distance.
    This provides a more comprehensive comparison than just percentiles.

    If field IDs are provided, it first checks if the fields should be compared based on their IDs.
    If field IDs don't match or are not similar, returns float('inf') to indicate maximum distance.

    :param candidate_stats: Dictionary with candidate fingerprint statistics.
    :param root_stats: Dictionary with root fingerprint statistics.
    :param candidate_field_id: Optional field ID from the candidate fingerprint
    :param root_field_id: Optional field ID from the root fingerprint
    :return: A float representing the Wasserstein distance between the two distributions,
             or float('inf') if field IDs don't match and shouldn't be compared.
    """
    if candidate_field_id is not None and root_field_id is not None:
        if not should_compare_fields(candidate_field_id, root_field_id):
            logger.debug(
                f"Skipping comparison due to dissimilar field IDs: {candidate_field_id} vs {root_field_id}"
            )
            return float("inf")

    candidate_values = extract_statistics(candidate_stats)
    root_values = extract_statistics(root_stats)

    logger.debug("Candidate statistics: %s", candidate_values)
    logger.debug("Root statistics: %s", root_values)

    distance = wasserstein_distance(candidate_values, root_values)
    logger.debug("Computed Wasserstein distance for all statistics: %f", distance)
    return distance


if __name__ == "__main__":
    pass
