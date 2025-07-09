from collections.abc import Iterator


def split_list(src_list: list, length: int) -> Iterator[list]:
    """Split list into chunks.

    Args:
        src_list (list): The list to split.
        length (int): The length of each chunk.

    Yields:
        list: Chunks of the original list of the specified length.
    """
    for i in range(0, len(src_list), length):
        yield src_list[i : i + length]


def get_response_value_errors(response, chunk) -> dict:
    """Collect relevant data for error logs.

    Returns
    -------
        dict: A dictionary containing the response type, status, description, import count, and conflicts.
    """
    if response is None:
        return None

    if chunk is None:
        return None

    try:
        out = {}
        for k in ["responseType", "status", "description", "importCount", "dataSetComplete"]:
            out[k] = response.get(k)
        if response.get("conflicts"):
            out["rejected_datapoints"] = []
            for i in response["rejectedIndexes"]:
                out["rejected_datapoints"].append(chunk[i])
            out["conflicts"] = {}
            for conflict in response["conflicts"]:
                out["conflicts"]["object"] = conflict.get("object")
                out["conflicts"]["objects"] = conflict.get("objects")
                out["conflicts"]["value"] = conflict.get("value")
                out["conflicts"]["errorCode"] = conflict.get("errorCode")
        return out
    except AttributeError:
        return None
