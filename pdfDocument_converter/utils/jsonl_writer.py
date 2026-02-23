"""
jsonl_writer.py

This file writes IR elements into JSONL format
and compresses them using gzip.
"""

import gzip
import io
from datetime import datetime, timezone
import orjson


def write_elements_to_jsonl_gzip(elements: list) -> bytes:
    """
    Convert list of IR elements into compressed JSONL (.jsonl.gz)

    Args:
        elements (list): List of dictionaries (IR elements)

    Returns:
        bytes: Gzipped JSONL content (ready for S3 upload)
    """

    # Create in-memory byte buffer
    buffer = io.BytesIO()

    # Open gzip writer
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gzip_file:

        for element in elements:
            # Ensure every element has created_at
            if "created_at" not in element:
                element["created_at"] = datetime.now(timezone.utc).isoformat()

            # Convert dict → JSON bytes
            json_bytes = orjson.dumps(element)

            # Write one JSON object per line
            gzip_file.write(json_bytes)
            gzip_file.write(b"\n")

    # Move cursor back to beginning
    buffer.seek(0)

    # Return compressed bytes
    return buffer.read()
