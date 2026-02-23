# Hashing
"""
hashing.py

This file is responsible for creating a hash from text.
A hash is a short fixed string created from longer text.
"""

import hashlib


def generate_text_hash(text: str) -> str:
    """
    Generate SHA-256 hash for given text.

    Why?
    - Helps identify duplicate content
    - Used later in chunking / embeddings

    Args:
        text (str): Input text

    Returns:
        str: Hexadecimal hash string
    """

    # If text is empty or None, return empty string
    if not text:
        return ""

    # Convert text to bytes (required by hashlib)
    text_bytes = text.encode("utf-8")

    # Create SHA-256 hash
    hash_object = hashlib.sha256(text_bytes)

    # Convert hash to readable hex string
    return hash_object.hexdigest()
