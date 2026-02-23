"""
logging.py

This file sets up application-wide logging.
It makes sure logs look clean and consistent.

Think of logging as:
"Print statements, but professional"
"""

import logging
from core.settings import LOG_LEVEL


def setup_logger(name: str) -> logging.Logger:
    """
    Create and return a logger instance.

    Parameters:
    name (str): Name of the logger (usually __name__)

    Returns:
    logging.Logger: Configured logger
    """

    # Create logger object
    logger = logging.getLogger(name)

    # Set log level (INFO, DEBUG, ERROR etc.)
    logger.setLevel(LOG_LEVEL)

    # Prevent duplicate logs in AWS Lambda
    if logger.handlers:
        return logger

    # Create log format
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # Create console handler (prints to stdout)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    # Attach handler to logger
    logger.addHandler(handler)

    return logger
