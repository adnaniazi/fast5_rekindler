import os
from datetime import datetime
from importlib.metadata import version
from typing import Any

from loguru import logger as base_logger

# Configure the base logger
logger = base_logger
# Default log directory
log_directory = "."


def get_version() -> Any:
    """Get the version of the app from pyproject.toml.

    Returns:
        app_version (Any): Version of the app.
    """
    app_version = version("fast5_rekindler")
    return app_version


def configure_logger(new_log_directory: str) -> str:
    """Configure the logger to log to a file.

    Params:
        new_log_directory (str): Path to the directory where the log file will be saved.

    Returns:
        log_filepath (str): Path to the log file.
    """
    global log_directory  # Declare log_directory as global to modify it

    if new_log_directory is not None:
        log_directory = (
            new_log_directory  # Update log directory if a new path is provided
        )

    # Ensure log directory exists
    os.makedirs(log_directory, exist_ok=True)
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    # Get current date and time
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")

    # Use the timestamp in the log file name
    app_version = get_version()
    log_filename = f"fast5_rekindler_v{app_version}_{timestamp}.log"

    log_filepath = os.path.join(log_directory, log_filename)

    # Configure logger to log to the file
    # No need to call logger.remove() as we want to keep the default stderr handler
    logger.add(log_filepath, format="{time} {level} {message}")

    # Now logs will be sent to both the terminal and log_filename
    logger.opt(depth=1).info(f"Started FAST5 Rekindler v({app_version})\n")
    return log_filepath
