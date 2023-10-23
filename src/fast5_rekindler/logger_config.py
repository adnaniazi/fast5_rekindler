from loguru import logger as base_logger
from datetime import datetime
import os
import toml

with open("pyproject.toml", encoding='utf-8') as file:
    app_version = toml.load(file)["tool"]["poetry"]["version"]
    
# Configure the base logger
logger = base_logger

log_directory = '.'  # Default log directory

def configure_logger(new_log_directory=None):
    global log_directory  # Declare log_directory as global to modify it
    
    if new_log_directory is not None:
        log_directory = new_log_directory  # Update log directory if a new path is provided

    # Ensure log directory exists
    os.makedirs(log_directory, exist_ok=True)
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    
    # Get current date and time
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")

    # Use the timestamp in the log file name
    log_filename = f"capfinder_v{app_version}_{timestamp}.log"
    
    log_filepath = os.path.join(log_directory, log_filename)

    # Configure logger to log to the file
    # No need to call logger.remove() as we want to keep the default stderr handler
    logger.add(log_filepath, format="{time} {level} {message}")

    # Now logs will be sent to both the terminal and log_filename
    logger.info("CAPFINDER")
    
# Initial configuration
configure_logger()