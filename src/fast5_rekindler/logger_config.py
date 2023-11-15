from loguru import logger as base_logger
from datetime import datetime
import os
import toml

# Configure the base logger
logger = base_logger
# Default log directory
log_directory = '.'  

def get_version():
    with open("pyproject.toml", encoding='utf-8') as file:
        app_version = toml.load(file)["tool"]["poetry"]["version"]
    return app_version
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
    app_version = get_version()
    log_filename = f"fast5_rekindler_v{app_version}_{timestamp}.log"
    
    log_filepath = os.path.join(log_directory, log_filename)

    # Configure logger to log to the file
    # No need to call logger.remove() as we want to keep the default stderr handler
    logger.add(log_filepath, format="{time} {level} {message}")

    # Now logs will be sent to both the terminal and log_filename
    logger.opt(depth=1).info(f'Started Fast5 Rekindler v({app_version})\n')
    return log_filepath
    
if __name__ == "__main__":
    # Initial configuration
    configure_logger()