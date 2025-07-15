import logging
import os
from scr.utility import configuration

# Create logs directory if not exists
LOG_DIR = configuration.config["logging"]["dir"]
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Configure logging
LOG_FILE = os.path.join(LOG_DIR, "app.log")
logging.basicConfig(
    level=configuration.config["logging"]["level"],  # Default level
    format="%(asctime)s - %(levelname)s - %(filename)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),  # Write logs to a file
        logging.StreamHandler()  # Print logs to console
    ]
)

# Create a logger
logger = logging.getLogger(__name__)

