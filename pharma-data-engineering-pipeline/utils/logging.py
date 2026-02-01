import logging
import os
from datetime import datetime

def get_logger(pipeline_name: str, layer: str = "general"):
    """
    Creates a logger with a timestamped log file.
    """

    # timestamp for this run
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # logs/ingestion/ingestion_2025-01-01_10-30-22.log
    log_dir = f"logs/{layer}"
    os.makedirs(log_dir, exist_ok=True)

    log_file = f"{log_dir}/{pipeline_name}_{timestamp}.log"

    logger = logging.getLogger(pipeline_name)
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers
    if not logger.handlers:

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
