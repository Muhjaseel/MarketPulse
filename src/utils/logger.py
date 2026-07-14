import logging
import sys

def get_logger(name: str) -> logging.Logger:
    """
    Creates a standardized, clean console logger for tracking data pipeline 
    execution states across all modules.
    """
    logger = logging.getLogger(name)
    
    # If the logger has already been initialized, return it to prevent duplicate handler traps
    if logger.handlers:
        return logger
        
    logger.setLevel(logging.INFO)
    
    # Clean layout: Timestamp | Log Level | Module Name | Message
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Setup console transmission channel
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger