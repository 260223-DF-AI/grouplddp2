import logging

def get_logger(name, log_name):
    # Create logger
    logger = logging.getLogger(name)

    # Prevent duplicate handlers
    if logger.handlers:
        return logger

    # Set logger level
    logger.setLevel(logging.INFO)

    # Create handlers
    file_handler = logging.FileHandler(log_name, mode='w')

    # Set levels for handlers
    file_handler.setLevel(logging.INFO)

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    
    return logger







def get_logger(name: str, log_file: str, level=logging.INFO) -> logging.Logger:
    """
    Create a logger that writes to a specific file.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.FileHandler(log_file)
        handler.setLevel(level)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)

        logger.addHandler(handler)

    return logger