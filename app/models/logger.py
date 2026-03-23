import logging

def get_logger(name, log_name):
    # Create logger
    logger = logging.getLogger(name)
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger

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
