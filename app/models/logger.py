import logging

def get_logger(name):
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger

    # Create handlers
    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler('error.log', mode='w')

    # Set levels for handlers
    console_handler.setLevel(logging.INFO)
    file_handler.setLevel(logging.ERROR)

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger
