import logging

class LoggerConfig:
    def __init__(self,name,log_file):
        self.logger = self.setup_logger(name, log_file)

    def setup_logger(self, name, log_file):

        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        console_handler = logging.StreamHandler()  # Log to console
        file_handler = logging.FileHandler(log_file)  # Log to file

        console_handler.setLevel(logging.INFO)  # Console logs only INFO and above
        file_handler.setLevel(logging.DEBUG)  # File logs DEBUG and above

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)  # Apply format to console handler
        file_handler.setFormatter(formatter)  # Apply format to file handler

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

        return logger



