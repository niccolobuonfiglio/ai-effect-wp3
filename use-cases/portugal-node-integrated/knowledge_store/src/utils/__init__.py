import logging

class RootLogger:
    @staticmethod
    def get_root_logger():
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(__name__)
