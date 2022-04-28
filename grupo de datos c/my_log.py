import logging
import logging.config
import os


def my_log(name):
    base_path = os.path.dirname(__file__)

    # create logger
    logging.config.fileConfig(os.path.join(base_path, 'logging.cfg'))
    logger = logging.getLogger(name)
    return logger
