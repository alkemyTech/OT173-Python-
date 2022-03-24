import logging

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d'
    )
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
