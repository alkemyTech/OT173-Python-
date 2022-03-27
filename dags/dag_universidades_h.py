import logging

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d',
    level=logging.INFO,
    filemode="a"
    )
logger = logging.getLogger(__name__)
