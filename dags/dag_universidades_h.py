import logging

logging.basicConfig(
    format='%(asctime)-5s %(name)-15s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d',
    level=logging.INFO,
    filemode="a"
    )
logger = logging.getLogger(__name__)
