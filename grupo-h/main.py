import logging
import logging.config

logging.config.fileConfig('logging.cfg')

logger = logging.getLogger('DatosH')
logger.info("Logger loaded")
