import logging
import logging.config
import logging.handlers
from os import path

# I put the path of logging.cfg into a variable
log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging.cfg')
# I configure the logs using the logging.cfg file
logging.config.fileConfig(log_file_path)
logger = logging.getLogger("Grupo_Datos_A")
logger.info("running")
