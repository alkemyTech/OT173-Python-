import logging
from time import strftime

logging.basicConfig(datefmt=logging.INFO, datefmt=("%Y-%m-%d"),
                    format='%(asctime)25.25s | %(levelname)10.10s | %(message)s')

log = logging.getLogger("dag_universidades_e")
