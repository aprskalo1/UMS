import logging
import os

os.makedirs('logs', exist_ok=True)

logger = logging.getLogger('embedder')
logger.setLevel(logging.INFO)

fh = logging.FileHandler('logs/embedder.log')
fh.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)