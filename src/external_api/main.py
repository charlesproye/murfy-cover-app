import logging
import os

# hypercorn logging format
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "DEBUG"),
    format="[%(asctime)s] [%(process)d] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %z",
)

