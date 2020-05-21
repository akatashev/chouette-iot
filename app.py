import gevent.monkey
import logging
from chouette import Scheduler
import time

VERSION = "0.0.1a"

logger = logging.getLogger("chouette")


if __name__ == "__main__":
    logger.setLevel("INFO")
    gevent.monkey.patch_all()
    logger.critical("Starting Chouette version %s.", VERSION)
