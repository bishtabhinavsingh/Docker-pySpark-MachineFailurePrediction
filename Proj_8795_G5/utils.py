import logging
import sys
from typing import List


def logger(name="Proj_8795_G5", level=logging.DEBUG):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    return logger


# For better logging, see this: https://stackoverflow.com/questions/27016870/how-should-logging-be-used-in-a-python-package
def logging_init():
    # Reduce the logging, especially when running in PyCharm
    logging.getLogger('pyspark').setLevel(logging.WARNING)
    logging.getLogger("py4j").setLevel(logging.WARNING)
    # load the logging configuration
    try:
        logging.config.fileConfig('logging.ini')
    except KeyError:
        print("Cannot find logging.ini file")
    logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s: %(message)s',
                        datefmt='%y/%m/%d %H:%M:%S',
                        level=logging.DEBUG,
                        stream=sys.stdout)


def flatten(aList: List):
    return [item for sublist in aList for item in sublist]
