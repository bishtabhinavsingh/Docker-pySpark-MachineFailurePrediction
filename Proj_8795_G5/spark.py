import glob
import importlib
import logging.config
import os
from functools import lru_cache
from os.path import join

from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

global spark_master
spark_master = None


def logger():
    return logging.getLogger("Proj_8795_G5.spark")


@lru_cache(maxsize=None)
def get_spark(master="local[*]", name="Proj_8795_G5"):
    global spark_master
    if master is None:
        master = spark_master
    else:
        spark_master = master
    return (SparkSession.builder
            .master(master)
            .appName(name)
            .getOrCreate())


@lru_cache(maxsize=None)
def get_SQLContext():
    return SQLContext(get_spark().sparkContext)


def spark_env_init(py_files, master, name="Proj_8795_G5"):
    # check if running in PySpark interpreter
    if importlib.util.find_spec("pyspark"):
        # sometimes needed to include paths for UDFs
        for f in py_files:
            get_spark(master=master, name=name).sparkContext.addPyFile(f)
        # spark_add_paths()


def spark_add_paths():
    """
    Adds the python files from the current directory to the Spark context (addPyfile).
    Used when running from the console (rather than command line, using -m)
    Necessary when using UDFs, which will result in undefined module error if files not loaded.
    Only need to load the files that define UDF; however, here, just load all.
    :return:
    """
    path = join(os.getcwd(), "Proj_8795_G5", "*.py")
    logger().debug("Path is {}".format(path))
    for f in glob.glob(path):
        logger().debug("Add file {} to path".format(f))
        get_spark().sparkContext.addPyFile(f)

