import logging.config

import pandas as pd
import pyspark.sql.functions as F
from pyspark.ml.feature import RFormula
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import regexp_extract

from airbnb.spark import get_spark, get_SQLContext
from airbnb.utils import logging_init, logger
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType
from pyspark.ml.fpm import FPGrowth


import wget
import findspark

__version__ = "0.1.0"

logging.getLogger('airbnb').addHandler(logging.NullHandler())
logging_init()

# https://stackoverflow.com/questions/53217767/py4j-protocol-py4jerror-org-apache-spark-api-python-pythonutils-getencryptionen
# Only necessary when running in Docker Python as a single node process (without entrypoint setup)
findspark.init()

data_url = "tmp/data.csv"


def main():
    logger().info("Start of Recommender Project")
    logger().debug("Reading data")
    df = read_data(data_url)
    logger().debug("Preparing data")
    # df = prep_data(df)
    logger().debug("Creating model")
    fitted_RFormula(df)
    model.freqItemsets.show()
    print("Model Asso Rules")
    model.associationRules.show()
    create_model(model)
    logger().info("End of airbnb")


def read_data(data_url):
    try:
        pd.read_csv(data_url)
    except Exception as e:
        logger().error("Cannot load data from path: " + str(e))
        exit(1)
    pdf = pd.read_csv(data_url, converters={i: str for i in range(100)})
    df = get_SQLContext().createDataFrame(pdf)
    cols = ("InvoiceDate", "Country")
    df.drop(cols)
    df = df.withColumn('InvoiceNo', df['InvoiceNo'].cast('int'))
    df = df.withColumn('StockCode', df['StockCode'].cast('string'))
    df = df.withColumn('Description', df['Description'].cast('string'))
    df = df.withColumn('Quantity', df['Quantity'].cast('int'))
    df = df.withColumn('UnitPrice', df['UnitPrice'].cast('double'))
    df = df.withColumn('CustomerID', df['CustomerID'].cast('int'))
    return (df)


def fitted_RFormula(df):
    fpGrowth = FPGrowth(itemsCol="Description", minSupport=0.5, minConfidence=0.6)
    model = fpGrowth.fit(df)
    return (model)

def create_model(model):
    model.transform(df).show()

