import logging.config

import pandas as pd
import pyspark.sql.functions as F

from airbnb.spark import get_spark, get_SQLContext
from airbnb.utils import logging_init, logger
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.ml.fpm import FPGrowth


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
    df.printSchema()
    logger().debug("Preparing data")
    # df = prep_data(df)
    logger().debug("Creating model")
    create_and_test_model(df)
    print(als.explainParams())
    create_model(predictions)
    print(" Root-mean-square error = %f" % rmse)
    logger().info("End of airbnb")




def read_data(data_url):
    try:
        pd.read_csv(data_url)
    except Exception as e:
        logger().error("Cannot load data from path: " + str(e))
        exit(1)
    pdf = pd.read_csv(data_url, converters={i: str for i in range(100)})
    pdf['StockID'] = range(len(pdf['StockCode']))
    df = get_SQLContext().createDataFrame(pdf)
    df.drop("InvoiceDate")
    df.drop("Country")
    df = df.withColumn('InvoiceNo', df['InvoiceNo'].cast('int'))
    df = df.withColumn('StockCode', df['StockCode'].cast('string'))
    df = df.withColumn('Description', df['Description'].cast('string'))
    df = df.withColumn('Quantity', df['Quantity'].cast('int'))
    df = df.withColumn('UnitPrice', df['UnitPrice'].cast('double'))
    df = df.withColumn('CustomerID', df['CustomerID'].cast('int'))
    df = df.withColumn('StockID', df['StockID'].cast('int'))
    return (df)


def create_and_test_model(df):
    als = ALS() \
        .setMaxIter(5) \
        .setRegParam(0.01) \
        .setUserCol("CustomerID")\
        .setItemCol("StockID")
    training, test = df.randomSplit([0.8, 0.2])
    alsModel = als.fit(training)
    predictions = alsModel.transform(test)
    return (predictions)


def create_model(predictions):
    evaluator = RegressionEvaluator()\
        .setMetricName("rmse")\
        .setLabelCol("rating")\
        .setPredictionCol("prediction")
    rmse = evaluator.evaluate(predictions)
    return (rmse)
