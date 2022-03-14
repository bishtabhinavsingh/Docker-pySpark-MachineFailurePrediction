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
    print(df)
    logger().debug("Preparing data")
    # df = prep_data(df)
    logger().debug("Creating model")
    fitted_RFormula(df)
    logger().info("End of airbnb")

def read_data(data_url):
    try:
        pd.read_csv(data_url)
    except Exception as e:
        logger().error("Cannot load data from path: " + str(e))
        exit(1)
    pdf = pd.read_csv(data_url, converters={i: str for i in range(100)})
    #nums = {str(x) for x in range(100)}
    #repeat_words = {"pack", "set", "of", ".", "small", "large", "blue", "black", "green", "red", "yellow", "gold", "silver", "white"}
    pdf['StockCode'] = [str(word).lower() for word in pdf['StockCode']]
    #pdf['Description'] = pdf['Description'].apply(lambda x: ' '.join([word for word in x.split() if word not in repeat_words]))
    #pdf['Description'] = pdf['Description'].apply(lambda x: ' '.join([word for word in x.split() if word not in nums]))
    pdf['StockCode'] = [" ".join(str(x).split()) for x in pdf['StockCode']]
    #pdf['Description'] = [word for word in pdf['Description'] if not str(word) in repeat_words]
    pdf = pdf[['InvoiceNo','StockCode']]
    print(pdf['StockCode'])
    pdf = pdf.groupby('InvoiceNo').agg([('StockCode',','.join)])
    #pdf["CustomerID"] = pdf.index
    pdf.columns = ['StockCode']
    pdf['StockCode'] = [x.split(',') for x in pdf['StockCode']]
    print(pdf)
    df = get_SQLContext().createDataFrame(pdf)
    df.printSchema()
    #df.drop("InvoiceDate")
    #df.drop("Country")
    #df.drop('InvoiceNo')
    #df = df.withColumn('InvoiceNo', df['InvoiceNo'].cast('int'))
    #df = df.withColumn('StockCode', df['StockCode'].cast('string'))
    #df = df.withColumn('Description', df['Description'].cast('string'))
    #df = df.withColumn('Quantity', df['Quantity'].cast('int'))
    #df = df.withColumn('UnitPrice', df['UnitPrice'].cast('double'))
    #df = df.withColumn('CustomerID', df['CustomerID'].cast('int'))
    return (df)

def fitted_RFormula(df):
    fpGrowth = FPGrowth(itemsCol='StockCode', minSupport=0.5, minConfidence=0.6)
    model = fpGrowth.fit(df)
    model.freqItemsets.show()
    print("Model Asso Rules")
    model.associationRules.show()
    model.transform(df).show()


