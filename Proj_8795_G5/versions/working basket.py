import logging.config

import pandas as pd
import pyspark.sql.functions as F

from airbnb.spark import get_spark, get_SQLContext
from airbnb.utils import logging_init, logger
import pandas as pd
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules

def to_binary(x):
    if x <= 0:
        return 0
    if x >= 1:
        return 1

pdf = pd.read_csv("data.csv", converters={i: str for i in range(100)})
pdf.dropna(axis=0, subset=['InvoiceNo'], inplace=True)
pdf['InvoiceNo'] = pdf['InvoiceNo'].astype('str')
pdf["Quantity"] = pd.to_numeric(pdf["Quantity"], errors = "coerce", downcast = "integer")
    #nums = {str(x) for x in range(100)}
    #repeat_words = {"pack", "set", "of", ".", "small", "large", "blue", "black", "green", "red", "yellow", "gold", "silver", "white"}
    #pdf['StockCode'] = [str(word).lower() for word in pdf['StockCode']]
    #pdf['Description'] = pdf['Description'].apply(lambda x: ' '.join([word for word in x.split() if word not in repeat_words]))
    #pdf['Description'] = pdf['Description'].apply(lambda x: ' '.join([word for word in x.split() if word not in nums]))
    #pdf['StockCode'] = [" ".join(str(x).split()) for x in pdf['StockCode']]
    #pdf['Description'] = [word for word in pdf['Description'] if not str(word) in repeat_words]
    #pdf = pdf[['InvoiceNo','StockCode']]
    #print(pdf['StockCode'])
pdf = pdf.groupby(['CustomerID', 'Description'])['Quantity'].sum().unstack().reset_index().fillna(0).set_index('CustomerID')
pdf = pdf.applymap(to_binary)

    #pdf["CustomerID"] = pdf.index
    #pdf.columns = ['StockCode']
    #pdf['StockCode'] = [x.split(',') for x in pdf['StockCode']]
print(pdf)
    #df = get_SQLContext().createDataFrame(pdf)
    #df.printSchema()
    #df.drop("InvoiceDate")
    #df.drop("Country")
    #df.drop('InvoiceNo')
    #df = df.withColumn('InvoiceNo', df['InvoiceNo'].cast('int'))
    #df = df.withColumn('StockCode', df['StockCode'].cast('string'))
    #df = df.withColumn('Description', df['Description'].cast('string'))
    #df = df.withColumn('Quantity', df['Quantity'].cast('int'))
    #df = df.withColumn('UnitPrice', df['UnitPrice'].cast('double'))
    #df = df.withColumn('CustomerID', df['CustomerID'].cast('int'))
frequent_itemsets = apriori(pdf, min_support=0.07, use_colnames=True)
rules = association_rules(frequent_itemsets, metric="lift", min_threshold=1)
print(rules.head())



