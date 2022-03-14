import logging.config

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.functions import collect_set, col, count
from pyspark.sql.functions import *
from pyspark.ml.feature import RFormula
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, NaiveBayes
from pyspark.ml import Pipeline, Model
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import termplotlib as tpl
import numpy as np

from Proj_8795_G5.spark import get_spark, get_SQLContext
from Proj_8795_G5.utils import logging_init, logger

import re
import wget
import findspark

__version__ = "0.1.0"

logging.getLogger('Proj_8795_G5').addHandler(logging.NullHandler())
logging_init()

# https://stackoverflow.com/questions/53217767/py4j-protocol-py4jerror-org-apache-spark-api-python-pythonutils-getencryptionen
# Only necessary when running in Docker Python as a single node process (without entrypoint setup)
findspark.init()

url_path = 'https://docs.google.com/uc?export=download&id=1AN9yp14GuYcOwIgCjfwlQzQySKdwADZD'

def main():
    logger().info("Start of Proj_8795_G5")
    logger().debug("Reading data")
    df = read_data(url_path)
    print("Data load success")
    print(df)
    logger().debug("Data loaded")
    logger().debug("Preparing data")
    train_data, test_data, columns = prep_data(df)
    logger().debug("Creating pipeline and running models")
    cvModel, paramGrid = model(train_data, columns)
    logger().debug("Model evaluation")
    metrics, model_names = model_eval(cvModel, paramGrid)
    logger().debug("Compare Model Plot")
    plotter(metrics, model_names)
    logger().debug("Calculate metrics")
    metrics, predictions = metric(test_data, cvModel)
    logger().debug("Model metrics, actuals vs predictions")
    confusion(metrics, predictions)
    logger().info("End of Proj_8795_G5")

def read_data(url_path):
    try:
        wget.download(url_path)
    except Exception as e:
        logger().error("Cannot load data from path: " + str(e))
        exit(1)
    pdf = pd.read_csv('predictive_maintenance.csv')
    df = get_SQLContext().createDataFrame(pdf)
    return (df)

def prep_data(df):
    df = df.withColumnRenamed("Product ID","Product_ID")
    df = df.withColumnRenamed("Air temperature [K]","air_temp")
    df = df.withColumnRenamed("Process temperature [K]","pro_temp")
    df = df.withColumnRenamed("Rotational speed [rpm]","rot_speed")
    df = df.withColumnRenamed("Torque [Nm]","Torq")
    df = df.withColumnRenamed("Tool Wear [min]","tool_wear")
    df = df.withColumnRenamed("Failure Type","fail_type")

    columns = df.columns

    columns.remove('Product_ID')
    columns.remove('fail_type')
    columns.remove('UDI')
    columns.remove('Type')
    columns.remove('Target')
    train_data, test_data  = df.randomSplit([0.6, 0.4], 24)   # proportions [], seed for random
    print("Number of training records: " + str(train_data.count()))
    print("Number of testing records : " + str(test_data.count()))
    return (train_data, test_data, columns)

def model(train_data, columns):
    formula = "{} ~ {}".format("Target", " + ".join(columns))
    print("Formula : {}".format(formula))
    rformula = RFormula(formula = formula)

    # Pipeline basic to be shared across model fitting and testing
    pipeline = Pipeline(stages=[])  # Must initialize with empty list!

    # base pipeline (the processing here should be reused across pipelines)
    basePipeline = [rformula]

    #############################################################
    # Logistic Regression model

    #############################################################
    lr = LogisticRegression(maxIter=10)
    pl_lr = basePipeline + [lr]
    pg_lr = ParamGridBuilder()\
              .baseOn({pipeline.stages: pl_lr})\
              .addGrid(lr.regParam,[0.01, .04])\
              .addGrid(lr.elasticNetParam,[0.1, 0.4])\
              .build()

    #############################################################
    # Random Forest Classifier model

    #############################################################
    rf = RandomForestClassifier(numTrees=50)
    pl_rf = basePipeline + [rf]
    pg_rf = ParamGridBuilder()\
          .baseOn({pipeline.stages: pl_rf})\
          .build()

    #############################################################
    # NaiveBayes model

    #############################################################
    nb = NaiveBayes()
    pl_nb = basePipeline + [nb]
    pg_nb = ParamGridBuilder()\
          .baseOn({pipeline.stages: pl_nb})\
          .addGrid(nb.smoothing,[0.4,1.0])\
          .build()

    # One grid from the individual grids
    paramGrid = pg_lr + pg_rf + pg_nb

    cv = CrossValidator()\
          .setEstimator(pipeline)\
          .setEvaluator(BinaryClassificationEvaluator())\
          .setEstimatorParamMaps(paramGrid)\
          .setNumFolds(3)

    cvModel = cv.fit(train_data.limit(500))
    return(cvModel, paramGrid)

def paramGrid_model_name(model):
  params = [v for v in model.values() if type(v) is not list]
  name = [v[-1] for v in model.values() if type(v) is list][0]
  name = re.match(r'([a-zA-Z]*)', str(name)).groups()[0]
  return "{}{}".format(name,params)

def model_eval(cvModel, paramGrid):
    print("Best Model")
    print(cvModel.getEstimatorParamMaps()[ np.argmax(cvModel.avgMetrics) ])
    print("Worst Model")
    print (cvModel.getEstimatorParamMaps()[ np.argmin(cvModel.avgMetrics) ])
    measures = zip(cvModel.avgMetrics, [paramGrid_model_name(m) for m in paramGrid])
    metrics,model_names = zip(*measures)
    return (metrics, model_names)

def plotter(metrics, model_names):
    fig = tpl.figure()
    fig.barh(metrics, model_names, force_ascii=True)
    fig.show()

def metric(test_data, cvModel):
    predictions = cvModel.transform(test_data)
    predictionAndLabels = predictions.select('prediction','label').rdd
    # Instantiate metrics object
    metrics = BinaryClassificationMetrics(predictionAndLabels)
    return (metrics, predictions)

def confusion(metrics, predictions):
    # Area under precision-recall curve
    print("Area under PR = %s" % metrics.areaUnderPR)
    # Area under ROC curve
    print("Area under ROC = %s" % metrics.areaUnderROC)
    # actuals and predited values
    print('Number of actual 1:', predictions.where('Target=1').count())
    print('Number of actual 0:', predictions.where('Target=0').count())
    print('Number of predicted 1:', predictions.where('prediction=1').count())
    print('Number of predicted 0:', predictions.where('prediction=0').count())

