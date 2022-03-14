#!/bin/bash

# Run on GCP with data and code within Docker image

# Using Google GCP, don't forget to run these commands, which are used by spark.kubernetes.authenticate.driver.serviceAccountName
# https://spark.apache.org/docs/latest/running-on-kubernetes.html
# kubectl create serviceaccount spark
# kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default

# Spark on Kubernetes doesn't support submitting locally stored files with spark-submit
# This means many submit parameters will not work unless the path is http, including py-files, files, archives
# * https://stackoverflow.com/questions/61711673/how-to-submit-pyspark-job-on-kubernetes-minikube-using-spark-submit
# Rely on run_env.py to load and run modules, which must be build into the Docker image

# If your deployment is Pending for a long time, you may have an issue with insufficient resources
# To view pods:
#   kubectl get pods
# To view a pods description:
#   kubectl describe pod POD_NAME_FROM_GET_PODS_ABOVE
# If the above indicates insuffient resources in log, then delete old pods:
#   kubectl delete pods -l app=Proj_8795_G5

# Local spark to run spark-submit
# TODO: Check that this is the path to your spark.
# TODO: If running in WSL2 Linux, then ensure that this is the path to spark install in WSL2 Linux
SPARK_HOME=/usr/local/Cellar/apache-spark/3.2.0/

# URL of Kubernetes master (get IP from the GKE Cluster panel; port default is 443)
# TODO: Ensure that this is the name of your GCP cluster
CLUSTER_NAME=my-first-cluster-1

MASTER_IP=`gcloud container clusters list --filter name=${CLUSTER_NAME} --format='value(MASTER_IP)'`
MASTER=k8s://https://${MASTER_IP}:443

# Module here is source zip (not egg file)
PYSPARK_APP_MODULE=Proj_8795_G5

# Image which includes run_env.py in working directory (Kubernetes loads :latest)
# TODO: Ensure that this is the name of your GCP project and image name (following the gcr.io/)
# Format is gcr.io/PROJECT_NAME/IMAGE_NAME
IMAGE=gcr.io/cis-8795/bishtabhinav/Proj_8795_G5:1.0


# working directory on image (default set by spark image creation, docker-image-tool.sh)
# https://levelup.gitconnected.com/spark-on-kubernetes-3d822969f85b
WORKING_DIR=/opt/spark/work-dir

# Image execution, values may be overridden with Kubernetes: spark.kubernetes.driver.limit.cores, spark.kubernetes.driver.request.cores
EXECUTORS=1
# Cores per executor. Yarn defaults to 1. Increases threaded execution within executor
EXECUTOR_CORES=1
# Maximum memory necessary
EXECUTOR_MEMORY=2g
DRIVER_MEMORY=1g

# Optimize the job with spark.kubernetes.executor.request.cores=:
# https://www.datamechanics.co/blog-post/setting-up-managing-monitoring-spark-on-kubernetes
# This results in unscheduable pods, unless you set autoscaling on the cluster
#  --conf spark.dynamicAllocation.enabled=true \
#  --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
# spark.shuffle.memoryFraction) from the default of 0.2
# https://stackoverflow.com/questions/30797724/how-to-optimize-shuffle-spill-in-apache-spark-application

SCRIPT=local://${WORKING_DIR}/run_env.py  # Run your module
#SCRIPT=local://${WORKING_DIR}/sleep.py   # For debugging (e.g., checking files in image)
# Shell into container; kubectl exec <CONTAINER ID> -ti /bin/bash

SPARK_CMD="$SPARK_HOME/bin/spark-submit \
  --master ${MASTER} \
  --deploy-mode cluster \
  --driver-memory ${DRIVER_MEMORY}
  --executor-memory ${EXECUTOR_MEMORY} \
  --conf spark.executor.instances=${EXECUTORS} \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.executor.heartbeatInterval=20s \
  --conf spark.shuffle.io.retryWait=60s \
  --conf spark.sql.shuffle.partitions=1000 \
  --conf spark.executor.cores=${EXECUTOR_CORES} \
  --conf spark.kubernetes.container.image=${IMAGE} \
  --conf spark.kubernetes.container.image.pullPolicy=Always \
  --name ${PYSPARK_APP_MODULE} \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark
  --conf spark.kubernetes.executor.label.app=${PYSPARK_APP_MODULE} \
  --conf spark.kubernetes.driver.label.app=${PYSPARK_APP_MODULE} \
  --conf spark.kubernetes.driverEnv.PYSPARK_APP_MODULE=${PYSPARK_APP_MODULE} \
  --conf spark.kubernetes.driverEnv.PYTHONPATH=${WORKING_DIR}/${PYSPARK_APP_MODULE}.zip \
  --conf spark.kubernetes.executorEnv.PYTHONPATH=${WORKING_DIR}/${PYSPARK_APP_MODULE}.zip  \
  ${SCRIPT} \
  --master=${MASTER} --py_files=${WORKING_DIR}/${PYSPARK_APP_MODULE}.zip"

echo ${SPARK_CMD}
echo
eval ${SPARK_CMD}
