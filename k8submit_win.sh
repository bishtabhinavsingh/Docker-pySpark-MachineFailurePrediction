#!/bin/bash

# Run on Docker for Windows with data and source in Docker image

# Spark on Kubernetes doesn't support submitting locally stored files with spark-submit
# This means many submit parameters will not work unless the path is http, including py-files, files, archives
# * https://stackoverflow.com/questions/61711673/how-to-submit-pyspark-job-on-kubernetes-minikube-using-spark-submit
# Rely on run_env.py to load and run modules, which must be built into the Docker image (or mounted)

# Local spark to run spark-submit
# TODO: Check that this is the path to your spark.
# TODO: If running in WSL2 Linux, then ensure that this is the path to spark install in WSL2 Linux
SPARK_HOME=/usr/local/Cellar/apache-spark/3.2.0/

# URL of Kubernetes master on Docker desktop
MASTER=k8s://https://localhost:6443

# Module here is source zip (not egg file). Must create a zip file from the source
PYSPARK_APP_MODULE=Proj_8795_G5

# Image execution, values may be overridden with Kubernetes: spark.kubernetes.driver.limit.cores, spark.kubernetes.driver.request.cores
EXECUTORS=1
EXECUTOR_MEMORY=2g
DRIVER_MEMORY=1g

# Image which includes run_env.py in working directory
# TODO: Ensure that this is the name of the image you built
IMAGE=bishtabhinav/proj_8795_g5:1.0

# working directory on image (default set by spark image creation, docker-image-tool.sh)
# https://levelup.gitconnected.com/spark-on-kubernetes-3d822969f85b
WORKING_DIR=/opt/spark/work-dir

SCRIPT=local://${WORKING_DIR}/run_env.py  # Run your module
#SCRIPT=local://${WORKING_DIR}/sleep.py   # For debugging (e.g., checking the mount)
# Shell into container; kubectl exec <CONTAINER ID> -ti /bin/bash

SPARK_CMD="$SPARK_HOME/bin/spark-submit \
  --master ${MASTER} \
  --deploy-mode cluster \
  --driver-memory ${DRIVER_MEMORY}
  --executor-memory ${EXECUTOR_MEMORY} \
  --conf spark.executor.instances=${EXECUTORS} \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.kubernetes.container.image=${IMAGE} \
  --name ${PYSPARK_APP_MODULE} \
  --conf spark.kubernetes.executor.label.app=${PYSPARK_APP_MODULE} \
  --conf spark.kubernetes.driver.label.app=${PYSPARK_APP_MODULE} \
  --conf spark.kubernetes.driverEnv.PYSPARK_MAJOR_PYTHON_VERSION=3 \
  --conf spark.kubernetes.driverEnv.PYSPARK_APP_MODULE=${PYSPARK_APP_MODULE} \
  ${SCRIPT} \
  --master=${MASTER} --py_files=${WORKING_DIR}/${PYSPARK_APP_MODULE}.zip"
#py runs on image

echo ${SPARK_CMD}
echo
eval ${SPARK_CMD}
