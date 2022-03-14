# Build spark image to run on Kubernetes
# See https://levelup.gitconnected.com/spark-on-kubernetes-3d822969f85b
FROM newfrontdocker/spark-py:v3.0.1-j14

# Reset to root to run installation tasks
USER 0

# Specify the official Spark User, working directory, and entry point
WORKDIR /opt/spark/work-dir

# app dependencies
# Spark official Docker image names for python
ENV APP_DIR=/opt/spark/work-dir \
    PYTHON=python3 \
    PIP=pip3

# Preinstall dependencies
COPY requirements.txt ${APP_DIR}
RUN ${PIP} install -r requirements.txt \
    && rm -f ${APP_DIR}/requirements.txt

# Specify the User that the actual main process will run as
ARG spark_uid=185
# Need home directory to download Python module data (NLTK)
RUN useradd -d /home/spark -ms /bin/bash -u ${spark_uid} spark \
    && chown -R spark /opt/spark/work-dir
USER ${spark_uid}

# Spark on Kubernetes doesn't support submitting locally stored files with spark-submit
# * https://stackoverflow.com/questions/61711673/how-to-submit-pyspark-job-on-kubernetes-minikube-using-spark-submit
# spark-submit local:///opt/spark/work-dir/run.py args...
# Install Python driver for modules (sleep for testing)
COPY run_env.py sleep.py ${APP_DIR}
