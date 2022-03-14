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
    PYTHON=python3.7 \
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

# Just for the simple single node run in Python
ENV PYTHONPATH=${APP_DIR}/Proj_8795_G5.zip:$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH


# Package installed directly into this image
# (Note: package can be included in submit/sparkoperator instead of this appoach)
COPY dist/Proj_8795_G5.zip ${APP_DIR}

# spark-submit local:///opt/spark/work-dir/run.py args...
# Install Python driver for modules (sleep for testing)
COPY run_env.py sleep.py ${APP_DIR}

# remove base entrypoint
#ENTRYPOINT []

# Simple single node run of program
CMD python3 -m Proj_8795_G5

COPY predictive_maintenance.csv ${APP_DIR}