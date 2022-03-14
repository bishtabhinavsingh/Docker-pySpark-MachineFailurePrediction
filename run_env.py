

"""
Wrapper script to use when running Python packages via egg file through spark-submit.

Rename this script to the fully qualified package and module name you want to run.
The module should provide a ``main`` function.

Pass any additional arguments to the script.

Usage:

  spark-submit --py-files <LIST-OF-EGGS> <PACKAGE>.<MODULE>.py <MODULE_ARGS>

See:
  https://stackoverflow.com/questions/47905546/how-to-pass-python-package-to-spark-job-and-invoke-main-file-from-package-with-a/52066867
"""
import importlib
import logging
import os
import sys
from os import listdir


def main():
    logger = logging.getLogger("run_env")
    print("run_env executing in {}".format(os.getcwd()))
    print(" the python sys path is: {}".format(sys.path))
    for f in listdir("."):
        print("file/folder in this directory: {}".format(f))
    env_key = "PYSPARK_APP_MODULE"
    print("Loading module specified in environment variable {}.".format(env_key))
    module_name = None
    try:
        module_name = os.environ.get(env_key)
    except KeyError:
        logger.error("No environment variable {} specified. Cannot load module.".format(env_key))
    if module_name:
        module = importlib.import_module(module_name)
        print("Imported module {}. Calling main".format(module_name))
        module.main()
        print("Done in main. Exiting.".format(module_name))

if __name__ == '__main__':
    main()
