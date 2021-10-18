import __main__

from os import environ, listdir, path
import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession


def get_spark_session(app_name='case_study', master='local[*]',
                files=[], spark_config={}):
    """
    Method to start a Spark session and also uses config.json file for spark job.
    If the config file is present, the contents is parsed into a dict, 
    which are returned as the second element in the tuple returned by this method. 
    If the file is not present then the return tuple only contains the Spark session 
    and None for config.
    param app_name: Spark app name.
    param master: Spark cluster connection master details (defaults to local[*]).
    param files: File list to send to Spark cluster.
    param spark_config: Dictionary of config key-value pairs.
    return: Spark session and config dict tuple.
    """

    # detect execution environment
    check_environ = not(hasattr(__main__, '__file__'))

    if not (check_environ):
        # get Spark session
        spark_builder_app = (
            SparkSession
            .builder
            .appName(app_name))
    else:
        # get Spark session
        spark_builder_app = (
            SparkSession
            .builder
            .master(master)
            .appName(app_name))

        spark_files = ','.join(list(files))
        spark_builder_app.config('spark.files', spark_files)

        # add other config params
        for key, val in spark_config.items():
            spark_builder_app.config(key, val)

    # create spark session
    spark_ses = spark_builder_app.getOrCreate()

    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('configs.json')]

    if config_files:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_dict = json.load(config_file)
    else:
        config_dict = None

    return spark_ses, config_dict