from __future__ import annotations

import logging
import os
from datetime import datetime

import numpy as np
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import mean, stddev, max, udf
from pyspark.sql.types import ArrayType, DoubleType

# Configure logging
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

# Load configuration from env variables
project_id = os.getenv('PROJECT_ID')
topic = os.getenv('TOPIC')
subscription = os.getenv('SUBSCRIPTION')
bucket_name = os.getenv('BUCKET_NAME')
folder_path = 'summarized'
keyfile_path = ''

# Set up binning options
batch_size = int(os.environ.get('BATCH_SIZE', '100'))
num_bins = int(os.environ.get('NUM_BINS', '10'))
lower_bound = float(os.environ.get('LOWER_BOUND', '0'))
upper_bound = float(os.environ.get('UPPER_BOUND', '50'))


def get_session() -> SparkSession:
    """Create a spark session for the Spark application.

    Returns:
        SparkSession.
    """
    # Configure Spark
    spark = SparkSession.builder \
    .appName("Read from BigQuery Stream") \
    .getOrCreate()

    # Configure GCS credentials
    spark.conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    spark.conf.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", keyfile_path)

    # Set GCS as the default file system
    spark.conf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    spark.conf.set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

    # Get app id
    app_id = spark.conf.get("spark.app.id")

    logging.info(f"SparkSession started successfully for app: {app_id}")

    return spark