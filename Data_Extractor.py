from __future__ import annotations

import logging
import os
from datetime import datetime

import numpy as np
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import mean, stddev, max, udf
from pyspark.sql.types import ArrayType, DoubleType
from tables import Column

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
print(num_bins)
print(lower_bound)
print(upper_bound)

cluster_manager  = "spark://164.92.85.68:7077"

def get_session() -> SparkSession:
    """Create a spark session for the Spark application.

    Returns:
        SparkSession.
    """
    # Configure Spark
    spark = SparkSession.builder \
    .appName("Read from BigQuery Stream") \
    .config('spark.master',cluster_manager)\
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
spark = get_session()


def histogram():
    
    num_bins = int(os.environ.get('NUM_BINS', '10'))
    lower_bound = float(os.environ.get('LOWER_BOUND', '0'))
    upper_bound = float(os.environ.get('UPPER_BOUND', '50'))
    column = "Demo_Column"
    
    """
    Calculate histogram of a column using numpy library.
    
    Parameters:
        column (str): column to calculate histogram on
        num_bins (int): number of bins to divide the range into
        lower_bound (float): minimum value of the range
        upper_bound (float): maximum value of the range

    Returns:
        pyspark.sql.Column: Column containing the histogram values
    """
# histogram()
    def histogram_func(values):
        hist, edges = np.histogram(values, bins=num_bins, range=(lower_bound, upper_bound))
        return list(hist.astype(np.double))

    return udf(histogram_func, ArrayType(DoubleType()))(column)

# Create example DataFrame
data = [(1,), (2,), (3,), (4,), (5,)]
columns = ["Demo_Column"]
df = spark.createDataFrame(data, columns)

# Apply the UDF to the DataFrame
df_result = df.select(histogram().alias("histogram_result"))

# Show the result
print(df_result.show(truncate=False))
# histogram_udf = histogram()
# print(histogram_udf)
# histogram()



