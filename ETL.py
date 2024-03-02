import csv
from cattrs import ClassValidationError
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators.python impor
from airflow.models import Variable
import os
import csv

def pull_data():
    file_path = "C:/Users/user/Desktop/Data_pieline_Gcp/Data/super store sales data set.csv"
    df = pd.read_csv(file_path)
    return df


def _move_dataframe_to_gcs(**kwargs): 
    csv_filename =  "C:/Users/user/Desktop/Data_pieline_Gcp/Data/super store sales data set.csv"
    print(csv_filename)
    bucket_name = 'bucket_name'
    destination_blob_name = 'destination_path'
    upload_command = f"gsutil cp {csv_filename} gs://{bucket_name}/{destination_blob_name}"
    # print(upload_command)
    try:
        # Attempt to upload the file
        print(f"Uploading {csv_filename} to GCS...")
        result = os.system(upload_command)
        print(result)
        
        # Check the result of the upload
        if result == 0:
            print("Upload successful!")
        else:
            print("Upload failed!")
            # Add your logic here for handling the failure
            
    except Exception as e:
        print(f"Error during upload: {e}")
_move_dataframe_to_gcs()


def _load_data_to_bigquery(**kwargs):
    dataset_id = 'mysql_to_gbq'
    table_id = 'employees'
    bucket_name = 'mysql-to-gbq-test'
    csv_filename =  "C:/Users/user/Desktop/Data_pieline_Gcp/Data/super store sales data set.csv"
    load_command = f"bq load --autodetect --source_format=CSV {dataset_id}.{table_id} gs://{bucket_name}/{csv_filename}"

    
    
    with DAG("move_dataframe_to_gcs", start_date=datetime(2023, 3, 2),
    schedule_interval='@daily', max_active_runs=1, catchup=False) as dag:
        Move_Data_Gcs = PythonOperator(
            task_id="move_data_to_gcs",
            python_callable = _move_dataframe_to_gcs,provide_context=True, dag=dag
            # python_callable = _load_data_to_bigquery,provide_context=True, dag=dag
    )

    with DAG("load_dta_big_query", start_date=datetime(2023, 3, 2),
    schedule_interval='@daily', max_active_runs=1, catchup=False) as dag:
        load_data_to_bigquery = PythonOperator(
            task_id="load_dta_big_query",
            python_callable = _load_data_to_bigquery,provide_context=True, dag=dag
    )

        # Move_Data_Gcs >> load_data_to_bigquery