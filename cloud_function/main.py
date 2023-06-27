import os
from google.cloud import dataproc_v1 as dataproc

def spark_submit(event, context):
    project_id = os.getenv("PROJECT")
    bucket = os.getenv("BUCKET")
    region = os.getenv("REGION")
    dag = os.getenv("DAG_PATH")
    dataset = os.getenv("DATASET")

    job_client = dataproc.JobControllerClient(client_options={'api_endpoint': '{}-dataproc.googleapis.com:443'.format(region)})
    job = {
        'placement': {
            'cluster_name': "multinode-spark-cluster"
        },
        'pyspark_job': {
            'main_python_file_uri': f'gs://{bucket}/{dag}',
            'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
            'args': ['--bucket', bucket, '--input_folder', f'gs://{bucket}/hires','--station_data', f'gs://{bucket}/london_cycle_stations.csv', 
                     '--project', project_id, '--table_hires', f'{dataset}.cycle_hires', '--table_daily_agg', f'{dataset}.daily_agg']
        }
    }
    job_client.submit_job(request={"project_id": project_id, "region": region, "job": job})