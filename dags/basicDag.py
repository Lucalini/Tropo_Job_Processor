from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta
import time
import docker
import logging

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


@dag(
    dag_id='time_based_demo_dag',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['Luca'],
)
def tropo_job_dag():
    
    @task
    def data_search():
        logging.info("Searching S3 for data over [DATE_RANGE]")
        time.sleep(10)
        s3_urls = ["s3://example_bucket", "s3://example_bucket2"]
        return s3_urls


    @task_group(group_id="tropo_job_group")
    def process_tropo_object(s3_url):

        @task
        def job_preprocessing():
            logging.info("Preprocessing job")
            time.sleep(10)
            return "Preprocessed job"

        @task
        def spinup_workers(s3_url):
            print(f"Processing {s3_url}")
            client = docker.from_env()
            try:
                container = client.containers.run(
                    image="hello-world",
                    command= None,
                    remove=True,
                    detach=False,
                )
                print(f"Container output: {container}")
                time.sleep(10)
                return f"Processed {s3_url} successfully"
            except Exception as e:
                print(f"Error processing {s3_url}: {str(e)}")
                raise

        @task 
        def post_processing():
            logging.info("PostProcessing job")
            time.sleep(10)
            return "Postprocessed job"

        preprocessing_result = job_preprocessing()
        processing_result = spinup_workers(s3_url)
        post_processing_result = post_processing()

        preprocessing_result >> processing_result >> post_processing_result
        

    
    s3_urls = data_search()
    process_tropo_object.expand(s3_url=s3_urls)


# Instantiate the DAG
job = tropo_job_dag()
