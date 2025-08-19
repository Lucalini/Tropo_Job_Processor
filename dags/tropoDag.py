from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta
import time
import logging
import yaml
import os
import docker
import sys
import uuid

dag_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(dag_dir)

from util import get_tropo_objects
import boto3
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator



bucket_name = ''
container_name = "Temp"

def create_modified_runconfig(template_path, output_path, **kwargs):
    """
    Create a modified run configuration based on a template.
    
    Args:
        template_path: Path to the template run configuration file
        output_path: Path where the modified configuration will be saved
        **kwargs: Key-value pairs for configuration modifications
    """
    with open(template_path, 'r') as file:
        config = yaml.safe_load(file)
    
    # Extract common parameters from kwargs
    input_file = kwargs.get('input_file')
    output_dir = kwargs.get('output_dir')
    scratch_dir = kwargs.get('scratch_dir')
    n_workers = kwargs.get('n_workers', 4)
    product_version = kwargs.get('product_version', '0.2')
    
    # Modify paths based on parameters
    config['RunConfig']['Groups']['PGE']['InputFilesGroup']['InputFilePaths'] = [input_file]
    config['RunConfig']['Groups']['SAS']['input_file']['input_file_path'] = input_file
    config['RunConfig']['Groups']['PGE']['ProductPathGroup']['OutputProductPath'] = output_dir
    config['RunConfig']['Groups']['PGE']['ProductPathGroup']['ScratchPath'] = scratch_dir
    config['RunConfig']['Groups']['SAS']['product_path_group']['product_path'] = output_dir
    config['RunConfig']['Groups']['SAS']['product_path_group']['scratch_path'] = scratch_dir
    config['RunConfig']['Groups']['SAS']['product_path_group']['sas_output_path'] = output_dir
    config['RunConfig']['Groups']['SAS']['worker_settings']['n_workers'] = n_workers
    config['RunConfig']['Groups']['PGE']['PrimaryExecutable']['ProductVersion'] = str(product_version)
    config['RunConfig']['Groups']['SAS']['product_path_group']['product_version'] = str(product_version)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(f"{output_path}runconfig.yaml", 'w') as file:
        yaml.dump(config, file, default_flow_style=False, sort_keys=False, indent=2)
    runconfig_output = f"{output_path}runconfig.yaml"
    return runconfig_output

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


@dag(
    dag_id='tropo_PGE',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['Luca'],
)
def tropo_job_dag():
    
    @task
    def data_search():
        #temporarily hardcoded data search 
        bucket_name = "opera-ecmwf"
        response = get_tropo_objects(bucket_name, date="2024-12-31")   
        logging.info(f"{response}")
        return response

    @task_group(group_id="tropo_job_group")
    def process_tropo_object(s3_uri):

        @task
        def job_preprocessing(s3_uri):

            #We need to upload the outputted file existing at output path to s3 
            #We output config path URI and Tropo Object URI
            s3 = boto3.resource("s3")
            logging.info(f"Generating runconfig for job {s3_uri}")

            DAG_DIR = os.path.dirname(__file__)
            template_file = os.path.join(DAG_DIR, "tropo_sample_runconfig-v3.0.0-er.3.1.yaml")
            local_config_path = create_modified_runconfig(
                template_path=template_file,
                output_path= f"/opt/airflow/storage/runconfigs/{s3_uri}",
                input_file=  f"/workdir/input/{s3_uri.split('/')[-1]}",
                output_dir="/workdir/output/",
                scratch_dir="/workdir/output/scratch",
                n_workers=4,
                product_version="0.3"
            )
            bucket_name = "opera-dev-cc-verweyen"
            bucket = s3.Bucket(bucket_name)
            s3_config_uri = f"tropo/runconfigs/{s3_uri.split('/')[-1]}"
            bucket.upload_file(local_config_path, s3_config_uri)
            #Return config uri, tropo object uri and the filepath to where both will be downloaded to in our tropo PGE
            return s3_config_uri

        job_id = str(uuid.uuid4()).replace('-', '')[:8].lower()  # Remove hyphens and ensure lowercase
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        
        # Environment variables for the main container
        env_vars = {
            "UID": "1000",  # Use fixed UID for container compatibility
            "CONFIG_PATH": f"/workdir/config/runconfig.yaml",
            "INPUT_DATA_PATH": "/workdir/input1/data.nc",
            "OUTPUT_PATH": "/workdir/output/",
            "S3_OUTPUT_BUCKET": "opera-dev-cc-verweyen",
            "JOB_ID": job_id
        }

        # Shared volume for data exchange between containers
        shared_volume = k8s.V1Volume(
            name="workdir",
            empty_dir=k8s.V1EmptyDirVolumeSource()
        )

        shared_mount = k8s.V1VolumeMount(
            name="workdir",
            mount_path="/workdir"
        )
                
        run_tropo_pge_k8s = KubernetesPodOperator(
            task_id="run_tropo_pge_kubernetes",
            namespace="opera-dev",
            name=f"tropo-pge-{job_id}",  # job_id is already lowercase and DNS-compliant
            image="opera_pge/tropo:3.0.0-er.3.1-tropo",
            in_cluster=True,
            config_file=None,
            
            # Simple command with -f runconfig flag
            cmds=["/bin/bash", "-c"],
            arguments=[
                "set -e && " +
                "echo 'Starting tropo PGE processing...' && " +
                "/usr/local/bin/tropo_pge_entrypoint.sh -f /workdir/config/runconfig.yaml && " +
                "echo 'Processing complete, uploading to S3...' && " +
                f"aws s3 cp /workdir/output/ 's3://opera-dev-cc-verweyen/tropo_outputs/{timestamp}_{job_id}/' --recursive --exclude '*' --include '*.nc' --include '*.h5' && " +
                f"echo 'Upload complete. Results available at: s3://opera-dev-cc-verweyen/tropo_outputs/{timestamp}_{job_id}/'"
            ],
            
            env_vars=env_vars,
            get_logs=True,
            is_delete_operator_pod=True,
            
            # CRITICAL: This service account must have IRSA annotation
            service_account_name="opera-pge-worker",  # Dedicated service account for PGE pods

            # Init containers for dual S3 downloads
            init_containers=[
                # Download from first bucket (tropo data)
                k8s.V1Container(
                    name="download-tropo-data",
                    image="amazon/aws-cli:2",
                    command=["/bin/sh", "-c"],
                    args=[
                        "set -e && "
                        "mkdir -p /workdir/input && "
                        f"aws s3 cp 's3://opera-ecmwf/{s3_uri}' '/workdir/input/{s3_uri.split('/')[-1]}' && "
                        f"echo 'Downloaded tropo object {s3_uri} to /workdir/input/'"
                    ],
                    volume_mounts=[shared_mount]
                ),
                
                # Download from second bucket (config data) 
                k8s.V1Container(
                    name="download-runconfig",
                    image="amazon/aws-cli:2", 
                    command=["/bin/sh", "-c"],
                    args=[
                        "set -e && "
                        "mkdir -p /workdir/config && "
                        f"aws s3 cp 's3://opera-dev-cc-verweyen/{preprocessing_result}' '/workdir/config/runconfig.yaml' && "
                        "echo 'Downloaded runconfig to /workdir/config/runconfig.yaml'"
                    ],
                    volume_mounts=[shared_mount]
                )
            ],

            volumes=[shared_volume],
            volume_mounts=[shared_mount]
        )
           

        @task 
        def post_processing():
            logging.info("PostProcessing job")
            time.sleep(10)
            return "Postprocessed job"
            
        preprocessing_result = job_preprocessing(s3_uri=s3_uri)
        post_processing_result = post_processing()

        # Set up task dependencies
        preprocessing_result >> run_tropo_pge_k8s >> post_processing_result
        
        return run_tropo_pge_k8s  # Return reference to the Kubernetes operator
    
    s3_uris = data_search()
    process_tropo_object.expand(s3_uri=s3_uris)

# Instantiate the DAG
job = tropo_job_dag()
