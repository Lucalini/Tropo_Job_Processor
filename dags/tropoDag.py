from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from datetime import datetime, timedelta
import time
import logging
import yaml
import os
import sys
import uuid

dag_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(dag_dir)

from util import get_tropo_objects
import boto3
from kubernetes.client import V1Pod, V1PodSpec, V1Container, models as k8s
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
    product_version = kwargs.get('product_version', '1.0')
    
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
    params = {
        "bucket": Param(None, type=str), 
        "date": Param(None, type=str),
        "start_datetime": Param(None, type= str),
        "end_datetime": Param(None, type=str) ,
        "prefix": Param(None, type= str),
        "forward_mode_age": Param(None, type = str)
    }
)
def tropo_job_dag():
    
    @task
    def data_search(**context):
        params = context["params"]
        response = get_tropo_objects(
            params["bucket"], 
            params["date"], 
            params["start_datetime"], 
            params["end_datetime"], 
            params["prefix"], 
            params["forward_mode_age"]
            )   
        logging.info(f"{response}")
        return [response[0]]

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
                output_path= f"/opt/airflow/storage/runconfigs/{s3_uri.split('/')[-1].split('.')[0]}",
                input_file=  f"/workdir/input/{s3_uri.split('/')[-1]}",
                output_dir="/workdir/output/",
                scratch_dir="/workdir/scratch",
                n_workers=4,
                product_version="1.0"
            )
            bucket_name = "opera-dev-cc-verweyen"
            bucket = s3.Bucket(bucket_name)
            s3_config_uri = f"tropo/runconfigs/{s3_uri.split('/')[-1].split('.')[0]}runconfig.yaml"
            bucket.upload_file(local_config_path, s3_config_uri)
            #Return config uri, tropo object uri and the filepath to where both will be downloaded to in our tropo PGE
            input_path = f"/workdir/input/{s3_uri.split('/')[-1]}"
            return {
            'tropo_uri': s3_uri,
            'config_uri': s3_config_uri
            }
        
        @task
        def run_tropo_pge(tropo_uri, config_uri, **context):
            job_id = str(uuid.uuid4()).replace('-', '')[:8].lower()  # Remove hyphens and ensure lowercase
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")       
            # Environment variables for the main container and init containers
            env_vars = {
                "UID": "1000", 
                "CONFIG_PATH": f"/workdir/config/runconfig.yaml",
                "INPUT_DATA_PATH": "/workdir/input1/data.nc",
                "OUTPUT_PATH": "/workdir/output/",
                "S3_OUTPUT_BUCKET": "opera-dev-cc-verweyen",
                "JOB_ID": job_id,
                "TROPO_OBJECT": tropo_uri,
                "RUN_CONFIG": config_uri
            }

            # Convert dict to k8s env list for V1Container.env
            init_env = [k8s.V1EnvVar(name=k, value=v) for k, v in env_vars.items()]

            # Shared volume for data exchange between containers
            shared_volume = k8s.V1Volume(
                name="workdir",
                empty_dir=k8s.V1EmptyDirVolumeSource()
            )

            shared_mount = k8s.V1VolumeMount(
                name="workdir",
                mount_path="/workdir"
            )

            main_container = V1Container(
                name="tropo-pge",
                image="artifactory-fn.jpl.nasa.gov:16001/gov/nasa/jpl/opera/sds/pge/opera_pge/tropo:3.0.0-rc.1.0-tropo",
                args=["-f", "/workdir/config/runconfig.yaml"],
                volume_mounts=[shared_mount],
                env=init_env,
                resources=k8s.V1ResourceRequirements(
                    requests={
                        "cpu": "12000m",     # 12 CPU cores (75% of 16)
                        "memory": "48Gi"     # 48GB RAM (75% of 64GB)
                    },
                    limits={
                        "cpu": "15000m",     # Max 15 CPU cores (leave some headroom)
                        "memory": "60Gi"     # Max 60GB RAM (leave some headroom)
                    }
                )
            )

            sidecar_container = V1Container(
                name="s3-upload-sidecar",
                image="amazon/aws-cli:2.17.52",
                command=["sh", "-c"],
                args=[
                    "echo 'Starting S3 sidecar, waiting for output files...'; "
                    "while true; do "
                    "  if [ -d /workdir/output ] && [ \"$(ls -A /workdir/output 2>/dev/null)\" ]; then "
                    "    echo 'Found output files! Starting 2-minute sync period...'; "
                    "    END_TIME=$(($(date +%s) + 120)); "  # 2 minutes from now
                    "    while [ $(date +%s) -lt $END_TIME ]; do "
                    "      echo 'Syncing to S3...'; "
                    "      aws s3 sync /workdir/output s3://$S3_OUTPUT_BUCKET/tropo/outputs/$JOB_ID/ --exclude 'scratch/*'; "
                    "      sleep 10; "
                    "    done; "
                    "    echo 'Final sync and exit'; "
                    "    aws s3 sync /workdir/output s3://$S3_OUTPUT_BUCKET/tropo/outputs/$JOB_ID/ --exclude 'scratch/*'; "
                    "    exit 0; "
                    "  fi; "
                    "  sleep 5; "
                    "done"
                ],
                env=init_env,
                volume_mounts=[shared_mount]
            )
            
            pod_spec = V1PodSpec(
                restart_policy="Never",
                init_containers=[
                    k8s.V1Container(
                        name="download-tropo-data",
                        image="amazon/aws-cli:2.17.52",
                        command=["/bin/sh", "-c"],
                        args=[
                            "set -e; "
                            "mkdir -p /workdir/input; "
                            "F=$(basename \"$TROPO_OBJECT\"); "
                            "aws s3 cp \"s3://opera-ecmwf/$TROPO_OBJECT\" \"/workdir/input/$F\"; "
                            "echo \"Downloaded $F to /workdir/input/\""
                        ],
                        volume_mounts=[shared_mount],
                        env=init_env
                    ),
                    k8s.V1Container(
                        name="download-runconfig",
                        image="amazon/aws-cli:2.17.52", 
                        command=["/bin/sh", "-c"],
                        args=[
                            "set -e; "
                            "mkdir -p /workdir/config; "
                            "aws s3 cp \"s3://$S3_OUTPUT_BUCKET/tropo/runconfigs/$RUN_CONFIG\" '/workdir/config/runconfig.yaml'; "
                            "echo 'Downloaded runconfig to /workdir/config/runconfig.yaml'"
                        ],
                        volume_mounts=[shared_mount],
                        env=init_env
                    )
                ],
                image_pull_secrets=[k8s.V1LocalObjectReference(name="artifactory-creds")],
                containers=[main_container, sidecar_container],
                volumes=[shared_volume],
                service_account_name="airflow-worker"
            )
                    
            operator =  KubernetesPodOperator(
                task_id=f"run_tropo_pge_kubernetes_{job_id}",
                namespace="opera-dev",
                name=f"tropo-pge-{job_id}", 
                in_cluster=True,
                kubernetes_conn_id=None,
                config_file=None,
                startup_timeout_seconds=600,
                full_pod_spec=V1Pod(
                    metadata=k8s.V1ObjectMeta(name=f"tropo-pge-{job_id}"),
                    spec=pod_spec
                ),
                get_logs=True,
                is_delete_operator_pod=True
            )

            result = operator.execute(context)

            return {
                "job_id": job_id,
                "result": result
            }
           

        @task 
        def post_processing():
            logging.info("PostProcessing job")
            time.sleep(10)
            return "Postprocessed job"
            
        preprocessing_result = job_preprocessing(s3_uri=s3_uri)
        pge_run = run_tropo_pge(preprocessing_result['tropo_uri'], preprocessing_result['config_uri'])
        post_processing_result = post_processing()

        # Set up task dependencies
        preprocessing_result >> pge_run >> post_processing_result
        
        return pge_run  # Return reference to the Kubernetes operator
    
    s3_uris = data_search()
    process_tropo_object.expand(s3_uri=s3_uris)

# Instantiate the DAG
job = tropo_job_dag()

