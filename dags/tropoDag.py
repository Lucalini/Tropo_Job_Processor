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
        "bucket": Param(None, type="string"), 
        "date": Param(None, type="string"),
        "start_datetime": Param(None, type= "string"),
        "end_datetime": Param(None, type="string") ,
        "prefix": Param(None, type="string"),
        "forward_mode_age": Param(None, type="string")
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
            template_file = os.path.join(DAG_DIR, "resources", "tropo_sample_runconfig-v3.0.0-er.3.1.yaml")
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
        
        # Define KubernetesPodOperator as a real task (templated with XCom from preprocessing)
        # Shared volume and mounts
        shared_volume = k8s.V1Volume(
            name="workdir",
            empty_dir=k8s.V1EmptyDirVolumeSource()
        )

        shared_mount = k8s.V1VolumeMount(
            name="workdir",
            mount_path="/workdir"
        )

        logging.info("{{ ti.xcom_pull(task_ids='tropo_job_group.job_preprocessing')['tropo_uri'] }}")

        # Environment variables for containers
        env_vars = [
            k8s.V1EnvVar(name="UID", value="1000"),
            k8s.V1EnvVar(name="CONFIG_PATH", value="/workdir/config/runconfig.yaml"),
            k8s.V1EnvVar(name="OUTPUT_PATH", value="/workdir/output/"),
            k8s.V1EnvVar(name="S3_OUTPUT_BUCKET", value="opera-dev-cc-verweyen"),
            k8s.V1EnvVar(name="JOB_ID", value="{{ ts_nodash }}-{{ ti.map_index }}"),
            k8s.V1EnvVar(name="TROPO_OBJECT", value="{{ ti.xcom_pull(task_ids='tropo_job_group.job_preprocessing')['tropo_uri'] }}"),
            k8s.V1EnvVar(name="RUN_CONFIG", value="{{ ti.xcom_pull(task_ids='tropo_job_group.job_preprocessing')['config_uri'] }}"),
        ]

        main_container = V1Container(
            name="tropo-pge",
            image="artifactory-fn.jpl.nasa.gov:16001/gov/nasa/jpl/opera/sds/pge/opera_pge/tropo:3.0.0-rc.1.0-tropo",
            args=["-f", "/workdir/config/runconfig.yaml"],
            volume_mounts=[shared_mount],
            env=env_vars,
            resources=k8s.V1ResourceRequirements(
                requests={
                    "cpu": "12000m",
                    "memory": "48Gi"
                },
                limits={
                    "cpu": "15000m",
                    "memory": "60Gi"
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
                "    END_TIME=$(($(date +%s) + 120)); "
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
            env=env_vars,
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
                        "set -e; ",
                        "mkdir -p /workdir/input; ",
                        "F=$(basename \"$TROPO_OBJECT\"); ",
                        "aws s3 cp \"s3://opera-ecmwf/$TROPO_OBJECT\" \"/workdir/input/$F\"; ",
                        "echo \"Downloaded $F to /workdir/input/\""
                    ],
                    volume_mounts=[shared_mount],
                    env=env_vars
                ),
                k8s.V1Container(
                    name="download-runconfig",
                    image="amazon/aws-cli:2.17.52",
                    command=["/bin/sh", "-c"],
                    args=[
                        "set -e; ",
                        "mkdir -p /workdir/config; ",
                        "aws s3 cp \"s3://$S3_OUTPUT_BUCKET/$RUN_CONFIG\" '/workdir/config/runconfig.yaml'; ",
                        "echo 'Downloaded runconfig to /workdir/config/runconfig.yaml'"
                    ],
                    volume_mounts=[shared_mount],
                    env=env_vars
                )
            ],
            image_pull_secrets=[k8s.V1LocalObjectReference(name="artifactory-creds")],
            containers=[main_container, sidecar_container],
            volumes=[shared_volume],
            service_account_name="airflow-worker"
        )

        kpo = KubernetesPodOperator(
            task_id="run_tropo_pge",
            namespace="opera-dev",
            name="tropo-pge-{{ ts_nodash }}-{{ ti.map_index }}",
            in_cluster=True,
            kubernetes_conn_id=None,
            config_file=None,
            startup_timeout_seconds=600,
            full_pod_spec=V1Pod(spec=pod_spec),
            get_logs=True,
            is_delete_operator_pod=False
        )
           

        @task 
        def post_processing():
            logging.info("PostProcessing job")
            time.sleep(10)
            return "Postprocessed job"
            
        preprocessing_result = job_preprocessing(s3_uri=s3_uri)
        post_processing_result = post_processing()

        # Set up task dependencies
        preprocessing_result >> kpo >> post_processing_result

        return kpo
    
    s3_uris = data_search()
    process_tropo_object.expand(s3_uri=s3_uris)

# Instantiate the DAG
job = tropo_job_dag()

