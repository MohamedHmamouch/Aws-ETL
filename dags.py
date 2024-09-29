from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import boto3
import time
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor


def glue_job_s3_redshift_transfer(job_name, **kwargs):
    session = AwsBaseHook(aws_conn_id='aws_s3_conn', client_type='glue')

    client = session.get_client_type('glue', region_name='eu-west-1')

    client.start_job_run(
        JobName=job_name,
    )


def get_run_id(**kwargs):

    time.sleep(8)


    sessions = AwsBaseHook(aws_conn_id='aws_s3_conn')
    boto3_session = sessions.get_session(region_name='eu-west-1')
    glue_client = boto3_session.client('glue')

    response = glue_client.get_job_runs(JobName='S3_upload_to_redshift_glue_job')
    job_run_id = response['JobRuns'][0]['Id']

    return job_run_id


default_args = {
    "owner": 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 29),
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15)
}

with DAG(
    'glue_s3_to_redshift_dag',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False
) as dag:

 
    glue_job_trigger = PythonOperator(
        task_id='tsk_glue_job_trigger',
        python_callable=glue_job_s3_redshift_transfer,
        op_kwargs={
            'job_name': 'S3_upload_to_redshift_glue_job'
        },
    )


    grab_glue_job_run_id = PythonOperator(
        task_id='tsk_grab_glue_job_run_id',
        python_callable=get_run_id,
    )


    is_glue_job_finished_running = GlueJobSensor(
        task_id='tsk_is_glue_job_finished_running',
        job_name='S3_upload_to_redshift_glue_job',
        run_id='{{ task_instance.xcom_pull(task_ids="tsk_grab_glue_job_run_id") }}',
        verbose=False,
        aws_conn_id='aws_s3_conn',
        poke_interval=60,
        timeout=3600,
    )


    glue_job_trigger >> grab_glue_job_run_id >> is_glue_job_finished_running
