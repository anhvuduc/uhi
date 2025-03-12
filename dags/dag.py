from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'task_01',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# Task 1: Export (Extract)
# The extract.py script should print only the number of images as its final output.
# extract_task = BashOperator(
#     task_id='extract_task',
#     bash_command='python /opt/airflow/tasks/extract.py',
#     do_xcom_push=True,  # Capture stdout and push it to XCom
#     dag=dag
# )

# # Task 2: Wait for Exports (Pre-download)
# # This script will be passed the expected image count (num_images) via a command-line argument.
# pre_download_task = BashOperator(
#     task_id='pre_download_task',
#     bash_command='python /opt/airflow/tasks/pre_download.py {{ ti.xcom_pull(task_ids="extract_task") }}',
#     dag=dag
# )

# Task 3: Download
# The download.py script is invoked with the num_images value as an argument.
download_task = BashOperator(
    task_id='download_task',
    bash_command='python /opt/airflow/tasks/download.py',
    dag=dag
)

# crop_task = BashOperator(
#     task_id='crop_task',
#     bash_command='python /opt/airflow/tasks/crop.py',
#     dag=dag
# )

# check_quality = BashOperator(
#     task_id='quality_task',
#     bash_command='python /opt/airflow/tasks/quality.py',
#     dag=dag
# )

# mean_task = BashOperator(
#     task_id='mean_task',
#     bash_command='python /opt/airflow/tasks/mean.py',
#     dag=dag
# )

# block_task = BashOperator(
#     task_id='block_task',
#     bash_command='python /opt/airflow/tasks/block.py',
#     dag=dag
# )

# mean_ee_task = BashOperator(
#     task_id='mean_ee_task',
#     bash_command='python /opt/airflow/tasks/mean_ee.py',
#     dag=dag
# )

# # mk_task = BashOperator(
# #     task_id='mk_task',
# #     bash_command='python /opt/airflow/tasks/mk.py',
# #     dag=dag
# # )
download_task
# # Define task dependencies: extract -> wait -> download
# extract_task >> download_task >> crop_task >> check_quality
# crop_task >> mean_task >> block_task #>> mk_task
# crop_task >> mean_ee_task