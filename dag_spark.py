from airflow.models import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'Dan',
    'start_date': datetime(2023, 2, 9),
    'retry_delay': timedelta(minutes=5)
}

with DAG(dag_id='spark_dag',
         default_args=default_args,
         schedule_interval='0 12 * * *',
         catchup=False,
         tags=['spark']
         ) as dag:
         
    # Define the tasks. Here we are going to define only one bash operator
    spark_task = BashOperator(
        task_id='run_spark_processing',
        bash_command='cd /home/danbouchard/Documents/AI_Core/pinterest-data-processing-pipeline && python spark_batch_processing.py',
        dag=dag)
