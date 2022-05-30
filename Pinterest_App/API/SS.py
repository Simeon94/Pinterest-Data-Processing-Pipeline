# import json
# import boto3

# s3_client = boto3.client('s3')

# dict1 ={
#             "emp1": {
#                 "name": "Lisa",
#                 "designation": "programmer",
#                 "age": "34",
#                 "salary": "54000"
#             },
#             "emp2": {
#                 "name": "Elis",
#                 "designation": "Trainee",
#                 "age": "24",
#                 "salary": "40000"
#             },
#         }
  
# # the json file where the output must be stored
# out_file = open("sample.json", "w")
  
# json.dump(dict1, out_file, indent = 6)
  
# out_file.close()
        
# s3_client.upload_file('./sample.json', 'simeon-streaming-bucket', 'out_file.json')

from datetime import datetime, timedelta
from textwrap import dedent
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
"""
Read instructions at the bottom of the file for usage
"""
with DAG(
    'spark',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )
    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )
    t1.doc_md = dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](https://www.google.com/url?sa=i&url=https%3A%2F%2Fwww.pngegg.com%2Fen%2Fsearch%3Fq%3Drandom&psig=AOvVaw1u3OiQw_B44b-dHcnoV7Xo&ust=1653689322081000&source=images&cd=vfe&ved=0CAwQjRxqFwoTCKiskPWW_vcCFQAAAAAdAAAAABAD)
    """
    )
    dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    t3 = BashOperator(
        task_id='spark_run',
        depends_on_past=False,
        # Change to full absolute path
        bash_command='python /Users/pissall/git/general-coding/Pinterest_App/Consumers/batch_consumer.py',
    )
    t1 >> [t2, t3]
"""
We need to copy this file to ~/airflow/dags (default location)
cp airflow_sample.py ~/airflow/dags/
Then we need to run the python file from there
python ~/airflow/dags/airflow_sample.py
Use following commands after setting the dag there
"""
"""
# initialize the database tables
airflow db init
# print the list of active DAGs
airflow dags list
# prints the list of tasks in the "tutorial" DAG
airflow tasks list spark
# prints the hierarchy of tasks in the "tutorial" DAG
airflow tasks list spark --tree
"""
"""
# command layout: command subcommand dag_id task_id date
# testing print_date
airflow tasks test spark print_date 2015-06-01
# testing sleep
airflow tasks test spark spark_run 2015-06-01
"""
