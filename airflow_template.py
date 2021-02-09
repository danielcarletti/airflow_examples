from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import yaml
import importlib
import os

with open(os.path.join(os.path.dirname(__file__), "config.yaml"), 'r') as yaml_stream:
    try:
        config = yaml.safe_load(yaml_stream)
    except yaml.YAMLError as exc:
        print(exc)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
for job, configs in config.items():
    dag = DAG(
        job,
        default_args=default_args,
        description='A standard description',
        schedule_interval=configs["schedule"],
        start_date=days_ago(100)
    )
    
    operators = []
    if configs["type"] == "spark": # add spark job
        for file in configs["files"]:
            operator = BashOperator(
                task_id='my_bash_operator',
                bash_command=f"spark-submit {file}",
                dag=dag,
            )
            operators.append(operator)
            
    elif configs["type"] == "python": # add python job
        for file in configs["files"]:
            module = file.split(".")[0]
            function = file.split(".")[1]
            def python_function():
                imported_module = importlib.import_module(module)
                function_object = getattr(imported_module, function)
                function_object()
            
            operator = PythonOperator(
                task_id='my_python_operator',
                python_callable=python_function,
                dag=dag,
            )
            operators.append(operator)
    elif configs["type"] == "bash": # add bash job
        for file in configs["files"]:
            operator = BashOperator(
                task_id='my_bash_operator',
                bash_command=f"{file} ",
                dag=dag,
            )
            operators.append(operator)
    else:
        raise Exception("Need to configure this new operator")
    
    # important: add your DAG to globals
    globals()[job] = dag
