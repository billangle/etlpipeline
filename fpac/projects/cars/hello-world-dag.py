from airflow import DAG 
from airflow.operators.python import PythonOperator 
from datetime import datetime 

  

def say_hello(): 

    print("Hello from Airflow!") 

  

# DAG definition 

with DAG( 

    dag_id="hello_world_dag", 
    start_date=datetime(2024, 1, 1), 
    schedule_interval="@daily", 
    catchup=False, 
    description="A simple Airflow DAG example", 

) as dag: 

  

    hello_task = PythonOperator( 
        task_id="hello_task", 
        python_callable=say_hello, 

    ) 

  

    hello_task 