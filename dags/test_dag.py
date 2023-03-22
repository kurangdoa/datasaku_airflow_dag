import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="my_dag",
    start_date=pendulum.datetime(2023, 1, 1),
    # run every hour
    schedule="0 * * * *",
    # default arguments to be used in every task
    default_args={"retries": 2},
):
    task1 = EmptyOperator(task_id="task1")
    task2 = EmptyOperator(task_id="task2")
    task3 = EmptyOperator(task_id="task3")
    op = BashOperator(task_id="dummy", bash_command="Hello World!")
    print(op.retries)  # 2

    task1 >> [task2, task3]
    task3 >> op