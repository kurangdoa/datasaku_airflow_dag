import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id = "test_bash_operator",
    start_date = pendulum.datetime(2023, 3, 1),
    # run every hour
    schedule = "0 0 * * *",
    # default arguments to be used in every task
    default_args = {"retries": 2},
    # make sure it does not run the backfill
    catchup = False,
):
    task1 = EmptyOperator(task_id="task1")
    task2 = EmptyOperator(task_id="task2")
    task3 = EmptyOperator(task_id="task3")
    op = BashOperator(task_id="dummy", bash_command='echo "hello world!"')
    test = BashOperator(task_id="print_secret", bash_command='echo "print{{ var.value.AWS_SECRET }}"')
    print(op.retries)  # 2

    task1 >> [task2, task3]
    task3 >> op >> test