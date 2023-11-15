import datetime
import time
from airflow.decorators import dag
from airflow.operators.python import PythonOperator


@dag(
    dag_id="fast_fail",
    start_date=datetime.datetime(2023, 9, 6),
    schedule="@once",
)
def fast_fail():
    width = 256
    height = 16

    def _sleep():
        time.sleep(30)

    for i in range(width):
        prev = None
        for j in range(height):
            op = PythonOperator(
                task_id=f"sleep_{i}_{j}",
                python_callable=_sleep,
            )
            if prev:
                prev >> op
            prev = op


fast_fail()
