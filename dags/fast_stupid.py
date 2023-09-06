import random
import time

from airflow.decorators import dag
from airflow.operators.python import PythonOperator


@dag(
    dag_id="fast_stupid",
    schedule="manual",
)
def fast_stupid():
    width = 256
    height = 16

    def _sleep():
        time.sleep(random.randint(1, 4))

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
