from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from airflow.providers.apache.livy.operators.livy import LivyOperator, LivyTrigger


@dag(dag_id="etltool",
     schedule="@daily",
     default_args={
         "owner": "airflow",
         "start_date": datetime(2024, 1, 25),
         "hello_first": False
     },
     catchup=False
     )
def etl_dag():

    LIVY_FILES_DIR = "/root/.livy-sessions/dags"



    @task
    def copy_files(**context):
        copy_files_bash_script = f"""
        mkdir -p {LIVY_FILES_DIR}
        kubectl cp /home/malachai/airflow/dags/spark spark-livy-0:{LIVY_FILES_DIR}
        """
        copy_files_task = BashOperator(
            task_id="copy_files",
            bash_command=copy_files_bash_script
        )
        copy_files_task.execute(context)

    @task()
    def update_accounts_dim(**context):
        update_dim_task = LivyOperator(
            task_id="update_accounts_dim",
            name="update_accounts_dim",
            livy_conn_id="livy_default",
            file=f"{LIVY_FILES_DIR}/spark/update_accounts_dim.py",
            conf={},
            py_files=None,
            driver_memory="2G",
            driver_cores=1,
            executor_memory="1G",
            executor_cores=1,
            num_executors=2,
            polling_interval=5
        )
        batch_id = update_dim_task.execute(context)
        # delete batch session when finished
        update_dim_task.kill()


    copy_files() >> update_accounts_dim()

etl_dag()
