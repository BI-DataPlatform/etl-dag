from datetime import datetime
import pytz

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from airflow.providers.apache.livy.operators.livy import LivyOperator


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
        kubectl exec spark-livy-0 -- mkdir -p {LIVY_FILES_DIR}
        kubectl cp /home/malachai/airflow/dags/spark spark-livy-0:{LIVY_FILES_DIR}
        """
        copy_files_task = BashOperator(
            task_id="copy_files",
            bash_command=copy_files_bash_script
        )
        copy_files_task.execute(context)



    @task()
    def update_accounts_dim(**context):
        """
        temp Iceberg 로그 테이블에서 로그를 추출해 partitioning, indexing, audit 등을 추가
        실패 후 재실행 시 해당 날짜 partition overwrite 진행
        """
        update_dim_task = LivyOperator(
            task_id="prepare_accounts_pipeline",
            name="prepare_accounts_pipeline",
            livy_conn_id="livy_default",
            file=f"{LIVY_FILES_DIR}/spark/prepare_pipeline.py",
            py_files=None,
            args=[
                "--src-table=deliveries.accounts_log",
                "--dst-table=deliveries.accounts_stage",
                f"--partition={context['ds_nodash']}",
                "--partition-col=ds",
                "--timestamp-col=inserted_at",
                f"--from={int(round(datetime(2024, 6, 3, 0, tzinfo=pytz.timezone('Asia/Seoul')).timestamp()))}",
                f"--to={int(round(datetime(2024, 6, 4, 0, tzinfo=pytz.timezone('Asia/Seoul')).timestamp()))}",
                "--surrogate-col=row_id"
            ],
            conf={},
            driver_memory="512m",
            driver_cores=1,
            executor_memory="512m",
            executor_cores=1,
            num_executors=1,
            polling_interval=5
        )
        try:
            batch_id = update_dim_task.execute(context)
        except Exception as e:
            # TODO: error handling
            print(e)
        finally:
            update_dim_task.kill()



    copy_files() >> update_accounts_dim()

etl_dag()
