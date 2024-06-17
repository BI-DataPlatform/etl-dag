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
        kubectl cp /home/malachai/airflow/dags/spark/spark-python spark-livy-0:{LIVY_FILES_DIR}/pyspark
        kubectl cp /home/malachai/airflow/dags/spark/spark-scala/target/scala-2.12/etltool-assembly-0.1.jar spark-livy-0:{LIVY_FILES_DIR}/spark/etltool-assembly-0.1.jar
        """
        copy_files_task = BashOperator(
            task_id="copy_files",
            bash_command=copy_files_bash_script
        )
        copy_files_task.execute(context)

    @task()
    def accounts_historic_load(**context):
        """
        temp Iceberg 로그 테이블에서 로그를 추출해 partitioning, indexing, audit 등을 추가
        실패 후 재실행 시 해당 날짜 partition overwrite 진행
        """
        accounts_historic_load_task = LivyOperator(
            task_id="accounts_historic_load_process",
            name="accounts_historic_load_process",
            livy_conn_id="livy_default",
            class_name="io.malachai.etltool.Main",
            file=f"{LIVY_FILES_DIR}/spark/etltool-assembly-0.1.jar",
            py_files=None,
            args=[
                "historic-load",
                "--src-table", "deliveries.accounts_log",
                "--dst-table", "deliveries.accounts_stage",
                "--partition", f"{context['ds_nodash']}",
                "--partition-col", "ds",
                "--timestamp-col", "inserted_at",
                "--from", f"{int(round(datetime(2024, 6, 3, 0, tzinfo=pytz.timezone('Asia/Seoul')).timestamp()))}",
                "--to", f"{int(round(datetime(2024, 6, 4, 0, tzinfo=pytz.timezone('Asia/Seoul')).timestamp()))}",
                "--surrogate-col", "row_id"
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
            batch_id = accounts_historic_load_task.execute(context)
        except Exception as e:
            # TODO: error handling
            print(e)
        finally:
            accounts_historic_load_task.kill()

    @task()
    def accounts_dimension_incremental_process(**context):
        """
        temp Iceberg 로그 테이블에서 로그를 추출해 partitioning, indexing, audit 등을 추가
        실패 후 재실행 시 해당 날짜 partition overwrite 진행
        """
        accounts_dimension_incremental_process_task = LivyOperator(
            task_id="accounts_dimension_incremental_process",
            name="accounts_dimension_incremental_process",
            livy_conn_id="livy_default",
            class_name="io.malachai.etltool.Main",
            file=f"{LIVY_FILES_DIR}/spark/etltool-assembly-0.1.jar",
            py_files=None,
            args=[
                "dimension-incremental"
                "--src-table", "deliveries.accounts_stage",
                "--dst-table", "deliveries.accounts_dimension"
                "--partition", f"{context['ds_nodash']}",
                "--partition-col", "ds",
                "--row-kind-col", "rowkind",
                "--start", "",
                "--end", ""
                "--index-col", "row_id",
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
            batch_id = accounts_dimension_incremental_process_task.execute(context)
        except Exception as e:
            # TODO: error handling
            print(e)
        finally:
            accounts_dimension_incremental_process_task.kill()


    copy_files() >> accounts_historic_load() >> accounts_dimension_incremental_process()


etl_dag()
