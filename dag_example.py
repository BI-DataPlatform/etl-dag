import pymysql
from airflow.decorators import dag, task
from pendulum import datetime

def create_dag(dag_id, schedule, dag_number, default_args):
    @dag(dag_id=dag_id, schedule=schedule, default_args=default_args, catchup=False)
    def hello_world_dag():

        @task()
        def hello_world(precede):
            print("Hello World")

        @task()
        def print_some(some, precede):
            print("This is DAG: {}".format(str(some)))

        task_q = []
        ret_q = []
        ret = None
        if dag_json["task_type"] == "hello":
            ret = hello_world(precede=ret)
        elif dag_json["task_type"] == "print":
            ret = print_some(dag_json["task_param"], precede=ret)

        for i in range(len(dag_json["children"])):
            task_q.append(dag_json["children"][i])
            ret_q.append(ret)
        while len(task_q) > 0:
            child = task_q.pop()
            ret = ret_q.pop()
            if child["task_type"] == "hello":
                ret = hello_world(precede=ret)
            elif child["task_type"] == "print":
                ret = print_some(child["task_param"], precede=ret)
            for i in range(len(child["children"])):
                task_q.append(child["children"][i])
                ret_q.append(ret)



    generated_dag = hello_world_dag()

    return generated_dag



dag_json = {
    "task_name": "first",
    "task_type": "hello",
    "children": [
        {
            "task_name": "second_parallel_1",
            "task_type": "print",
            "task_param": "second_param",
            "children": [
                {
                    "task_name": "third_1",
                    "task_type": "print",
                    "task_param": "third_param",
                    "children": []
                }
            ]
        },
        {
            "task_name": "second_parallel_2",
            "task_type": "hello",
            "children": [
                {
                    "task_name": "third_2",
                    "task_type": "print",
                    "task_param": "third_param",
                    "children": [
                        {
                            "task_name": "fourth_1",
                            "task_type": "hello",
                            "children": []
                        },
                        {
                            "task_name": "fourth_2",
                            "task_type": "hello",
                            "children": []
                        }
                    ]
                }
            ]
        }
    ]
}

dag_id = "dag_loop_hello_world"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 25),
    "hello_first": False
}

schedule = "@daily"

host = "localhost"
port = 3306
database = "crawler"
username = "spring"
password = "spring"

conn = pymysql.connect(host=host, user=username, password=password, database=database, port=port, use_unicode=True, charset='utf8')
cursor = conn.cursor()

query = "SELECT * FROM airflow_dag_test"
cursor.execute(query)

result = cursor.fetchall()
conn.close()

for dag_id in result:
    dag_id = str(dag_id[0])
    create_dag(dag_id, schedule, dag_json, default_args)
