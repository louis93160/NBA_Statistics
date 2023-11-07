import os

from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

# $IMPORT_BEGIN
# noreorder
import json


# $IMPORT_END

DBT_DIR = os.getenv("DBT_DIR")


def load_manifest(file: str) -> dict:
    """
    Reads the json `file` and returns it as a dict.
    """
    with open(file) as f:
        data = json.load(f)
    return data


def make_dbt_task(node: str, dbt_verb: str) -> BashOperator:
    """
    Returns a BashOperator with a bash command to run or test the given node.
    Adds the project-dir argument and names the tasks as shown by the below examples.
    Cleans the node's name when it is a test.

    Examples:
    >>> print(make_dbt_task('model.dbt_lewagon.my_first_dbt_model', 'run'))
    BashOperator(
        task_id=model.dbt_lewagon.my_first_dbt_model,
        bash_command= "dbt run --models my_first_dbt_model --project-dir /app/airflow/dbt_lewagon"
    )

    >>> print(make_dbt_task('test.dbt_lewagon.not_null_my_first_dbt_model_id.5fb22c2710', 'test'))
    BashOperator(
        task_id=test.dbt_lewagon.not_null_my_first_dbt_model_id,
        bash_command= "dbt test --models not_null_my_first_dbt_model_id --project-dir /app/airflow/dbt_lewagon"
    )
    """
    # $CHA_BEGIN
    if dbt_verb == "run":
        model = node.split(".")[-1]
        task_id = node
    elif dbt_verb == "test":
        model = node.split(".")[-2]
        task_id = ".".join(node.split(".")[:-1])

    return BashOperator(
        task_id=task_id,
        bash_command=(f"dbt {dbt_verb} --models {model} --project-dir {DBT_DIR}"),
    )
    # $CHA_END


def create_tasks(data: dict) -> dict:
    """
    This function should iterate through data["nodes"] keys and call make_dbt_task
    to build and return a new dict containing as keys all nodes' names and their corresponding dbt tasks as values.
    """
    # $CHA_BEGIN
    dbt_tasks = {}
    for node in data["nodes"].keys():
        if node.split(".")[0] == "model":
            dbt_tasks[node] = make_dbt_task(node, "run")
        elif node.split(".")[0] == "test":
            dbt_tasks[node] = make_dbt_task(node, "test")
    return dbt_tasks
    # $CHA_END


def create_dags_dependencies(data: dict, dbt_tasks: dict):
    """
    Iterate over every node and their dependencies (by using data and the "depends_on" key)
    to order the Airflow tasks properly.
    """
    # $CHA_BEGIN
    for node in data["nodes"].keys():
        if node.split(".")[0] in ("model", "test"):
            for upstream_node in data["nodes"][node]["depends_on"]["nodes"]:
                upstream_node_type = upstream_node.split(".")[0]
                if upstream_node_type in ("model", "test"):
                    dbt_tasks[upstream_node] >> dbt_tasks[node]
    # $CHA_END


with DAG(
    "dbt_advanced",
    default_args={"depends_on_past": False,},
    start_date=pendulum.today("UTC").add(days=-1),
    schedule_interval="@daily",
    catchup=True,
) as dag:

    with open(f"{DBT_DIR}/manifest.json") as f:
        data = json.load(f)
    dbt_tasks = create_tasks(data)
    create_dags_dependencies(data, dbt_tasks)
