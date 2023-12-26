from __future__ import annotations


import datetime

from pathlib import Path


from airflow import DAG

from airflow.decorators import task

from airflow.models.dagrun import DagRun

from airflow.models.param import Param

from airflow.models.taskinstance import TaskInstance

from airflow.utils.trigger_rule import TriggerRule


with DAG(
    dag_id=Path(__file__).stem,
    description="Bla",
    doc_md=__doc__,
    schedule=None,
    start_date=datetime.datetime(2022, 3, 4),
    catchup=False,
    tags=["example_ui"],
    render_template_as_native_obj=True,
    params={
        "names": Param(
            ["Linda", "Martha", "Thomas"],
            type="array",
            description="Define the list of names for which greetings should be generated in the logs."
            " Please have one name per line.",
            title="Names to greet",
        ),
        "english": Param(True, type="boolean", title="English"),
        "german": Param(True, type="boolean", title="German (Formal)"),
        "french": Param(True, type="boolean", title="French"),
    },
) as dag:

    @task(task_id="get_names")
    def get_names(**kwargs) -> list[str]:
        ti: TaskInstance = kwargs["ti"]
        dag_run: DagRun = ti.dag_run
        if "names" not in dag_run.conf:
            print("Uuups, no names given, was no UI used to trigger?")
            return []
        return dag_run.conf["names"]

    names = get_names()
    print(names)
