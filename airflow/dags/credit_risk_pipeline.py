
from datetime import timedelta
import logging

import pendulum

from airflow.sdk import DAG, Variable
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
    PythonOperator,
    BranchPythonOperator,
)
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.standard.operators.bash import BashOperator


# 1. Variable
config = Variable.get(
    "credit_pipeline_config",
    deserialize_json=True
)

POSTGRES_TABLE = config["postgres_table"]
S3_PATH = config["s3_path"]


# 2. Dünənin tarixini hesablayan vahid funksiya
def get_yesterday(logical_date):
    if logical_date is None:
        raise ValueError(
            "DAG-i explicit logical_date ile trigger edin."
        )

    return (
        logical_date - timedelta(days=1)
    ).strftime("%Y-%m-%d")


# 3. Uğursuz task üçün callback
def failure_callback(context):
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id

    message = (
        f"DAG: {dag_id} | "
        f"Task: {task_id} | "
        "Status: FAILED"
    )

    logging.error(message)


# 4. Sətir sayına görə branch
def choose_branch(**context):
    yesterday = get_yesterday(context["logical_date"])

    hook = PostgresHook(postgres_conn_id="pg_conn_id")

    sql = f"""
        SELECT COUNT(*)
        FROM {POSTGRES_TABLE}
        WHERE application_date = %s
    """

    row_count = hook.get_first(
        sql,
        parameters=(yesterday,)
    )[0]

    logging.info(
        "Date: %s | Row count: %s",
        yesterday,
        row_count
    )

    if row_count > 2:
        return "run_spark"

    return "fail_task"


# 5. Qəsdən xəta yaradan funksiya
def fail_pipeline():
    raise AirflowFailException(
        "Dunun melumat sayi 2-den cox deyil!"
    )


# 6. DAG
with DAG(
    dag_id="credit_risk_pipeline",
    start_date=pendulum.datetime(2026, 3, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data_engineering",
        "retries": 0,
        "on_failure_callback": failure_callback,
    },
    user_defined_macros={
        "get_yesterday": get_yesterday
    },
    tags=["credit", "spark", "postgres", "minio"],
) as dag:

    # START
    start = EmptyOperator(
        task_id="start"
    )

    # BONUS: Sensorları TaskGroup daxilinə yığırıq
    with TaskGroup(group_id="data_checks"):

        check_yesterday_data = SqlSensor(
            task_id="check_yesterday_data",
            conn_id="pg_conn_id",
            sql=f"""
                SELECT COUNT(*)
                FROM {POSTGRES_TABLE}
                WHERE application_date =
                    '{{{{ get_yesterday(logical_date) }}}}'
            """,
            mode="reschedule",
            poke_interval=10,
            timeout=60,
        )

        check_blacklist_file = FileSensor(
            task_id="check_blacklist_file",
            fs_conn_id="fs_default",
            filepath="/opt/airflow/data/blacklist.csv",
            mode="reschedule",
            poke_interval=10,
            timeout=60,
        )

    # BRANCH
    branch_on_row_count = BranchPythonOperator(
        task_id="branch_on_row_count",
        python_callable=choose_branch,
    )

    # SPARK
    run_spark = SparkSubmitOperator(
        task_id="run_spark",
        conn_id="spark_default",
        application="/opt/airflow/spark_jobs/credit_risk_job.py",
        name="CreditRiskPipeline",
        packages=(
            "org.postgresql:postgresql:42.7.4,"
            "io.delta:delta-spark_2.12:3.3.2,"
            "org.apache.hadoop:hadoop-aws:3.3.4"
        ),
        conf={
            "spark.sql.extensions":
                "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog":
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
        application_args=[
            "--table", POSTGRES_TABLE,
            "--s3-path", S3_PATH,
        ],
        env_vars={
            "PG_USER": "{{ conn.pg_conn_id.login }}",
            "PG_PASSWORD": "{{ conn.pg_conn_id.password }}",
        },
        verbose=False,
    )
    
    # SPARK (BashOperator ilə, bonus)
    run_spark_bash = BashOperator(
        task_id="run_spark_bash",
        bash_command=(
            "spark-submit "
            "--master '{{ conn.spark_default.host }}' "
            "--packages org.postgresql:postgresql:42.7.4,"
            "io.delta:delta-spark_2.12:3.3.2,"
            "org.apache.hadoop:hadoop-aws:3.3.4 "
            "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
            "--conf spark.sql.catalog.spark_catalog="
            "org.apache.spark.sql.delta.catalog.DeltaCatalog "
            "/opt/airflow/spark_jobs/credit_risk_job.py "
            "--table {{ var.json.credit_pipeline_config.postgres_table }} "
            "--s3-path {{ var.json.credit_pipeline_config.s3_path }}"
        ),
        env={
            "PG_USER": "{{ conn.pg_conn_id.login }}",
            "PG_PASSWORD": "{{ conn.pg_conn_id.password }}",
        },
        append_env=True,
    )

    # FAIL
    fail_task = PythonOperator(
        task_id="fail_task",
        python_callable=fail_pipeline,
    )

    # END
    end_task = EmptyOperator(
        task_id="end_task",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # 7. Dependencies
    start >> [check_yesterday_data, check_blacklist_file]

    [check_yesterday_data, check_blacklist_file] >> branch_on_row_count

    branch_on_row_count >> [run_spark, fail_task]

    run_spark >> run_spark_bash

    [run_spark_bash, fail_task] >> end_task