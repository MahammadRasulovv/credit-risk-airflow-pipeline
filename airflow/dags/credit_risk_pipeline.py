import pendulum

from airflow.sdk import DAG
from airflow.sdk.execution_time.macros import ds_add
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator



PG_CONN = "pg_conn_id"


def get_yesterday(ds):
    return ds_add(ds, -1)

def choose_branch(**context):

    day = get_yesterday(context["ds"])

    config = Variable.get(
        "credit_pipeline_config",
        deserialize_json=True
    )

    table = config["postgres_table"]

    pg = PostgresHook(postgres_conn_id=PG_CONN)

    count = pg.get_first(
        f"SELECT COUNT(*) FROM {table} WHERE application_date = %s",
        parameters=(day,)
    )[0]

    print(f"Date: {day}, Applications: {count}")

    if count > 2:
        return "run_spark"
    else:
        return "fail_task"


def fail_pipeline():
    raise AirflowFailException(
        "Dunenin kredit muracietlerinin sayi 2-den cox deyil!"
    )

def failure_callback(context):

    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id

    print(f"FAILED! DAG: {dag_id}, Task: {task_id}")



with DAG(
    dag_id="credit_risk_pipeline_updated",
    start_date=pendulum.datetime(2026, 3, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    user_defined_macros={
        "get_yesterday": get_yesterday
    }
) as dag:

    spark_env = {
        "PG_HOST": "{{ conn.pg_conn_id.host }}",
        "PG_PORT": "{{ conn.pg_conn_id.port }}",
        "PG_DATABASE": "{{ conn.pg_conn_id.schema }}",
        "PG_USER": "{{ conn.pg_conn_id.login }}",
        "PG_PASSWORD": "{{ conn.pg_conn_id.password }}",
        "MINIO_ENDPOINT": "{{ conn.minio_s3.extra_dejson.endpoint_url }}",
        "MINIO_ACCESS_KEY": "{{ conn.minio_s3.login }}",
        "MINIO_SECRET_KEY": "{{ conn.minio_s3.password }}"
    }

    start = EmptyOperator(
        task_id="start"
    )


    branch = BranchPythonOperator(
        task_id="branch_on_row_count",
        python_callable=choose_branch
    )


    check_yesterday_data = SqlSensor(
        task_id="check_yesterday_data",
        conn_id=PG_CONN,
        sql="""
            SELECT COUNT(*)
            FROM {{ var.json.credit_pipeline_config.postgres_table }}
            WHERE application_date = '{{ get_yesterday(ds) }}'
        """,
        mode="reschedule",
        poke_interval=10,
        timeout=60
    )

    check_blacklist_file = FileSensor(
        task_id="check_blacklist_file",
        fs_conn_id="fs_default",
        filepath="/opt/airflow/data/blacklist.csv",
        mode="reschedule",
        poke_interval=10,
        timeout=60
    )

    fail_task = PythonOperator(
        task_id="fail_task",
        python_callable=fail_pipeline,
        on_failure_callback=failure_callback
    )

    run_spark = SparkSubmitOperator(
        task_id="run_spark",
        conn_id="spark_default",
        application="/opt/airflow/spark_jobs/credit_risk_job.py",
        name="CreditRiskProcessing",
        application_args=[
            "{{ var.json.credit_pipeline_config.postgres_table }}",
            "{{ var.json.credit_pipeline_config.s3_path }}"
        ],
        env_vars=spark_env,
        packages="org.postgresql:postgresql:42.7.4,io.delta:delta-spark_2.12:3.3.2,org.apache.hadoop:hadoop-aws:3.3.4",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
    )

    end_task = EmptyOperator(
        task_id="end_task",
        trigger_rule="all_done"
    )


    final_status = EmptyOperator(
        task_id="final_status",
        trigger_rule="none_failed_min_one_success"
    )


    start >> [check_yesterday_data, check_blacklist_file]

    [check_yesterday_data, check_blacklist_file] >> branch

    branch >> [fail_task, run_spark]

    [end_task, run_spark, fail_task] >> final_status