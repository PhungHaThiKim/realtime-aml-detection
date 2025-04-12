from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import time
from pyspark.sql import SparkSession
from airflow.operators.python import get_current_context
from neo4j import GraphDatabase
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pyspark.sql.functions import concat_ws
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# Fake functions to simulate actual work
def choose_date():
    context = get_current_context()
    run_date = context["dag_run"].conf.get("run_date") if context["dag_run"] else None

    if run_date is None:
        run_date = "2022-09-02"  # hoặc datetime.today().strftime("%Y-%m-%d")
        print(f"⚠️ Không nhập run_date, dùng mặc định: {run_date}")
    else:
        print(f"📅 Ngày được chọn: {run_date}")

    return run_date


def transform_data(**context):
    ti = context["ti"]
    run_date = ti.xcom_pull(task_ids="choose_date")
    print(f"🔧 Transform dữ liệu cho ngày: {run_date}")

    dt = datetime.strptime(run_date, "%Y-%m-%d")
    year, month, day = dt.year, dt.month, dt.day
    print(f"📅 Running Spark query for {run_date} (Y:{year}, M:{month}, D:{day})")

    spark = SparkSession.builder \
        .appName("TransformDataOverwrite") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # Đọc dữ liệu theo partition
    path = f"hdfs://hadoop:9000/data/transactions/partitioned/year={year}/month={month}/day={day}"
    df = spark.read.parquet(path)
    df.show(5)

    # # Overwrite luôn cột gốc
    df_transformed = df.withColumn(
        "From Account", concat_ws("_", df["From Bank"], df["From Account"])
    ).withColumn(
        "To Account", concat_ws("_", df["To Bank"], df["To Account"])
    )

    df_transformed.write.mode("overwrite").parquet(
        f"hdfs://hadoop:9000/data/transactions/tmp/query_result_transformed/{run_date}"
    )

    df_transformed.select("From Bank", "From Account", "To Bank", "To Account").show(5, truncate=False)
    spark.stop()


with DAG(
    dag_id="batch-transaction-to-graph",
    description="Chuyển dữ liệu giao dịch thành graph và lưu vào Neo4j",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    t1 = PythonOperator(task_id="choose_date", python_callable=choose_date)

    t2 = PythonOperator(task_id="transform_data", python_callable=transform_data)

    write_neo4j = SparkSubmitOperator(
        task_id="write_to_neo4j",
        application="/opt/airflow/scripts/write_to_neo4j.py",
        application_args=["{{ ti.xcom_pull(task_ids='choose_date') }}"],
        conn_id="spark_standalone"
    )

    call_DAG_add_features = TriggerDagRunOperator(
        task_id="call_DAG_add_features",
        trigger_dag_id="add-features-daily",  # DAG bạn muốn gọi tiếp
        conf={"run_date": "{{ ti.xcom_pull(task_ids='choose_date') }}"},  # truyền ngày đã xử lý
        wait_for_completion=False,  # nếu muốn đợi DAG kia hoàn thành thì đặt True
    )

    t1 >> t2 >> write_neo4j >> call_DAG_add_features
    # t1 >> t3 >> write_neo4j >> t5
