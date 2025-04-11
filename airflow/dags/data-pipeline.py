from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import time
from pyspark.sql import SparkSession
from airflow.operators.python import get_current_context
from neo4j import GraphDatabase
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


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

def spark_query(**context):
    ti = context["ti"]
    run_date = ti.xcom_pull(task_ids="choose_date")

    dt = datetime.strptime(run_date, "%Y-%m-%d")
    year, month, day = dt.year, dt.month, dt.day

    print(f"📅 Running Spark query for {run_date} (Y:{year}, M:{month}, D:{day})")

    # Tạo SparkSession
    spark = SparkSession.builder \
        .appName("AirflowInlineSparkQuery") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # Đọc dữ liệu theo partition
    df = spark.read.parquet("hdfs://hadoop:9000/data/transactions/partitioned")
    df_filtered = df.filter(
        (df["year"] == year) &
        (df["month"] == month) &
        (df["day"] == day)
    )

    # Hiển thị vài dòng đầu
    df_filtered.show(5, truncate=False)

    # Ghi tạm ra HDFS để bước tiếp theo dùng
    df_filtered.write.mode("overwrite") \
        .parquet(f"hdfs://hadoop:9000/data/transactions/tmp/query_result/{run_date}")

    spark.stop()



def transform_data():
    print("🔧 Transform dữ liệu nếu cần (lọc, chuẩn hóa, ...) ")
    time.sleep(2)


def cleanup():
    print("🧹 Dọn dẹp tài nguyên sau khi hoàn thành")
    time.sleep(1)


with DAG(
    dag_id="batch-transaction-to-graph",
    description="Chuyển dữ liệu giao dịch thành graph và lưu vào Neo4j",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    t1 = PythonOperator(task_id="choose_date", python_callable=choose_date)

    t2 = PythonOperator(
        task_id="run_spark_query",
        python_callable=spark_query,
        provide_context=True,
    )

    t3 = PythonOperator(task_id="transform_data", python_callable=transform_data)

    write_neo4j = SparkSubmitOperator(
        task_id="write_to_neo4j",
        application="/opt/airflow/scripts/write_to_neo4j.py",
        application_args=["{{ ti.xcom_pull(task_ids='choose_date') }}"],
        conn_id="spark_standalone"
    )

    t5 = PythonOperator(task_id="cleanup", python_callable=cleanup)

    t1 >> t2 >> t3 >> write_neo4j >> t5
    # t1 >> t3 >> write_neo4j >> t5
