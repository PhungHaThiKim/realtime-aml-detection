import sys
from pyspark.sql import SparkSession

run_date = sys.argv[1]  # ← ngày truyền từ Airflow CLI
print(f"📅 Received run_date: {run_date}")

spark = SparkSession.builder \
    .appName("QueryTransactionByDate") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Parse run_date nếu cần
from datetime import datetime
dt = datetime.strptime(run_date, "%Y-%m-%d")
year, month, day = dt.year, dt.month, dt.day

# Load dữ liệu phân vùng theo ngày
df = spark.read.parquet("hdfs://hadoop:9000/data/transactions/partitioned")
df_filtered = df.filter(
    (df["year"] == year) & (df["month"] == month) & (df["day"] == day)
)

df_filtered.show(5)
spark.stop()