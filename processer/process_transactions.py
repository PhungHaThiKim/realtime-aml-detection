from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, year, month, dayofmonth, col

# Tạo SparkSession kết nối đến cluster
spark = (
    SparkSession.builder.appName("Transaction Processor")
    .master("spark://localhost:7077")
    .getOrCreate()
)

# Đọc CSV từ HDFS
df = spark.read.csv(
    "hdfs://192.168.9.30:9000/data/transactions/raw/HI-Small_Trans.csv",
    header=True,
    inferSchema=True,
)
df.printSchema()
df.show(5)

# # Parse timestamp & tạo cột phân vùng
# df = df.withColumn("ts", to_timestamp(col("Timestamp"), "yyyy/MM/dd HH:mm"))
# df = (
#     df.withColumn("year", year("ts"))
#     .withColumn("month", month("ts"))
#     .withColumn("date", dayofmonth("ts"))
#     .withColumn("bank", col("From Bank"))
# )

# # Loại bỏ cột tạm thời
# df = df.drop("ts")

# # Ghi vào HDFS phân vùng
# df.write.partitionBy("year", "month", "date", "bank").mode("overwrite").parquet(
#     "hdfs://localhost:9000/data/transactions_partitioned"
# )

# print("✅ Done")
