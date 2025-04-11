from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("TestConnectionToCluster")
    .master("spark://localhost:7077")
    .getOrCreate()
)

print("✅ Connected to Spark version:", spark.version)

df = spark.createDataFrame([(1, "OK"), (2, "Success")], ["id", "status"])
df.show()
spark.stop()
