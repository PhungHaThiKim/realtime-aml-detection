# === setup raw data
-> cp HI-Small_Trans.csv ~/workspace/aml-detection/dataset/
docker network create --driver bridge aml-detection-network

# === setup HADOOP
chmod +x entrypoint.sh
make up-hadoop
docker exec -it hadoop hdfs dfs -mkdir -p /data/transactions/raw #-> create folder on hadoop
docker exec -it hadoop hdfs dfs -put -f /app/dataset/HI-Small_Trans.csv /data/transactions/raw/
docker exec -it hadoop hdfs dfs -chmod -R 777 /data/transactions

# === setup SPARK
make build-spark
make up-spark

# === setup NOTEBOOK
make build-notebook
make up-notebook

# === setup first data partitioned
-> run partition hadoop: pre_TransactionPartition.ipynb
-> run tạo ra file csv để view: pre_test.ipynb

# === setup NEO4J
make up-neo4j

# === setup AIRFLOW
bash setup-airflow.sh
make build-airflow
make up-airflow
#---------------------(lưu ý nên test trên máy window sửa dockerfile: /usr/lib/jvm/java-17-openjdk-[amd64])

# === TEST Flow BATCH
-> create connection from airflow to spark
-> run batch notebook: 1_batch-transactions.ipynb
-> test graph
MATCH (bank1:Bank)-[:HAS]->(a:Account)-[t:TRANSACTION]->(b:Account)<-[:HAS]-(bank2:Bank)
WHERE t.date >= "2022-09-01" AND t.date <= "2022-09-10"
LIMIT 200
RETURN bank1, a, t, b, bank2
## delete nếu muốn làm lại MATCH (n) DETACH DELETE n

# === setup kafka
make up-kafka
#------------- lưu ý nhiều khi kafka không run lên thì cần phải run lại trong docker desktop

# === setup flink
make build-flink
make up-flink
#---------------------(lưu ý nên test trên máy window sửa dockerfile: /usr/lib/jvm/java-17-openjdk-[amd64])
-> cp DATE.csv ~/workspace/aml-detection/notebooks

# === setup mlmodel
make build-mlmodel
make up-mlmodel
curl -X POST http://localhost:5000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "tx_count_delta": -3,
    "avg_amount_spike": 1.5,
    "target_growth": 0.4,
    "smurfing_score": 0.02,
    "round_trip_combined": 1,
    "avg_round_trip_len": 3
  }'

# === TEST Flow STREAM
docker exec -it flink-jobmanager flink run -py /opt/flink/jobs/flink_kafka_stream.py
-> run stream notebook: 2_stream-transactions.ipynb

docker exec -it flink-jobmanager python /opt/flink/jobs/flink_kafka_stream.py


MATCH ()-[r]->()
WHERE r.date STARTS WITH "2025-04-06"
DELETE r
