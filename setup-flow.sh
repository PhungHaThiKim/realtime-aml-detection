# === setup raw data
-> cp HI-Small_Trans.csv ~/workspace/aml-detection/dataset/

# === setup HADOOP
chmod +x entrypoint.sh
make up-hadoop
docker exec -it hadoop hdfs dfs -mkdir -p /data/transactions/raw
docker exec -it hadoop hdfs dfs -put -f /app/dataset/HI-Small_Trans.csv /data/transactions/raw/
docker exec -it hadoop hdfs dfs -chmod -R 777 /data/transactions

# === setup SPARK
make up-spark

# === setup NOTEBOOK
make up-notebook

# === setup first data partitioned
-> run partition hadoop: pre_TransactionPartition.ipynb
-> run tạo ra file csv để view: pre_test.ipynb

# === setup NEO4J (lưu ý là có 2 db)
make up-neo4j

# === setup AIRFLOW
bash setup-airflow.sh
make up-airflow
#---------------------(lưu ý nên test trên máy window sửa dockerfile: /usr/lib/jvm/java-17-openjdk-[amd64])

# === TEST Flow BATCH
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
make up-flink
#---------------------(lưu ý nên test trên máy window sửa dockerfile: /usr/lib/jvm/java-17-openjdk-[amd64])
-> cp DATE.csv ~/workspace/aml-detection/notebooks

# === TEST Flow STREAM
docker exec -it flink-jobmanager flink run -py /opt/flink/jobs/flink_kafka_stream.py
-> run stream notebook: 2_stream-transactions.ipynb