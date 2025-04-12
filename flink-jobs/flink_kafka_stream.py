import logging
import sys
from neo4j import GraphDatabase

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common import Configuration
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.kafka import KafkaSink, DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.common import Types
from feature_process import FeatureMergeProcessor
from neo4j_process import Neo4jProcessor
from predict_process import PredictProcessor

NEO4J_URI = "bolt://neo4j-stream:7687"
# NEO4J_AUTH = ("neo4j", "password123")


def create_neo4j_indexes():
    index_queries = [
        "CREATE INDEX IF NOT EXISTS FOR (a:Account) ON (a.id)",
        "CREATE INDEX IF NOT EXISTS FOR (b:Bank) ON (b.id)",
        "CREATE INDEX IF NOT EXISTS FOR ()-[t:TRANSACTION]-() ON (t.txn_id)",
    ]

    driver = GraphDatabase.driver(NEO4J_URI, auth=None)
    with driver.session() as session:
        for query in index_queries:
            print(f"📌 Creating index: {query}")
            session.run(query)
    driver.close()
    print("✅ Indexes ready.")


def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=None)
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")

    driver.close()
    print("✅ Cleared all data from Neo4j `realtime` DB")
    create_neo4j_indexes()

    # 👇 Tạo config và gán jar connector
    config = Configuration()
    config.set_string(
        "pipeline.jars",
        "file:///opt/flink/jars/flink-sql-connector-kafka-3.3.0-1.20.jar",
    )

    # 👇 Truyền config vào môi trường Flink
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)
    env.add_python_file("/opt/flink/jobs/feature_process.py")
    env.add_python_file("/opt/flink/jobs/neo4j_process.py")
    env.add_python_file("/opt/flink/jobs/predict_process.py")
    env.enable_checkpointing(5000)

    # 👇 Tạo Flink Consumer
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_topics("transactions")
        .set_group_id("flink-consumer")
        # .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets())
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # 👇 Add source và in ra stdout
    ds = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Kafka Source",
    )

    # Ghi data -> Neo4j -> Features -> Predict
    processed = ds.process(Neo4jProcessor()).process(FeatureMergeProcessor()).process(PredictProcessor())
    processed.print()

    env.execute("Flink 1.20.1 KafkaSource Print")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    main()
