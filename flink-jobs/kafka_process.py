import logging
from pyflink.datastream.functions import ProcessFunction, RuntimeContext
import json
from kafka import KafkaProducer

class KafkaWriterProcessor(ProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self.producer = KafkaProducer(
            bootstrap_servers="kafka:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("📤 Kafka producer initialized")

    def close(self):
        self.producer.close()
        print("✅ Kafka producer closed")

    def process_element(self, value, ctx):
        try:
            record = json.loads(value)
            features = {
                "txn_id": record["txn_id"],
                "amount": record["amount_paid"],
                "is_laundering": record["is_laundering"]
            }
            self.producer.send("features", key=record['from_bank'], value=features)
            print(f"📤 Kafka: sent feature {features}")
        except Exception as e:
            print(f"❌ Kafka send error: {e}")
        yield value