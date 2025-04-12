import json
import requests
from kafka import KafkaProducer
from pyflink.datastream.functions import ProcessFunction, RuntimeContext

class PredictProcessor(ProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self.producer = KafkaProducer(
            bootstrap_servers="kafka:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8")
        )
        self.model_url = "http://ml_model:5000/predict"
        print("🤖 Prediction processor initialized")

    def close(self):
        self.producer.close()
        print("✅ Prediction processor closed")

    def check_rules(self, record):
        reasons = []
        if record.get("smurfing_score", 0) > 10:
            reasons.append("Smurfing")
        if record.get("target_growth", 0) > 2:
            reasons.append("New Targets Spike")
        if record.get("tx_count_delta", 0) > 10:
            reasons.append("Tx Count Burst")
        if record.get("round_trip_combined", 0) > 0 and record.get("avg_round_trip_len", 0) >= 3:
            reasons.append("Round-trip")
        if record.get("avg_amount_spike", 0) > 5:
            reasons.append("Avg Amount Spike")

        return {
            "rule_hit": len(reasons) > 0,
            "rule_reason": ", ".join(reasons)
        }

    def process_element(self, value, ctx):
        try:
            record = json.loads(value)

            features = {
                "tx_count_delta": record.get("tx_count_delta", 0),
                "avg_amount_spike": record.get("avg_amount_spike", 0),
                "target_growth": record.get("target_growth", 0),
                "smurfing_score": record.get("smurfing_score", 0),
                "round_trip_combined": record.get("round_trip_combined", 0),
                "avg_round_trip_len": record.get("avg_round_trip_len", 0)
            }

            response = requests.post(self.model_url, json=features)
            prediction = response.json() if response.ok else {"error": response.text}

            rule_eval = self.check_rules(record)

            output = {
                "txn_id": record.get("txn_id"),
                "account_id": record.get("account_id"),
                "score": prediction.get("score"),
                "is_anomaly": prediction.get("is_anomaly"),
                **rule_eval
            }

            self.producer.send("predict", key=record.get("account_id"), value=output)
            print(f"✅ Prediction sent: {output}")

        except Exception as e:
            print(f"❌ Prediction processing error: {e}")

        yield json.dumps(output)
