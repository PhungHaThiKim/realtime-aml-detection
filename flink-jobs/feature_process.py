import json
import networkx as nx
from kafka import KafkaProducer
from pyflink.datastream.functions import ProcessFunction, RuntimeContext
from neo4j import GraphDatabase
from datetime import datetime, timedelta

NEO4J_STREAM_URI = "bolt://neo4j-stream:7687"
NEO4J_BATCH_URI = "bolt://neo4j-batch:7687"

class FeatureMergeProcessor(ProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self.producer = KafkaProducer(
            bootstrap_servers="kafka:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8")
        )
        self.driver_stream = GraphDatabase.driver(NEO4J_STREAM_URI, auth=None)
        self.driver_batch = GraphDatabase.driver(NEO4J_BATCH_URI, auth=None)
        print("🚀 Feature processor initialized")

    def close(self):
        self.producer.close()
        self.driver_stream.close()
        self.driver_batch.close()
        print("✅ Resources closed")

    def query_feature_snapshot(self, driver, acc_id):
        with driver.session() as session:
            res = session.run(
                """
                MATCH (a:Account {id: $id})
                RETURN a.feature_day_tx_count AS tx_count,
                       a.feature_day_avg_amount AS avg_amount,
                       a.feature_day_unique_targets AS unique_targets,
                       a.feature_round_trip_count AS round_trip,
                       a.feature_small_tx_count AS small_tx
                """,
                id=acc_id
            ).single()
            return dict(res) if res else {}

    def query_today_stats(self, driver, acc_id):
        with driver.session() as session:
            res = session.run(
                """
                MATCH (a:Account {id: $id})-[t:TRANSACTION]->(b:Account)
                RETURN count(t) AS today_tx, avg(t.amount_paid) AS today_avg, count(DISTINCT b.id) AS today_targets
                """,
                id=acc_id
            ).single()
            return dict(res) if res else {}

    def detect_round_trip_combined(self, acc_id, run_date_str):
        from_date = (datetime.fromisoformat(run_date_str) - timedelta(days=5)).strftime("%Y-%m-%d")
        to_date = run_date_str
        G = nx.MultiDiGraph()

        for driver in [self.driver_batch, self.driver_stream]:
            with driver.session() as session:
                res = session.run(
                    """
                    MATCH p = (a:Account {id: $id})-[r:TRANSACTION*2..3]->(b:Account)
                    WHERE all(rel IN relationships(p)
                        WHERE rel.date > date($from_date) AND rel.date <= date($to_date))
                    RETURN nodes(p) AS nodes
                    """,
                    id=acc_id,
                    from_date=from_date,
                    to_date=to_date
                )
                for row in res:
                    nodes = [n["id"] for n in row["nodes"]]
                    for i in range(len(nodes) - 1):
                        G.add_edge(nodes[i], nodes[i + 1])

        cycles = [c for c in nx.simple_cycles(G) if acc_id in c]
        return {
            "has_round_trip": len(cycles) > 0,
            "cycle_count": len(cycles),
            "avg_cycle_len": sum(len(c) for c in cycles) / len(cycles) if cycles else 0
        }

    def process_element(self, value, ctx):
        try:
            record = json.loads(value)
            acc_id = f"{record['from_acc']}"

            today_feat = self.query_today_stats(self.driver_stream, acc_id)
            batch_feat = self.query_feature_snapshot(self.driver_batch, acc_id)
            round_info = self.detect_round_trip_combined(acc_id, run_date_str=record["run_date"])

            print("today_feat", today_feat)
            print("batch_feat", batch_feat)
            print("round_info", round_info)

            today_tx = today_feat.get("today_tx") or 0
            today_avg = today_feat.get("today_avg") or 0
            today_targets = today_feat.get("today_targets") or 0

            tx_count = batch_feat.get("tx_count") or 0
            avg_amount = batch_feat.get("avg_amount") or 0
            unique_targets = batch_feat.get("unique_targets") or 0

            tx_count_delta = today_tx - tx_count
            avg_spike = (today_avg - avg_amount) / (avg_amount + 1e-6)
            target_growth = today_targets / (unique_targets + 1e-6)

            smurfing_score = today_tx / (today_avg + 1e-6) if today_avg < 10000 else 0

            features = {
                "txn_id": record["txn_id"],
                "account_id": acc_id,
                "tx_count_delta": tx_count_delta,
                "avg_amount_spike": avg_spike,
                "target_growth": target_growth,
                "smurfing_score": smurfing_score,
                "round_trip_combined": round_info["cycle_count"],
                "avg_round_trip_len": round_info["avg_cycle_len"],
                "feature_tx_count": batch_feat.get("tx_count", 0),
                "feature_avg_amount": batch_feat.get("avg_amount", 0),
                "feature_unique_targets": batch_feat.get("unique_targets", 0),
                "feature_small_tx": batch_feat.get("small_tx", 0)
            }

            self.producer.send("features", key=record["from_bank"], value=features)
            print(f"📤 Sent combined features: {features}")

        except Exception as e:
            print(f"❌ Feature processing error: {e}")

        yield json.dumps(features)
