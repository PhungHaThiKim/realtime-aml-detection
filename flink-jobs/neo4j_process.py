import logging
from pyflink.datastream.functions import ProcessFunction, RuntimeContext
from neo4j import GraphDatabase
import json

NEO4J_URI = "bolt://neo4j-stream:7687"


class Neo4jProcessor(ProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=None)
        print("🔥 Neo4j realtime processor initialized")

    def close(self):
        self.driver.close()
        print("✅ Neo4j connection closed")

    def process_element(self, value, ctx):
        try:
            record = json.loads(value)

            cypher = """
            MERGE (fromBank:Bank {id: $from_bank})
            MERGE (fromAcc:Account {id: $from_acc})
            MERGE (fromBank)-[:HAS]->(fromAcc)
            MERGE (toBank:Bank {id: $to_bank})
            MERGE (toAcc:Account {id: $to_acc})
            MERGE (toBank)-[:HAS]->(toAcc)
            MERGE (fromAcc)-[t:TRANSACTION {txn_id: $txn_id}]->(toAcc)
            SET t.timestamp = $ts,
                t.amount_received = $amount_received,
                t.receiving_currency = $receiving_currency,
                t.amount_paid = $amount_paid,
                t.payment_currency = $payment_currency,
                t.payment_format = $payment_format,
                t.date = $run_date
            """

            params = {
                "from_bank": int(record["from_bank"]),
                "from_acc": f"{record['from_bank']}_{record['from_acc']}",
                "to_bank": int(record["to_bank"]),
                "to_acc": f"{record['to_bank']}_{record['to_acc']}",
                "txn_id": int(record["txn_id"]),
                "amount_received": float(record["amount_received"]),
                "receiving_currency": record["receiving_currency"],
                "amount_paid": float(record["amount_paid"]),
                "payment_currency": record["payment_currency"],
                "payment_format": record["payment_format"],
                "ts": record["ts"],
                "run_date": record["run_date"],
            }

            with self.driver.session() as session:
                session.execute_write(lambda tx: tx.run(cypher, **params))

            print(f"✅ Saved transaction {params['txn_id']} to Neo4j realtime")
            logging.info(f"Saved transaction {params} to Neo4j realtime")

        except Exception as e:
            print(f"❌ Error processing message: {value}\n{e}")
            logging.error("Neo4j insert failed", exc_info=True)
            yield json.dumps({"error": str(e), "original": value})
        str_params = json.dumps(params)
        print(f"✅ Processed parameters: {str_params} - {type(str_params)}")
        yield str_params
