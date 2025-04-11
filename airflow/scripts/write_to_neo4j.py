from pyspark.sql import SparkSession
from neo4j import GraphDatabase
from datetime import datetime
from itertools import islice
import sys

NEO4J_URI = "bolt://neo4j-batch:7687"
NEO4J_AUTH = None


# ------------------------
# CREATE INDEXES
# ------------------------
def create_neo4j_indexes():
    index_queries = [
        "CREATE INDEX IF NOT EXISTS FOR (a:Account) ON (a.id)",
        "CREATE INDEX IF NOT EXISTS FOR (b:Bank) ON (b.id)",
        "CREATE INDEX IF NOT EXISTS FOR ()-[t:TRANSACTION]-() ON (t.txn_id)",
    ]

    driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    with driver.session() as session:
        for query in index_queries:
            print(f"📌 Creating index: {query}")
            session.run(query)
    driver.close()
    print("✅ Indexes ready.")


# ------------------------
# BATCH HELPER
# ------------------------
def batch(iterable, size=500):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk


# ------------------------
# SAVE PARTITION
# ------------------------
def save_partition_batch(partition, run_date: str):
    cypher = """
    UNWIND $rows AS row
    MERGE (fromBank:Bank {id: row.from_bank})
    MERGE (fromAcc:Account {id: row.from_acc})
    MERGE (fromBank)-[:HAS]->(fromAcc)
    MERGE (toBank:Bank {id: row.to_bank})
    MERGE (toAcc:Account {id: row.to_acc})
    MERGE (toBank)-[:HAS]->(toAcc)
    MERGE (fromAcc)-[t:TRANSACTION {txn_id: row.txn_id}]->(toAcc)
    SET t.timestamp = row.ts,
        t.amount_received = row.amount_received,
        t.receiving_currency = row.receiving_currency,
        t.amount_paid = row.amount_paid,
        t.payment_currency = row.payment_currency,
        t.payment_format = row.payment_format,
        t.is_laundering = row.is_laundering,
        t.date = row.run_date
    """

    driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    with driver.session() as session:
        for i, chunk in enumerate(batch(partition, size=500), start=1):
            if not chunk:
                continue

            print(f"🚀 Writing batch {i} with {len(chunk)} rows")

            records = [
                {
                    "from_bank": int(row["From Bank"]),
                    "from_acc": row["From Account"],
                    "to_bank": int(row["To Bank"]),
                    "to_acc": row["To Account"],
                    "txn_id": int(row["txn_id"]),
                    "amount_received": float(row["Amount Received"]),
                    "receiving_currency": row["Receiving Currency"],
                    "amount_paid": float(row["Amount Paid"]),
                    "payment_currency": row["Payment Currency"],
                    "payment_format": row["Payment Format"],
                    "is_laundering": int(row["Is Laundering"]),
                    "ts": row["ts"].strftime("%Y-%m-%d %H:%M:%S"),
                    "run_date": run_date,
                }
                for row in chunk
            ]

            session.execute_write(lambda tx: tx.run(cypher, rows=records))
    driver.close()


# ------------------------
# MAIN
# ------------------------
if __name__ == "__main__":
    run_date = sys.argv[1]
    print(f"📅 Processing for date: {run_date}")

    # Step 1: Create indexes
    create_neo4j_indexes()

    # Step 2: Delete old data
    print(f"🧨 Deleting old transactions for date {run_date}")
    driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    with driver.session() as session:
        session.run(
            """
            MATCH (:Account)-[t:TRANSACTION]->(:Account)
            WHERE t.date = $run_date
            DELETE t
        """,
            {"run_date": run_date},
        )
    driver.close()
    print("✅ Old transactions deleted")

    # Step 3: Load data from Spark
    spark = (
        SparkSession.builder.appName("write_to_neo4j")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    df = (
        spark.read.parquet(
            f"hdfs://hadoop:9000/data/transactions/tmp/query_result/{run_date}"
        )
        .orderBy("ts", "txn_id")
        .repartition(2)
    )  # ✅ tùy vào cluster

    # Step 4: Write to Neo4j
    df.foreachPartition(lambda p: save_partition_batch(p, run_date))

    spark.stop()
    print("✅ Done inserting to Neo4j for date:", run_date)
