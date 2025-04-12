from datetime import datetime
import time
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.utils.dates import days_ago
from neo4j import GraphDatabase
import pandas as pd

NEO4J_URI = "bolt://neo4j-batch:7687"

def choose_date():
    context = get_current_context()
    run_date = context["dag_run"].conf.get("run_date") if context["dag_run"] else None

    if run_date is None:
        run_date = "2022-09-02"
        print(f"⚠️ Không nhập run_date, dùng mặc định: {run_date}")
    else:
        print(f"📅 Ngày được chọn: {run_date}")

    return run_date

def enrich_tx_stats(**context):
    ti = context["ti"]
    run_date = ti.xcom_pull(task_ids="choose_date")
    print(f"📅 Enriching tx stats for {run_date}")

    driver = GraphDatabase.driver(NEO4J_URI, auth=None)
    from_date = datetime.fromisoformat(run_date) - pd.Timedelta(days=5)

    with driver.session() as session:
        result = session.run(
            """
            MATCH (a:Account)-[t:TRANSACTION]->(b:Account)
            WHERE t.date > $from_date AND t.date <= $to_date
            RETURN a.id as account_id, count(*) as tx_count,
                avg(t.amount_paid) as avg_amount,
                count(DISTINCT b.id) as unique_targets
            """,
            from_date=from_date.strftime("%Y-%m-%d"),
            to_date=run_date
        )

        for row in result:
            feat = row.data()
            session.run(
                """
                MERGE (a:Account {id: $id})
                SET a.feature_day_tx_count = $tx_count,
                    a.feature_day_avg_amount = $avg_amount,
                    a.feature_day_unique_targets = $unique_targets
                """,
                id=feat["account_id"],
                tx_count=feat["tx_count"],
                avg_amount=feat["avg_amount"],
                unique_targets=feat["unique_targets"]
            )
    driver.close()
    print("✅ TX stats enrichment completed")

def enrich_round_tripping(**context):
    run_date = context["ti"].xcom_pull(task_ids="choose_date")

    from_date = datetime.fromisoformat(run_date) - pd.Timedelta(days=5)
    from_date_str = from_date.strftime("%Y-%m-%d")

    driver = GraphDatabase.driver(NEO4J_URI, auth=None)
    with driver.session() as session:
        result = session.run(
            """
            MATCH (a:Account)
            WHERE EXISTS {
                MATCH (a)-[t:TRANSACTION]->()
                WHERE t.date > $from_date AND t.date <= $to_date
            }
            MATCH p = (a)-[t:TRANSACTION*2..3]->(a)
            WITH a, p, relationships(p) AS rels
            WHERE all(r IN rels WHERE r.date > $from_date AND r.date <= $to_date)
            RETURN a.id AS account_id, count(p) AS round_trip_count, avg(length(p)) AS avg_round_trip_length
            """,
            from_date=from_date_str,
            to_date=run_date
        )
        for row in result:
            feat = row.data()
            session.run(
                """
                MERGE (a:Account {id: $id})
                SET a.feature_round_trip_count = $count,
                    a.feature_round_trip_avg_len = $avg_len
                """,
                id=feat["account_id"],
                count=feat["round_trip_count"],
                avg_len=feat["avg_round_trip_length"]
            )
    driver.close()
    print("✅ Round-tripping enrichment completed")

def enrich_structuring(**context):
    run_date = context["ti"].xcom_pull(task_ids="choose_date")
    from_date = datetime.fromisoformat(run_date) - pd.Timedelta(days=5)
    from_date_str = from_date.strftime("%Y-%m-%d")

    driver = GraphDatabase.driver(NEO4J_URI, auth=None)
    with driver.session() as session:
        result = session.run(
            """
            MATCH (a:Account)-[t:TRANSACTION]->()
            WHERE t.amount_paid < 10000 AND t.date > $from_date AND t.date <= $to_date
            RETURN a.id AS account_id, count(t) AS small_tx_count
            """,
            from_date=from_date_str,
            to_date=run_date
        )
        for row in result:
            feat = row.data()
            session.run(
                """
                MERGE (a:Account {id: $id})
                SET a.feature_small_tx_count = $count
                """,
                id=feat["account_id"],
                count=feat["small_tx_count"]
            )
    driver.close()
    print("✅ Structuring enrichment completed")

def cleanup():
    print("🧹 Dọn dẹp tài nguyên sau khi hoàn thành")
    time.sleep(1)

with DAG(
    dag_id="add-features-daily",
    description="Tính toán và lưu các đặc trưng (feature) phức tạp cho node Account",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    t1 = PythonOperator(task_id="choose_date", python_callable=choose_date)

    tx_stats = PythonOperator(
        task_id="add_tx_stats",
        python_callable=enrich_tx_stats,
        provide_context=True,
    )

    round_trip = PythonOperator(
        task_id="add_round_tripping",
        python_callable=enrich_round_tripping,
        provide_context=True,
    )

    structuring = PythonOperator(
        task_id="add_structuring",
        python_callable=enrich_structuring,
        provide_context=True,
    )

    t5 = PythonOperator(task_id="cleanup", python_callable=cleanup)

    t1 >> [tx_stats, round_trip, structuring] >> t5