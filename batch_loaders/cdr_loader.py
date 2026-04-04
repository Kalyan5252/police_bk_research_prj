import uuid
import pandas as pd
from utils.batch import chunks
from utils.excel_reader import read_excel
from neo4j_client import driver

BATCH_SIZE = 2000

def load_cdr_fast(path):
    df = read_excel(path)

    rows = []

    for _, r in df.iterrows():
        rows.append({
            "a": str(r["A PARTY"]),
            "b": str(r["B PARTY"]),
            "imei": str(r["IMEI A"]),
            "timestamp": f"{r['DATE']} {r['TIME']}",
            "duration": int(r["DURATION"]) if not pd.isna(r["DURATION"]) else 0,
            "type": r["CALL TYPE"],
            "event_id": str(uuid.uuid4())
        })

    def run_batch(tx, batch):
        tx.run("""
        UNWIND $rows AS row

        MERGE (a:PhoneNumber {msisdn: row.a})
        MERGE (b:PhoneNumber {msisdn: row.b})
        MERGE (d:Device {imei: row.imei})

        CREATE (e:CommunicationEvent {
            event_id: row.event_id,
            timestamp: row.timestamp,
            duration: row.duration,
            type: row.type
        })

        MERGE (a)-[:INITIATED]->(e)
        MERGE (e)-[:TARGET]->(b)
        MERGE (e)-[:USED_DEVICE]->(d)
        """, rows=batch)

    with driver.session() as session:
        for i, batch in enumerate(chunks(rows, BATCH_SIZE)):
            session.execute_write(run_batch, batch)
            print(f"CDR batch {i+1} done")
