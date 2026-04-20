import uuid
import pandas as pd
from utils.excel_reader import read_excel
from neo4j import GraphDatabase
import config

driver = GraphDatabase.driver(
    config.NEO4J_URI,
    auth=(config.NEO4J_USER, config.NEO4J_PASSWORD)
)

BATCH_SIZE = 2000

def safe_float(val):
    if pd.isna(val):
        return None
    return float(val)

def load_tower_dump_fast(path):
    df = read_excel(path)

    rows = []

    for _, r in df.iterrows():
        rows.append({
            "phone": str(r["A PARTY"]),
            "imei": str(r["IMEI A"]),
            "cell_id": str(r["FIRST CELL ID A"]),
            "lat": safe_float(r["LATITUDE"]),
            "lon": safe_float(r["LONGITUDE"]),
            "timestamp": f"{r['DATE']} {r['TIME']}",
            "type": r["CALL TYPE"],
            "duration": int(r["DURATION"]) if not pd.isna(r["DURATION"]) else 0,
            "event_id": str(uuid.uuid4())
        })

    def run_batch(tx, batch):
        tx.run("""
        UNWIND $rows AS row

        MERGE (ph:PhoneNumber {msisdn: row.phone})
        MERGE (d:Device {imei: row.imei})
        MERGE (loc:Location {cell_id: row.cell_id})

        CREATE (e:PresenceEvent {
            event_id: row.event_id,
            timestamp: row.timestamp,
            type: row.type,
            duration: row.duration
        })

        MERGE (ph)-[:SEEN_AT]->(e)
        MERGE (e)-[:AT_LOCATION]->(loc)
        MERGE (e)-[:USED_DEVICE]->(d)
        """, rows=batch)

    with driver.session() as session:
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i+BATCH_SIZE]
            if hasattr(session, "execute_write"):
                session.execute_write(run_batch, batch)
            else:
                session.write_transaction(run_batch, batch)
            print(f"Ingested {i + len(batch)} rows")
