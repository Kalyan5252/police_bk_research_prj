import uuid
import pandas as pd
from utils.batch import chunks
from utils.excel_reader import read_excel
from neo4j_client import driver

BATCH_SIZE = 2000

def find_column(df, keywords):
    for col in df.columns:
        if all(k.lower() in col.lower() for k in keywords):
            return col
    raise Exception(f"Column not found for {keywords}")

def load_ipdr_fast(path):
    df = read_excel(path)
    df.columns = df.columns.str.strip()

    msisdn_col = find_column(df, ["msisdn"])
    imei_col = find_column(df, ["imei"])
    dest_ip_col = find_column(df, ["destination", "ip"])
    start_time_col = find_column(df, ["start time"])
    end_time_col = find_column(df, ["end time"])
    start_date_col = find_column(df, ["start date"])
    end_date_col = find_column(df, ["end date"])

    rows = []

    for _, r in df.iterrows():
        rows.append({
            "msisdn": str(r[msisdn_col]),
            "imei": str(r[imei_col]),
            "dest_ip": str(r[dest_ip_col]),
            "start": f"{r[start_date_col]} {r[start_time_col]}",
            "end": f"{r[end_date_col]} {r[end_time_col]}",
            "session_id": str(uuid.uuid4())
        })

    def run_batch(tx, batch):
        tx.run("""
        UNWIND $rows AS row

        MERGE (ph:PhoneNumber {msisdn: row.msisdn})
        MERGE (ip:IPAddress {ip: row.dest_ip})
        MERGE (d:Device {imei: row.imei})

        CREATE (s:InternetSession {
            session_id: row.session_id,
            start_time: row.start,
            end_time: row.end
        })

        MERGE (ph)-[:USED]->(s)
        MERGE (s)-[:CONNECTED_TO]->(ip)
        MERGE (s)-[:USED_DEVICE]->(d)
        """, rows=batch)

    with driver.session() as session:
        for i, batch in enumerate(chunks(rows, BATCH_SIZE)):
            session.execute_write(run_batch, batch)
            print(f"IPDR batch {i+1} done")
