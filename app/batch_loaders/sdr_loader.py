import uuid
from utils.batch import chunks
from utils.excel_reader import read_excel
from neo4j_client import driver

BATCH_SIZE = 2000

def load_sdr_fast(path):
    df = read_excel(path)

    rows = []

    for _, r in df.iterrows():
        rows.append({
            "id": str(uuid.uuid4()),
            "name": r["Subscriber Name"],
            "phone": str(r["Phone Number"])
        })

    def run_batch(tx, batch):
        tx.run("""
        UNWIND $rows AS row

        MERGE (p:Person {person_id: row.id})
        SET p.name = row.name

        MERGE (ph:PhoneNumber {msisdn: row.phone})

        MERGE (p)-[:OWNS]->(ph)
        """, rows=batch)

    with driver.session() as session:
        for i, batch in enumerate(chunks(rows, BATCH_SIZE)):
            session.execute_write(run_batch, batch)
            print(f"SDR batch {i+1} done")
