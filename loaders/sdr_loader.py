from utils.excel_reader import read_excel
from neo4j_client import run_query
import uuid

def load_sdr(path):
    df = read_excel(path)

    query = """
    UNWIND $rows AS row

    MERGE (p:Person {person_id: row.id})
    SET p.name = row.name

    MERGE (ph:PhoneNumber {msisdn: row.phone})

    MERGE (p)-[:OWNS]->(ph)
    """

    rows = []

    for _, r in df.iterrows():
        rows.append({
            "id": str(uuid.uuid4()),
            "name": r["Subscriber Name"],
            "phone": str(r["Phone Number"])
        })

    run_query(query, {"rows": rows})
