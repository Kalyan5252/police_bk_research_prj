import uuid
from utils.excel_reader import read_excel
from neo4j_client import run_query

def load_cdr(path):
    df = read_excel(path)

    query = """
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
    """

    rows = []

    for _, r in df.iterrows():
        rows.append({
            "a": str(r["A PARTY"]),
            "b": str(r["B PARTY"]),
            "imei": str(r["IMEI A"]),
            "timestamp": f"{r['DATE']} {r['TIME']}",
            "duration": int(r["DURATION"]),
            "type": r["CALL TYPE"],
            "event_id": str(uuid.uuid4())
        })

    run_query(query, {"rows": rows})
