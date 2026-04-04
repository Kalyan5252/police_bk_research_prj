import uuid
from utils.excel_reader import read_excel
from neo4j_client import run_query

def load_ipdr(path):
    df = read_excel(path)

    query = """
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
    """

    rows = []

    for _, r in df.iterrows():
        rows.append({
            "msisdn": str(r["Landline/MSISDN/MDN/Leased Circuit ID"]),
            "dest_ip": str(r["Destination IP Address"]),
            "imei": str(r["IMEI"]),
            "start": r["Start Date of Public IP Address Allocation"],
            "end": r["End Date of Public IP Address Allocation"],
            "session_id": str(uuid.uuid4())
        })

    run_query(query, {"rows": rows})
