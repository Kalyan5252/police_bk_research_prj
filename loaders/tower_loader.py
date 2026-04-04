import uuid
import pandas as pd
from utils.excel_reader import read_excel
from neo4j_client import run_query

def safe_float(val):
    if pd.isna(val):
        return None
    return float(val)

def load_tower_dump(path):
    df = read_excel(path)

    query = """
    UNWIND $rows AS row

    MERGE (ph:PhoneNumber {msisdn: row.phone})
    MERGE (d:Device {imei: row.imei})

    MERGE (loc:Location {cell_id: row.cell_id})
    SET loc.lat = row.lat,
        loc.lon = row.lon

    CREATE (e:PresenceEvent {
        event_id: row.event_id,
        timestamp: row.timestamp,
        type: row.type,
        duration: row.duration
    })

    MERGE (ph)-[:SEEN_AT]->(e)
    MERGE (e)-[:AT_LOCATION]->(loc)
    MERGE (e)-[:USED_DEVICE]->(d)
    """

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

    run_query(query, {"rows": rows})
