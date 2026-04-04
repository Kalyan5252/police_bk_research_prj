from utils.pdf_parser import extract_text, extract_kyc_fields
from neo4j_client import run_query
import uuid

def load_kyc(pdf_path):
    text = extract_text(pdf_path)
    fields = extract_kyc_fields(text)

    query = """
    MERGE (p:Person {person_id:$id})
    SET p.name=$name

    MERGE (ph:PhoneNumber {msisdn:$mobile})
    MERGE (p)-[:OWNS]->(ph)

    MERGE (doc:Document {doc_number:$pan})
    MERGE (p)-[:HAS_DOCUMENT]->(doc)
    """

    run_query(query, {
        "id": str(uuid.uuid4()),
        "name": fields["name"],
        "mobile": fields["mobile"],
        "pan": fields["pan"]
    })
