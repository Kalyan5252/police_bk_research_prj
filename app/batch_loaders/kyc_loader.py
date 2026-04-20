import uuid
from utils.pdf_parser import extract_text, extract_kyc_fields
from neo4j_client import driver

def load_kyc_fast(pdf_path):
    text = extract_text(pdf_path)
    fields = extract_kyc_fields(text)

    def write(tx):
        tx.run("""
        MERGE (p:Person {person_id:$id})
        SET p.name=$name

        MERGE (ph:PhoneNumber {msisdn:$mobile})
        MERGE (p)-[:OWNS]->(ph)

        MERGE (doc:Document {doc_number:$pan})
        MERGE (p)-[:HAS_DOCUMENT]->(doc)
        """,
        id=str(uuid.uuid4()),
        name=fields["name"],
        mobile=fields["mobile"],
        pan=fields["pan"])

    with driver.session() as session:
        session.execute_write(write)
