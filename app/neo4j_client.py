from neo4j import GraphDatabase
import config

driver = GraphDatabase.driver(
    config.NEO4J_URI,
    auth=(config.NEO4J_USER, config.NEO4J_PASSWORD)
)

def run_query(query, params=None):
    with driver.session() as session:
        session.run(query, params or {})
