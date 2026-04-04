from neo4j_client import run_query

queries = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE p.person_id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (ph:PhoneNumber) REQUIRE ph.msisdn IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Device) REQUIRE d.imei IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (b:BankAccount) REQUIRE b.account_number IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (ip:IPAddress) REQUIRE ip.ip IS UNIQUE",
]

for q in queries:
    run_query(q)

print("Constraints created")
