import uuid
from utils.excel_reader import read_excel
from neo4j_client import run_query

def load_bank(path, account_number):
    df = read_excel(path)

    query = """
    UNWIND $rows AS row

    MERGE (acc:BankAccount {account_number: $account})

    CREATE (t:FinancialTransaction {
        txn_id: row.txn_id,
        date: row.date,
        debit: row.debit,
        credit: row.credit,
        desc: row.desc
    })

    MERGE (acc)-[:PERFORMED]->(t)
    """

    rows = []

    for _, r in df.iterrows():
        rows.append({
            "txn_id": str(uuid.uuid4()),
            "date": r["TRAN_DATE"],
            "debit": float(r["Debit"] or 0),
            "credit": float(r["Credit"] or 0),
            "desc": r["PARTICULARS"]
        })

    run_query(query, {"rows": rows, "account": account_number})
