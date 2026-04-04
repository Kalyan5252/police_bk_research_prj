import uuid
import pandas as pd
from utils.batch import chunks
from utils.excel_reader import read_excel
from neo4j_client import driver

BATCH_SIZE = 2000

def load_bank_fast(path, account_number):
    df = read_excel(path)
    print(df.columns.tolist())

    rows = []

    for _, r in df.iterrows():
        rows.append({
            "txn_id": str(uuid.uuid4()),
            "date": r["TRAN_DATE"],
            "debit": float(r["Debit"]) if not pd.isna(r["Debit"]) else 0,
            "credit": float(r["Credit"]) if not pd.isna(r["Credit"]) else 0,
            "desc": r["PARTICULARS"]
        })

    def run_batch(tx, batch):
        tx.run("""
        MERGE (acc:BankAccount {account_number:$account})

        WITH acc

        UNWIND $rows AS row

        CREATE (t:FinancialTransaction {
            txn_id: row.txn_id,
            date: row.date,
            debit: row.debit,
            credit: row.credit,
            desc: row.desc
        })

        MERGE (acc)-[:PERFORMED]->(t)
        """, rows=batch, account=account_number)

    with driver.session() as session:
        for i, batch in enumerate(chunks(rows, BATCH_SIZE)):
            session.execute_write(run_batch, batch)
            print(f"Bank batch {i+1} done")
