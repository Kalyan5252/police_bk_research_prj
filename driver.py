import os
from loaders.cdr_loader import load_cdr
from loaders.ipdr_loader import load_ipdr
from loaders.bank_loader import load_bank
from loaders.sdr_loader import load_sdr
from loaders.kyc_loader import load_kyc
from loaders.tower_loader import load_tower_dump


from batch_loaders.cdr_loader import load_cdr_fast
from batch_loaders.tower_loader import load_tower_dump_fast
from batch_loaders.ipdr_loader import load_ipdr_fast
from batch_loaders.kyc_loader import load_kyc_fast
from batch_loaders.bank_loader import load_bank_fast
from batch_loaders.sdr_loader import load_sdr_fast

# CDR INGESTION
for file in os.listdir("./data/cdr"):
    if file.endswith(".xlsx"):
        load_cdr_fast(f"./data/cdr/{file}")

# IPDR INGESTION
# for file in os.listdir("./data/ipdr"):
#     if file.endswith(".xlsx"):
#         load_ipdr_fast(f"./data/ipdr/{file}")

# TOWER INGESTION
# for file in os.listdir("./data/tower"):
#     if file.endswith(".xlsx"):
#         load_tower_dump_fast(f"./data/tower/{file}")

# BANK INGESTION
# load_bank_fast(f"./data/banks/Aleem bankstament.xlsx", "201017455953")
# load_bank_fast(f"./data/banks/Srujan Bankstatement.xlsx", "369401504800")
# load_bank_fast(f"./data/banks/Varshith Banakstatement.xlsx", "924020024188422")

# KYC INGESTION
# for file in os.listdir("./data/kyc"):
#     if file.endswith(".pdf"):
#         load_kyc_fast(f"./data/kyc/{file}")