from fastapi import APIRouter, UploadFile, File, Form, HTTPException
import shutil
import tempfile
import os

from batch_loaders.cdr_loader import load_cdr_fast
from batch_loaders.tower_loader import load_tower_dump_fast
from batch_loaders.ipdr_loader import load_ipdr_fast
from batch_loaders.kyc_loader import load_kyc_fast
from batch_loaders.bank_loader import load_bank_fast
from batch_loaders.sdr_loader import load_sdr_fast

router = APIRouter()

def save_upload_file_tmp(upload_file: UploadFile) -> str:
    try:
        suffix = os.path.splitext(upload_file.filename)[1] if upload_file.filename else ""
        fd, path = tempfile.mkstemp(suffix=suffix)
        with os.fdopen(fd, 'wb') as f:
            shutil.copyfileobj(upload_file.file, f)
        return path
    finally:
        upload_file.file.close()

@router.post("/cdr")
async def upload_cdr(file: UploadFile = File(...)):
    tmp_path = save_upload_file_tmp(file)
    try:
        load_cdr_fast(tmp_path)
        return {"message": "CDR uploaded and processed successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        os.remove(tmp_path)

@router.post("/ipdr")
async def upload_ipdr(file: UploadFile = File(...)):
    tmp_path = save_upload_file_tmp(file)
    try:
        load_ipdr_fast(tmp_path)
        return {"message": "IPDR uploaded and processed successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        os.remove(tmp_path)

@router.post("/tower")
async def upload_tower(file: UploadFile = File(...)):
    tmp_path = save_upload_file_tmp(file)
    try:
        load_tower_dump_fast(tmp_path)
        return {"message": "Tower dump uploaded and processed successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        os.remove(tmp_path)

@router.post("/sdr")
async def upload_sdr(file: UploadFile = File(...)):
    tmp_path = save_upload_file_tmp(file)
    try:
        load_sdr_fast(tmp_path)
        return {"message": "SDR uploaded and processed successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        os.remove(tmp_path)

@router.post("/kyc")
async def upload_kyc(file: UploadFile = File(...)):
    tmp_path = save_upload_file_tmp(file)
    try:
        load_kyc_fast(tmp_path)
        return {"message": "KYC loaded and processed successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        os.remove(tmp_path)

@router.post("/bank")
async def upload_bank(account_number: str = Form(...), file: UploadFile = File(...)):
    tmp_path = save_upload_file_tmp(file)
    try:
        load_bank_fast(tmp_path, account_number)
        return {"message": "Bank statement uploaded and processed successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        os.remove(tmp_path)
