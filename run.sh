#!/bin/bash
source bkenv/bin/activate
uvicorn api.main:app --reload
