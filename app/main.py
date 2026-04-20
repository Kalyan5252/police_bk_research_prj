from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import loaders

app = FastAPI(title="Data Loaders API", version="1.0.0")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(loaders.router, prefix="/api/v1/loaders", tags=["loaders"])

@app.get("/health")
def health_check():
    return {"status": "ok"}
