import logging
import uvicorn
import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.api.routes import router as api_router

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix="/api")


@app.get("/")
def read_root():
    return {
        "message": "SERP API",
        "version": settings.API_VERSION,
        "status": "running",
        "endpoints": [
            "POST /api/serp/search    — trigger a DataForSEO SERP search",
            "POST /api/serp/webhook   — receive DataForSEO results callback",
            "GET  /api/serp/{job_id}  — poll job status",
        ],
    }


@app.get("/health")
def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8002, reload=True)
