import os
import asyncio
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Security, Depends, Response, status
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from starlette.status import HTTP_403_FORBIDDEN
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

from app.main import run_analytics_pipeline as run_pipeline
from app.pdf_generator import generate_monthly_pdf

BASE_DIR = Path(__file__).resolve().parent.parent
CACHE_FILE = BASE_DIR / "data" / "analytics_cache.json"
PDF_FILE = BASE_DIR / "data" / "Monthly_Executive_Summary.pdf"

load_dotenv()

EXPECTED_API_KEY = os.getenv("API_SECRET_KEY", "fallback-dev-key")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def verify_api_key(api_key: str = Security(api_key_header)):
    if not api_key or api_key != EXPECTED_API_KEY:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN, detail="Could not validate API Key"
        )


scheduler = AsyncIOScheduler()
app_state = {"is_ready": False}


async def scheduled_pipeline_job():
    """Executes the synchronous pipeline off the main asyncio thread."""
    try:
        await asyncio.to_thread(run_pipeline)
    except Exception as e:
        print(f"Scheduled pipeline run failed: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Fire startup task asynchronously without awaiting so Uvicorn binds port 5000 immediately
    async def initial_startup_task():
        try:
            await asyncio.to_thread(run_pipeline)
        except Exception as e:
            print(f"Startup pipeline run failed: {e}")
        finally:
            app_state["is_ready"] = True

    asyncio.create_task(initial_startup_task())

    # 2. Schedule daily execution off-thread at midnight UTC
    scheduler.add_job(scheduled_pipeline_job, "cron", hour=0, minute=0)
    scheduler.start()

    yield

    scheduler.shutdown()


# Initialize FastAPI with lifespan attached
app = FastAPI(title="Campus Telemetry Analytics API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Unauthenticated Health Probes (For Orchestrators/Cloud Run) ---

@app.get("/healthz", status_code=status.HTTP_200_OK)
async def liveness_probe():
    """Liveness probe: Instant 200 OK so container health checks pass instantly."""
    return {"status": "alive"}


@app.get("/ready")
async def readiness_probe(response: Response):
    """Readiness probe: Returns 503 until warm-up completes or cached data exists."""
    if not app_state["is_ready"] and not CACHE_FILE.exists():
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "warming_up"}
    return {"status": "ready"}


# --- Authenticated Application Routes ---

@app.get("/analytics", dependencies=[Depends(verify_api_key)])
async def get_analytics():
    if not CACHE_FILE.exists():
        raise HTTPException(
            status_code=503, detail="Analytics cache file not yet generated."
        )
    return FileResponse(
        CACHE_FILE,
        media_type="application/json",
        headers={"Cache-Control": "public, max-age=300"},
    )


@app.get("/analytics/pdf", dependencies=[Depends(verify_api_key)])
async def get_analytics_pdf():
    if not PDF_FILE.exists():
        if CACHE_FILE.exists():
            await asyncio.to_thread(
                generate_monthly_pdf, str(CACHE_FILE), str(PDF_FILE)
            )
        else:
            raise HTTPException(
                status_code=503, detail="Cache file missing; cannot generate PDF."
            )

    return FileResponse(
        PDF_FILE,
        media_type="application/pdf",
        filename="Monthly_Executive_Summary.pdf",
    )