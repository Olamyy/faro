import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response

from .ingest import buffer
from .models import CaptureEvent
from .routers import health, violations, values

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    flush_task = asyncio.create_task(buffer.start_periodic_flush())
    try:
        yield
    finally:
        flush_task.cancel()
        try:
            await flush_task
        except asyncio.CancelledError:
            pass
        buffer.flush_all()


app = FastAPI(title="faro-api", lifespan=lifespan)

app.include_router(health.router)
app.include_router(violations.router)
app.include_router(values.router)


@app.post("/ingest", status_code=202)
async def ingest(event: CaptureEvent, request: Request) -> Response:
    try:
        buffer.add(event)
    except Exception:  # noqa: BLE001
        logger.exception("Failed to buffer ingest event")
    return Response(status_code=202)

