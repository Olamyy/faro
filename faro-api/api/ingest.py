import asyncio
import logging
import threading
from collections import deque

from .config import settings
from .models import CaptureEvent
from .store import ParquetStore

logger = logging.getLogger(__name__)


class _IngestionBuffer:

    def __init__(self) -> None:
        self._buffer: deque[CaptureEvent] = deque()
        self._lock = threading.Lock()
        self._flush_task: asyncio.Task | None = None

    def add(self, event: CaptureEvent) -> None:
        with self._lock:
            self._buffer.append(event)
            should_flush = len(self._buffer) >= settings.flush_buffer_size

        if should_flush:
            self._flush_sync()

    def _flush_sync(self) -> None:
        with self._lock:
            if not self._buffer:
                return
            batch = list(self._buffer)
            self._buffer.clear()

        try:
            ParquetStore.write_events(batch)
        except Exception:  # noqa: BLE001
            logger.exception("Failed to flush %d events to Parquet", len(batch))

    async def start_periodic_flush(self) -> None:
        while True:
            await asyncio.sleep(settings.flush_interval_seconds)
            try:
                self._flush_sync()
            except Exception:  # noqa: BLE001
                logger.exception("Periodic flush failed")

    def flush_all(self) -> None:
        self._flush_sync()


buffer = _IngestionBuffer()
