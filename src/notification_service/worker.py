import asyncio
import json
import logging
import signal
import sqlite3
import time

import httpx

from .config import Settings
from .delivery import deliver, retry_delay
from .storage import Repository

logger = logging.getLogger("notification_service")


class Worker:
    def __init__(self, settings: Settings, transport=None):
        self.settings = settings
        self.repo = Repository(settings)
        self.transport = transport

    async def process(self, client, task):
        try:
            result = await deliver(client, task, self.settings)
            delay = retry_delay(task["attempt_count"], self.settings.retry_base, result.retry_after) if result.retryable else None
            saved = await asyncio.to_thread(self.repo.finish, task, result, delay)
            logger.info(json.dumps({"event": "delivery_finished", "notification_id": task["id"],
                                    "attempt_number": task["attempt_count"], "outcome": "succeeded" if result.success else result.code,
                                    "http_status": result.http_status, "duration_ms": result.duration_ms, "saved": saved}))
        except asyncio.CancelledError:
            raise
        except Exception:
            # No raw exceptions: they may contain URL query parameters or credentials.
            logger.error(json.dumps({"event": "delivery_processing_error", "notification_id": task["id"],
                                     "attempt_number": task["attempt_count"]}))
            # The persisted lease, rather than an in-memory retry, owns recovery.

    async def run(self, stop: asyncio.Event):
        if not await asyncio.to_thread(self.repo.ready):
            raise RuntimeError("Initialize the database before starting Worker")
        logger.info('{"event":"worker_started"}')
        active = set()
        heartbeat = 0
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(self.settings.request_timeout, connect=self.settings.connect_timeout),
            limits=httpx.Limits(max_connections=self.settings.concurrency, max_keepalive_connections=self.settings.concurrency),
            trust_env=False, follow_redirects=False, transport=self.transport,
        ) as client:
            try:
                while not stop.is_set():
                    active = {t for t in active if not t.done()}
                    try:
                        count = await asyncio.to_thread(self.repo.recover)
                        if count:
                            logger.info(json.dumps({"event": "leases_recovered", "count": count}))
                        while len(active) < self.settings.concurrency and not stop.is_set():
                            task = await asyncio.to_thread(self.repo.claim)
                            if task is None:
                                break
                            active.add(asyncio.create_task(self.process(client, task)))
                    except sqlite3.Error:
                        logger.warning('{"event":"worker_database_unavailable"}')
                    if time.monotonic() >= heartbeat:
                        logger.info(json.dumps({"event": "worker_heartbeat", "active": len(active)}))
                        heartbeat = time.monotonic() + 30
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=self.settings.poll_seconds)
                    except TimeoutError:
                        pass
            finally:
                if active:
                    _, pending = await asyncio.wait(active, timeout=self.settings.request_timeout + self.settings.db_busy_seconds)
                    for task in pending:
                        task.cancel()
                    await asyncio.gather(*active, return_exceptions=True)
                logger.info('{"event":"worker_stopped"}')


async def async_main():
    settings = Settings.from_env()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))
    await Worker(settings).run(stop)


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
