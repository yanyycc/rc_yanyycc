"""Local development supplier. Never use this unauthenticated app in production."""
import asyncio
from collections import defaultdict
from fastapi import FastAPI, Request
from fastapi.responses import Response

app = FastAPI()
counts = defaultdict(int)


@app.api_route("/{mode}", methods=["POST", "PUT", "PATCH", "DELETE"])
async def supplier(mode: str, request: Request):
    await request.body()
    event = request.headers.get("x-demo-event", "default")
    counts[(mode, event)] += 1
    if mode == "flaky" and counts[(mode, event)] <= 2:
        return Response(status_code=503)
    if mode == "bad":
        return Response(status_code=400)
    if mode == "slow":
        await asyncio.sleep(15)
    if mode not in {"success", "flaky", "bad", "slow"}:
        return Response(status_code=404)
    return Response(status_code=204)
