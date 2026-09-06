import argparse
import json
import os
import time
import uuid

import httpx

parser = argparse.ArgumentParser()
parser.add_argument("--mode", choices=["success", "flaky", "bad", "slow"], default="flaky")
parser.add_argument("--key", default=None, help="Reuse a key to demonstrate submission deduplication")
parser.add_argument("--api", default="http://127.0.0.1:8000")
args = parser.parse_args()
key = args.key or str(uuid.uuid4())
with httpx.Client(headers={"Authorization": f"Bearer {os.environ['INTERNAL_API_TOKEN']}"}, timeout=10) as client:
    response = client.post(args.api + "/v1/notifications", headers={"Idempotency-Key": key}, json={
        "url": f"http://127.0.0.1:9001/{args.mode}", "method": "POST",
        "headers": {"Content-Type": "application/json", "X-Demo-Event": key},
        "body": json.dumps({"event": "subscription_paid", "contact_id": "c_123"}),
    })
    response.raise_for_status()
    print("Accepted:", response.json())
    location = args.api + response.headers["location"]
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        response = client.get(location)
        response.raise_for_status()
        task = response.json()
        print(task["status"], "attempts:", task["attempt_count"])
        if task["status"] in {"succeeded", "failed"}:
            print(json.dumps(client.get(location + "/attempts").json(), indent=2))
            break
        time.sleep(1)
    else:
        raise SystemExit("Still pending after 180 seconds; query the task later")
