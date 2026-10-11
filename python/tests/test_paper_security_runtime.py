"""Real API workers and shared Redis limits; account authority stays in PostgreSQL."""
from __future__ import annotations
import hashlib
import json
import os
import subprocess
import sys
import uuid

import httpx
import pytest
from redis import Redis
from test_autonomous_paper import ROOT, TOKEN, sql, wait

pytestmark = pytest.mark.skipif(os.getenv("ARB_RUN_PAPER_INTEGRATION") != "1", reason="requires real PostgreSQL/Redis")


@pytest.fixture(scope="module")
def workers(tmp_path_factory):
    directory = tmp_path_factory.mktemp("security-api")
    account = "security-"+uuid.uuid4().hex[:12]
    viewer, operator = "viewer-"+uuid.uuid4().hex, "operator-"+uuid.uuid4().hex
    grants = {hashlib.sha256(token.encode()).hexdigest(): {"subject": role, "role": role, "accounts": [account]}
              for token, role in ((viewer, "viewer"), (operator, "operator"))}
    env = dict(os.environ, ARB_CONTROL_API_TOKEN=TOKEN, ARB_PAPER_API_KEYS_JSON=json.dumps(grants),
               ARB_API_RATE_LIMIT_PER_MINUTE="20", ARB_API_MAX_BODY_BYTES="1024")
    processes, logs, clients = [], [], []
    try:
        for port in (58084, 58085):
            log = (directory/f"api-{port}.log").open("w")
            logs.append(log)
            process = subprocess.Popen([sys.executable, "-m", "uvicorn", "api.main:app", "--host", "127.0.0.1", "--port", str(port)], cwd=ROOT/"python", env=env, stdout=log, stderr=subprocess.STDOUT)
            processes.append(process)
            client = httpx.Client(base_url=f"http://127.0.0.1:{port}", trust_env=False)
            clients.append(client)
            wait(lambda: client.get("/livez").status_code == 200)
        response = clients[0].post("/paper/accounts", headers={"Authorization": "Bearer "+TOKEN},
                                   json={"account_id": account, "amount": "500", "request_id": uuid.uuid4().hex})
        assert response.status_code == 200, response.text
        yield clients, account, viewer, operator
    finally:
        for client in clients:
            client.close()
        for process in processes:
            process.terminate()
            process.wait(timeout=10)
        for log in logs:
            log.close()


def test_real_account_authorization_and_stop(workers):
    clients, account, viewer, operator = workers
    client = clients[0]
    viewer_headers, operator_headers = ({"Authorization": "Bearer "+t} for t in (viewer, operator))
    own = client.get(f"/paper/{account}/status", headers=viewer_headers)
    assert own.status_code == 200 and own.json()["can_control"] is False
    assert client.get("/paper/foreign/status", headers=viewer_headers).status_code == 403
    payload = {"request_id": uuid.uuid4().hex, "reason": "scoped paper security test"}
    assert client.post(f"/paper/{account}/control/start", headers=viewer_headers, json=payload).status_code == 403
    assert client.post(f"/paper/{account}/control/stop", headers=operator_headers, json=payload).status_code == 200
    assert client.post("/paper/accounts", headers=operator_headers, json={"account_id": "forged", "amount": "1", "request_id": uuid.uuid4().hex}).status_code == 403
    assert sql("SELECT available,reserved FROM paper_balances WHERE account_id=%s", (account,))[0] == (500, 0)


def test_real_workers_share_atomic_request_quota(workers):
    clients, account, viewer, operator = workers
    key = "arb.api.rate:"+hashlib.sha256(("credential:"+viewer).encode()).hexdigest()
    redis = Redis.from_url(os.environ["ARB_REDIS_URL"])
    redis.delete(key)  # This fixture owns this randomly generated credential quota.
    headers = {"Authorization": "Bearer "+viewer}
    for i in range(20):
        assert clients[i % 2].get(f"/paper/{account}/status", headers=headers).status_code == 200
    response = clients[1].get(f"/paper/{account}/status", headers=headers)
    assert response.status_code == 429 and response.headers["retry-after"] == "60"
    assert redis.get(key) == b"21" and 0 < redis.ttl(key) <= 60
    payload = {"request_id": uuid.uuid4().hex, "reason": "stop remains available"}
    assert clients[0].post(f"/paper/{account}/control/stop", headers={"Authorization": "Bearer "+operator}, json=payload).status_code == 200
    redis.delete(key)
    redis.close()
