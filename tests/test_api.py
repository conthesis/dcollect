from fastapi.testclient import TestClient
import main

client = TestClient(main.app)


def test_healthz():
    res = client.get("/healthz")
    assert res.status_code == 200

def test_readyz():
    res = client.get("/readyz")
    assert res.status_code == 200    
