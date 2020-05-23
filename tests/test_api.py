import uuid
from fastapi.testclient import TestClient
import main

client = TestClient(main.app)


def rand_entity():
    inst = uuid.uuid4().hex
    return f"/entity/{inst}"


def get_json(url, required_status=200):
    res = client.get(url)
    assert res.status_code == required_status
    return res.json()


def post_json(url, json, required_status=200):
    res = client.post(url, json=json)
    assert res.status_code == required_status
    return res.json()


def test_ingest():
    input_data = {"a": 1}
    entity = rand_entity()

    data1 = post_json(entity, input_data)
    assert data1.get("version") is not None, "Version must be set"
    assert data1.get("pointer") is not None, "Pointer must be set"
    data2 = post_json(entity, json=input_data)

    assert data1["pointer"] == data2["pointer"], "Must be stable"
    read_data = get_json(entity)
    assert read_data == input_data, "We can read what we ingest"
    read_history = get_json(f"{entity}/history")
    assert len(read_history.get("history", [])) == 2
    p1 = read_history["history"][0].get("pointer")
    p2 = read_history["history"][1].get("pointer")
    assert p1 is not None and p2 is not None
    assert p1 == p2, "Pointers should be the same"

def test_ingest_changes():
    input_data1 = { "a": 1 }
    input_data2 = { "a": 2 }
    entity = rand_entity()
    post_json(entity, input_data1)

    saved_1 = get_json(entity)
    assert saved_1 == input_data1

    post_json(entity, input_data2)
    saved_2 = get_json(entity)
    assert saved_2 == input_data2

    read_history = get_json(f"{entity}/history")
    assert len(read_history.get("history", [])) == 2
    p1 = read_history["history"][0].get("pointer")
    p2 = read_history["history"][1].get("pointer")
    assert p1 is not None and p2 is not None
    assert p1 != p2, "Pointers should not be the same"


def test_healthz():
    res = client.get("/healthz")
    assert res.status_code == 200


def test_readyz():
    res = client.get("/readyz")
    assert res.status_code == 200
