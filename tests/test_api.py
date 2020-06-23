import os
import uuid

from fastapi.testclient import TestClient

import dcollect.main as main

os.environ["REDIS_URL"] = "__unittest__"
os.environ["NO_SUBSCRIBE"] = "1"

client = TestClient(main.app)


def rand_entity():
    inst = uuid.uuid4().hex
    return f"/entity-ptr/{inst}"


def read_bytes(r):
    bfr = b""
    for chunk in r.iter_content(chunk_size=128):
        bfr += chunk
    return bfr


def get_json(url, required_status=200):
    return get(url, required_status).json()


def get(url, required_status=200):
    res = client.get(url)
    assert res.status_code == required_status
    return res


def post(url, data, required_status=200):
    res = client.post(url, data=data)
    assert res.status_code == required_status
    return res


def test_ingest():
    input_data = b"foobar"
    entity = rand_entity()

    data1 = post(entity, input_data).json()
    assert data1.get("version") is not None, "Version must be set"
    assert data1.get("pointer") is not None, "Pointer must be set"
    data2 = post(entity, input_data).json()

    assert data1["pointer"] == data2["pointer"], "Must be stable"
    read_data = read_bytes(get(entity))
    assert read_data == input_data, "We can read what we ingest"
    history_ent = entity.replace("entity-ptr", "entity")
    read_history = get_json(f"{history_ent}/history")
    assert len(read_history.get("history", [])) == 2
    p1 = read_history["history"][0].get("pointer")
    p2 = read_history["history"][1].get("pointer")
    assert p1 is not None and p2 is not None
    assert p1 == p2, "Pointers should be the same"


def test_ingest_changes():
    input_data1 = b"abc"
    input_data2 = b"def"
    entity = rand_entity()
    post(entity, input_data1)

    saved_1 = read_bytes(get(entity))
    assert saved_1 == input_data1

    post(entity, input_data2)
    saved_2 = read_bytes(get(entity))
    assert saved_2 == input_data2

    entity = entity.replace("entity-ptr", "entity")
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
