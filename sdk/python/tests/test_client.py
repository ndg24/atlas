import base64
import json

import httpx
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from atlas_sdk import AtlasClient, AtlasError


def _arrow_ipc_batch(table: pa.Table) -> str:
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return base64.b64encode(sink.getvalue().to_pybytes()).decode()


def _client(handler) -> AtlasClient:
    return AtlasClient(
        "http://coordinator.test",
        token="tok",
        transport=httpx.MockTransport(handler),
    )


def test_signup_stores_token():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/auth/signup"
        assert "Authorization" not in request.headers
        body = json.loads(request.content)
        assert body == {
            "email": "a@b.com",
            "password": "pw",
            "workspace_name": "acme",
        }
        return httpx.Response(201, json={"token": "minted-token"})

    client = AtlasClient(
        "http://coordinator.test", transport=httpx.MockTransport(handler)
    )
    token = client.signup("a@b.com", "pw", workspace_name="acme")
    assert token == "minted-token"
    assert client.token == "minted-token"


def test_login_stores_token():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/auth/login"
        return httpx.Response(200, json={"token": "logged-in-token"})

    client = AtlasClient(
        "http://coordinator.test", transport=httpx.MockTransport(handler)
    )
    token = client.login("a@b.com", "pw")
    assert token == "logged-in-token"


def test_request_without_token_raises_before_any_call():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json=[])

    client = AtlasClient(
        "http://coordinator.test", transport=httpx.MockTransport(handler)
    )
    with pytest.raises(AtlasError) as exc:
        client.list_datasets()
    assert exc.value.status_code == 401
    assert not calls


def test_list_datasets_sends_bearer_token():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer tok"
        assert request.method == "GET"
        assert request.url.path == "/datasets"
        return httpx.Response(200, json=[{"name": "patients"}])

    client = _client(handler)
    assert client.list_datasets() == [{"name": "patients"}]


def test_query_decodes_arrow_batches_to_table():
    table = pa.table({"diagnosis": ["flu", "cold"], "n": [3, 5]})
    batch_b64 = _arrow_ipc_batch(table)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/query"
        body = json.loads(request.content)
        assert body == {"dataset": "patients", "sql": "SELECT 1"}
        return httpx.Response(
            200,
            json={
                "query_id": "q1",
                "duration_ms": 12,
                "arrow_ipc_batches": [batch_b64],
                "cache_hit": False,
            },
        )

    client = _client(handler)
    result = client.query("patients", "SELECT 1")
    assert result.query_id == "q1"
    assert result.cache_hit is False
    assert result.to_table().to_pydict() == {
        "diagnosis": ["flu", "cold"],
        "n": [3, 5],
    }
    assert result.to_pandas()["n"].sum() == 8


def test_query_nl_passes_narrate_flag_and_exposes_explanation():
    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        assert body["narrate"] is True
        return httpx.Response(
            200,
            json={
                "query_id": "q2",
                "duration_ms": 5,
                "arrow_ipc_batches": [],
                "cache_hit": False,
                "raw_llm_output": '{"node":"aggregate"}',
                "explanation": "Flu is the most common diagnosis.",
            },
        )

    client = _client(handler)
    result = client.query_nl("patients", "most common diagnosis?", narrate=True)
    assert result.explanation == "Flu is the most common diagnosis."
    assert result.to_table().num_rows == 0


def test_error_response_raises_atlas_error_with_message():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": "dataset \"x\" has no committed snapshot yet"})

    client = _client(handler)
    with pytest.raises(AtlasError) as exc:
        client.explain("x", "SELECT 1")
    assert exc.value.status_code == 400
    assert "no committed snapshot" in exc.value.message


def test_insights_and_research_and_summary_routes():
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path))
        return httpx.Response(200, json={"ok": True})

    client = _client(handler)
    client.summary("patients")
    client.insights("patients")
    client.research("which hospital sees the most patients?", "patients", "papers")
    client.history()

    assert seen == [
        ("POST", "/datasets/patients/summary"),
        ("POST", "/datasets/patients/insights"),
        ("POST", "/research"),
        ("GET", "/history"),
    ]


def test_context_manager_closes_underlying_client():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    with _client(handler) as client:
        client.list_datasets()
    assert client._client.is_closed
