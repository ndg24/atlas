# atlas-sdk

A thin Python client over the coordinator's REST API — the notebook-style
counterpart to `atlas-cli`, for querying by SQL or natural language, pulling
insights, and running research reports without shelling out.

It talks to exactly one thing: the coordinator's REST API (default
`http://localhost:8080`). It never touches the catalog, workers, or AI
service directly, and it never decodes anything the coordinator itself
doesn't already hand back — Arrow IPC batches are decoded into
`pyarrow.Table`/`pandas.DataFrame` client-side, since that's the one piece of
work a REST client actually needs to do that a browser client (the
dashboard) doesn't.

## Install

```
cd sdk/python
uv sync   # or: pip install -e .
```

## Usage

```python
from atlas_sdk import AtlasClient

client = AtlasClient("http://localhost:8080")
client.login("you@example.com", "your-password")
# -- or, for a fresh workspace: client.signup("you@example.com", "pw", workspace_name="acme")
# -- or, to reuse a token minted elsewhere: AtlasClient(token=os.environ["ATLAS_TOKEN"])

# SQL, through optimize -> schedule -> distributed execute
df = client.query("patients", "SELECT diagnosis, COUNT(*) AS n FROM t GROUP BY diagnosis ORDER BY n DESC").to_pandas()

# natural language -- same execution path, plus an optional narration
result = client.query_nl("patients", "which diagnosis is most common?", narrate=True)
print(result.explanation)

# pre/post-optimization plan and pruning, no tasks dispatched
plan = client.explain("patients", "SELECT * FROM t WHERE year = 2024")

# pure-engine summary, then summary + narrated findings + suggested questions
summary = client.summary("patients")
insights = client.insights("patients")

# multi-agent research report -- claims tagged [data] / [literature:doc_id]
report = client.research("which hospital sees the most patients?", "patients", corpus_id="papers")
print(report["report"])

client.list_datasets()
client.history()
```

`AtlasClient` is also a context manager (`with AtlasClient(...) as client:`)
so the underlying HTTP connection pool is closed deterministically in
scripts that don't run as a long-lived notebook kernel.

## Testing

```
uv sync
uv run pytest
```

Tests run entirely against `httpx.MockTransport` (see `tests/test_client.py`)
— no live coordinator required.
