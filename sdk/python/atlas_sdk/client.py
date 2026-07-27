"""AtlasClient: a thin, notebook-friendly wrapper over the coordinator's REST
API (docs/atlas-implementation-spec.md's cross-cutting "CLI/SDK" note --
"Python SDK can be added once Phase 6 makes notebook-style NL querying worth
having a client for"). It does not talk to the catalog, workers, or AI
service directly -- every method here is a thin call to one coordinator
route, same boundary the dashboard's server-side proxy (dashboard/app/api/atlas)
and atlas-cli both already respect.
"""

from __future__ import annotations

import httpx

from .exceptions import AtlasError
from .models import QueryResult

DEFAULT_BASE_URL = "http://localhost:8080"


class AtlasClient:
    """A client bound to one coordinator instance and (optionally) one bearer
    token. Typical notebook usage::

        client = AtlasClient("http://localhost:8080")
        client.login("you@example.com", "your-password")
        df = client.query("patients", "SELECT diagnosis, COUNT(*) AS n FROM t GROUP BY diagnosis").to_pandas()

    A token minted elsewhere (dashboard signup, `tokengen`) can be passed
    directly instead of calling login/signup::

        client = AtlasClient("http://localhost:8080", token=os.environ["ATLAS_TOKEN"])
    """

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        *,
        token: str | None = None,
        timeout: float = 30.0,
        transport: httpx.BaseTransport | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self._client = httpx.Client(
            base_url=self.base_url, timeout=timeout, transport=transport
        )

    def __enter__(self) -> "AtlasClient":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    # -- auth -----------------------------------------------------------

    def signup(
        self, email: str, password: str, workspace_name: str | None = None
    ) -> str:
        """Create a user (and optionally a named workspace) and store the
        resulting bearer token on this client. Returns the token."""
        body: dict = {"email": email, "password": password}
        if workspace_name:
            body["workspace_name"] = workspace_name
        resp = self._request("POST", "/auth/signup", json=body, auth=False)
        self.token = resp["token"]
        return self.token

    def login(self, email: str, password: str) -> str:
        """Authenticate and store the resulting bearer token on this client.
        Returns the token."""
        resp = self._request(
            "POST",
            "/auth/login",
            json={"email": email, "password": password},
            auth=False,
        )
        self.token = resp["token"]
        return self.token

    # -- datasets ---------------------------------------------------------

    def list_datasets(self) -> list[dict]:
        return self._request("GET", "/datasets")

    def create_dataset(self, name: str, schema_json: str) -> dict:
        return self._request(
            "POST", "/datasets", json={"name": name, "schema_json": schema_json}
        )

    # -- querying -----------------------------------------------------------

    def query(self, dataset: str, sql: str) -> QueryResult:
        """Run SQL through optimize -> schedule -> distributed execute."""
        resp = self._request(
            "POST", "/query", json={"dataset": dataset, "sql": sql}
        )
        return QueryResult._from_json(resp)

    def query_nl(
        self, dataset: str, question: str, *, narrate: bool = False
    ) -> QueryResult:
        """Compile a natural-language question to the same LogicalPlan a SQL
        query would produce and run it through the unchanged execute path."""
        resp = self._request(
            "POST",
            "/query/nl",
            json={"dataset": dataset, "question": question, "narrate": narrate},
        )
        return QueryResult._from_json(resp)

    def explain(self, dataset: str, sql: str) -> dict:
        """Dry-run compile + prune: pre/post-optimization plan, manifests
        before/after pruning, and whether the equivalent /query call would
        hit the result cache. Dispatches no tasks."""
        return self._request(
            "POST", "/explain", json={"dataset": dataset, "sql": sql}
        )

    # -- AI analyst (Phase 7) ---------------------------------------------

    def summary(self, dataset: str) -> dict:
        """Row/column counts, null rates, distinct-count estimates -- pure
        engine output, no LLM call."""
        return self._request("POST", f"/datasets/{dataset}/summary")

    def insights(self, dataset: str) -> dict:
        """Summary + data-quality/outlier/trend findings, narrated in plain
        English, plus suggested example questions guaranteed to compile to a
        runnable plan."""
        return self._request("POST", f"/datasets/{dataset}/insights")

    # -- research (Phase 8) ------------------------------------------------

    def research(self, question: str, dataset: str, corpus_id: str = "") -> dict:
        """Run the Planner -> Execution -> Visualization -> Explanation ->
        Report pipeline. Returns {"report": str, "state": dict} -- report
        claims are tagged [data] or [literature:doc_id]."""
        body = {"question": question, "dataset": dataset}
        if corpus_id:
            body["corpus_id"] = corpus_id
        return self._request("POST", "/research", json=body)

    # -- history -------------------------------------------------------------

    def history(self) -> list[dict]:
        return self._request("GET", "/history")

    # -- internals -----------------------------------------------------------

    def _request(
        self, method: str, path: str, *, json: dict | None = None, auth: bool = True
    ) -> dict:
        headers = {}
        if auth:
            if not self.token:
                raise AtlasError(
                    401,
                    "no bearer token set -- call .login(...)/.signup(...) or "
                    "pass token= to AtlasClient(...) first",
                )
            headers["Authorization"] = f"Bearer {self.token}"

        response = self._client.request(method, path, json=json, headers=headers)
        if response.status_code >= 400:
            message = response.text
            try:
                message = response.json().get("error", message)
            except ValueError:
                pass
            raise AtlasError(response.status_code, message)
        return response.json()
