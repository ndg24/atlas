"""Response wrappers for AtlasClient.

QueryResult is the one type here that needs real logic: /query and
/query/nl (coordinator/internal/api/server.go) return each result batch as
a base64-encoded, self-contained Arrow IPC stream (queryResponse's own doc
comment: "the coordinator never decodes the Arrow IPC bytes flowing through
it"), so decoding into something a notebook can plot or feed to pandas is
squarely the SDK's job, not the coordinator's.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass, field


@dataclass
class QueryResult:
    query_id: str
    duration_ms: int
    cache_hit: bool
    arrow_ipc_batches: list[bytes] = field(default_factory=list)
    raw_llm_output: str | None = None
    explanation: str | None = None

    @classmethod
    def _from_json(cls, body: dict) -> "QueryResult":
        return cls(
            query_id=body["query_id"],
            duration_ms=body["duration_ms"],
            cache_hit=body["cache_hit"],
            arrow_ipc_batches=[
                base64.b64decode(b) for b in body.get("arrow_ipc_batches", [])
            ],
            raw_llm_output=body.get("raw_llm_output"),
            explanation=body.get("explanation") or None,
        )

    def to_table(self):
        """Decode every batch and concatenate into one pyarrow.Table."""
        import pyarrow as pa
        import pyarrow.ipc as ipc

        if not self.arrow_ipc_batches:
            return pa.table({})
        tables = [ipc.open_stream(b).read_all() for b in self.arrow_ipc_batches]
        return tables[0] if len(tables) == 1 else pa.concat_tables(tables)

    def to_pandas(self):
        """Decode every batch and concatenate into one pandas.DataFrame."""
        return self.to_table().to_pandas()
