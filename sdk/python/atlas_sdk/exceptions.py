"""Exceptions raised by AtlasClient."""


class AtlasError(Exception):
    """An error response from the coordinator's REST API.

    Every coordinator route reports failures as ``{"error": "<message>"}``
    (coordinator/internal/api/server.go's writeError) -- this mirrors that
    shape rather than surfacing a raw httpx.HTTPStatusError.
    """

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"atlas API error ({status_code}): {message}")
