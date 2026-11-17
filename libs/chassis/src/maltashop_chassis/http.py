"""FastAPI middleware: correlation IDs and one structured log line per request."""

from __future__ import annotations

import logging
import time

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from .logging import correlation_id_var, new_correlation_id

CORRELATION_HEADER = "X-Correlation-ID"
log = logging.getLogger("http")


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Reuse the caller's X-Correlation-ID (set by the API gateway in
    production) or mint a new one, and echo it back on the response."""

    async def dispatch(self, request: Request, call_next) -> Response:
        cid = request.headers.get(CORRELATION_HEADER) or new_correlation_id()
        token = correlation_id_var.set(cid)
        started = time.perf_counter()
        try:
            response = await call_next(request)
        finally:
            correlation_id_var.reset(token)
        response.headers[CORRELATION_HEADER] = cid
        if not request.url.path.startswith("/health"):
            correlation_id_var.set(cid)
            log.info(
                "%s %s -> %s",
                request.method,
                request.url.path,
                response.status_code,
                extra={
                    "event": "http.request",
                    "method": request.method,
                    "path": request.url.path,
                    "status": response.status_code,
                    "duration_ms": round((time.perf_counter() - started) * 1000, 1),
                },
            )
            correlation_id_var.set("-")
        return response
