from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import AsyncIterator, Mapping, Sequence

from aiohttp import web
from aiohttp.test_utils import TestServer


@dataclass(frozen=True)
class HttpResponse:
    status: int = 200
    body: bytes = b""
    headers: Mapping[str, str] = field(default_factory=dict)


@asynccontextmanager
async def serve_http(responses: Sequence[HttpResponse]) -> AsyncIterator[tuple[str, list[str]]]:
    pending = deque(responses)
    requests = []

    async def handle(request: web.Request) -> web.Response:
        requests.append(str(request.rel_url))
        if not pending:
            return web.Response(status=500, text="Unexpected request")

        response = pending.popleft()
        return web.Response(status=response.status, body=response.body, headers=response.headers)

    app = web.Application()
    app.router.add_route("*", "/{path:.*}", handle)
    server = TestServer(app)
    await server.start_server()
    try:
        yield str(server.make_url("/")).rstrip("/"), requests
    finally:
        await server.close()
