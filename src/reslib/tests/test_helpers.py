from datetime import datetime, timezone

import httpx
import pytest

from reslib.helpers import NoopTelemetry, send_timed_request, utc_now_iso


@pytest.mark.asyncio
async def test_send_timed_request_returns_timed_response():
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(transport=transport) as client:
        timed_response = await send_timed_request(client, "https://example.com/health")

    assert timed_response.response.status_code == 200
    assert timed_response.response.json() == {"ok": True}
    assert timed_response.latency >= 0
    assert timed_response.timestamp.tzinfo == timezone.utc


@pytest.mark.asyncio
async def test_send_timed_request_propagates_http_errors():
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, request=request)

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await send_timed_request(client, "https://example.com/health")


def test_utc_now_iso_returns_utc_timestamp():
    timestamp = utc_now_iso()
    parsed = datetime.fromisoformat(timestamp)

    assert parsed.tzinfo == timezone.utc


def test_noop_telemetry_discards_payloads():
    telemetry = NoopTelemetry()

    assert telemetry.emit_event(event=object()) is None
    assert telemetry.emit_metrics(metrics=object()) is None
