import asyncio
import json
import secrets
import sqlite3
from importlib.resources import files

from aiohttp import web

from arbitrage.config import Settings
from arbitrage.monitor.controller import MonitorController
from arbitrage.observability import log_event


def read_history(path) -> dict:
    if not path.exists():
        return {"orders": []}
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=2) as db:
        exists = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='maker_orders'"
        ).fetchone()
        if not exists:
            return {"orders": []}
        orders = [
            json.loads(row[0])
            for row in db.execute("SELECT payload FROM maker_orders ORDER BY rowid DESC LIMIT 50")
        ]
    return {"orders": orders}


def create_app(controller: MonitorController) -> web.Application:
    token = secrets.token_urlsafe(32)

    @web.middleware
    async def local_only(request, handler):
        if request.host.split(":", 1)[0] not in {"127.0.0.1", "localhost"}:
            raise web.HTTPForbidden(text="Local access only")
        if request.method == "POST":
            origin = request.headers.get("Origin")
            if origin and origin != f"http://{request.host}":
                raise web.HTTPForbidden(text="Cross-origin control is not allowed")
            if not secrets.compare_digest(request.headers.get("X-Control-Token", ""), token):
                raise web.HTTPForbidden(text="Missing control token")
        response = await handler(request)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'"
        )
        return response

    app = web.Application(middlewares=[local_only], client_max_size=1024)

    async def asset(request):
        name = request.match_info.get("name", "index.html")
        types = {"index.html": "text/html", "style.css": "text/css", "app.js": "text/javascript"}
        if name not in types:
            raise web.HTTPNotFound()
        content = files("arbitrage.monitor").joinpath("static", name).read_text(encoding="utf-8")
        return web.Response(text=content, content_type=types[name])

    async def status(request):
        return web.json_response({**controller.snapshot(), "control_token": token})

    async def control(request):
        if request.match_info["action"] == "start":
            controller.start()
        else:
            controller.stop()
        return web.json_response({"status": controller.status})

    async def history(request):
        try:
            result = await asyncio.to_thread(read_history, controller.settings.database.path)
        except (sqlite3.Error, ValueError) as exc:
            return web.json_response(
                {"orders": [], "error": f"History unavailable: {exc}"}, status=503
            )
        return web.json_response(result)

    async def direction(request):
        try:
            payload = await request.json()
            if not isinstance(payload, dict) or set(payload) != {"mode"}:
                raise ValueError("Expected mode only")
            controller.select_direction(payload["mode"])
        except (ValueError, TypeError):
            return web.json_response({"error": "mode must be a, b or both"}, status=400)
        return web.json_response(controller.snapshot()["entry_selection"])

    async def placement(request):
        try:
            payload = await request.json()
            if not isinstance(payload, dict) or set(payload) != {"mode", "request_id"}:
                raise ValueError("Expected mode and request_id")
            if not isinstance(payload["request_id"], str):
                raise ValueError("request_id must be a UUID string")
            controller.control_placement(payload["mode"], payload["request_id"])
        except (ValueError, TypeError, AttributeError):
            return web.json_response({"error": "Invalid placement mode or request_id"}, status=400)
        except RuntimeError as exc:
            return web.json_response({"error": str(exc)}, status=409)
        return web.json_response(controller.snapshot()["placement"])

    async def cleanup(app):
        await controller.close()

    app.router.add_get("/", asset)
    app.router.add_get("/assets/{name}", asset)
    app.router.add_get("/api/status", status)
    app.router.add_get("/api/history", history)
    app.router.add_post("/api/direction", direction)
    app.router.add_post("/api/placement", placement)
    app.router.add_post("/api/{action:start|stop}", control)
    app.on_cleanup.append(cleanup)
    return app


async def run_dashboard(settings: Settings, *, port: int = 8765) -> None:
    controller = MonitorController(settings)
    runner = web.AppRunner(create_app(controller), access_log=None)
    try:
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", port)
        await site.start()
        log_event("dashboard_ready", url=f"http://127.0.0.1:{port}", mode="paper")
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()
