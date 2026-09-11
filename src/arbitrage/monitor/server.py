import asyncio
import json
import secrets
import sqlite3
from importlib.resources import files
from uuid import UUID

from aiohttp import web

from arbitrage.config import Settings
from arbitrage.monitor.controller import MonitorController
from arbitrage.observability import dumps, log_event


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
        types = {
            "index.html": "text/html",
            "style.css": "text/css",
            "app.js": "text/javascript",
            "workbench.js": "text/javascript",
        }
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

    async def legacy_control(request):
        return web.json_response(
            {"error": "全局自动挂单已停用，请刷新页面并手动创建条件单"}, status=410
        )

    async def condition_control(request):
        try:
            action = request.match_info.get("action", "create")
            payload = (
                await request.json() if action == "create" else str(UUID(request.match_info["id"]))
            )
            result = await controller.condition_command(action, payload)
        except (ValueError, TypeError, AttributeError) as exc:
            return web.json_response({"error": str(exc)}, status=400)
        except TimeoutError:
            return web.json_response(
                {"error": "请求仍在处理，请查看条件单列表后使用相同编号重试"}, status=504
            )
        return web.json_response(json.loads(dumps(result)))

    async def cleanup(app):
        await controller.close()

    async def close_all(request):
        try:
            result = await controller.condition_command("close_all", None)
            return web.json_response(result)
        except (ValueError, TimeoutError) as exc:
            return web.json_response({"error": str(exc)}, status=400)

    app.router.add_get("/", asset)
    app.router.add_get("/assets/{name}", asset)
    app.router.add_get("/api/status", status)
    app.router.add_get("/api/history", history)
    app.router.add_post("/api/direction", legacy_control)
    app.router.add_post("/api/placement", legacy_control)
    app.router.add_post("/api/conditions", condition_control)
    app.router.add_post("/api/close-all", close_all)
    app.router.add_post(
        "/api/conditions/{id}/{action:cancel|resume|pause|close}", condition_control
    )
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
        log_event("dashboard_ready", url=f"http://127.0.0.1:{port}", mode=settings.mode)
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()
