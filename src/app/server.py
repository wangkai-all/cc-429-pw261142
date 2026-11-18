"""标准库 HTTP 服务：JSON API + 单页网页，启动时自动恢复中断作业。"""
from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from . import pipeline
from .services import Service
from .store import Store

WEB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "web")
STATIC = {"/": ("index.html", "text/html; charset=utf-8"),
          "/index.html": ("index.html", "text/html; charset=utf-8"),
          "/app.js": ("app.js", "application/javascript; charset=utf-8"),
          "/styles.css": ("styles.css", "text/css; charset=utf-8")}


def make_handler(store: Store):
    svc = Service(store)

    class Handler(BaseHTTPRequestHandler):
        server_version = "CaliberPipeline/1.0"

        def log_message(self, fmt, *args):  # 安静一点
            pass

        # --- 基础工具 -----------------------------------------------------------
        def _send(self, code, obj, ctype="application/json; charset=utf-8"):
            data = obj if isinstance(obj, bytes) else json.dumps(
                obj, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _body(self):
            n = int(self.headers.get("Content-Length", 0))
            if n == 0:
                return {}
            raw = self.rfile.read(n)
            try:
                return json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise ValueError(f"请求体不是合法 JSON: {exc}")

        def _handle(self, fn):
            try:
                self._send(200, fn())
            except KeyError as exc:
                self._send(404, {"error": f"不存在: {exc.args[0]}"})
            except ValueError as exc:
                self._send(400, {"error": str(exc)})
            except Exception as exc:  # noqa: BLE001 - 服务不能因单请求崩溃
                self._send(500, {"error": repr(exc)})

        # --- 路由 ---------------------------------------------------------------
        def do_GET(self):
            parsed = urlparse(self.path)
            path, qs = parsed.path, parse_qs(parsed.query)
            if path in STATIC:
                return self._static(path)
            routes = {
                "/api/overview": lambda: svc.overview(),
                "/api/mapping": lambda: svc.get_active_mapping(),
                "/api/mapping/edit": lambda: svc.clone_active_mapping(),
                "/api/reports": lambda: svc.overview()["reports"],
            }
            if path in routes:
                return self._handle(routes[path])
            if path.startswith("/api/mapping/"):
                return self._handle(lambda: svc.mapping_version(path.rsplit("/", 1)[1]))
            if path == "/api/job":
                return self._handle(lambda: svc.job_detail(qs["id"][0]))
            if path == "/api/report":
                return self._handle(lambda: svc.report(qs["id"][0]))
            if path == "/api/incompat":
                jid = qs.get("id", [None])[0]
                return self._handle(lambda: svc.incompatibilities(jid))
            if path == "/api/compare":
                return self._handle(lambda: svc.compare(qs["a"][0], qs["b"][0]))
            self._send(404, {"error": "not found"})

        def do_POST(self):
            path = urlparse(self.path).path
            if path == "/api/ingest":
                def op():
                    body = self._body()
                    payload = body.get("payload")
                    if payload is None and body.get("sample"):
                        payload = svc.load_demo_payload(body["sample"])
                    crash_file = None
                    if body.get("crash"):
                        crash_file = pipeline.DEFAULT_CRASH_FILE
                        os.makedirs(os.path.dirname(crash_file), exist_ok=True)
                        with open(crash_file, "w", encoding="utf-8") as f:
                            f.write("scope")  # 在 scope 阶段前中断一次
                    return svc.ingest_and_run(
                        payload, body.get("note", ""),
                        body.get("include_previous"), crash_file=crash_file)
                return self._handle(op)
            if path == "/api/resume":
                def op():
                    body = self._body()
                    if body.get("id"):
                        return {"job": svc.resume(body["id"])}
                    return {"jobs": svc.resume_any()}
                return self._handle(op)
            if path == "/api/arm-crash":
                def op():
                    os.makedirs(os.path.dirname(pipeline.DEFAULT_CRASH_FILE), exist_ok=True)
                    with open(pipeline.DEFAULT_CRASH_FILE, "w", encoding="utf-8") as f:
                        f.write("1")
                    return {"armed": True, "file": pipeline.DEFAULT_CRASH_FILE}
                return self._handle(op)
            self._send(404, {"error": "not found"})

        def do_PUT(self):
            path = urlparse(self.path).path
            if path == "/api/mapping":
                def op():
                    body = self._body()
                    return svc.update_mapping(body["snapshot"], body.get("note", ""))
                return self._handle(op)
            self._send(404, {"error": "not found"})

        def _static(self, path):
            fname, ctype = STATIC[path]
            try:
                with open(os.path.join(WEB_DIR, fname), "rb") as f:
                    self._send(200, f.read(), ctype)
            except FileNotFoundError:
                self._send(404, b"not found", "text/plain")

    return Handler


def serve(db_path: str, host: str = "127.0.0.1", port: int = 8080, open_browser: bool = False):
    store = Store(db_path)
    # 启动即沿原输入版恢复上次中断的作业
    for j in store.resumable_jobs():
        pipeline.resume_job(store, j["job_id"])
    httpd = ThreadingHTTPServer((host, port), make_handler(store))
    print(f"服务已启动: http://{host}:{port}  数据库: {db_path}")
    if open_browser:
        import webbrowser
        webbrowser.open(f"http://{host}:{port}/")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        store.close()
