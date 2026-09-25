"""提供不依赖第三方框架的 HTTP/JSON 边界。"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .errors import DomainError, ValidationError
from .scheduling import SchedulingService
from .service import DomainService
from .storage import Database


def route(service: DomainService, method: str, path: str, body: dict[str, Any] | None,
          headers: dict[str, str] | None = None) -> tuple[int, dict[str, Any]]:
    """把一个 HTTP 语义请求分派到领域服务。"""

    headers = headers or {}
    body = body or {}
    parsed = urlparse(path)
    actor_id = headers.get("X-Actor-Id", "")
    try:
        if method == "GET" and parsed.path == "/health":
            valid, count = service.verify_audit()
            return 200, {"status": "ok", "audit_valid": valid, "audit_events": count}
        if method == "POST" and parsed.path == "/organizations":
            receipt = service.register_organization(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "POST" and parsed.path == "/actors":
            receipt = service.register_actor(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "POST" and parsed.path == "/sites":
            receipt = service.register_site(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "POST" and parsed.path == "/domain-records":
            receipt = service.record_domain_data(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "GET" and parsed.path == "/domain-records":
            query = parse_qs(parsed.query)
            site_id = query.get("site_id", [""])[0]
            if not site_id:
                raise ValidationError("site_id 不能为空")
            category = query.get("category", [None])[0]
            return 200, {"items": [item.__dict__ for item in service.list_domain_data(site_id, category)]}
        if method == "GET" and parsed.path == "/audit-events":
            query = parse_qs(parsed.query)
            after = int(query.get("after_sequence", ["0"])[0])
            return 200, {"items": service.audit_events(after)}
        return 404, {"error": "route_not_found", "message": "接口不存在"}
    except DomainError as exc:
        return exc.status, {"error": exc.code, "message": str(exc)}
    except (TypeError, ValueError) as exc:
        return 400, {"error": "invalid_request", "message": str(exc)}


def scheduling_route(service: SchedulingService, method: str, path: str,
                     body: dict[str, Any] | None,
                     headers: dict[str, str] | None = None) -> tuple[int, dict[str, Any]]:
    """把训练排程相关的 HTTP 语义请求分派到排程领域服务。"""

    headers = headers or {}
    body = body or {}
    parsed = urlparse(path)
    actor_id = headers.get("X-Actor-Id", "")
    try:
        if method == "POST" and parsed.path == "/scheduling/templates":
            return _created(service.create_template(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/scheduling/qualifications":
            return _created(service.grant_qualification(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/scheduling/areas":
            return _created(service.register_area(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/scheduling/areas/status":
            return _created(service.set_area_status(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/scheduling/equipment":
            return _created(service.register_equipment(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/scheduling/equipment/status":
            return _created(service.set_equipment_status(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/scheduling/restrictions":
            return _created(service.create_restriction(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/scheduling/restrictions/escalate":
            return _created(service.escalate_restriction(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/scheduling/restrictions/lift":
            return _created(service.lift_restriction(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/scheduling/plans":
            return _created(service.create_plan(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/scheduling/plans/regenerate":
            return _created(service.regenerate_plan(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/scheduling/releases":
            return _created(service.release_plan(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/scheduling/task-events":
            return _created(service.record_task_event(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/scheduling/dispositions/resolve":
            return _created(service.resolve_disposition(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/scheduling/reviews/complete":
            return _created(service.complete_review(actor_id=actor_id, **body))
        if method == "GET" and parsed.path == "/scheduling/plan":
            query = parse_qs(parsed.query)
            plan_id = query.get("plan_id", [""])[0]
            if not plan_id:
                raise ValidationError("plan_id 不能为空")
            return 200, service.get_plan(plan_id)
        if method == "GET" and parsed.path == "/scheduling/task":
            query = parse_qs(parsed.query)
            slot_id = query.get("slot_id", [""])[0]
            if not slot_id:
                raise ValidationError("slot_id 不能为空")
            return 200, service.get_task(slot_id)
        if method == "GET" and parsed.path == "/scheduling/reviews":
            query = parse_qs(parsed.query)
            site_id = query.get("site_id", [""])[0]
            if not site_id:
                raise ValidationError("site_id 不能为空")
            return 200, {"items": service.list_reviews(site_id, query.get("status", [None])[0])}
        if method == "GET" and parsed.path == "/scheduling/dispositions":
            query = parse_qs(parsed.query)
            site_id = query.get("site_id", [""])[0]
            if not site_id:
                raise ValidationError("site_id 不能为空")
            return 200, {"items": service.list_dispositions(site_id, query.get("status", [None])[0])}
        if method == "GET" and parsed.path == "/scheduling/resources":
            query = parse_qs(parsed.query)
            site_id = query.get("site_id", [""])[0]
            if not site_id:
                raise ValidationError("site_id 不能为空")
            return 200, service.get_resources(site_id)
        return 404, {"error": "route_not_found", "message": "接口不存在"}
    except DomainError as exc:
        return exc.status, {"error": exc.code, "message": str(exc)}
    except (TypeError, ValueError) as exc:
        return 400, {"error": "invalid_request", "message": str(exc)}


def _created(result: tuple[dict[str, Any], bool]) -> tuple[int, dict[str, Any]]:
    """把 (响应, 是否重放) 转换为带 replayed 标记的 HTTP 结果。"""

    response, replayed = result
    return (200 if replayed else 201), {**response, "replayed": replayed}


class Handler(BaseHTTPRequestHandler):
    """把标准库 HTTP 请求转换为路由调用。"""

    service: DomainService
    scheduling: SchedulingService

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._write(400, {"error": "invalid_json", "message": "请求体必须是 UTF-8 JSON"})
            return
        headers = {"X-Actor-Id": self.headers.get("X-Actor-Id", "")}
        if self.path.startswith("/scheduling"):
            status, payload = scheduling_route(self.scheduling, self.command, self.path, body, headers)
        else:
            status, payload = route(self.service, self.command, self.path, body, headers)
        self._write(status, payload)

    def _write(self, status: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> int:
    """启动本地 HTTP 服务。"""

    parser = argparse.ArgumentParser(description="启动技能赛训协作基础服务")
    parser.add_argument("--database", default="service.sqlite3")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    database = Database(args.database)
    Handler.service = DomainService(database)
    Handler.scheduling = SchedulingService(database, domain=Handler.service)
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        database.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
