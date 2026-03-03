"""Alarm Judge runtime: FastAPI + MQTT + alarm evaluation."""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import paho.mqtt.client as mqtt
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.config import AppSettings, load_settings
from app.mysql_database import MySQLDatabase
from app.postgresql_database import PostgresDatabase, now_ts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("alarm_judge")


class ShelvePoint(BaseModel):
    group: str
    tag: str
    start_time: datetime | None = None
    end_time: datetime | None = None


class UnshelvePoint(BaseModel):
    group: str
    tag: str


class ConfirmRequest(BaseModel):
    alarm_key: str
    operator: str = Field(min_length=1)


@dataclass
class AlarmRule:
    group: str
    tag: str
    type_code: int
    setpoint: float | None
    threshold: float | None

    @property
    def key(self) -> str:
        return f"{self.group}:{self.tag}:{self.type_code}"


class ShelveManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._shelved: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _key(group: str, tag: str) -> str:
        return f"{group}:{tag}"

    def shelve(self, point: ShelvePoint) -> None:
        key = self._key(point.group, point.tag)
        with self._lock:
            self._shelved[key] = {
                "group": point.group,
                "tag": point.tag,
                "start_time": (point.start_time or now_ts()).strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": point.end_time.strftime("%Y-%m-%d %H:%M:%S") if point.end_time else None,
                "shelved": True,
            }

    def unshelve(self, group: str, tag: str) -> bool:
        key = self._key(group, tag)
        with self._lock:
            return self._shelved.pop(key, None) is not None

    def is_shelved(self, group: str, tag: str) -> bool:
        key = self._key(group, tag)
        with self._lock:
            item = self._shelved.get(key)
            if not item:
                return False
            end_time = item.get("end_time")
            if end_time and datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S") < now_ts():
                self._shelved.pop(key, None)
                return False
            return True

    def list_shelved(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return dict(self._shelved)


class AlarmEngine:
    def __init__(self, postgres_db: PostgresDatabase, shelve_manager: ShelveManager):
        self.postgres_db = postgres_db
        self.shelve_manager = shelve_manager
        self.rules: dict[str, AlarmRule] = {}
        self.last_values: dict[str, tuple[float, datetime]] = {}

    def load_rules(self, rows: list[dict[str, Any]]) -> None:
        self.rules = {}
        for row in rows:
            rule = AlarmRule(
                group=row["group"],
                tag=row["tag"],
                type_code=int(row["type_code"]),
                setpoint=float(row["setpoint"]) if row.get("setpoint") is not None else None,
                threshold=float(row["threshold"]) if row.get("threshold") is not None else None,
            )
            self.rules[rule.key] = rule
        logger.info("Loaded %d rules", len(self.rules))

    def process(self, group: str, tag: str, value: Any, ts: datetime | None = None) -> None:
        ts = ts or now_ts()
        value = float(value)
        if self.shelve_manager.is_shelved(group, tag):
            logger.info("Skip shelved point %s:%s", group, tag)
            return

        matching_rules = [r for r in self.rules.values() if r.group == group and r.tag == tag]
        for rule in matching_rules:
            alarm_key = rule.key
            active = self._check_trigger(rule, value, ts)
            payload = {
                "alarm_key": alarm_key,
                "alarm_group": group,
                "tag": tag,
                "type_code": rule.type_code,
                "current_value": value,
                "message": f"{alarm_key} value={value}",
            }
            if active:
                self.postgres_db.upsert_live_alarm(payload)
                self.postgres_db.append_alarm_record({**payload, "event": "trigger", "operator": None, "event_time": ts})
            else:
                self.postgres_db.clear_live_alarm(alarm_key)
                self.postgres_db.append_alarm_record({**payload, "event": "clear", "operator": None, "event_time": ts})

        self.last_values[f"{group}:{tag}"] = (value, ts)

    def _check_trigger(self, rule: AlarmRule, value: float, ts: datetime) -> bool:
        last = self.last_values.get(f"{rule.group}:{rule.tag}")
        threshold = rule.threshold if rule.threshold is not None else 0.0
        setpoint = rule.setpoint if rule.setpoint is not None else 0.0

        match rule.type_code:
            case 2:
                return value > setpoint
            case 1:
                return value > setpoint
            case -1:
                return value < setpoint
            case -2:
                return value < setpoint
            case 5:
                return bool(value)
            case 6:
                return not bool(value)
            case 7:
                return abs(value - setpoint) > threshold
            case 8:
                if not last:
                    return False
                last_v, last_ts = last
                dt = max((ts - last_ts).total_seconds(), 1.0)
                return (value - last_v) / dt > threshold
            case 9:
                if not last:
                    return False
                last_v, last_ts = last
                dt = max((ts - last_ts).total_seconds(), 1.0)
                return (last_v - value) / dt > threshold
            case _:
                return False


class AlarmJudgeApp:
    def __init__(self) -> None:
        self.settings: AppSettings = load_settings()
        self.mysql_db = MySQLDatabase(self.settings.mysql)
        self.postgres_db = PostgresDatabase(self.settings.postgres)
        self.shelve_manager = ShelveManager()
        self.engine = AlarmEngine(self.postgres_db, self.shelve_manager)
        self.api = FastAPI(title="Alarm Judge", version="1.0.0")
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.settings.mqtt.client_id)
        self._register_routes()
        self._register_mqtt_handlers()

    def start(self) -> None:
        self.postgres_db.ensure_schema()
        self.reload_rules()
        self._start_mqtt()

    def reload_rules(self) -> None:
        rows = self.mysql_db.load_alarm_config()
        self.engine.load_rules(rows)

    def _register_routes(self) -> None:
        @self.api.get("/health")
        def health() -> dict[str, str]:
            return {"status": "success", "message": "ok"}

        @self.api.post("/shelve")
        def shelve(points: list[ShelvePoint]) -> dict[str, Any]:
            for point in points:
                self.shelve_manager.shelve(point)
            return {"status": "success", "message": f"成功搁置{len(points)}个点位", "count": len(points)}

        @self.api.post("/unshelve")
        def unshelve(points: list[UnshelvePoint]) -> dict[str, Any]:
            count = 0
            for point in points:
                if self.shelve_manager.unshelve(point.group, point.tag):
                    count += 1
            return {"status": "success", "message": f"成功取消搁置{count}个点位", "count": count}

        @self.api.get("/shelved_points")
        def shelved_points() -> dict[str, Any]:
            items = self.shelve_manager.list_shelved()
            return {"status": "success", "message": "获取搁置点位成功", "count": len(items), "data": items}

        @self.api.post("/confirm")
        def confirm(req: ConfirmRequest) -> dict[str, Any]:
            if not self.postgres_db.confirm_alarm(req.alarm_key, req.operator):
                raise HTTPException(status_code=404, detail="未找到激活报警")
            self.postgres_db.append_alarm_record(
                {
                    "alarm_key": req.alarm_key,
                    "alarm_group": req.alarm_key.split(":")[0],
                    "tag": req.alarm_key.split(":")[1],
                    "type_code": int(req.alarm_key.split(":")[2]),
                    "current_value": None,
                    "message": "alarm confirmed",
                    "event": "confirm",
                    "operator": req.operator,
                    "event_time": now_ts(),
                }
            )
            return {"status": "success", "message": "报警确认成功", "count": 1}

        @self.api.get("/alarms/live")
        def live_alarms() -> dict[str, Any]:
            data = self.postgres_db.list_active_alarms()
            return {"status": "success", "message": "获取实时报警成功", "count": len(data), "data": data}

    def _register_mqtt_handlers(self) -> None:
        def on_connect(client: mqtt.Client, _userdata: Any, _flags: Any, reason_code: Any, _properties: Any) -> None:
            if reason_code == 0:
                client.subscribe(self.settings.mqtt.topic)
                logger.info("Connected MQTT, subscribed topic: %s", self.settings.mqtt.topic)
            else:
                logger.error("MQTT connect failed: %s", reason_code)

        def on_message(_client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
            try:
                payload = json.loads(msg.payload.decode("utf-8"))
                group = payload["group"]
                tag = payload["tag"]
                value = payload["value"]
                ts = datetime.fromisoformat(payload["timestamp"]) if payload.get("timestamp") else now_ts()
                self.engine.process(group, tag, value, ts)
            except Exception:
                logger.exception("Failed to process mqtt message: %s", msg.payload)

        self.mqtt_client.on_connect = on_connect
        self.mqtt_client.on_message = on_message

    def _start_mqtt(self) -> None:
        if self.settings.mqtt.username:
            self.mqtt_client.username_pw_set(self.settings.mqtt.username, self.settings.mqtt.password)
        self.mqtt_client.connect_async(self.settings.mqtt.host, self.settings.mqtt.port)
        self.mqtt_client.loop_start()


app_instance = AlarmJudgeApp()
app = app_instance.api


@app.on_event("startup")
def _startup() -> None:
    app_instance.start()


def main() -> None:
    uvicorn.run("app.alarm_judge:app", host="0.0.0.0", port=5000, reload=False)


if __name__ == "__main__":
    main()
