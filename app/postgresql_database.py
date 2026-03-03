"""PostgreSQL persistence for live alarms and alarm records."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import psycopg2
import psycopg2.extras

from app.config import PostgresSettings
from app.database import BaseDatabase


class PostgresDatabase(BaseDatabase):
    def __init__(self, settings: PostgresSettings) -> None:
        super().__init__()
        self.settings = settings

    def connect(self) -> None:
        self._conn = psycopg2.connect(
            host=self.settings.host,
            port=self.settings.port,
            user=self.settings.user,
            password=self.settings.password,
            dbname=self.settings.database,
        )

    def ensure_schema(self) -> None:
        with self.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS alarm_live (
                    id SERIAL PRIMARY KEY,
                    alarm_key VARCHAR(255) UNIQUE NOT NULL,
                    alarm_group VARCHAR(64) NOT NULL,
                    tag VARCHAR(128) NOT NULL,
                    type_code INT NOT NULL,
                    current_value DOUBLE PRECISION,
                    message TEXT,
                    active BOOLEAN NOT NULL DEFAULT TRUE,
                    confirmed BOOLEAN NOT NULL DEFAULT FALSE,
                    confirmed_by VARCHAR(128),
                    confirmed_at TIMESTAMP,
                    triggered_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS alarm_record (
                    id SERIAL PRIMARY KEY,
                    alarm_key VARCHAR(255) NOT NULL,
                    alarm_group VARCHAR(64) NOT NULL,
                    tag VARCHAR(128) NOT NULL,
                    type_code INT NOT NULL,
                    current_value DOUBLE PRECISION,
                    message TEXT,
                    event VARCHAR(32) NOT NULL,
                    operator VARCHAR(128),
                    event_time TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )

    def upsert_live_alarm(self, payload: dict[str, Any]) -> None:
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO alarm_live (
                    alarm_key, alarm_group, tag, type_code, current_value, message, active, updated_at
                ) VALUES (%(alarm_key)s, %(alarm_group)s, %(tag)s, %(type_code)s, %(current_value)s, %(message)s, TRUE, NOW())
                ON CONFLICT (alarm_key) DO UPDATE SET
                    current_value = EXCLUDED.current_value,
                    message = EXCLUDED.message,
                    active = TRUE,
                    updated_at = NOW();
                """,
                payload,
            )

    def clear_live_alarm(self, alarm_key: str) -> None:
        with self.cursor() as cur:
            cur.execute(
                "UPDATE alarm_live SET active = FALSE, updated_at = NOW() WHERE alarm_key = %s",
                (alarm_key,),
            )

    def append_alarm_record(self, payload: dict[str, Any]) -> None:
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO alarm_record (
                    alarm_key, alarm_group, tag, type_code, current_value, message, event, operator, event_time
                ) VALUES (
                    %(alarm_key)s, %(alarm_group)s, %(tag)s, %(type_code)s,
                    %(current_value)s, %(message)s, %(event)s, %(operator)s, %(event_time)s
                )
                """,
                payload,
            )

    def confirm_alarm(self, alarm_key: str, operator: str) -> bool:
        with self.cursor() as cur:
            cur.execute(
                """
                UPDATE alarm_live
                SET confirmed = TRUE, confirmed_by = %s, confirmed_at = NOW(), updated_at = NOW()
                WHERE alarm_key = %s AND active = TRUE
                """,
                (operator, alarm_key),
            )
            return cur.rowcount > 0

    def list_active_alarms(self) -> list[dict[str, Any]]:
        with self.cursor() as cur:
            dict_cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            try:
                dict_cur.execute(
                    "SELECT * FROM alarm_live WHERE active = TRUE ORDER BY triggered_at DESC"
                )
                return list(dict_cur.fetchall())
            finally:
                dict_cur.close()


def now_ts() -> datetime:
    return datetime.now()
