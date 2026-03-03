"""MySQL operations for reading alarm configuration."""

from __future__ import annotations

from typing import Any

import pymysql

from app.config import MysqlSettings
from app.database import BaseDatabase


class MySQLDatabase(BaseDatabase):
    def __init__(self, settings: MysqlSettings) -> None:
        super().__init__()
        self.settings = settings

    def connect(self) -> None:
        self._conn = pymysql.connect(
            host=self.settings.host,
            port=self.settings.port,
            user=self.settings.user,
            password=self.settings.password,
            database=self.settings.database,
            charset="utf8mb4",
            autocommit=False,
            cursorclass=pymysql.cursors.DictCursor,
        )

    def load_alarm_config(self) -> list[dict[str, Any]]:
        with self.cursor() as cur:
            cur.execute(
                """
                SELECT `group`, `tag`, `type_code`, `setpoint`, `threshold`, `enabled`
                FROM config
                WHERE enabled = 1
                """
            )
            return list(cur.fetchall())
