"""Configuration loader for Alarm Judge."""

from __future__ import annotations

from configparser import ConfigParser
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class MysqlSettings:
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass(frozen=True)
class MqttSettings:
    host: str
    port: int
    topic: str
    client_id: str
    username: str = ""
    password: str = ""


@dataclass(frozen=True)
class AppSettings:
    mysql: MysqlSettings
    postgres: PostgresSettings
    mqtt: MqttSettings


DEFAULT_CONFIG_PATH = Path(__file__).with_name("config.ini")


def load_settings(config_path: Path | None = None) -> AppSettings:
    parser = ConfigParser()
    parser.read(config_path or DEFAULT_CONFIG_PATH, encoding="utf-8")

    mysql = MysqlSettings(
        host=parser.get("mysql", "host", fallback="127.0.0.1"),
        port=parser.getint("mysql", "port", fallback=3306),
        user=parser.get("mysql", "user", fallback="root"),
        password=parser.get("mysql", "password", fallback=""),
        database=parser.get("mysql", "database", fallback="alarm_config"),
    )

    postgres = PostgresSettings(
        host=parser.get("postgresql", "host", fallback="127.0.0.1"),
        port=parser.getint("postgresql", "port", fallback=5432),
        user=parser.get("postgresql", "user", fallback="postgres"),
        password=parser.get("postgresql", "password", fallback=""),
        database=parser.get("postgresql", "database", fallback="alarm_judge"),
    )

    mqtt = MqttSettings(
        host=parser.get("mqtt", "host", fallback="127.0.0.1"),
        port=parser.getint("mqtt", "port", fallback=1883),
        topic=parser.get("mqtt", "topic", fallback="alarm_judge/+/+"),
        client_id=parser.get("mqtt", "client_id", fallback="alarm_judge"),
        username=parser.get("mqtt", "username", fallback=""),
        password=parser.get("mqtt", "password", fallback=""),
    )

    return AppSettings(mysql=mysql, postgres=postgres, mqtt=mqtt)
