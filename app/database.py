"""Common database helpers."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Generator


class DatabaseError(RuntimeError):
    """Database operation error."""


class BaseDatabase:
    def __init__(self) -> None:
        self._conn = None

    def connect(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @contextmanager
    def cursor(self) -> Generator:
        if self._conn is None:
            self.connect()
        cursor = self._conn.cursor()
        try:
            yield cursor
            self._conn.commit()
        except Exception as exc:
            self._conn.rollback()
            raise DatabaseError(str(exc)) from exc
        finally:
            cursor.close()
