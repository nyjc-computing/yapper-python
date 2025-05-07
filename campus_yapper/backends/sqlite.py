"""campus_yapper.backends.sqlite

SQLite backend for campus_yapper.
This implementation is for local testing purposes only.
It is not intended for production use.
"""
from campus_yapper.base import ClientId, Event, EventLabel, EventData, YapperInterface
import sqlite3
from typing import Any, Generator, TypedDict
from contextlib import contextmanager


def dict_factory(cursor: sqlite3.Cursor, row: sqlite3.Row) -> dict[str, Any]:
    """Convert SQLite rows to dictionaries."""
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


class SQLiteResult(TypedDict):
    """Typed dictionary for SQLite query results."""
    result: list[dict]
    lastrowid: int
    rowcount: int


class SQLiteYapper(YapperInterface):

    def __init__(self, client_id: ClientId, *, db: str = ":memory:") -> None:
        super().__init__(client_id)
        self.db_uri = db

    @contextmanager
    def _get_conn(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager for SQLite database connection."""
        conn = sqlite3.connect(self.db_uri)
        conn.row_factory = dict_factory
        conn.execute("PRAGMA foreign_keys = ON")
        conn.isolation_level = None  # Use autocommit mode
        try:
            yield conn
        finally:
            conn.close()

    def _execute(
            self,
            query: str,
            params: tuple = (),
    ) -> SQLiteResult:
        """Execute a SQL query."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            result = cursor.fetchall()
        # Test implementation does not expect large result set,
        # fetchall() considered safe.
        return {
            "result": result,
            "lastrowid": cursor.lastrowid,
            "rowcount": cursor.rowcount,
        }
    
    def _executemany(
            self,
            query: str,
            params: list[tuple] = [],
    ) -> SQLiteResult:
        """Execute a SQL query with multiple parameters."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, params)
            result = cursor.fetchall()
        return {
            "result": result,
            "lastrowid": cursor.lastrowid,
            "rowcount": cursor.rowcount,
        }

    def _init_db(self) -> None:
        """Initialize the SQLite database."""
        self._execute(
            """CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY,
                label TEXT NOT NULL,
                data TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );"""
        )
        self._execute(
            """CREATE TABLE IF NOT EXISTS subscribers (
                client_id TEXT NOT NULL,
                label TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (client_id, label)
            );"""
        )
        self._execute(
            """CREATE TABLE IF NOT EXISTS unread (
                client_id TEXT NOT NULL,
                event_id INTEGER NOT NULL REFERENCES events(id),
                PRIMARY KEY (client_id, event_id)
            );"""
        )

    def emit(self, label: EventLabel, data: EventData | None = None) -> None:
        """Emit an event to the message broker."""
        data = data or {}
        result = self._execute(
            "INSERT INTO events (label, data) VALUES (?, ?)",
            (label, str(data))
        )
        # Get subscribers
        subscribers = [
            row["client_id"] for row in self._execute(
                "SELECT client_id FROM subscribers WHERE label = ?",
                (label,)
            )["result"]
        ]
        # Notify subscribers
        self._executemany(
            "INSERT INTO unread (client_id, event_id) VALUES (?, ?)",
            [(client_id, result["lastrowid"]) for client_id in subscribers]
        )

    def subscribe(self, label: EventLabel) -> None:
        """Subscribe to an event label."""
        self._execute(
            "INSERT OR IGNORE INTO subscribers (client_id, label) VALUES (?, ?)",
            (self.client_id, label)
        )
    
    def unsubscribe(self, label: EventLabel) -> None:
        """Unsubscribe from an event label."""
        self._execute(
            "DELETE FROM subscribers WHERE client_id = ? AND label = ?",
            (self.client_id, label)
        )
        self._sweep()  # Clean up old events
    
    def listen(self) -> list[Event]:
        """Listen for events from the message broker."""
        # Get and clear unread events
        result = self._execute(
            "SELECT events.label, events.data FROM events "
            "JOIN unread ON events.id = unread.event_id "
            "WHERE unread.client_id = ?",
            (self.client_id,)
        )
        # Clear unread events
        self._execute(
            "DELETE FROM unread WHERE client_id = ?",
            (self.client_id,)
        )
        # Return events
        return [
            Event(row["label"], row["data"])
            for row in result["result"]
        ]
    
    def _sweep(self) -> None:
        """Clear old events without subscribers."""
        self._execute(
            "DELETE FROM events WHERE id NOT IN "
            "(SELECT DISTINCT event_id FROM unread)"
        )

    def start(self) -> None:
        """Start the Yapper instance."""
        self._init_db()
        super().start()
    
    def pause(self) -> None:
        """Pause the Yapper instance.

        This differs from stop() in that it does not clear
        the event queue.
        """
        super().stop()

    def stop(self) -> None:
        """Stop the Yapper instance."""
        self.pause()
        self.listen()  # clear event queue
        # Unsubscribe from all events
        self._execute(
            "DELETE FROM unread WHERE client_id = ?",
            (self.client_id,)
        )
        self._execute(
            "DELETE FROM subscribers WHERE client_id = ?",
            (self.client_id,)
        )
        self._sweep()
