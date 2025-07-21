"""Tests for the PostgreSQL backend."""
import pytest
from unittest.mock import Mock, patch, MagicMock
from campus_yapper.backends.postgres import PostgreSQLYapper
from campus_yapper.base import Event


@pytest.fixture
def mock_psycopg2():
    """Mock psycopg2 for testing without requiring a real database."""
    with patch('campus_yapper.backends.postgres.psycopg2') as mock:
        # Mock connection
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = []
        mock_cursor.rowcount = 0
        mock_cursor.lastrowid = None
        mock_conn.cursor.return_value = mock_cursor
        mock.connect.return_value = mock_conn
        
        yield mock


def test_postgres_yapper_init():
    """Test PostgreSQLYapper initialization."""
    yapper = PostgreSQLYapper(
        "test_client",
        db_uri="postgresql://testuser:testpass@testhost:5432/testdb"
    )
    
    assert yapper.client_id == "test_client"
    assert yapper.db_uri == "postgresql://testuser:testpass@testhost:5432/testdb"


def test_postgres_yapper_emit(mock_psycopg2):
    """Test emitting an event."""
    yapper = PostgreSQLYapper("test_client")
    
    # Mock the return value for the INSERT ... RETURNING query
    mock_cursor = mock_psycopg2.connect.return_value.cursor.return_value
    mock_cursor.fetchall.return_value = [{"id": 1}]
    
    yapper.emit("test_label", {"key": "value"})
    
    # Verify that execute was called (at least twice - once for insert, once for select subscriptions)
    assert mock_cursor.execute.call_count >= 2


def test_postgres_yapper_subscribe(mock_psycopg2):
    """Test subscribing to an event label."""
    yapper = PostgreSQLYapper("test_client")
    
    yapper.subscribe("test_label")
    
    mock_cursor = mock_psycopg2.connect.return_value.cursor.return_value
    mock_cursor.execute.assert_called()


def test_postgres_yapper_listen(mock_psycopg2):
    """Test listening for events."""
    yapper = PostgreSQLYapper("test_client")
    
    # Mock the return value for the SELECT query
    mock_cursor = mock_psycopg2.connect.return_value.cursor.return_value
    mock_cursor.fetchall.return_value = [
        {"label": "test_label", "data": "{'key': 'value'}"}
    ]
    
    events = yapper.listen()
    
    assert len(events) == 1
    assert events[0].label == "test_label"
    assert events[0].data == "{'key': 'value'}"
    
    # Verify that execute was called at least twice (select events + clear unread)
    assert mock_cursor.execute.call_count >= 2
