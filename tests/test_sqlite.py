import unittest
import os
import tempfile
from campus_yapper.backends.sqlite import SQLiteYapper
from campus_yapper.base import Event

class TestSQLiteYapper(unittest.TestCase):

    def setUp(self):
        """Set up a SQLiteYapper instance for each test."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False)
        self.yapper = SQLiteYapper(client_id="test_client", db=self.temp_db.name)
        self.yapper._init_db()

    def tearDown(self):
        """Clean up after each test."""
        self.yapper.stop()
        os.unlink(self.temp_db.name)  # Delete the temporary database file

    def test_emit_creates_event(self):
        """Test that emit inserts an event into the database."""
        self.yapper.start()
        self.yapper.on_event("test_event")
        self.yapper.emit("test_event", None)
        events = self.yapper.listen()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].label, "test_event")

    def test_listen_retrieves_events(self):
        """Test that listen retrieves and clears unread events."""
        self.yapper.start()
        self.yapper.on_event("test_event")
        self.yapper.emit("test_event", None)
        events = self.yapper.listen()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].label, "test_event")
        # Ensure events are cleared after listening
        events = self.yapper.listen()
        self.assertEqual(len(events), 0)

    def test_on_event_registers_handler(self):
        """Test that on_event registers and invokes a handler."""
        self.yapper.start()
        handled = []

        @self.yapper.on_event("test_event")
        def handler(event):
            handled.append(event)

        self.yapper.emit("test_event", None)
        self.yapper.handle_event(Event("test_event", None))
        self.assertEqual(len(handled), 1)
        self.assertEqual(handled[0].label, "test_event")

    def test_start_initializes_database(self):
        """Test that start initializes the database."""
        self.yapper.start()
        self.yapper.on_event("test_event")
        # Emit an event to ensure the database is initialized
        self.yapper.emit("test_event", None)
        events = self.yapper.listen()
        self.assertEqual(len(events), 1)

    def test_stop_cleans_up(self):
        """Test that stop clears the event queue and unsubscribes."""
        self.yapper.start()
        self.yapper.on_event("test_event")
        self.yapper.emit("test_event", None)
        self.yapper.stop()
        # Ensure no events remain after stop
        events = self.yapper.listen()
        self.assertEqual(len(events), 0)

    def test_running_state(self):
        """Test the running state transitions."""
        self.assertFalse(self.yapper.running)
        self.yapper.start()
        self.assertTrue(self.yapper.running)
        self.yapper.stop()
        self.assertFalse(self.yapper.running)

if __name__ == "__main__":
    unittest.main()