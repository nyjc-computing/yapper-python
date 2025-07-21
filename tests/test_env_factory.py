"""Tests for environment-based factory function."""
import os
import unittest
from unittest.mock import patch
import campus_yapper
from campus_yapper.backends.sqlite import SQLiteYapper
from campus_yapper.backends.postgres import PostgreSQLYapper


class TestEnvFactory(unittest.TestCase):
    """Test cases for environment-based factory function."""

    def setUp(self):
        """Set up test environment variables."""
        # Store original values to restore later
        self.original_env = {
            'CLIENT_ID': os.environ.get('CLIENT_ID'),
            'CLIENT_SECRET': os.environ.get('CLIENT_SECRET'),
            'ENV': os.environ.get('ENV')
        }

    def tearDown(self):
        """Restore original environment variables."""
        for key, value in self.original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_create_requires_client_id(self):
        """Test that CLIENT_ID environment variable is required."""
        # Clear environment variables
        os.environ.pop("CLIENT_ID", None)
        os.environ.pop("CLIENT_SECRET", None)
        
        with self.assertRaises(ValueError) as context:
            campus_yapper.create()
        self.assertIn("CLIENT_ID environment variable is required", str(context.exception))

    def test_create_requires_client_secret(self):
        """Test that CLIENT_SECRET environment variable is required."""
        os.environ["CLIENT_ID"] = "test_client"
        os.environ.pop("CLIENT_SECRET", None)
        
        with self.assertRaises(ValueError) as context:
            campus_yapper.create()
        self.assertIn("CLIENT_SECRET environment variable is required", str(context.exception))

    def test_create_development_env(self):
        """Test that development environment uses SQLite."""
        os.environ["CLIENT_ID"] = "test_client"
        os.environ["CLIENT_SECRET"] = "test_secret"
        os.environ["ENV"] = "development"
        
        yapper = campus_yapper.create()
        self.assertIsInstance(yapper, SQLiteYapper)
        self.assertEqual(yapper.client_id, "test_client")

    def test_create_testing_env(self):
        """Test that testing environment uses SQLite."""
        os.environ["CLIENT_ID"] = "test_client"
        os.environ["CLIENT_SECRET"] = "test_secret"
        os.environ["ENV"] = "testing"
        
        yapper = campus_yapper.create()
        self.assertIsInstance(yapper, SQLiteYapper)

    @patch('campus_yapper.backends.postgres.psycopg2')
    def test_create_staging_env(self, mock_psycopg2):
        """Test that staging environment uses PostgreSQL."""
        os.environ["CLIENT_ID"] = "test_client"
        os.environ["CLIENT_SECRET"] = "test_secret"
        os.environ["ENV"] = "staging"
        
        yapper = campus_yapper.create(db_uri="postgresql://test@localhost:5432/test_db")
        self.assertIsInstance(yapper, PostgreSQLYapper)

    @patch('campus_yapper.backends.postgres.psycopg2')
    def test_create_production_env(self, mock_psycopg2):
        """Test that production environment uses PostgreSQL."""
        os.environ["CLIENT_ID"] = "test_client"
        os.environ["CLIENT_SECRET"] = "test_secret"
        os.environ["ENV"] = "production"
        
        yapper = campus_yapper.create(db_uri="postgresql://test@localhost:5432/test_db")
        self.assertIsInstance(yapper, PostgreSQLYapper)

    def test_create_default_env(self):
        """Test that missing ENV defaults to development (SQLite)."""
        os.environ["CLIENT_ID"] = "test_client"
        os.environ["CLIENT_SECRET"] = "test_secret"
        os.environ.pop("ENV", None)
        
        yapper = campus_yapper.create()
        self.assertIsInstance(yapper, SQLiteYapper)

    def test_create_invalid_env(self):
        """Test that invalid ENV raises ValueError."""
        os.environ["CLIENT_ID"] = "test_client"
        os.environ["CLIENT_SECRET"] = "test_secret"
        os.environ["ENV"] = "invalid"
        
        with self.assertRaises(ValueError) as context:
            campus_yapper.create()
        self.assertIn("Unsupported ENV value: invalid", str(context.exception))

    def test_create_with_kwargs(self):
        """Test that kwargs are passed to backend constructors."""
        os.environ["CLIENT_ID"] = "test_client"
        os.environ["CLIENT_SECRET"] = "test_secret"
        os.environ["ENV"] = "development"
        
        yapper = campus_yapper.create(db="./test.db")
        self.assertIsInstance(yapper, SQLiteYapper)
        # Check that the database path was set correctly
        self.assertEqual(yapper.db_uri, "./test.db")  # type: ignore

    def test_create_staging_env_missing_db_uri(self):
        """Test that staging environment requires db_uri parameter."""
        os.environ["CLIENT_ID"] = "test_client"
        os.environ["CLIENT_SECRET"] = "test_secret"
        os.environ["ENV"] = "staging"
        
        with self.assertRaises(ValueError) as context:
            campus_yapper.create()
        self.assertIn("db_uri parameter is required for staging environment", str(context.exception))

    def test_create_production_env_missing_db_uri(self):
        """Test that production environment requires db_uri parameter."""
        os.environ["CLIENT_ID"] = "test_client"
        os.environ["CLIENT_SECRET"] = "test_secret"
        os.environ["ENV"] = "production"
        
        with self.assertRaises(ValueError) as context:
            campus_yapper.create()
        self.assertIn("db_uri parameter is required for production environment", str(context.exception))


if __name__ == '__main__':
    unittest.main()
