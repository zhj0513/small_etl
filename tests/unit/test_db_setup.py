"""Tests for database setup utilities."""

from unittest.mock import MagicMock, patch

import pytest


class TestCreateDatabaseIfNotExists:
    """Tests for create_database_if_not_exists function."""

    def test_database_already_exists(self):
        """Test when database already exists."""
        from small_etl.data_access.db_setup import create_database_if_not_exists

        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (1,)  # Database exists

        mock_conn.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        with patch("small_etl.data_access.db_setup.create_engine", return_value=mock_engine):
            result = create_database_if_not_exists(database="test_db")

        assert result is False
        mock_engine.dispose.assert_called_once()

    def test_database_created(self):
        """Test when database is created."""
        from small_etl.data_access.db_setup import create_database_if_not_exists

        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None  # Database doesn't exist

        mock_conn.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        with patch("small_etl.data_access.db_setup.create_engine", return_value=mock_engine):
            result = create_database_if_not_exists(database="new_test_db")

        assert result is True
        # Should have called execute twice: once for check, once for create
        assert mock_conn.execute.call_count == 2
        mock_engine.dispose.assert_called_once()


class TestCreateTables:
    """Tests for create_tables function."""

    def test_create_tables(self):
        """Test creating tables."""
        from small_etl.data_access.db_setup import create_tables

        mock_engine = MagicMock()
        mock_metadata = MagicMock()

        with patch("small_etl.data_access.db_setup.create_engine", return_value=mock_engine), \
             patch("small_etl.data_access.db_setup.SQLModel") as mock_sqlmodel:
            mock_sqlmodel.metadata = mock_metadata
            create_tables("postgresql://test:test@localhost/test")

        mock_metadata.create_all.assert_called_once_with(mock_engine)
        mock_engine.dispose.assert_called_once()


class TestDropTables:
    """Tests for drop_tables function."""

    def test_drop_tables(self):
        """Test dropping tables."""
        from small_etl.data_access.db_setup import drop_tables

        mock_engine = MagicMock()
        mock_metadata = MagicMock()

        with patch("small_etl.data_access.db_setup.create_engine", return_value=mock_engine), \
             patch("small_etl.data_access.db_setup.SQLModel") as mock_sqlmodel:
            mock_sqlmodel.metadata = mock_metadata
            drop_tables("postgresql://test:test@localhost/test")

        mock_metadata.drop_all.assert_called_once_with(mock_engine)
        mock_engine.dispose.assert_called_once()


class TestSetupTestDatabase:
    """Tests for setup_test_database function."""

    def test_setup_test_database(self):
        """Test setting up test database."""
        from small_etl.data_access.db_setup import setup_test_database

        with patch("small_etl.data_access.db_setup.create_database_if_not_exists") as mock_create_db, \
             patch("small_etl.data_access.db_setup.create_tables") as mock_create_tables:
            result = setup_test_database(
                host="localhost",
                port=5432,
                user="test",
                password="test",
                database="test_db",
            )

        assert "postgresql://" in result
        assert "test_db" in result
        mock_create_db.assert_called_once()
        mock_create_tables.assert_called_once()
