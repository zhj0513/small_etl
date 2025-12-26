"""Unit tests for PostgresRepository."""

from unittest.mock import MagicMock, patch

import pytest

from small_etl.data_access.postgres_repository import PostgresRepository


class TestPostgresRepositoryMocked:
    """Unit tests for PostgresRepository using mocks."""

    @pytest.fixture
    def mock_engine(self):
        """Create a mock database engine."""
        return MagicMock()

    @pytest.fixture
    def repo_with_mock(self, mock_engine):
        """Create a PostgresRepository with mocked engine."""
        with patch("small_etl.data_access.postgres_repository.create_engine") as mock_create:
            mock_create.return_value = mock_engine
            repo = PostgresRepository("postgresql://test:test@localhost/test")
            return repo

    def test_init(self, repo_with_mock):
        """Test repository initialization."""
        assert repo_with_mock._engine is not None

    def test_get_session(self, repo_with_mock, mock_engine):
        """Test get_session returns a Session."""
        with patch("small_etl.data_access.postgres_repository.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value = mock_session

            repo_with_mock.get_session()

            mock_session_class.assert_called_once_with(repo_with_mock._engine)

    def test_close(self, repo_with_mock, mock_engine):
        """Test close disposes of the engine."""
        repo_with_mock.close()
        mock_engine.dispose.assert_called_once()
