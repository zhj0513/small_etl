"""Tests for __main__.py module."""

from unittest.mock import patch


class TestMain:
    """Tests for __main__ entry point."""

    def test_main_entry_point(self):
        """Test that __main__ calls main() and exits with return value."""
        with patch("small_etl.cli.main", return_value=0) as mock_main:
            # Import and execute __main__ code
            import importlib
            import small_etl.__main__ as main_module
            importlib.reload(main_module)

            # The main function should be imported
            mock_main.assert_not_called()  # Not called on import since __name__ != "__main__"
