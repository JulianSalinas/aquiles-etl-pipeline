"""
Tests for database awakening functionality.
"""
import pytest
from unittest.mock import Mock, patch, call
from core.database import ensure_connection_established


class TestDatabaseAwakening:
    """Test cases for database awakening functionality."""
    
    def test_connection_success_first_try(self):
        """Test successful connection on first attempt."""
        mock_engine = Mock()
        mock_conn = Mock()
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        mock_conn.execute.return_value.fetchone.return_value = (1,)
        
        ensure_connection_established(mock_engine)
    
    @patch('time.sleep')  # Mock sleep to speed up test
    def test_connection_retry_then_success(self, mock_sleep):
        """Test connection fails first time, succeeds on retry."""
        mock_engine = Mock()
        mock_conn = Mock()
        
        # First call fails, second succeeds
        mock_engine.connect.side_effect = [
            Exception("Database sleeping"),
            Mock(__enter__=Mock(return_value=mock_conn), __exit__=Mock(return_value=None))
        ]
        mock_conn.execute.return_value.fetchone.return_value = (1,)
        
        ensure_connection_established(mock_engine, retries_left=2)
        
        assert mock_engine.connect.call_count == 2
        mock_sleep.assert_called_once_with(1)  # First retry waits 1 second