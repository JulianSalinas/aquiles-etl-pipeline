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
        
        result = ensure_connection_established(mock_engine)
        
        assert result == (1,)
        assert mock_engine.connect.call_count == 1
    
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
        
        result = ensure_connection_established(mock_engine, max_retries=2)
        
        assert result == (1,)
        assert mock_engine.connect.call_count == 2
        mock_sleep.assert_called_once_with(1)  # First retry waits 1 second
    
    @patch('time.sleep')  # Mock sleep to speed up test
    def test_connection_max_retries_exceeded(self, mock_sleep):
        """Test connection fails after max retries."""
        mock_engine = Mock()
        mock_engine.connect.side_effect = Exception("Database unavailable")
        
        with pytest.raises(Exception, match="Database unavailable"):
            ensure_connection_established(mock_engine, max_retries=2)
        
        assert mock_engine.connect.call_count == 2
        assert mock_sleep.call_count == 1  # Called once before final attempt
    
    @patch('time.sleep')  # Mock sleep to speed up test
    def test_exponential_backoff_timing(self, mock_sleep):
        """Test that retry delays follow exponential backoff."""
        mock_engine = Mock()
        mock_engine.connect.side_effect = [
            Exception("Retry 1"),
            Exception("Retry 2"), 
            Exception("Max retries")
        ]
        
        with pytest.raises(Exception, match="Max retries"):
            ensure_connection_established(mock_engine, max_retries=3)
        
        # Verify exponential backoff: 1s, 2s
        expected_calls = [call(1), call(2)]
        mock_sleep.assert_has_calls(expected_calls)