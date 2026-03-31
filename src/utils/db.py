import logging
import sqlite3
from contextlib import contextmanager
from typing import Generator

from config import DB_CHECK_SAME_THREAD, DB_PATH, DB_TIMEOUT

logger = logging.getLogger(__name__)


def get_connection() -> sqlite3.Connection:
    """Create and return a database connection with proper configuration."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        str(DB_PATH),
        timeout=DB_TIMEOUT,
        check_same_thread=DB_CHECK_SAME_THREAD,
    )
    conn.row_factory = sqlite3.Row  # Enable column access by name
    return conn


@contextmanager
def db_connection() -> Generator[sqlite3.Connection, None, None]:
    """Context manager for database connections with automatic cleanup."""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Database error occurred, transaction rolled back: {e}")
        raise
    finally:
        conn.close()
