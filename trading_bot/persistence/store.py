"""
State Storage
==============

Abstract and concrete implementations for state persistence.
"""

import json
import sqlite3
import os
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import logging
import pickle
import gzip

logger = logging.getLogger(__name__)


class StateStore(ABC):
    """Abstract base class for state storage."""
    
    @abstractmethod
    def save(self, key: str, value: Any, category: str = "default") -> bool:
        """Save a value with a key."""
        pass
    
    @abstractmethod
    def load(self, key: str, category: str = "default") -> Optional[Any]:
        """Load a value by key."""
        pass
    
    @abstractmethod
    def delete(self, key: str, category: str = "default") -> bool:
        """Delete a value by key."""
        pass
    
    @abstractmethod
    def exists(self, key: str, category: str = "default") -> bool:
        """Check if key exists."""
        pass
    
    @abstractmethod
    def list_keys(self, category: str = "default") -> List[str]:
        """List all keys in a category."""
        pass
    
    @abstractmethod
    def clear(self, category: str = "default") -> bool:
        """Clear all data in a category."""
        pass


class FileStore(StateStore):
    """
    File-based state storage.
    
    Uses JSON files for human-readable state and pickle for complex objects.
    """
    
    def __init__(self, base_path: str = "./data/state"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"FileStore initialized at {self.base_path}")
    
    def _get_path(self, key: str, category: str) -> Path:
        """Get file path for a key."""
        category_path = self.base_path / category
        category_path.mkdir(parents=True, exist_ok=True)
        # Sanitize key for filename
        safe_key = key.replace("/", "_").replace("\\", "_").replace(":", "_")
        return category_path / f"{safe_key}.json"
    
    def save(self, key: str, value: Any, category: str = "default") -> bool:
        """Save value to JSON file."""
        try:
            path = self._get_path(key, category)
            
            # Wrap value with metadata
            data = {
                "key": key,
                "category": category,
                "timestamp": datetime.utcnow().isoformat(),
                "value": value,
            }
            
            # Write atomically using temp file
            temp_path = path.with_suffix(".tmp")
            with open(temp_path, "w") as f:
                json.dump(data, f, indent=2, default=str)
            
            temp_path.replace(path)
            logger.debug(f"Saved state: {category}/{key}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving state {category}/{key}: {e}")
            return False
    
    def load(self, key: str, category: str = "default") -> Optional[Any]:
        """Load value from JSON file."""
        try:
            path = self._get_path(key, category)
            
            if not path.exists():
                return None
            
            with open(path, "r") as f:
                data = json.load(f)
            
            return data.get("value")
            
        except Exception as e:
            logger.error(f"Error loading state {category}/{key}: {e}")
            return None
    
    def delete(self, key: str, category: str = "default") -> bool:
        """Delete a state file."""
        try:
            path = self._get_path(key, category)
            
            if path.exists():
                path.unlink()
                logger.debug(f"Deleted state: {category}/{key}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error deleting state {category}/{key}: {e}")
            return False
    
    def exists(self, key: str, category: str = "default") -> bool:
        """Check if state file exists."""
        return self._get_path(key, category).exists()
    
    def list_keys(self, category: str = "default") -> List[str]:
        """List all keys in a category."""
        try:
            category_path = self.base_path / category
            
            if not category_path.exists():
                return []
            
            keys = []
            for path in category_path.glob("*.json"):
                keys.append(path.stem)
            
            return keys
            
        except Exception as e:
            logger.error(f"Error listing keys in {category}: {e}")
            return []
    
    def clear(self, category: str = "default") -> bool:
        """Clear all files in a category."""
        try:
            category_path = self.base_path / category
            
            if category_path.exists():
                for path in category_path.glob("*.json"):
                    path.unlink()
            
            logger.info(f"Cleared category: {category}")
            return True
            
        except Exception as e:
            logger.error(f"Error clearing category {category}: {e}")
            return False
    
    def save_binary(self, key: str, value: Any, category: str = "default") -> bool:
        """Save complex object using pickle with compression."""
        try:
            category_path = self.base_path / category
            category_path.mkdir(parents=True, exist_ok=True)
            safe_key = key.replace("/", "_").replace("\\", "_").replace(":", "_")
            path = category_path / f"{safe_key}.pkl.gz"
            
            temp_path = path.with_suffix(".tmp")
            with gzip.open(temp_path, "wb") as f:
                pickle.dump(value, f)
            
            temp_path.replace(path)
            logger.debug(f"Saved binary state: {category}/{key}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving binary state {category}/{key}: {e}")
            return False
    
    def load_binary(self, key: str, category: str = "default") -> Optional[Any]:
        """Load complex object from pickle file."""
        try:
            category_path = self.base_path / category
            safe_key = key.replace("/", "_").replace("\\", "_").replace(":", "_")
            path = category_path / f"{safe_key}.pkl.gz"
            
            if not path.exists():
                return None
            
            with gzip.open(path, "rb") as f:
                return pickle.load(f)
                
        except Exception as e:
            logger.error(f"Error loading binary state {category}/{key}: {e}")
            return None


class SQLiteStore(StateStore):
    """
    SQLite-based state storage.
    
    Good for structured data and queries.
    """
    
    def __init__(self, db_path: str = "./data/state.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        logger.info(f"SQLiteStore initialized at {self.db_path}")
    
    def _init_db(self) -> None:
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS state (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    category TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(category, key)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_state_category_key 
                ON state(category, key)
            """)
            conn.commit()
    
    def save(self, key: str, value: Any, category: str = "default") -> bool:
        """Save value to SQLite."""
        try:
            json_value = json.dumps(value, default=str)
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO state (category, key, value, updated_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(category, key) DO UPDATE SET
                        value = excluded.value,
                        updated_at = CURRENT_TIMESTAMP
                """, (category, key, json_value))
                conn.commit()
            
            logger.debug(f"Saved state to SQLite: {category}/{key}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving state to SQLite {category}/{key}: {e}")
            return False
    
    def load(self, key: str, category: str = "default") -> Optional[Any]:
        """Load value from SQLite."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT value FROM state WHERE category = ? AND key = ?",
                    (category, key)
                )
                row = cursor.fetchone()
            
            if row:
                return json.loads(row[0])
            return None
            
        except Exception as e:
            logger.error(f"Error loading state from SQLite {category}/{key}: {e}")
            return None
    
    def delete(self, key: str, category: str = "default") -> bool:
        """Delete from SQLite."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "DELETE FROM state WHERE category = ? AND key = ?",
                    (category, key)
                )
                conn.commit()
            
            logger.debug(f"Deleted state from SQLite: {category}/{key}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting state from SQLite {category}/{key}: {e}")
            return False
    
    def exists(self, key: str, category: str = "default") -> bool:
        """Check if key exists in SQLite."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT 1 FROM state WHERE category = ? AND key = ?",
                    (category, key)
                )
                return cursor.fetchone() is not None
                
        except Exception as e:
            logger.error(f"Error checking existence in SQLite {category}/{key}: {e}")
            return False
    
    def list_keys(self, category: str = "default") -> List[str]:
        """List all keys in a category."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT key FROM state WHERE category = ?",
                    (category,)
                )
                return [row[0] for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Error listing keys in SQLite {category}: {e}")
            return []
    
    def clear(self, category: str = "default") -> bool:
        """Clear all data in a category."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "DELETE FROM state WHERE category = ?",
                    (category,)
                )
                conn.commit()
            
            logger.info(f"Cleared category in SQLite: {category}")
            return True
            
        except Exception as e:
            logger.error(f"Error clearing category in SQLite {category}: {e}")
            return False
    
    def query(
        self,
        category: str,
        like_pattern: Optional[str] = None,
        limit: int = 100,
    ) -> List[Tuple[str, Any]]:
        """Query keys matching a pattern."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                if like_pattern:
                    cursor = conn.execute(
                        "SELECT key, value FROM state WHERE category = ? AND key LIKE ? LIMIT ?",
                        (category, like_pattern, limit)
                    )
                else:
                    cursor = conn.execute(
                        "SELECT key, value FROM state WHERE category = ? LIMIT ?",
                        (category, limit)
                    )
                
                return [(row[0], json.loads(row[1])) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Error querying SQLite: {e}")
            return []
