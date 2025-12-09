import time
from typing import Optional, Any, Dict
from threading import Lock


class SimpleCache:
    """Thread-safe in-memory cache with TTL support."""

    def __init__(self, ttl: int = 300):
        """
        Initialize cache.

        Args:
            ttl: Time to live in seconds
        """
        self.ttl = ttl
        self._cache: Dict[str, tuple[Any, float]] = {}
        self._lock = Lock()

    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found or expired
        """
        with self._lock:
            if key not in self._cache:
                return None

            value, timestamp = self._cache[key]

            if time.time() - timestamp > self.ttl:
                del self._cache[key]
                return None

            return value

    def set(self, key: str, value: Any) -> None:
        """
        Set value in cache.

        Args:
            key: Cache key
            value: Value to cache
        """
        with self._lock:
            self._cache[key] = (value, time.time())

    def invalidate(self, key: str) -> None:
        """
        Invalidate a cache entry.

        Args:
            key: Cache key to invalidate
        """
        with self._lock:
            if key in self._cache:
                del self._cache[key]

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()

    def size(self) -> int:
        """Get number of items in cache."""
        with self._lock:
            return len(self._cache)
