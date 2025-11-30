"""
Caching layer with Redis and in-memory fallback
Improves performance for repeated operations
"""

import os
import json
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Any, Callable
from functools import wraps
from dataclasses import dataclass
from cachetools import TTLCache, LRUCache
from rich.console import Console

console = Console()

# Try to import Redis
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


@dataclass
class CacheEntry:
    """A cached value with metadata"""
    value: Any
    created_at: datetime
    expires_at: Optional[datetime] = None
    hits: int = 0


class CacheManager:
    """
    Multi-tier caching with Redis (distributed) and in-memory (local).
    Provides decorator-based caching for functions.
    """
    
    def __init__(
        self,
        redis_url: Optional[str] = None,
        default_ttl: int = 3600,
        max_memory_items: int = 1000
    ):
        self.default_ttl = default_ttl
        self.redis_client = None
        self.redis_available = False
        
        # Initialize Redis if available
        if REDIS_AVAILABLE and redis_url:
            try:
                self.redis_client = redis.from_url(
                    redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0"),
                    decode_responses=True
                )
                self.redis_client.ping()
                self.redis_available = True
                console.print("[green]Redis cache connected[/green]")
            except Exception as e:
                console.print(f"[yellow]Redis unavailable, using memory cache: {e}[/yellow]")
        
        # In-memory cache as fallback/L1
        self.memory_cache = TTLCache(maxsize=max_memory_items, ttl=default_ttl)
        self.lru_cache = LRUCache(maxsize=max_memory_items)
        
        # Cache statistics
        self.stats = {
            "hits": 0,
            "misses": 0,
            "sets": 0,
            "deletes": 0
        }
    
    def _make_key(self, key: str, namespace: str = "default") -> str:
        """Create a namespaced cache key"""
        return f"snowlink:{namespace}:{key}"
    
    def _hash_key(self, *args, **kwargs) -> str:
        """Create a hash key from arguments"""
        content = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True, default=str)
        return hashlib.md5(content.encode()).hexdigest()
    
    def get(self, key: str, namespace: str = "default") -> Optional[Any]:
        """Get a value from cache"""
        full_key = self._make_key(key, namespace)
        
        # Try memory cache first (L1)
        if full_key in self.memory_cache:
            self.stats["hits"] += 1
            return self.memory_cache[full_key]
        
        # Try Redis (L2)
        if self.redis_available:
            try:
                value = self.redis_client.get(full_key)
                if value:
                    self.stats["hits"] += 1
                    # Populate L1 cache
                    parsed = json.loads(value)
                    self.memory_cache[full_key] = parsed
                    return parsed
            except Exception:
                pass
        
        self.stats["misses"] += 1
        return None
    
    def set(
        self,
        key: str,
        value: Any,
        namespace: str = "default",
        ttl: Optional[int] = None
    ):
        """Set a value in cache"""
        full_key = self._make_key(key, namespace)
        ttl = ttl or self.default_ttl
        
        # Set in memory cache
        self.memory_cache[full_key] = value
        
        # Set in Redis
        if self.redis_available:
            try:
                serialized = json.dumps(value, default=str)
                self.redis_client.setex(full_key, ttl, serialized)
            except Exception:
                pass
        
        self.stats["sets"] += 1
    
    def delete(self, key: str, namespace: str = "default"):
        """Delete a value from cache"""
        full_key = self._make_key(key, namespace)
        
        # Remove from memory
        self.memory_cache.pop(full_key, None)
        self.lru_cache.pop(full_key, None)
        
        # Remove from Redis
        if self.redis_available:
            try:
                self.redis_client.delete(full_key)
            except Exception:
                pass
        
        self.stats["deletes"] += 1
    
    def clear_namespace(self, namespace: str):
        """Clear all keys in a namespace"""
        pattern = f"snowlink:{namespace}:*"
        
        # Clear memory cache
        keys_to_delete = [k for k in self.memory_cache.keys() if k.startswith(f"snowlink:{namespace}:")]
        for key in keys_to_delete:
            self.memory_cache.pop(key, None)
        
        # Clear Redis
        if self.redis_available:
            try:
                cursor = 0
                while True:
                    cursor, keys = self.redis_client.scan(cursor, match=pattern, count=100)
                    if keys:
                        self.redis_client.delete(*keys)
                    if cursor == 0:
                        break
            except Exception:
                pass
    
    def cached(
        self,
        namespace: str = "default",
        ttl: Optional[int] = None,
        key_func: Optional[Callable] = None
    ):
        """
        Decorator for caching function results
        
        Usage:
            @cache.cached(namespace="schemas", ttl=3600)
            def get_schema(page_id: str) -> dict:
                ...
        """
        def decorator(func: Callable):
            @wraps(func)
            def wrapper(*args, **kwargs):
                # Generate cache key
                if key_func:
                    cache_key = key_func(*args, **kwargs)
                else:
                    cache_key = f"{func.__name__}:{self._hash_key(*args, **kwargs)}"
                
                # Try cache
                cached_value = self.get(cache_key, namespace)
                if cached_value is not None:
                    return cached_value
                
                # Execute function
                result = func(*args, **kwargs)
                
                # Cache result
                if result is not None:
                    self.set(cache_key, result, namespace, ttl)
                
                return result
            
            return wrapper
        return decorator
    
    def get_stats(self) -> dict:
        """Get cache statistics"""
        total = self.stats["hits"] + self.stats["misses"]
        hit_rate = (self.stats["hits"] / total * 100) if total > 0 else 0
        
        return {
            **self.stats,
            "hit_rate": f"{hit_rate:.1f}%",
            "memory_items": len(self.memory_cache),
            "redis_available": self.redis_available
        }
    
    def warm_cache(self, items: list[tuple[str, Any, str]]):
        """
        Pre-populate cache with items
        
        Args:
            items: List of (key, value, namespace) tuples
        """
        for key, value, namespace in items:
            self.set(key, value, namespace)
        
        console.print(f"[green]Warmed cache with {len(items)} items[/green]")


# Global cache instance
_cache: Optional[CacheManager] = None


def get_cache() -> CacheManager:
    """Get or create the global cache instance"""
    global _cache
    if _cache is None:
        _cache = CacheManager(
            redis_url=os.getenv("REDIS_URL"),
            default_ttl=int(os.getenv("CACHE_TTL", "3600"))
        )
    return _cache
