"""Redis client utility for the NiFi orchestrator.

Provides a singleton `RedisClient` with connection pooling and a few
high-level helpers for caching integration metadata.

Dependencies: redis-py (>=5.0)
"""

from __future__ import annotations

import json
from typing import Optional, Dict
import threading

import redis

from ..config import config
from ..logging import get_logger


class RedisClient:
    """Singleton Redis client with connection pooling and convenience helpers.

    Usage:
        client = RedisClient.get_instance()
        client.set("foo", "bar", ex=60)
        value = client.get("foo")
    """

    _instance: Optional["RedisClient"] = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        self.logger = get_logger(__name__)

        self.logger.info(
            f"Initializing Redis connection pool to {config.redis_host}:{config.redis_port}"
        )

        # Build connection kwargs
        pool_kwargs = {
            "host": config.redis_host,
            "port": config.redis_port,
            "db": config.redis_db,
            "decode_responses": True,  # str in/out
        }

        if config.redis_password:
            pool_kwargs["password"] = config.redis_password
        if config.redis_ssl:
            pool_kwargs["ssl"] = True

        # Create pool + client
        self._pool = redis.ConnectionPool(**pool_kwargs)
        self._client = redis.Redis(connection_pool=self._pool)

        # Probe connection eagerly so we fail-fast
        try:
            self._client.ping()
            self.logger.info("Redis connection established successfully")
        except Exception:
            # Re-raise so upstream backoff/retry can kick in
            self.logger.error("Failed to connect to Redis")
            raise

    # ---------- Singleton API ----------
    @classmethod
    def get_instance(cls) -> "RedisClient":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    # ---------- Core KV / Hash Helpers ----------
    def hgetall(self, key: str) -> Dict[str, str]:
        try:
            return self._client.hgetall(key)
        except Exception as e:
            self.logger.error(f"Redis hgetall failed for key='{key}': {e}")
            raise

    def hset(self, hash: str, key: str, mapping: Dict[str, str]) -> None:
        try:
            self._client.hset(name=key, key=hash, value=json.dumps(mapping))
        except Exception as e:
            self.logger.error(f"Redis hset failed for key='{key}': {e}")
            raise

    def set(self, key: str, value: str, ex: Optional[int] = None) -> None:
        try:
            self._client.set(name=key, value=value, ex=ex)
        except Exception as e:
            self.logger.error(f"Redis set failed for key='{key}': {e}")
            raise

    def get(self, key: str) -> Optional[str]:
        try:
            result = self._client.get(name=key)
            return result if result is not None else None
        except Exception as e:
            self.logger.error(f"Redis get failed for key='{key}': {e}")
            raise

    def hget(self, hash:str, key: str) -> Optional[str]:
        try:
            result = self._client.hget(name=key, key=hash)
            return result if result is not None else None
        except Exception as e:
            self.logger.error(f"Redis hget failed for key='{key}': {e}")
            raise

    def delete(self, key: str) -> None:
        try:
            self._client.delete(key)
        except Exception as e:
            self.logger.error(f"Redis delete failed for key='{key}': {e}")
            raise

    # ---------- Integration helpers ----------
    def _integration_key(self, tenant: str, category: str, integration: str) -> str:
        env = getattr(config, "orchestrator_env", "dev")
        cluster = getattr(config, "orchestrator_cluster", "local")
        return f"nifi:{env}:{cluster}:int:{tenant}:{category}:{integration}"

    def cache_integration(
            self,
            tenant: str,
            category: str,
            integration: str,
            data: Dict[str, str],
    ) -> None:
        """Cache integration metadata under a structured key.

        Example values could be: {"integration_pg_id": "...", "category_pg_id": "..."}
        """
        key = self._integration_key(tenant, category, integration)
        self.hset(key, data)

    def get_integration(
            self, tenant: str, category: str, integration: str
    ) -> Optional[Dict[str, str]]:
        key = self._integration_key(tenant, category, integration)
        result = self.hgetall(key)
        return result or None
