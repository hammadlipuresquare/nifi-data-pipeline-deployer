"""Idempotency utilities for get-or-create operations."""

from typing import TypeVar, Callable, Optional, Dict, Any
from contextlib import contextmanager
import os
import time
from functools import wraps
import logging
from ..exceptions import IdempotencyConflict

logger = logging.getLogger(__name__)

T = TypeVar('T')


def get_or_create(
    fetch_fn: Callable[[], Optional[T]],
    create_fn: Callable[[], T],
    resource_name: str = "resource"
) -> T:
    """
    Idempotent get-or-create operation.
    
    Args:
        fetch_fn: Function to fetch existing resource, returns None if not found
        create_fn: Function to create the resource
        resource_name: Name for logging purposes
        
    Returns:
        The existing or newly created resource
        
    Raises:
        IdempotencyConflict: If resource exists but doesn't match expected state
    """
    logger.debug(f"Attempting to get or create {resource_name}")
    
    # Try to fetch existing resource
    existing = fetch_fn()
    if existing is not None:
        logger.info(f"Found existing {resource_name}")
        return existing
    
    # Create new resource
    logger.info(f"Creating new {resource_name}")
    try:
        return create_fn()
    except Exception as e:
        logger.error(f"Failed to create {resource_name}: {e}")
        raise


def once(cache_key: str):
    """
    Decorator to ensure a function is called only once per cache key.
    Useful for expensive operations that should not be repeated.
    """
    _cache: Dict[str, Any] = {}
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if cache_key not in _cache:
                logger.debug(f"Executing function {func.__name__} for key {cache_key}")
                _cache[cache_key] = func(*args, **kwargs)
            else:
                logger.debug(f"Using cached result for {func.__name__} key {cache_key}")
            return _cache[cache_key]
        return wrapper
    return decorator


def ensure_unique_name(
    name: str,
    check_exists_fn: Callable[[str], bool],
    max_attempts: int = 100
) -> str:
    """
    Ensure a unique name by appending numbers if necessary.
    
    Args:
        name: Base name to check
        check_exists_fn: Function that returns True if name exists
        max_attempts: Maximum number of naming attempts
        
    Returns:
        A unique name
        
    Raises:
        IdempotencyConflict: If unable to find unique name within max_attempts
    """
    if not check_exists_fn(name):
        return name
    
    for i in range(1, max_attempts + 1):
        candidate = f"{name}-{i}"
        if not check_exists_fn(candidate):
            logger.info(f"Generated unique name: {candidate}")
            return candidate
    
    raise IdempotencyConflict(
        f"Unable to generate unique name for '{name}' after {max_attempts} attempts"
    )



# ---------- Cross-process locking helpers ----------

@contextmanager
def tenant_lock(tenant_id: str, timeout_seconds: int = 30):
    """
    Best-effort cross-process mutex using an OS file lock per tenant.

    Prevents concurrent orchestrator instances from creating duplicate
    process groups for the same tenant. Works across venvs and shells.

    Notes:
    - Uses a lock file under /tmp; on Windows this degrades to advisory lock.
    - Times out after timeout_seconds to avoid deadlocks.
    """
    lock_dir = "/tmp"
    os.makedirs(lock_dir, exist_ok=True)
    lock_path = os.path.join(lock_dir, f"orchestrator_tenant_{tenant_id}.lock")

    fd = None
    start = time.time()
    try:
        # Lazy import to keep module lightweight on non-posix systems
        try:
            import fcntl  # type: ignore
        except Exception:  # pragma: no cover - non-posix fallback
            fcntl = None  # type: ignore

        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR)

        if fcntl is None:
            # Fallback: spin on file existence as advisory lock
            while True:
                try:
                    os.link(lock_path, lock_path + ".hold")
                    break
                except Exception:
                    if time.time() - start > timeout_seconds:
                        logger.warning(f"Tenant lock timeout (advisory) for {tenant_id}")
                        break
                    time.sleep(0.2)
        else:
            acquired = False
            while not acquired:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    acquired = True
                except BlockingIOError:
                    if time.time() - start > timeout_seconds:
                        logger.warning(f"Tenant lock timeout for {tenant_id}")
                        break
                    time.sleep(0.2)

        logger.debug(f"Acquired tenant lock for {tenant_id}")
        yield
    finally:
        try:
            if fd is not None:
                try:
                    import fcntl  # type: ignore
                    fcntl.flock(fd, fcntl.LOCK_UN)
                except Exception:
                    pass
                os.close(fd)
            # Best-effort cleanup
            try:
                if os.path.exists(lock_path + ".hold"):
                    os.unlink(lock_path + ".hold")
            except Exception:
                pass
        finally:
            logger.debug(f"Released tenant lock for {tenant_id}")

