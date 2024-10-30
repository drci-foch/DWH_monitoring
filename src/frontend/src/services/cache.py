from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple, Set
import logging

logger = logging.getLogger(__name__)

class EndpointCache:
    def __init__(self):
        """Initialize an infinite TTL cache that persists until connection reset."""
        self.cache: Dict[str, Dict[str, Any]] = {}
        
    def get_cache_key(self, endpoint: str, origin_codes: Optional[List[str]] = None) -> str:
        """Generate a cache key from endpoint and sorted origin codes."""
        if origin_codes:
            return f"{endpoint}:{','.join(sorted(origin_codes))}"
        return endpoint

    def get(self, endpoint: str, origin_codes: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """Get cached data if available."""
        cache_key = self.get_cache_key(endpoint, origin_codes)
        
        if cache_key in self.cache:
            logger.debug(f"Cache hit for key: {cache_key}")
            return self.cache[cache_key]
        return None

    def set(self, endpoint: str, data: Dict[str, Any], origin_codes: Optional[List[str]] = None):
        """Cache the data with endpoint and optional origin codes as key."""
        cache_key = self.get_cache_key(endpoint, origin_codes)
        self.cache[cache_key] = data
        logger.debug(f"Cached data for key: {cache_key}")

    def is_subset_cached(self, endpoint: str, origin_codes: List[str]) -> Tuple[bool, Optional[Set[str]]]:
        """Check if the requested origin codes are a subset of any cached data."""
        requested_set = set(origin_codes)
        
        for cached_key in self.cache:
            if cached_key.startswith(f"{endpoint}:"):
                cached_origins = set(cached_key.split(':', 1)[1].split(','))
                if requested_set.issubset(cached_origins):
                    return True, cached_origins
        return False, None

    def clear(self):
        """Clear all cached data."""
        self.cache.clear()
        logger.debug("Cache cleared")