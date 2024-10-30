import streamlit as st
from datetime import datetime, timedelta
import requests
from typing import Dict, Optional, List, Union, Any, Tuple, Set
from ..api.client import APIClient
from ..data.generators import (
    generate_document_counts,
    generate_document_counts_by_year,
    generate_recent_document_counts_by_month,
    generate_top_users,
    generate_archive_sample_data,
    generate_sample_data,
    generate_document_metrics,
)
import logging

logger = logging.getLogger(__name__)


class DataCache:
    def __init__(self, ttl_seconds: int = 3600):  # 1 hour default TTL
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.timestamps: Dict[str, datetime] = {}
        self.ttl = timedelta(seconds=ttl_seconds)

    def get_cache_key(self, endpoint: str, origin_codes: List[str]) -> str:
        """Generate a cache key from endpoint and sorted origin codes"""
        return f"{endpoint}:{','.join(sorted(origin_codes))}"

    def get(self, endpoint: str, origin_codes: List[str]) -> Optional[Dict[str, Any]]:
        """Get cached data if available and not expired"""
        cache_key = self.get_cache_key(endpoint, origin_codes)
        
        if cache_key not in self.cache:
            return None

        if datetime.now() - self.timestamps[cache_key] > self.ttl:
            del self.cache[cache_key]
            del self.timestamps[cache_key]
            return None

        logger.debug(f"Cache hit for key: {cache_key}")
        return self.cache[cache_key]

    def set(self, endpoint: str, origin_codes: List[str], data: Dict[str, Any]):
        """Cache the data with endpoint and origin codes as key"""
        cache_key = self.get_cache_key(endpoint, origin_codes)
        self.cache[cache_key] = data
        self.timestamps[cache_key] = datetime.now()
        logger.debug(f"Cached data for key: {cache_key}")

    def is_subset_cached(self, endpoint: str, origin_codes: List[str]) -> Tuple[bool, Optional[Set[str]]]:
        """Check if the requested origin codes are a subset of any cached data"""
        requested_set = set(origin_codes)
        
        for cached_key in self.cache:
            if cached_key.startswith(f"{endpoint}:"):
                cached_origins = set(cached_key.split(':', 1)[1].split(','))
                if requested_set.issubset(cached_origins):
                    if datetime.now() - self.timestamps[cached_key] <= self.ttl:
                        return True, cached_origins
        
        return False, None
    
class DataService:
    def __init__(self):
        """Initialize DataService with API client and endpoint mappings."""
        self.base_url = "http://localhost:8000"
        self.client = APIClient(self.base_url)
        self.cache = DataCache()
        self.available_origins_cache: Optional[List[str]] = None
        self.setup_endpoints()

    def setup_endpoints(self):
        """Setup both API endpoints and their simulation mappings."""
        # API endpoints
        self.api_endpoints = {
            "summary": "/api/v1/summary/api/summary",
            "document_metrics": "/api/v1/documents/api/document_metrics",
            "document_counts": "/api/v1/documents/api/document_counts",
            "recent_document_counts": "/api/v1/documents/api/recent_document_counts",
            "top_users": "/api/v1/users/api/top_users",
            "top_users_current_year": "/api/v1/users/api/top_users_current_year",
            "document_counts_by_year": "/api/v1/sources/api/document_counts_by_year",
            "recent_document_counts_by_month": "/api/v1/sources/api/recent_document_counts_by_month",
            "archive_status": "/api/v1/archives/api/archive_status",
            "available_origins": "/api/v1/sources/api/available_origins"  # New endpoint
        }

        # Simulation data generators
        self.simulation_handlers = {
            "summary": lambda: generate_sample_data()["summary"],
            "document_metrics": generate_document_metrics,
            "document_counts": generate_document_counts,
            "recent_document_counts": generate_document_counts,
            "top_users": lambda: generate_top_users(current_year=False),
            "top_users_current_year": lambda: generate_top_users(current_year=True),
            "document_counts_by_year": self._handle_yearly_counts,
            "recent_document_counts_by_month": self._handle_monthly_counts,
            "archive_status": generate_archive_sample_data
        }

    async def get_available_origins(self, force_refresh: bool = False) -> List[str]:
        """
        Get list of available origin codes from the API.
        
        Args:
            force_refresh: Whether to force refresh the cache
            
        Returns:
            List[str]: List of available origin codes
        """
        if self.available_origins_cache is None or force_refresh:
            try:
                response = self._get_api_data("available_origins")
                self.available_origins_cache = response
            except requests.RequestException as e:
                logger.error(f"Failed to fetch available origins: {str(e)}")
                return []
        return self.available_origins_cache

    def validate_origin_codes(self, origin_codes: Union[str, List[str]]) -> List[str]:
        """
        Validate and format origin codes.
        
        Args:
            origin_codes: Origin codes as string or list
            
        Returns:
            List[str]: List of validated origin codes
        """
        if isinstance(origin_codes, str):
            origin_codes = [code.strip() for code in origin_codes.split(",") if code.strip()]
        
        if not origin_codes:
            raise ValueError("No valid origin codes provided")
            
        return origin_codes

    def fetch_data(self, endpoint_key: str, use_simulation: bool = False, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Fetch data either from API or simulation based on the use_simulation flag.
        
        Args:
            endpoint_key: Key for the endpoint to use
            use_simulation: Whether to use simulated data
            params: Optional parameters for the API call
            
        Returns:
            Optional[Dict]: The fetched or simulated data
        """
        try:
            # Validate origin codes if present in params
            if params and "origin_codes" in params:
                params["origin_codes"] = ",".join(self.validate_origin_codes(params["origin_codes"]))

            if use_simulation:
                return self._get_simulated_data(endpoint_key, params)
            return self._get_api_data(endpoint_key, params)
            
        except ValueError as e:
            logger.error(f"Validation error: {str(e)}")
            st.error(str(e))
            return None
        except Exception as e:
            logger.error(f"Error fetching data for {endpoint_key}: {str(e)}", exc_info=True)
            st.error(f"Error fetching data: {str(e)}")
            return None

    def _filter_data_for_origins(self, data: Dict[str, Any], origin_codes: List[str]) -> Dict[str, Any]:
        """Filter cached data for specific origin codes"""
        if isinstance(data, list):
            return [item for item in data if item.get('origin') in origin_codes]
        return data  # Return as-is if format doesn't match expectations

    def validate_and_clean_origin_codes(self, origin_codes: Union[str, List[str]]) -> List[str]:
        """Validate and clean origin codes"""
        if isinstance(origin_codes, str):
            origin_codes = [code.strip() for code in origin_codes.split(",") if code.strip()]
        
        # Remove duplicates while preserving order
        return list(dict.fromkeys(origin_codes))


    def _get_api_data(self, endpoint_key: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Get data from API with caching"""
        if endpoint_key not in self.api_endpoints:
            raise ValueError(f"Unknown endpoint key: {endpoint_key}")

        try:
            # Check cache first
            if params and "origin_codes" in params:
                origin_codes = [code.strip() for code in params["origin_codes"].split(",")]
                
                # Try to get exact cache match
                cached_data = self.cache.get(endpoint_key, origin_codes)
                if cached_data is not None:
                    logger.debug(f"Returning cached data for {endpoint_key}")
                    return cached_data

                # Check if request is subset of cached data
                is_subset, cached_set = self.cache.is_subset_cached(endpoint_key, origin_codes)
                if is_subset and cached_set:
                    logger.debug(f"Request is subset of cached data for {endpoint_key}")
                    cached_data = self.cache.get(endpoint_key, list(cached_set))
                    if cached_data is not None:
                        # Filter cached data for requested origins
                        filtered_data = self._filter_data_for_origins(cached_data, origin_codes)
                        return filtered_data

            # If not in cache, fetch from API
            url = f"{self.base_url}{self.api_endpoints[endpoint_key]}"
            logger.debug(f"Fetching from API: {url} with params: {params}")
            
            response = requests.get(url, params=params)
            
            if response.status_code == 400:
                error_detail = response.json().get("detail", "Bad request")
                raise ValueError(f"API Error: {error_detail}")
                
            response.raise_for_status()
            data = response.json()

            # Cache the response
            if params and "origin_codes" in params:
                self.cache.set(endpoint_key, origin_codes, data)

            return data
            
        except requests.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            raise
            

    def _get_simulated_data(self, endpoint_key: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Get simulated data."""
        if endpoint_key not in self.simulation_handlers:
            raise ValueError(f"No simulation handler for: {endpoint_key}")
            
        handler = self.simulation_handlers[endpoint_key]
        if params and callable(handler):
            return handler(params)
        return handler() if callable(handler) else handler

    def _handle_yearly_counts(self, params: Optional[Dict] = None) -> Optional[Dict]:
        """Handle document counts by year endpoint."""
        if not params or "origin_codes" not in params:
            logger.warning("No origin codes provided for yearly counts")
            return []

        origin_codes = params["origin_codes"]
        if isinstance(origin_codes, str):
            origin_codes = origin_codes.split(",")
        return generate_document_counts_by_year(origin_codes)

    def _handle_monthly_counts(self, params: Optional[Dict] = None) -> Optional[Dict]:
        """Handle monthly document counts endpoint."""
        if not params or "origin_codes" not in params:
            logger.warning("No origin codes provided for monthly counts")
            return []

        origin_codes = params["origin_codes"]
        if isinstance(origin_codes, str):
            origin_codes = origin_codes.split(",")
        return generate_recent_document_counts_by_month(origin_codes)
