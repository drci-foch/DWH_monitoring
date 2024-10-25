import streamlit as st
import requests
import json
from typing import Dict, Optional
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

class DataService:
    def __init__(self):
        """Initialize DataService with API client and endpoint mappings."""
        self.base_url = "http://localhost:8000"
        self.client = APIClient(self.base_url)
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
            "archive_status": "/api/v1/archives/api/archive_status"
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
            if use_simulation:
                return self._get_simulated_data(endpoint_key, params)
            return self._get_api_data(endpoint_key, params)
            
        except Exception as e:
            logger.error(f"Error fetching data for {endpoint_key}: {str(e)}", exc_info=True)
            st.error(f"Error fetching data: {str(e)}")
            return None

    def _get_api_data(self, endpoint_key: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Get data from real API."""
        if endpoint_key not in self.api_endpoints:
            raise ValueError(f"Unknown endpoint key: {endpoint_key}")
            
        try:
            url = f"{self.base_url}{self.api_endpoints[endpoint_key]}"
            logger.debug(f"Fetching from API: {url} with params: {params}")
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
            
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
