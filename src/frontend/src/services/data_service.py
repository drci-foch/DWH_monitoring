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
        self.setup_endpoint_mapping()

    def setup_endpoint_mapping(self):
        """Setup endpoint mapping for simulated data."""
        self.endpoint_mapping = {
            "/api/document_counts": generate_document_counts,
            "/api/document_metrics": generate_document_metrics,
            "/api/recent_document_counts": generate_document_counts,
            "/api/top_users": lambda: generate_top_users(current_year=False),
            "/api/top_users_current_year": lambda: generate_top_users(
                current_year=True
            ),
            "/api/summary": lambda: generate_sample_data()["summary"],
            "/archives/api/archive_status": generate_archive_sample_data,
            "/sources/document_counts_by_year": self._handle_yearly_counts,
            "api/v1/sources/recent_document_counts_by_month": self._handle_monthly_counts,
        }

    def fetch_data(
        self, endpoint: str, params: Optional[Dict] = None
    ) -> Optional[Dict]:
        """Fetch data from a real API."""
        try:
            url = f"{self.base_url}{endpoint}"
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Request failed for {endpoint}: {str(e)}")
            st.error(f"Error fetching data from {endpoint}: {str(e)}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response from {endpoint}: {str(e)}")
            st.error(f"Error decoding response from {endpoint}: {str(e)}")
            return None

    def fetch_simulated_data(
        self, endpoint: str, params: Optional[Dict] = None
    ) -> Optional[Dict]:
        """Simulate API responses with proper error handling."""
        try:
            endpoint = endpoint.rstrip("/")

            if endpoint in self.endpoint_mapping:
                handler = self.endpoint_mapping[endpoint]
                if params and callable(handler):
                    return handler(params)
                return handler() if callable(handler) else handler

            logger.warning(f"Unknown endpoint: {endpoint}")
            st.error(f"Unknown endpoint: {endpoint}")
            return None

        except Exception as e:
            logger.error(f"Simulation error for {endpoint}: {str(e)}", exc_info=True)
            st.error(f"Error generating simulated data for {endpoint}: {str(e)}")
            return None

    def _handle_yearly_counts(self, params: Optional[Dict] = None) -> Optional[Dict]:
        """Handle document counts by year endpoint."""
        if not params or "origin_codes" not in params:
            st.warning("No origin codes provided for document counts by year")
            return []

        origin_codes = params["origin_codes"]
        if isinstance(origin_codes, str):
            origin_codes = origin_codes.split(",")
        return generate_document_counts_by_year(origin_codes)

    def _handle_monthly_counts(self, params: Optional[Dict] = None) -> Optional[Dict]:
        """Handle monthly document counts endpoint."""
        if not params or "origin_codes" not in params:
            st.warning("No origin codes provided for recent document counts")
            return []

        origin_codes = params["origin_codes"]
        if isinstance(origin_codes, str):
            origin_codes = origin_codes.split(",")
        return generate_recent_document_counts_by_month(origin_codes)
