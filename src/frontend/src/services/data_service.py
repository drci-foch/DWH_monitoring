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
    generate_sample_data
)
import logging

logger = logging.getLogger(__name__)

class DataService:
    def __init__(self):
        """Initialize DataService with API client and endpoint mappings."""
        self.base_url = "http://localhost:8000"
        self.client = APIClient(self.base_url)
        self.endpoint_mapping = {
            "/api/document_counts": generate_document_counts,
            "/api/recent_document_counts": generate_document_counts,
            "/api/top_users": lambda: generate_top_users(current_year=False),
            "/api/top_users_current_year": lambda: generate_top_users(current_year=True),
            "/summary/api/summary": lambda: generate_sample_data()["summary"],
            "/archives/api/archive_status": generate_archive_sample_data,
            "/sources/document_counts_by_year": self._handle_yearly_counts,
            "api/v1/sources/recent_document_counts_by_month": self._handle_monthly_counts
        }

    def fetch_data(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Fetch data from a real API.
        
        Args:
            endpoint (str): API endpoint path
            params (Optional[Dict]): Optional query parameters
            
        Returns:
            Optional[Dict]: JSON response data or None if request fails
        """
        try:
            url = f"{self.base_url}{endpoint}"
            response = requests.get(url, params=params)
            response.raise_for_status()  # Raise an error for bad responses (4xx and 5xx)
            return response.json()  # Return the parsed JSON response
        except requests.RequestException as e:
            logger.error(f"Request failed for {endpoint}: {str(e)}")
            st.error(f"Error fetching data from {endpoint}: {str(e)}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response from {endpoint}: {str(e)}")
            st.error(f"Error decoding response from {endpoint}: {str(e)}")
            return None


    def fetch_simulated_data(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Simulate API responses with proper error handling.
        
        Args:
            endpoint (str): API endpoint to simulate
            params (Optional[Dict]): Query parameters for the request
            
        Returns:
            Optional[Dict]: Simulated data matching the API structure
        """
        try:
            endpoint = endpoint.rstrip('/')
            
            # Handle direct endpoint matches
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
        """
        Handle document counts by year endpoint.
        
        Args:
            params (Optional[Dict]): Must contain 'origin_codes' key
            
        Returns:
            Optional[Dict]: Yearly document counts or empty list
        """
        if not params or "origin_codes" not in params:
            st.warning("No origin codes provided for document counts by year")
            return []
            
        origin_codes = params["origin_codes"]
        if isinstance(origin_codes, str):
            origin_codes = origin_codes.split(',')
        return generate_document_counts_by_year(origin_codes)

    def _handle_monthly_counts(self, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Handle monthly document counts endpoint.
        
        Args:
            params (Optional[Dict]): Must contain 'origin_codes' key
            
        Returns:
            Optional[Dict]: Monthly document counts or empty list
        """
        if not params or "origin_codes" not in params:
            st.warning("No origin codes provided for recent document counts")
            return []
            
        origin_codes = params["origin_codes"]
        if isinstance(origin_codes, str):
            origin_codes = origin_codes.split(',')
        return generate_recent_document_counts_by_month(origin_codes)