import streamlit as st
import json
import requests
from typing import Dict, Optional
import logging

from call_api import generate_document_counts, generate_document_counts_by_year, generate_recent_document_counts_by_month, generate_top_users, generate_archive_sample_data, generate_sample_data

# Data Fetching Functions
def fetch_data(endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
    """Fetch data from real API"""
    base_url = "http://localhost:8000"
    try:
        url = f"{base_url}{endpoint}"
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        st.error(f"Error fetching data from {endpoint}: {str(e)}")
        return None
    except json.JSONDecodeError as e:
        st.error(f"Error decoding response from {endpoint}: {str(e)}")
        return None

def fetch_simulated_data(endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
    """
    Simulate API responses with proper error handling and consistent data structures.
    
    Args:
        endpoint (str): The API endpoint to simulate
        params (Optional[Dict]): Query parameters for the request
        
    Returns:
        Optional[Dict]: Simulated data matching the API structure
    """
    try:
        # Clean the endpoint by removing trailing slash if present
        endpoint = endpoint.rstrip('/')
        
        # Map endpoints to their respective data generation functions
        # Add user endpoints to the mapping
        endpoint_mapping = {
            "/api/document_counts": lambda: generate_document_counts(),
            "/api/recent_document_counts": lambda: generate_document_counts(),
            "/api/top_users": lambda: generate_top_users(current_year=False),
            "/api/top_users_current_year": lambda: generate_top_users(current_year=True),
            "/summary/api/summary": lambda: generate_sample_data()["summary"],
            "/archives/api/archive_status": lambda: generate_archive_sample_data(),
        }
        
        
        # Handle direct endpoint matches
        if endpoint in endpoint_mapping:
            return endpoint_mapping[endpoint]()
        
        # Handle endpoints with parameters
        if endpoint == "/sources/document_counts_by_year":
            if not params or "origin_codes" not in params:
                st.warning("No origin codes provided for document counts by year")
                return []
                
            origin_codes = params["origin_codes"]
            if isinstance(origin_codes, str):
                origin_codes = origin_codes.split(',')
            return generate_document_counts_by_year(origin_codes)
            
        elif endpoint == "api/v1/sources/recent_document_counts_by_month":
            if not params or "origin_codes" not in params:
                st.warning("No origin codes provided for recent document counts")
                return []
                
            origin_codes = params["origin_codes"]
            if isinstance(origin_codes, str):
                origin_codes = origin_codes.split(',')
            return generate_recent_document_counts_by_month(origin_codes)
        
        # Handle unknown endpoints
        st.error(f"Unknown endpoint: {endpoint}")
        return None
        
    except Exception as e:
        st.error(f"Error generating simulated data for {endpoint}: {str(e)}")
        logging.error(f"Simulation error for {endpoint}: {str(e)}", exc_info=True)
        return None
    
