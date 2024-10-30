import streamlit as st
from datetime import datetime
import requests
import pandas as pd
from typing import Dict, Optional, List, Union, Any
from .cache import EndpointCache
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
        self.base_url = "http://localhost:8000"
        self.client = APIClient(self.base_url)
        # Initialize cache in session state if not exists
        if 'endpoint_cache' not in st.session_state:
            st.session_state.endpoint_cache = {}
            
        self.setup_endpoints()
        self.available_origins_cache: Optional[List[str]] = None

    def setup_endpoints(self):
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
            "available_origins": "/api/v1/sources/api/available_origins",
            "pmsi": "/api/v1/pmsi/api/pmsi",  
            "users_stats": "/api/v1/users/api/users_stats"  
        }

        self.simulation_handlers = {
            "summary": lambda: generate_sample_data()["summary"],
            "document_metrics": generate_document_metrics,
            "document_counts": generate_document_counts,
            "recent_document_counts": generate_document_counts,
            "top_users": lambda: generate_top_users(current_year=False),
            "top_users_current_year": lambda: generate_top_users(current_year=True),
            "document_counts_by_year": self._handle_yearly_counts,
            "recent_document_counts_by_month": self._handle_monthly_counts,
            "archive_status": generate_archive_sample_data,
            "pmsi": self._handle_pmsi_simulation,  
            "users_stats": self._handle_user_stats_simulation
        }

    async def get_available_origins(self, force_refresh: bool = False) -> List[str]:
        if not force_refresh:
            cached_origins = self.cache.get("available_origins")
            if cached_origins is not None:
                return cached_origins

        try:
            response = self._get_api_data("available_origins")
            self.cache.set("available_origins", response)
            return response
        except requests.RequestException as e:
            logger.error(f"Failed to fetch available origins: {str(e)}")
            return []

    def validate_origin_codes(self, origin_codes: Union[str, List[str]]) -> List[str]:
        if isinstance(origin_codes, str):
            origin_codes = [code.strip() for code in origin_codes.split(",") if code.strip()]
        
        if not origin_codes:
            raise ValueError("No valid origin codes provided")
            
        return origin_codes

    def fetch_data(self, endpoint_key: str, use_simulation: bool = False, params: Optional[Dict] = None) -> Optional[Dict]:
        try:
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
        if isinstance(data, list):
            return [item for item in data if item.get('origin') in origin_codes]
        return data

    def validate_and_clean_origin_codes(self, origin_codes: Union[str, List[str]]) -> List[str]:
        if isinstance(origin_codes, str):
            origin_codes = [code.strip() for code in origin_codes.split(",") if code.strip()]
        return list(dict.fromkeys(origin_codes))

    def _aggregate_data(self, data: List[Dict], aggregate: bool, data_type: str = 'yearly') -> List[Dict]:
        if not aggregate:
            return data
            
        aggregated_data = []
        easily_sums = {}
        doc_externe_sums = {}
        
        if data_type == 'yearly':
            # Current yearly aggregation logic
            for entry in data:
                origin = entry['document_origin_code']
                year = entry['year']
                count = entry['count']
                
                if origin.startswith('Easil'):
                    easily_sums[year] = easily_sums.get(year, 0) + count
                elif origin.startswith('DOC_EXTERN'):
                    doc_externe_sums[year] = doc_externe_sums.get(year, 0) + count
                else:
                    aggregated_data.append(entry)
            
            # Add aggregated yearly data
            for year in sorted(easily_sums):
                aggregated_data.append({
                    'document_origin_code': 'Easily_ALL',
                    'year': year,
                    'count': easily_sums[year]
                })
                
            for year in sorted(doc_externe_sums):
                aggregated_data.append({
                    'document_origin_code': 'DOC_EXTERNE_ALL',
                    'year': year,
                    'count': doc_externe_sums[year]
                })
                
        else:
            # Monthly data comes as a DataFrame
            df = pd.DataFrame(data)
            df['month'] = pd.to_datetime(df['month'])
            
            # Group by month and aggregate
            easily_mask = df['document_origin_code'].str.startswith('Easil')
            doc_externe_mask = df['document_origin_code'].str.startswith('DOC_EXTERN')
            
            # Keep non-aggregated records
            aggregated_data = df[~(easily_mask | doc_externe_mask)].to_dict('records')
            
            # Aggregate Easily data
            if easily_mask.any():
                easily_agg = df[easily_mask].groupby('month')['count'].sum().reset_index()
                easily_agg['document_origin_code'] = 'Easily_ALL'
                aggregated_data.extend(easily_agg.to_dict('records'))
                
            # Aggregate DOC_EXTERNE data
            if doc_externe_mask.any():
                doc_externe_agg = df[doc_externe_mask].groupby('month')['count'].sum().reset_index()
                doc_externe_agg['document_origin_code'] = 'DOC_EXTERNE_ALL'
                aggregated_data.extend(doc_externe_agg.to_dict('records'))
        
        return aggregated_data

    def _get_api_data(self, endpoint_key: str, params: Optional[Dict] = None) -> Optional[Dict]:
        if endpoint_key not in self.api_endpoints:
            raise ValueError(f"Unknown endpoint key: {endpoint_key}")

        try:
            # Extract aggregate parameter before sending to API
            aggregate = False
            if params and 'aggregate' in params:
                aggregate = params.pop('aggregate') == 'true'

            if endpoint_key in st.session_state.endpoint_cache:
                data = st.session_state.endpoint_cache[endpoint_key]
                if isinstance(data, list):
                    return self._aggregate_data(data, aggregate)
                return data

            url = f"{self.base_url}{self.api_endpoints[endpoint_key]}"
            response = requests.get(url, params=params)
            
            if response.status_code == 400:
                error_detail = response.json().get("detail", "Bad request")
                raise ValueError(f"API Error: {error_detail}")
                
            response.raise_for_status()
            data = response.json()

            # Cache the raw data
            st.session_state.endpoint_cache[endpoint_key] = data

            # Apply aggregation if needed
            if isinstance(data, list):
                return self._aggregate_data(data, aggregate)
            return data
            
        except requests.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            raise

    def _get_simulated_data(self, endpoint_key: str, params: Optional[Dict] = None) -> Optional[Dict]:
        if endpoint_key not in self.simulation_handlers:
            raise ValueError(f"No simulation handler for: {endpoint_key}")
            
        handler = self.simulation_handlers[endpoint_key]
        return handler(params) if params and callable(handler) else handler() if callable(handler) else handler

    def _handle_yearly_counts(self, params: Optional[Dict] = None) -> Optional[Dict]:
        if not params or "origin_codes" not in params:
            logger.warning("No origin codes provided for yearly counts")
            return []

        origin_codes = params["origin_codes"].split(",") if isinstance(params["origin_codes"], str) else params["origin_codes"]
        return generate_document_counts_by_year(origin_codes)

    def _handle_monthly_counts(self, params: Optional[Dict] = None) -> Optional[Dict]:
        if not params or "origin_codes" not in params:
            logger.warning("No origin codes provided for monthly counts")
            return []

        origin_codes = params["origin_codes"].split(",") if isinstance(params["origin_codes"], str) else params["origin_codes"]
        return generate_recent_document_counts_by_month(origin_codes)
    
    def _handle_pmsi_simulation(self, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Simulate PMSI data for development."""
        return {
            "last_upload": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "time_period": {
                "start_date": "2023-01-01",
                "end_date": "2024-01-01",
                "months_count": 12
            },
            "monthly_counts": [
                {"month": f"2023-{m:02d}-01", "count": 1000 + m * 100}
                for m in range(1, 13)
            ],
            "gaps": ["2023-03-01", "2023-07-01"],
            "stats": {
                "total_documents": 15000,
                "avg_monthly_documents": 1250,
                "max_monthly_documents": 2200,
                "months_with_gaps": 2
            }
        }
    

    def _handle_user_stats_simulation(self, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Simulate user statistics for development."""
        return {
            "low_activity_users_count": 25,
            "total_users": 100,
            "avg_queries": 15.5,
            "max_queries": 150,
            "min_queries": 1,
            "low_activity_percentage": 25.0,
            "low_activity_details": [
                {"name": "John Doe", "count": 1},
                {"name": "Jane Smith", "count": 2},
                {"name": "Bob Wilson", "count": 3}
            ]
        }