import requests
from typing import Dict, Optional
from .exceptions import APIError
import logging

logger = logging.getLogger(__name__)

class APIClient:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url

    def get(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Make a GET request to the API endpoint.
        
        Args:
            endpoint: API endpoint path
            params: Optional query parameters
            
        Returns:
            JSON response data
            
        Raises:
            APIError: If request fails or response is invalid
        """
        try:
            url = f"{self.base_url}{endpoint}"
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Request failed for {endpoint}: {str(e)}")
            raise APIError(f"Error fetching data from {endpoint}: {str(e)}")
        except ValueError as e:
            logger.error(f"Invalid JSON response from {endpoint}: {str(e)}")
            raise APIError(f"Invalid response from {endpoint}: {str(e)}")