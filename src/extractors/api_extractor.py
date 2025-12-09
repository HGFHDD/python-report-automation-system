"""
API Data Extractor
"""
from typing import Any, Dict, List, Optional
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.extractors.base import BaseExtractor


class APIExtractor(BaseExtractor):
    """
    Extractor for REST APIs
    """

    def __init__(self, base_url: str, headers: Optional[Dict[str, str]] = None,
                 auth: Optional[tuple] = None, timeout: int = 30, **kwargs):
        """
        Initialize API extractor

        Args:
            base_url: Base URL of the API
            headers: Optional default headers
            auth: Optional authentication tuple (username, password)
            timeout: Request timeout in seconds
            **kwargs: Additional configuration
        """
        super().__init__(kwargs)
        self.base_url = base_url.rstrip('/')
        self.headers = headers or {}
        self.auth = auth
        self.timeout = timeout
        self.session = None

    def connect(self) -> None:
        """Initialize session with retry logic"""
        self.session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set default headers
        self.session.headers.update(self.headers)
        
        if self.auth:
            self.session.auth = self.auth
            
        self.logger.info(f"API session initialized for {self.base_url}")

    def disconnect(self) -> None:
        """Close session"""
        if self.session:
            self.session.close()
            self.session = None
            self.logger.info("API session closed")

    def extract(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Extract data from API endpoint

        Args:
            query: API endpoint path
            params: Optional query parameters

        Returns:
            DataFrame with API response data
        """
        if not self.session:
            raise ConnectionError("Session not initialized. Call connect() first.")

        url = f"{self.base_url}/{query.lstrip('/')}"
        
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            
            # Handle different response structures
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                # Check for common patterns
                if 'data' in data:
                    df = pd.DataFrame(data['data'])
                elif 'results' in data:
                    df = pd.DataFrame(data['results'])
                elif 'items' in data:
                    df = pd.DataFrame(data['items'])
                else:
                    df = pd.DataFrame([data])
            else:
                df = pd.DataFrame()
            
            self.logger.info(f"API request successful: {len(df)} rows from {url}")
            return df
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {str(e)}")
            raise

    def extract_post(self, endpoint: str, data: Dict[str, Any] = None,
                    json_data: Dict[str, Any] = None) -> pd.DataFrame:
        """
        Extract data using POST request

        Args:
            endpoint: API endpoint path
            data: Form data
            json_data: JSON body data

        Returns:
            DataFrame with response data
        """
        if not self.session:
            raise ConnectionError("Session not initialized")

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        try:
            response = self.session.post(
                url, data=data, json=json_data, timeout=self.timeout
            )
            response.raise_for_status()
            
            result = response.json()
            
            if isinstance(result, list):
                df = pd.DataFrame(result)
            elif isinstance(result, dict) and 'data' in result:
                df = pd.DataFrame(result['data'])
            else:
                df = pd.DataFrame([result])
            
            self.logger.info(f"POST request successful: {len(df)} rows")
            return df
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"POST request failed: {str(e)}")
            raise

    def extract_paginated(self, endpoint: str, page_param: str = 'page',
                         limit_param: str = 'limit', limit: int = 100,
                         max_pages: int = 100) -> pd.DataFrame:
        """
        Extract paginated data from API

        Args:
            endpoint: API endpoint path
            page_param: Name of page parameter
            limit_param: Name of limit parameter
            limit: Items per page
            max_pages: Maximum pages to fetch

        Returns:
            DataFrame with all paginated data
        """
        all_data = []
        page = 1

        while page <= max_pages:
            params = {page_param: page, limit_param: limit}
            
            try:
                df = self.extract(endpoint, params=params)
                
                if df.empty:
                    break
                    
                all_data.append(df)
                
                if len(df) < limit:
                    break
                    
                page += 1
                
            except Exception as e:
                self.logger.error(f"Pagination failed at page {page}: {str(e)}")
                break

        if all_data:
            combined = pd.concat(all_data, ignore_index=True)
            self.logger.info(f"Paginated extraction complete: {len(combined)} total rows")
            return combined
        
        return pd.DataFrame()

    def set_bearer_token(self, token: str) -> None:
        """
        Set Bearer token for authentication

        Args:
            token: Bearer token string
        """
        self.headers['Authorization'] = f'Bearer {token}'
        if self.session:
            self.session.headers['Authorization'] = f'Bearer {token}'

    def set_api_key(self, key: str, header_name: str = 'X-API-Key') -> None:
        """
        Set API key header

        Args:
            key: API key string
            header_name: Header name for the API key
        """
        self.headers[header_name] = key
        if self.session:
            self.session.headers[header_name] = key
