#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Naver Image Handler
------------------
This module provides functionality for handling Naver product images in Excel files.
It includes validation, transformation, and cleanup of image data.
"""

import os
import logging
from pathlib import Path
import pandas as pd
from typing import Dict, Any, Optional, Union
import json
import re

logger = logging.getLogger(__name__)

def fix_naver_image_data(img_data: Union[Dict[str, Any], str, None]) -> Optional[Dict[str, Any]]:
    """
    Fix and validate Naver image data structure.
    
    Args:
        img_data: Raw image data that could be a dictionary, string, or None
        
    Returns:
        dict: Cleaned and validated image data dictionary or None if invalid
    """
    if img_data is None or pd.isna(img_data) or img_data == '-':
        return None
        
    # Convert string representation of dict to actual dict if needed
    if isinstance(img_data, str):
        try:
            if img_data.strip().startswith('{'):
                img_data = eval(img_data)  # Safe since we checked it starts with '{'
            else:
                # If it's a URL string, create a basic image data structure
                if img_data.startswith(('http://', 'https://')):
                    img_data = {'url': img_data}
                else:
                    return None
        except:
            return None
            
    if not isinstance(img_data, dict):
        return None
        
    # Ensure required fields exist
    cleaned_data = {
        'url': img_data.get('url', ''),
        'local_path': img_data.get('local_path', ''),
        'product_url': img_data.get('product_url', ''),
        'score': img_data.get('score', img_data.get('similarity', 0.0)),
        'source': 'naver'
    }
    
    # Validate URL
    if not cleaned_data['url'] or not isinstance(cleaned_data['url'], str):
        return None
    if not cleaned_data['url'].startswith(('http://', 'https://')):
        return None
        
    # Remove any invalid local paths
    if cleaned_data['local_path'] and not os.path.exists(cleaned_data['local_path']):
        cleaned_data['local_path'] = ''
        
    # Validate similarity score
    if not isinstance(cleaned_data['score'], (int, float)):
        cleaned_data['score'] = 0.0
    cleaned_data['score'] = float(cleaned_data['score'])
    
    # Keep additional metadata if present
    for key in ['original_path', 'timestamp', 'metadata']:
        if key in img_data:
            cleaned_data[key] = img_data[key]
            
    return cleaned_data

class NaverImageHandler:
    """
    Handles processing and validation of Naver product images in Excel data.
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize the NaverImageHandler.
        
        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}
        self.image_dir = Path(self.config.get('image_dir', 'C:\\RPA\\Image\\Main\\Naver'))
        self.image_dir.mkdir(parents=True, exist_ok=True)
        
        # Configure minimum similarity score
        self.min_similarity_score = float(self.config.get('min_similarity_score', 0.4))
        
    def fix_image_data_in_dataframe(self, df: pd.DataFrame, naver_img_column: str = '네이버 이미지') -> pd.DataFrame:
        """
        Fix and validate all Naver image data in a DataFrame.
        
        Args:
            df: Input DataFrame
            naver_img_column: Column name containing Naver image data
            
        Returns:
            DataFrame: DataFrame with fixed image data
        """
        if naver_img_column not in df.columns:
            return df
            
        df = df.copy()
        
        # Process each row
        for idx in range(len(df)):
            img_data = df.loc[idx, naver_img_column]
            fixed_data = fix_naver_image_data(img_data)
            
            # Remove low similarity images
            if fixed_data and fixed_data.get('score', 0) < self.min_similarity_score:
                fixed_data = None
                
            df.loc[idx, naver_img_column] = fixed_data if fixed_data else None
            
        return df
        
    def transform_for_upload(self, df: pd.DataFrame, 
                           result_column: str = '네이버 이미지',
                           upload_column: str = '네이버쇼핑(이미지링크)') -> pd.DataFrame:
        """
        Transform image data from result file format to upload file format.
        
        Args:
            df: Input DataFrame
            result_column: Column name in result file containing image data
            upload_column: Column name in upload file for image URLs
            
        Returns:
            DataFrame: Transformed DataFrame
        """
        if result_column not in df.columns:
            return df
            
        df = df.copy()
        if upload_column not in df.columns:
            df[upload_column] = '-'
            
        # Convert image data to URLs
        for idx in range(len(df)):
            img_data = df.loc[idx, result_column]
            
            if isinstance(img_data, dict):
                # Prefer product_url if available
                url = img_data.get('product_url') or img_data.get('url')
                if url and isinstance(url, str) and url.startswith(('http://', 'https://')):
                    df.loc[idx, upload_column] = url
                else:
                    df.loc[idx, upload_column] = '-'
            else:
                df.loc[idx, upload_column] = '-'
                
        return df 