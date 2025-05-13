#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Naver Image Handler
------------------
Dedicated module for handling Naver product images with improved reliability.
This module replaces the previous fragmented Naver image handling code.

Key improvements:
1. Robust image URL extraction and validation
2. Proper image storage with consistent naming
3. Deep verification of image-product matching
4. Fallback mechanisms for handling edge cases
"""

import os
import re
import logging
import asyncio
import aiohttp
import hashlib
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Union, Any
from PIL import Image
import io
import shutil
import time

# Configure logging
logger = logging.getLogger(__name__)

class NaverImageHandler:
    """
    Comprehensive handler for Naver product images with improved reliability.
    """
    
    def __init__(self, config=None):
        """
        Initialize the Naver Image Handler.
        
        Args:
            config: Configuration object (configparser.ConfigParser instance)
        """
        self.config = config
        self.image_dir = self._get_image_directory()
        self._ensure_image_directory()
        
        # Configurable settings
        self.max_retries = self._get_config_int('Network', 'max_retries', 3)
        self.timeout = self._get_config_int('Network', 'timeout', 30)
        self.min_image_size = self._get_config_int('ImageMatching', 'min_image_size', 1000)
        self.verify_images = self._get_config_bool('Matching', 'verify_image_urls', True)
        
        # Set up file operation semaphore to prevent race conditions
        self.file_semaphore = asyncio.Semaphore(1)
        
        # Track processed images for deduplication
        self.processed_images = {}
        
    def _get_image_directory(self) -> Path:
        """Get the Naver image directory from config."""
        if self.config:
            try:
                base_dir = self.config.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main')
                return Path(base_dir) / 'Naver'
            except Exception as e:
                logger.warning(f"Error getting image directory from config: {e}")
        
        # Default path if config doesn't provide one
        return Path('C:\\RPA\\Image\\Main\\Naver')
    
    def _ensure_image_directory(self):
        """Create the Naver image directory if it doesn't exist."""
        try:
            self.image_dir.mkdir(parents=True, exist_ok=True)
            if not os.access(self.image_dir, os.W_OK):
                logger.warning(f"No write permission to {self.image_dir}")
                # Try alternative location
                alt_dir = Path('C:\\RPA\\Image\\Main\\Naver')
                alt_dir.mkdir(parents=True, exist_ok=True)
                self.image_dir = alt_dir
                logger.info(f"Using alternative image directory: {self.image_dir}")
        except Exception as e:
            logger.error(f"Failed to create Naver image directory: {e}")
            # Use a fallback directory
            self.image_dir = Path('C:\\RPA\\Image\\Main\\Naver')
            self.image_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_config_int(self, section: str, option: str, default: int) -> int:
        """Get integer config value with fallback to default."""
        if not self.config:
            return default
        try:
            return self.config.getint(section, option, fallback=default)
        except Exception:
            return default
    
    def _get_config_bool(self, section: str, option: str, default: bool) -> bool:
        """Get boolean config value with fallback to default."""
        if not self.config:
            return default
        try:
            return self.config.getboolean(section, option, fallback=default)
        except Exception:
            return default
    
    def extract_product_id_from_url(self, url: str) -> Optional[str]:
        """
        Extract product ID from Naver URL using multiple patterns.
        
        Args:
            url: Naver product URL
            
        Returns:
            Product ID if found, None otherwise
        """
        if not url or not isinstance(url, str):
            return None
        
        # Multiple patterns for different Naver URL formats
        patterns = [
            r'main_(\d+)/(\d+)',  # Standard pattern: main_1234567/1234567.jpg
            r'cat_id=(\d+)',      # Catalog ID pattern
            r'products/(\d+)',    # Product detail page pattern
            r'id=(\d+)',          # Simple ID pattern
            r'_([0-9]{8,})\.jpg', # Image filename pattern
            r'_([0-9]{8,})\.png'  # Image filename pattern (PNG)
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        
        return None
    
    def normalize_url(self, url: str) -> Optional[str]:
        """
        Normalize Naver image URLs to a consistent format.
        Handles various URL patterns and fixes common issues.
        
        Args:
            url: Image URL to normalize
            
        Returns:
            Normalized URL or None if invalid
        """
        if not url or not isinstance(url, str):
            return None
            
        # Clean URL - replace backslashes, normalize spaces
        url = url.replace('\\', '/').strip()
        
        # Skip invalid URLs
        if not url or url == '-' or url == 'None':
            return None
            
        # Add protocol if missing
        if not url.startswith(('http://', 'https://')):
            if url.startswith('//'):
                url = f"https:{url}"
            else:
                url = f"https://{url}"
        
        # Fix potentially problematic URLs
        if "pstatic.net/front/" in url:
            logger.warning(f"Detected unreliable 'front' URL: {url}")
            # Extract product ID if possible to create a better URL
            product_id = self.extract_product_id_from_url(url)
            if product_id:
                return f"https://shopping-phinf.pstatic.net/main_{product_id}/{product_id}.jpg"
            return None  # Skip unreliable front URLs if we can't fix them
        
        return url
    
    def generate_image_filename(self, url: str, product_name: str = None) -> str:
        """
        Generate a consistent filename for a Naver image.
        
        Args:
            url: Image URL
            product_name: Optional product name for additional context
            
        Returns:
            Filename for the image
        """
        # Create a hash from URL
        url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
        
        # Product hash component (if provided)
        product_hash = ""
        if product_name:
            product_hash = hashlib.md5(product_name.encode()).hexdigest()[:6]
            
        # Extract extension from URL
        ext = '.jpg'  # Default extension
        if url.lower().endswith('.png'):
            ext = '.png'
        elif url.lower().endswith('.gif'):
            ext = '.gif'
        
        # Add unique identifiers to prevent collisions
        timestamp = int(time.time() * 1000) % 10000  # Last 4 digits of current timestamp in ms
        
        # Structure: naver_<url-hash>_<product-hash>_<timestamp><extension>
        if product_hash:
            return f"naver_{url_hash}_{product_hash}_{timestamp}{ext}"
        return f"naver_{url_hash}_{timestamp}{ext}"
    
    async def verify_image_url(self, session: aiohttp.ClientSession, url: str) -> Tuple[str, bool, Optional[str]]:
        """
        Verify if a URL is a valid Naver image.
        
        Args:
            session: aiohttp session
            url: Image URL to verify
            
        Returns:
            Tuple of (url, is_valid, error_message)
        """
        if not url:
            return url, False, "Empty URL"
            
        # Normalize URL
        url = self.normalize_url(url)
        if not url:
            return url, False, "Invalid URL format"
        
        # Skip URL verification if disabled
        if not self.verify_images:
            return url, True, None
            
        try:
            async with session.get(url, timeout=self.timeout, allow_redirects=True) as response:
                if response.status != 200:
                    return url, False, f"HTTP status {response.status}"
                
                # Check content type
                content_type = response.headers.get('Content-Type', '')
                if not content_type.startswith('image/'):
                    # Special exception for Naver images that might not have correct content type
                    if 'pstatic.net' not in url:
                        return url, False, f"Not an image (content-type: {content_type})"
                
                # Check content length
                if 'Content-Length' in response.headers:
                    content_length = int(response.headers['Content-Length'])
                    if content_length < self.min_image_size:
                        return url, False, f"Content too small: {content_length} bytes"
                
                # Read a portion of the image to verify
                chunk = await response.content.read(10240)  # Read up to 10KB
                if len(chunk) < self.min_image_size:
                    return url, False, f"Response too small: {len(chunk)} bytes"
                
                # Verify image data format
                try:
                    img = Image.open(io.BytesIO(chunk))
                    img.verify()  # Verify it's valid image data
                    return url, True, None
                except Exception as e:
                    # Sometimes Naver images don't validate properly but still work
                    if 'pstatic.net' in url:
                        logger.warning(f"Naver image validation failed but accepting: {url} - {e}")
                        return url, True, None
                    return url, False, f"Invalid image data: {e}"
                
        except asyncio.TimeoutError:
            return url, False, "Request timeout"
        except aiohttp.ClientError as e:
            return url, False, f"Connection error: {e}"
        except Exception as e:
            return url, False, f"Unexpected error: {e}"
    
    async def download_image(self, session: aiohttp.ClientSession, url: str, 
                            product_name: str = None, retry_count: int = 0) -> Tuple[str, bool, Dict[str, Any]]:
        """
        Download a Naver image and save it locally.
        
        Args:
            session: aiohttp client session
            url: Image URL to download
            product_name: Name of the product for reference
            retry_count: Current retry attempt (for internal use)
            
        Returns:
            Tuple of (url, success, image_data)
            where image_data contains:
                - local_path: Path to saved image file
                - url: Original image URL
                - source: 'naver'
                - product_name: Reference product name
                - score: Confidence score (default 0.8 for direct downloads)
        """
        # Create result dictionary with default values
        result_data = {
            'url': url,
            'local_path': None,
            'source': 'naver',
            'product_name': product_name,
            'score': 0.8  # High default score for direct downloads
        }
        
        # Normalize URL
        url = self.normalize_url(url)
        if not url:
            result_data['error'] = "Invalid URL format"
            return url, False, result_data
        
        # Generate consistent filename
        filename = self.generate_image_filename(url, product_name)
        local_path = self.image_dir / filename
        
        # Create nobg variant filename
        nobg_filename = local_path.stem + "_nobg.png"
        nobg_path = self.image_dir / nobg_filename
        
        # Check if already downloaded
        if os.path.exists(local_path) and os.path.getsize(local_path) > self.min_image_size:
            result_data['local_path'] = str(local_path)
            return url, True, result_data
            
        # Set Naver-specific headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://shopping.naver.com/',
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
        }
        
        try:
            async with session.get(url, headers=headers, timeout=self.timeout) as response:
                if response.status != 200:
                    if retry_count < self.max_retries:
                        logger.warning(f"HTTP {response.status} for {url}, retrying ({retry_count + 1}/{self.max_retries})...")
                        await asyncio.sleep(1.0 * (retry_count + 1))  # Exponential backoff
                        return await self.download_image(session, url, product_name, retry_count + 1)
                    
                    result_data['error'] = f"HTTP error {response.status}"
                    return url, False, result_data
                
                # Read image data
                data = await response.read()
                
                # Verify it's a valid image
                try:
                    img = Image.open(io.BytesIO(data))
                    img.verify()  # Verify it's valid image data
                    
                    # Get image format
                    img = Image.open(io.BytesIO(data))
                    width, height = img.size
                    
                    # Skip very small images
                    if width < 100 or height < 100:
                        if retry_count < self.max_retries:
                            logger.warning(f"Image too small ({width}x{height}), trying alternative URL...")
                            
                            # Try to find an alternative URL based on product ID
                            product_id = self.extract_product_id_from_url(url)
                            if product_id:
                                alt_url = f"https://shopping-phinf.pstatic.net/main_{product_id}/{product_id}.jpg"
                                if alt_url != url:
                                    logger.info(f"Trying alternative URL: {alt_url}")
                                    return await self.download_image(session, alt_url, product_name, retry_count + 1)
                            
                            # If no alternative URL, retry original with backoff
                            await asyncio.sleep(1.0 * (retry_count + 1))
                            return await self.download_image(session, url, product_name, retry_count + 1)
                        else:
                            logger.warning(f"Image dimensions too small: {width}x{height} for {url}")
                    
                    # Use semaphore to prevent concurrent file operations
                    async with self.file_semaphore:
                        # Create directory if needed
                        os.makedirs(self.image_dir, exist_ok=True)
                        
                        # Save image
                        with open(local_path, 'wb') as f:
                            f.write(data)
                        
                        # Create a copy for the _nobg version (will be processed by background removal later)
                        with open(nobg_path, 'wb') as f:
                            f.write(data)
                        
                        logger.debug(f"Saved Naver image to {local_path}")
                        
                        # Update result with success
                        result_data['local_path'] = str(local_path)
                        return url, True, result_data
                        
                except Exception as img_err:
                    # Some Naver images might still be usable despite validation errors
                    if 'pstatic.net' in url:
                        logger.warning(f"Naver image validation warning but proceeding: {url} - {img_err}")
                        
                        # Save the image despite validation issues
                        async with self.file_semaphore:
                            os.makedirs(self.image_dir, exist_ok=True)
                            with open(local_path, 'wb') as f:
                                f.write(data)
                            logger.debug(f"Saved Naver image despite validation issues: {local_path}")
                            
                            result_data['local_path'] = str(local_path)
                            result_data['score'] = 0.6  # Lower score for problematic images
                            return url, True, result_data
                    
                    if retry_count < self.max_retries:
                        logger.warning(f"Invalid image data for {url}: {img_err}, retrying...")
                        await asyncio.sleep(1.0 * (retry_count + 1))
                        return await self.download_image(session, url, product_name, retry_count + 1)
                    
                    result_data['error'] = f"Invalid image data: {img_err}"
                    return url, False, result_data
                    
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            if retry_count < self.max_retries:
                logger.warning(f"Connection error for {url}, retrying: {e}")
                await asyncio.sleep(1.0 * (retry_count + 1))
                return await self.download_image(session, url, product_name, retry_count + 1)
            
            result_data['error'] = f"Connection error: {e}"
            return url, False, result_data
            
        except Exception as e:
            result_data['error'] = f"Unexpected error: {e}"
            return url, False, result_data
    
    async def download_batch(self, product_data_list: List[Dict]) -> Dict[str, Dict]:
        """
        Download images for a batch of Naver products.
        
        Args:
            product_data_list: List of product data dictionaries, each containing:
                - product_name: Name of product
                - url/image_url/image_link: URL of product image
                
        Returns:
            Dictionary mapping product names to image data dictionaries
        """
        result = {}
        
        if not product_data_list:
            logger.warning("Empty product list provided to Naver image downloader")
            return result
            
        logger.info(f"Starting batch download of {len(product_data_list)} Naver images")
        
        # Configure aiohttp session
        timeout = aiohttp.ClientTimeout(total=self.timeout * 2)
        connector = aiohttp.TCPConnector(limit=5, ssl=False)
        
        # Extract image URLs from product data
        download_tasks = []
        product_to_url = {}
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            for product_data in product_data_list:
                if not isinstance(product_data, dict):
                    continue
                    
                product_name = product_data.get('product_name') or product_data.get('name') or product_data.get('original_product_name')
                if not product_name:
                    continue
                    
                # Find image URL in various possible fields
                image_url = None
                for field in ['image_url', 'url', 'image_link', 'image_path', 'image', 'img_url']:
                    if field in product_data and product_data[field]:
                        image_url = product_data[field]
                        break
                
                if not image_url:
                    # Try extracting from product URL
                    product_url = product_data.get('product_url') or product_data.get('link')
                    if product_url:
                        product_id = self.extract_product_id_from_url(product_url)
                        if product_id:
                            image_url = f"https://shopping-phinf.pstatic.net/main_{product_id}/{product_id}.jpg"
                
                if image_url:
                    normalized_url = self.normalize_url(image_url)
                    if normalized_url:
                        product_to_url[product_name] = normalized_url
                        download_tasks.append(self.download_image(session, normalized_url, product_name))
            
            if not download_tasks:
                logger.warning("No valid Naver image URLs found in product data")
                return result
                
            logger.info(f"Processing {len(download_tasks)} Naver image downloads")
            download_results = await asyncio.gather(*download_tasks, return_exceptions=True)
            
            success_count = 0
            for i, res in enumerate(download_results):
                if isinstance(res, Exception):
                    logger.error(f"Error downloading Naver image: {res}")
                    continue
                    
                url, success, image_data = res
                if success and image_data.get('local_path'):
                    success_count += 1
                    # Find product name that corresponds to this URL
                    for product_name, product_url in product_to_url.items():
                        if product_url == url:
                            result[product_name] = image_data
                            break
            
            logger.info(f"Completed Naver image batch download: {success_count}/{len(download_tasks)} successful")
        
        return result
    
    def fix_image_data_in_dataframe(self, df, naver_img_column='네이버 이미지'):
        """
        Fix Naver image data in a DataFrame to ensure consistent format.
        
        Args:
            df: Pandas DataFrame containing Naver image data
            naver_img_column: Name of column containing Naver image data
            
        Returns:
            DataFrame with fixed Naver image data
        """
        if naver_img_column not in df.columns:
            logger.warning(f"Column '{naver_img_column}' not found in DataFrame")
            return df
            
        # Process each row
        for idx in range(len(df)):
            try:
                img_data = df.loc[idx, naver_img_column]
                
                # Skip empty or invalid data
                if img_data is None or img_data == '-':
                    continue
                    
                # Handle dictionary format
                if isinstance(img_data, dict):
                    url = img_data.get('url')
                    local_path = img_data.get('local_path')
                    
                    # Fix URL if needed
                    if url:
                        normalized_url = self.normalize_url(url)
                        if normalized_url and normalized_url != url:
                            img_data['url'] = normalized_url
                            df.loc[idx, naver_img_column] = img_data
                            
                    # Check if local path exists
                    if local_path and not os.path.exists(local_path):
                        # Try to find the file with a different extension or in a different location
                        base_path = os.path.splitext(local_path)[0]
                        for ext in ['.jpg', '.png', '.jpeg']:
                            alt_path = f"{base_path}{ext}"
                            if os.path.exists(alt_path):
                                img_data['local_path'] = alt_path
                                df.loc[idx, naver_img_column] = img_data
                                logger.debug(f"Fixed local path for row {idx}: {alt_path}")
                                break
                        else:
                            # Check for _nobg version
                            nobg_path = f"{base_path}_nobg.png"
                            if os.path.exists(nobg_path):
                                img_data['local_path'] = nobg_path
                                df.loc[idx, naver_img_column] = img_data
                                logger.debug(f"Using _nobg version for row {idx}: {nobg_path}")
                
                # Handle string format (URL)
                elif isinstance(img_data, str) and img_data.startswith(('http://', 'https://')):
                    url = img_data
                    normalized_url = self.normalize_url(url)
                    
                    if normalized_url and normalized_url != url:
                        # Create dictionary structure
                        df.loc[idx, naver_img_column] = {
                            'url': normalized_url,
                            'local_path': '',
                            'source': 'naver'
                        }
                
            except Exception as e:
                logger.error(f"Error fixing Naver image data for row {idx}: {e}")
                
        return df

    def transform_for_upload(self, df, result_column='네이버 이미지', upload_column='네이버쇼핑(이미지링크)'):
        """
        Transform Naver image data from result format to upload format.
        
        Args:
            df: Pandas DataFrame containing Naver image data
            result_column: Name of column containing detailed image data
            upload_column: Name of column to store URLs for upload
            
        Returns:
            DataFrame with added upload column containing only image URLs
        """
        if result_column not in df.columns:
            logger.warning(f"Column '{result_column}' not found in DataFrame")
            return df
            
        # Create upload column if it doesn't exist
        if upload_column not in df.columns:
            df[upload_column] = '-'
            
        # Process each row
        for idx in range(len(df)):
            try:
                img_data = df.loc[idx, result_column]
                
                # Skip empty or invalid data
                if img_data is None or img_data == '-':
                    df.loc[idx, upload_column] = '-'
                    continue
                    
                # Extract URL from dictionary format
                if isinstance(img_data, dict):
                    url = img_data.get('url')
                    if url and isinstance(url, str) and url.startswith(('http://', 'https://')):
                        df.loc[idx, upload_column] = url
                    else:
                        df.loc[idx, upload_column] = '-'
                
                # Handle string format (URL)
                elif isinstance(img_data, str) and img_data.startswith(('http://', 'https://')):
                    df.loc[idx, upload_column] = img_data
                else:
                    df.loc[idx, upload_column] = '-'
                    
            except Exception as e:
                logger.error(f"Error transforming Naver image data for row {idx}: {e}")
                df.loc[idx, upload_column] = '-'
                
        return df
    
    async def ensure_local_images(self, df, naver_img_column='네이버 이미지'):
        """
        Ensure all Naver images in DataFrame have valid local paths.
        Downloads missing images if URLs are available.
        
        Args:
            df: Pandas DataFrame containing Naver image data
            naver_img_column: Name of column containing Naver image data
            
        Returns:
            DataFrame with ensured local image paths
        """
        if naver_img_column not in df.columns:
            logger.warning(f"Column '{naver_img_column}' not found in DataFrame")
            return df
            
        # Collect all image URLs that need downloading
        download_tasks = []
        row_indices = []
        
        # Configure aiohttp session
        timeout = aiohttp.ClientTimeout(total=self.timeout * 2)
        connector = aiohttp.TCPConnector(limit=5, ssl=False)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # First pass: identify images that need downloading
            for idx in range(len(df)):
                try:
                    img_data = df.loc[idx, naver_img_column]
                    
                    # Skip empty or invalid data
                    if img_data is None or img_data == '-':
                        continue
                        
                    # Handle dictionary format
                    if isinstance(img_data, dict):
                        url = img_data.get('url')
                        local_path = img_data.get('local_path')
                        product_name = img_data.get('product_name') or df.loc[idx, '상품명'] if '상품명' in df.columns else None
                        
                        # Download if we have URL but no valid local path
                        if url and (not local_path or not os.path.exists(local_path)):
                            normalized_url = self.normalize_url(url)
                            if normalized_url:
                                download_tasks.append(self.download_image(session, normalized_url, product_name))
                                row_indices.append(idx)
                    
                    # Handle string format (URL)
                    elif isinstance(img_data, str) and img_data.startswith(('http://', 'https://')):
                        url = img_data
                        normalized_url = self.normalize_url(url)
                        product_name = df.loc[idx, '상품명'] if '상품명' in df.columns else None
                        
                        if normalized_url:
                            download_tasks.append(self.download_image(session, normalized_url, product_name))
                            row_indices.append(idx)
                            
                except Exception as e:
                    logger.error(f"Error processing Naver image in row {idx}: {e}")
            
            if not download_tasks:
                logger.info("No Naver images need downloading")
                return df
                
            logger.info(f"Downloading {len(download_tasks)} missing Naver images")
            download_results = await asyncio.gather(*download_tasks, return_exceptions=True)
            
            # Second pass: update DataFrame with downloaded images
            success_count = 0
            for i, res in enumerate(download_results):
                if isinstance(res, Exception):
                    logger.error(f"Error downloading Naver image: {res}")
                    continue
                    
                if i >= len(row_indices):
                    continue
                    
                idx = row_indices[i]
                url, success, image_data = res
                
                if success and image_data.get('local_path'):
                    success_count += 1
                    img_data = df.loc[idx, naver_img_column]
                    
                    # Update existing dictionary
                    if isinstance(img_data, dict):
                        img_data['local_path'] = image_data['local_path']
                        img_data['url'] = url  # Ensure URL is updated to normalized version
                        df.loc[idx, naver_img_column] = img_data
                    else:
                        # Create new dictionary
                        df.loc[idx, naver_img_column] = image_data
            
            logger.info(f"Downloaded {success_count}/{len(download_tasks)} missing Naver images")
        
        return df

# Utility functions for direct use without class instance

async def download_naver_image(url: str, product_name: str = None, config=None) -> Dict[str, Any]:
    """
    Simplified function to download a single Naver image.
    
    Args:
        url: Image URL to download
        product_name: Optional product name for reference
        config: Optional configuration object
        
    Returns:
        Dictionary with image data or None on failure
    """
    handler = NaverImageHandler(config)
    
    # Configure aiohttp session
    timeout = aiohttp.ClientTimeout(total=handler.timeout * 2)
    connector = aiohttp.TCPConnector(ssl=False)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        _, success, image_data = await handler.download_image(session, url, product_name)
        
        if success:
            return image_data
        return None

def fix_naver_image_data(img_data: Dict) -> Dict:
    """
    Fix and normalize Naver image data.
    
    Args:
        img_data: Dictionary containing image data
        
    Returns:
        Fixed image data dictionary
    """
    if not isinstance(img_data, dict):
        return img_data
        
    # Clone dictionary to avoid modifying the original
    fixed_data = img_data.copy()
    
    # Ensure required fields exist
    if 'source' not in fixed_data:
        fixed_data['source'] = 'naver'
    
    # Fix URL if present
    if 'url' in fixed_data and fixed_data['url']:
        handler = NaverImageHandler()
        normalized_url = handler.normalize_url(fixed_data['url'])
        if normalized_url:
            fixed_data['url'] = normalized_url
    
    # Check local path if present
    if 'local_path' in fixed_data and fixed_data['local_path']:
        local_path = fixed_data['local_path']
        if not os.path.exists(local_path):
            # Try different extensions
            base_path = os.path.splitext(local_path)[0]
            for ext in ['.jpg', '.png', '.jpeg']:
                alt_path = f"{base_path}{ext}"
                if os.path.exists(alt_path):
                    fixed_data['local_path'] = alt_path
                    break
            else:
                # Check for _nobg version
                nobg_path = f"{base_path}_nobg.png"
                if os.path.exists(nobg_path):
                    fixed_data['local_path'] = nobg_path
    
    return fixed_data

# Module initialization - create default paths during import time
# This isn't strictly necessary but helps ensure early path creation
try:
    default_dir = Path('C:\\RPA\\Image\\Main\\Naver')
    default_dir.mkdir(parents=True, exist_ok=True)
except Exception as e:
    logger.warning(f"Failed to create default Naver image directory: {e}") 