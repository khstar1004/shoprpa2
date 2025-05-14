#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Fix Naver Images in Excel Files
--------------------------------
This script fixes issues with Naver images in Excel files by:
1. Verifying Naver product info matches with images
2. Removing misplaced Naver images
3. Ensuring images are in correct columns
4. Fixing image paths and URLs

Usage:
    python fix_naver_images.py --input [input_excel_file] --output [output_excel_file]
"""

import os
import sys
import logging
import pandas as pd
import argparse
from pathlib import Path
import re
import shutil
import hashlib
from datetime import datetime
import asyncio
import aiohttp
import ast
import random
from playwright.async_api import Page

def fix_naver_image_data(img_data):
    """Fallback version when module is not available"""
    return img_data

class NaverImageHandler:
    """Simplified fallback version"""
    def __init__(self, config=None):
        self.image_dir = Path('C:\\RPA\\Image\\Main\\Naver')
        self.image_dir.mkdir(parents=True, exist_ok=True)
        
    def fix_image_data_in_dataframe(self, df, naver_img_column='네이버 이미지'):
        """Simplified version"""
        return df
        
    def transform_for_upload(self, df, result_column='네이버 이미지', upload_column='네이버쇼핑(이미지링크)'):
        """Simplified version"""
        if result_column not in df.columns:
            return df
        if upload_column not in df.columns:
            df[upload_column] = '-'
            
        for idx in range(len(df)):
            img_data = df.loc[idx, result_column]
            if isinstance(img_data, dict) and 'url' in img_data:
                df.loc[idx, upload_column] = img_data['url']
            else:
                df.loc[idx, upload_column] = '-'
        return df

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('fix_naver_images.log')
    ]
)
logger = logging.getLogger('fix_naver_images')

def verify_naver_product_info(row_data):
    """
    Verify if a row has valid Naver product information.
    
    Args:
        row_data: DataFrame row
        
    Returns:
        bool: True if valid Naver product info exists
    """
    # Check for Naver image data
    if '네이버 이미지' in row_data and pd.notna(row_data['네이버 이미지']):
        image_data = row_data['네이버 이미지']
        
        # Handle dictionary format
        if isinstance(image_data, dict):
            # Check for product URL first (preferred)
            if 'product_url' in image_data and isinstance(image_data['product_url'], str):
                if image_data['product_url'].startswith(('http://', 'https://')):
                    return True
            
            # Then check for regular URL
            if 'url' in image_data and isinstance(image_data['url'], str):
                if image_data['url'].startswith(('http://', 'https://')):
                    return True
                    
            # Check for local path
            if 'local_path' in image_data and image_data['local_path']:
                if os.path.exists(image_data['local_path']):
                    return True
    
    # Check for Naver link
    naver_link_cols = ['네이버 쇼핑 링크', '네이버 링크']
    for col in naver_link_cols:
        if col in row_data and pd.notna(row_data[col]):
            link = str(row_data[col]).strip()
            if link and link not in ['-', 'None', ''] and link.startswith(('http://', 'https://')):
                return True
    
    # Check for Naver price
    price_cols = ['판매단가(V포함)(3)', '네이버 판매단가', '판매단가3 (VAT포함)', '네이버 기본수량']
    for col in price_cols:
        if col in row_data and pd.notna(row_data[col]):
            price = row_data[col]
            if isinstance(price, (int, float)) and price > 0:
                return True
            elif isinstance(price, str):
                try:
                    price = float(price.replace(',', ''))
                    if price > 0:
                        return True
                except:
                    continue
    
    return False

def extract_naver_image_info(img_data):
    """
    Extract relevant information from Naver image data.
    
    Args:
        img_data: Image data (dict or string)
        
    Returns:
        dict: Extracted image information
    """
    info = {
        'url': None,
        'local_path': None,
        'is_valid': False,
        'source': 'naver'
    }
    
    if isinstance(img_data, dict):
        info['url'] = img_data.get('url', '')
        info['local_path'] = img_data.get('local_path', '')
        info['original_path'] = img_data.get('original_path', '')
        info['score'] = img_data.get('score', 0)
        
        # Check if it's a valid Naver image URL
        if info['url']:
            if 'pstatic.net' in info['url'] and 'front' not in info['url']:
                info['is_valid'] = True
            elif 'shopping.naver.com' in info['url']:
                info['is_valid'] = True
    
    elif isinstance(img_data, str) and img_data.startswith(('http://', 'https://')):
        info['url'] = img_data
        if 'pstatic.net' in img_data and 'front' not in img_data:
            info['is_valid'] = True
        elif 'shopping.naver.com' in img_data:
            info['is_valid'] = True
    
    return info

# New function to remove Naver product data if image is missing
def remove_naver_data_if_image_missing(df):
    """
    Remove Naver product data from rows where Naver image is missing or invalid.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: Cleaned DataFrame
    """
    result_df = df.copy()
    rows_fixed = 0
    
    # Define columns that should be cleared if Naver image is missing
    naver_columns = [
        '기본수량(3)',
        '판매단가(V포함)(3)',
        '가격차이(3)',
        '가격차이(3)(%)',
        '공급사명',
        '네이버 쇼핑 링크',
        '공급사 상품링크'
    ]
    
    for idx, row in result_df.iterrows():
        # Check if Naver image is missing or invalid
        has_valid_image = False
        
        if '네이버 이미지' in row and pd.notna(row['네이버 이미지']):
            image_data = row['네이버 이미지']
            
            # Handle dictionary format
            if isinstance(image_data, dict) and 'url' in image_data:
                url = image_data['url']
                if url and url != '-' and isinstance(url, str) and url.startswith(('http://', 'https://')):
                    has_valid_image = True
            
            # Handle string format
            elif isinstance(image_data, str) and image_data.startswith(('http://', 'https://')):
                has_valid_image = True
        
        # If no valid Naver image, clear all Naver product data
        if not has_valid_image:
            # Clear Naver image column
            result_df.at[idx, '네이버 이미지'] = '-'
            
            # Clear all related Naver columns
            for col in naver_columns:
                if col in result_df.columns:
                    result_df.at[idx, col] = '-'
            
            rows_fixed += 1
            logger.info(f"Row {idx}: Removed Naver product data due to missing image")
    
    logger.info(f"Total rows with Naver data removed due to missing images: {rows_fixed}")
    return result_df

def fix_naver_images(df):
    """
    Fix Naver image issues in the DataFrame.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: Fixed DataFrame
    """
    result_df = df.copy()
    
    # Initialize the NaverImageHandler for better processing
    naver_handler = NaverImageHandler()
    
    # Track statistics
    stats = {
        'total_rows': len(df),
        'rows_with_naver_info': 0,
        'misplaced_images_removed': 0,
        'images_fixed': 0,
        'invalid_urls_removed': 0
    }
    
    # First run the handler's fix method to normalize URLs and check local paths
    result_df = naver_handler.fix_image_data_in_dataframe(result_df, naver_img_column='네이버 이미지')
    
    # Process each row
    for idx, row in result_df.iterrows():
        # Check if row has valid Naver product info
        has_naver_info = verify_naver_product_info(row)
        
        if has_naver_info:
            stats['rows_with_naver_info'] += 1
            
            # Get Naver image data
            naver_img = row.get('네이버 이미지', None)
            if naver_img:
                img_info = extract_naver_image_info(naver_img)
                
                if not img_info['is_valid']:
                    # Remove invalid Naver image
                    result_df.at[idx, '네이버 이미지'] = '-'
                    stats['invalid_urls_removed'] += 1
                    logger.info(f"Row {idx}: Removed invalid Naver image URL")
                elif img_info['url'] and 'front' in img_info['url']:
                    # Remove unreliable 'front' URLs
                    result_df.at[idx, '네이버 이미지'] = '-'
                    stats['invalid_urls_removed'] += 1
                    logger.info(f"Row {idx}: Removed unreliable 'front' URL")
                else:
                    # Update image data with clean format
                    clean_img_data = {
                        'url': img_info['url'],
                        'local_path': img_info['local_path'],
                        'source': 'naver',
                        'score': img_info.get('score', 0.5)
                    }
                    result_df.at[idx, '네이버 이미지'] = clean_img_data
                    stats['images_fixed'] += 1
        else:
            # No Naver product info - remove any Naver image
            if '네이버 이미지' in result_df.columns:
                current_img = row.get('네이버 이미지')
                if current_img and current_img != '-':
                    result_df.at[idx, '네이버 이미지'] = '-'
                    stats['misplaced_images_removed'] += 1
                    logger.info(f"Row {idx}: Removed misplaced Naver image (no product info)")
    
    # Apply new function to remove Naver data from rows without valid images
    result_df = remove_naver_data_if_image_missing(result_df)
    
    # Log statistics
    logger.info("=== Naver Image Fix Statistics ===")
    logger.info(f"Total rows processed: {stats['total_rows']}")
    logger.info(f"Rows with Naver product info: {stats['rows_with_naver_info']}")
    logger.info(f"Misplaced images removed: {stats['misplaced_images_removed']}")
    logger.info(f"Invalid URLs removed: {stats['invalid_urls_removed']}")
    logger.info(f"Images fixed: {stats['images_fixed']}")
    
    return result_df

def fix_excel_file(input_file, output_file=None):
    """
    Fix Naver images in an Excel file.
    
    Args:
        input_file: Path to input Excel file
        output_file: Path to output Excel file (optional)
        
    Returns:
        str: Path to the output file if successful, None otherwise
    """
    try:
        # Validate input file
        if not os.path.exists(input_file):
            logger.error(f"Input file not found: {input_file}")
            return None
            
        # Set output file if not specified
        if not output_file:
            base_name = os.path.basename(input_file)
            file_name, ext = os.path.splitext(base_name)
            output_dir = os.path.join('C:', 'RPA', 'Output')
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(output_dir, f"{file_name}_naver_fixed{ext}")
        
        # Ensure output directory exists
        output_dir = os.path.dirname(os.path.abspath(output_file))
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info(f"Processing Excel file: {input_file}")
        logger.info(f"Output will be saved to: {output_file}")
        
        # Read Excel file
        df = pd.read_excel(input_file)
        
        # Fix Naver images
        fixed_df = fix_naver_images(df)
        
        # Save the fixed DataFrame
        fixed_df.to_excel(output_file, index=False)
        logger.info(f"Saved fixed Excel file to: {output_file}")
        
        # Verify file was created
        if not os.path.exists(output_file):
            logger.error(f"Failed to create output file: {output_file}")
            return None
            
        return output_file
        
    except Exception as e:
        logger.error(f"Error fixing Excel file: {e}")
        return None

def validate_and_fix_naver_image_placement(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates and fixes Naver image placement in the DataFrame.
    
    Args:
        df: DataFrame containing Naver image data
        
    Returns:
        DataFrame with validated and fixed Naver image placement
    """
    if df.empty:
        return df
        
    result_df = df.copy()
    
    # Initialize NaverImageHandler for better processing
    naver_handler = NaverImageHandler()
    
    # First use the handler to fix image data format and normalize URLs
    result_df = naver_handler.fix_image_data_in_dataframe(result_df, naver_img_column='네이버 이미지')
    
    # Track statistics
    fixed_count = 0
    removed_count = 0
    
    # Process Naver image column
    naver_img_col = '네이버 이미지'
    naver_link_col = '네이버 쇼핑 링크'
    
    if naver_img_col not in result_df.columns:
        logger.warning(f"Naver image column '{naver_img_col}' not found")
        return result_df
        
    # Process each row
    for idx in result_df.index:
        try:
            # 1. 네이버 상품 정보 확인 (네이버 쇼핑 링크)
            has_product_info = False
            if naver_link_col in result_df.columns and pd.notna(result_df.at[idx, naver_link_col]):
                link = str(result_df.at[idx, naver_link_col]).strip()
                if link and link not in ['-', 'None', ''] and link.startswith(('http://', 'https://')):
                    has_product_info = True
            
            # 2. 네이버 상품 정보가 없으면 이미지도 삭제
            if not has_product_info:
                if naver_img_col in result_df.columns:
                    result_df.at[idx, naver_img_col] = '-'
                    removed_count += 1
                    logger.debug(f"Row {idx}: Removed Naver image due to missing product info")
                continue
            
            # 3. 이미지 데이터 확인
            img_data = result_df.at[idx, naver_img_col]
            
            # 4. 빈 데이터면 건너뛰기
            if pd.isna(img_data) or img_data == '-':
                continue
                
            # 5. 이미지 URL 유효성 검사
            has_valid_url = False
            
            if isinstance(img_data, dict):
                url = img_data.get('url', '')
                if url and isinstance(url, str) and url.startswith(('http://', 'https://')):
                    if 'shopping-phinf.pstatic.net' in url:
                        has_valid_url = True
            elif isinstance(img_data, str) and img_data.startswith(('http://', 'https://')):
                if 'shopping-phinf.pstatic.net' in img_data:
                    has_valid_url = True
            
            # 6. 이미지 링크가 없거나 유효하지 않으면 이미지 제거
            if not has_valid_url:
                result_df.at[idx, naver_img_col] = '-'
                removed_count += 1
                logger.debug(f"Row {idx}: Removed invalid Naver image URL")
            else:
                # 7. 유효한 이미지 정보면 형식 통일
                if isinstance(img_data, dict):
                    # URL 및 필요한 정보만 유지
                    clean_data = {
                        'url': img_data.get('url', ''),
                        'local_path': img_data.get('local_path', ''),
                        'source': 'naver',
                        'score': img_data.get('score', 0.7)
                    }
                    result_df.at[idx, naver_img_col] = clean_data
                elif isinstance(img_data, str):
                    # 문자열 URL을 딕셔너리 형태로 변환
                    clean_data = {
                        'url': img_data,
                        'local_path': '',
                        'source': 'naver',
                        'score': 0.7
                    }
                    result_df.at[idx, naver_img_col] = clean_data
                
                fixed_count += 1
                logger.debug(f"Row {idx}: Fixed Naver image format")
                
        except Exception as e:
            logger.error(f"Error processing row {idx}: {e}")
            # 오류 발생시 안전하게 이미지 제거
            result_df.at[idx, naver_img_col] = '-'
            removed_count += 1
            
    logger.info(f"Naver image validation complete: {fixed_count} fixed, {removed_count} removed")
    
    # Apply new function to remove Naver data from rows without valid images
    result_df = remove_naver_data_if_image_missing(result_df)
    
    return result_df

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Fix Naver images in Excel files')
    parser.add_argument('--input', '-i', required=True, help='Input Excel file path')
    parser.add_argument('--output', '-o', help='Output Excel file path (optional)')
    
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    output_dir = os.path.join('C:', 'RPA', 'Output')
    os.makedirs(output_dir, exist_ok=True)
    
    # If no output path specified, create one based on input filename
    if not args.output:
        input_basename = os.path.basename(args.input)
        filename, ext = os.path.splitext(input_basename)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output = os.path.join(output_dir, f"{filename}_fixed_{timestamp}{ext}")
    else:
        # If output path is specified but directory doesn't exist, create it
        output_parent_dir = os.path.dirname(args.output)
        if output_parent_dir:
            os.makedirs(output_parent_dir, exist_ok=True)
    
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    
    result = fix_excel_file(args.input, args.output)
    
    if result and os.path.exists(result):
        print(f"✅ Successfully fixed Naver images.")
        print(f"✅ Output saved to: {result}")
        return 0
    else:
        print("❌ Failed to fix Naver images. Check the log for details.")
        return 1

def prepare_naver_columns_for_excel_output(df: pd.DataFrame, is_upload_file: bool = False) -> pd.DataFrame:
    """
    Prepares Naver image columns for Excel output, handling different naming conventions
    between result file and upload file.
    
    Args:
        df: DataFrame to process
        is_upload_file: If True, prepare for upload file (URLs only with different column name)
        
    Returns:
        DataFrame with properly formatted Naver image columns
    """
    if df.empty:
        return df
    
    # Initialize NaverImageHandler for better processing
    naver_handler = NaverImageHandler()
    
    # Column names for different file types
    result_col_name = '네이버 이미지'
    upload_col_name = '네이버쇼핑(이미지링크)'
    
    # First ensure the Naver image column exists
    if result_col_name not in df.columns:
        logger.warning(f"Column '{result_col_name}' not found, cannot prepare Naver images")
        return df
    
    # Apply new function to remove Naver data from rows without valid images
    df = remove_naver_data_if_image_missing(df)
    
    if is_upload_file:
        # For upload file: Use the handler's transform_for_upload method
        df = naver_handler.transform_for_upload(df, result_column=result_col_name, upload_column=upload_col_name)
        
        # Optionally remove the result column if not needed in upload file
        if result_col_name in df.columns:
            df = df.drop(columns=[result_col_name])
            
        logger.info(f"Created '{upload_col_name}' column for upload file with URLs only")
    else:
        # For result file: Make sure Naver image column has consistent format
        df = naver_handler.fix_image_data_in_dataframe(df, naver_img_column=result_col_name)
        logger.info(f"Maintained '{result_col_name}' column for result file with both paths and URLs")
    
    return df

def transform_between_file_types(df: pd.DataFrame, file_type: str) -> pd.DataFrame:
    """
    Transforms a DataFrame between result and upload file formats by
    properly handling the Naver image columns.
    
    Args:
        df: DataFrame to transform
        file_type: Either 'result' or 'upload'
        
    Returns:
        Transformed DataFrame with appropriate Naver image columns
    """
    if file_type.lower() not in ['result', 'upload']:
        logger.error(f"Invalid file_type: {file_type}, must be 'result' or 'upload'")
        return df
    
    is_upload = file_type.lower() == 'upload'
    
    # Check if transformation is needed
    result_col_name = '네이버 이미지'
    upload_col_name = '네이버쇼핑(이미지링크)'
    
    # Apply new function to remove Naver data from rows without valid images
    df = remove_naver_data_if_image_missing(df)
    
    # For upload file
    if is_upload:
        return prepare_naver_columns_for_excel_output(df, is_upload_file=True)
    # For result file
    else:
        # If the DataFrame already has the upload column but needs result column
        if upload_col_name in df.columns and result_col_name not in df.columns:
            # Initialize NaverImageHandler
            naver_handler = NaverImageHandler()
            
            # Convert from upload format to result format
            # Create a new column with empty dictionaries
            df[result_col_name] = None
            
            # Process each row
            for idx, row in df.iterrows():
                if pd.isna(row[upload_col_name]) or row[upload_col_name] == '-':
                    df.at[idx, result_col_name] = '-'
                else:
                    url = row[upload_col_name]
                    if isinstance(url, str) and url.startswith(('http://', 'https://')):
                        # Create dictionary structure for result file
                        df.at[idx, result_col_name] = {
                            'url': url,
                            'local_path': '',  # No local path available
                            'source': 'naver',
                            'score': 0.7  # Default score
                        }
                    else:
                        df.at[idx, result_col_name] = '-'
            
            logger.info(f"Created '{result_col_name}' column from '{upload_col_name}' column")
        
        return df 

async def download_image(session, url, filepath):
    """Download an image from a URL and save it to filepath."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                content = await response.read()
                with open(filepath, 'wb') as f:
                    f.write(content)
                logging.info(f"Successfully downloaded: {url}")
                return True
            else:
                logging.warning(f"Failed to download {url}, status: {response.status}")
                return False
    except Exception as e:
        logging.error(f"Error downloading {url}: {e}")
        return False

async def ensure_naver_local_images_async(df: pd.DataFrame, naver_image_dir: str) -> pd.DataFrame:
    """Async version of ensure_naver_local_images"""
    try:
        # Create output directory if it doesn't exist
        os.makedirs(naver_image_dir, exist_ok=True)
        logging.info(f"Saving images to: {naver_image_dir}")
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            for idx, row in df.iterrows():
                try:
                    if '네이버 이미지' not in row or pd.isna(row['네이버 이미지']):
                        continue
                        
                    img_data = row['네이버 이미지']
                    url = None
                    local_path = None
                    
                    # Get URL and local path from dictionary or string
                    if isinstance(img_data, dict):
                        url = img_data.get('url')
                        local_path = img_data.get('local_path')
                    elif isinstance(img_data, str) and img_data.startswith('http'):
                        url = img_data
                    
                    # Skip if we already have a valid local path
                    if local_path and os.path.exists(local_path) and os.path.getsize(local_path) > 1000:
                        logging.debug(f"Image already exists at {local_path}, skipping download")
                        continue
                    
                    if url and 'shopping-phinf.pstatic.net' in url:
                        # Create filename from URL
                        filename = f"naver_{hashlib.md5(url.encode()).hexdigest()[:10]}.jpg"
                        filepath = os.path.join(naver_image_dir, filename)
                        
                        # Skip if file already exists and is valid
                        if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
                            logging.debug(f"Image already exists at {filepath}, skipping download")
                            # Update DataFrame with existing file
                            if isinstance(img_data, dict):
                                img_data['local_path'] = filepath
                                df.at[idx, '네이버 이미지'] = img_data
                            else:
                                df.at[idx, '네이버 이미지'] = {
                                    'url': url,
                                    'local_path': filepath,
                                    'source': 'naver'
                                }
                            continue
                            
                        task = asyncio.create_task(download_image(session, url, filepath))
                        tasks.append((idx, url, filepath, task))
                except Exception as e:
                    logging.error(f"Error processing row {idx}: {e}")
                    continue
            
            if tasks:
                results = await asyncio.gather(*(t[3] for t in tasks), return_exceptions=True)
                for (idx, url, filepath, _), success in zip(tasks, results):
                    if success:
                        current_data = df.at[idx, '네이버 이미지']
                        if isinstance(current_data, dict):
                            current_data['local_path'] = filepath
                            df.at[idx, '네이버 이미지'] = current_data
                        else:
                            df.at[idx, '네이버 이미지'] = {
                                'url': url,
                                'local_path': filepath,
                                'source': 'naver'
                            }
        
        # Count successful downloads
        downloaded = sum(1 for _, row in df.iterrows() 
                        if isinstance(row.get('네이버 이미지'), dict) 
                        and row['네이버 이미지'].get('local_path')
                        and os.path.exists(row['네이버 이미지']['local_path']))
        
        logging.info(f"Successfully downloaded {downloaded} Naver images")
        
    except Exception as e:
        logging.error(f"Error in image download process: {e}")
    
    return df

def ensure_naver_local_images(df: pd.DataFrame, naver_image_dir: str) -> pd.DataFrame:
    """Wrapper function to run async code"""
    try:
        # Check if there's a running event loop
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
        # Create a new event loop if the current one is closed
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
        # Run the async function in the event loop
        if loop.is_running():
            # If loop is already running, create a new one in a separate thread
            import threading
            def run_async():
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                return new_loop.run_until_complete(ensure_naver_local_images_async(df, naver_image_dir))
                
            thread = threading.Thread(target=run_async)
            thread.start()
            thread.join()
        else:
            # If loop is not running, use it directly
            return loop.run_until_complete(ensure_naver_local_images_async(df, naver_image_dir))
            
        return df
        
    except Exception as e:
        logging.error(f"Error in ensure_naver_local_images: {e}")
        return df

async def handle_captcha(page: Page) -> bool:
    """캡차 처리 함수"""
    try:
        captcha_selectors = [
            'form#captcha_form', 
            'img[alt*="captcha"]', 
            'div.captcha_wrap',
            'input[name="captchaBotKey"]',
            'div[class*="captcha"]'
        ]
        
        for selector in captcha_selectors:
            if await page.query_selector(selector):
                logger.info("CAPTCHA detected, waiting and retrying...")
                
                # 브라우저 재시작
                context = page.context
                browser = context.browser
                
                # 새 컨텍스트 생성
                new_context = await browser.new_context(
                    viewport={"width": 1366, "height": 768},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                    java_script_enabled=True
                )
                
                # 랜덤 대기
                await asyncio.sleep(random.uniform(3.0, 5.0))
                
                # 새 페이지로 다시 시도
                new_page = await new_context.new_page()
                await new_page.goto(page.url, wait_until='networkidle')
                
                # 캡차가 여전히 있는지 확인
                still_has_captcha = False
                for selector in captcha_selectors:
                    if await new_page.query_selector(selector):
                        still_has_captcha = True
                        break
                
                if not still_has_captcha:
                    return True
                
                # 이전 컨텍스트 정리
                await context.close()
                return False
                
        return True
    except Exception as e:
        logger.error(f"Error handling CAPTCHA: {e}")
        return False

if __name__ == "__main__":
    sys.exit(main()) 