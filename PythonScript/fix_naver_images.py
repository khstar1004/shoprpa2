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

# Import the new NaverImageHandler
try:
    from naver_image_handler import NaverImageHandler, fix_naver_image_data
except ImportError:
    # If direct import fails, try from PythonScript prefix
    try:
        from PythonScript.naver_image_handler import NaverImageHandler, fix_naver_image_data
    except ImportError:
        # Define a simple version as fallback
        logging.warning("Unable to import NaverImageHandler module. Using simplified version.")
        
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
            output_file = os.path.join(os.path.dirname(input_file), f"{file_name}_naver_fixed{ext}")
        
        logger.info(f"Processing Excel file: {input_file}")
        
        # Read Excel file
        df = pd.read_excel(input_file)
        
        # Fix Naver images
        fixed_df = fix_naver_images(df)
        
        # Save the fixed DataFrame
        fixed_df.to_excel(output_file, index=False)
        logger.info(f"Saved fixed Excel file to: {output_file}")
        
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
            img_data = result_df.at[idx, naver_img_col]
            
            # Skip empty or invalid data
            if pd.isna(img_data) or img_data == '-':
                continue
                
            # Check if the row has a matching score that indicates it passed matching
            passed_matching = False
            if '네이버 유사도' in result_df.columns and pd.notna(result_df.at[idx, '네이버 유사도']):
                try:
                    score = float(result_df.at[idx, '네이버 유사도'])
                    # Typically scores above 0.5 are considered matches
                    if score >= 0.5:
                        passed_matching = True
                except (ValueError, TypeError):
                    pass
            
            # Always keep images that passed matching
            if passed_matching:
                fixed_count += 1
                continue
                
            # Handle dictionary format - use improved validation logic
            if isinstance(img_data, dict):
                # Check URL and ensure it's valid
                url = img_data.get('url', '')
                local_path = img_data.get('local_path', '')
                
                # Check if URL is valid (improved validation)
                url_valid = False
                if url and isinstance(url, str) and url.startswith(('http://', 'https://')):
                    # Reject known problematic URL patterns
                    if 'pstatic.net/front/' not in url:
                        url_valid = True
                
                # Check if local path exists
                path_valid = False
                if local_path and os.path.exists(local_path):
                    path_valid = True
                    
                # Decision logic
                if url_valid or path_valid:
                    # Make sure we have a consistent structure
                    fixed_data = fix_naver_image_data(img_data)
                    result_df.at[idx, naver_img_col] = fixed_data
                    fixed_count += 1
                else:
                    # No valid URL or path - try to generate a URL from product link
                    has_product_link = False
                    product_url = None
                    
                    # Check for Naver link
                    if naver_link_col in result_df.columns and pd.notna(result_df.at[idx, naver_link_col]):
                        product_url = str(result_df.at[idx, naver_link_col]).strip()
                        if product_url and product_url not in ['-', 'None', ''] and product_url.startswith(('http://', 'https://')):
                            has_product_link = True
                    
                    # Try alternative link columns
                    if not has_product_link:
                        for alt_col in ['네이버 링크', '네이버 상품 URL']:
                            if alt_col in result_df.columns and pd.notna(result_df.at[idx, alt_col]):
                                product_url = str(result_df.at[idx, alt_col]).strip()
                                if product_url and product_url not in ['-', 'None', ''] and product_url.startswith(('http://', 'https://')):
                                    has_product_link = True
                                    break
                    
                    if has_product_link:
                        # Extract product ID to generate image URL
                        product_id = None
                        patterns = [
                            r'main_(\d+)/(\d+)',  # Standard pattern: main_1234567/1234567.jpg
                            r'cat_id=(\d+)',      # Catalog ID pattern
                            r'products/(\d+)',    # Product detail page pattern
                            r'id=(\d+)'           # Simple ID pattern
                        ]
                        
                        for pattern in patterns:
                            match = re.search(pattern, product_url)
                            if match:
                                product_id = match.group(1)
                                break
                        
                        if product_id:
                            # Generate image URL from product ID
                            generated_url = f"https://shopping-phinf.pstatic.net/main_{product_id}/{product_id}.jpg"
                            
                            # Update image data with generated URL
                            img_data['url'] = generated_url
                            img_data['source'] = 'naver'
                            if 'score' not in img_data:
                                img_data['score'] = 0.7  # Moderate confidence for generated URLs
                            
                            result_df.at[idx, naver_img_col] = img_data
                            fixed_count += 1
                            logger.info(f"Row {idx}: Generated Naver image URL from product link")
                        else:
                            # No product ID found, remove the image data
                            result_df.at[idx, naver_img_col] = '-'
                            removed_count += 1
                    else:
                        # No product link to generate URL from, remove the image data
                        result_df.at[idx, naver_img_col] = '-'
                        removed_count += 1
            
            # Handle string format (URL)
            elif isinstance(img_data, str):
                if img_data.startswith(('http://', 'https://')):
                    if 'pstatic.net/front/' in img_data:
                        # Remove unreliable front URL
                        result_df.at[idx, naver_img_col] = '-'
                        removed_count += 1
                    else:
                        # Create proper dictionary structure
                        clean_img_data = {
                            'url': img_data,
                            'local_path': '',  # No local path available
                            'source': 'naver',
                            'score': 0.7  # Moderate confidence for direct URLs
                        }
                        result_df.at[idx, naver_img_col] = clean_img_data
                        fixed_count += 1
                elif os.path.exists(img_data):
                    # It's a local file path
                    clean_img_data = {
                        'url': '',  # No URL available
                        'local_path': img_data,
                        'source': 'naver',
                        'score': 0.8  # Higher confidence for local files
                    }
                    result_df.at[idx, naver_img_col] = clean_img_data
                    fixed_count += 1
                else:
                    # Not a valid URL or path
                    result_df.at[idx, naver_img_col] = '-'
                    removed_count += 1
            else:
                # Invalid data type
                result_df.at[idx, naver_img_col] = '-'
                removed_count += 1
                
        except Exception as e:
            logger.error(f"Error processing row {idx}: {e}")
            result_df.at[idx, naver_img_col] = '-'
            removed_count += 1
            
    logger.info(f"Naver image validation complete: {fixed_count} fixed, {removed_count} removed")
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
    
    result = fix_excel_file(args.input, args.output)
    
    if result:
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
                    
                    # Get URL from dictionary or string
                    if isinstance(img_data, dict) and 'url' in img_data:
                        url = img_data['url']
                    elif isinstance(img_data, str) and img_data.startswith('http'):
                        url = img_data
                    
                    if url and 'shopping-phinf.pstatic.net' in url:
                        # Create filename from URL
                        filename = f"naver_{hashlib.md5(url.encode()).hexdigest()[:10]}.jpg"
                        filepath = os.path.join(naver_image_dir, filename)
                        
                        if not os.path.exists(filepath):
                            task = asyncio.create_task(download_image(session, url, filepath))
                            tasks.append((idx, url, filepath, task))
                        else:
                            logging.info(f"Image already exists: {filepath}")
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

if __name__ == "__main__":
    sys.exit(main()) 