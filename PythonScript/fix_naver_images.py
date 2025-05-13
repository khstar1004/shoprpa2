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
    
    # Track statistics
    stats = {
        'total_rows': len(df),
        'rows_with_naver_info': 0,
        'misplaced_images_removed': 0,
        'images_fixed': 0,
        'invalid_urls_removed': 0
    }
    
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
                
            # Handle dictionary format
            if isinstance(img_data, dict):
                # Check if image path exists and is valid
                if 'local_path' in img_data and img_data['local_path']:
                    local_path = img_data['local_path']
                    if os.path.exists(local_path):
                        # Local file exists, make sure URL is also set
                        if 'url' not in img_data or not img_data['url']:
                            # Try to set URL if missing
                            if 'image_url' in img_data:
                                img_data['url'] = img_data['image_url']
                            elif 'product_url' in img_data:
                                img_data['url'] = img_data['product_url']
                        
                        # Ensure consistent structure
                        if 'source' not in img_data:
                            img_data['source'] = 'naver'
                            
                        result_df.at[idx, naver_img_col] = img_data
                        fixed_count += 1
                        continue
                
                # Check for product URL or regular URL if local path doesn't exist
                if ('product_url' in img_data and isinstance(img_data['product_url'], str) and 
                    img_data['product_url'].startswith(('http://', 'https://'))):
                    # Create a consistent structure with the URL
                    clean_img_data = {
                        'url': img_data['product_url'],
                        'local_path': img_data.get('local_path', ''),
                        'source': 'naver'
                    }
                    result_df.at[idx, naver_img_col] = clean_img_data
                    fixed_count += 1
                    continue
                elif ('url' in img_data and isinstance(img_data['url'], str) and 
                      img_data['url'].startswith(('http://', 'https://')) and
                      'pstatic.net/front/' not in img_data['url']):
                    # URL is valid and not a front URL
                    clean_img_data = {
                        'url': img_data['url'],
                        'local_path': img_data.get('local_path', ''),
                        'source': 'naver'
                    }
                    result_df.at[idx, naver_img_col] = clean_img_data
                    fixed_count += 1
                    continue
                elif ('image_url' in img_data and isinstance(img_data['image_url'], str) and 
                      img_data['image_url'].startswith(('http://', 'https://')) and
                      'pstatic.net/front/' not in img_data['image_url']):
                    # image_url is valid
                    clean_img_data = {
                        'url': img_data['image_url'],
                        'local_path': img_data.get('local_path', ''),
                        'source': 'naver'
                    }
                    result_df.at[idx, naver_img_col] = clean_img_data
                    fixed_count += 1
                    continue
                        
                # If we get here, no valid image data was found
                result_df.at[idx, naver_img_col] = '-'
                removed_count += 1
                
            # Handle string format
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
                            'source': 'naver'
                        }
                        result_df.at[idx, naver_img_col] = clean_img_data
                        fixed_count += 1
                elif os.path.exists(img_data):
                    # It's a local file path
                    clean_img_data = {
                        'url': '',  # No URL available
                        'local_path': img_data,
                        'source': 'naver'
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

# Add a new helper function to ensure local image paths for Naver images
def ensure_naver_local_images(df: pd.DataFrame, naver_image_dir: str = None) -> pd.DataFrame:
    """
    Ensures Naver images have valid local paths by checking and fixing paths.
    
    Args:
        df: DataFrame containing Naver image data
        naver_image_dir: Base directory for Naver images (optional)
        
    Returns:
        DataFrame with validated local image paths
    """
    if df.empty:
        return df
    
    result_df = df.copy()
    
    # Set default Naver image directory if not provided
    if not naver_image_dir:
        naver_image_dir = os.path.join('C:\\RPA\\Image\\Main', 'Naver')
        if not os.path.exists(naver_image_dir):
            try:
                os.makedirs(naver_image_dir, exist_ok=True)
            except Exception as e:
                logger.error(f"Failed to create Naver image directory: {e}")
    
    naver_img_col = '네이버 이미지'
    
    if naver_img_col not in result_df.columns:
        return result_df
    
    fixed_count = 0
    failed_count = 0
    
    for idx in result_df.index:
        try:
            img_data = result_df.at[idx, naver_img_col]
            
            # Skip empty or invalid data
            if pd.isna(img_data) or img_data == '-':
                continue
            
            # Handle dictionary format
            if isinstance(img_data, dict):
                # Check if local path exists
                local_path = img_data.get('local_path', '')
                url = img_data.get('url', '')
                
                # If local path is missing or invalid but URL exists, try to extract filename from URL
                if (not local_path or not os.path.exists(local_path)) and url:
                    try:
                        # Extract filename from URL or generate a hash-based filename
                        if '/' in url:
                            filename = url.split('/')[-1]
                            if '?' in filename:
                                filename = filename.split('?')[0]
                            if not filename or len(filename) < 5:
                                # Fallback to hash if filename is too short
                                filename = f"naver_{hashlib.md5(url.encode()).hexdigest()[:10]}.jpg"
                        else:
                            filename = f"naver_{hashlib.md5(url.encode()).hexdigest()[:10]}.jpg"
                        
                        # Check for both regular and _nobg versions
                        new_path = os.path.join(naver_image_dir, filename)
                        nobg_path = os.path.join(naver_image_dir, filename.replace('.', '_nobg.', 1))
                        
                        if os.path.exists(new_path):
                            img_data['local_path'] = new_path
                            fixed_count += 1
                        elif os.path.exists(nobg_path):
                            img_data['local_path'] = nobg_path
                            fixed_count += 1
                        else:
                            # File doesn't exist locally, keep URL only
                            logger.debug(f"Local image not found for URL: {url}")
                            failed_count += 1
                        
                        result_df.at[idx, naver_img_col] = img_data
                    except Exception as e:
                        logger.error(f"Error processing URL for row {idx}: {e}")
                        failed_count += 1
                
                # If there's a local path, verify it exists
                elif local_path:
                    if os.path.exists(local_path):
                        # Path is valid, ensure it's absolute
                        img_data['local_path'] = os.path.abspath(local_path)
                        result_df.at[idx, naver_img_col] = img_data
                        fixed_count += 1
                    else:
                        # Check if the file might be in the Naver directory
                        filename = os.path.basename(local_path)
                        alt_path = os.path.join(naver_image_dir, filename)
                        alt_nobg_path = os.path.join(naver_image_dir, filename.replace('.', '_nobg.', 1))
                        
                        if os.path.exists(alt_path):
                            img_data['local_path'] = alt_path
                            result_df.at[idx, naver_img_col] = img_data
                            fixed_count += 1
                        elif os.path.exists(alt_nobg_path):
                            img_data['local_path'] = alt_nobg_path
                            result_df.at[idx, naver_img_col] = img_data
                            fixed_count += 1
                        else:
                            logger.debug(f"Local image not found: {local_path}")
                            failed_count += 1
            
            # Handle string values (not as common but possible)
            elif isinstance(img_data, str) and img_data != '-':
                if img_data.startswith(('http://', 'https://')):
                    # It's a URL, convert to dictionary format
                    try:
                        url = img_data
                        # Extract/generate filename
                        if '/' in url:
                            filename = url.split('/')[-1]
                            if '?' in filename:
                                filename = filename.split('?')[0]
                            if not filename or len(filename) < 5:
                                filename = f"naver_{hashlib.md5(url.encode()).hexdigest()[:10]}.jpg"
                        else:
                            filename = f"naver_{hashlib.md5(url.encode()).hexdigest()[:10]}.jpg"
                        
                        # Check if file exists
                        new_path = os.path.join(naver_image_dir, filename)
                        nobg_path = os.path.join(naver_image_dir, filename.replace('.', '_nobg.', 1))
                        
                        local_path = ''
                        if os.path.exists(new_path):
                            local_path = new_path
                        elif os.path.exists(nobg_path):
                            local_path = nobg_path
                        
                        # Create dictionary structure
                        clean_img_data = {
                            'url': url,
                            'local_path': local_path,
                            'source': 'naver'
                        }
                        result_df.at[idx, naver_img_col] = clean_img_data
                        fixed_count += 1
                    except Exception as e:
                        logger.error(f"Error converting URL to dict for row {idx}: {e}")
                        failed_count += 1
                elif os.path.exists(img_data):
                    # It's a local file path, convert to dictionary
                    clean_img_data = {
                        'url': '',
                        'local_path': os.path.abspath(img_data),
                        'source': 'naver'
                    }
                    result_df.at[idx, naver_img_col] = clean_img_data
                    fixed_count += 1
        
        except Exception as e:
            logger.error(f"Error ensuring local image for row {idx}: {e}")
            failed_count += 1
    
    logger.info(f"Naver local image validation complete: {fixed_count} fixed, {failed_count} failed")
    return result_df

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Fix Naver images in Excel files')
    parser.add_argument('--input', '-i', required=True, help='Input Excel file path')
    parser.add_argument('--output', '-o', help='Output Excel file path (optional)')
    
    args = parser.parse_args()
    
    result = fix_excel_file(args.input, args.output)
    
    if result:
        print(f"✅ Successfully fixed Naver images.")
        print(f"✅ Output saved to: {result}")
        return 0
    else:
        print("❌ Failed to fix Naver images. Check the log for details.")
        return 1

if __name__ == "__main__":
    sys.exit(main())

# Add a new function to handle different column names between result and upload files
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
    
    result_df = df.copy()
    
    # Column names for different file types
    result_col_name = '네이버 이미지'
    upload_col_name = '네이버쇼핑(이미지링크)'
    
    # First ensure the Naver image column exists
    if result_col_name not in result_df.columns:
        logger.warning(f"Column '{result_col_name}' not found, cannot prepare Naver images")
        return result_df
    
    if is_upload_file:
        # For upload file: Create column with only URLs
        urls = []
        for idx, row in result_df.iterrows():
            if pd.isna(row[result_col_name]) or row[result_col_name] == '-':
                urls.append('-')
                continue
                
            url = None
            # Extract URL from dictionary structure
            if isinstance(row[result_col_name], dict):
                url = row[result_col_name].get('url', None)
            # Handle string URLs
            elif isinstance(row[result_col_name], str) and row[result_col_name].startswith(('http://', 'https://')):
                url = row[result_col_name]
            
            urls.append(url if url else '-')
        
        # Add upload column
        result_df[upload_col_name] = urls
        
        # Optionally remove the result column if not needed in upload file
        if result_col_name in result_df.columns:
            result_df = result_df.drop(columns=[result_col_name])
            
        logger.info(f"Created '{upload_col_name}' column for upload file with URLs only")
        
    else:
        # For result file: Ensure correct format with both local paths and URLs
        # This is already handled by validate_and_fix_naver_image_placement and ensure_naver_local_images
        logger.info(f"Maintained '{result_col_name}' column for result file with both paths and URLs")
    
    return result_df

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
            # Convert from upload format to result format
            # This is less common but included for completeness
            result_entries = []
            for idx, row in df.iterrows():
                if pd.isna(row[upload_col_name]) or row[upload_col_name] == '-':
                    result_entries.append('-')
                else:
                    url = row[upload_col_name]
                    if isinstance(url, str) and url.startswith(('http://', 'https://')):
                        # Create dictionary structure for result file
                        result_entries.append({
                            'url': url,
                            'local_path': '',  # No local path available
                            'source': 'naver'
                        })
                    else:
                        result_entries.append('-')
            
            df[result_col_name] = result_entries
            logger.info(f"Created '{result_col_name}' column from '{upload_col_name}' column")
        
        return df 