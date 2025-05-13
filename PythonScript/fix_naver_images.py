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
                
            # Handle dictionary format
            if isinstance(img_data, dict):
                # Check for product URL first
                if 'product_url' in img_data and isinstance(img_data['product_url'], str):
                    if img_data['product_url'].startswith(('http://', 'https://')):
                        # Keep product URL, it's valid
                        fixed_count += 1
                        continue
                        
                # Check regular URL
                if 'url' in img_data:
                    url = img_data['url']
                    if isinstance(url, str) and url.startswith(('http://', 'https://')):
                        if 'pstatic.net/front/' in url:
                            # Remove unreliable front URL
                            img_data['url'] = ''
                            result_df.at[idx, naver_img_col] = img_data
                            removed_count += 1
                        else:
                            # URL is valid
                            fixed_count += 1
                            continue
                            
                # Check local path
                if 'local_path' in img_data and img_data['local_path']:
                    if os.path.exists(img_data['local_path']):
                        # Local file exists, keep it
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
                        # URL is valid
                        fixed_count += 1
                else:
                    # Not a valid URL
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