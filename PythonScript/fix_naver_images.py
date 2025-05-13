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
    # Check for Naver link
    naver_link_cols = ['네이버 쇼핑 링크', '네이버 링크']
    has_link = False
    for col in naver_link_cols:
        if col in row_data and isinstance(row_data[col], str):
            link = row_data[col].strip()
            if link and link not in ['-', 'None', '']:
                has_link = True
                break
    
    # Check for Naver price
    price_cols = ['판매단가(V포함)(3)', '네이버 판매단가', '판매단가3 (VAT포함)', '네이버 기본수량']
    has_price = False
    for col in price_cols:
        if col in row_data and pd.notna(row_data[col]):
            price = row_data[col]
            if price not in [0, '-', '', None]:
                has_price = True
                break
    
    return has_link or has_price

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