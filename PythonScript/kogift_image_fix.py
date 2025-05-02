#!/usr/bin/env python3
"""
고려기프트 이미지 URL 처리 문제 해결 스크립트
"""

import os
import pandas as pd
import logging
import re
import json
import shutil
from pathlib import Path
import sys
import traceback
import requests
from urllib.parse import urlparse
import hashlib

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("kogift_image_fix.log")
    ]
)

def download_image(url, save_path):
    """Download an image from URL to the specified path"""
    try:
        response = requests.get(url, stream=True, timeout=10)
        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                response.raw.decode_content = True
                shutil.copyfileobj(response.raw, f)
            return True
        else:
            logging.error(f"Failed to download image. Status code: {response.status_code}, URL: {url}")
            return False
    except Exception as e:
        logging.error(f"Error downloading image from {url}: {e}")
        return False

def extract_id_from_url(url):
    """Extract a unique ID from the URL to use in the filename"""
    parsed = urlparse(url)
    path = parsed.path
    
    # Try to extract ID patterns from various URL formats
    
    # For KoreaGift URLs like https://koreagift.com/ez/upload/mall/1620007229154047_R0.jpg
    koreagift_match = re.search(r'/(\d+_[A-Za-z0-9]+\.[a-zA-Z]+)$', path)
    if koreagift_match:
        return koreagift_match.group(1)
    
    # For KoreaGift URLs like https://koreagift.com/ez/upload/mall/shop_153352127113608_0.jpg
    koreagift_shop_match = re.search(r'/(shop_\d+_\d+\.[a-zA-Z]+)$', path)
    if koreagift_shop_match:
        return koreagift_shop_match.group(1)
    
    # If no specific pattern matches, create a hash of the URL
    hash_obj = hashlib.md5(url.encode())
    return hash_obj.hexdigest()[:10]

def fix_kogift_images(excel_path):
    """
    Fix Kogift images in Excel file by downloading missing images and updating local paths
    
    Args:
        excel_path: Path to the Excel file to fix
    """
    logging.info(f"Starting to fix Kogift images in: {excel_path}")
    
    try:
        # Load Excel file
        df = pd.read_excel(excel_path)
        logging.info(f"Loaded Excel file with {len(df)} rows")
        
        # Check if the column exists
        if '고려기프트 이미지' not in df.columns:
            logging.error("Excel file doesn't have the '고려기프트 이미지' column")
            return False
            
        # Create image directory if it doesn't exist
        base_dir = os.path.dirname(os.path.abspath(excel_path))
        kogift_img_dir = os.path.join(base_dir, "Kogift_Images")
        os.makedirs(kogift_img_dir, exist_ok=True)
        
        # Create a base directory for downloaded images
        download_dir = os.path.join(kogift_img_dir, "Downloaded")
        os.makedirs(download_dir, exist_ok=True)
        
        # Count variables for reporting
        total_rows = len(df)
        fixed_rows = 0
        failed_rows = 0
        skipped_rows = 0
        
        # Process each row
        for idx, row in df.iterrows():
            kogift_img_value = row['고려기프트 이미지']
            
            # Skip if the value is already a dictionary with local_path
            if isinstance(kogift_img_value, dict) and 'local_path' in kogift_img_value and os.path.exists(kogift_img_value['local_path']):
                logging.debug(f"Row {idx}: Already has valid local path: {kogift_img_value['local_path']}")
                skipped_rows += 1
                continue
                
            # Try to parse string as JSON if it's a string
            if isinstance(kogift_img_value, str) and kogift_img_value.strip() != '' and kogift_img_value != '-':
                try:
                    if kogift_img_value.startswith('{') and kogift_img_value.endswith('}'):
                        kogift_img_value = json.loads(kogift_img_value)
                except:
                    # Not valid JSON, might be a URL or other string
                    pass
            
            # Get URL from various possible formats
            url = None
            if isinstance(kogift_img_value, dict) and 'url' in kogift_img_value:
                url = kogift_img_value['url']
            elif isinstance(kogift_img_value, str) and kogift_img_value.startswith('http'):
                url = kogift_img_value
                
            if not url or url == '-' or url.strip() == '':
                logging.debug(f"Row {idx}: No valid URL found")
                skipped_rows += 1
                continue
                
            # Check if URL is valid
            if not url.startswith(('http://', 'https://')):
                logging.warning(f"Row {idx}: Invalid URL format: {url}")
                failed_rows += 1
                continue
                
            # Extract ID from URL for filename
            img_id = extract_id_from_url(url)
            
            # Determine file extension
            file_ext = '.jpg'  # Default to jpg
            if '.' in img_id:
                file_ext = '.' + img_id.split('.')[-1]
                img_id = img_id.rsplit('.', 1)[0]  # Remove extension from ID
            
            # Create filename
            filename = f"kogift_{img_id}{file_ext}"
            save_path = os.path.join(download_dir, filename)
            
            # Download the image if it doesn't exist
            if not os.path.exists(save_path):
                logging.info(f"Row {idx}: Downloading image from {url} to {save_path}")
                if not download_image(url, save_path):
                    logging.error(f"Row {idx}: Failed to download image from {url}")
                    failed_rows += 1
                    continue
            else:
                logging.debug(f"Row {idx}: Image already exists at {save_path}")
            
            # Create or update image data dictionary
            if isinstance(kogift_img_value, dict):
                kogift_img_value['local_path'] = save_path
                if 'original_path' not in kogift_img_value:
                    kogift_img_value['original_path'] = save_path
                if 'source' not in kogift_img_value:
                    kogift_img_value['source'] = 'kogift'
            else:
                kogift_img_value = {
                    'local_path': save_path,
                    'source': 'kogift',
                    'url': url,
                    'original_path': save_path,
                    'score': 1.0
                }
                
            # Update the DataFrame
            df.at[idx, '고려기프트 이미지'] = kogift_img_value
            fixed_rows += 1
            logging.info(f"Row {idx}: Fixed Kogift image data")
            
        # Save the updated Excel file
        output_path = excel_path.replace('.xlsx', '_fixed.xlsx')
        df.to_excel(output_path, index=False)
        
        logging.info(f"Summary: Total rows: {total_rows}, Fixed: {fixed_rows}, Failed: {failed_rows}, Skipped: {skipped_rows}")
        logging.info(f"Saved fixed Excel file to: {output_path}")
        
        return True
        
    except Exception as e:
        logging.error(f"Error fixing Kogift images: {e}")
        logging.error(traceback.format_exc())
        return False

def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print("Usage: python kogift_image_fix.py [path_to_excel_file]")
        sys.exit(1)
        
    excel_path = sys.argv[1]
    if not os.path.exists(excel_path):
        print(f"Error: File not found: {excel_path}")
        sys.exit(1)
        
    success = fix_kogift_images(excel_path)
    if success:
        print(f"Successfully fixed Kogift images. Check the log file for details.")
        sys.exit(0)
    else:
        print(f"Failed to fix Kogift images. Check the log file for details.")
        sys.exit(1)

if __name__ == "__main__":
    main() 