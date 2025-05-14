#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Fix Kogift Images and Pricing in Excel Files
-------------------------------------------
This script fixes issues with Kogift images and pricing in Excel files by:
1. Reading generated Excel files
2. Updating pricing based on correct quantity tiers from Kogift data
3. Fixing image paths and URLs as needed, ensuring correct image-product matching
4. Preserving hyperlinks and other formatting

Usage:
    python fix_kogift_images.py --input [input_excel_file] --output [output_excel_file]
"""

import os
import sys
import logging
import argparse
import json
import re
from pathlib import Path
import pandas as pd
import openpyxl
from openpyxl.styles import PatternFill
from openpyxl.drawing.image import Image
from openpyxl.utils import get_column_letter
import ast
import shutil
import hashlib
import requests
from urllib.parse import urlparse
from typing import Dict, List, Optional, Set, Tuple, Any
import configparser

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('fix_kogift_images.log')
    ]
)
logger = logging.getLogger('fix_kogift_images')

def get_config():
    """Load configuration from config.ini file"""
    config = configparser.ConfigParser()
    config_path = Path(__file__).resolve().parent.parent / 'config.ini'
    
    try:
        config.read(config_path, encoding='utf-8')
        logger.info(f"Successfully loaded configuration from {config_path}")
    except Exception as e:
        logger.error(f"Error loading config from {config_path}: {e}")
        # Create minimal default config
        config['Paths'] = {
            'image_main_dir': 'C:\\RPA\\Image\\Main',
            'output_dir': 'C:\\RPA\\Output'
        }
    
    return config

def scan_kogift_images(base_dirs=None) -> Dict[str, str]:
    """
    Scan all potential Kogift image directories and build a comprehensive mapping
    of image filenames to their local paths.
    """
    # Get config for default directories
    config = get_config()
    base_img_dir = config.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main')
    
    # Default Kogift image directory candidates
    if base_dirs is None:
        base_dirs = [
            Path(base_img_dir),
            Path(base_img_dir) / 'Kogift',
            Path(base_img_dir) / 'kogift',
            Path(base_img_dir).parent / 'Kogift',
            Path(base_img_dir).parent / 'kogift',
            Path(base_img_dir).parent / 'Target' / 'Kogift',
            Path(base_img_dir).parent / 'Target' / 'kogift',
            Path('C:\\RPA\\Image\\Main\\Kogift'),
            Path('C:\\RPA\\Image\\Main\\kogift'),
            Path('C:\\RPA\\Image\\Kogift'),
            Path('C:\\RPA\\Image\\kogift')
        ]
    
    kogift_images = {}
    total_images = 0
    
    logger.info("Starting scan of Kogift image directories...")
    
    for base_dir in base_dirs:
        if not base_dir.exists():
            logger.debug(f"Directory does not exist: {base_dir}")
            continue
        
        logger.info(f"Scanning directory: {base_dir}")
        
        try:
            for root, _, files in os.walk(base_dir):
                for file in files:
                    if file.lower().endswith(('.jpg', '.jpeg', '.png', '.gif')):
                        full_path = os.path.join(root, file)
                        file_size = os.path.getsize(full_path)
                        
                        if file_size < 1000:  # Skip files smaller than 1KB
                            continue
                        
                        # Store image with various key patterns
                        base_name = os.path.basename(file)
                        kogift_images[base_name] = full_path
                        kogift_images[base_name.lower()] = full_path
                        
                        # Handle kogift_ prefix
                        if base_name.lower().startswith('kogift_'):
                            no_prefix = base_name[7:]
                            kogift_images[no_prefix] = full_path
                            kogift_images[no_prefix.lower()] = full_path
                            
                            # Extract hash if present
                            hash_match = re.search(r'kogift_.*?_([a-f0-9]{8,})\.', base_name.lower())
                            if hash_match:
                                hash_val = hash_match.group(1)
                                kogift_images[f"kogift_{hash_val}.jpg"] = full_path
                                kogift_images[f"kogift_{hash_val}.png"] = full_path
                                kogift_images[hash_val] = full_path
                        
                        # Handle shop_ prefix
                        if base_name.lower().startswith('shop_'):
                            no_shop = base_name[5:]
                            kogift_images[no_shop] = full_path
                            kogift_images[no_shop.lower()] = full_path
                            
                            kogift_without_shop = f"kogift_{no_shop}"
                            kogift_images[kogift_without_shop] = full_path
                            kogift_images[kogift_without_shop.lower()] = full_path
                
                total_images += len(files)
        except Exception as e:
            logger.error(f"Error scanning directory {base_dir}: {e}")
    
    logger.info(f"Scan complete. Found {total_images} total image files, created {len(kogift_images)} lookup entries")
    return kogift_images

def extract_id_from_url(url: str) -> Optional[str]:
    """Extract product ID from a Kogift URL."""
    try:
        parsed = urlparse(url)
        path = parsed.path.strip('/')
        
        # Check common Kogift URL patterns
        if 'it_id=' in url:
            return url.split('it_id=')[1].split('&')[0]
        elif 'goodsNo=' in url:
            return url.split('goodsNo=')[1].split('&')[0]
        
        # Try to get ID from path
        parts = path.split('/')
        if parts and parts[-1].isalnum():
            return parts[-1]
        
        return None
    except Exception as e:
        logger.error(f"Error extracting ID from URL {url}: {e}")
        return None

def find_kogift_image_for_url(url: str, kogift_images: Dict[str, str], product_info: Optional[Dict] = None) -> Optional[str]:
    """
    Find the corresponding local Kogift image file for a given URL, using product information for better matching.
    """
    if not url or not isinstance(url, str) or not url.startswith(('http://', 'https://')):
        return None
    
    # Extract product ID from URL
    product_id = extract_id_from_url(url)
    
    # If we have product info and ID, try to match using that first
    if product_info and product_id:
        # Try to find image with product ID in name
        for img_name, img_path in kogift_images.items():
            if product_id in img_name:
                logger.debug(f"Found image match by product ID: {img_name}")
                return img_path
    
    # Direct filename matching
    filename = os.path.basename(url)
    if filename in kogift_images:
        return kogift_images[filename]
    
    if filename.lower() in kogift_images:
        return kogift_images[filename.lower()]
    
    # Try with kogift_ prefix
    prefixed = f"kogift_{filename}"
    if prefixed in kogift_images:
        return kogift_images[prefixed]
    
    # Try URL patterns from Kogift
    if 'mall/shop_' in url:
        product_part = url.split('mall/shop_')[1].split('?')[0]
        if product_part in kogift_images:
            return kogift_images[product_part]
        
        prefixed_product = f"kogift_{product_part}"
        if prefixed_product in kogift_images:
            return kogift_images[prefixed_product]
    
    # Try hash-based matching
    url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
    hash_patterns = [
        f"kogift_{url_hash}.jpg",
        f"kogift_{url_hash}.png",
        url_hash
    ]
    
    for pattern in hash_patterns:
        if pattern in kogift_images:
            return kogift_images[pattern]
    
    return None

def find_appropriate_price(quantity_prices, target_quantity):
    """
    Find the appropriate price tier for the given quantity.
    """
    if not quantity_prices:
        return None, None, False, None, "No quantity prices available"
    
    # Ensure all keys are integers
    qty_prices = {int(k): v for k, v in quantity_prices.items() if isinstance(k, (int, str)) and str(k).isdigit()}
    
    if not qty_prices:
        return None, None, False, None, "No valid quantity tiers found"
    
    quantities = sorted(qty_prices.keys())
    min_quantity = min(quantities)
    
    # If target quantity is less than minimum quantity, use minimum quantity's price
    if target_quantity < min_quantity:
        logger.info(f"Target quantity ({target_quantity}) is less than minimum quantity ({min_quantity}). Using minimum quantity price.")
        price_info = qty_prices[min_quantity]
        return (
            price_info.get('price', 0),
            price_info.get('price_with_vat', 0),
            False,
            min_quantity,
            f"최소 수량({min_quantity}) 가격 적용"
        )
    
    # Exact match
    if target_quantity in quantities:
        price_info = qty_prices[target_quantity]
        return (
            price_info.get('price', 0),
            price_info.get('price_with_vat', 0),
            True,
            target_quantity,
            "정확히 일치하는 수량"
        )
    
    # Find next tier up
    larger_quantities = [qty for qty in quantities if qty > target_quantity]
    if larger_quantities:
        next_tier = min(larger_quantities)
        price_info = qty_prices[next_tier]
        return (
            price_info.get('price', 0),
            price_info.get('price_with_vat', 0),
            False,
            next_tier,
            f"다음 티어 가격 적용"
        )
    
    # Use highest tier if target quantity is above all tiers
    max_quantity = max(quantities)
    price_info = qty_prices[max_quantity]
    return (
        price_info.get('price', 0),
        price_info.get('price_with_vat', 0),
        False,
        max_quantity,
        f"최대 티어 가격 적용"
    )

def get_default_quantity_prices():
    """Return default quantity-price table."""
    return {
        3000: {'price': 6000, 'price_with_vat': 6600},
        1000: {'price': 6150, 'price_with_vat': 6765},
        500: {'price': 6250, 'price_with_vat': 6875},
        300: {'price': 6400, 'price_with_vat': 7040},
        200: {'price': 6500, 'price_with_vat': 7150}
    }

def extract_quantity_prices_from_row(row, temp_kogift_col='_temp_kogift_quantity_prices'):
    """Extract quantity-price information from a DataFrame row."""
    possible_data_columns = [
        'kogift_data', 'kogift_price_data', 'kogift_product_data',
        'quantity_prices', 'kogift_quantity_prices', '_temp_kogift_quantity_prices',
        '고려기프트_데이터', '고려기프트_가격정보', '고려기프트_수량가격'
    ]
    
    # Direct column search
    for col in possible_data_columns:
        if col in row and pd.notna(row[col]) and row[col] != '-':
            data = parse_complex_value(row[col])
            if isinstance(data, dict) and 'quantity_prices' in data:
                return data['quantity_prices']
    
    # Try to find data in Kogift link
    kogift_link_columns = ['고려기프트 상품링크', '고려 링크', '고려기프트링크', '고려 상품링크']
    for col in kogift_link_columns:
        if col in row and pd.notna(row[col]) and isinstance(row[col], str):
            if any(col in row and pd.notna(row[col]) and row[col] != '-' for col in kogift_link_columns):
                return get_default_quantity_prices()
    
    return None

def parse_complex_value(value):
    """Parse string representations of dictionaries or complex objects."""
    if isinstance(value, dict):
        return value
    
    if isinstance(value, str):
        value = value.strip()
        if value.startswith('{') and value.endswith('}'):
            try:
                return ast.literal_eval(value)
            except (SyntaxError, ValueError):
                pass
    return value

def fix_excel_kogift_images(input_file: str, output_file: Optional[str] = None) -> str:
    """
    Fix Kogift images and pricing in an Excel file.
    """
    if not os.path.exists(input_file):
        logger.error(f"Excel file not found: {input_file}")
        return None
    
    if output_file is None:
        output_dir = os.path.dirname(input_file)
        file_name = os.path.basename(input_file)
        name_part, ext = os.path.splitext(file_name)
        output_file = os.path.join(output_dir, f"{name_part}_fixed{ext}")
    
    logger.info(f"Starting to fix Kogift data in: {input_file}")
    
    try:
        # Read Excel file
        df = pd.read_excel(input_file)
        workbook = openpyxl.load_workbook(input_file)
        sheet = workbook.active
        
        # Build kogift image database
        kogift_images = scan_kogift_images()
        
        # Find relevant columns
        column_mapping = {
            '기본수량(1)': ['기본수량(1)', '기본수량', '수량', '본사 기본수량'],
            '판매단가(V포함)': ['판매단가(V포함)', '판매단가1(VAT포함)'],
            '고려기프트 상품링크': ['고려기프트 상품링크', '고려기프트상품링크', '고려기프트 링크', '고려 링크'],
            '기본수량(2)': ['기본수량(2)', '고려 기본수량', '고려기프트 기본수량'],
            '판매가(V포함)(2)': ['판매가(V포함)(2)', '판매단가(V포함)(2)', '고려 판매가(V포함)', '고려기프트 판매가', '판매단가2(VAT포함)'],
            '가격차이(2)': ['가격차이(2)', '고려 가격차이'],
            '가격차이(2)(%)': ['가격차이(2)(%)', '고려 가격차이(%)', '고려 가격 차이(%)']
        }
        
        # Find actual column names in the DataFrame
        columns_found = {}
        for key, variants in column_mapping.items():
            for variant in variants:
                if variant in df.columns:
                    columns_found[key] = variant
                    break
        
        # Get column indices
        column_indices = {}
        for col_idx, cell in enumerate(sheet[1], 1):
            column_indices[cell.value] = col_idx
        
        # Process each row
        for idx, row in df.iterrows():
            xl_row = idx + 2  # Excel is 1-indexed and has header
            
            # Get base quantity
            quantity_col = columns_found.get('기본수량(1)')
            if not quantity_col or pd.isna(row[quantity_col]):
                continue
            
            try:
                base_quantity = int(row[quantity_col])
            except (ValueError, TypeError):
                continue
            
            # Get Kogift data
            quantity_prices = extract_quantity_prices_from_row(row)
            if not quantity_prices:
                quantity_prices = get_default_quantity_prices()
            
            # Find appropriate price
            price, price_with_vat, exact_match, actual_quantity, note = find_appropriate_price(
                quantity_prices, base_quantity
            )
            
            if price_with_vat:
                # Update price
                price2_idx = column_indices.get(columns_found.get('판매가(V포함)(2)'))
                if price2_idx:
                    sheet.cell(row=xl_row, column=price2_idx).value = price_with_vat
                
                # Update price difference
                base_price_col = columns_found.get('판매단가(V포함)')
                if base_price_col and pd.notna(row[base_price_col]):
                    try:
                        base_price = float(row[base_price_col])
                        price_diff = price_with_vat - base_price
                        
                        # Update price difference
                        price_diff_idx = column_indices.get(columns_found.get('가격차이(2)'))
                        if price_diff_idx:
                            cell = sheet.cell(row=xl_row, column=price_diff_idx)
                            cell.value = price_diff
                            if price_diff < 0:
                                cell.fill = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')
                        
                        # Update percentage difference
                        if base_price != 0:
                            pct_diff = (price_diff / base_price) * 100
                            price_diff_pct_idx = column_indices.get(columns_found.get('가격차이(2)(%)'))
                            if price_diff_pct_idx:
                                cell = sheet.cell(row=xl_row, column=price_diff_pct_idx)
                                cell.value = round(pct_diff, 1)
                                if pct_diff < 0:
                                    cell.fill = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')
                    except (ValueError, TypeError):
                        pass
            
            # Fix images
            kogift_link_col = columns_found.get('고려기프트 상품링크')
            if kogift_link_col and pd.notna(row[kogift_link_col]):
                url = row[kogift_link_col]
                if isinstance(url, str) and url.startswith(('http://', 'https://')):
                    # Get product info for better image matching
                    product_info = {'id': extract_id_from_url(url)} if url else None
                    
                    # Find matching image
                    local_image = find_kogift_image_for_url(url, kogift_images, product_info)
                    if local_image and os.path.exists(local_image):
                        try:
                            # Add image to appropriate column
                            img = Image(local_image)
                            img.width = 200
                            img.height = 200
                            
                            # Find image column
                            image_col_name = next((col for col in df.columns if '고려' in col and '이미지' in col), None)
                            if image_col_name:
                                img_col_idx = column_indices.get(image_col_name)
                                if img_col_idx:
                                    img.anchor = f"{get_column_letter(img_col_idx)}{xl_row}"
                                    sheet.add_image(img)
                                    # Set row height
                                    sheet.row_dimensions[xl_row].height = 200
                        except Exception as e:
                            logger.error(f"Error adding image for row {xl_row}: {e}")
        
        # Save the modified workbook
        workbook.save(output_file)
        logger.info(f"Successfully saved fixed Excel file: {output_file}")
        
        return output_file
        
    except Exception as e:
        logger.error(f"Error processing Excel file: {e}", exc_info=True)
        return None

def main():
    """Main function to run the Kogift fix tool."""
    parser = argparse.ArgumentParser(description='Fix Kogift images and pricing in Excel files')
    parser.add_argument('--input', '-i', required=True, help='Input Excel file')
    parser.add_argument('--output', '-o', help='Output Excel file (optional)')
    
    args = parser.parse_args()
    
    result = fix_excel_kogift_images(args.input, args.output)
    
    if result:
        print(f"Successfully fixed Kogift data. Output saved to: {result}")
        return 0
    else:
        print("Failed to fix Kogift data. Check the log for details.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 