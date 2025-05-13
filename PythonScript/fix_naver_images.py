#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Fix Naver Images in Excel Files
--------------------------------
This script validates and fixes issues with Naver images in Excel files by:
1. Verifying image positions match product names
2. Checking for misplaced images
3. Repairing incorrect image placements
4. Generating a validation report

Usage:
    python fix_naver_images.py --input [input_excel_file] --output [output_excel_file]
"""

import os
import sys
import logging
import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import shutil
import re
import hashlib
from typing import Dict, List, Tuple, Optional
import json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('fix_naver_images.log')
    ]
)
logger = logging.getLogger('fix_naver_images')

def extract_product_info(df: pd.DataFrame) -> Dict[str, Dict]:
    """
    Extract product information and associated image data from DataFrame.
    
    Args:
        df: Input DataFrame containing product and image information
        
    Returns:
        Dictionary mapping product names to their image and metadata
    """
    product_info = {}
    
    for idx, row in df.iterrows():
        product_name = row.get('상품명')
        if not product_name or pd.isna(product_name):
            continue
            
        # Get Naver image data
        naver_image = row.get('네이버 이미지')
        if isinstance(naver_image, str):
            try:
                # Try to parse string as JSON/dict
                naver_image = json.loads(naver_image)
            except:
                # If not JSON, might be direct URL
                if naver_image.startswith(('http://', 'https://')):
                    naver_image = {'url': naver_image}
                else:
                    naver_image = None
        
        # Get product code and other metadata
        product_info[product_name] = {
            'code': row.get('Code'),
            'naver_image': naver_image,
            'naver_link': row.get('네이버 쇼핑 링크'),
            'row_index': idx
        }
    
    return product_info

def validate_image_placement(product_info: Dict[str, Dict]) -> Tuple[List[str], List[str], List[str]]:
    """
    Validate image placements and identify issues.
    
    Returns:
        Tuple of (misplaced_images, missing_images, invalid_images)
    """
    misplaced = []
    missing = []
    invalid = []
    
    for product_name, info in product_info.items():
        naver_image = info.get('naver_image')
        
        if not naver_image:
            missing.append(product_name)
            continue
            
        # Check if image data is valid
        if not isinstance(naver_image, dict) or 'url' not in naver_image:
            invalid.append(product_name)
            continue
            
        # Check if image matches product (using URL patterns or metadata)
        url = naver_image['url']
        if not verify_image_product_match(url, product_name, info.get('code')):
            misplaced.append(product_name)
            
    return misplaced, missing, invalid

def verify_image_product_match(image_url: str, product_name: str, product_code: str) -> bool:
    """
    Verify if an image URL matches the expected product.
    """
    if not image_url:
        return False
        
    # Convert product name to searchable format
    search_name = re.sub(r'[^\w\s-]', '', product_name.lower())
    search_terms = search_name.split()
    
    # Check URL for product identifiers
    url_lower = image_url.lower()
    
    # Look for product code in URL
    if product_code and product_code.lower() in url_lower:
        return True
        
    # Look for significant parts of product name in URL
    significant_matches = 0
    for term in search_terms:
        if len(term) >= 3 and term in url_lower:  # Only check terms of 3+ chars
            significant_matches += 1
            
    # Consider it a match if we find enough significant terms
    return significant_matches >= min(2, len(search_terms))

def fix_image_placement(df: pd.DataFrame, product_info: Dict[str, Dict], 
                       misplaced: List[str]) -> pd.DataFrame:
    """
    Fix misplaced images in the DataFrame.
    """
    df_fixed = df.copy()
    
    # Create image placement map
    image_map = {}
    for product_name in misplaced:
        info = product_info[product_name]
        row_idx = info['row_index']
        
        # Find correct image for this product
        correct_image = find_correct_image(product_name, product_info)
        if correct_image:
            image_map[row_idx] = correct_image
            
    # Apply fixes
    for row_idx, image_data in image_map.items():
        df_fixed.at[row_idx, '네이버 이미지'] = image_data
        
    return df_fixed

def find_correct_image(product_name: str, product_info: Dict[str, Dict]) -> Optional[Dict]:
    """
    Find the correct image for a product by analyzing all available images.
    """
    target_info = product_info[product_name]
    best_match = None
    best_score = 0
    
    # Look through all images to find best match
    for other_name, other_info in product_info.items():
        if other_name == product_name:
            continue
            
        other_image = other_info.get('naver_image')
        if not other_image or not isinstance(other_image, dict):
            continue
            
        # Calculate match score
        score = calculate_match_score(product_name, target_info, other_image)
        if score > best_score:
            best_score = score
            best_match = other_image
            
    return best_match

def calculate_match_score(product_name: str, product_info: Dict, image_data: Dict) -> float:
    """
    Calculate how well an image matches a product.
    Returns score between 0 and 1.
    """
    score = 0.0
    url = image_data.get('url', '')
    
    if not url:
        return score
        
    # Check product code match
    if product_info.get('code') and product_info['code'].lower() in url.lower():
        score += 0.5
        
    # Check product name match
    search_name = re.sub(r'[^\w\s-]', '', product_name.lower())
    search_terms = search_name.split()
    
    url_lower = url.lower()
    for term in search_terms:
        if len(term) >= 3 and term in url_lower:
            score += 0.1
            
    return min(1.0, score)

def generate_report(input_file: str, misplaced: List[str], missing: List[str], 
                   invalid: List[str], fixed_count: int) -> str:
    """
    Generate a detailed report of the validation and fixes.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = f"naver_image_report_{timestamp}.txt"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("네이버 이미지 검증 및 수정 보고서\n")
        f.write("=" * 50 + "\n\n")
        
        f.write(f"검증 파일: {input_file}\n")
        f.write(f"검증 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("1. 잘못 배치된 이미지\n")
        f.write("-" * 30 + "\n")
        for prod in misplaced:
            f.write(f"- {prod}\n")
        f.write(f"\n총 {len(misplaced)}개 발견\n\n")
        
        f.write("2. 누락된 이미지\n")
        f.write("-" * 30 + "\n")
        for prod in missing:
            f.write(f"- {prod}\n")
        f.write(f"\n총 {len(missing)}개 발견\n\n")
        
        f.write("3. 잘못된 이미지 데이터\n")
        f.write("-" * 30 + "\n")
        for prod in invalid:
            f.write(f"- {prod}\n")
        f.write(f"\n총 {len(invalid)}개 발견\n\n")
        
        f.write("4. 수정 결과\n")
        f.write("-" * 30 + "\n")
        f.write(f"수정된 이미지: {fixed_count}개\n")
        
    return report_file

def fix_naver_images(input_file: str, output_file: Optional[str] = None) -> bool:
    """
    Main function to validate and fix Naver images in Excel file.
    
    Args:
        input_file: Path to input Excel file
        output_file: Optional path for output file (default: input_file with _fixed suffix)
        
    Returns:
        bool: True if fixes were applied successfully
    """
    try:
        # Read Excel file
        logger.info(f"Reading Excel file: {input_file}")
        df = pd.read_excel(input_file)
        
        # Extract product information
        logger.info("Extracting product information...")
        product_info = extract_product_info(df)
        
        # Validate image placements
        logger.info("Validating image placements...")
        misplaced, missing, invalid = validate_image_placement(product_info)
        
        # Generate report first
        logger.info("Generating validation report...")
        report_file = generate_report(input_file, misplaced, missing, invalid, len(misplaced))
        
        # Fix misplaced images if any found
        if misplaced:
            logger.info(f"Fixing {len(misplaced)} misplaced images...")
            df_fixed = fix_image_placement(df, product_info, misplaced)
            
            # Save fixed file
            if not output_file:
                base_name = os.path.splitext(input_file)[0]
                output_file = f"{base_name}_fixed.xlsx"
                
            logger.info(f"Saving fixed file to: {output_file}")
            df_fixed.to_excel(output_file, index=False)
            
            logger.info(f"Fixes applied. Report saved to: {report_file}")
            return True
        else:
            logger.info("No misplaced images found.")
            logger.info(f"Validation report saved to: {report_file}")
            return True
            
    except Exception as e:
        logger.error(f"Error fixing Naver images: {e}", exc_info=True)
        return False

def main():
    """Command-line interface"""
    parser = argparse.ArgumentParser(description='Fix Naver images in Excel files')
    parser.add_argument('--input', '-i', required=True, help='Input Excel file path')
    parser.add_argument('--output', '-o', help='Output Excel file path (optional)')
    
    args = parser.parse_args()
    
    # Validate input file
    if not os.path.exists(args.input):
        logger.error(f"Input file not found: {args.input}")
        return 1
        
    # Run the fix
    success = fix_naver_images(args.input, args.output)
    
    if success:
        print("✅ Successfully validated/fixed Naver images.")
        print("✅ Check the generated report for details.")
        return 0
    else:
        print("❌ Failed to process Naver images. Check the log for details.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 