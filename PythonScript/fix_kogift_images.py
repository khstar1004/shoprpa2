#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Fix Kogift Images and Pricing in Excel Files
-------------------------------------------
This script fixes issues with Kogift images and pricing in Excel files by:
1. Reading generated Excel files
2. Updating pricing based on correct quantity tiers from Kogift data
3. Fixing image paths and URLs as needed
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
import ast
import shutil

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

def find_appropriate_price(quantity_prices, target_quantity):
    """
    Find the appropriate price tier for the given quantity.
    
    Args:
        quantity_prices: Dictionary of quantity-price information
        target_quantity: Target quantity to match
        
    Returns:
        tuple: (price, price_with_vat, exact_match, actual_quantity, note)
    """
    if not quantity_prices:
        return None, None, False, None, "No quantity prices available"
    
    # Ensure all keys are integers (sometimes they're stored as strings)
    qty_prices = {}
    for k, v in quantity_prices.items():
        try:
            qty_prices[int(k)] = v
        except (ValueError, TypeError):
            continue
    
    # Get available quantities, sorted
    quantities = sorted(qty_prices.keys())
    if not quantities:
        return None, None, False, None, "No valid quantity tiers found"
    
    # Check if target quantity exactly matches a tier
    if target_quantity in quantities:
        price_info = qty_prices[target_quantity]
        return (
            price_info.get('price', 0),
            price_info.get('price_with_vat', 0),
            True,
            target_quantity,
            "Exact match"
        )
    
    # Find the highest tier that's not greater than the target quantity
    applicable_quantities = [q for q in quantities if q <= target_quantity]
    if applicable_quantities:
        best_qty = max(applicable_quantities)
        price_info = qty_prices[best_qty]
        return (
            price_info.get('price', 0),
            price_info.get('price_with_vat', 0),
            False,
            best_qty,
            f"Using tier {best_qty}"
        )
    
    # If target is below smallest tier, use the smallest tier
    min_qty = min(quantities)
    price_info = qty_prices[min_qty]
    return (
        price_info.get('price', 0),
        price_info.get('price_with_vat', 0),
        False,
        min_qty,
        f"Using minimum tier {min_qty}"
    )

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

def extract_quantity_prices_from_row(row, temp_kogift_col='_temp_kogift_quantity_prices'):
    """
    Extract quantity-price information from a DataFrame row.
    
    Args:
        row: DataFrame row
        temp_kogift_col: Name of temporary column with Kogift quantity price data
        
    Returns:
        dict: Dictionary of quantity prices or None
    """
    # Check if the dedicated temporary column exists
    if temp_kogift_col in row and not pd.isna(row[temp_kogift_col]):
        # This is the ideal case - data processing stored quantity prices here
        qty_prices = parse_complex_value(row[temp_kogift_col])
        if isinstance(qty_prices, dict):
            return qty_prices
    
    # Try to extract from the image data as it might contain quantity_prices
    for col in ['고려기프트 이미지', '고려기프트 데이터', 'kogift_data']:
        if col in row and not pd.isna(row[col]) and row[col] != '-':
            data = parse_complex_value(row[col])
            if isinstance(data, dict) and 'quantity_prices' in data:
                return data['quantity_prices']
    
    # Try to parse from the raw JSON in any column that might contain it
    for col_name in row.index:
        if isinstance(row[col_name], str) and 'quantity_prices' in row[col_name]:
            try:
                # Look for quantity_prices in JSON string
                match = re.search(r'"quantity_prices"\s*:\s*(\{.*?\})', row[col_name])
                if match:
                    qty_prices_str = match.group(1)
                    qty_prices = json.loads(qty_prices_str)
                    if isinstance(qty_prices, dict):
                        return qty_prices
            except (json.JSONDecodeError, ValueError):
                pass
    
    # If we reach here, we couldn't find quantity price data
    return None

def fix_excel_kogift_images(input_file, output_file=None):
    """
    Fix Kogift images and pricing in Excel files.
    
    Args:
        input_file: Path to input Excel file
        output_file: Path to output Excel file (optional)
        
    Returns:
        str: Path to output file if successful, None otherwise
    """
    try:
        logger.info(f"Reading Excel file: {input_file}")
        
        # Set output file path if not specified
        if not output_file:
            input_path = Path(input_file)
            output_file = str(input_path.parent / f"{input_path.stem}_fixed{input_path.suffix}")
        
        # Read the Excel file
        df = pd.read_excel(input_file)
        logger.info(f"Successfully read Excel file with {len(df)} rows")
        
        # Check if this is a result or upload file
        is_result_file = "result" in os.path.basename(input_file).lower()
        is_upload_file = "upload" in os.path.basename(input_file).lower()
        file_type = "result" if is_result_file else "upload" if is_upload_file else "unknown"
        logger.info(f"Detected file type: {file_type}")
        
        # Make a copy of the workbook with openpyxl to preserve formatting and hyperlinks
        workbook = openpyxl.load_workbook(input_file)
        sheet = workbook.active
        
        # Check for required columns
        required_columns = ['기본수량(1)', '고려기프트 상품링크']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return None
        
        # Find column indices for updating (1-indexed for openpyxl)
        column_indices = {}
        for col_idx, cell in enumerate(sheet[1], 1):  # 1-indexed columns
            column_indices[cell.value] = col_idx
        
        # Log found columns 
        logger.info(f"Found column indices: {column_indices}")
        
        # Important column names to look for
        if '기본수량(2)' not in column_indices:
            logger.warning("Column '기본수량(2)' not found. Prices may not be updated correctly.")
        
        price_column_name = '판매가(V포함)(2)'
        if price_column_name not in column_indices:
            # Try alternative name
            price_column_name = '판매단가(V포함)(2)'
            if price_column_name not in column_indices:
                logger.warning(f"Neither '판매가(V포함)(2)' nor '판매단가(V포함)(2)' found. Prices cannot be updated.")
        
        # Process each row that has Kogift data
        update_count = 0
        for idx, row in df.iterrows():
            # Skip rows without Kogift data
            if pd.isna(row['고려기프트 상품링크']) or not row['고려기프트 상품링크']:
                continue
            
            # Extract quantity-price information
            quantity_prices = extract_quantity_prices_from_row(row)
            if not quantity_prices:
                logger.debug(f"No quantity price data found for row {idx+1}")
                continue
            
            # Get the base quantity
            base_quantity = row['기본수량(1)'] if pd.notna(row['기본수량(1)']) else 1
            
            try:
                # Convert to integer (some files might have it as string or float)
                base_quantity = int(base_quantity)
            except (ValueError, TypeError):
                logger.warning(f"Invalid base quantity in row {idx+1}: {base_quantity}")
                continue
            
            # Find the appropriate price tier
            price, price_with_vat, exact_match, actual_quantity, note = find_appropriate_price(
                quantity_prices, base_quantity
            )
            
            if price_with_vat:
                # Calculate row in Excel (1-indexed and header row)
                xl_row = idx + 2
                
                # Update quantity column
                if '기본수량(2)' in column_indices:
                    sheet.cell(row=xl_row, column=column_indices['기본수량(2)']).value = base_quantity
                
                # Update price column
                if price_column_name in column_indices:
                    sheet.cell(row=xl_row, column=column_indices[price_column_name]).value = price_with_vat
                
                # Update price difference if possible
                if '가격차이(2)' in column_indices and '판매단가(V포함)' in df.columns and pd.notna(row['판매단가(V포함)']):
                    base_price = float(row['판매단가(V포함)'])
                    price_diff = price_with_vat - base_price
                    sheet.cell(row=xl_row, column=column_indices['가격차이(2)']).value = price_diff
                    
                    # Highlight negative price differences
                    if price_diff < 0:
                        sheet.cell(row=xl_row, column=column_indices['가격차이(2)']).fill = PatternFill(
                            start_color='FFC7CE', end_color='FFC7CE', fill_type='solid'
                        )
                    
                    # Update percentage difference
                    if '가격차이(2)(%)' in column_indices and base_price != 0:
                        pct_diff = (price_diff / base_price) * 100
                        sheet.cell(row=xl_row, column=column_indices['가격차이(2)(%)']).value = round(pct_diff, 1)
                        
                        # Highlight negative percentage differences
                        if pct_diff < 0:
                            sheet.cell(row=xl_row, column=column_indices['가격차이(2)(%)']).fill = PatternFill(
                                start_color='FFC7CE', end_color='FFC7CE', fill_type='solid'
                            )
                
                update_count += 1
                logger.debug(f"Updated row {idx+1}: Quantity {base_quantity}, Price {price_with_vat}, Tier {actual_quantity}")
        
        # Save the modified workbook
        workbook.save(output_file)
        logger.info(f"Successfully updated {update_count} rows with correct pricing")
        logger.info(f"Saved updated Excel file to: {output_file}")
        
        return output_file
        
    except Exception as e:
        logger.error(f"Error processing Excel file: {e}", exc_info=True)
        return None

def main():
    """Standalone script to fix Kogift images and pricing in Excel files"""
    parser = argparse.ArgumentParser(description='Fix Kogift images and pricing in Excel files')
    parser.add_argument('--input', '-i', required=True, help='Input Excel file path')
    parser.add_argument('--output', '-o', help='Output Excel file path (optional)')
    
    args = parser.parse_args()
    
    # Validate input file
    input_file = args.input
    if not os.path.exists(input_file):
        logger.error(f"Input file not found: {input_file}")
        return 1
    
    # Set output file if not specified
    output_file = args.output
    
    logger.info(f"Starting Kogift fix process")
    logger.info(f"Input file: {input_file}")
    logger.info(f"Output file: {output_file or 'Will be auto-generated'}")
    
    # Call the fix function
    result = fix_excel_kogift_images(input_file, output_file)
    
    if result:
        logger.info(f"Successfully fixed Kogift images and pricing. Output saved to: {result}")
        print(f"✅ Successfully fixed Kogift images and pricing in Excel file.")
        print(f"✅ Output saved to: {result}")
        return 0
    else:
        logger.error("Failed to fix Kogift images and pricing")
        print("❌ Failed to fix Kogift images and pricing. Check the log for details.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 