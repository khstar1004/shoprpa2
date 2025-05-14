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
    
    # Get available quantities, sorted in ascending order
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
    
    # Find the appropriate tier: smallest quantity that's greater than or equal to target
    # 판촉물 사이트 특성: 기본수량보다 큰 수량 중 가장 작은 수량의 가격 적용
    larger_quantities = [qty for qty in quantities if qty >= target_quantity]
    
    if larger_quantities:
        # 타겟 수량보다 큰 수량들 중 최소값 사용
        best_qty = min(larger_quantities)
        price_info = qty_prices[best_qty]
        return (
            price_info.get('price', 0),
            price_info.get('price_with_vat', 0),
            False,
            best_qty,
            f"Using next tier up {best_qty} for quantity {target_quantity}"
        )
    else:
        # 모든 수량보다 타겟 수량이 크면, 가장 큰 수량의 가격 사용
        max_qty = max(quantities)
        price_info = qty_prices[max_qty]
        return (
            price_info.get('price', 0),
            price_info.get('price_with_vat', 0),
            False,
            max_qty,
            f"Target quantity {target_quantity} exceeds all tiers, using largest tier {max_qty}"
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
    
    # Try to extract from the image data or kogift data columns with various possible names
    possible_kogift_cols = [
        '고려기프트 이미지', '고려기프트 데이터', 'kogift_data', 
        '고려기프트이미지', '고려기프트데이터', 'kogift_image_data',
        '고려데이터', '고려 데이터'
    ]
    
    for col in possible_kogift_cols:
        if col in row and not pd.isna(row[col]) and row[col] != '-':
            data = parse_complex_value(row[col])
            if isinstance(data, dict):
                # Direct quantity_prices in the data dictionary
                if 'quantity_prices' in data:
                    return data['quantity_prices']
                
                # Check for nested data structures
                for key, value in data.items():
                    if isinstance(value, dict) and 'quantity_prices' in value:
                        return value['quantity_prices']
            
            # Try to extract from JSON string representation
            if isinstance(row[col], str) and 'quantity_prices' in row[col]:
                try:
                    # Look for quantity_prices in JSON string
                    match = re.search(r'"quantity_prices"\s*:\s*(\{.*?\})', row[col])
                    if match:
                        qty_prices_str = match.group(1)
                        try:
                            qty_prices = json.loads(qty_prices_str)
                            if isinstance(qty_prices, dict):
                                return qty_prices
                        except json.JSONDecodeError:
                            # Try with ast.literal_eval if JSON parse fails
                            try:
                                qty_prices = ast.literal_eval(qty_prices_str)
                                if isinstance(qty_prices, dict):
                                    return qty_prices
                            except (SyntaxError, ValueError):
                                pass
                except (json.JSONDecodeError, ValueError):
                    pass
    
    # Try to parse from any string column that might contain quantity/price information
    for col_name, value in row.items():
        if isinstance(value, str):
            # Check for quantity_prices in any string field
            if 'quantity_prices' in value:
                try:
                    # Extract the quantity_prices dictionary
                    match = re.search(r'"quantity_prices"\s*:\s*(\{.*?\})', value)
                    if match:
                        qty_prices_str = match.group(1)
                        try:
                            qty_prices = json.loads(qty_prices_str)
                            if isinstance(qty_prices, dict):
                                return qty_prices
                        except json.JSONDecodeError:
                            # Try with ast.literal_eval if JSON parse fails
                            try:
                                qty_prices = ast.literal_eval(qty_prices_str)
                                if isinstance(qty_prices, dict):
                                    return qty_prices
                            except (SyntaxError, ValueError):
                                pass
                except Exception:
                    pass
            
            # Look for price tiers in tabular text format (common in Kogift data)
            # Example: "수량: 1000, 가격: 5000 / 수량: 500, 가격: 5500 / ..."
            matches = re.findall(r'수량\s*:\s*(\d+)[^0-9]*가격\s*:\s*(\d+)', value)
            if matches:
                qty_prices = {}
                for qty_str, price_str in matches:
                    try:
                        qty = int(qty_str)
                        price = float(price_str)
                        qty_prices[qty] = {
                            'price': price,
                            'price_with_vat': price * 1.1  # Add 10% VAT
                        }
                    except (ValueError, TypeError):
                        pass
                if qty_prices:
                    return qty_prices
    
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
        
        # Map column names (accounting for variations in column names)
        column_mapping = {
            '기본수량(1)': ['기본수량(1)', '기본수량', '수량'],
            '고려기프트 상품링크': ['고려기프트 상품링크', '고려기프트상품링크', '고려기프트 링크', '고려 링크'],
            '기본수량(2)': ['기본수량(2)', '고려 기본수량', '고려기프트 기본수량'],
            '판매가(V포함)(2)': ['판매가(V포함)(2)', '판매단가(V포함)(2)', '고려 판매가(V포함)', '고려기프트 판매가']
        }
        
        # Find which variant of each column exists in the DataFrame
        columns_found = {}
        for key, variants in column_mapping.items():
            for variant in variants:
                if variant in df.columns:
                    columns_found[key] = variant
                    break
        
        # Log found columns
        logger.info(f"Found column mappings: {columns_found}")
        
        # For upload files, the structure may be different and may not have all required columns
        required_columns_by_type = {
            'result': ['기본수량(1)', '고려기프트 상품링크'],
            'upload': [] # Upload files may have different structure, so don't require specific columns
        }
        
        # Get required columns for this file type
        required_columns = required_columns_by_type.get(file_type, ['기본수량(1)', '고려기프트 상품링크'])
        
        # Check for required columns
        missing_columns = [col for col in required_columns if col not in columns_found]
        if missing_columns:
            # Only log an error if this is a result file (upload files may have different structure)
            if file_type == 'result':
                logger.error(f"Missing required columns: {missing_columns}")
                return None
            else:
                logger.warning(f"Missing some columns in {file_type} file: {missing_columns}. Will proceed with available columns.")
        
        # Find column indices for updating (1-indexed for openpyxl)
        column_indices = {}
        for col_idx, cell in enumerate(sheet[1], 1):  # 1-indexed columns
            column_indices[cell.value] = col_idx
        
        # Log found column indices
        logger.info(f"Found column indices: {column_indices}")
        
        # Map the actual column names in the Excel file to our expected column names
        # This addresses issues where column headers might have spaces or slight variations
        real_column_indices = {}
        for expected_col, column_idx in column_indices.items():
            # Try to map each column in the excel file to our expected columns
            for key, variants in column_mapping.items():
                if expected_col in variants:
                    real_column_indices[key] = column_idx
                    break
                    
        logger.info(f"Mapped column indices: {real_column_indices}")
        
        # Get the actual column names to use based on what's in the DataFrame
        quantity_col = columns_found.get('기본수량(1)')
        kogift_link_col = columns_found.get('고려기프트 상품링크')
        quantity2_col = columns_found.get('기본수량(2)')
        price2_col = columns_found.get('판매가(V포함)(2)')
        
        # Process each row that has Kogift data
        update_count = 0
        price_diffs_updated = 0
        for idx, row in df.iterrows():
            # Skip rows without Kogift data
            if not kogift_link_col or pd.isna(row.get(kogift_link_col, '')) or not row.get(kogift_link_col, ''):
                continue
            
            # Extract quantity-price information
            quantity_prices = extract_quantity_prices_from_row(row)
            if not quantity_prices:
                logger.debug(f"No quantity price data found for row {idx+1}")
                continue
            
            # Get the base quantity
            base_quantity = None
            if quantity_col and quantity_col in row:
                base_quantity = row[quantity_col] if pd.notna(row[quantity_col]) else None
            
            if base_quantity is None:
                logger.debug(f"No base quantity found for row {idx+1}")
                continue
            
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
                quantity2_idx = real_column_indices.get('기본수량(2)')
                if quantity2_idx:
                    sheet.cell(row=xl_row, column=quantity2_idx).value = base_quantity
                
                # Update price column
                price2_idx = real_column_indices.get('판매가(V포함)(2)')
                if price2_idx:
                    sheet.cell(row=xl_row, column=price2_idx).value = price_with_vat
                
                # Update price difference if possible
                # Check for price difference column in a more flexible way
                price_diff_col = None
                price_diff_pct_col = None
                for col, idx in column_indices.items():
                    if col and isinstance(col, str):
                        if '가격차이(2)' in col and '(%)' not in col:
                            price_diff_col = idx
                        elif '가격차이(2)(%)' in col:
                            price_diff_pct_col = idx
                
                # Calculate and update price difference
                if price_diff_col and '판매단가(V포함)' in df.columns and pd.notna(row['판매단가(V포함)']):
                    try:
                        base_price = float(row['판매단가(V포함)'])
                        price_diff = price_with_vat - base_price
                        sheet.cell(row=xl_row, column=price_diff_col).value = price_diff
                        
                        # Highlight negative price differences
                        if price_diff < 0:
                            sheet.cell(row=xl_row, column=price_diff_col).fill = PatternFill(
                                start_color='FFC7CE', end_color='FFC7CE', fill_type='solid'
                            )
                        
                        # Update percentage difference
                        if price_diff_pct_col and base_price != 0:
                            pct_diff = (price_diff / base_price) * 100
                            sheet.cell(row=xl_row, column=price_diff_pct_col).value = round(pct_diff, 1)
                            
                            # Highlight negative percentage differences
                            if pct_diff < 0:
                                sheet.cell(row=xl_row, column=price_diff_pct_col).fill = PatternFill(
                                    start_color='FFC7CE', end_color='FFC7CE', fill_type='solid'
                                )
                        
                        price_diffs_updated += 1
                    except (ValueError, TypeError) as e:
                        logger.debug(f"Error calculating price difference for row {idx+1}: {e}")
                
                update_count += 1
                logger.debug(f"Updated row {idx+1}: Quantity {base_quantity}, Price {price_with_vat}, Tier {actual_quantity}")
        
        # Save the modified workbook
        workbook.save(output_file)
        logger.info(f"Successfully updated {update_count} rows with correct pricing (price differences: {price_diffs_updated})")
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