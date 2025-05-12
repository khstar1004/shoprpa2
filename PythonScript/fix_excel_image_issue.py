"""
Script to fix Excel export issues with complex dictionary objects in image columns.

This script will:
1. Process any Excel output files that failed due to complex dictionary objects
2. Convert complex dictionary objects to simple string URLs
3. Save fixed Excel files

Usage:
    python fix_excel_image_issue.py --input <input_path> --output <output_path>
"""

import os
import sys
import argparse
import pandas as pd
import logging
import json
import traceback
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ExcelFixer")

def extract_url_from_complex_value(value):
    """Extract URL from complex dictionary objects or return string representation"""
    # Handle None/NaN values
    if pd.isna(value) or value is None:
        return ""

    # Handle strings
    if isinstance(value, str):
        return value

    # Handle numbers
    if isinstance(value, (int, float)):
        return value
        
    # Handle dictionary values
    if isinstance(value, dict):
        try:
            # Case 1: Nested URL structure {'url': {'url': 'actual_url', ...}}
            if 'url' in value and isinstance(value['url'], dict) and 'url' in value['url']:
                return value['url']['url']
            
            # Case 2: Direct URL {'url': 'actual_url'}
            elif 'url' in value and isinstance(value['url'], str):
                return value['url']
                
            # Case 3: Local path
            elif 'local_path' in value and value['local_path']:
                return value['local_path']
            
            # Case 4: Product name
            elif 'product_name' in value:
                return f"Product: {value['product_name']}"
            
            # Default: Convert to string
            return json.dumps(value, ensure_ascii=False)
        except:
            return str(value)
            
    # Handle list/tuple
    if isinstance(value, (list, tuple)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except:
            return str(value)
            
    # Default case
    return str(value)

def sanitize_dataframe_for_excel(df):
    """Convert all complex data types in DataFrame to Excel-compatible strings"""
    if df is None or df.empty:
        return df
        
    # Create a copy to avoid modifying the original
    result_df = df.copy()
    
    # Process each column
    for col in result_df.columns:
        if result_df[col].dtype == 'object':
            result_df[col] = result_df[col].apply(extract_url_from_complex_value)
    
    return result_df

def fix_excel_file(input_path, output_dir=None):
    """Fix Excel file containing complex dictionary objects"""
    try:
        logger.info(f"Processing file: {input_path}")
        
        # Determine output path
        if output_dir is None:
            output_dir = os.path.dirname(input_path)
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate output filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = os.path.splitext(os.path.basename(input_path))[0]
        output_path = os.path.join(output_dir, f"{base_name}_fixed_{timestamp}.xlsx")
        
        # Read input DataFrame
        try:
            df = pd.read_excel(input_path)
            logger.info(f"Successfully read input file. Shape: {df.shape}")
        except Exception as e:
            logger.error(f"Error reading Excel file: {e}")
            return False
        
        # Identify image columns
        image_cols = [col for col in df.columns if any(img_term in col.lower() 
                    for img_term in ['이미지', 'image'])]
        logger.info(f"Found {len(image_cols)} image-related columns: {image_cols}")
        
        # Sanitize DataFrame
        df_fixed = sanitize_dataframe_for_excel(df)
        
        # Write fixed DataFrame to Excel
        df_fixed.to_excel(output_path, index=False)
        logger.info(f"Successfully saved fixed Excel file to: {output_path}")
        
        return output_path
        
    except Exception as e:
        logger.error(f"Error fixing Excel file: {e}")
        logger.error(traceback.format_exc())
        return False

def find_and_fix_excel_files(input_path, output_dir=None):
    """Find and fix all Excel files in the given directory"""
    fixed_files = []
    
    if os.path.isfile(input_path):
        # Single file
        if input_path.lower().endswith(('.xlsx', '.xls')):
            result = fix_excel_file(input_path, output_dir)
            if result:
                fixed_files.append(result)
    else:
        # Directory - process all Excel files
        for root, _, files in os.walk(input_path):
            for filename in files:
                if filename.lower().endswith(('.xlsx', '.xls')):
                    file_path = os.path.join(root, filename)
                    result = fix_excel_file(file_path, output_dir)
                    if result:
                        fixed_files.append(result)
    
    return fixed_files

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Fix Excel files with complex dictionary objects")
    parser.add_argument("--input", required=True, help="Input Excel file or directory")
    parser.add_argument("--output", help="Output directory (default: same as input)")
    args = parser.parse_args()
    
    if not os.path.exists(args.input):
        logger.error(f"Input path does not exist: {args.input}")
        return 1
    
    logger.info(f"Starting Excel file fix process...")
    fixed_files = find_and_fix_excel_files(args.input, args.output)
    
    if fixed_files:
        logger.info(f"Successfully fixed {len(fixed_files)} Excel files:")
        for file_path in fixed_files:
            logger.info(f"  - {file_path}")
        return 0
    else:
        logger.warning("No Excel files were fixed.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 