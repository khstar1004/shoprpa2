"""
Excel output fixing script.
This script applies the correct column naming when creating Excel output files.
It also ensures that images are embedded in the result file.
"""

import os
import logging
import pandas as pd
import sys
from pathlib import Path

# First import our column patch to ensure correct names
try:
    from PythonScript.column_patch import patch_column_names
except ImportError:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from PythonScript.column_patch import patch_column_names

# Import needed modules after patch is applied
from PythonScript.excel_utils import (
    FINAL_COLUMN_ORDER, IMAGE_COLUMNS, LINK_COLUMNS_FOR_HYPERLINK,
    create_split_excel_outputs, finalize_dataframe_for_excel
)
from PythonScript.utils import setup_logging

def fix_excel_output(input_file, output_dir=None):
    """
    Fix the Excel output by ensuring correct column naming and image embedding.
    
    Args:
        input_file: Path to the Excel file to process
        output_dir: Directory to save the output files to (default is C:\\RPA\\Output)
        
    Returns:
        Tuple of (result_file_path, upload_file_path)
    """
    # Setup logging
    logger = logging.getLogger()
    setup_logging(logger)
    
    # Default output directory
    if output_dir is None:
        output_dir = "C:\\RPA\\Output"
        
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Load the Excel file
    logger.info(f"Loading Excel file: {input_file}")
    df = pd.read_excel(input_file)
    
    # Log the original column names
    logger.info(f"Original columns: {df.columns.tolist()}")
    
    # Apply our column patch if not already applied
    if '본사 이미지' not in IMAGE_COLUMNS:
        success = patch_column_names()
        if not success:
            logger.error("Failed to patch column names. Output may not match expected format.")
    
    # Finalize the DataFrame for Excel output
    logger.info("Finalizing DataFrame for Excel output...")
    df_finalized = finalize_dataframe_for_excel(df)
    
    # Log the finalized column names
    logger.info(f"Finalized columns: {df_finalized.columns.tolist()}")
    
    # Get a proper name for the output file
    source_name = "상품관리"  # Default name instead of "Unknown"
    if '구분' in df_finalized.columns and not df_finalized.empty:
        if df_finalized['구분'].iloc[0] == 'A':
            source_name = "승인관리"
        elif df_finalized['구분'].iloc[0] == 'P':
            source_name = "가격관리"
            
    # If there's a filename in the input, use that as part of the name
    if isinstance(input_file, str):
        input_basename = os.path.basename(input_file)
        input_name = os.path.splitext(input_basename)[0]
        if input_name and input_name.lower() not in ['temp', 'output', 'result', 'unknown']:
            source_name = f"{source_name}_{input_name}"
    
    # Create the output files
    logger.info(f"Creating Excel output files in directory: {output_dir}")
    # Set a custom source_name in the environment variable for excel_utils to use
    os.environ['EXCEL_SOURCE_NAME'] = source_name
    result_success, upload_success, result_path, upload_path = create_split_excel_outputs(
        df_finalized, output_dir)
    
    # Log the results
    if result_success and upload_success:
        logger.info("Successfully created both result and upload files:")
        logger.info(f"  - Result file (with images): {result_path}")
        logger.info(f"  - Upload file (links only): {upload_path}")
    elif result_success:
        logger.info(f"Successfully created result file: {result_path}")
        logger.warning("Failed to create upload file.")
    elif upload_success:
        logger.warning("Failed to create result file.")
        logger.info(f"Successfully created upload file: {upload_path}")
    else:
        logger.error("Failed to create both result and upload files.")
    
    return result_path, upload_path

if __name__ == "__main__":
    # Handle command-line arguments
    if len(sys.argv) < 2:
        print("Usage: python fix_excel_output.py <input_excel_file> [output_directory]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else None
    
    result_path, upload_path = fix_excel_output(input_file, output_dir)
    
    print("\nExcel files created:")
    if result_path:
        print(f"Result file (with images): {result_path}")
    if upload_path:
        print(f"Upload file (links only): {upload_path}") 