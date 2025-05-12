import os
import logging
import pandas as pd
import openpyxl
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Union, Tuple

# Import from other modules
from excel_constants import (
    FINAL_COLUMN_ORDER, COLUMN_MAPPING_FINAL_TO_UPLOAD,
    UPLOAD_COLUMN_ORDER, IMAGE_COLUMNS
)
from excel_data_processing import (
    flatten_nested_image_dicts, prepare_naver_image_urls_for_upload,
    _prepare_data_for_excel
)
from excel_formatting import (
    _apply_basic_excel_formatting, _apply_upload_file_formatting,
    _add_hyperlinks_to_worksheet, _add_header_footer,
    _apply_conditional_formatting, _process_image_columns,
    _adjust_image_cell_dimensions
)
from excel_image_utils import _process_image_columns

# Initialize logger
logger = logging.getLogger(__name__)

def safe_excel_operation(func):
    """
    데코레이터: Excel 작업 중 발생할 수 있는 예외를 안전하게 처리합니다.
    """
    import functools
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Excel operation failed in {func.__name__}: {str(e)}", exc_info=True)
            return False
    return wrapper

@safe_excel_operation
def create_split_excel_outputs(df_finalized: pd.DataFrame, output_path_base: str) -> Tuple[bool, bool, Optional[str], Optional[str]]:
    """
    Create two Excel files:
    1. Result file: With images, for viewing
    2. Upload file: URL links only, for uploading to systems

    Args:
        df_finalized: The finalized DataFrame with all data
        output_path_base: The base path for output files

    Returns:
        tuple: (result_success, upload_success, result_path, upload_path)
    """
    # Ensure we have valid data
    if df_finalized is None or df_finalized.empty:
        logger.error("No data to write to Excel. DataFrame is empty or None.")
        return False, False, None, None

    # Flatten any nested image dictionaries to prevent Excel conversion errors
    df_finalized = flatten_nested_image_dicts(df_finalized)
    
    logger.info(f"Starting creation of split Excel outputs from finalized DataFrame (Shape: {df_finalized.shape})")
    
    # Default return values (used in case of error)
    result_path = None
    result_success = False
    upload_path = None
    upload_success = False

    try:
        # -----------------------------------------
        # 1. Create Result File (with images)
        # -----------------------------------------
        result_path = f"{output_path_base}_result.xlsx"
        logger.info(f"Creating result file: {result_path} with {len(df_finalized)} rows.")
        
        # Create a new workbook for result file
        workbook_result = openpyxl.Workbook()
        worksheet_result = workbook_result.active
        worksheet_result.title = "제품 가격 비교"
        
        # Write data using the new helper function
        if not _write_data_to_worksheet(worksheet_result, df_finalized):
            logger.error("Failed to write data to result worksheet")
            return False, False, None, None
            
        # Apply common formatting
        _apply_basic_excel_formatting(worksheet_result, df_finalized.columns.tolist())
        _add_hyperlinks_to_worksheet(worksheet_result, df_finalized, hyperlinks_as_formulas=False)
        _add_header_footer(worksheet_result)
        
        # Remove auto filter
        if hasattr(worksheet_result, 'auto_filter') and worksheet_result.auto_filter:
            worksheet_result.auto_filter.ref = None
            logger.info("Removed filter from result Excel file")
        
        # Save without images first
        workbook_result.save(result_path)
        result_success = True
        
        # -----------------------------------------
        # 2. Create Upload File (without images)
        # -----------------------------------------
        upload_path = f"{output_path_base}_upload.xlsx"
        logger.info(f"Creating upload file: {upload_path}")
        
        # Create upload version DataFrame (without image data)
        df_upload = df_finalized.copy()
        for col in IMAGE_COLUMNS:
            if col in df_upload.columns:
                df_upload[col] = '-'
        
        # Create new workbook for upload file
        workbook_upload = openpyxl.Workbook()
        worksheet_upload = workbook_upload.active
        worksheet_upload.title = "제품 가격 비교"
        
        # Write data using the new helper function
        if not _write_data_to_worksheet(worksheet_upload, df_upload):
            logger.error("Failed to write data to upload worksheet")
            return result_success, False, result_path, None
            
        # Apply basic formatting
        _apply_basic_excel_formatting(worksheet_upload, df_upload.columns.tolist())
        _add_hyperlinks_to_worksheet(worksheet_upload, df_upload, hyperlinks_as_formulas=True)
        _add_header_footer(worksheet_upload)
        
        # Save upload file
        workbook_upload.save(upload_path)
        upload_success = True
        
        return result_success, upload_success, result_path, upload_path
        
    except PermissionError as pe:
        logger.error(f"Permission denied writing Excel file. Is it open? Error: {pe}")
        return result_success, upload_success, result_path, upload_path
    except Exception as e:
        logger.error(f"Error creating Excel files: {e}")
        return result_success, upload_success, result_path, upload_path

@safe_excel_operation
def create_final_output_excel(df: pd.DataFrame, output_path: str) -> bool:
    """
    Create a combined Excel output file with images and various formatting.
    Unlike create_split_excel_outputs, this creates a single Excel file with advanced formatting.
    
    Args:
        df: DataFrame with the data
        output_path: Path where to save the Excel file
        
    Returns:
        bool: True if successful, False otherwise
    """
    if df is None:
        logger.error("Cannot create Excel file: Input DataFrame is None.")
        return False

    logger.info(f"Starting creation of single final Excel output: {output_path}")
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    # Prepare the DataFrame (rename columns, order, clean)
    from excel_data_processing import finalize_dataframe_for_excel
    df_finalized = finalize_dataframe_for_excel(df)
    
    if df_finalized.empty and not df.empty:
        logger.error("DataFrame became empty after finalization step. Cannot save Excel.")
        return False
    
    # Save finalized data to Excel using openpyxl engine
    try:
        logger.info(f"Writing final Excel: {output_path} with {len(df_finalized)} rows.")
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df_finalized.to_excel(writer, index=False, sheet_name='Results', na_rep='')
            worksheet = writer.sheets['Results']
            
            # Apply Full Formatting
            from excel_formatting import (
                _apply_column_widths,
                _apply_cell_styles_and_alignment,
                _apply_conditional_formatting,
                _setup_page_layout,
                _add_header_footer,
                _add_hyperlinks_to_worksheet
            )
            
            _apply_column_widths(worksheet, df_finalized)
            _apply_cell_styles_and_alignment(worksheet, df_finalized)
            if not df_finalized.empty:
                _process_image_columns(worksheet, df_finalized)
                _adjust_image_cell_dimensions(worksheet, df_finalized)
            _add_hyperlinks_to_worksheet(worksheet, df_finalized)
            _apply_conditional_formatting(worksheet, df_finalized)
            _setup_page_layout(worksheet)
            _add_header_footer(worksheet)
        
        logger.info(f"Successfully created and formatted final Excel file: {output_path}")
        return True
        
    except PermissionError as pe:
        logger.error(f"Permission denied writing Excel file '{output_path}'. Is it open? Error: {pe}")
        return False
    except Exception as e:
        logger.error(f"Error creating Excel file '{output_path}': {e}")
        return False

def _write_data_to_worksheet(worksheet, df_for_excel):
    """Write data to worksheet with proper handling of complex data types"""
    try:
        # Write header
        for col_idx, col_name in enumerate(df_for_excel.columns, 1):
            worksheet.cell(row=1, column=col_idx, value=col_name)
        
        # Write data with proper type conversion
        for row_idx, row in enumerate(df_for_excel.itertuples(), 2):
            for col_idx, value in enumerate(row[1:], 1):  # Skip the index
                if pd.isna(value):
                    cell_value = ""
                elif isinstance(value, dict):
                    # Extract URL from dictionary structure
                    if 'url' in value:
                        if isinstance(value['url'], dict) and 'url' in value['url']:
                            cell_value = value['url']['url']
                        else:
                            cell_value = value['url']
                    else:
                        cell_value = str(value)
                elif isinstance(value, pd.Series):
                    # Handle Series objects
                    cell_value = "-"
                    for item in value:
                        if pd.notna(item) and item not in ['-', '']:
                            if isinstance(item, dict) and 'url' in item:
                                if isinstance(item['url'], dict) and 'url' in item['url']:
                                    cell_value = item['url']['url']
                                else:
                                    cell_value = item['url']
                            else:
                                cell_value = str(item)
                            break
                else:
                    cell_value = value
                
                worksheet.cell(row=row_idx, column=col_idx, value=cell_value)
        
        return True
    except Exception as e:
        logger.error(f"Error writing data to worksheet: {e}")
        return False

def flatten_nested_image_dicts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten nested image dictionaries in DataFrame to simple URL strings.
    This makes the data suitable for Excel output.
    """
    if df is None or df.empty:
        return df

    df = df.copy()
    
    # Define image-related columns
    image_cols = [col for col in df.columns if any(img_type in col.lower() for img_type in ['이미지', 'image'])]
    
    for col in image_cols:
        for idx in df.index:
            value = df.at[idx, col]
            
            # Skip if value is already a string or None
            if isinstance(value, str) or pd.isna(value):
                continue
                
            try:
                # Handle dictionary values
                if isinstance(value, dict):
                    # Try to extract URL in order of preference
                    url = None
                    
                    # First check for nested URL structure
                    if 'url' in value and isinstance(value['url'], dict) and 'url' in value['url']:
                        url = value['url']['url']
                    # Then check for direct URL
                    elif 'url' in value and isinstance(value['url'], str):
                        url = value['url']
                    # Check for product URL (common in Naver data)
                    elif 'product_url' in value and isinstance(value['product_url'], str):
                        url = value['product_url']
                    # Finally check other possible URL fields
                    else:
                        for key in ['image_url', 'original_url', 'src', 'link']:
                            if key in value and isinstance(value[key], str):
                                url = value[key]
                                break
                    
                    # Set the flattened value
                    df.at[idx, col] = url if url else '-'
                
                # Handle Series or other iterable types
                elif isinstance(value, (pd.Series, list, tuple)):
                    # Convert to list if it's a Series
                    items = value.tolist() if isinstance(value, pd.Series) else value
                    
                    # Try to find a valid URL in the items
                    url = None
                    for item in items:
                        if isinstance(item, dict):
                            # Apply the same URL extraction logic as above
                            if 'url' in item and isinstance(item['url'], dict) and 'url' in item['url']:
                                url = item['url']['url']
                                break
                            elif 'url' in item and isinstance(item['url'], str):
                                url = item['url']
                                break
                            elif 'product_url' in item and isinstance(item['product_url'], str):
                                url = item['product_url']
                                break
                            else:
                                for key in ['image_url', 'original_url', 'src', 'link']:
                                    if key in item and isinstance(item[key], str):
                                        url = item[key]
                                        break
                                if url:
                                    break
                        elif isinstance(item, str) and item.startswith(('http://', 'https://')):
                            url = item
                            break
                    
                    # Set the flattened value
                    df.at[idx, col] = url if url else '-'
                
                else:
                    # For any other type, convert to string
                    df.at[idx, col] = str(value)
                    
            except Exception as e:
                logger.warning(f"Error flattening image data in column {col}, row {idx}: {e}")
                df.at[idx, col] = '-'
    
    return df 