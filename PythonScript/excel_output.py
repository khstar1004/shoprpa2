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
        # Validate the DataFrame
        if df_finalized is None or df_finalized.empty:
            logger.error("Input DataFrame is None or empty. Cannot create Excel files.")
            return False, False, None, None
        
        # Ensure columns are properly ordered
        if not all(col in FINAL_COLUMN_ORDER for col in df_finalized.columns):
            logger.warning("Input DataFrame columns are not in the exact FINAL_COLUMN_ORDER. Reordering.")
            # Recreate with only the expected columns in the correct order
            ordered_df = pd.DataFrame()
            for col in FINAL_COLUMN_ORDER:
                if col in df_finalized.columns:
                    ordered_df[col] = df_finalized[col]
            df_finalized = ordered_df
        
        # Get file source info for naming
        source_info = "Unknown"
        mgmt_type = "승인관리"  # Default type
        row_count = len(df_finalized)
        
        try:
            # Check the appropriate column for management type
            if '구분' in df_finalized.columns:
                source_val = df_finalized['구분'].iloc[0]
                if source_val == 'A':
                    mgmt_type = "승인관리"
                elif source_val == 'P':
                    mgmt_type = "가격관리"
                else:
                    mgmt_type = str(source_val)
                    
            # Get company name for filename
            if '업체명' in df_finalized.columns:
                company_counts = df_finalized['업체명'].value_counts()
                if not company_counts.empty:
                    source_info = company_counts.index[0]
        except Exception as e:
            logger.warning(f"Error getting source name: {e}")
            source_info = "Mixed"
        
        # Create timestamped filenames
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        date_part = datetime.now().strftime("%Y%m%d")
        
        # Format: {company}({count})-{mgmt_type}-{date}_{type}_{timestamp}.xlsx
        result_filename = f"{source_info}({row_count}개)-{mgmt_type}-{date_part}_result_{timestamp}.xlsx"
        upload_filename = f"{source_info}({row_count}개)-{mgmt_type}-{date_part}_upload_{timestamp}.xlsx"
        
        # Make sure output_path_base is a directory, not a file
        if output_path_base.lower().endswith('.xlsx'):
            output_path_base = os.path.dirname(output_path_base)
            
        # Ensure the output directory exists
        os.makedirs(output_path_base, exist_ok=True)
        
        # Build full paths
        result_path = os.path.join(output_path_base, result_filename)
        upload_path = os.path.join(output_path_base, upload_filename)
        
        logger.info(f"Result file path (with images): {result_path}")
        logger.info(f"Upload file path (links only): {upload_path}")
        
        # -----------------------------------------
        # 1. Create Result File (with images)
        # -----------------------------------------
        try:
            logger.info(f"Creating result file: {result_path} with {len(df_finalized)} rows.")
            
            # Create a new workbook for result file
            workbook_result = openpyxl.Workbook()
            worksheet_result = workbook_result.active
            worksheet_result.title = "제품 가격 비교"
            
            # Convert image dictionaries to strings for initial data writing
            df_for_excel = df_finalized.copy()
            
            # Convert any dictionary or complex objects to strings
            for col in df_for_excel.columns:
                for idx in df_for_excel.index:
                    value = df_for_excel.loc[idx, col]
                    if isinstance(value, dict):
                        # For dictionary values, store just the URL to make Excel happy
                        if 'url' in value:
                            # Handle case where url itself is a dictionary (nested dict)
                            if isinstance(value['url'], dict) and 'url' in value['url']:
                                df_for_excel.at[idx, col] = value['url']['url']
                            else:
                                df_for_excel.at[idx, col] = value['url']
                        else:
                            # Just convert to string representation if no URL
                            df_for_excel.at[idx, col] = str(value)
                    elif isinstance(value, pd.Series):
                        # For Series objects, convert to string
                        for item in value:
                            if pd.notna(item) and item not in ['-', '']:
                                if isinstance(item, dict) and 'url' in item:
                                    if isinstance(item['url'], dict) and 'url' in item['url']:
                                        df_for_excel.at[idx, col] = item['url']['url']
                                    else:
                                        df_for_excel.at[idx, col] = item['url']
                                else:
                                    df_for_excel.at[idx, col] = str(item)
                                break
                        else:
                            df_for_excel.at[idx, col] = "-"
            
            # Write header
            for col_idx, col_name in enumerate(df_for_excel.columns, 1):
                worksheet_result.cell(row=1, column=col_idx, value=col_name)
            
            # Write data
            for row_idx, row in enumerate(df_for_excel.itertuples(), 2):
                for col_idx, value in enumerate(row[1:], 1):  # Skip the index
                    # Convert None to empty string to avoid writing 'None' to cells
                    worksheet_result.cell(row=row_idx, column=col_idx, value=value if not pd.isna(value) else "")
            
            # Apply common formatting
            _apply_basic_excel_formatting(worksheet_result, df_for_excel.columns.tolist())
            _add_hyperlinks_to_worksheet(worksheet_result, df_for_excel, hyperlinks_as_formulas=False)
            _add_header_footer(worksheet_result)
            
            # Remove auto filter
            if hasattr(worksheet_result, 'auto_filter') and worksheet_result.auto_filter:
                worksheet_result.auto_filter.ref = None
                logger.info("Removed filter from result Excel file")
            
            # Save without images first
            workbook_result.save(result_path)
            
            # Now load the saved file and add images
            try:
                logger.info("Adding images to result file...")
                # Load the workbook
                workbook_with_images = openpyxl.load_workbook(result_path)
                worksheet_with_images = workbook_with_images.active
                
                # Process image columns
                _process_image_columns(worksheet_with_images, df_finalized)
                
                # Remove filter
                if hasattr(worksheet_with_images, 'auto_filter') and worksheet_with_images.auto_filter:
                    worksheet_with_images.auto_filter.ref = None
                
                # Save the workbook with images
                workbook_with_images.save(result_path)
                logger.info(f"Successfully added images to result file")
            except Exception as img_err:
                logger.error(f"Error adding images to result file: {img_err}")
                # Continue with the file without images
            
            result_success = True
            logger.info(f"Successfully created result file: {result_path}")
            
        except Exception as e:
            logger.error(f"Error creating result file: {e}")
            logger.debug(traceback.format_exc())
            result_success = False
        
        # -----------------------------------------
        # 2. Create Upload File (links only)
        # -----------------------------------------
        try:
            logger.info(f"Preparing data for upload file: {upload_path}")
            
            # Create a deep copy of the original DataFrame to avoid modifying it
            df_with_image_urls = df_finalized.copy()
            
            # Process image columns to extract web URLs
            for img_col in IMAGE_COLUMNS:
                if img_col in df_finalized.columns:
                    logger.info(f"Extracting image URLs from {img_col} column...")
                    
                    # Map the column names to the upload file column names
                    upload_img_col = COLUMN_MAPPING_FINAL_TO_UPLOAD.get(img_col, img_col)

                    # Create the target upload column if it doesn't exist
                    if upload_img_col not in df_with_image_urls.columns:
                        df_with_image_urls[upload_img_col] = ""
                    
                    # Process all rows to extract image URLs
                    for idx in df_finalized.index:
                        value = df_finalized.at[idx, img_col]
                        
                        # Default to empty string
                        image_url = ""
                        
                        try:
                            # Extract URL from dictionary structure
                            if isinstance(value, dict):
                                # Check for product_url first (for Naver)
                                if 'product_url' in value and isinstance(value['product_url'], str) and value['product_url'].startswith(('http://', 'https://')):
                                    image_url = value['product_url'].strip()
                                # Then check for regular 'url' key
                                elif 'url' in value and isinstance(value['url'], str) and value['url'].startswith(('http://', 'https://')):
                                    image_url = value['url'].strip()
                                else:
                                    # Fallback: Check other potential keys
                                    for url_key in ['image_url', 'original_url', 'src']:
                                        fallback_url = value.get(url_key)
                                        if fallback_url and isinstance(fallback_url, str) and fallback_url.startswith(('http://', 'https://')):
                                            image_url = fallback_url.strip()
                                            break
                            
                            # Handle string URL format
                            elif isinstance(value, str) and value.strip() and value != '-':
                                url = value.strip()
                                if url.startswith(('http://', 'https://')):
                                    image_url = url
                            
                        except Exception as e:
                            logger.error(f"Error extracting image URL: {e}")
                            image_url = ""
                        
                        # Store the extracted image URL
                        df_with_image_urls.at[idx, upload_img_col] = image_url if image_url else ""

            # Special handling for Naver image column - replace with product links
            df_with_image_urls = prepare_naver_image_urls_for_upload(df_with_image_urls)

            # Map columns from result format to upload format 
            df_upload = pd.DataFrame()
            
            for target_col in UPLOAD_COLUMN_ORDER:
                # Find corresponding source column
                source_col = None
                for result_col, upload_col in COLUMN_MAPPING_FINAL_TO_UPLOAD.items():
                    if upload_col == target_col:
                        source_col = result_col
                        break
                
                if target_col in ['해오름(이미지링크)', '고려기프트(이미지링크)', '네이버쇼핑(이미지링크)']:
                    # Get the already processed image URL
                    if target_col in df_with_image_urls.columns:
                        df_upload[target_col] = df_with_image_urls[target_col]
                    else:
                        df_upload[target_col] = ''
                elif source_col and source_col in df_finalized.columns:
                    # Get non-image data from the original finalized DataFrame
                    df_upload[target_col] = df_finalized[source_col]
                else:
                    # If no matching column found, add an empty column
                    df_upload[target_col] = ''

            # Create new workbook for upload file
            workbook_upload = openpyxl.Workbook()
            worksheet_upload = workbook_upload.active
            worksheet_upload.title = "제품 가격 비교 (업로드용)"
            
            logger.info(f"Writing upload file: {upload_path} with {len(df_upload)} rows.")
            
            # Write header
            for col_idx, col_name in enumerate(df_upload.columns, 1):
                worksheet_upload.cell(row=1, column=col_idx, value=col_name)
            
            # Write data
            for row_idx, row in enumerate(df_upload.itertuples(), 2):
                for col_idx, value in enumerate(row[1:], 1):  # Skip the index
                    cell_value = "" if pd.isna(value) or value is None else value
                    worksheet_upload.cell(row=row_idx, column=col_idx, value=cell_value)
            
            # Apply upload file specific formatting
            _apply_upload_file_formatting(worksheet_upload, df_upload.columns.tolist())
            
            # Add hyperlinks to all image URL cells
            try:
                logger.info("Adding hyperlinks to image URLs in upload file...")
                
                # Define upload image columns
                upload_image_cols = ['해오름(이미지링크)', '고려기프트(이미지링크)', '네이버쇼핑(이미지링크)']
                
                # Get the column indices for these columns
                col_indices = {}
                for i, col_name in enumerate(df_upload.columns, 1):
                    if col_name in upload_image_cols:
                        col_indices[col_name] = i
                
                # Add hyperlinks to the cells
                for row_idx in range(2, len(df_upload) + 2):  # Start from row 2 (after header)
                    for col_name, col_idx in col_indices.items():
                        cell = worksheet_upload.cell(row=row_idx, column=col_idx)
                        url = cell.value
                        
                        # Only add hyperlink if the cell contains a valid URL
                        if isinstance(url, str) and url.strip() and url.startswith(('http://', 'https://')):
                            cell.hyperlink = url
                            cell.font = openpyxl.styles.Font(color="0563C1", underline="single")
                
                logger.info("Hyperlinks added to upload file successfully")
            except Exception as e:
                logger.warning(f"Error adding hyperlinks to upload file: {e}")
            
            # Save upload file
            workbook_upload.save(upload_path)
            upload_success = True
            logger.info(f"Successfully created upload file: {upload_path}")
                
        except Exception as upload_err:
            logger.error(f"Error creating upload file: {upload_err}")
            logger.debug(traceback.format_exc())
            upload_success = False
        
        return result_success, upload_success, result_path, upload_path
        
    except Exception as main_error:
        logger.error(f"Unexpected error in create_split_excel_outputs: {main_error}")
        logger.debug(traceback.format_exc())
        return False, False, None, None

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