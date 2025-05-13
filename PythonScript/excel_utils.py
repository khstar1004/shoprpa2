import os
import logging
import pandas as pd
import functools
import json
from typing import Optional, Tuple, List, Dict, Any, Union
from pathlib import Path
import openpyxl
from datetime import datetime
import traceback
import psutil

# Import from other modules
from excel_constants import (
    FINAL_COLUMN_ORDER,
    UPLOAD_COLUMN_ORDER, IMAGE_COLUMNS,
    REQUIRED_INPUT_COLUMNS,
    UPLOAD_COLUMN_MAPPING
)
from excel_formatting import (
    _apply_basic_excel_formatting,
    _apply_upload_file_formatting,
    _add_hyperlinks_to_worksheet,
    _add_header_footer
)
from excel_image_utils import (
    _process_image_columns,
    _adjust_image_cell_dimensions
)
from excel_file_utils import (
    generate_file_path,
    get_source_info
)

# Initialize logger
logger = logging.getLogger(__name__)

# Import constants only
from excel_constants import (
    FINAL_COLUMN_ORDER, COLUMN_RENAME_MAP, PRICE_COLUMNS, 
    QUANTITY_COLUMNS, PERCENTAGE_COLUMNS, IMAGE_COLUMNS,
    UPLOAD_COLUMN_ORDER,
    ERROR_MESSAGES, ERROR_MESSAGE_VALUES, REQUIRED_INPUT_COLUMNS,
    UPLOAD_COLUMN_MAPPING
)

# Import base classes
from excel_formatting import ExcelFormatter
from excel_image_utils import ImageProcessor
from excel_data_processing import finalize_dataframe_for_excel, find_excel_file

def safe_excel_operation(func):
    """
    Decorator: Safely handles exceptions in Excel operations.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Excel operation failed in {func.__name__}: {str(e)}", exc_info=True)
            return False
    return wrapper

class ExcelGenerator:
    """Main class for Excel file generation with improved error handling and memory management"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ExcelGenerator, cls).__new__(cls)
        return cls._instance
        
    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True
            self.logger = logging.getLogger(__name__)
            # Initialize formatter and image processor
            self.formatter = ExcelFormatter()
            self.image_processor = ImageProcessor()
            
    @safe_excel_operation
    def create_excel_output(self, 
                          df: pd.DataFrame, 
                          output_path: str, 
                          create_upload_file: bool = True) -> Tuple[bool, bool, Optional[str], Optional[str]]:
        """
        Create Excel output file(s) with improved error handling and memory management.
        
        Args:
            df: Input DataFrame
            output_path: Base path for output files
            create_upload_file: Whether to create upload version
            
        Returns:
            Tuple of (result_success, upload_success, result_path, upload_path)
        """
        try:
            # Input validation
            if df is None or df.empty:
                self.logger.error("Cannot create Excel output: Input DataFrame is empty")
                return False, False, None, None
                
            # Verify memory availability
            available_memory = psutil.virtual_memory().available
            estimated_memory = len(df) * len(df.columns) * 100  # Rough estimate
            
            if estimated_memory > available_memory * 0.5:  # Use max 50% of available memory
                self.logger.error(f"Insufficient memory. Need ~{estimated_memory/1024/1024:.1f}MB, "
                               f"have {available_memory/1024/1024:.1f}MB available")
                return False, False, None, None
                
            # Create output directory if needed
            output_dir = os.path.dirname(output_path)
            if output_dir:
                try:
                    os.makedirs(output_dir, exist_ok=True)
                except Exception as e:
                    self.logger.error(f"Failed to create output directory: {e}")
                    return False, False, None, None
                    
            # Check write permissions
            if not os.access(output_dir, os.W_OK):
                self.logger.error(f"No write permission for output directory: {output_dir}")
                return False, False, None, None
                
            # Process in chunks if necessary
            chunk_size = 1000
            if len(df) > chunk_size:
                self.logger.info(f"Processing large DataFrame in chunks of {chunk_size} rows")
                result_success = False
                upload_success = False
                result_path = None
                upload_path = None
                
                for i in range(0, len(df), chunk_size):
                    chunk = df.iloc[i:i+chunk_size].copy()
                    
                    # Process chunk
                    if i == 0:  # First chunk
                        result_success, upload_success, result_path, upload_path = self._create_split_outputs(
                            chunk, output_path
                        )
                    else:  # Append to existing files
                        self._append_to_existing_files(chunk, result_path, upload_path)
                        
                return result_success, upload_success, result_path, upload_path
            else:
                # Process entire DataFrame at once
                return self._create_split_outputs(df, output_path)
                
        except Exception as e:
            self.logger.error(f"Error in create_excel_output: {e}")
            self.logger.debug(traceback.format_exc())
            return False, False, None, None
            
    def _append_to_existing_files(self, df: pd.DataFrame, result_path: str, upload_path: str):
        """Append data to existing Excel files."""
        try:
            if result_path and os.path.exists(result_path):
                with pd.ExcelWriter(result_path, mode='a', engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, startrow=writer.sheets['Sheet1'].max_row, header=False)
                    
            if upload_path and os.path.exists(upload_path):
                with pd.ExcelWriter(upload_path, mode='a', engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, startrow=writer.sheets['Sheet1'].max_row, header=False)
                    
        except Exception as e:
            self.logger.error(f"Error appending to Excel files: {e}")
            self.logger.debug(traceback.format_exc())
            
    def _create_split_outputs(self, 
                            df: pd.DataFrame, 
                            output_path: str) -> Tuple[bool, bool, str, str]:
        """Create separate result and upload Excel files."""
        try:
            # Create result file
            result_success, result_path = self._create_result_file(df, output_path)
            
            # Create upload file if requested
            upload_success = False
            upload_path = None
            
            if result_success:
                upload_success, upload_path = self._create_upload_file(df, output_path)
                
            return result_success, upload_success, result_path, upload_path
            
        except Exception as e:
            self.logger.error(f"Error creating split outputs: {e}")
            self.logger.debug(traceback.format_exc())
            return False, False, None, None
            
    @staticmethod
    def _generate_file_path(base_path: str, file_type: str, source_info: str, row_count: int, mgmt_type: str) -> str:
        """Generate appropriate file path based on type."""
        dir_path = os.path.dirname(base_path)
        date_part = datetime.now().strftime("%Y%m%d")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Format: {company}({count})-{mgmt_type}-{date}_{type}_{timestamp}.xlsx
        if file_type == 'result':
            filename = f"{source_info}({row_count}개)-{mgmt_type}-{date_part}_result_{timestamp}.xlsx"
        elif file_type == 'upload':
            filename = f"{source_info}({row_count}개)-{mgmt_type}-{date_part}_upload_{timestamp}.xlsx"
        else:
            filename = os.path.basename(base_path)
        
        return os.path.join(dir_path, filename)
    
    def _create_result_file(self, 
                          df: pd.DataFrame, 
                          output_path: str) -> Tuple[bool, str]:
        """Create result file with images"""
        try:
            # Generate result file path
            result_path = self._generate_file_path(output_path, "result", "", 0, "")
            os.makedirs(os.path.dirname(result_path), exist_ok=True)
            
            # Create and format workbook
            workbook = openpyxl.Workbook()
            worksheet = workbook.active
            worksheet.title = "제품 가격 비교"
            
            # Write data
            self._write_data_to_worksheet(worksheet, df)
            
            # Apply formatting
            self.formatter.format_result_file(worksheet, df)
            
            # Process images
            if hasattr(self.image_processor, 'process_image_columns'):
                self.image_processor.process_image_columns(worksheet, df)
            
            # Save file
            workbook.save(result_path)
            logger.info(f"Successfully created result file: {result_path}")
            
            return True, result_path
            
        except Exception as e:
            logger.error(f"Error creating result file: {e}")
            return False, ""
    
    def _create_upload_file(self, 
                          df: pd.DataFrame, 
                          output_path: str) -> Tuple[bool, str]:
        """Create upload file with links"""
        try:
            # Generate upload file path
            upload_path = self._generate_file_path(output_path, "upload", "", 0, "")
            os.makedirs(os.path.dirname(upload_path), exist_ok=True)
            
            # Prepare data for upload file
            from excel_data_processing import prepare_naver_image_urls_for_upload
            df_upload = prepare_naver_image_urls_for_upload(df)
            
            # Create and format workbook
            workbook = openpyxl.Workbook()
            worksheet = workbook.active
            worksheet.title = "제품 가격 비교 (업로드용)"
            
            # Write data
            self._write_data_to_worksheet(worksheet, df_upload)
            
            # Apply formatting
            self.formatter.format_upload_file(worksheet, df_upload)
            
            # Save file
            workbook.save(upload_path)
            logger.info(f"Successfully created upload file: {upload_path}")
            
            return True, upload_path
            
        except Exception as e:
            logger.error(f"Error creating upload file: {e}")
            return False, ""
    
    @staticmethod
    def _extract_url_from_complex_value(value):
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
    
    @staticmethod
    def _write_data_to_worksheet(worksheet: openpyxl.worksheet.worksheet.Worksheet, 
                               df: pd.DataFrame) -> None:
        """Write DataFrame to worksheet with proper handling of complex data types"""
        try:
            # Write header
            for col_idx, col_name in enumerate(df.columns, 1):
                worksheet.cell(row=1, column=col_idx, value=col_name)
            
            # Write data
            for row_idx, row in enumerate(df.itertuples(), 2):
                for col_idx, value in enumerate(row[1:], 1):
                    try:
                        # Extract clean value for Excel, handling complex types
                        cell_value = ExcelGenerator._extract_url_from_complex_value(value)
                        worksheet.cell(row=row_idx, column=col_idx, value=cell_value)
                    except Exception as cell_err:
                        logger.error(f"Error processing cell value at row {row_idx}, col {col_idx}: {cell_err}")
                        # Use empty string as fallback for problematic cells
                        worksheet.cell(row=row_idx, column=col_idx, value="")
        except Exception as e:
            logger.error(f"Error writing data to worksheet: {e}")
            raise

def sanitize_dataframe_for_excel(df):
    """
    Convert all complex data types in a DataFrame to simple types that Excel can handle.
    
    Args:
        df: DataFrame to sanitize
        
    Returns:
        A new DataFrame with all complex types converted to strings
    """
    if df is None or df.empty:
        return df
        
    # Create a copy to avoid modifying the original
    result_df = df.copy()
    
    # Extract URL function (same logic as in ExcelGenerator._extract_url_from_complex_value)
    def extract_url(value):
        if pd.isna(value) or value is None:
            return ""
        if isinstance(value, str):
            return value
        if isinstance(value, (int, float)):
            return value
        if isinstance(value, dict):
            try:
                if 'url' in value and isinstance(value['url'], dict) and 'url' in value['url']:
                    return value['url']['url']
                elif 'url' in value and isinstance(value['url'], str):
                    return value['url']
                elif 'local_path' in value and value['local_path']:
                    return value['local_path']
                elif 'product_name' in value:
                    return f"Product: {value['product_name']}"
                return json.dumps(value, ensure_ascii=False)
            except:
                return str(value)
        if isinstance(value, (list, tuple)):
            try:
                return json.dumps(value, ensure_ascii=False)
            except:
                return str(value)
        return str(value)
    
    # Process each column
    for col in result_df.columns:
        # Check if the column contains complex types
        if result_df[col].dtype == 'object':
            result_df[col] = result_df[col].apply(extract_url)
    
    return result_df

# Create singleton instance
excel_generator = ExcelGenerator()

# Export public interface
__all__ = ['excel_generator', 'find_excel_file', 'finalize_dataframe_for_excel', 'IMAGE_COLUMNS', 'REQUIRED_INPUT_COLUMNS', 'FINAL_COLUMN_ORDER']

def generate_timestamped_filenames(source_info: str, row_count: int, mgmt_type: str) -> Tuple[str, str]:
    """
    Generate timestamped filenames for result and upload files.
    
    Args:
        source_info: Source information for the file
        row_count: Number of rows in the data
        mgmt_type: Management type identifier
        
    Returns:
        Tuple[str, str]: (result_filename, upload_filename)
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    date_part = datetime.now().strftime("%Y%m%d")
    
    # Format: {company}({count})-{mgmt_type}-{date}_{type}_{timestamp}.xlsx
    result_filename = f"{source_info}({row_count}개)-{mgmt_type}-{date_part}_result_{timestamp}.xlsx"
    upload_filename = f"{source_info}({row_count}개)-{mgmt_type}-{date_part}_upload_{timestamp}.xlsx"
    
    return result_filename, upload_filename

def create_split_excel_outputs(df_finalized: pd.DataFrame, output_path_base: str) -> tuple:
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
    if df_finalized is None or df_finalized.empty:
        logger.error("No data to write to Excel. DataFrame is empty or None.")
        return False, False, None, None

    # Get source info for file naming
    source_info, mgmt_type, row_count = get_source_info(df_finalized)

    # Create result file
    result_success = False
    result_path = None
    try:
        # Generate result file path
        result_path = generate_file_path(output_path_base, "result", source_info, row_count, mgmt_type)
        os.makedirs(os.path.dirname(result_path), exist_ok=True)

        # Create workbook
        workbook = openpyxl.Workbook()
        worksheet = workbook.active
        worksheet.title = "제품 가격 비교"

        # Write data
        for col_idx, col_name in enumerate(df_finalized.columns, 1):
            worksheet.cell(row=1, column=col_idx, value=col_name)

        for row_idx, row in enumerate(df_finalized.itertuples(), 2):
            for col_idx, value in enumerate(row[1:], 1):
                worksheet.cell(row=row_idx, column=col_idx, value=value if pd.notna(value) else "")

        # Apply formatting
        _apply_basic_excel_formatting(worksheet, df_finalized.columns.tolist())
        _add_hyperlinks_to_worksheet(worksheet, df_finalized)
        _add_header_footer(worksheet)

        # Process images
        _process_image_columns(worksheet, df_finalized)
        _adjust_image_cell_dimensions(worksheet, df_finalized)

        # Save file
        workbook.save(result_path)
        result_success = True
        logger.info(f"Successfully created result file: {result_path}")

    except Exception as e:
        logger.error(f"Error creating result file: {e}")
        logger.debug(traceback.format_exc())

    # Create upload file
    upload_success = False
    upload_path = None
    try:
        # Generate upload file path
        upload_path = generate_file_path(output_path_base, "upload", source_info, row_count, mgmt_type)
        os.makedirs(os.path.dirname(upload_path), exist_ok=True)

        # Prepare data for upload file
        df_upload = df_finalized.copy()
        
        # Map columns to upload format
        df_upload = df_upload.rename(columns=UPLOAD_COLUMN_MAPPING)
        
        # Reorder columns
        df_upload = df_upload[UPLOAD_COLUMN_ORDER]

        # Create workbook
        workbook = openpyxl.Workbook()
        worksheet = workbook.active
        worksheet.title = "제품 가격 비교"

        # Write data
        for col_idx, col_name in enumerate(df_upload.columns, 1):
            worksheet.cell(row=1, column=col_idx, value=col_name)

        for row_idx, row in enumerate(df_upload.itertuples(), 2):
            for col_idx, value in enumerate(row[1:], 1):
                worksheet.cell(row=row_idx, column=col_idx, value=value if pd.notna(value) else "")

        # Apply upload-specific formatting
        _apply_upload_file_formatting(worksheet, df_upload.columns.tolist())

        # Save file
        workbook.save(upload_path)
        upload_success = True
        logger.info(f"Successfully created upload file: {upload_path}")

    except Exception as e:
        logger.error(f"Error creating upload file: {e}")
        logger.debug(traceback.format_exc())

    return result_success, upload_success, result_path, upload_path 