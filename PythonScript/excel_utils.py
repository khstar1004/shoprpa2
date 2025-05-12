import os
import logging
import pandas as pd
import functools
from typing import Optional, Tuple, List, Dict, Any, Union
from pathlib import Path
import openpyxl
from datetime import datetime

# Set up logger
logger = logging.getLogger(__name__)

# Import constants only
from excel_constants import (
    FINAL_COLUMN_ORDER, COLUMN_RENAME_MAP, PRICE_COLUMNS, 
    QUANTITY_COLUMNS, PERCENTAGE_COLUMNS, IMAGE_COLUMNS,
    UPLOAD_COLUMN_ORDER, COLUMN_MAPPING_FINAL_TO_UPLOAD,
    ERROR_MESSAGES, ERROR_MESSAGE_VALUES
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
    """Main class for Excel file generation"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ExcelGenerator, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.formatter = ExcelFormatter()
            self.image_processor = ImageProcessor()
            self._initialized = True
    
    @safe_excel_operation
    def create_excel_output(self, 
                          df: pd.DataFrame, 
                          output_path: str, 
                          create_upload_file: bool = True) -> Tuple[bool, bool, Optional[str], Optional[str]]:
        """
        Create Excel output file(s)
        
        Args:
            df: Input DataFrame
            output_path: Output file path
            create_upload_file: Whether to create upload file
            
        Returns:
            Tuple of (result_success, upload_success, result_path, upload_path)
        """
        try:
            if df is None or df.empty:
                logger.error("Cannot create Excel file: Input DataFrame is empty or None.")
                return (False, False, None, None)
            
            # Finalize the DataFrame
            df_finalized = finalize_dataframe_for_excel(df)
            
            if create_upload_file:
                return self._create_split_outputs(df_finalized, output_path)
            else:
                result_success, result_path = self._create_single_output(df_finalized, output_path)
                return (result_success, False, result_path, None)
        except Exception as e:
            logger.error(f"Error in create_excel_output: {e}")
            return (False, False, None, None)
    
    def _create_split_outputs(self, 
                            df: pd.DataFrame, 
                            output_path: str) -> Tuple[bool, bool, str, str]:
        """Create both result and upload files"""
        try:
            result_success, result_path = self._create_result_file(df, output_path)
            if not result_success:
                return False, False, None, None
                
            upload_success, upload_path = self._create_upload_file(df, output_path)
            return result_success, upload_success, result_path, upload_path
        except Exception as e:
            logger.error(f"Error in _create_split_outputs: {e}")
            return False, False, None, None
    
    def _create_single_output(self, 
                            df: pd.DataFrame, 
                            output_path: str) -> Tuple[bool, str]:
        """Create single output file with images"""
        result_success, result_path = self._create_result_file(df, output_path)
        return result_success, result_path
    
    def _create_result_file(self, 
                          df: pd.DataFrame, 
                          output_path: str) -> Tuple[bool, str]:
        """Create result file with images"""
        try:
            # Generate result file path
            result_path = self._generate_file_path(output_path, "result")
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
            upload_path = self._generate_file_path(output_path, "upload")
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
    def _generate_file_path(base_path: str, file_type: str) -> str:
        """Generate appropriate file path"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if base_path.lower().endswith('.xlsx'):
                base_dir = os.path.dirname(base_path)
                base_name = os.path.splitext(os.path.basename(base_path))[0]
            else:
                base_dir = base_path
                base_name = "excel_output"
                
            return os.path.join(base_dir, f"{base_name}_{file_type}_{timestamp}.xlsx")
        except Exception as e:
            logger.error(f"Error generating file path: {e}")
            return f"excel_output_{file_type}_{timestamp}.xlsx"
    
    @staticmethod
    def _write_data_to_worksheet(worksheet: openpyxl.worksheet.worksheet.Worksheet, 
                               df: pd.DataFrame) -> None:
        """Write DataFrame to worksheet"""
        try:
            # Write header
            for col_idx, col_name in enumerate(df.columns, 1):
                worksheet.cell(row=1, column=col_idx, value=col_name)
            
            # Write data
            for row_idx, row in enumerate(df.itertuples(), 2):
                for col_idx, value in enumerate(row[1:], 1):
                    cell_value = "" if pd.isna(value) else value
                    worksheet.cell(row=row_idx, column=col_idx, value=cell_value)
        except Exception as e:
            logger.error(f"Error writing data to worksheet: {e}")
            raise

# Create singleton instance
excel_generator = ExcelGenerator()

# Export public interface
__all__ = ['excel_generator', 'find_excel_file', 'finalize_dataframe_for_excel', 'IMAGE_COLUMNS', 'REQUIRED_INPUT_COLUMNS'] 