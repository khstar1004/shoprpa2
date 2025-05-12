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
    _prepare_data_for_excel, finalize_dataframe_for_excel
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
    작업메뉴얼에 따라 두 가지 Excel 파일을 생성합니다:
    1. Result file (A): 이미지 포함, 조회용 (원본 컬럼 이름 유지)
    2. Upload file (P): URL 링크만 포함, 업로드용 (컬럼 이름 변환)

    Args:
        df_finalized: 최종 처리된 DataFrame
        output_path_base: 출력 파일의 기본 경로

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
    
    # Default return values
    result_path = None
    result_success = False
    upload_path = None
    upload_success = False

    try:
        # -----------------------------------------
        # 1. Create Result File (A) - with images
        # -----------------------------------------
        result_path = f"{output_path_base}_result.xlsx"
        logger.info(f"Creating result file (A): {result_path} with {len(df_finalized)} rows.")
        
        # 골든 예시처럼 판매가(V포함)(2)와 판매단가(V포함)(2) 컬럼 모두 포함
        if '판매가(V포함)(2)' in df_finalized.columns and '판매단가(V포함)(2)' not in df_finalized.columns:
            df_finalized['판매단가(V포함)(2)'] = df_finalized['판매가(V포함)(2)']
        
        # Create a new workbook for result file
        workbook_result = openpyxl.Workbook()
        worksheet_result = workbook_result.active
        worksheet_result.title = "제품 가격 비교"
        
        # Write data using the helper function
        if not _write_data_to_worksheet(worksheet_result, df_finalized):
            logger.error("Failed to write data to result worksheet")
            return False, False, None, None
            
        # Apply formatting based on manual requirements
        _apply_basic_excel_formatting(worksheet_result, df_finalized.columns.tolist())
        _add_hyperlinks_to_worksheet(worksheet_result, df_finalized, hyperlinks_as_formulas=False)
        _add_header_footer(worksheet_result)
        
        # Remove auto filter as per manual
        if hasattr(worksheet_result, 'auto_filter') and worksheet_result.auto_filter:
            worksheet_result.auto_filter.ref = None
            logger.info("Removed filter from result Excel file")
        
        # Save result file
        workbook_result.save(result_path)
        result_success = True
        
        # -----------------------------------------
        # 2. Create Upload File (P) - without images, with column mapping
        # -----------------------------------------
        upload_path = f"{output_path_base}_upload.xlsx"
        logger.info(f"Creating upload file (P): {upload_path}")
        
        # Create upload version DataFrame
        df_upload = df_finalized.copy()
        
        # Replace image data with web URLs only
        for col in IMAGE_COLUMNS:
            if col in df_upload.columns:
                df_upload[col] = df_upload[col].apply(
                    lambda x: x.get('url') if isinstance(x, dict) and 'url' in x 
                    else (x if isinstance(x, str) and x.startswith(('http://', 'https://')) 
                    else '')
                )
        
        # Prepare Naver image URLs for upload
        df_upload = prepare_naver_image_urls_for_upload(df_upload)
        
        # Apply column name mapping for upload file based on @엑셀골든_upload 예시
        # 컬럼 이름을 upload 파일 형식에 맞게 변환
        upload_columns_mapping = {
            '구분': '구분(승인관리:A/가격관리:P)',
            '업체명': '공급사명',
            '업체코드': '공급처코드',
            'Code': '상품코드',
            '중분류카테고리': '카테고리(중분류)',
            '기본수량(1)': '본사 기본수량',
            '판매단가(V포함)': '판매단가1(VAT포함)',
            '본사상품링크': '본사링크',
            '기본수량(2)': '고려 기본수량',
            '판매가(V포함)(2)': '판매단가2(VAT포함)',
            '가격차이(2)': '고려 가격차이',
            '가격차이(2)(%)': '고려 가격차이(%)',
            '고려기프트 상품링크': '고려 링크',
            '기본수량(3)': '네이버 기본수량',
            '판매단가(V포함)(3)': '판매단가3 (VAT포함)',
            '가격차이(3)': '네이버 가격차이',
            '가격차이(3)(%)': '네이버가격차이(%)',
            '공급사명': '네이버 공급사명',
            '네이버 쇼핑 링크': '네이버 링크',
            '본사 이미지': '해오름(이미지링크)',
            '고려기프트 이미지': '고려기프트(이미지링크)',
            '네이버 이미지': '네이버쇼핑(이미지링크)'
        }
        
        # 존재하는 컬럼만 매핑
        upload_columns_mapping_filtered = {k: v for k, v in upload_columns_mapping.items() if k in df_upload.columns}
        df_upload.rename(columns=upload_columns_mapping_filtered, inplace=True)
        
        # 매뉴얼에 따라 컬럼 순서 지정
        upload_columns_order = [
            '구분(승인관리:A/가격관리:P)', '담당자', '공급사명', '공급처코드', '상품코드', 
            '카테고리(중분류)', '상품명', '본사 기본수량', '판매단가1(VAT포함)', '본사링크',
            '고려 기본수량', '판매단가2(VAT포함)', '고려 가격차이', '고려 가격차이(%)', '고려 링크',
            '네이버 기본수량', '판매단가3 (VAT포함)', '네이버 가격차이', '네이버가격차이(%)',
            '네이버 공급사명', '네이버 링크', '해오름(이미지링크)', '고려기프트(이미지링크)', '네이버쇼핑(이미지링크)'
        ]
        
        # 존재하는 컬럼만 순서 지정
        upload_columns_order_filtered = [col for col in upload_columns_order if col in df_upload.columns]
        extra_columns = [col for col in df_upload.columns if col not in upload_columns_order_filtered]
        df_upload = df_upload[upload_columns_order_filtered + extra_columns]
        
        # 엑셀골든_upload 예시처럼 마지막에 추가 행 삽입
        # 1) 원래 데이터 길이 저장
        original_length = len(df_upload)
        
        # 2) 추가 행 삽입 (빈 행 + '\' 포함 행)
        df_upload.loc[original_length] = ''  # 빈 행 추가
        
        # 3) '\' 문자가 있는 행 추가 - 첫 번째 열에만 '\' 추가하고 나머지는 빈 값
        backslash_row = ['\\'] + [''] * (len(df_upload.columns) - 1)
        df_upload.loc[original_length + 1] = backslash_row
        
        # Create new workbook for upload file
        workbook_upload = openpyxl.Workbook()
        worksheet_upload = workbook_upload.active
        worksheet_upload.title = "제품 가격 비교"
        
        # Write data using the helper function
        if not _write_data_to_worksheet(worksheet_upload, df_upload):
            logger.error("Failed to write data to upload worksheet")
            return result_success, False, result_path, None
            
        # Apply upload-specific formatting
        _apply_basic_excel_formatting(worksheet_upload, df_upload.columns.tolist())
        _add_hyperlinks_to_worksheet(worksheet_upload, df_upload, hyperlinks_as_formulas=True)
        _add_header_footer(worksheet_upload)
        
        # Save upload file
        workbook_upload.save(upload_path)
        upload_success = True
        
        # Log success
        logger.info(f"Successfully created both result (A) and upload (P) files")
        logger.info(f"Result file: {result_path}")
        logger.info(f"Upload file: {upload_path}")
        
        return result_success, upload_success, result_path, upload_path
        
    except PermissionError as pe:
        logger.error(f"Permission denied writing Excel file. Is it open? Error: {pe}")
        return result_success, upload_success, result_path, upload_path
    except Exception as e:
        logger.error(f"Error creating Excel files: {e}")
        logger.error(f"Error details: {traceback.format_exc()}")
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
    df_finalized = finalize_dataframe_for_excel(df)
    
    if df_finalized.empty and not df.empty:
        logger.error("DataFrame became empty after finalization step. Cannot save Excel.")
        return False
    
    # Flatten any nested image dictionaries
    df_finalized = flatten_nested_image_dicts(df_finalized)
    
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
        # 골든 예시의 오류 메시지 목록
        error_messages = {
            "가격 범위내에 없거나 텍스트 유사율을 가진 상품이 없음",
            "가격이 범위내에 없거나 검색된 상품이 없음",
            "일정 정확도 이상의 텍스트 유사율을 가진 상품이 없음",
            "검색 결과 0",
            "이미지를 찾을 수 없음"
        }
        
        def extract_url_from_complex_value(value):
            """Helper function to safely extract URL from complex data structures"""
            if pd.isna(value) or value is None:
                return ""
                
            # Handle error messages
            if isinstance(value, str) and value in error_messages:
                return value
                
            # Handle simple string values
            if isinstance(value, str):
                if value == '-' or value == '':
                    return '-'
                return value
                
            # Handle numbers
            if isinstance(value, (int, float)):
                return value
                
            # Handle dictionary values
            if isinstance(value, dict):
                # Try to convert dictionary to a string representation for safe handling
                try:
                    # First look for URL in various formats
                    # Case 1: Nested URL structure {'url': {'url': 'actual_url', ...}}
                    if 'url' in value and isinstance(value['url'], dict) and 'url' in value['url']:
                        return value['url']['url']
                    
                    # Case 2: Direct URL {'url': 'actual_url'}
                    elif 'url' in value and isinstance(value['url'], str):
                        return value['url']
                    
                    # Case 3: Look for other common URL fields
                    for url_field in ['image_url', 'src', 'link', 'href', 'product_url']:
                        if url_field in value and isinstance(value[url_field], str):
                            return value[url_field]
                    
                    # Case 4: Look for local path as fallback
                    for path_field in ['local_path', 'path', 'file_path', 'original_path']:
                        if path_field in value and value[path_field]:
                            return str(value[path_field])
                    
                    # Case 5: Product name as last resort
                    if 'product_name' in value:
                        return f"Product: {value['product_name']}"
                    
                    # If no useful field found, convert to simple string
                    # But limit string length to avoid Excel cell size issues
                    dict_str = str(value)
                    if len(dict_str) > 255:  # Excel cell max~32,767 chars, but keep smaller
                        return f"Complex data (Dict with {len(value)} keys)"
                    return dict_str
                    
                except Exception as dict_err:
                    logger.debug(f"Error extracting from dict: {dict_err}")
                    return "Complex dictionary data"
                
            # Handle list/tuple values
            if isinstance(value, (list, tuple)):
                try:
                    # Try to extract URL from first item
                    if len(value) > 0:
                        first_item = extract_url_from_complex_value(value[0])
                        if first_item and first_item != '-':
                            return first_item
                    
                    # If no URL in items, use a generic representation
                    return f"List with {len(value)} items"
                except Exception as list_err:
                    logger.debug(f"Error processing list: {list_err}")
                    return "Complex list data"
            
            # Default case - convert to string safely
            try:
                return str(value)
            except:
                return "Complex data (unconvertible)"
        
        # Write header
        for col_idx, col_name in enumerate(df_for_excel.columns, 1):
            worksheet.cell(row=1, column=col_idx, value=col_name)
        
        # Write data
        for row_idx, row in enumerate(df_for_excel.itertuples(), 2):
            for col_idx, value in enumerate(row[1:], 1):  # Skip the index
                try:
                    # Extract clean value for Excel
                    cell_value = extract_url_from_complex_value(value)
                    
                    # Set the cell value
                    worksheet.cell(row=row_idx, column=col_idx, value=cell_value)
                    
                except Exception as e:
                    # Log the error but continue with other cells
                    logger.error(f"Error writing cell at row {row_idx}, col {col_idx}: {str(e)}")
                    # Put a placeholder in the cell to avoid further errors
                    worksheet.cell(row=row_idx, column=col_idx, value="-")
        
        return True
    except Exception as e:
        logger.error(f"Error writing data to worksheet: {str(e)}")
        logger.error(f"Error details: {traceback.format_exc()}")
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