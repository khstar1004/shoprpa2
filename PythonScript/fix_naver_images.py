#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Fix Naver Images in Excel Files
--------------------------------
This script fixes issues with Naver images in Excel files by:
1. Verifying Naver product info matches with images
2. Removing misplaced Naver images
3. Ensuring images are in correct columns
4. Fixing image paths and URLs

Usage:
    python fix_naver_images.py --input [input_excel_file] --output [output_excel_file]
"""

import os
import sys
import logging
import pandas as pd
import argparse
from pathlib import Path
import re
import shutil
import hashlib
from datetime import datetime
import asyncio
import aiohttp
import ast
import random
from playwright.async_api import Page

# Import the new NaverImageHandler
try:
    from naver_image_handler import NaverImageHandler, fix_naver_image_data
except ImportError:
    # If direct import fails, try from PythonScript prefix
    try:
        from PythonScript.naver_image_handler import NaverImageHandler, fix_naver_image_data
    except ImportError:
        # Define a simple version as fallback
        logging.warning("Unable to import NaverImageHandler module. Using simplified version.")
        
        def fix_naver_image_data(img_data):
            """Fallback version when module is not available"""
            return img_data
            
        class NaverImageHandler:
            """Simplified fallback version"""
            def __init__(self, config=None):
                self.image_dir = Path('C:\\RPA\\Image\\Main\\Naver')
                self.image_dir.mkdir(parents=True, exist_ok=True)
                
            def fix_image_data_in_dataframe(self, df, naver_img_column='네이버 이미지'):
                """Simplified version"""
                return df
                
            def transform_for_upload(self, df, result_column='네이버 이미지', upload_column='네이버쇼핑(이미지링크)'):
                """Simplified version"""
                if result_column not in df.columns:
                    return df
                if upload_column not in df.columns:
                    df[upload_column] = '-'
                    
                for idx in range(len(df)):
                    img_data = df.loc[idx, result_column]
                    if isinstance(img_data, dict) and 'url' in img_data:
                        df.loc[idx, upload_column] = img_data['url']
                    else:
                        df.loc[idx, upload_column] = '-'
                return df

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('fix_naver_images.log')
    ]
)
logger = logging.getLogger('fix_naver_images')

def verify_naver_product_info(row_data):
    """
    Verify if a row has valid Naver product information.
    
    Args:
        row_data: DataFrame row
        
    Returns:
        bool: True if valid Naver product info exists
    """
    # Check for Naver image data
    if '네이버 이미지' in row_data and pd.notna(row_data['네이버 이미지']):
        image_data = row_data['네이버 이미지']
        
        # Handle dictionary format
        if isinstance(image_data, dict):
            # Check for product URL first (preferred)
            if 'product_url' in image_data and isinstance(image_data['product_url'], str):
                if image_data['product_url'].startswith(('http://', 'https://')):
                    return True
            
            # Then check for regular URL
            if 'url' in image_data and isinstance(image_data['url'], str):
                if image_data['url'].startswith(('http://', 'https://')):
                    return True
                    
            # Check for local path
            if 'local_path' in image_data and image_data['local_path']:
                if os.path.exists(image_data['local_path']):
                    return True
    
    # Check for Naver link
    naver_link_cols = ['네이버 쇼핑 링크', '네이버 링크']
    for col in naver_link_cols:
        if col in row_data and pd.notna(row_data[col]):
            link = str(row_data[col]).strip()
            if link and link not in ['-', 'None', ''] and link.startswith(('http://', 'https://')):
                return True
    
    # Check for Naver price
    price_cols = ['판매단가(V포함)(3)', '네이버 판매단가', '판매단가3 (VAT포함)', '네이버 기본수량']
    for col in price_cols:
        if col in row_data and pd.notna(row_data[col]):
            price = row_data[col]
            if isinstance(price, (int, float)) and price > 0:
                return True
            elif isinstance(price, str):
                try:
                    price = float(price.replace(',', ''))
                    if price > 0:
                        return True
                except:
                    continue
    
    return False

def extract_naver_image_info(img_data):
    """
    Extract relevant information from Naver image data.
    
    Args:
        img_data: Image data (dict or string)
        
    Returns:
        dict: Extracted image information
    """
    info = {
        'url': None,
        'local_path': None,
        'is_valid': False,
        'source': 'naver'
    }
    
    if isinstance(img_data, dict):
        # 일반 URL 확인
        if 'url' in img_data and img_data['url'] and isinstance(img_data['url'], str):
            info['url'] = img_data['url'].strip()
        # 로컬 경로 확인
        if 'local_path' in img_data and img_data['local_path']:
            info['local_path'] = img_data['local_path']
        # 원본 경로 정보 보존
        if 'original_path' in img_data:
            info['original_path'] = img_data['original_path']
        # 유사도 점수 확인
        if 'score' in img_data:
            info['score'] = img_data['score']
        elif 'similarity' in img_data:
            info['score'] = img_data['similarity']
        
        # URL이 없는 경우 유효하지 않은 이미지로 판단
        if not info['url']:
            info['is_valid'] = False
            return info
            
        # URL 형식 확인
        if not info['url'].startswith(('http://', 'https://')):
            info['is_valid'] = False
            return info
        
        # Naver 이미지 URL 패턴 확인
        if 'pstatic.net' in info['url']:
            # 신뢰할 수 없는 'front' URL 필터링
            if 'front' in info['url']:
                info['is_valid'] = False
            else:
                info['is_valid'] = True
        elif 'shopping.naver.com' in info['url'] or 'search.shopping.naver.com' in info['url']:
            info['is_valid'] = True
        else:
            # 네이버 도메인 외부의 URL은 추가 검증
            # 상품 이미지인지 확인할 수 있는 키워드 체크
            image_patterns = ['.jpg', '.jpeg', '.png', '.gif', '/img/', '/image/', '/images/']
            if any(pattern in info['url'].lower() for pattern in image_patterns):
                info['is_valid'] = True
            else:
                info['is_valid'] = False
    
    elif isinstance(img_data, str):
        if img_data.startswith(('http://', 'https://')):
            info['url'] = img_data.strip()
            
            # Naver 이미지 URL 패턴 확인 (문자열 버전)
            if 'pstatic.net' in info['url'] and 'front' not in info['url']:
                info['is_valid'] = True
            elif 'shopping.naver.com' in info['url'] or 'search.shopping.naver.com' in info['url']:
                info['is_valid'] = True
            else:
                # 네이버 도메인 외부의 URL은 추가 검증
                image_patterns = ['.jpg', '.jpeg', '.png', '.gif', '/img/', '/image/', '/images/']
                if any(pattern in info['url'].lower() for pattern in image_patterns):
                    info['is_valid'] = True
        elif img_data.startswith('/'):
            # 상대 경로인 경우 (로컬 파일일 수 있음)
            info['local_path'] = img_data
            # 파일이 실제로 존재하는지 확인
            if os.path.exists(img_data) and os.path.getsize(img_data) > 0:
                info['is_valid'] = True
    
    # 로컬 파일 존재 여부 확인 (파일이 존재하면 URL이 없어도 유효)
    if info['local_path'] and os.path.exists(info['local_path']) and os.path.getsize(info['local_path']) > 0:
        # 실제 존재하는 이미지 파일이면 유효함
        info['is_valid'] = True
    
    return info

def fix_naver_images(df):
    """
    Fix Naver image issues in the DataFrame.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: Fixed DataFrame
    """
    result_df = df.copy()
    
    # Initialize the NaverImageHandler for better processing
    naver_handler = NaverImageHandler()
    
    # Track statistics
    stats = {
        'total_rows': len(df),
        'rows_with_naver_info': 0,
        'misplaced_images_removed': 0,
        'images_fixed': 0,
        'invalid_urls_removed': 0,
        'low_similarity_removed': 0,  # 유사도 낮은 이미지 카운트 추가
        'all_info_removed': 0         # 모든 네이버 정보 제거 카운트 추가
    }
    
    # First run the handler's fix method to normalize URLs and check local paths
    result_df = naver_handler.fix_image_data_in_dataframe(result_df, naver_img_column='네이버 이미지')
    
    # Naver related columns (상품 정보 관련 컬럼들)
    naver_columns = [
        '네이버 이미지', '네이버 쇼핑 링크', '네이버 링크', '네이버 유사도', 
        '네이버 판매단가', '네이버 공급사', '네이버 상품명', '네이버쇼핑(이미지링크)',
        '판매단가(V포함)(3)', '판매단가3 (VAT포함)', '공급사3', '공급사 상품링크'
    ]
    
    # Process each row
    for idx, row in result_df.iterrows():
        # Check if row has valid Naver product info
        has_naver_info = verify_naver_product_info(row)
        
        # 유사도 값을 확인 (있는 경우)
        similarity_score = None
        if '네이버 유사도' in row and pd.notna(row['네이버 유사도']):
            try:
                similarity_score = float(row['네이버 유사도'])
            except (ValueError, TypeError):
                pass
        
        # 네이버 상품 정보와 이미지 링크 점검
        naver_link_exists = False
        for link_col in ['네이버 쇼핑 링크', '네이버 링크', '공급사 상품링크']:
            if link_col in row and pd.notna(row[link_col]):
                link_value = row[link_col]
                if isinstance(link_value, str) and link_value not in ['-', '', 'None'] and link_value.startswith(('http://', 'https://')):
                    naver_link_exists = True
                    break
        
        # 네이버 이미지 데이터 가져오기
        naver_img = row.get('네이버 이미지', None)
        
        # 네이버 링크가 없는 경우 - 모든 네이버 정보 제거
        if not naver_link_exists:
            # 모든 네이버 관련 컬럼의 데이터 제거
            for col in naver_columns:
                if col in result_df.columns and pd.notna(row.get(col)):
                    result_df.at[idx, col] = '-'
            
            stats['all_info_removed'] += 1
            logger.info(f"Row {idx}: Removed all Naver information because no product link found")
            continue
        
        if has_naver_info:
            stats['rows_with_naver_info'] += 1
            
            # 유사도가 너무 낮은 경우 (0.4 미만) 처리
            if similarity_score is not None and similarity_score < 0.4:
                # 유사도가 너무 낮은 이미지는 제거
                if naver_img and naver_img != '-':
                    result_df.at[idx, '네이버 이미지'] = '-'
                    # 네이버쇼핑(이미지링크) 컬럼도 함께 제거 (업로드 파일용)
                    if '네이버쇼핑(이미지링크)' in result_df.columns:
                        result_df.at[idx, '네이버쇼핑(이미지링크)'] = '-'
                    stats['low_similarity_removed'] += 1
                    logger.info(f"Row {idx}: Removed Naver image due to low similarity score: {similarity_score}")
                continue
            
            # 이미지 데이터 검증 및 처리
            if naver_img:
                img_info = extract_naver_image_info(naver_img)
                
                # 이미지 URL이 유효하지 않은 경우
                if not img_info['is_valid'] or not img_info['url']:
                    # 유효하지 않은 이미지 또는, URL이 없는 이미지 제거
                    result_df.at[idx, '네이버 이미지'] = '-'
                    # 네이버쇼핑(이미지링크) 컬럼도 함께 제거 (업로드 파일용)
                    if '네이버쇼핑(이미지링크)' in result_df.columns:
                        result_df.at[idx, '네이버쇼핑(이미지링크)'] = '-'
                    stats['invalid_urls_removed'] += 1
                    logger.info(f"Row {idx}: Removed invalid Naver image URL or image without URL")
                elif img_info['url'] and 'front' in img_info['url']:
                    # 신뢰할 수 없는 'front' URL 제거
                    result_df.at[idx, '네이버 이미지'] = '-'
                    # 네이버쇼핑(이미지링크) 컬럼도 함께 제거 (업로드 파일용)
                    if '네이버쇼핑(이미지링크)' in result_df.columns:
                        result_df.at[idx, '네이버쇼핑(이미지링크)'] = '-'
                    stats['invalid_urls_removed'] += 1
                    logger.info(f"Row {idx}: Removed unreliable 'front' URL")
                else:
                    # 이미지 데이터 깔끔하게 정리
                    clean_img_data = {
                        'url': img_info['url'],
                        'local_path': img_info['local_path'],
                        'source': 'naver',
                        'score': img_info.get('score', 0.5)
                    }
                    result_df.at[idx, '네이버 이미지'] = clean_img_data
                    
                    # 네이버쇼핑(이미지링크) 컬럼도 함께 업데이트 (업로드 파일용)
                    if '네이버쇼핑(이미지링크)' in result_df.columns:
                        result_df.at[idx, '네이버쇼핑(이미지링크)'] = img_info['url']
                    
                    stats['images_fixed'] += 1
            else:
                # 이미지가 없지만 상품 정보가 있는 경우 (이미지 없음 명시)
                result_df.at[idx, '네이버 이미지'] = '-'
                # 네이버쇼핑(이미지링크) 컬럼도 함께 제거 (업로드 파일용)
                if '네이버쇼핑(이미지링크)' in result_df.columns:
                    result_df.at[idx, '네이버쇼핑(이미지링크)'] = '-'
        else:
            # 네이버 상품 정보가 없는 경우 - 모든 네이버 이미지 제거
            if '네이버 이미지' in result_df.columns:
                current_img = row.get('네이버 이미지')
                if current_img and current_img != '-':
                    result_df.at[idx, '네이버 이미지'] = '-'
                    # 네이버쇼핑(이미지링크) 컬럼도 함께 제거 (업로드 파일용)
                    if '네이버쇼핑(이미지링크)' in result_df.columns:
                        result_df.at[idx, '네이버쇼핑(이미지링크)'] = '-'
                    stats['misplaced_images_removed'] += 1
                    logger.info(f"Row {idx}: Removed misplaced Naver image (no product info)")
    
    # 로그에 통계 정보
    logger.info("=== Naver Image Fix Statistics ===")
    logger.info(f"Total rows processed: {stats['total_rows']}")
    logger.info(f"Rows with Naver product info: {stats['rows_with_naver_info']}")
    logger.info(f"Misplaced images removed: {stats['misplaced_images_removed']}")
    logger.info(f"Invalid URLs removed: {stats['invalid_urls_removed']}")
    logger.info(f"Low similarity images removed: {stats['low_similarity_removed']}")
    logger.info(f"All Naver info removed (no links): {stats['all_info_removed']}")
    logger.info(f"Images fixed: {stats['images_fixed']}")
    
    return result_df

def fix_excel_file(input_file, output_file=None):
    """
    Fix Naver images in an Excel file.
    
    Args:
        input_file: Path to the input Excel file
        output_file: Path to the output Excel file (optional)
        
    Returns:
        str: Path to the output file if successful, None otherwise
    """
    try:
        # Read Excel file
        logger.info(f"Reading Excel file: {input_file}")
        df = pd.read_excel(input_file)
        
        if df.empty:
            logger.error("Input Excel file is empty")
            return None
        
        # Check if this is an upload file (typically has fewer columns and might have columns like "네이버쇼핑(이미지링크)")
        is_upload_file = False
        if '네이버쇼핑(이미지링크)' in df.columns and len(df.columns) < 20:
            is_upload_file = True
            logger.info("Detected upload file format (has '네이버쇼핑(이미지링크)' column and fewer columns)")
        
        # Print initial statistics
        logger.info(f"Initial DataFrame shape: {df.shape}")
        logger.info(f"File type: {'Upload file' if is_upload_file else 'Result file'}")
        
        # Log which columns are present
        column_list = df.columns.tolist()
        logger.info(f"Columns in the file: {column_list}")
        
        # Check for Naver image column
        naver_img_col = '네이버 이미지'
        naver_img_link_col = '네이버쇼핑(이미지링크)'
        naver_price_col = None
        
        # Determine which columns to check
        key_columns = []
        if naver_img_col in column_list:
            key_columns.append(naver_img_col)
        if naver_img_link_col in column_list:
            key_columns.append(naver_img_link_col)
        
        # Find Naver price column
        for col in ['판매단가(V포함)(3)', '네이버 판매단가', '판매단가3 (VAT포함)']:
            if col in column_list:
                naver_price_col = col
                key_columns.append(col)
                break
        
        # Initial counts of rows with data in key columns
        initial_counts = {}
        for col in key_columns:
            valid_count = df[col].apply(lambda x: pd.notna(x) and x != '-').sum()
            initial_counts[col] = valid_count
            logger.info(f"Initial count for {col}: {valid_count} rows")
        
        # 1. 이미지 수정 적용
        logger.info("Applying fix_naver_images...")
        df_fixed = fix_naver_images(df)
        
        # 2. 이미지 위치 검증 및 수정
        logger.info("Validating and fixing image placement...")
        df_fixed = validate_and_fix_naver_image_placement(df_fixed)
        
        # 3. 추가 처리 - 링크와 이미지 간의 일관성 확인
        # 네이버 쇼핑 링크 컬럼들
        link_columns = ['네이버 쇼핑 링크', '네이버 링크', '공급사 상품링크']
        link_columns = [col for col in link_columns if col in df_fixed.columns]
        
        # 링크가 없는 행에서는 이미지 및 다른 네이버 정보도 제거
        rows_with_data_removed = 0
        naver_columns = [
            '네이버 이미지', '네이버 쇼핑 링크', '네이버 링크', '네이버 유사도', 
            '네이버 판매단가', '네이버 공급사', '네이버 상품명', '네이버쇼핑(이미지링크)',
            '판매단가(V포함)(3)', '판매단가3 (VAT포함)', '공급사3', '공급사 상품링크'
        ]
        naver_columns = [col for col in naver_columns if col in df_fixed.columns]
        
        for idx, row in df_fixed.iterrows():
            # 링크가 있는지 확인
            has_link = False
            for link_col in link_columns:
                if pd.notna(row.get(link_col)) and row.get(link_col) != '-':
                    link_value = row.get(link_col)
                    if isinstance(link_value, str) and link_value.strip() and link_value not in ['-', '', 'None'] and link_value.startswith(('http://', 'https://')):
                        has_link = True
                        break
            
            # 링크가 없는 경우 모든 네이버 관련 정보 제거
            if not has_link:
                any_data_removed = False
                for col in naver_columns:
                    if col in df_fixed.columns and pd.notna(row.get(col)) and row.get(col) != '-':
                        df_fixed.at[idx, col] = '-'
                        any_data_removed = True
                
                if any_data_removed:
                    rows_with_data_removed += 1
        
        logger.info(f"Additional rows with data removed due to missing links: {rows_with_data_removed}")
        
        # Final counts of rows with data in key columns
        final_counts = {}
        for col in key_columns:
            valid_count = df_fixed[col].apply(lambda x: pd.notna(x) and x != '-').sum()
            final_counts[col] = valid_count
            change = valid_count - initial_counts.get(col, 0)
            logger.info(f"Final count for {col}: {valid_count} rows (Change: {change})")
        
        # Create output path if none specified
        if output_file is None:
            input_basename = os.path.basename(input_file)
            filename, ext = os.path.splitext(input_basename)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join('C:', 'RPA', 'Output', f"{filename}_fixed_{timestamp}{ext}")
        
        # Ensure output directory exists
        output_dir = os.path.dirname(output_file)
        os.makedirs(output_dir, exist_ok=True)
        
        # Save to Excel
        logger.info(f"Saving fixed Excel file to: {output_file}")
        df_fixed.to_excel(output_file, index=False)
        
        logger.info(f"Excel file fixed successfully: {output_file}")
        return output_file
        
    except Exception as e:
        logger.error(f"Error fixing Excel file: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def validate_and_fix_naver_image_placement(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates and fixes Naver image placement in the DataFrame.
    
    Args:
        df: DataFrame containing Naver image data
        
    Returns:
        DataFrame with validated and fixed Naver image placement
    """
    if df.empty:
        return df
        
    result_df = df.copy()
    
    # Initialize NaverImageHandler for better processing
    naver_handler = NaverImageHandler()
    
    # First use the handler to fix image data format and normalize URLs
    result_df = naver_handler.fix_image_data_in_dataframe(result_df, naver_img_column='네이버 이미지')
    
    # Track statistics
    fixed_count = 0
    removed_count = 0
    low_similarity_removed = 0
    no_link_removed = 0
    all_info_removed = 0
    
    # Naver related columns (상품 정보 관련 컬럼들)
    naver_columns = [
        '네이버 이미지', '네이버 쇼핑 링크', '네이버 링크', '네이버 유사도', 
        '네이버 판매단가', '네이버 공급사', '네이버 상품명', '네이버쇼핑(이미지링크)',
        '판매단가(V포함)(3)', '판매단가3 (VAT포함)', '공급사3', '공급사 상품링크'
    ]
    
    # Process Naver image column
    naver_img_col = '네이버 이미지'
    naver_img_link_col = '네이버쇼핑(이미지링크)'  # 업로드 파일용 이미지 링크 컬럼
    
    if naver_img_col not in result_df.columns:
        logger.warning(f"Naver image column '{naver_img_col}' not found")
        return result_df
        
    # Process each row
    for idx in result_df.index:
        try:
            img_data = result_df.at[idx, naver_img_col]
            
            # Skip empty or invalid data
            if pd.isna(img_data) or img_data == '-':
                continue
            
            # 1. 유사도 검사
            similarity_score = None
            if '네이버 유사도' in result_df.columns and pd.notna(result_df.at[idx, '네이버 유사도']):
                try:
                    similarity_score = float(result_df.at[idx, '네이버 유사도'])
                except (ValueError, TypeError):
                    pass
            elif isinstance(img_data, dict) and ('score' in img_data or 'similarity' in img_data):
                # 이미지 데이터 안에 점수가 있는 경우
                similarity_score = img_data.get('score', img_data.get('similarity', None))
                if similarity_score is not None:
                    try:
                        similarity_score = float(similarity_score)
                    except (ValueError, TypeError):
                        similarity_score = None
            
            # 2. 네이버 링크 검사
            has_naver_link = False
            for link_column in ['네이버 쇼핑 링크', '네이버 링크', '공급사 상품링크']:
                if link_column in result_df.columns and pd.notna(result_df.at[idx, link_column]):
                    link_value = result_df.at[idx, link_column]
                    if isinstance(link_value, str) and link_value.strip() and link_value.strip() not in ['-', 'None', ''] and link_value.startswith(('http://', 'https://')):
                        has_naver_link = True
                        break
            
            # 링크가 없는 경우 - 모든 네이버 관련 정보 제거
            if not has_naver_link:
                # 모든 네이버 관련 컬럼의 데이터 제거
                for col in naver_columns:
                    if col in result_df.columns and pd.notna(result_df.at[idx, col]):
                        result_df.at[idx, col] = '-'
                
                all_info_removed += 1
                logger.info(f"Row {idx}: Removed all Naver information because no Naver product link found")
                continue
            
            # 유사도가 너무 낮으면 이미지 제거 (0.4 미만)
            if similarity_score is not None and similarity_score < 0.4:
                result_df.at[idx, naver_img_col] = '-'
                # 업로드 파일용 이미지 링크도 함께 제거
                if naver_img_link_col in result_df.columns:
                    result_df.at[idx, naver_img_link_col] = '-'
                low_similarity_removed += 1
                logger.info(f"Row {idx}: Removed image due to low similarity score: {similarity_score}")
                continue
            
            # 3. 상품 가격 검사 (Naver 상품 정보 확인)
            has_naver_price = False
            for price_col in ['판매단가(V포함)(3)', '네이버 판매단가', '판매단가3 (VAT포함)']:
                if price_col in result_df.columns and pd.notna(result_df.at[idx, price_col]):
                    try:
                        price_value = result_df.at[idx, price_col]
                        if isinstance(price_value, (int, float)) and price_value > 0:
                            has_naver_price = True
                            break
                        elif isinstance(price_value, str):
                            # 문자열인 경우 숫자로 변환 시도
                            price_str = price_value.replace(',', '')
                            if price_str.replace('.', '').isdigit() and float(price_str) > 0:
                                has_naver_price = True
                                break
                    except:
                        pass
            
            # 네이버 가격 정보가 없고, 유사도도 없는 경우 (유효하지 않은 상품 정보)
            if not has_naver_price and similarity_score is None:
                result_df.at[idx, naver_img_col] = '-'
                # 업로드 파일용 이미지 링크도 함께 제거
                if naver_img_link_col in result_df.columns:
                    result_df.at[idx, naver_img_link_col] = '-'
                removed_count += 1
                logger.info(f"Row {idx}: Removed image because no Naver price information found")
                continue
                
            # Handle dictionary format - use improved validation logic
            if isinstance(img_data, dict):
                # URL 확인 및 유효성 검사
                url = img_data.get('url', '')
                local_path = img_data.get('local_path', '')
                
                # URL이 없거나 유효하지 않은 경우
                if not url or not isinstance(url, str) or not url.startswith(('http://', 'https://')):
                    # 로컬 파일만 있고 URL이 없는 경우 - URL 없이 로컬 파일만 있는지 확인
                    if local_path and os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                        # 로컬 파일은 있지만 URL이 없는 경우 - 유지 (이미지는 있으므로)
                        img_data['url'] = ''  # URL 값 명시적으로 비움
                        result_df.at[idx, naver_img_col] = img_data
                        # 업로드 파일용 이미지 링크는 비움
                        if naver_img_link_col in result_df.columns:
                            result_df.at[idx, naver_img_link_col] = '-'
                        fixed_count += 1
                        logger.debug(f"Row {idx}: Fixed image data - kept local path without URL")
                    else:
                        # 로컬 파일도 없고 URL도 없는 경우 - 이미지 제거
                        result_df.at[idx, naver_img_col] = '-'
                        # 업로드 파일용 이미지 링크도 함께 제거
                        if naver_img_link_col in result_df.columns:
                            result_df.at[idx, naver_img_link_col] = '-'
                        removed_count += 1
                        logger.info(f"Row {idx}: Removed image because no valid URL or local path found")
                else:
                    # URL이 있는 경우 - 확인 및 전처리
                    if 'pstatic.net/front/' in url:
                        # 신뢰할 수 없는 'front' URL 제거
                        result_df.at[idx, naver_img_col] = '-'
                        # 업로드 파일용 이미지 링크도 함께 제거
                        if naver_img_link_col in result_df.columns:
                            result_df.at[idx, naver_img_link_col] = '-'
                        removed_count += 1
                        logger.info(f"Row {idx}: Removed unreliable 'front' URL: {url[:50]}...")
                    else:
                        # 유효한 URL - 이미지 데이터 정리
                        fixed_data = {
                            'url': url,
                            'local_path': local_path if local_path and os.path.exists(local_path) else '',
                            'source': 'naver',
                            'score': img_data.get('score', img_data.get('similarity', 0.5))
                        }
                        result_df.at[idx, naver_img_col] = fixed_data
                        
                        # 업로드 파일용 이미지 링크도 함께 업데이트
                        if naver_img_link_col in result_df.columns:
                            result_df.at[idx, naver_img_link_col] = url
                            
                        fixed_count += 1
                        logger.debug(f"Row {idx}: Fixed image data with valid URL")
            elif isinstance(img_data, str):
                # 문자열 형태의 이미지 데이터 처리
                if img_data.startswith(('http://', 'https://')):
                    # URL 문자열
                    if 'pstatic.net/front/' in img_data:
                        # 신뢰할 수 없는 'front' URL 제거
                        result_df.at[idx, naver_img_col] = '-'
                        # 업로드 파일용 이미지 링크도 함께 제거
                        if naver_img_link_col in result_df.columns:
                            result_df.at[idx, naver_img_link_col] = '-'
                        removed_count += 1
                        logger.info(f"Row {idx}: Removed unreliable 'front' URL string: {img_data[:50]}...")
                    else:
                        # 유효한 URL 문자열 - 딕셔너리 형태로 변환
                        result_df.at[idx, naver_img_col] = {
                            'url': img_data,
                            'local_path': '',
                            'source': 'naver',
                            'score': similarity_score if similarity_score is not None else 0.5
                        }
                        
                        # 업로드 파일용 이미지 링크도 함께 업데이트
                        if naver_img_link_col in result_df.columns:
                            result_df.at[idx, naver_img_link_col] = img_data
                            
                        fixed_count += 1
                        logger.debug(f"Row {idx}: Converted URL string to image data dictionary")
                else:
                    # 문자열인데 URL이 아닌 경우
                    # 로컬 파일 경로인지 확인
                    if os.path.exists(img_data) and os.path.getsize(img_data) > 0:
                        # 로컬 파일 경로인 경우
                        result_df.at[idx, naver_img_col] = {
                            'url': '',
                            'local_path': img_data,
                            'source': 'naver',
                            'score': similarity_score if similarity_score is not None else 0.5
                        }
                        
                        # 업로드 파일용 이미지 링크는 비움
                        if naver_img_link_col in result_df.columns:
                            result_df.at[idx, naver_img_link_col] = '-'
                            
                        fixed_count += 1
                        logger.debug(f"Row {idx}: Converted local path string to image data dictionary")
                    else:
                        # 유효하지 않은 문자열 - 제거
                        result_df.at[idx, naver_img_col] = '-'
                        # 업로드 파일용 이미지 링크도 함께 제거
                        if naver_img_link_col in result_df.columns:
                            result_df.at[idx, naver_img_link_col] = '-'
                        removed_count += 1
                        logger.info(f"Row {idx}: Removed invalid image data string: {img_data[:30]}...")
            else:
                # 유효하지 않은 타입 - 제거
                result_df.at[idx, naver_img_col] = '-'
                # 업로드 파일용 이미지 링크도 함께 제거
                if naver_img_link_col in result_df.columns:
                    result_df.at[idx, naver_img_link_col] = '-'
                removed_count += 1
                logger.info(f"Row {idx}: Removed invalid image data type: {type(img_data)}")
                
        except Exception as e:
            logger.error(f"Error processing row {idx}: {e}")
            # 오류 발생 시 이미지 데이터 제거
            result_df.at[idx, naver_img_col] = '-'
            # 업로드 파일용 이미지 링크도 함께 제거
            if naver_img_link_col in result_df.columns:
                result_df.at[idx, naver_img_link_col] = '-'
            removed_count += 1
            
    logger.info(f"Naver image validation complete: {fixed_count} fixed, {removed_count} removed")
    logger.info(f"Low similarity images removed: {low_similarity_removed}")
    logger.info(f"Images removed due to missing links: {no_link_removed}")
    logger.info(f"All Naver info removed (no links): {all_info_removed}")
    
    return result_df

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Fix Naver images in Excel files')
    parser.add_argument('--input', '-i', required=True, help='Input Excel file path')
    parser.add_argument('--output', '-o', help='Output Excel file path (optional)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.info("Verbose logging enabled")
    
    # Create output directory if it doesn't exist
    output_dir = os.path.join('C:', 'RPA', 'Output')
    os.makedirs(output_dir, exist_ok=True)
    
    # If no output path specified, create one based on input filename
    if not args.output:
        input_basename = os.path.basename(args.input)
        filename, ext = os.path.splitext(input_basename)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output = os.path.join(output_dir, f"{filename}_fixed_{timestamp}{ext}")
    else:
        # If output path is specified but directory doesn't exist, create it
        output_parent_dir = os.path.dirname(args.output)
        if output_parent_dir:
            os.makedirs(output_parent_dir, exist_ok=True)
    
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    
    # Log parameters
    logger.info(f"Input file: {args.input}")
    logger.info(f"Output file: {args.output}")
    
    try:
        # 1. 먼저 Excel 파일 읽기
        logger.info(f"Reading Excel file: {args.input}")
        df = pd.read_excel(args.input)
        
        if df.empty:
            logger.error("Input Excel file is empty!")
            print("❌ Input Excel file is empty!")
            return 1
            
        # 초기 상태 분석
        total_rows = len(df)
        naver_info_count = 0
        naver_image_count = 0
        
        # 네이버 이미지 열 확인
        naver_img_col = '네이버 이미지'
        
        if naver_img_col in df.columns:
            # 네이버 이미지가 있는 행 수 확인
            naver_image_count = df[naver_img_col].apply(lambda x: pd.notna(x) and x != '-').sum()
            
            # 네이버 상품 정보가 있는 행 수 확인
            for idx, row in df.iterrows():
                if verify_naver_product_info(row):
                    naver_info_count += 1
            
            # 기존 불일치 확인 (상품 정보는 있는데 이미지 링크는 없는 경우)
            mismatch_count = 0
            no_link_with_image_count = 0
            
            for idx, row in df.iterrows():
                # 상품 정보가 있는지 확인
                has_product_info = verify_naver_product_info(row)
                
                # 이미지가 있는지 확인
                has_image = pd.notna(row.get(naver_img_col)) and row.get(naver_img_col) != '-'
                
                # 네이버 링크가 있는지 확인
                has_link = False
                for link_col in ['네이버 쇼핑 링크', '네이버 링크']:
                    if link_col in row and pd.notna(row[link_col]):
                        link_value = row[link_col]
                        if isinstance(link_value, str) and link_value not in ['-', '', 'None'] and link_value.startswith(('http://', 'https://')):
                            has_link = True
                            break
                
                # 상품 정보는 있는데 링크가 없는 경우
                if has_product_info and not has_link:
                    mismatch_count += 1
                    
                    # 링크가 없는데 이미지가 있는 경우 (주요 문제 케이스)
                    if has_image:
                        no_link_with_image_count += 1
            
            logger.info(f"Initial analysis:")
            logger.info(f"- Total rows: {total_rows}")
            logger.info(f"- Rows with Naver product info: {naver_info_count}")
            logger.info(f"- Rows with Naver images: {naver_image_count}")
            logger.info(f"- Rows with product info but no Naver link: {mismatch_count}")
            logger.info(f"- Rows with image but no Naver link (PROBLEM CASES): {no_link_with_image_count}")
        else:
            logger.warning(f"Column '{naver_img_col}' not found in the input Excel file")
        
        # 2. 이미지 수정 적용
        logger.info("Applying image fixes...")
        df_fixed = fix_naver_images(df)
        
        # 3. 네이버 이미지 위치 검증 및 수정
        logger.info("Validating and fixing Naver image placement...")
        df_fixed = validate_and_fix_naver_image_placement(df_fixed)
        
        # 수정 후 상태 분석
        fixed_naver_image_count = 0
        fixed_no_link_with_image_count = 0
        
        if naver_img_col in df_fixed.columns:
            # 수정 후 네이버 이미지가 있는 행 수 확인
            fixed_naver_image_count = df_fixed[naver_img_col].apply(lambda x: pd.notna(x) and x != '-').sum()
            
            # 수정 후 링크 없이 이미지 있는 행 수 확인 (문제 케이스)
            for idx, row in df_fixed.iterrows():
                # 네이버 링크가 있는지 확인
                has_link = False
                for link_col in ['네이버 쇼핑 링크', '네이버 링크']:
                    if link_col in row and pd.notna(row[link_col]):
                        link_value = row[link_col]
                        if isinstance(link_value, str) and link_value not in ['-', '', 'None'] and link_value.startswith(('http://', 'https://')):
                            has_link = True
                            break
                
                # 이미지가 있는지 확인
                has_image = pd.notna(row.get(naver_img_col)) and row.get(naver_img_col) != '-'
                
                # 링크가 없는데 이미지가 있는 경우 (문제 케이스)
                if not has_link and has_image:
                    fixed_no_link_with_image_count += 1
            
            # 이미지 제거 통계
            images_removed = naver_image_count - fixed_naver_image_count
            problem_cases_fixed = no_link_with_image_count - fixed_no_link_with_image_count
            
            logger.info(f"Fix results:")
            logger.info(f"- Images before fix: {naver_image_count}")
            logger.info(f"- Images after fix: {fixed_naver_image_count}")
            logger.info(f"- Images removed: {images_removed}")
            logger.info(f"- Problem cases before fix: {no_link_with_image_count}")
            logger.info(f"- Problem cases after fix: {fixed_no_link_with_image_count}")
            logger.info(f"- Problem cases fixed: {problem_cases_fixed}")
        
        # 4. 수정된 데이터 저장
        logger.info(f"Saving fixed Excel file to: {args.output}")
        df_fixed.to_excel(args.output, index=False)
        
        # 5. 저장된 파일 확인
        if not os.path.exists(args.output):
            logger.error("Failed to create output file!")
            print("❌ Failed to create output file!")
            return 1
            
        # 성공 메시지 및 통계 출력
        print(f"✅ Successfully fixed Naver images.")
        print(f"✅ Output saved to: {args.output}")
        
        print(f"\nFix Statistics:")
        print(f"- Images before fix: {naver_image_count}")
        print(f"- Images after fix: {fixed_naver_image_count}")
        print(f"- Images removed: {images_removed}")
        print(f"- Problem cases fixed: {problem_cases_fixed}")
        
        return 0
        
    except FileNotFoundError:
        logger.error(f"Input file not found: {args.input}")
        print(f"❌ Input file not found: {args.input}")
        return 1
    except PermissionError:
        logger.error(f"Permission denied when accessing file: {args.output}")
        print(f"❌ Permission denied when accessing file. Make sure Excel is not open.")
        return 1
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        import traceback
        logger.error(traceback.format_exc())
        print(f"❌ An error occurred: {e}")
        return 1

def prepare_naver_columns_for_excel_output(df: pd.DataFrame, is_upload_file: bool = False) -> pd.DataFrame:
    """
    Prepares Naver image columns for Excel output, handling different naming conventions
    between result file and upload file.
    
    Args:
        df: DataFrame to process
        is_upload_file: If True, prepare for upload file (URLs only with different column name)
        
    Returns:
        DataFrame with properly formatted Naver image columns
    """
    if df.empty:
        return df
    
    # Initialize NaverImageHandler for better processing
    naver_handler = NaverImageHandler()
    
    # Column names for different file types
    result_col_name = '네이버 이미지'
    upload_col_name = '네이버쇼핑(이미지링크)'
    
    # First ensure the Naver image column exists
    if result_col_name not in df.columns:
        logger.warning(f"Column '{result_col_name}' not found, cannot prepare Naver images")
        return df
    
    if is_upload_file:
        # For upload file: Use the handler's transform_for_upload method
        df = naver_handler.transform_for_upload(df, result_column=result_col_name, upload_column=upload_col_name)
        
        # Optionally remove the result column if not needed in upload file
        if result_col_name in df.columns:
            df = df.drop(columns=[result_col_name])
            
        logger.info(f"Created '{upload_col_name}' column for upload file with URLs only")
    else:
        # For result file: Make sure Naver image column has consistent format
        df = naver_handler.fix_image_data_in_dataframe(df, naver_img_column=result_col_name)
        logger.info(f"Maintained '{result_col_name}' column for result file with both paths and URLs")
    
    return df

def transform_between_file_types(df: pd.DataFrame, file_type: str) -> pd.DataFrame:
    """
    Transforms a DataFrame between result and upload file formats by
    properly handling the Naver image columns.
    
    Args:
        df: DataFrame to transform
        file_type: Either 'result' or 'upload'
        
    Returns:
        Transformed DataFrame with appropriate Naver image columns
    """
    if file_type.lower() not in ['result', 'upload']:
        logger.error(f"Invalid file_type: {file_type}, must be 'result' or 'upload'")
        return df
    
    is_upload = file_type.lower() == 'upload'
    
    # Check if transformation is needed
    result_col_name = '네이버 이미지'
    upload_col_name = '네이버쇼핑(이미지링크)'
    
    # For upload file
    if is_upload:
        return prepare_naver_columns_for_excel_output(df, is_upload_file=True)
    # For result file
    else:
        # If the DataFrame already has the upload column but needs result column
        if upload_col_name in df.columns and result_col_name not in df.columns:
            # Initialize NaverImageHandler
            naver_handler = NaverImageHandler()
            
            # Convert from upload format to result format
            # Create a new column with empty dictionaries
            df[result_col_name] = None
            
            # Process each row
            for idx, row in df.iterrows():
                if pd.isna(row[upload_col_name]) or row[upload_col_name] == '-':
                    df.at[idx, result_col_name] = '-'
                else:
                    url = row[upload_col_name]
                    if isinstance(url, str) and url.startswith(('http://', 'https://')):
                        # Create dictionary structure for result file
                        df.at[idx, result_col_name] = {
                            'url': url,
                            'local_path': '',  # No local path available
                            'source': 'naver',
                            'score': 0.7  # Default score
                        }
                    else:
                        df.at[idx, result_col_name] = '-'
            
            logger.info(f"Created '{result_col_name}' column from '{upload_col_name}' column")
        
        return df 

async def download_image(session, url, filepath):
    """Download an image from a URL and save it to filepath."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                content = await response.read()
                with open(filepath, 'wb') as f:
                    f.write(content)
                logging.info(f"Successfully downloaded: {url}")
                return True
            else:
                logging.warning(f"Failed to download {url}, status: {response.status}")
                return False
    except Exception as e:
        logging.error(f"Error downloading {url}: {e}")
        return False

async def ensure_naver_local_images_async(df: pd.DataFrame, naver_image_dir: str) -> pd.DataFrame:
    """Async version of ensure_naver_local_images"""
    try:
        # Create output directory if it doesn't exist
        os.makedirs(naver_image_dir, exist_ok=True)
        logging.info(f"Saving images to: {naver_image_dir}")
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            for idx, row in df.iterrows():
                try:
                    if '네이버 이미지' not in row or pd.isna(row['네이버 이미지']):
                        continue
                        
                    img_data = row['네이버 이미지']
                    url = None
                    local_path = None
                    
                    # Get URL and local path from dictionary or string
                    if isinstance(img_data, dict):
                        url = img_data.get('url')
                        local_path = img_data.get('local_path')
                    elif isinstance(img_data, str) and img_data.startswith('http'):
                        url = img_data
                    
                    # Skip if we already have a valid local path
                    if local_path and os.path.exists(local_path) and os.path.getsize(local_path) > 1000:
                        logging.debug(f"Image already exists at {local_path}, skipping download")
                        continue
                    
                    if url and 'shopping-phinf.pstatic.net' in url:
                        # Create filename from URL
                        filename = f"naver_{hashlib.md5(url.encode()).hexdigest()[:10]}.jpg"
                        filepath = os.path.join(naver_image_dir, filename)
                        
                        # Skip if file already exists and is valid
                        if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
                            logging.debug(f"Image already exists at {filepath}, skipping download")
                            # Update DataFrame with existing file
                            if isinstance(img_data, dict):
                                img_data['local_path'] = filepath
                                df.at[idx, '네이버 이미지'] = img_data
                            else:
                                df.at[idx, '네이버 이미지'] = {
                                    'url': url,
                                    'local_path': filepath,
                                    'source': 'naver'
                                }
                            continue
                            
                        task = asyncio.create_task(download_image(session, url, filepath))
                        tasks.append((idx, url, filepath, task))
                except Exception as e:
                    logging.error(f"Error processing row {idx}: {e}")
                    continue
            
            if tasks:
                results = await asyncio.gather(*(t[3] for t in tasks), return_exceptions=True)
                for (idx, url, filepath, _), success in zip(tasks, results):
                    if success:
                        current_data = df.at[idx, '네이버 이미지']
                        if isinstance(current_data, dict):
                            current_data['local_path'] = filepath
                            df.at[idx, '네이버 이미지'] = current_data
                        else:
                            df.at[idx, '네이버 이미지'] = {
                                'url': url,
                                'local_path': filepath,
                                'source': 'naver'
                            }
        
        # Count successful downloads
        downloaded = sum(1 for _, row in df.iterrows() 
                        if isinstance(row.get('네이버 이미지'), dict) 
                        and row['네이버 이미지'].get('local_path')
                        and os.path.exists(row['네이버 이미지']['local_path']))
        
        logging.info(f"Successfully downloaded {downloaded} Naver images")
        
    except Exception as e:
        logging.error(f"Error in image download process: {e}")
    
    return df

def ensure_naver_local_images(df: pd.DataFrame, naver_image_dir: str) -> pd.DataFrame:
    """Wrapper function to run async code"""
    try:
        # Check if there's a running event loop
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
        # Create a new event loop if the current one is closed
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
        # Run the async function in the event loop
        if loop.is_running():
            # If loop is already running, create a new one in a separate thread
            import threading
            def run_async():
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                return new_loop.run_until_complete(ensure_naver_local_images_async(df, naver_image_dir))
                
            thread = threading.Thread(target=run_async)
            thread.start()
            thread.join()
        else:
            # If loop is not running, use it directly
            return loop.run_until_complete(ensure_naver_local_images_async(df, naver_image_dir))
            
        return df
        
    except Exception as e:
        logging.error(f"Error in ensure_naver_local_images: {e}")
        return df

async def handle_captcha(page: Page) -> bool:
    """캡차 처리 함수"""
    try:
        captcha_selectors = [
            'form#captcha_form', 
            'img[alt*="captcha"]', 
            'div.captcha_wrap',
            'input[name="captchaBotKey"]',
            'div[class*="captcha"]'
        ]
        
        for selector in captcha_selectors:
            if await page.query_selector(selector):
                logger.info("CAPTCHA detected, waiting and retrying...")
                
                # 브라우저 재시작
                context = page.context
                browser = context.browser
                
                # 새 컨텍스트 생성
                new_context = await browser.new_context(
                    viewport={"width": 1366, "height": 768},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                    java_script_enabled=True
                )
                
                # 랜덤 대기
                await asyncio.sleep(random.uniform(3.0, 5.0))
                
                # 새 페이지로 다시 시도
                new_page = await new_context.new_page()
                await new_page.goto(page.url, wait_until='networkidle')
                
                # 캡차가 여전히 있는지 확인
                still_has_captcha = False
                for selector in captcha_selectors:
                    if await new_page.query_selector(selector):
                        still_has_captcha = True
                        break
                
                if not still_has_captcha:
                    return True
                
                # 이전 컨텍스트 정리
                await context.close()
                return False
                
        return True
    except Exception as e:
        logger.error(f"Error handling CAPTCHA: {e}")
        return False

if __name__ == "__main__":
    sys.exit(main()) 