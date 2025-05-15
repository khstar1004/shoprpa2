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
    
    # 크롤링 로직과 동일하게 최소 수량 확인
    min_quantity = min(quantities)
    logger.info(f"테이블 최소 수량: {min_quantity}개")
    
    # 주문 수량이 최소 수량보다 작은 경우 최소 수량의 가격 적용
    if target_quantity < min_quantity:
        logger.info(f"주문 수량({target_quantity})이 최소 수량({min_quantity})보다 작습니다. 최소 수량의 가격을 적용합니다.")
        price_info = qty_prices[min_quantity]
        return (
            price_info.get('price', 0),
            price_info.get('price_with_vat', 0),
            False,
            min_quantity,
            f"최소 수량({min_quantity}) 가격 적용"
        )
    
    # 정확히 일치하는 수량이 있는 경우
    if target_quantity in quantities:
        logger.info(f"수량 {target_quantity}개 정확히 일치: {qty_prices[target_quantity].get('price', 0)}원")
        price_info = qty_prices[target_quantity]
        return (
            price_info.get('price', 0),
            price_info.get('price_with_vat', 0),
            True,
            target_quantity,
            "정확히 일치하는 수량"
        )
    
    # 주문 수량보다 큰 수량 중 가장 작은 수량 찾기
    larger_quantities = [qty for qty in quantities if qty > target_quantity]
    
    if larger_quantities:
        next_tier = min(larger_quantities)
        logger.info(f"주문 수량({target_quantity})보다 큰 다음 티어({next_tier}) 가격 적용: {qty_prices[next_tier].get('price', 0)}원")
        price_info = qty_prices[next_tier]
        return (
            price_info.get('price', 0),
            price_info.get('price_with_vat', 0),
            False,
            next_tier,
            f"다음 티어 가격 적용"
        )
    
    # 주문 수량이 모든 티어보다 큰 경우 (가장 큰 티어 적용)
    max_quantity = max(quantities)
    logger.info(f"주문 수량({target_quantity})이 최대 티어보다 큽니다. 최대 티어({max_quantity}) 가격 적용: {qty_prices[max_quantity].get('price', 0)}원")
    price_info = qty_prices[max_quantity]
    return (
        price_info.get('price', 0),
        price_info.get('price_with_vat', 0),
        False,
        max_quantity,
        f"최대 티어 가격 적용"
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
    It should primarily look for '고려기프트_실제가격티어'.
    
    Args:
        row: DataFrame row
        
    Returns:
        dict: Dictionary of quantity prices or None if not found or parse error
    """
    # Dedicated column for actual crawled price tiers
    actual_tiers_col_name = '고려기프트_실제가격티어'
    row_identifier = f"Row {row.name if hasattr(row, 'name') else 'N/A'} (Product: '{row.get("상품명", "Unknown")}')"

    if actual_tiers_col_name in row and pd.notna(row[actual_tiers_col_name]) and row[actual_tiers_col_name] != '-':
        try:
            data_str = row[actual_tiers_col_name]
            if isinstance(data_str, str):
                # Ensure keys are integers after parsing
                parsed_data = ast.literal_eval(data_str)
                if isinstance(parsed_data, dict):
                    # Convert string keys to int keys if necessary, as ast.literal_eval might keep them as strings
                    # e.g., {'100': {...}} -> {100: {...}}
                    int_key_parsed_data = {}
                    all_keys_valid = True
                    for k, v in parsed_data.items():
                        try:
                            int_key = int(k)
                            int_key_parsed_data[int_key] = v
                        except ValueError:
                            logger.warning(f"{row_identifier}: Invalid key '{k}' in {actual_tiers_col_name} data. Skipping this key.")
                            all_keys_valid = False # Mark if any key is problematic
                    
                    if not int_key_parsed_data: # If all keys were invalid or dict was empty
                        logger.warning(f"{row_identifier}: No valid integer keys found in {actual_tiers_col_name} after parsing: {data_str}")
                        return None
                    
                    logger.info(f"{row_identifier}: Successfully parsed quantity_prices from '{actual_tiers_col_name}'")
                    return int_key_parsed_data
                else:
                    logger.warning(f"{row_identifier}: Parsed data from '{actual_tiers_col_name}' is not a dict: {type(parsed_data)}")    
            elif isinstance(data_str, dict): # If it's already a dict (e.g. if DataFrame wasn't purely from Excel)
                logger.info(f"{row_identifier}: Directly using dict quantity_prices from '{actual_tiers_col_name}'")
                # Ensure keys are integers for consistency
                int_key_data = {int(k): v for k, v in data_str.items() if str(k).isdigit()} # simple conversion for already dict case
                if not int_key_data:
                     logger.warning(f"{row_identifier}: No valid integer keys in already dict data from '{actual_tiers_col_name}': {data_str}")
                     return None
                return int_key_data
        except Exception as e:
            logger.warning(f"{row_identifier}: Error parsing quantity_prices from '{actual_tiers_col_name}': {e}. Value: {str(row.get(actual_tiers_col_name, ''))[:200]}")
            return None
    else:
        # Check if there is a Kogift link, if so, it *should* have tier data.
        kogift_link_columns = ['고려기프트 상품링크', '고려 링크', '고려기프트링크', '고려 상품링크']
        has_kogift_link = False
        for col_variant in kogift_link_columns:
            actual_col_name = None
            if col_variant in row.index:
                 actual_col_name = col_variant
            elif col_variant.replace(" ", "") in row.index: # try with spaces removed
                 actual_col_name = col_variant.replace(" ", "")
            
            if actual_col_name and pd.notna(row[actual_col_name]) and row[actual_col_name] != '-':
                has_kogift_link = True
                break
        
        if has_kogift_link:
            logger.warning(f"{row_identifier}: Column '{actual_tiers_col_name}' is missing or empty, but a Kogift link exists. Crawled tier data might be missing.")
        else:
            logger.debug(f"{row_identifier}: Column '{actual_tiers_col_name}' is missing or empty, and no Kogift link found. Likely not a Kogift item or no data.")

    # Fallback to trying other columns if the main one isn't there - this is a legacy check and should ideally not be needed.
    possible_data_columns = [
        'kogift_data', 'kogift_price_data', 'kogift_product_data', 
        'quantity_prices', 'kogift_quantity_prices', # temp_kogift_col is already passed
        '고려기프트_데이터', '고려기프트_가격정보', '고려기프트_수량가격'
    ]

    for col in possible_data_columns:
        if col in row and pd.notna(row[col]) and row[col] != '-':
            data = parse_complex_value(row[col])
            if isinstance(data, dict):
                # Check for quantity_prices within this potentially complex dict
                if 'quantity_prices' in data and isinstance(data['quantity_prices'], dict):
                    logger.info(f"{row_identifier}: Found quantity_prices in nested structure under column '{col}'")
                    # Ensure keys are integers
                    return {int(k): v for k, v in data['quantity_prices'].items() if str(k).isdigit()}
                # Sometimes the dict itself is the quantity_prices table
                # Check if keys look like quantities and values look like price info
                is_likely_tier_table = True
                temp_tier_table = {}
                if not data: is_likely_tier_table = False
                for k, v_dict in data.items():
                    if not (str(k).isdigit() and isinstance(v_dict, dict) and ('price' in v_dict or 'price_with_vat' in v_dict)):
                        is_likely_tier_table = False
                        break
                    temp_tier_table[int(k)] = v_dict
                if is_likely_tier_table:
                    logger.info(f"{row_identifier}: Found quantity_prices directly in column '{col}'")
                    return temp_tier_table

    logger.warning(f"{row_identifier}: Could not find or parse any valid quantity-price tier data from any known column.")
    return None # Explicitly return None if no valid data is found

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
        
        # 고려기프트 이미지 디렉토리 경로 확인
        kogift_image_dir = os.path.join('C:', 'RPA', 'Image', 'Main', 'Kogift')
        if not os.path.exists(kogift_image_dir):
            logger.warning(f"고려기프트 이미지 디렉토리가 없습니다: {kogift_image_dir}")
            kogift_image_dir = None
        
        # Map column names (accounting for variations in column names)
        column_mapping = {
            '기본수량(1)': ['기본수량(1)', '기본수량', '수량', '본사 기본수량'],
            '판매단가(V포함)': ['판매단가(V포함)', '판매단가1(VAT포함)'],
            '고려기프트 상품링크': ['고려기프트 상품링크', '고려기프트상품링크', '고려기프트 링크', '고려 링크'],
            '기본수량(2)': ['기본수량(2)', '고려 기본수량', '고려기프트 기본수량'],
            '판매가(V포함)(2)': ['판매가(V포함)(2)', '판매단가(V포함)(2)', '고려 판매가(V포함)', '고려기프트 판매가', '판매단가2(VAT포함)'],
            '가격차이(2)': ['가격차이(2)', '고려 가격차이'],
            '가격차이(2)(%)': ['가격차이(2)(%)', '고려 가격차이(%)', '고려 가격 차이(%)'],
            '고려기프트 이미지': ['고려기프트 이미지', '고려기프트이미지', '고려 이미지', 'kogift_image']
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
            'upload': ['기본수량(1)', '고려기프트 상품링크']  # 업로드 파일에서도 동일한 칼럼 찾기 (매핑된 이름)
        }
        
        # Get required columns for this file type
        required_columns = required_columns_by_type.get(file_type, ['기본수량(1)', '고려기프트 상품링크'])
        
        # Check for required columns
        missing_columns = [col for col in required_columns if col not in columns_found]
        if missing_columns:
            # 파일 타입에 따라 다른 경고 메시지 표시
            if file_type == 'result':
                logger.warning(f"result 파일에서 필요한 칼럼이 없습니다: {missing_columns}. 가능한 칼럼으로 진행합니다.")
            else:
                logger.warning(f"upload 파일에서 필요한 칼럼이 없습니다: {missing_columns}. 가능한 칼럼으로 진행합니다.")
        
        # Find column indices for updating (1-indexed for openpyxl)
        column_indices = {}
        for col_idx, cell in enumerate(sheet[1], 1):  # 1-indexed columns
            column_indices[cell.value] = col_idx
        
        # Log found column indices
        logger.info(f"Found column indices: {column_indices}")
        
        # Map the actual column names in the Excel file to our expected column names
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
        base_price_col = columns_found.get('판매단가(V포함)')
        kogift_link_col = columns_found.get('고려기프트 상품링크')
        quantity2_col = columns_found.get('기본수량(2)')
        price2_col = columns_found.get('판매가(V포함)(2)')
        price_diff_col = columns_found.get('가격차이(2)')
        price_diff_pct_col = columns_found.get('가격차이(2)(%)')
        kogift_image_col = columns_found.get('고려기프트 이미지')
        
        # 칼럼을 찾지 못한 경우 로그 남기기
        if not quantity_col:
            logger.warning("기본수량(1) 칼럼을 찾을 수 없습니다.")
        if not kogift_link_col:
            logger.warning("고려기프트 상품링크 칼럼을 찾을 수 없습니다.")
        if not price2_col:
            logger.warning("판매가(V포함)(2) 칼럼을 찾을 수 없습니다. 가격 정보를 업데이트할 수 없습니다.")
        
        # Process each row that has Kogift data
        update_count = 0
        price_diffs_updated = 0
        wrong_image_count = 0
        
        # 특별히 관심 있는 수량 값들 추적
        small_quantity_handling = {}  # 수량이 작은 행 처리 결과 추적
        
        # 행 별로 처리
        for idx, row in df.iterrows():
            # Kogift 링크가 있고 기본수량 칼럼이 있는 행만 처리
            has_kogift_link = False
            if kogift_link_col and kogift_link_col in row:
                has_kogift_link = not pd.isna(row[kogift_link_col]) and row[kogift_link_col] != '-'
            
            if not has_kogift_link:
                continue
            
            # 이미지 데이터 검증
            if kogift_image_col and kogift_image_col in row:
                image_data = parse_complex_value(row[kogift_image_col])
                if isinstance(image_data, dict):
                    local_path = image_data.get('local_path') or image_data.get('image_path')
                    if local_path and isinstance(local_path, str):
                        # 이미지가 올바른 디렉토리에 있는지 확인
                        if kogift_image_dir and not local_path.replace('\\', '/').startswith(kogift_image_dir.replace('\\', '/')):
                            wrong_image_count += 1
                            logger.warning(f"잘못된 고려기프트 이미지 경로 (행 {idx+1}): {local_path}")
                            
                            # 이미지 데이터 초기화
                            xl_row = idx + 2  # Excel은 1-based indexing이고 헤더가 있으므로 +2
                            kogift_image_idx = real_column_indices.get('고려기프트 이미지')
                            if kogift_image_idx:
                                sheet.cell(row=xl_row, column=kogift_image_idx).value = '-'
                            continue
            
            # 기본수량 확인
            base_quantity = None
            if quantity_col and quantity_col in row and pd.notna(row[quantity_col]):
                try:
                    # 수량을 정수로 변환
                    base_quantity = int(row[quantity_col])
                    logger.info(f"Processing row {idx+1}: Product name: {row.get('상품명', 'Unknown')} with quantity {base_quantity}")
                except (ValueError, TypeError):
                    logger.warning(f"Invalid base quantity in row {idx+1}: {row.get(quantity_col)}")
                    continue
            else:
                logger.debug(f"No base quantity found for row {idx+1}")
                continue
            
            # 특별히 주시하는 경우: 수량이 100과 같이 작은 경우
            is_special_case = base_quantity < 200
            
            # 크롤링된 수량-가격 정보 추출 시도
            quantity_prices = extract_quantity_prices_from_row(row.copy()) # Pass a copy to avoid SettingWithCopyWarning if row is a slice
            
            if not quantity_prices:
                logger.warning(f"Row {idx+1} (Product: '{row.get("상품명", "Unknown")}'): No valid crawled quantity-price data found. Skipping price update for this row.")
                continue
            
            # 로그 출력
            if quantity_prices:
                logger.info(f"Row {idx+1}: 사용 가능한 수량 티어: {sorted(quantity_prices.keys())}")
            else:
                logger.warning(f"Row {idx+1}: 사용 가능한 수량-가격 정보 없음")
                continue
            
            if is_special_case:
                logger.info(f"!! 특별 케이스 발견 !! - Row {idx+1}: 수량이 {base_quantity}개인 경우 처리")
                
                # 이전 가격 정보 저장 (수정 확인용)
                old_price = None
                price2_idx = real_column_indices.get('판매가(V포함)(2)')
                if price2_idx:
                    old_cell = sheet.cell(row=idx+2, column=price2_idx)
                    old_price = old_cell.value
                    logger.info(f"   현재 가격: {old_price}")
            
            # 적절한 가격 티어 찾기
            price, price_with_vat, exact_match, actual_quantity, note = find_appropriate_price(
                quantity_prices, base_quantity
            )
            
            if is_special_case:
                # 특별 케이스인 경우 처리 결과 저장
                small_quantity_handling[idx] = {
                    'row': idx+1,
                    'product_name': row.get('상품명', 'Unknown'),
                    'base_quantity': base_quantity,
                    'available_tiers': sorted(quantity_prices.keys()),
                    'selected_tier': actual_quantity,
                    'price': price,
                    'price_with_vat': price_with_vat,
                    'note': note,
                    'old_price': old_price
                }
                logger.info(f"   해결 결과: 티어 {actual_quantity} 선택, 가격 {price}원 (부가세 포함: {price_with_vat}원)")
                logger.info(f"   처리 내용: {note}")
            
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
                    current_price = sheet.cell(row=xl_row, column=price2_idx).value
                    sheet.cell(row=xl_row, column=price2_idx).value = price_with_vat
                    logger.info(f"Row {idx+1}: 가격 업데이트: {current_price} -> {price_with_vat}")
                
                # Update price difference if possible
                price_diff_idx = real_column_indices.get('가격차이(2)')
                price_diff_pct_idx = real_column_indices.get('가격차이(2)(%)')
                
                # 본사 가격 찾기 (판매단가(V포함) 또는 판매단가1(VAT포함) 칼럼 이름 사용)
                base_price = None
                base_price_col_name = columns_found.get('판매단가(V포함)')
                
                if base_price_col_name and base_price_col_name in row and pd.notna(row[base_price_col_name]):
                    try:
                        base_price = float(row[base_price_col_name])
                    except (ValueError, TypeError):
                        logger.warning(f"행 {idx+1}: 본사 가격 '{row[base_price_col_name]}'를 숫자로 변환할 수 없습니다.")
                
                # 가격 차이 계산 및 업데이트
                if price_diff_idx and base_price is not None:
                    try:
                        price_diff = price_with_vat - base_price
                        sheet.cell(row=xl_row, column=price_diff_idx).value = price_diff
                        
                        # 음수 가격 차이일 경우 빨간색 배경 적용
                        if price_diff < 0:
                            sheet.cell(row=xl_row, column=price_diff_idx).fill = PatternFill(
                                start_color='FFC7CE', end_color='FFC7CE', fill_type='solid'
                            )
                        
                        # 퍼센트 가격 차이 계산 및 업데이트
                        if price_diff_pct_idx and base_price != 0:
                            pct_diff = (price_diff / base_price) * 100
                            sheet.cell(row=xl_row, column=price_diff_pct_idx).value = round(pct_diff, 1)
                            
                            # 음수 퍼센트 가격 차이일 경우 빨간색 배경 적용
                            if pct_diff < 0:
                                sheet.cell(row=xl_row, column=price_diff_pct_idx).fill = PatternFill(
                                    start_color='FFC7CE', end_color='FFC7CE', fill_type='solid'
                                )
                        
                        price_diffs_updated += 1
                        logger.debug(f"행 {idx+1}: 가격차이={price_diff:.1f}, 가격차이(%)={pct_diff:.1f}% 업데이트 완료")
                    except (ValueError, TypeError, NameError) as e:
                        logger.debug(f"행 {idx+1}: 가격차이 계산 중 오류: {e}")
                        
                # 특정 열에 대한 인덱스를 찾지 못한 경우 로그
                if not price_diff_idx and base_price is not None:
                    logger.debug(f"행 {idx+1}: 가격차이 열을 찾을 수 없어 가격차이 업데이트 불가")
                if not price_diff_pct_idx and base_price is not None:
                    logger.debug(f"행 {idx+1}: 가격차이(%) 열을 찾을 수 없어 퍼센트 가격차이 업데이트 불가")
                
                update_count += 1
                logger.debug(f"Updated row {idx+1}: Quantity {base_quantity}, Price {price_with_vat}, Tier {actual_quantity}")
        
        # 특별 케이스 처리 결과 요약
        if small_quantity_handling:
            logger.info("\n===== 적은 수량 특별 처리 결과 요약 =====")
            for case_idx, case_data in small_quantity_handling.items():
                logger.info(f"행 #{case_data['row']}: {case_data['product_name']}")
                logger.info(f"  수량: {case_data['base_quantity']}, 가능한 티어: {case_data['available_tiers']}")
                logger.info(f"  기존 가격: {case_data['old_price']} -> 새 가격: {case_data['price_with_vat']} (티어 {case_data['selected_tier']})")
                logger.info(f"  비고: {case_data['note']}")
                logger.info("-" * 40)
            logger.info("========================================")
        
        # Save the modified workbook
        workbook.save(output_file)
        logger.info(f"성공적으로 {update_count}개 행의 가격 정보가 수정되었습니다. (가격차이 계산: {price_diffs_updated}개)")
        logger.info(f"수정된 엑셀 파일 저장 경로: {output_file}")
        
        # 최종 확인 로그 추가
        if update_count == 0:
            logger.warning("!! 주의 !! - 업데이트된 행이 없습니다. 칼럼 매핑을 확인하세요.")
        
        # 문제가 발생한 경우 알림
        missing_column_list = []
        for key_col in ['기본수량(1)', '판매단가(V포함)', '고려기프트 상품링크', '판매가(V포함)(2)']:
            if key_col not in columns_found:
                missing_column_list.append(key_col)
        
        if missing_column_list:
            logger.warning(f"!! 주의 !! - 일부 중요 칼럼을 찾지 못했습니다: {missing_column_list}")
            logger.warning("이로 인해 일부 행이 처리되지 않았을 수 있습니다.")
        
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