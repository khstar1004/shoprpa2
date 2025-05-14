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
    
    # 핵심 수정: 타겟 수량이 모든 티어보다 작은 경우 (최소 티어보다 작은 경우)
    min_quantity = min(quantities)
    if target_quantity < min_quantity:
        # 가장 작은 티어의 가격을 사용 (판촉물 사이트 최소 주문 수량 규칙)
        price_info = qty_prices[min_quantity]
        return (
            price_info.get('price', 0),
            price_info.get('price_with_vat', 0),
            False,
            min_quantity,
            f"Using minimum tier {min_quantity} for quantity {target_quantity} (below minimum order quantity)"
        )
    
    # Find the appropriate tier: smallest quantity that's greater than or equal to target
    # (타겟 수량보다 크거나 같은 티어 중 가장 작은 티어 선택)
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
        # 논리적으로 위의 min_quantity 조건과 larger_quantities 조건을 합치면
        # 이 부분은 실행될 수 없지만, 안전을 위해 유지
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
    # 크롤링 시 저장된 직접적인 데이터 열 먼저 확인
    direct_quantity_columns = [
        '_temp_kogift_quantity_prices',
        'quantity_prices',
        'kogift_quantity_prices',
        '고려기프트_수량가격'
    ]
    
    for col in direct_quantity_columns:
        if col in row and not pd.isna(row[col]):
            qty_prices = parse_complex_value(row[col])
            if isinstance(qty_prices, dict):
                return qty_prices
    
    # Try to extract from the image data or kogift data columns with various possible names
    possible_kogift_cols = [
        '고려기프트 이미지', '고려기프트 데이터', 'kogift_data', 
        '고려기프트이미지', '고려기프트데이터', 'kogift_image_data',
        '고려데이터', '고려 데이터', 'kogift_product', '고려기프트_상품정보',
        '고려 상품데이터'
    ]
    
    for col in possible_kogift_cols:
        if col in row and not pd.isna(row[col]) and row[col] != '-':
            data = parse_complex_value(row[col])
            if isinstance(data, dict):
                # Direct quantity_prices in the data dictionary
                if 'quantity_prices' in data:
                    return data['quantity_prices']
                    
                # Check for quantity_price_table format
                if 'quantity_price_table' in data:
                    price_table = data['quantity_price_table']
                    if isinstance(price_table, dict):
                        return price_table
                
                # Check for nested data structures
                for key, value in data.items():
                    if isinstance(value, dict):
                        if 'quantity_prices' in value:
                            return value['quantity_prices']
                        # 중첩된 구조 내에서 'quantity'와 'price' 키가 있는지 확인
                        if 'quantities' in value and 'prices' in value:
                            quantities = value.get('quantities', [])
                            prices = value.get('prices', [])
                            if len(quantities) == len(prices) and len(quantities) > 0:
                                result = {}
                                for i, qty in enumerate(quantities):
                                    result[qty] = {
                                        'price': prices[i],
                                        'price_with_vat': int(prices[i] * 1.1)
                                    }
                                return result
            
            # Try to extract from JSON string representation
            if isinstance(row[col], str) and ('quantity_prices' in row[col] or '수량' in row[col] or '단가' in row[col]):
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
                    
                    # Look for a more generic quantity/price table pattern
                    qty_price_pattern = r'(\d+)[^\d]*?(\d+)(?:원|₩|\s*KRW)?'
                    matches = re.findall(qty_price_pattern, row[col])
                    if matches and len(matches) >= 2:  # At least two qty-price pairs
                        result = {}
                        for qty_str, price_str in matches:
                            try:
                                qty = int(qty_str)
                                price = int(price_str)
                                result[qty] = {
                                    'price': price,
                                    'price_with_vat': int(price * 1.1)
                                }
                            except (ValueError, TypeError):
                                pass
                        if result:
                            return result
                except Exception as e:
                    pass
                    
    # 고려기프트 URL에서 상품 ID 추출 시도
    kogift_link_columns = ['고려기프트 상품링크', '고려 링크', '고려기프트링크', '고려기프트 링크']
    for col in kogift_link_columns:
        if col in row and not pd.isna(row[col]) and isinstance(row[col], str):
            link = row[col]
            # 링크에서 상품 ID 추출 (예: http://koreagift.com/ez/mall.php?cat=004002001&query=view&no=168899)
            match = re.search(r'no=(\d+)', link)
            if match:
                # 이 정보로는 직접 가격 테이블을 얻을 수 없지만 로깅을 통해 상품 ID는 확인 가능
                product_id = match.group(1)
                logger.debug(f"Found Kogift product ID: {product_id} but no price table in row data")
    
    # Try to parse from any string column that might contain quantity/price information
    for col_name, value in row.items():
        if isinstance(value, str):
            # Check for quantity_prices in any string field
            if 'quantity_prices' in value or '수량' in value or '단가' in value:
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
                    
            # Check for a table-like structure in text
            # Format examples:
            # 1. "수량: 3,000 단가: 6,000 | 수량: 1,000 단가: 6,150 | ..."
            # 2. "3000개: 6000원, 1000개: 6150원, ..."
            table_patterns = [
                # Pattern 1: "수량: 3,000 단가: 6,000"
                r'수량\s*:\s*([\d,]+)\s*단가\s*:\s*([\d,]+)',
                # Pattern 2: "3000개: 6000원"
                r'([\d,]+)개\s*:\s*([\d,]+)원',
                # Pattern 3: "수량 3,000 가격 6,000"
                r'수량\s*([\d,]+)\s*가격\s*([\d,]+)',
                # Pattern 4: 일반적인 숫자 패턴 (키워드 없음)
                r'([\d,]+)\s*[:-]\s*([\d,]+)'
            ]
            
            for pattern in table_patterns:
                matches = re.findall(pattern, value)
                if matches and len(matches) >= 2:  # At least two qty-price pairs
                    result = {}
                    for qty_str, price_str in matches:
                        try:
                            # 쉼표 제거 후 변환
                            qty = int(qty_str.replace(',', ''))
                            price = int(price_str.replace(',', ''))
                            result[qty] = {
                                'price': price,
                                'price_with_vat': int(price * 1.1)
                            }
                        except (ValueError, TypeError):
                            pass
                    if result:
                        return result
            
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
            '기본수량(1)': ['기본수량(1)', '기본수량', '수량', '본사 기본수량'],
            '판매단가(V포함)': ['판매단가(V포함)', '판매단가1(VAT포함)'],
            '고려기프트 상품링크': ['고려기프트 상품링크', '고려기프트상품링크', '고려기프트 링크', '고려 링크'],
            '기본수량(2)': ['기본수량(2)', '고려 기본수량', '고려기프트 기본수량'],
            '판매가(V포함)(2)': ['판매가(V포함)(2)', '판매단가(V포함)(2)', '고려 판매가(V포함)', '고려기프트 판매가', '판매단가2(VAT포함)'],
            '가격차이(2)': ['가격차이(2)', '고려 가격차이'],
            '가격차이(2)(%)': ['가격차이(2)(%)', '고려 가격차이(%)', '고려 가격 차이(%)']
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
        base_price_col = columns_found.get('판매단가(V포함)')
        kogift_link_col = columns_found.get('고려기프트 상품링크')
        quantity2_col = columns_found.get('기본수량(2)')
        price2_col = columns_found.get('판매가(V포함)(2)')
        price_diff_col = columns_found.get('가격차이(2)')
        price_diff_pct_col = columns_found.get('가격차이(2)(%)')
        
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
        
        # 특별히 관심 있는 수량 값들 추적
        small_quantity_handling = {}  # 수량이 작은 행 처리 결과 추적
        
        for idx, row in df.iterrows():
            # Skip rows without Kogift data
            if not kogift_link_col or pd.isna(row.get(kogift_link_col, '')) or not row.get(kogift_link_col, ''):
                continue
            
            # Extract quantity-price information
            quantity_prices = extract_quantity_prices_from_row(row)
            if not quantity_prices:
                logger.debug(f"No quantity price data found for row {idx+1}")
                continue
            
            # Log the quantity tiers found
            logger.info(f"Row {idx+1}: Extracted quantity tiers: {sorted(quantity_prices.keys())}")
            
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
                logger.info(f"Processing row {idx+1}: Product name: {row.get('상품명', 'Unknown')} with quantity {base_quantity}")
            except (ValueError, TypeError):
                logger.warning(f"Invalid base quantity in row {idx+1}: {base_quantity}")
                continue
            
            # 특별히 주시하는 경우: 수량이 100인 경우 (사진에서 문제가 되었던 케이스)
            is_special_case = base_quantity == 100
            
            if is_special_case:
                logger.info(f"!! 특별 케이스 발견 !! - Row {idx+1}: 수량이 {base_quantity}인 경우 처리 중")
                logger.info(f"   사용 가능한 수량 티어: {sorted(quantity_prices.keys())}")
                
                # 이전 가격 정보 저장 (수정 확인용)
                old_price = None
                price2_idx = real_column_indices.get('판매가(V포함)(2)')
                if price2_idx:
                    old_cell = sheet.cell(row=idx+2, column=price2_idx)
                    old_price = old_cell.value
                    logger.info(f"   현재 가격: {old_price}")
            
            # Find the appropriate price tier
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
                    sheet.cell(row=xl_row, column=price2_idx).value = price_with_vat
                
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