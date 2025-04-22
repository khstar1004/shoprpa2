import os
import glob
import logging
import pandas as pd
from datetime import datetime
import configparser
from excel_utils import create_final_output_excel, FINAL_COLUMN_ORDER
import re
import time
from typing import Optional, Tuple, Dict, List

def process_input_file(config: configparser.ConfigParser) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """Processes the main input Excel file, reading config with ConfigParser."""
    try:
        input_dir = config.get('Paths', 'input_dir')
    except configparser.Error as e:
        logging.error(f"Error reading configuration for input processing: {e}. Cannot proceed.")
        return None, None
        
    logging.info(f"Checking for input file in {input_dir}")
    start_time = time.time()

    try:
        excel_files = glob.glob(os.path.join(input_dir, '*.xlsx'))
        excel_files = [f for f in excel_files if not os.path.basename(f).startswith('~')]

        if not excel_files:
            logging.warning(f"No Excel (.xlsx) file found in {input_dir}.")
            return None, None

        # Process only the first found Excel file
        input_file = excel_files[0]
        input_filename = os.path.basename(input_file)
        logging.info(f"Processing input file: {input_file}")

        required_cols = ['Code', '상품명', '본사상품링크', '구분']

        # Read the entire Excel file at once
        df = pd.read_excel(input_file, sheet_name=0)
        logging.info(f"Read {len(df)} rows from '{input_filename}'")

        # Check for required columns
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logging.error(f"Input file '{input_filename}' missing columns: {missing_cols}.")
            return None, input_filename

        read_time = time.time() - start_time
        logging.info(f"Read {len(df)} rows from '{input_filename}' in {read_time:.2f} sec.")
        return df, input_filename

    except FileNotFoundError:
        logging.error(f"Input file {input_file} not found during read attempt.")
        return None, None
    except Exception as e:
        logging.error(f"Error reading Excel '{input_file}': {e}", exc_info=True)
        return None, input_filename

def filter_results(df: pd.DataFrame, config: configparser.ConfigParser) -> pd.DataFrame:
    """결과 데이터프레임 필터링"""
    if df.empty:
        return df
        
    # 가격 차이 필터링
    price_diff_threshold = config.getfloat('PriceDifference', 'threshold', fallback=0.1)
    
    # 고려 가격 차이 필터링
    if '고려_가격차이' in df.columns:
        df = df[df['고려_가격차이'].abs() <= price_diff_threshold]
        
    # 네이버 가격 차이 필터링
    if '네이버_가격차이' in df.columns:
        df = df[df['네이버_가격차이'].abs() <= price_diff_threshold]
        
    # 매칭 품질 필터링
    quality_threshold = config.getfloat('MatchQualityThresholds', 'low_quality', fallback=0.50)
    
    # 고려 매칭 품질 필터링
    if '고려_매칭품질' in df.columns:
        df = df[df['고려_매칭품질'].isin(['high', 'medium', 'low'])]
        
    # 네이버 매칭 품질 필터링
    if '네이버_매칭품질' in df.columns:
        df = df[df['네이버_매칭품질'].isin(['high', 'medium', 'low'])]
        
    return df

# Note: save_and_format_output and format_output_file were likely replaced by
# create_final_output_excel in excel_utils.py. We remove them here to avoid duplication.
# If they are still needed, they should also be updated to use ConfigParser.

# def save_and_format_output(df, input_filename_base, config: configparser.ConfigParser, progress_queue=None):
#     """(DEPRECATED - Functionality moved to excel_utils.create_final_output_excel)
#        Saves the final DataFrame to an Excel file and applies formatting.
#     """
#     try:
#         output_dir = config.get('Paths', 'output_dir')
#     except configparser.Error as e:
#          logging.error(f"Cannot save output: Error reading output_dir from config: {e}")
#          return None
#          
#     # ... (rest of the saving logic, using config parser where needed) ...
#     # Make sure to call styling/hyperlink functions from excel_utils which should use openpyxl
#     pass

# def format_output_file(file_path, config: configparser.ConfigParser, progress_queue=None):
#     """(DEPRECATED - Functionality moved to excel_utils.create_final_output_excel)
#        Applies final formatting using external utility function.
#     """
#     pass 

def format_product_data_for_output(input_df: pd.DataFrame, kogift_results: Dict[str, List[Dict]], naver_results: Dict[str, List[Dict]]) -> pd.DataFrame:
    """
    Formats and combines original input data with KoGift and Naver crawl results.
    Calculates price differences and prepares the DataFrame for Excel output.
    """
    if input_df is None or input_df.empty:
        logging.warning("Input DataFrame is empty. Returning empty DataFrame with required columns.")
        return pd.DataFrame(columns=FINAL_COLUMN_ORDER)

    output_df = input_df.copy()
    logging.info(f"Starting data formatting. Input rows: {len(output_df)}")

    # Ensure all required columns exist
    required_columns = [
        '구분', '담당자', '업체명', '업체코드', 'Code', 
        '중분류카테고리', '상품명', '기본수량(1)', '판매단가(V포함)', '본사상품링크'
    ]
    
    # Check if all required columns exist
    missing_columns = [col for col in required_columns if col not in output_df.columns]
    if missing_columns:
        logging.error(f"Missing required columns in input DataFrame: {missing_columns}")
        raise ValueError(f"Input DataFrame is missing required columns: {missing_columns}")

    # Initialize optional columns with '-' if they don't exist
    optional_columns = [
        '기본수량(2)', '판매가(V포함)(2)', '판매단가(V포함)(2)', 
        '가격차이(2)', '가격차이(2)(%)', '고려기프트 상품링크',
        '기본수량(3)', '판매단가(V포함)(3)', '가격차이(3)', 
        '가격차이(3)(%)', '공급사명', '네이버 쇼핑 링크', '공급사 상품링크'
    ]
    
    for col in optional_columns:
        if col not in output_df.columns:
            output_df[col] = '-'

    # --- Process Each Row --- 
    for idx, row in output_df.iterrows():
        product_name = row.get('상품명')
        if not product_name or pd.isna(product_name):
            logging.warning(f"Skipping row index {idx}: Missing or invalid product name.")
            continue

        # --- Get Base Price (Haoreum Price) --- 
        try:
            # Get price from the required column
            price_str = str(row['판매단가(V포함)']).replace(',', '').strip()
            if price_str and price_str != '-':
                haoreum_price = float(price_str)
                if haoreum_price <= 0:
                    logging.warning(f"Invalid Haoreum price ({haoreum_price}) for product: {product_name}")
                    haoreum_price = None
            else:
                haoreum_price = None
                logging.warning(f"Empty price for product: {product_name}")
        except Exception as e:
            haoreum_price = None
            logging.warning(f"Could not parse Haoreum price for product: {product_name}, Error: {str(e)}")

        # --- Process KoGift Data ---
        if product_name in kogift_results and kogift_results[product_name]:
            best_match = kogift_results[product_name][0]
            
            try:
                kogift_price = float(str(best_match.get('price', '')).replace(',', '').strip())
                if kogift_price <= 0:
                    raise ValueError(f"Invalid KoGift price: {kogift_price}")
                    
                output_df.loc[idx, '판매단가(V포함)(2)'] = f"{kogift_price:,.0f}"
                output_df.loc[idx, '기본수량(2)'] = best_match.get('quantity', '-')
                output_df.loc[idx, '고려기프트 상품링크'] = best_match.get('link', '-')
                output_df.loc[idx, '고려기프트 이미지'] = best_match.get('image_path', '-')
                
                # Calculate price differences
                if haoreum_price and kogift_price and haoreum_price > 0:
                    price_diff = kogift_price - haoreum_price
                    price_diff_percent = (price_diff / haoreum_price) * 100
                    output_df.loc[idx, '가격차이(2)'] = f"{price_diff:,.0f}"
                    output_df.loc[idx, '가격차이(2)(%)'] = f"{price_diff_percent:.1f}"
                    
                    # Calculate total price for quantity
                    if best_match.get('quantity'):
                        try:
                            quantity = float(str(best_match['quantity']).replace(',', ''))
                            if quantity > 0:
                                total_price = kogift_price * quantity
                                output_df.loc[idx, '판매가(V포함)(2)'] = f"{total_price:,.0f}"
                            else:
                                output_df.loc[idx, '판매가(V포함)(2)'] = '-'
                                logging.warning(f"Invalid quantity ({quantity}) for KoGift product: {product_name}")
                        except (ValueError, TypeError) as e:
                            output_df.loc[idx, '판매가(V포함)(2)'] = '-'
                            logging.warning(f"Error calculating total price for KoGift product {product_name}: {e}")
            except (ValueError, TypeError) as e:
                logging.warning(f"Error processing KoGift price for {product_name}: {e}")
                output_df.loc[idx, ['판매단가(V포함)(2)', '가격차이(2)', '가격차이(2)(%)', '판매가(V포함)(2)']] = '-'
        else:
            # If no KoGift results, set error message
            output_df.loc[idx, '고려기프트 상품링크'] = '가격 범위내에 없거나 텍스트 유사율을 가진 상품이 없음'

        # --- Process Naver Data ---
        if isinstance(naver_results, pd.DataFrame):
            # Handle DataFrame format from crawl_naver_products
            naver_row = naver_results[naver_results['original_row'].apply(
                lambda x: isinstance(x, dict) and x.get('상품명') == product_name
            )]
            if not naver_row.empty:
                best_match = {
                    'price': naver_row.iloc[0].get('판매단가(V포함)(3)'),
                    'quantity': naver_row.iloc[0].get('기본수량(3)'),
                    'link': naver_row.iloc[0].get('네이버 쇼핑 링크'),
                    'mallName': naver_row.iloc[0].get('공급사명'),
                    'mallProductUrl': naver_row.iloc[0].get('공급사 상품링크'),
                    'image_url': naver_row.iloc[0].get('네이버 이미지')
                }
            else:
                best_match = None
        elif isinstance(naver_results, dict) and product_name in naver_results and naver_results[product_name]:
            best_match = naver_results[product_name][0]
        else:
            best_match = None

        if best_match:
            try:
                # Get price from the appropriate field based on data structure
                price_str = str(best_match.get('price', '')).replace(',', '').strip()
                if price_str and price_str != '-':
                    try:
                        naver_price = float(price_str)
                        if naver_price > 0:
                            output_df.loc[idx, '판매단가(V포함)(3)'] = f"{naver_price:,.0f}"
                            output_df.loc[idx, '기본수량(3)'] = best_match.get('quantity', '1')
                            output_df.loc[idx, '네이버 쇼핑 링크'] = best_match.get('link', '-')
                            output_df.loc[idx, '공급사명'] = best_match.get('mallName', '-')
                            output_df.loc[idx, '공급사 상품링크'] = best_match.get('mallProductUrl', '-')
                            output_df.loc[idx, '네이버 이미지'] = best_match.get('image_url', '-')

                            # Calculate price differences only if both prices are valid
                            if haoreum_price and haoreum_price > 0:
                                price_diff = naver_price - haoreum_price
                                price_diff_percent = (price_diff / haoreum_price) * 100
                                output_df.loc[idx, '가격차이(3)'] = f"{price_diff:,.0f}"
                                output_df.loc[idx, '가격차이(3)(%)'] = f"{price_diff_percent:.1f}"
                    except (ValueError, TypeError) as e:
                        logging.warning(f"Error converting Naver price '{price_str}' for {product_name}: {e}")
                        output_df.loc[idx, ['판매단가(V포함)(3)', '가격차이(3)', '가격차이(3)(%)']] = '-'
            except Exception as e:
                logging.error(f"Error processing Naver data for {product_name}: {e}")
                output_df.loc[idx, ['판매단가(V포함)(3)', '가격차이(3)', '가격차이(3)(%)']] = '-'
        else:
            # If no Naver results, set error message
            output_df.loc[idx, '네이버 쇼핑 링크'] = '가격이 범위내에 없거나 검색된 상품이 없음'
            output_df.loc[idx, ['판매단가(V포함)(3)', '가격차이(3)', '가격차이(3)(%)', '공급사명', '공급사 상품링크', '네이버 이미지']] = '-'

    # Format numeric columns
    numeric_columns = [
        '판매단가(V포함)', '판매단가(V포함)(2)', '판매단가(V포함)(3)',
        '판매가(V포함)(2)', '가격차이(2)', '가격차이(3)'
    ]
    for col in numeric_columns:
        if col in output_df.columns:
            output_df[col] = output_df[col].apply(lambda x: 
                f"{float(str(x).replace(',', '')):,.0f}" 
                if pd.notna(x) and str(x).strip() != '-' and not isinstance(x, str) 
                else x
            )

    # Format percentage columns
    percent_columns = ['가격차이(2)(%)', '가격차이(3)(%)']
    for col in percent_columns:
        if col in output_df.columns:
            output_df[col] = output_df[col].apply(lambda x: 
                f"{float(str(x).replace('%', '')):,.1f}" 
                if pd.notna(x) and str(x).strip() != '-' and not isinstance(x, str)
                else x
            )

    # Ensure all required columns exist and are in the correct order
    for col in FINAL_COLUMN_ORDER:
        if col not in output_df.columns:
            output_df[col] = '-'
    output_df = output_df[FINAL_COLUMN_ORDER]
    
    logging.info(f"Data formatting completed. Output rows: {len(output_df)}")
    return output_df

def process_input_data(df: pd.DataFrame, config: Optional[configparser.ConfigParser] = None) -> pd.DataFrame:
    """
    Process input DataFrame with necessary data processing steps.
    """
    if config is None:
        config = configparser.ConfigParser()
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.ini')
        config.read(config_path)
    
    try:
        # Apply initial filtering
        filtered_df = filter_results(df, config)
        if filtered_df is None:
            logging.error("Failed to filter results")
            return df
            
        # Format data for output
        kogift_results = {}  # This would normally come from kogift processing
        naver_results = {}   # This would normally come from naver processing
        
        formatted_df = format_product_data_for_output(filtered_df, kogift_results, naver_results)
        
        return formatted_df
        
    except Exception as e:
        logging.error(f"Error in process_input_data: {e}", exc_info=True)
        return df 