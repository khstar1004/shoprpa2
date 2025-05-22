import os
import sys
import logging
import argparse
import time
import asyncio
import multiprocessing
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import configparser
import hashlib
import datetime
import traceback
import shutil
from pathlib import Path
import openpyxl
import warnings
import gc
import platform
import json

# Add the parent directory of the script to the Python path
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Import local modules
from .email_sender import validate_email_config, send_excel_by_email
from .path_fix import fix_image_directories, update_env_variables
from .matching_logic import match_products, post_process_matching_results
from .data_processing import process_input_file, filter_results, format_product_data_for_output
from .excel_utils import (
    create_split_excel_outputs,
    find_excel_file,
    finalize_dataframe_for_excel,
    IMAGE_COLUMNS
)
from .crawling_logic import crawl_all_sources
from .utils import preprocess_and_download_images
from .execution_setup import initialize_environment, clear_temp_files, _load_and_validate_config
from .image_integration import integrate_and_filter_images
from .price_highlighter import apply_price_highlighting_to_files
from .upload_filter import apply_filter_to_upload_excel
from .excel_formatter import apply_excel_formatting
from .fix_kogift_images import fix_excel_kogift_images
from .naver_data_cleaner import clean_naver_data, get_invalid_naver_rows
from .naver_data_cleaner import fix_missing_naver_images # Re-enabled import
from .excel_image_placer import create_excel_with_placed_images # 이미지 배치 함수 추가
from .price_difference_filter import filter_by_price_difference, apply_kogift_data_filter # 가격차이 필터링 함수 추가

# Global configuration
warnings.filterwarnings('ignore')

# Set high DPI awareness for Windows (prevents display scaling issues)
if platform.system() == "Windows":
    try:
        import ctypes
        ctypes.windll.shcore.SetProcessDpiAwareness(2)
    except Exception:
        pass

def verify_excel_images(excel_path: str) -> dict:
    """
    Verify if Kogift and Naver images are correctly included in the Excel file.
    
    Args:
        excel_path: Path to the Excel file
        
    Returns:
        dict: Dictionary with counts of image types found
    """
    try:
        import openpyxl
        import pandas as pd
        from pathlib import Path
        
        logging.info(f"Verifying images in Excel file: {excel_path}")
        
        # Check if file exists
        if not Path(excel_path).exists():
            logging.warning(f"Excel file not found: {excel_path}")
            return {"exists": False}
            
        # Try to read Excel file
        try:
            # First try with pandas to check the data
            df = pd.read_excel(excel_path)
            
            # Look for image columns
            image_cols = [col for col in df.columns if '이미지' in col]
            if not image_cols:
                logging.warning(f"No image columns found in Excel file: {excel_path}")
                return {"exists": True, "image_columns": 0}
                
            logging.info(f"Found {len(image_cols)} image columns: {image_cols}")
            
            # Count cells with image data by type
            kogift_cells = 0
            naver_cells = 0
            haereum_cells = 0
            
            # Check for image data in DataFrame
            for col in image_cols:
                for idx, value in enumerate(df[col]):
                    if isinstance(value, str) and value.startswith(('http://', 'https://')):
                        if 'koreagift' in value.lower() or 'kogift' in value.lower():
                            kogift_cells += 1
                        elif 'pstatic.net' in value.lower() or 'naver' in value.lower():
                            naver_cells += 1
                        elif 'jclgift' in value.lower() or 'haereum' in value.lower():
                            haereum_cells += 1
            
            # Now try with openpyxl to check for actual images
            wb = openpyxl.load_workbook(excel_path)
            ws = wb.active
            
            actual_images = len(ws._images)
            logging.info(f"Found {actual_images} actual images in the Excel file.")
            
            # Count by column (rough estimation of image types)
            col_indices = {}
            for col_idx, col_name in enumerate(df.columns, 1):
                for img_type in ['고려기프트', '네이버', '본사']:
                    if img_type in col_name:
                        col_indices[img_type] = col_idx
            
            # Return summary
            result = {
                "exists": True,
                "image_columns": len(image_cols),
                "actual_images": actual_images,
                "kogift_urls": kogift_cells,
                "naver_urls": naver_cells,
                "haereum_urls": haereum_cells,
                "column_names": image_cols
            }
            
            # Log summary
            logging.info(f"Excel image verification complete: {result}")
            
            # Special warning for Naver images
            if naver_cells == 0 and '네이버 이미지' in df.columns:
                logging.warning("⚠️ NO NAVER IMAGES found in Excel file despite column existing!")
                logging.warning("Check 'filter_images_by_similarity' function in image_integration.py")
                logging.warning("Consider lowering naver_similarity_threshold in config.ini [ImageFiltering] section")
            
            return result
        except Exception as e:
            logging.error(f"Error reading Excel file: {e}")
            return {"exists": True, "error": str(e)}
            
    except Exception as e:
        logging.error(f"Error verifying Excel images: {e}")
        return {"exists": False, "error": str(e)}

async def main(config: configparser.ConfigParser, gpu_available: bool, progress_queue=None):
    """Main function orchestrating the RPA process (now asynchronous)."""
    try:
        main_start_time = time.time()
        logging.info("========= RPA Process Starting ========")

        # Run directory case fix before anything else
        fix_image_directories()
        update_env_variables()
        logging.info("Fixed image directory capitalization issues")

        # Add logging here
        logging.info(f"main_rpa.py: Received config. Input file path: {config.get('Paths', 'input_file', fallback='Not Set')}")

        # Add debug mode check
        debug_mode = config.getboolean('Debug', 'enabled', fallback=False)
        if debug_mode:
            logging.info("Debug mode enabled - detailed logging will be shown")
            
        def log_step(step_num, total_steps, message):
            """Helper function for consistent step logging"""
            log_msg = f"[Step {step_num}/{total_steps}] {message}"
            logging.info(log_msg)
            if progress_queue:
                progress_queue.emit("status", message)
            if debug_mode:
                logging.debug(f"Debug: {log_msg}")

        total_steps = 7
        
        # --- Concurrency Settings ---
        log_step(1, total_steps, "Initializing and configuring...")
        # Get max workers from config, provide safe defaults
        default_workers = max(1, os.cpu_count() // 2) # Default to half the cores (min 1)
        try:
            # Access config using ConfigParser methods
            download_workers = config.getint('Concurrency', 'max_crawl_workers', fallback=default_workers)
            matcher_workers_config = config.getint('Concurrency', 'max_match_workers', fallback=default_workers)
        except (configparser.NoSectionError, configparser.NoOptionError, ValueError) as e:
            logging.warning(f"Error reading concurrency settings from config: {e}. Using defaults ({default_workers} workers).")
            download_workers = default_workers
            matcher_workers_config = default_workers

        # Adjust matcher workers based on GPU availability
        if gpu_available:
            try:
                # Optimize for more VRAM utilization to increase accuracy
                matcher_workers = config.getint('Concurrency', 'matcher_max_workers_gpu', fallback=2)
                logging.info(f"GPU detected. Using up to {matcher_workers} CPU workers for matching coordination/CPU-bound tasks (GPU handles main load).")
            except (configparser.NoSectionError, configparser.NoOptionError, ValueError):
                logging.warning("MATCHER_MAX_WORKERS_GPU not found in config [Concurrency]. Using default GPU worker setting (2).")
                matcher_workers = 2
        else:
            # For CPU-only mode, use more workers but not too many to prevent contention
            matcher_workers = min(matcher_workers_config, max(2, os.cpu_count() - 1))
            logging.info(f"No GPU detected. Using up to {matcher_workers} CPU workers for matching.")
        logging.info(f"Using up to {download_workers} workers for downloads/preprocessing.")

        # Environment setup (config, logging, dirs, gpu, validation) is done before calling main

        # 1. Clear previous temp files
        log_step(2, total_steps, "Clearing temporary files...")
        step_start_time = time.time()
        try:
            clear_temp_files(config)
            logging.debug(f"Temp files cleared in {time.time() - step_start_time:.2f} sec")
        except Exception as e:
            logging.error(f"Error clearing temp files: {e}")
            if debug_mode:
                logging.debug(traceback.format_exc())

        # 2. Process Input File
        log_step(3, total_steps, "Reading input file...")
        step_start_time = time.time()
        try:
            haoreum_df, input_filename = process_input_file(config)
            if haoreum_df is None or haoreum_df.empty:
                raise Exception("No valid input data found")
            logging.debug(f"Input processing completed in {time.time() - step_start_time:.2f} sec")
            logging.debug(f"Input data shape: {haoreum_df.shape}")

            # Dynamically set the input_file path in the config object
            # so it can be accessed by other modules like crawling_kogift.py
            if input_filename:
                if not config.has_section('Input'):
                    config.add_section('Input')
                config.set('Input', 'input_file', input_filename)
                logging.info(f"Updated config in memory: [Input] input_file = {input_filename}")
                # ADD THIS LOG FOR DETAILED VERIFICATION
                logging.info(f"DETAILED_CONFIG_SET: [Input] input_file has been set to: '{config.get('Input', 'input_file')}'")
            else:
                # This case should ideally be handled more robustly,
                # perhaps by raising an error if input_filename is crucial and not found.
                logging.error("input_filename was not determined by process_input_file. Cannot set it in config for crawlers.")
                # Depending on desired behavior, you might want to raise an exception here:
                # raise Exception("Critical: Input file path could not be determined.")

        except Exception as e:
            logging.error(f"Error processing input file: {e}")
            if debug_mode:
                logging.debug(traceback.format_exc())
            return

        # 2.5 Preprocess Haoreum Images from Input
        step_start_time = time.time()
        logging.info("[Step 3/7] Preprocessing images from input file (if any)...")
        if progress_queue: progress_queue.emit("status", "Preprocessing input images...")
        
        try:
            # <<< CHOOSE CORRECT URL COLUMN >>>
            if '본사 이미지' in haoreum_df.columns:
                input_url_col = '본사 이미지'
            elif '본사상품링크' in haoreum_df.columns:
                input_url_col = '본사상품링크'
                logging.info("'본사 이미지' column not found, using '본사상품링크' for input image URLs.")
            else:
                input_url_col = None # No suitable column found
                logging.warning("Neither '본사 이미지' nor '본사상품링크' found for input image preprocessing.")

            if input_url_col: # Proceed only if a URL column was found
                # Ensure background removal is properly configured
                try:
                    use_bg_removal = config.getboolean('Matching', 'use_background_removal', fallback=True)
                    if not use_bg_removal:
                        logging.info("Background removal is disabled in config. Images will be downloaded without background removal.")
                    else:
                        # Initialize background removal early to avoid concurrent initialization issues
                        from image_utils import initialize_rembg_session
                        initialize_rembg_session()
                        logging.info("Background removal is enabled. Images will be processed with background removal.")
                except (configparser.Error, ValueError, ImportError) as e:
                    logging.warning(f"Error configuring background removal: {e}. Proceeding with default settings.")
                
                input_file_image_map = await preprocess_and_download_images(
                    df=haoreum_df,
                    url_column_name=input_url_col, # Use the determined column name
                    id_column_name='Code',
                    prefix='input',
                    config=config,
                    max_workers=download_workers,
                    product_name_column='상품명'  # Use product name for consistent image naming
                )
                processed_count = len(input_file_image_map)
            else:
                input_file_image_map = {}
                
            logging.info(f"[Step 3/7] Input file images preprocessed. Processed {processed_count} images. Duration: {time.time() - step_start_time:.2f} sec")
            if progress_queue: progress_queue.emit("status", "Finished preprocessing input images.")
        except Exception as e:
            logging.error(f"Error preprocessing input images: {e}")
            if debug_mode:
                logging.debug(traceback.format_exc())
            input_file_image_map = {}

        # 3. Crawl External Data - Increased crawling and image downloads
        log_step(4, total_steps, "Starting enhanced crawling process (increased depth for accuracy)...")
        step_start_time = time.time()
        try:
            # Increase crawling depth for better accuracy
            original_max_items = config.getint('ScraperSettings', 'kogift_max_items', fallback=10)
            # Temporarily increase the crawling depth by 50% for more candidates
            config.set('ScraperSettings', 'kogift_max_items', str(int(original_max_items * 1.5)))
            
            # Also increase the retry count for better results
            original_retries = config.getint('Network', 'max_retries', fallback=2)
            config.set('Network', 'max_retries', str(original_retries + 1))
            
            logging.info(f"Enhanced crawling: items per product increased to {int(original_max_items * 1.5)}, retries to {original_retries + 1}")
            
            kogift_crawl_results, naver_crawl_results, haereum_image_url_map = await crawl_all_sources(haoreum_df, config)
            logging.debug(f"Enhanced crawling completed in {time.time() - step_start_time:.2f} sec")
            
            # Reset config values to their original settings
            config.set('ScraperSettings', 'kogift_max_items', str(original_max_items))
            config.set('Network', 'max_retries', str(original_retries))
            
            # Validate crawl results immediately
            if debug_mode:
                logging.debug(f"Kogift results type: {type(kogift_crawl_results)}")
                logging.debug(f"Naver results type: {type(naver_crawl_results)}")
                logging.debug(f"Haereum map type: {type(haereum_image_url_map)}")
                
            # 여기서 해오름 이미지 URL 맵을 안전하게 보관 (원본 데이터로 저장)
            # 이 맵은 엑셀 생성 단계에서 바로 사용됨
            original_haereum_image_urls = haereum_image_url_map.copy() if isinstance(haereum_image_url_map, dict) else {}
            logging.info(f"원본 해오름 이미지 URL {len(original_haereum_image_urls)}개를 안전하게 보관하였습니다. (키: 상품코드 또는 상품명)")
            
            # 해오름 이미지 URL 디버그 로깅 (최대 5개)
            if debug_mode and original_haereum_image_urls:
                sample_count = 0
                for prod_identifier, url_info in list(original_haereum_image_urls.items())[:5]: # Changed variable names
                    logging.debug(f"보관된 해오름 이미지 URL 샘플 #{sample_count+1}: {prod_identifier} -> {url_info}")
                    sample_count += 1
                
        except Exception as e:
            logging.error(f"Error during crawling: {e}")
            if debug_mode:
                logging.debug(traceback.format_exc())
            kogift_crawl_results, naver_crawl_results, haereum_image_url_map = {}, [], {}
            original_haereum_image_urls = {}  # 에러 시 빈 딕셔너리로 초기화

        # --- Process Crawl Results (Handle potential failures) ---
        try:
            # Initialize with safe defaults
            kogift_crawl_results = {} if kogift_crawl_results is None else kogift_crawl_results
            naver_crawl_results = [] if naver_crawl_results is None else naver_crawl_results
            haereum_image_url_map = {} if haereum_image_url_map is None else haereum_image_url_map

            # Debug logging for result types
            logging.debug(f"Processing crawl results:")
            logging.debug(f"- Kogift results type: {type(kogift_crawl_results)}")
            logging.debug(f"- Naver results type: {type(naver_crawl_results)}")
            logging.debug(f"- Haereum map type: {type(haereum_image_url_map)}")

            # Validate Kogift results
            if isinstance(kogift_crawl_results, dict):
                # Count actual items with data, not just the number of product entries
                valid_product_count = sum(1 for items in kogift_crawl_results.values() if items and len(items) > 0)
                valid_item_count = sum(len(items) for items in kogift_crawl_results.values() if items)
                kogift_count = valid_product_count
                logging.debug(f"Kogift results: {valid_product_count} products with matches (total {valid_item_count} items)")
                if kogift_count == 0:
                    logging.warning("Kogift crawl resulted in empty dictionary")
            elif isinstance(kogift_crawl_results, pd.DataFrame):
                kogift_count = len(kogift_crawl_results) if not kogift_crawl_results.empty else 0
                if kogift_crawl_results.empty:
                    logging.warning("Kogift crawl resulted in empty DataFrame")
            else:
                kogift_count = 0
                logging.warning(f"Unexpected Kogift results type: {type(kogift_crawl_results)}")

            # Validate Naver results
            if isinstance(naver_crawl_results, list) and len(naver_crawl_results) > 0:
                try:
                    naver_map = {}
                    for item in naver_crawl_results:
                        if isinstance(item, dict) and 'original_product_name' in item:
                            product_name = item.get('original_product_name')
                            if product_name:
                                # Extract Naver data and put it in a list for each product
                                naver_data = {k: v for k, v in item.items() if k != 'original_product_name'}
                                if product_name not in naver_map:
                                    naver_map[product_name] = []
                                naver_map[product_name].append(naver_data)
                    
                    logging.debug(f"Created Naver map with {len(naver_map)} entries.")
                    
                    # 샘플 이미지 URL 로깅 (디버깅용)
                    sample_count = 0
                    for name, items in naver_map.items():
                        if items and sample_count < 5:  # 최대 5개 샘플만
                            for item in items[:1]:  # 각 제품당 첫 번째 항목만
                                img_url = item.get('image_url') or item.get('image_path')
                                if img_url:
                                    logging.debug(f"네이버 이미지 URL 샘플 #{sample_count+1}: {img_url}")
                                    sample_count += 1
                    
                    # Ensure Naver images are downloaded to the correct directory
                    naver_image_dir = os.path.join(config.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main'), 'Naver')
                    os.makedirs(naver_image_dir, exist_ok=True)
                    logging.info(f"Ensuring Naver images directory exists: {naver_image_dir}")
                    
                    # Fix any image paths to ensure they're in the right directory
                    img_fix_count = 0
                    for name, items in naver_map.items():
                        for item in items:
                            img_path = item.get('image_path')
                            img_url = item.get('image_url')
                            
                            # Make sure we have a URL for each item (needed for excel_utils.py)
                            if not img_url and img_path and img_path.startswith('http'):
                                item['image_url'] = img_path
                                img_url = img_path
                            
                            # Process local image paths
                            if img_path and isinstance(img_path, str) and not img_path.startswith('http') and os.path.exists(img_path):
                                # Check if the image is in the wrong directory
                                if 'Naver' not in img_path.replace('\\', '/').split('/'):
                                    # Move to correct directory
                                    filename = os.path.basename(img_path)
                                    new_path = os.path.join(naver_image_dir, filename)
                                    try:
                                        shutil.copy2(img_path, new_path)
                                        item['image_path'] = new_path
                                        img_fix_count += 1
                                        logging.debug(f"Fixed Naver image path: {img_path} -> {new_path}")
                                    except Exception as e:
                                        logging.error(f"Error fixing Naver image path: {e}")
                                
                            # Ensure the item has both 'url' and 'local_path' structure for excel_utils.py
                            if img_url:
                                # Update the item's image data to dictionary format for excel_utils.py
                                # Safely handle missing 'image_path' key
                                if 'image_path' not in item:
                                    logging.warning(f"Missing image_path for Naver item with URL: {img_url}")
                                    # Try to use the image_url as a fallback or set to None
                                    item['image_path'] = img_url if img_url.startswith('http') else None
                                
                                image_data = {
                                    'url': img_url,
                                    'local_path': item.get('image_path'), # Safely get image_path
                                    'original_path': item.get('original_path', item.get('image_path')), # Keep original if available
                                    'source': 'naver'
                                }
                                item['image_data'] = image_data
                    
                    if img_fix_count > 0:
                        logging.info(f"Fixed {img_fix_count} Naver image paths to ensure correct directory")
                except Exception as e:
                    logging.error(f"Error creating Naver map: {e}", exc_info=True)
                    naver_map = {}
            else:
                naver_map = {}
                logging.warning("Naver results are empty or not in expected format")

            # Validate Haereum results
            if isinstance(haereum_image_url_map, dict):
                # Count only entries with actual URLs
                valid_haereum_count = sum(1 for item in haereum_image_url_map.values() if item)
                haereum_count = valid_haereum_count
                logging.debug(f"Haereum URL results: {valid_haereum_count} products with valid URLs out of {len(haereum_image_url_map)} total")
            else:
                haereum_count = len(haereum_image_url_map) if hasattr(haereum_image_url_map, '__len__') else 0
            if haereum_count == 0:
                logging.warning("Haereum URL crawl resulted in empty map")

            # Log crawl statistics
            logging.info("Crawl Results Summary:")
            logging.info(f"- Kogift items: {kogift_count}")
            logging.info(f"- Naver items: {len(naver_crawl_results) if isinstance(naver_crawl_results, list) else 'Naver results are not in list format'}")
            logging.info(f"- Haereum URLs: {haereum_count}")

            if progress_queue:
                progress_queue.emit("status", f"크롤링 완료 (Kogift: {kogift_count}, Naver: {len(naver_crawl_results) if isinstance(naver_crawl_results, list) else 'Naver results are not in list format'}, Haereum: {haereum_count})")

        except Exception as e:
            logging.error(f"Error processing crawl results: {e}")
            if debug_mode:
                logging.debug(traceback.format_exc())
            # Set safe defaults on error
            kogift_crawl_results = {}
            naver_crawl_results = []
            haereum_image_url_map = {}

        # --- Prepare Data for Matching ---
        map_prep_start_time = time.time()
        logging.info("Preparing crawled data maps for matching...")
        kogift_map = {}
        if kogift_crawl_results:
            try:
                # Assuming kogift_crawl_results is a map {product_name: [results_list]}
                # 고려기프트 데이터 검사 및 로깅 추가
                valid_items = 0
                missing_img_items = 0
                for name, data_list in kogift_crawl_results.items():
                    # 각 상품에 대한 데이터 유효성 검사
                    valid_data = []
                    for item in data_list:
                        # 이미지 URL 확인
                        has_image = False
                        if 'image_path' in item and item['image_path']:
                            has_image = True
                        elif 'src' in item and item['src']:
                            # 이전 호환성을 위해 src 필드도 확인
                            item['image_path'] = item['src']
                            has_image = True
                        elif 'image_url' in item and item['image_url']:
                            # 다른 형식의 이미지 URL도 확인
                            item['image_path'] = item['image_url']
                            has_image = True
                        
                        # 필수 필드 확인 (이름, 가격, 링크, 이미지)
                        if item.get('name') and (
                            item.get('price') or item.get('price', 0) == 0
                        ) and (
                            item.get('link') or item.get('href')
                        ):
                            # 필수 데이터는 있으나 이미지 없는 경우 로그 남김
                            if not has_image:
                                missing_img_items += 1
                                logging.warning(f"고려기프트 데이터에 이미지 URL 없음: {item.get('name')}")
                                
                            # 링크 필드 표준화
                            if not item.get('link') and item.get('href'):
                                item['link'] = item['href']
                            
                            valid_data.append(item)
                    
                    if valid_data:
                        valid_items += len(valid_data)
                        kogift_map[name] = valid_data
                
                logging.info(f"고려기프트 데이터 처리: 총 {valid_items}개 항목 (이미지 없는 항목: {missing_img_items}개)")
                logging.debug(f"Created Kogift map with {len(kogift_map)} entries.")
                
                # 샘플 이미지 URL 로깅 (디버깅용)
                sample_count = 0
                for name, items in kogift_map.items():
                    if items and sample_count < 5:  # 최대 5개 샘플만
                        for item in items[:1]:  # 각 제품당 첫 번째 항목만
                            img_url = item.get('image_path') or item.get('src') or item.get('image_url')
                            if img_url:
                                logging.debug(f"고려기프트 이미지 URL 샘플 #{sample_count+1}: {img_url}")
                                sample_count += 1
            except Exception as e:
                 logging.error(f"Error creating Kogift map: {e}", exc_info=True)
                 kogift_map = {}

        # Process Naver results
        if isinstance(naver_crawl_results, list) and len(naver_crawl_results) > 0:
            try:
                naver_map = {}
                for item in naver_crawl_results:
                    if isinstance(item, dict) and 'original_product_name' in item:
                        product_name = item.get('original_product_name')
                        if product_name:
                            # Extract Naver data and put it in a list for each product
                            naver_data = {k: v for k, v in item.items() if k != 'original_product_name'}
                            if product_name not in naver_map:
                                naver_map[product_name] = []
                            naver_map[product_name].append(naver_data)
                
                logging.debug(f"Created Naver map with {len(naver_map)} entries.")
                
                # 샘플 이미지 URL 로깅 (디버깅용)
                sample_count = 0
                for name, items in naver_map.items():
                    if items and sample_count < 5:  # 최대 5개 샘플만
                        for item in items[:1]:  # 각 제품당 첫 번째 항목만
                            img_url = item.get('image_url') or item.get('image_path')
                            if img_url:
                                logging.debug(f"네이버 이미지 URL 샘플 #{sample_count+1}: {img_url}")
                                sample_count += 1
                
                # Ensure Naver images are downloaded to the correct directory
                naver_image_dir = os.path.join(config.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main'), 'Naver')
                os.makedirs(naver_image_dir, exist_ok=True)
                logging.info(f"Ensuring Naver images directory exists: {naver_image_dir}")
                
                # Fix any image paths to ensure they're in the right directory
                img_fix_count = 0
                for name, items in naver_map.items():
                    for item in items:
                        img_path = item.get('image_path')
                        img_url = item.get('image_url')
                        
                        # Make sure we have a URL for each item (needed for excel_utils.py)
                        if not img_url and img_path and img_path.startswith('http'):
                            item['image_url'] = img_path
                            img_url = img_path
                        
                        # Process local image paths
                        if img_path and isinstance(img_path, str) and not img_path.startswith('http') and os.path.exists(img_path):
                            # Check if the image is in the wrong directory
                            if 'Naver' not in img_path.replace('\\', '/').split('/'):
                                # Move to correct directory
                                filename = os.path.basename(img_path)
                                new_path = os.path.join(naver_image_dir, filename)
                                try:
                                    shutil.copy2(img_path, new_path)
                                    item['image_path'] = new_path
                                    img_fix_count += 1
                                    logging.debug(f"Fixed Naver image path: {img_path} -> {new_path}")
                                except Exception as e:
                                    logging.error(f"Error fixing Naver image path: {e}")
                            
                            # Ensure the item has both 'url' and 'local_path' structure for excel_utils.py
                            if img_url:
                                # Update the item's image data to dictionary format for excel_utils.py
                                # Safely handle missing 'image_path' key
                                if 'image_path' not in item:
                                    logging.warning(f"Missing image_path for Naver item with URL: {img_url}")
                                    # Try to use the image_url as a fallback or set to None
                                    item['image_path'] = img_url if img_url.startswith('http') else None
                                
                                image_data = {
                                    'url': img_url,
                                    'local_path': item.get('image_path'), # Safely get image_path
                                    'original_path': item.get('original_path', item.get('image_path')), # Keep original if available
                                    'source': 'naver'
                                }
                                item['image_data'] = image_data
                    
                if img_fix_count > 0:
                    logging.info(f"Fixed {img_fix_count} Naver image paths to ensure correct directory")
            except Exception as e:
                logging.error(f"Error creating Naver map: {e}", exc_info=True)
                naver_map = {}
        else:
            naver_map = {}
            logging.warning("Naver results are empty or not in expected format")

        logging.info(f"Data maps prepared for matching. Duration: {time.time() - map_prep_start_time:.2f} sec")
        
        # 일반 로그 출력
        logging.info(f"읽어온 설정값: GPU={gpu_available}, 텍스트 임계치={config.getfloat('Matching', 'text_threshold', fallback=0.55)}, 이미지 임계치={config.getfloat('Matching', 'image_threshold', fallback=0.5)}")

        # 고려기프트 이미지 다운로드 사전 확인 - 모든 이미지 다운로드
        if kogift_map:
            logging.info("고려기프트 이미지 사전 다운로드 시작 (향상된 정확도를 위해 모든 이미지 다운로드)...")
            kogift_img_start_time = time.time()
            
            # 다운로드할 이미지 URL 목록 생성
            img_urls_to_download = []
            for name, items in kogift_map.items():
                for item in items:
                    img_url = item.get('image_path') or item.get('src')
                    if img_url and isinstance(img_url, str) and img_url.startswith('http'):
                        # 고유 식별자 생성
                        item_id = hashlib.md5((img_url + name).encode()).hexdigest()[:10]
                        img_urls_to_download.append((item_id, img_url))
            
            # 이미지 다운로드 실행
            if img_urls_to_download:
                logging.info(f"사전 다운로드할 고려기프트 이미지: {len(img_urls_to_download)}개")
                
                # 임시 DataFrame 생성하여 기존 다운로드 함수 활용
                temp_df = pd.DataFrame({
                    'id': [item[0] for item in img_urls_to_download],
                    'url': [item[1] for item in img_urls_to_download]
                })
                
                kogift_image_map = await preprocess_and_download_images(
                    df=temp_df,
                    url_column_name='url',
                    id_column_name='id',
                    prefix='kogift',
                    config=config,
                    max_workers=download_workers,
                    product_name_column=None  # No product name column in the temp_df, will fallback to URL-based naming
                )
                
                download_success = len(kogift_image_map)
                logging.info(f"고려기프트 이미지 사전 다운로드 완료: {download_success}/{len(img_urls_to_download)} 성공. 소요시간: {time.time() - kogift_img_start_time:.2f}초")
            else:
                logging.warning("다운로드할 고려기프트 이미지 URL이 없습니다.")

        # 4. Match Products with improved accuracy (longer but more accurate)
        step_start_time = time.time()
        
        # Use config values instead of hardcoding
        use_ensemble = config.getboolean('Matching', 'use_ensemble_models', fallback=True)
        use_multiple_models = config.getboolean('ImageMatching', 'use_multiple_models', fallback=True)
        use_tfidf = config.getboolean('Matching', 'use_tfidf', fallback=True)
        
        logging.info(f"[Step 5/7] Starting product matching with enhanced accuracy settings (GPU: {gpu_available}, CPU Workers: {matcher_workers})...")
        if progress_queue: progress_queue.emit("status", "Matching products with enhanced accuracy (might take longer)...")
        matched_df = pd.DataFrame() # Initialize empty DataFrame
        try:
            # Ensure Haoreum DataFrame is valid before proceeding
            if haoreum_df is None or haoreum_df.empty:
                logging.error("[Step 5/7] Haoreum DataFrame is empty or None before matching. Cannot proceed.")
                raise Exception("Input data processing failed to produce valid Haoreum data.")

            # Log columns before matching
            logging.info(f"Columns in haoreum_df BEFORE matching: {haoreum_df.columns.tolist()}")

            # Use ThreadPoolExecutor instead of asyncio.to_thread
            with ThreadPoolExecutor(max_workers=matcher_workers) as executor:
                loop = asyncio.get_event_loop()
                matched_df = await loop.run_in_executor(
                    executor,
                    match_products,
                    haoreum_df,
                    kogift_map,
                    naver_map,
                    input_file_image_map,
                    config,
                    gpu_available,
                    progress_queue,
                    matcher_workers
                )
                
                # Log columns after matching
                logging.info(f"Columns in matched_df AFTER matching: {matched_df.columns.tolist() if matched_df is not None else 'None'}")

                if matched_df is None or matched_df.empty:
                    raise Exception("Product matching returned no results")
                    
                match_count = len(matched_df)
                logging.info(f"[Step 5/7] Product matching finished. Matched {match_count} potential rows. Duration: {time.time() - step_start_time:.2f} sec")

        except Exception as match_err:
            logging.error(f"[Step 5/7] Error during product matching: {match_err}", exc_info=True)
            if progress_queue: progress_queue.emit("error", f"Matching failed: {str(match_err)}")
            # Create empty DataFrame with same structure as input
            matched_df = pd.DataFrame(columns=haoreum_df.columns)
            logging.warning("Created empty DataFrame due to matching error")

        if matched_df.empty:
            logging.warning("Matching resulted in an empty DataFrame. No data to filter or output.")
            total_time = time.time() - main_start_time
            logging.info(f"========= RPA Process Finished (No Matching Results) - Total Time: {total_time:.2f} sec ==========")
            if progress_queue: progress_queue.emit("finished", "True")
            return # Exit early

        # <<< ADDED: Post-process matching results (cleaning, conditional clearing) >>>
        try:
            logging.info(f"Post-processing {len(matched_df)} matched rows (cleaning, formatting, conditional clearing)...")
            # Call the renamed function from matching_logic
            processed_matched_df = post_process_matching_results(matched_df, config)
            logging.info(f"Post-processing finished. Rows remaining: {len(processed_matched_df)}") 
            # Note: post_process_matching_results should not drop rows, just modify
        except Exception as post_process_err:
            logging.error(f"Error during matching post-processing: {post_process_err}", exc_info=True)
            # Fallback: use the original matched_df if post-processing fails
            processed_matched_df = matched_df 
            logging.warning("Using original matched data due to post-processing error.")
        # <<< END ADDED STEP >>>

        # 5. Filter Results (Use the post-processed DataFrame)
        step_start_time = time.time()
        # Use processed_matched_df here
        logging.info(f"[Step 6/7] Filtering {len(processed_matched_df)} post-processed rows...") 
        if progress_queue: progress_queue.emit("status", "Filtering results...")
        try:
            # Pass the processed DataFrame to filter_results
            filtered_df = filter_results(processed_matched_df, config) 
            # Log columns after filtering
            logging.info(f"Columns in filtered_df AFTER filtering: {filtered_df.columns.tolist() if filtered_df is not None else 'None'}")

            filter_count = len(filtered_df)
            logging.info(f"[Step 6/7] Filtering finished. {filter_count} rows remaining. Duration: {time.time() - step_start_time:.2f} sec")
        except Exception as filter_err:
            logging.error(f"Error during filtering: {filter_err}", exc_info=True)
            if progress_queue: progress_queue.emit("error", f"Filtering failed: {str(filter_err)}")
            # Use the processed_matched_df as fallback if filtering fails
            filtered_df = processed_matched_df  
            logging.warning("Using unfiltered (but post-processed) data due to filtering error")

        if filtered_df.empty:
            logging.warning("Filtering removed all rows. No data to output.")
            total_time = time.time() - main_start_time
            logging.info(f"========= RPA Process Finished (No Filtered Output) - Total Time: {total_time:.2f} sec ==========")
            if progress_queue: progress_queue.emit("finished", "True")
            return # Exit early

        # 6. Save and Format Output File
        step_start_time = time.time()
        logging.info(f"[Step 7/7] Saving and formatting {len(filtered_df)} final rows...")
        output_path = None
        if input_filename:
            try:
                # First ensure that all image URLs are properly included
                logging.info("Formatting product data with image URLs for output...")
                
                # Make sure all crawled images are included in the Excel output
                formatted_df = format_product_data_for_output(
                    input_df=filtered_df, 
                    kogift_results=kogift_map, 
                    naver_results=naver_map,
                    input_file_image_map=input_file_image_map
                )
                
                # <<< ADDED: Populate '본사이미지URL' in formatted_df before image integration >>>
                if '본사이미지URL' not in formatted_df.columns:
                    formatted_df['본사이미지URL'] = pd.NA # Use pd.NA for consistency

                if original_haereum_image_urls and not formatted_df.empty:
                    logging.info(f"Populating '본사이미지URL' in formatted_df using {len(original_haereum_image_urls)} scraped Haereum URLs...")
                    applied_count = 0
                    for idx, row in formatted_df.iterrows():
                        # Prefer 'Code' for lookup, fallback to '상품명'
                        product_code_val = row.get('Code')
                        product_name_val = row.get('상품명')
                        
                        scraped_url_data = None
                        # Try by product code first
                        if pd.notna(product_code_val):
                            # Ensure product_code_key is a string
                            code_key = str(int(product_code_val)) if isinstance(product_code_val, float) and product_code_val.is_integer() else str(product_code_val).strip()
                            scraped_url_data = original_haereum_image_urls.get(code_key)
                        
                        # Fallback to product name if code lookup failed or code was not present
                        if not scraped_url_data and pd.notna(product_name_val):
                            name_key = str(product_name_val).strip()
                            scraped_url_data = original_haereum_image_urls.get(name_key)
                            if scraped_url_data:
                                logging.debug(f"Row {idx}: Used product name '{name_key}' for Haereum URL lookup as Code lookup failed or Code was NA.")

                        actual_url = None
                        if isinstance(scraped_url_data, dict):
                            actual_url = scraped_url_data.get('url')
                        elif isinstance(scraped_url_data, str): # backward compatibility for plain URL strings
                            actual_url = scraped_url_data
                            
                        if actual_url and isinstance(actual_url, str) and actual_url.startswith(('http://', 'https://')):
                            formatted_df.at[idx, '본사이미지URL'] = actual_url
                            applied_count += 1
                        # else:
                            # Optional: Log if no URL found for a row after trying code and name
                            # logging.debug(f"Row {idx}: No scraped Haereum URL found via Code ('{product_code_val}') or Name ('{product_name_val}').")

                    logging.info(f"Applied {applied_count} scraped Haereum URLs to '본사이미지URL' column in formatted_df.")
                # <<< END ADDED BLOCK >>>
                
                # Create output directory if it doesn't exist
                output_dir = config.get('Paths', 'output_dir')
                os.makedirs(output_dir, exist_ok=True)
                
                # Generate output filename
                input_filename_base = input_filename.rsplit('.', 1)[0]
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                output_path = os.path.join(output_dir, f"{input_filename_base}")

                # DO NOT create the full output path with timestamp here
                # Instead, pass the base directory and filename to create_split_excel_outputs
                # and let it add the appropriate suffix (_result or _upload) and timestamp
                
                # --- Moved Image Integration Here ---
                try:
                    logging.info("Integrating and filtering images immediately before Excel generation...")
                    # Log DataFrame state BEFORE integration
                    logging.info(f"DataFrame shape BEFORE image integration: {formatted_df.shape}")
                    logging.debug(f"DataFrame columns BEFORE image integration: {formatted_df.columns.tolist()}")
                    if not formatted_df.empty:
                        logging.debug(f"Sample data BEFORE integration:\n{formatted_df.head().to_string()}")

                    # Perform image integration
                    integrated_df = integrate_and_filter_images(formatted_df, config, save_excel_output=False)
                    logging.info("Image integration and filtering complete.")

                    # Log DataFrame state AFTER integration
                    logging.info(f"DataFrame shape AFTER image integration: {integrated_df.shape}")
                    logging.debug(f"DataFrame columns AFTER image integration: {integrated_df.columns.tolist()}")
                    if not integrated_df.empty:
                        logging.debug(f"Sample data AFTER integration:\n{integrated_df.head().to_string()}")
                        # Explicitly check image column sample data
                        if IMAGE_COLUMNS:
                            img_cols_to_log = [col for col in IMAGE_COLUMNS if col in integrated_df.columns]
                            if img_cols_to_log:
                                 logging.debug(f"Sample image column data AFTER integration:\n{integrated_df[img_cols_to_log].head().to_string()}")


                except Exception as e:
                    logging.error(f"Error during image integration and filtering step: {e}", exc_info=True)
                    # Fallback: use the pre-integration DataFrame if integration fails
                    integrated_df = formatted_df
                    logging.warning("Proceeding with pre-integration data due to error.")
                # --- End Image Integration ---

                # Finalize the DataFrame structure before saving to Excel
                logging.info("Finalizing DataFrame structure for Excel output...")
                try:
                    # Ensure the finalize function is called with the correct DataFrame
                    df_to_save = finalize_dataframe_for_excel(integrated_df)
                    
                    # Log the result of finalization
                    if df_to_save.empty and not integrated_df.empty:
                        logging.error("DataFrame became empty after finalization. Skipping Excel creation.")
                        # Optionally: Emit error to progress_queue if available
                        if progress_queue:
                            progress_queue.emit("error", "Error during data finalization stage.")
                        # Skip Excel creation steps
                        result_success, upload_success = False, False
                        result_path, upload_path = None, None
                    else:
                        logging.info(f"DataFrame finalized successfully. Shape: {df_to_save.shape}")
                        logging.debug(f"Finalized columns: {df_to_save.columns.tolist()}")
                        
                        # Clean Naver data before applying Haereum URLs
                        try:
                            # Log invalid Naver rows for debugging
                            invalid_rows = get_invalid_naver_rows(df_to_save)
                            if invalid_rows:
                                logging.warning(f"Found {len(invalid_rows)} rows with invalid Naver data:")
                                for row in invalid_rows:
                                    logging.warning(f"Row {row['index']}: {row['product_name']}")
                            
                            # Clean the DataFrame
                            original_len = len(df_to_save)
                            df_to_save = clean_naver_data(df_to_save)
                            removed_count = original_len - len(df_to_save)
                            if removed_count > 0:
                                logging.info(f"Removed {removed_count} rows with invalid Naver data")
                                if progress_queue:
                                    progress_queue.emit("status", f"Removed {removed_count} rows with invalid Naver data")
                                    
                            # Fix missing Naver images by finding local files
                            logging.info("Fixing Naver images with missing local paths...")
                            try:
                                # Temporarily disabled fix_missing_naver_images due to local image issues
                                # df_to_save = fix_missing_naver_images(df_to_save, result_file=True)
                                # if progress_queue:
                                # progress_queue.emit("status", "Naver image fixing temporarily disabled")
                                
                                # Pass the config object to the function
                                df_to_save = fix_missing_naver_images(df_to_save, result_file=True, config_obj=config) 
                                logging.info("Attempted to fix missing Naver images.")
                                if progress_queue:
                                    progress_queue.emit("status", "Naver image paths checked and updated.")
                            except Exception as fix_err:
                                logging.warning(f"Non-critical error in fix_missing_naver_images: {fix_err}. Continuing with current data.")
                                # Continue with the original DataFrame
                        except Exception as clean_err:
                            logging.error(f"Error cleaning Naver data: {clean_err}", exc_info=True)
                            # Continue with original DataFrame if cleaning fails
                        
                        # 여기서 원본 해오름 이미지 URL을 DataFrame에 적용
                        try:
                            # 해오름 이미지 URL을 엑셀 데이터에 적용하는 로직
                            if original_haereum_image_urls and not df_to_save.empty:
                                # 'Code' 컬럼과 '상품명' 컬럼이 있는지 확인
                                if 'Code' in df_to_save.columns and '상품명' in df_to_save.columns:
                                    applied_count = 0
                                    logging.info(f"원본 해오름 이미지 URL ({len(original_haereum_image_urls)}개) 적용 시작 (키: 상품코드)... ")
                                    
                                    if '해오름 이미지 URL' not in df_to_save.columns:
                                        df_to_save['해오름 이미지 URL'] = pd.NA # Use pd.NA for consistency
                                    
                                    for idx, row in df_to_save.iterrows():
                                        product_code_val = row['Code']
                                        product_name_val = row['상품명'] # Needed for image_data dict

                                        if pd.notna(product_code_val):
                                            # Convert product_code to string, remove .0 if it was float/int, handle various types
                                            if isinstance(product_code_val, float) and product_code_val.is_integer():
                                                product_code_key = str(int(product_code_val))
                                            else:
                                                product_code_key = str(product_code_val).strip()
                                            
                                            haereum_img_data = original_haereum_image_urls.get(product_code_key)
                                            
                                            # Attempt fallback to product_name if code lookup fails and name is valid
                                            if not haereum_img_data and pd.notna(product_name_val) and str(product_name_val).strip():
                                                product_name_key = str(product_name_val).strip()
                                                haereum_img_data = original_haereum_image_urls.get(product_name_key)
                                                if haereum_img_data:
                                                    logging.debug(f"Row {idx}: Haereum image for Code '{product_code_key}' not found. Using fallback to ProductName '{product_name_key}'.")

                                            orig_url = None
                                            if isinstance(haereum_img_data, dict):
                                                orig_url = haereum_img_data.get('url')
                                            elif isinstance(haereum_img_data, str): # Backward compatibility
                                                orig_url = haereum_img_data
                                            
                                            if orig_url:
                                                # Apply to '해오름 이미지 URL' column
                                                # Ensure orig_url is a string, not a dict if only URL is needed here
                                                if isinstance(orig_url, dict): # Should not happen if haereum_img_data was processed correctly
                                                    actual_url_val = orig_url.get('url', pd.NA)
                                                else:
                                                    actual_url_val = str(orig_url) if pd.notna(orig_url) else pd.NA
                                                
                                                df_to_save.at[idx, '해오름 이미지 URL'] = actual_url_val
                                                
                                                # Update '본사 이미지' column as well
                                                if '본사 이미지' in df_to_save.columns:
                                                    current_bosa_image_val = df_to_save.at[idx, '본사 이미지']
                                                    # Prepare the full image_data dict using the scraped Haereum info
                                                    new_bosa_image_data = {}
                                                    if isinstance(haereum_img_data, dict):
                                                        new_bosa_image_data = haereum_img_data.copy() # Start with the full dict
                                                        new_bosa_image_data['url'] = actual_url_val # Ensure the final URL is there
                                                    else: # If only URL string was stored
                                                        new_bosa_image_data['url'] = actual_url_val
                                                    
                                                    # Ensure common fields are present
                                                    new_bosa_image_data.setdefault('source', 'haereum')
                                                    new_bosa_image_data.setdefault('product_name', product_name_val)
                                                    # Add product_code if available and not already there from scraper
                                                    new_bosa_image_data.setdefault('product_code', product_code_key)

                                                    # If current '본사 이미지' is a dict, update it; otherwise, replace.
                                                    # Also ensure we don't overwrite a more complete dict from image_integration with a less complete one.
                                                    if isinstance(current_bosa_image_val, dict) and current_bosa_image_val.get('local_path'):
                                                        # If current has a local path, it's likely from image_integration. Prioritize its structure.
                                                        current_bosa_image_val['url'] = new_bosa_image_data.get('url', current_bosa_image_val.get('url'))
                                                        current_bosa_image_val['original_path'] = new_bosa_image_data.get('original_path', current_bosa_image_val.get('original_path'))
                                                        current_bosa_image_val['product_code'] = new_bosa_image_data.get('product_code', current_bosa_image_val.get('product_code'))
                                                        df_to_save.at[idx, '본사 이미지'] = current_bosa_image_val
                                                    elif isinstance(current_bosa_image_val, dict):
                                                        current_bosa_image_val.update(new_bosa_image_data)
                                                        df_to_save.at[idx, '본사 이미지'] = current_bosa_image_val
                                                    else: # Replace if not a dict or if it's just a placeholder
                                                        df_to_save.at[idx, '본사 이미지'] = new_bosa_image_data
                                                applied_count += 1
                                        else:
                                            # Product code is missing or NaN
                                            if pd.notna(product_name_val) and str(product_name_val).strip():
                                                # Try lookup by product name if code is missing
                                                product_name_key = str(product_name_val).strip()
                                                haereum_img_data = original_haereum_image_urls.get(product_name_key)
                                                if haereum_img_data:
                                                    logging.debug(f"Row {idx}: ProductCode is missing. Found Haereum image via ProductName '{product_name_key}'.")
                                                    orig_url = None
                                                    if isinstance(haereum_img_data, dict):
                                                        orig_url = haereum_img_data.get('url')
                                                    elif isinstance(haereum_img_data, str):
                                                        orig_url = haereum_img_data
                                                    
                                                    if orig_url: # Simplified application for name-based fallback
                                                        actual_url_val = str(orig_url) if pd.notna(orig_url) else pd.NA
                                                        df_to_save.at[idx, '해오름 이미지 URL'] = actual_url_val
                                                        # Update '본사 이미지' (simplified for this fallback path)
                                                        if '본사 이미지' in df_to_save.columns:
                                                            bosa_img_dict = {
                                                                'url': actual_url_val, 
                                                                'source': 'haereum', 
                                                                'product_name': product_name_val
                                                            }
                                                            if isinstance(haereum_img_data, dict):
                                                                # Merge, ensuring URL is the primary one
                                                                temp_data = haereum_img_data.copy()
                                                                temp_data['url'] = actual_url_val
                                                                bosa_img_dict.update(temp_data)
                                                            df_to_save.at[idx, '본사 이미지'] = bosa_img_dict
                                                        applied_count += 1
                                                
                                    logging.info(f"원본 해오름 이미지 URL {applied_count}개 적용 완료.")
                                else:
                                    logging.warning("'Code' 또는 '상품명' 컬럼이 DataFrame에 없어 해오름 이미지 URL을 적용할 수 없습니다.")
                        except Exception as url_apply_err:
                            logging.error(f"해오름 이미지 URL 적용 중 오류 발생: {url_apply_err}", exc_info=True)
                            # 이 오류는 치명적이지 않으므로 계속 진행
                        
                        # Add Detailed Logging Before Saving
                        if df_to_save is not None and not df_to_save.empty:
                            logging.info("--- DataFrame Snapshot Before Excel Write ---")
                            logging.info(f"Shape: {df_to_save.shape}")
                            logging.info(f"Columns: {df_to_save.columns.tolist()}")
                            logging.info(f"dtypes:\n{df_to_save.dtypes.to_string()}")
                            # Log first 2 rows data, especially image columns
                            image_cols_in_final = [col for col in IMAGE_COLUMNS if col in df_to_save.columns]
                            log_limit = min(2, len(df_to_save))
                            logging.info(f"Sample Data (first {log_limit} rows):")
                            try:
                                # Use to_string for better formatting of rows/cols
                                logging.info(f"\n{df_to_save.head(log_limit).to_string()}")
                                # Specifically log types in image columns for first few rows
                                if image_cols_in_final:
                                    logging.info(f"Image Column Data Types (first {log_limit} rows):")
                                    for i in range(log_limit):
                                        for col in image_cols_in_final:
                                            value = df_to_save.iloc[i][col]
                                            logging.info(f"  Row {i}, Col '{col}': Type={type(value).__name__}, Value=\"{str(value)[:80]}...\"")
                            except Exception as log_snap_err:
                                logging.error(f"Could not log DataFrame snapshot: {log_snap_err}")
                            logging.info("--- End DataFrame Snapshot ---")
                        elif df_to_save is None:
                            logging.warning("Skipping Excel write step because DataFrame finalization failed.")
                        else: # df_to_save is empty
                            logging.warning("DataFrame is empty after finalization. Excel files will have headers only.")

                        # Only proceed to create Excel if finalization succeeded
                        if df_to_save is not None:
                            try:
                                # Create Excel files (even if df_to_save is empty, to get headers)
                                logging.info(f"Proceeding to call create_split_excel_outputs. DataFrame shape: {df_to_save.shape}, Using input_filename: {input_filename}")
                                result_success, upload_success, result_path, upload_path = create_split_excel_outputs(df_to_save, output_path, input_filename)
                                
                                # --- Success/Failure Logging for Excel Creation ---
                                if result_success and upload_success:
                                    logging.info("Successfully created both Excel files:")
                                    logging.info(f"- Result file (with images): {result_path}")
                                    logging.info(f"- Upload file (links only): {upload_path}")

                                    # --- Apply Kogift Price Fixes ---
                                    try:
                                        if result_path and os.path.exists(result_path):
                                            logging.info(f"Applying Kogift price corrections to result file: {result_path}")
                                            fixed_result_path = fix_excel_kogift_images(result_path, result_path) # Overwrite
                                            if fixed_result_path:
                                                logging.info(f"Kogift price corrections applied to result file. Path: {fixed_result_path}")
                                                result_path = fixed_result_path # Update path (should be same if overwriting)
                                            else:
                                                logging.warning(f"Failed to apply Kogift price corrections to result file: {result_path}")
                                        
                                        if upload_path and os.path.exists(upload_path):
                                            logging.info(f"Applying Kogift price corrections to upload file: {upload_path}")
                                            fixed_upload_path = fix_excel_kogift_images(upload_path, upload_path) # Overwrite
                                            if fixed_upload_path:
                                                logging.info(f"Kogift price corrections applied to upload file. Path: {fixed_upload_path}")
                                                upload_path = fixed_upload_path # Update path
                                            else:
                                                logging.warning(f"Failed to apply Kogift price corrections to upload file: {upload_path}")
                                    except Exception as kogift_fix_err:
                                        logging.error(f"Error applying Kogift price corrections: {kogift_fix_err}", exc_info=True)
                                    # --- End Kogift Price Fixes ---
                                    
                                    # --- NEW: Verify images in result Excel file ---
                                    try:
                                        if result_path and os.path.exists(result_path):
                                            logging.info("Verifying image counts in final Excel file...")
                                            image_stats = verify_excel_images(result_path)
                                            
                                            # Log important stats and report to UI
                                            total_images = image_stats.get("actual_images", 0)
                                            naver_urls = image_stats.get("naver_urls", 0)
                                            kogift_urls = image_stats.get("kogift_urls", 0)
                                            
                                            status_msg = f"Image verification: Total={total_images}, Naver={naver_urls}, Kogift={kogift_urls}"
                                            logging.info(status_msg)
                                            
                                            if progress_queue:
                                                progress_queue.emit("status", status_msg)
                                                
                                            # Check for potential issues with Naver images
                                            if naver_urls == 0 and "네이버 이미지" in image_stats.get("column_names", []):
                                                alert_msg = "⚠️ WARNING: No Naver images found despite column existing. Check config.ini [ImageFiltering] section."
                                                logging.warning(alert_msg)
                                                if progress_queue:
                                                    progress_queue.emit("warning", alert_msg)
                                    except Exception as verify_err:
                                        logging.error(f"Error verifying Excel images: {verify_err}", exc_info=True)
                                    # --- End image verification ---

                                    # --- Apply Filter to Upload File (Remove rows with no external data) ---
                                    try:
                                        # Check if upload path is valid before filtering
                                        if upload_path and isinstance(upload_path, str):
                                            logging.info(f"Applying filter to upload file: {upload_path}")
                                            filter_applied = apply_filter_to_upload_excel(upload_path, config)
                                            if filter_applied:
                                                logging.info("Filter successfully applied to upload file.")
                                            else:
                                                logging.warning("Filter could not be applied to the upload file. Proceeding without this filter.")
                                        else:
                                            logging.warning(f"Invalid or missing upload path ({upload_path}), skipping upload file filter.")
                                    except Exception as filter_err:
                                        logging.error(f"Error applying filter to upload file {upload_path}: {filter_err}", exc_info=True)
                                    # --- End Apply Filter ---
                                    
                                    # --- Apply Price Difference Filter to Upload File (Remove rows with price difference >= -1) ---
                                    try:
                                        # Check if upload path is valid before filtering
                                        if upload_path and isinstance(upload_path, str):
                                            logging.info(f"Applying price difference filter to upload file: {upload_path}")
                                            price_filter_applied = filter_by_price_difference(upload_path, config)
                                            if price_filter_applied:
                                                logging.info("Price difference filter successfully applied to upload file.")
                                            else:
                                                logging.warning("Price difference filter could not be applied to the upload file. Proceeding without this filter.")
                                        else:
                                            logging.warning(f"Invalid or missing upload path ({upload_path}), skipping price difference filter.")
                                    except Exception as price_filter_err:
                                        logging.error(f"Error applying price difference filter to upload file {upload_path}: {price_filter_err}", exc_info=True)
                                    # --- End Price Difference Filter ---
                                    
                                    # --- Apply Kogift Data Filter (Clear Kogift data when image is missing) ---
                                    try:
                                        # Apply to result file
                                        if result_path and isinstance(result_path, str) and os.path.exists(result_path):
                                            logging.info(f"Applying Kogift data filter to result file: {result_path}")
                                            result_kogift_filter_applied = apply_kogift_data_filter(result_path, config)
                                            if result_kogift_filter_applied:
                                                logging.info("Kogift data filter successfully applied to result file.")
                                            else:
                                                logging.warning("Kogift data filter could not be applied to the result file.")
                                        
                                        # Apply to upload file
                                        if upload_path and isinstance(upload_path, str) and os.path.exists(upload_path):
                                            logging.info(f"Applying Kogift data filter to upload file: {upload_path}")
                                            upload_kogift_filter_applied = apply_kogift_data_filter(upload_path, config)
                                            if upload_kogift_filter_applied:
                                                logging.info("Kogift data filter successfully applied to upload file.")
                                            else:
                                                logging.warning("Kogift data filter could not be applied to the upload file.")
                                    except Exception as kogift_filter_err:
                                        logging.error(f"Error applying Kogift data filter: {kogift_filter_err}", exc_info=True)
                                    # --- End Kogift Data Filter ---
                                    
                                    # --- Try to fix Naver images in both files ---
                                    try:
                                        # Fix Naver images in result file
                                        if result_path and os.path.exists(result_path):
                                            logging.info(f"Fixing Naver images in result file: {result_path}")
                                            try:
                                                # Load the Excel file
                                                result_df = pd.read_excel(result_path)
                                                # Apply the fix - temporarily disabled
                                                # fixed_result_df = fix_missing_naver_images(result_df, result_file=True)
                                                # Save back to the same file
                                                # fixed_result_df.to_excel(result_path, index=False)
                                                # logging.info("Naver image fixing temporarily disabled")

                                                # Pass the config object to the function
                                                fixed_result_df = fix_missing_naver_images(result_df, result_file=True, config_obj=config)
                                                fixed_result_df.to_excel(result_path, index=False)
                                                logging.info(f"Naver image paths in result file {result_path} checked and updated.")
                                            except Exception as result_fix_err:
                                                logging.warning(f"Non-critical error fixing Naver images in result file: {result_fix_err}. Proceeding without this fix.")
                                        
                                        # Fix Naver images in upload file (with result_file=False flag)
                                        if upload_path and os.path.exists(upload_path):
                                            logging.info(f"Fixing Naver images in upload file: {upload_path}")
                                            try:
                                                # Load the Excel file
                                                upload_df = pd.read_excel(upload_path)
                                                # Apply the fix - temporarily disabled
                                                # fixed_upload_df = fix_missing_naver_images(upload_df, result_file=True)
                                                # Save back to the same file
                                                # fixed_upload_df.to_excel(upload_path, index=False)
                                                # logging.info("Naver image fixing temporarily disabled")

                                                # Pass the config object to the function for upload file as well, 
                                                # but with result_file=False (as original logic might have intended to differentiate)
                                                # Note: fix_missing_naver_images currently doesn't use result_file much internally after recent changes.
                                                fixed_upload_df = fix_missing_naver_images(upload_df, result_file=False, config_obj=config)
                                                fixed_upload_df.to_excel(upload_path, index=False)
                                                logging.info(f"Naver image paths in upload file {upload_path} checked and updated.")
                                            except Exception as upload_fix_err:
                                                logging.warning(f"Non-critical error fixing Naver images in upload file: {upload_fix_err}. Proceeding without this fix.")
                                    except Exception as fix_err:
                                        logging.error(f"Error fixing Naver images in Excel files: {fix_err}", exc_info=True)
                                        # Don't stop the process for Naver image errors - continue with other steps
                                    # --- End Naver Image Fix ---

                                    # --- Place Images in Excel (NEW) ---
                                    try:
                                        # 결과 Excel 파일에 이미지 배치
                                        if result_path and os.path.exists(result_path):
                                            logging.info(f"결과 파일 이미지 배치 시작: {result_path}")
                                            # 결과 Excel 파일 로드
                                            result_df = pd.read_excel(result_path)
                                            # 이미지 배치 실행
                                            create_excel_with_placed_images(result_df, result_path)
                                            logging.info(f"결과 파일 이미지 배치 완료: {result_path}")
                                    except Exception as image_place_err:
                                        logging.error(f"Excel 이미지 배치 중 오류 발생: {image_place_err}", exc_info=True)
                                        # 이미지 배치 실패는 치명적인 오류가 아니므로 계속 진행
                                    # --- End Image Placement ---

                                    # --- Apply Excel Formatting (NEW) ---
                                    try:
                                        logging.info("Applying final Excel formatting to result and upload files...")
                                        format_success_count, total_format_files = apply_excel_formatting(
                                            result_path=result_path if result_success else None,
                                            upload_path=upload_path if upload_success else None
                                        )
                                        
                                        if format_success_count > 0:
                                            logging.info(f"Excel formatting successfully applied to {format_success_count}/{total_format_files} files")
                                            if progress_queue:
                                                progress_queue.emit("status", f"Excel formatting applied to {format_success_count} files")
                                        else:
                                            logging.warning("Excel formatting could not be applied to any files")
                                    except Exception as format_err:
                                        logging.error(f"Error applying Excel formatting: {format_err}", exc_info=True)
                                        # Don't treat formatting failure as a critical error, continue with the process
                                    # --- End Excel Formatting ---

                                    # --- Apply Price Highlighting to Excel files ---
                                    try:
                                        logging.info("Applying price difference highlighting to the generated Excel files...")
                                        # Get threshold value from config, default to -1
                                        threshold = config.getfloat('PriceHighlighting', 'threshold', fallback=-1)
                                        logging.info(f"Using price difference threshold: {threshold}")
                                        
                                        # Apply highlighting to both result and upload files
                                        highlight_success_count, total_files = apply_price_highlighting_to_files(
                                            result_path=result_path if result_success else None,
                                            upload_path=upload_path if upload_success else None,
                                            threshold=threshold
                                        )
                                        
                                        if highlight_success_count > 0:
                                            logging.info(f"Price highlighting successfully applied to {highlight_success_count}/{total_files} files")
                                            if progress_queue:
                                                progress_queue.emit("status", f"Price highlighting applied to {highlight_success_count} files")
                                        else:
                                            logging.warning("Price highlighting could not be applied to any files")
                                    except Exception as highlight_err:
                                        logging.error(f"Error applying price highlighting: {highlight_err}", exc_info=True)
                                        # Don't treat highlighting failure as a critical error, continue with the process
                                    # --- End Price Highlighting ---
                                    
                                    # --- Send Excel files by email ---
                                    try:
                                        # Check if email functionality is enabled in config
                                        email_enabled = config.getboolean('Email', 'enabled', fallback=False)
                                        
                                        if email_enabled:
                                            logging.info("Email functionality is enabled. Validating email configuration...")
                                            # Validate email configuration
                                            if validate_email_config(config):
                                                logging.info("Email configuration is valid. Preparing to send email...")
                                                
                                                # Prepare paths for email
                                                excel_paths = {
                                                    'result': result_path if result_success else None,
                                                    'upload': upload_path if upload_success else None
                                                }
                                                
                                                # Get email subject prefix from config
                                                subject_prefix = config.get('Email', 'email_subject_prefix', fallback="ShopRPA 결과")
                                                
                                                # Send email
                                                email_sent = send_excel_by_email(excel_paths, config, subject_prefix)
                                                
                                                if email_sent:
                                                    logging.info("Email sent successfully with Excel attachments.")
                                                    if progress_queue:
                                                        progress_queue.emit("status", "Email sent successfully.")
                                                else:
                                                    logging.warning("Failed to send email with Excel attachments.")
                                                    if progress_queue:
                                                        progress_queue.emit("status", "Failed to send email.")
                                            else:
                                                logging.warning("Email configuration is invalid. Email will not be sent.")
                                                if progress_queue:
                                                    progress_queue.emit("status", "Email configuration is invalid.")
                                        else:
                                            logging.info("Email functionality is disabled in configuration.")
                                    except Exception as email_err:
                                        logging.error(f"Error in email sending step: {email_err}", exc_info=True)
                                        # Don't treat email failure as a critical error, continue with the process
                                    # --- End Email Sending ---
                                        
                                    if progress_queue:
                                        progress_queue.emit("status", "Output files saved successfully")
                                        if isinstance(upload_path, str) and os.path.exists(upload_path):
                                            progress_queue.emit("final_path", upload_path)
                                            logging.info(f"Emitting final upload path: {upload_path}")
                                        else:
                                            logging.warning(f"Upload path is invalid or does not exist: {upload_path}")
                                            progress_queue.emit("final_path", "Error: Upload file not found")
                                else:
                                    # Check if files were actually created despite the reported failure
                                    files_exist = (result_path and os.path.exists(result_path)) or (upload_path and os.path.exists(upload_path))
                                    
                                    if files_exist:
                                        # At least one file was created successfully despite the failure flags
                                        logging.info("Files were created successfully despite reported failure, continuing with processing")
                                        
                                        # Update success flags since files exist
                                        if result_path and os.path.exists(result_path):
                                            result_success = True
                                        if upload_path and os.path.exists(upload_path):
                                            upload_success = True
                                            
                                        # Emit success message
                                        if progress_queue:
                                            progress_queue.emit("status", "Output files saved successfully")
                                            if upload_path and os.path.exists(upload_path):
                                                progress_queue.emit("final_path", upload_path)
                                                logging.info(f"Emitting final upload path: {upload_path}")
                                    else:
                                        # No files were created, log error
                                        logging.error("엑셀 파일 생성 실패 (create_split_excel_outputs). 이전 로그를 확인하세요.")
                                        if progress_queue:
                                            progress_queue.emit("error", "Failed to create one or both Excel output files.")
                                        output_path = None
                            except Exception as save_err:
                                error_msg = f"Failed during Excel creation step: {str(save_err)}"
                                logging.error(f"[Step 7/7] {error_msg}", exc_info=True)
                                if progress_queue:
                                    progress_queue.emit("error", error_msg)
                                output_path = None
                                result_success, upload_success = False, False
                except Exception as finalize_err:
                    logging.error(f"Error during DataFrame finalization step: {finalize_err}", exc_info=True)
                    if progress_queue:
                        progress_queue.emit("error", f"Error finalizing data: {finalize_err}")
                    result_success, upload_success = False, False
                    result_path, upload_path = None, None
                    output_path = None
            except Exception as e:
                error_msg = f"Error during output file saving: {str(e)}"
                logging.error(f"[Step 7/7] {error_msg}", exc_info=True)
                if progress_queue: progress_queue.emit("error", error_msg)
                output_path = None
                result_success, upload_success = False, False
            else:
                if not input_filename:
                    error_msg = "Could not determine input filename base, cannot save output file"
                    logging.error(f"[Step 7/7] {error_msg}")
                    if progress_queue: progress_queue.emit("error", error_msg)
                    output_path = None
                    result_success, upload_success = False, False

            # --- Final Summary ---
            total_time = time.time() - main_start_time
            logging.info(f"========= RPA Process Finished - Total Time: {total_time:.2f} sec ==========")
            if progress_queue:
                # Check for any valid output path - prefer upload_path if it exists
                valid_path = None
                # First check upload_path (most important for final output)
                if upload_path and os.path.exists(upload_path):
                    valid_path = upload_path
                    logging.info(f"Using upload_path as final output path: {valid_path}")
                # Then check result_path
                elif result_path and os.path.exists(result_path):
                    valid_path = result_path
                    logging.info(f"Using result_path as final output path: {valid_path}")
                # Lastly check output_path
                elif output_path:
                    valid_path = output_path
                    logging.info(f"Using base output_path as final path: {valid_path}")
                
                # Now use valid_path for final emission
                if valid_path:
                    if os.path.exists(valid_path):
                        logging.info(f"Emitting final path to UI: {valid_path}")
                        progress_queue.emit("final_path", valid_path)
                    else:
                        logging.warning(f"Final path does not exist: {valid_path}")
                        progress_queue.emit("final_path", f"Error: File not found at {valid_path}")
                else:
                    # No valid path found
                    logging.warning("No valid output path available")
                    progress_queue.emit("final_path", "Error: No output file created")
                
                # Then mark the process as finished
                progress_queue.emit("finished", "True")

    except Exception as e:
        logging.error(f"Error in main: {e}", exc_info=True)
        if progress_queue:
            progress_queue.emit("error", str(e))
            progress_queue.emit("finished", "False")
        return

def run_cli():
    """Run the RPA process in CLI mode"""
    # Remove the --cli flag from sys.argv before parsing arguments
    if '--cli' in sys.argv:
        sys.argv.remove('--cli')
        
    parser = argparse.ArgumentParser(description="Run ShopRPA process.")
    parser.add_argument("-c", "--config", default=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.ini'), 
                      help="Path to configuration file (config.ini).")
    args = parser.parse_args()

    # Initialize environment and run RPA
    CONFIG, gpu_available_detected, validation_passed = initialize_environment(args.config)
    if not validation_passed:
        logging.error("Environment validation failed. Exiting.")
        sys.exit(1)

    try:
        asyncio.run(main(config=CONFIG, gpu_available=gpu_available_detected, progress_queue=None))
    except KeyboardInterrupt:
        logging.warning("RPA process interrupted by user.")
        print("\nProcess interrupted by user.")
    except Exception as e:
        logging.critical(f"An unhandled exception occurred: {e}", exc_info=True)
        print(f"\nAn critical error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Check if running in CLI mode
    if len(sys.argv) > 1 and sys.argv[1] == "--cli":
        run_cli()
    else:
        print("Please use --cli flag to run in command line mode, or use the GUI application.")