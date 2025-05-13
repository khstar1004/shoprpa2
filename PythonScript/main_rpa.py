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
from email_sender import validate_email_config, send_excel_by_email

# --- Import Refactored Modules ---
from matching_logic import match_products, post_process_matching_results
from data_processing import process_input_file, filter_results, format_product_data_for_output
from excel_data_processing import find_excel_file, finalize_dataframe_for_excel
from excel_utils import (
    excel_generator,  # Changed: Now using singleton instance
    IMAGE_COLUMNS
)
from crawling_logic import crawl_all_sources
from utils import preprocess_and_download_images
from execution_setup import initialize_environment, clear_temp_files, _load_and_validate_config
from image_integration import integrate_and_filter_images
from price_highlighter import apply_price_highlighting_to_files
from upload_filter import apply_filter_to_upload_excel
from fix_naver_images import fix_naver_images  # Add this import

async def main(config: configparser.ConfigParser, gpu_available: bool, progress_queue=None):
    """Main function orchestrating the RPA process (now asynchronous)."""
    try:
        main_start_time = time.time()
        logging.info("========= RPA Process Starting ========")

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
                    max_workers=download_workers
                )
                processed_count = len(input_file_image_map)
            else:
                input_file_image_map = {}
                processed_count = 0
                
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
            logging.info(f"원본 해오름 이미지 URL {len(original_haereum_image_urls)}개를 안전하게 보관하였습니다. 엑셀 생성 단계에서 사용 예정.")
            
            # 해오름 이미지 URL 디버그 로깅 (최대 5개)
            if debug_mode and original_haereum_image_urls:
                sample_count = 0
                for prod_name, url in list(original_haereum_image_urls.items())[:5]:
                    logging.debug(f"보관된 해오름 이미지 URL 샘플 #{sample_count+1}: {prod_name} -> {url}")
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
                                image_data = {
                                    'url': img_url,
                                    'local_path': item['image_path'], # Use corrected path
                                    'original_path': item.get('original_path', item['image_path']), # Keep original if available
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
                                image_data = {
                                    'url': img_url,
                                    'local_path': item['image_path'], # Use corrected path
                                    'original_path': item.get('original_path', item['image_path']), # Keep original if available
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
                    max_workers=download_workers
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
                # Get base name without extension for output file naming
                input_filename_base = os.path.splitext(input_filename)[0]
                
                # First ensure that all image URLs are properly included
                logging.info("Formatting product data with image URLs for output...")
                
                # Make sure all crawled images are included in the Excel output
                formatted_df = format_product_data_for_output(
                    input_df=filtered_df, 
                    kogift_results=kogift_map, 
                    naver_results=naver_map,
                    input_file_image_map=input_file_image_map
                )
                
                # Create output directory if it doesn't exist
                output_dir = config.get('Paths', 'output_dir')
                os.makedirs(output_dir, exist_ok=True)
                
                # Generate output filename
                try:
                    # Get company name and management type
                    company_name = "Unknown"
                    mgmt_type = "승인관리"  # Default
                    
                    # Try to get company name from DataFrame
                    if '업체명' in formatted_df.columns and not formatted_df.empty:
                        company_names = formatted_df['업체명'].unique()
                        if len(company_names) == 1:
                            company_name = company_names[0]
                        else:
                            # If multiple companies, use the most frequent one
                            company_name = formatted_df['업체명'].mode().iloc[0]
                    
                    # Get management type from '구분' column
                    if '구분' in formatted_df.columns and not formatted_df.empty:
                        mgmt_val = formatted_df['구분'].iloc[0]
                        if mgmt_val == 'A':
                            mgmt_type = "승인관리"
                        elif mgmt_val == 'P':
                            mgmt_type = "가격관리"
                    
                    # Get row count
                    row_count = len(formatted_df)
                    
                    # Generate timestamp
                    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    date_part = datetime.datetime.now().strftime("%Y%m%d")
                    
                    # Create base filename
                    base_filename = f"{company_name}({row_count}개)-{mgmt_type}-{date_part}"
                    
                    # Generate result and upload filenames
                    result_filename = f"{base_filename}_result_{timestamp}.xlsx"
                    upload_filename = f"{base_filename}_upload_{timestamp}.xlsx"
                    
                    # Set output paths
                    result_path = os.path.join(output_dir, result_filename)
                    upload_path = os.path.join(output_dir, upload_filename)
                    
                    logging.info(f"Generated filenames:")
                    logging.info(f"Result file: {result_filename}")
                    logging.info(f"Upload file: {upload_filename}")
                    
                except Exception as e:
                    logging.error(f"Error generating filenames: {e}", exc_info=True)
                    # Fallback to simple timestamp-based names if error occurs
                    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    result_path = os.path.join(output_dir, f"result_{timestamp}.xlsx")
                    upload_path = os.path.join(output_dir, f"upload_{timestamp}.xlsx")

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
                        if progress_queue: progress_queue.emit("error", "Error during data finalization stage.")
                        # Skip Excel creation steps
                        result_success, upload_success = False, False
                        result_path, upload_path = None, None
                    else:
                        logging.info(f"DataFrame finalized successfully. Shape: {df_to_save.shape}")
                        logging.debug(f"Finalized columns: {df_to_save.columns.tolist()}")
                        
                        # 여기서 원본 해오름 이미지 URL을 DataFrame에 적용
                        try:
                            # 해오름 이미지 URL을 엑셀 데이터에 적용하는 로직
                            if original_haereum_image_urls and not df_to_save.empty:
                                # '상품명' 컬럼이 있는지 확인
                                if '상품명' in df_to_save.columns:
                                    applied_count = 0
                                    logging.info(f"원본 해오름 이미지 URL ({len(original_haereum_image_urls)}개) 적용 시작...")
                                    
                                    # 원본 이미지 URL을 저장할 새 컬럼 생성
                                    if '해오름 이미지 URL' not in df_to_save.columns:
                                        df_to_save['해오름 이미지 URL'] = '-'  # 기본값 설정
                                    
                                    # 각 행에 원본 URL 적용
                                    for idx, row in df_to_save.iterrows():
                                        product_name = row['상품명']
                                        if product_name in original_haereum_image_urls:
                                            orig_url = original_haereum_image_urls[product_name]
                                            if orig_url:
                                                df_to_save.at[idx, '해오름 이미지 URL'] = orig_url
                                                
                                                # 본사 이미지 컬럼이 있으면 해당 컬럼에도 URL 적용 (딕셔너리 형태면 url 키에 적용)
                                                if '본사 이미지' in df_to_save.columns:
                                                    current_value = df_to_save.at[idx, '본사 이미지']
                                                    if isinstance(current_value, dict):
                                                        current_value['url'] = orig_url
                                                        df_to_save.at[idx, '본사 이미지'] = current_value
                                                    else:
                                                        # 딕셔너리 아닌 경우 새로 생성
                                                        image_data = {
                                                            'url': orig_url,
                                                            'source': 'haereum',
                                                            'product_name': product_name
                                                        }
                                                        df_to_save.at[idx, '본사 이미지'] = image_data
                                                applied_count += 1
                                                
                                    logging.info(f"원본 해오름 이미지 URL {applied_count}개 적용 완료.")
                                else:
                                    logging.warning("'상품명' 컬럼이 DataFrame에 없어 해오름 이미지 URL을 적용할 수 없습니다.")
                            else:
                                if not original_haereum_image_urls:
                                    logging.warning("적용할 원본 해오름 이미지 URL이 없습니다.")
                                if df_to_save.empty:
                                    logging.warning("DataFrame이 비어있어 해오름 이미지 URL을 적용할 수 없습니다.")
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
                                logging.info(f"Proceeding to call create_excel_output. DataFrame shape: {df_to_save.shape}")
                                result_success, upload_success, result_path, upload_path = excel_generator.create_excel_output(
                                    df=df_to_save,
                                    output_path=os.path.join(output_dir, f"{input_filename_base}_{timestamp}"),
                                    create_upload_file=True
                                )
                                
                                # --- Success/Failure Logging for Excel Creation ---
                                if result_success and upload_success:
                                    logging.info("Successfully created both Excel files:")
                                    logging.info(f"- Result file (with images): {result_path}")
                                    logging.info(f"- Upload file (links only): {upload_path}")

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

                                    # --- Apply Excel Formatting (NEW) ---
                                    try:
                                        logging.info("Applying final Excel formatting to result and upload files...")
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
                                    except Exception as format_err:
                                        logging.error(f"Error applying Excel formatting: {format_err}", exc_info=True)
                                        # Don't treat formatting failure as a critical error, continue with the process
                                    # --- End Excel Formatting ---

                                    # --- NEW: Validate and Fix Naver Images ---
                                    try:
                                        logging.info("Validating Naver image placements...")
                                        if result_path and os.path.exists(result_path):
                                            # Run Naver image validation on result file
                                            naver_validation_success = fix_naver_images(result_path)
                                            if naver_validation_success:
                                                logging.info("Naver image validation completed successfully")
                                                if progress_queue:
                                                    progress_queue.emit("status", "Naver images validated and fixed if needed")
                                            else:
                                                logging.warning("Naver image validation completed with issues")
                                                if progress_queue:
                                                    progress_queue.emit("status", "Issues found with Naver images - check report")
                                    except Exception as naver_err:
                                        logging.error(f"Error during Naver image validation: {naver_err}", exc_info=True)
                                        # Don't treat validation failure as a critical error
                                    # --- End Naver Image Validation ---

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
                                    logging.error("엑셀 파일 생성 실패 (create_excel_output). 이전 로그를 확인하세요.")
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
                # First send the result path (which is now upload_path) if available
                if output_path: # output_path here corresponds to the base name structure, but the actual final path is emitted above
                    # Check if the specifically emitted upload_path exists
                    final_emitted_path = upload_path # Use the variable we know holds the upload path
                    if final_emitted_path and os.path.exists(final_emitted_path):
                        logging.info(f"Emitting final upload path: {final_emitted_path}")
                        # Ensure final_path signal emission logic remains consistent (already done above)
                        # progress_queue.emit("final_path", final_emitted_path) # This is now redundant as it's emitted earlier
                    else:
                        logging.warning(f"Upload path does not exist or was not generated: {final_emitted_path}")
                        progress_queue.emit("final_path", f"Error: Upload file not found at {final_emitted_path}")
                else:
                    # If output_path wasn't even determined (earlier error maybe)
                    logging.warning("No base output path available, cannot check for upload file.")
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