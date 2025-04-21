import os
import sys
import logging
import argparse
import time
import asyncio
import multiprocessing
import pandas as pd # Added back as it's used for DataFrame checks/creation
from concurrent.futures import ThreadPoolExecutor # Keep, might be used internally by some sync functions
import configparser # Import configparser
import hashlib
import datetime

# --- Import Refactored Modules ---
from matching_logic import match_products, _init_worker_matcher
from data_processing import process_input_file, filter_results, format_product_data_for_output # Added format_product_data_for_output
from excel_utils import create_final_output_excel # Added new excel output function
from crawling_logic import crawl_all_sources
from utils import preprocess_and_download_images
from execution_setup import initialize_environment, clear_temp_files, _load_and_validate_config # Import the refactored config loader

# --- Global Variables (Keep essential ones) ---
# CONFIG = None # Loaded via initialize_environment
# gpu_available_detected = False # Set via initialize_environment

async def main(config: configparser.ConfigParser, gpu_available: bool, progress_queue=None):
    """Main function orchestrating the RPA process (now asynchronous)."""
    main_start_time = time.time() # Renamed for clarity
    logging.info("========= RPA Process Starting ========")

    # --- Concurrency Settings ---
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
            matcher_workers = config.getint('Concurrency', 'matcher_max_workers_gpu', fallback=2)
            logging.info(f"GPU detected. Using up to {matcher_workers} CPU workers for matching coordination/CPU-bound tasks (GPU handles main load).")
        except (configparser.NoSectionError, configparser.NoOptionError, ValueError):
            logging.warning("MATCHER_MAX_WORKERS_GPU not found in config [Concurrency]. Using default GPU worker setting (2).")
            matcher_workers = 2
    else:
        matcher_workers = matcher_workers_config
        logging.info(f"No GPU detected. Using up to {matcher_workers} CPU workers for matching.")
    logging.info(f"Using up to {download_workers} workers for downloads/preprocessing.")

    # Environment setup (config, logging, dirs, gpu, validation) is done before calling main

    # 1. Clear previous temp files
    step_start_time = time.time()
    logging.info("[Step 1/7] Clearing temporary files...")
    clear_temp_files(config)
    logging.info(f"[Step 1/7] Temporary files cleared. Duration: {time.time() - step_start_time:.2f} sec")
    if progress_queue: progress_queue.put(("status", "Cleared temporary files"))

    # 2. Process Input File
    step_start_time = time.time()
    logging.info("[Step 2/7] Reading and processing input Excel file...")
    if progress_queue: progress_queue.put(("status", "Reading input file..."))
    haoreum_df, input_filename = process_input_file(config) # Now uses chunking internally
    if haoreum_df is None or haoreum_df.empty:
        logging.error("No valid input data found or read error. Exiting.")
        if progress_queue: progress_queue.put(("error", "No input data"))
        return
    total_products = len(haoreum_df)
    logging.info(f"[Step 2/7] Input file processed. Found {total_products} products. Duration: {time.time() - step_start_time:.2f} sec")
    if progress_queue: progress_queue.put(("status", f"Read {total_products} products from input."))

    # 2.5 Preprocess Haoreum Images from Input
    step_start_time = time.time()
    logging.info("[Step 3/7] Preprocessing images from input file (if any)...")
    if progress_queue: progress_queue.put(("status", "Preprocessing input images..."))
    # This uses ThreadPoolExecutor internally, could be wrapped in to_thread if it blocks significantly
    # For now, assume it's acceptable as it's primarily I/O bound.
    input_file_image_map = await preprocess_and_download_images(
        df=haoreum_df,
        url_column_name='본사 이미지',
        id_column_name='Code',
        prefix='input',
        config=config,
        max_workers=download_workers
    )
    processed_count = len(input_file_image_map)
    logging.info(f"[Step 3/7] Input file images preprocessed. Processed {processed_count} images. Duration: {time.time() - step_start_time:.2f} sec")
    if progress_queue: progress_queue.put(("status", "Finished preprocessing input images."))

    # 3. Crawl External Data Concurrently
    step_start_time = time.time()
    logging.info("[Step 4/7] Starting concurrent crawls (Kogift, Naver, Haereum URLs)...")
    if progress_queue: progress_queue.put(("status", "Crawling external sites..."))
    kogift_crawl_results, naver_crawl_results, haereum_image_url_map = None, None, None # Init
    try:
        # Pass ConfigParser object to crawl_all_sources
        kogift_crawl_results, naver_crawl_results, haereum_image_url_map = await crawl_all_sources(haoreum_df, config)
        logging.info(f"[Step 4/7] Concurrent crawls finished. Duration: {time.time() - step_start_time:.2f} sec")
        if progress_queue: progress_queue.put(("status", "Finished concurrent crawls."))
    except Exception as crawl_err:
        logging.error(f"[Step 4/7] Error during crawl_all_sources execution: {crawl_err}", exc_info=True)
        logging.info(f"[Step 4/7] Concurrent crawls failed. Duration: {time.time() - step_start_time:.2f} sec")
        kogift_crawl_results, naver_crawl_results, haereum_image_url_map = [], [], {} # Set empty defaults
        if progress_queue: progress_queue.put(("error", "Crawling failed"))

    # --- Process Crawl Results (Handle potential failures) ---
    kogift_crawl_results = kogift_crawl_results if kogift_crawl_results is not None else []
    naver_crawl_results = naver_crawl_results if naver_crawl_results is not None else []
    haereum_image_url_map = haereum_image_url_map if haereum_image_url_map is not None else {}
    if not kogift_crawl_results: logging.warning("Kogift crawl resulted in empty data.")
    if not naver_crawl_results: logging.warning("Naver crawl resulted in empty data.")
    if not haereum_image_url_map: logging.warning("Haereum URL crawl resulted in empty data.")


    # --- Merge & Download Crawled Haereum Data ---
    merge_dl_start_time = time.time()
    logging.info("Merging crawled Haereum image URLs and downloading images...")
    # Merge URLs
    haoreum_df['해오름이미지URL'] = haoreum_df['상품명'].map(haereum_image_url_map).fillna('')
    added_url_count = (haoreum_df['해오름이미지URL'] != '').sum()
    logging.debug(f"Merged {added_url_count} Haereum image URLs.")
    # Download Images
    crawled_haereum_image_path_map = await preprocess_and_download_images(
        df=haoreum_df[haoreum_df['해오름이미지URL'] != ''], # Only process rows with URLs
        url_column_name='해오름이미지URL',
        id_column_name='Code',
        prefix='haereum',
        config=config,
        max_workers=download_workers
    )
    # Merge Paths
    haoreum_df['해오름이미지경로'] = haoreum_df['Code'].map(crawled_haereum_image_path_map).fillna('')
    added_path_count = (haoreum_df['해오름이미지경로'] != '').sum()
    logging.info(f"Downloaded and merged {added_path_count} crawled Haereum images. Duration: {time.time() - merge_dl_start_time:.2f} sec")
    if progress_queue: progress_queue.put(("status", "Processed crawled Haereum images."))


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

    naver_map = {}
    if naver_crawl_results:
        try:
            # Assuming naver_crawl_results is a list of dicts [{'original_row': row, 'naver_data': data, ...}]
            naver_map = {
                entry['original_row'].get('상품명'): entry['naver_data']
                for entry in naver_crawl_results
                if entry and isinstance(entry.get('original_row'), dict) and entry['original_row'].get('상품명') and entry.get('naver_data') is not None
            }
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
        except Exception as e:
             logging.error(f"Error creating Naver map: {e}", exc_info=True)
             naver_map = {}

    logging.info(f"Data maps prepared for matching. Duration: {time.time() - map_prep_start_time:.2f} sec")
    
    # 고려기프트 이미지 다운로드 사전 확인 (선택적)
    if kogift_map and config.getboolean('Matching', 'predownload_kogift_images', fallback=False):
        logging.info("고려기프트 이미지 사전 다운로드 시작...")
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
                prefix='kogift_pre',
                config=config,
                max_workers=download_workers
            )
            
            download_success = len(kogift_image_map)
            logging.info(f"고려기프트 이미지 사전 다운로드 완료: {download_success}/{len(img_urls_to_download)} 성공. 소요시간: {time.time() - kogift_img_start_time:.2f}초")
        else:
            logging.warning("다운로드할 고려기프트 이미지 URL이 없습니다.")


    # 4. Match Products (Run in thread to avoid blocking asyncio loop)
    step_start_time = time.time()
    logging.info(f"[Step 5/7] Starting product matching (GPU: {gpu_available}, CPU Workers: {matcher_workers})...")
    if progress_queue: progress_queue.put(("status", "Matching products..."))
    matched_df = pd.DataFrame() # Initialize empty DataFrame
    try:
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
        match_count = len(matched_df) if not matched_df.empty else 0
        logging.info(f"[Step 5/7] Product matching finished. Matched {match_count} potential rows. Duration: {time.time() - step_start_time:.2f} sec")

    except Exception as match_err:
        logging.error(f"[Step 5/7] Error during product matching: {match_err}", exc_info=True)
        logging.info(f"[Step 5/7] Product matching failed. Duration: {time.time() - step_start_time:.2f} sec")
        # matched_df remains empty
        if progress_queue: progress_queue.put(("error", "Matching failed"))


    if matched_df.empty:
        logging.warning("Matching resulted in an empty DataFrame. No data to filter or output.")
        total_time = time.time() - main_start_time
        logging.info(f"========= RPA Process Finished (No Matching Results) - Total Time: {total_time:.2f} sec ==========")
        if progress_queue: progress_queue.put(("finished", True))
        return # Exit early


    # 5. Filter Results
    step_start_time = time.time()
    logging.info(f"[Step 6/7] Filtering {len(matched_df)} matched rows...")
    if progress_queue: progress_queue.put(("status", "Filtering results..."))
    filtered_df = filter_results(matched_df, config, progress_queue)
    filter_count = len(filtered_df)
    logging.info(f"[Step 6/7] Filtering finished. {filter_count} rows remaining. Duration: {time.time() - step_start_time:.2f} sec")

    if filtered_df.empty:
        logging.warning("Filtering removed all rows. No data to output.")
        total_time = time.time() - main_start_time
        logging.info(f"========= RPA Process Finished (No Filtered Output) - Total Time: {total_time:.2f} sec ==========")
        if progress_queue: progress_queue.put(("finished", True))
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
                naver_results=naver_map
            )
            
            # Add additional logic to ensure Haereum images are included
            if '본사 이미지' in formatted_df.columns and '해오름이미지URL' in formatted_df.columns:
                # Use the Haereum image URL if the original image is missing
                haoreum_img_missing = (formatted_df['본사 이미지'].isnull()) | (formatted_df['본사 이미지'] == '') | (formatted_df['본사 이미지'] == '-')
                haoreum_url_present = ~(formatted_df['해오름이미지URL'].isnull() | (formatted_df['해오름이미지URL'] == ''))
                
                # Only update cells that need it
                update_mask = haoreum_img_missing & haoreum_url_present
                if update_mask.any():
                    formatted_df.loc[update_mask, '본사 이미지'] = formatted_df.loc[update_mask, '해오름이미지URL']
                    logging.info(f"Updated {update_mask.sum()} missing Haereum images with crawled URLs")
            
            input_filename_base = input_filename.rsplit('.', 1)[0]
            # Create output path using config
            output_dir = config.get('Paths', 'output_dir')
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(output_dir, f"{input_filename_base}_{timestamp}.xlsx")
            
            # Call the create_final_output_excel with the prepared output path
            output_path = create_final_output_excel(formatted_df, output_path)
            logging.info(f"[Step 7/7] Output file saved and formatted: {output_path}. Duration: {time.time() - step_start_time:.2f} sec")
        except Exception as save_err:
             logging.error(f"[Step 7/7] Failed to save or format output file: {save_err}", exc_info=True)
             logging.info(f"[Step 7/7] Saving/Formatting failed. Duration: {time.time() - step_start_time:.2f} sec")
    else:
        logging.error("[Step 7/7] Could not determine input filename base, cannot save output file with standard naming.")


    # --- Final Summary ---
    total_time = time.time() - main_start_time
    logging.info(f"========= RPA Process Finished - Total Time: {total_time:.2f} sec ==========")
    if progress_queue:
        progress_queue.put(("finished", True))
        progress_queue.put(("final_path", output_path if output_path else "Error")) # Send final path or error


if __name__ == "__main__":
    # --- Initialization ---
    parser = argparse.ArgumentParser(description="Run ShopRPA process.")
    parser.add_argument("-c", "--config", default=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.ini'), help="Path to configuration file (config.ini).")
    args = parser.parse_args()

    # Initialize environment (loads config, sets up logging, detects gpu, ensures dirs, validates)
    CONFIG, gpu_available_detected, validation_passed = initialize_environment(args.config)

    if not validation_passed:
         logging.error("Environment validation failed. Exiting.")
         sys.exit(1)

    # Multiprocessing setup for 'process' mode (if needed by match_products)
    # Note: _init_worker_matcher might need adjustments if used with asyncio.to_thread
    if CONFIG.get('Settings', 'MATCHER_EXECUTOR_TYPE', fallback='thread').lower() == 'process':
         # Ensure spawn method for compatibility, especially on Windows/macOS
        if sys.platform.startswith('win') or sys.platform.startswith('darwin'):
             try:
                  if multiprocessing.get_start_method(allow_none=True) != 'spawn':
                       multiprocessing.set_start_method('spawn', force=True)
                       logging.info("Set multiprocessing start method to 'spawn'.")
             except Exception as e:
                  logging.warning(f"Could not force multiprocessing start method to 'spawn': {e}")
        logging.info(f"Using multiprocessing start method: {multiprocessing.get_start_method()}")
        # Worker initialization (_init_worker_matcher) happens within match_products when ProcessPoolExecutor is created.

    # --- Run Main Process using asyncio ---
    try:
        # Pass None for progress_queue if running standalone
        asyncio.run(main(config=CONFIG, gpu_available=gpu_available_detected, progress_queue=None))
    except KeyboardInterrupt:
        logging.warning("RPA process interrupted by user.")
        print("\nProcess interrupted by user.") # User feedback
    except Exception as e:
        logging.critical(f"An unhandled exception occurred in the main async process: {e}", exc_info=True)
        # Consider more user-friendly error message here if needed
        print(f"\nAn critical error occurred: {e}")
        sys.exit(1)
    finally:
        logging.info("========= Main script execution finished ==========")