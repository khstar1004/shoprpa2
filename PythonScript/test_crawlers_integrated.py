"""
Integrated test script for crawlers and image download functionality.
Tests Kogift, Haereum, and Naver crawlers with image download capabilities.
"""

import asyncio
import logging
import os
from datetime import datetime
from playwright.async_api import async_playwright
import pandas as pd
import httpx

# Import crawler modules
from crowling_kogift import scrape_data as scrape_kogift
from crawling_haereum_standalone import scrape_haereum_data
from crawl_naver_api import crawl_naver_products
from utils import load_config, get_async_httpx_client

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)
logger = logging.getLogger("integrated_test")

def validate_config(config):
    """Validate configuration and create necessary directories."""
    required_paths = ['image_main_dir', 'image_target_dir', 'temp_dir']
    for path_key in required_paths:
        if not config.has_option('Paths', path_key):
            raise ValueError(f"Missing required config option: Paths.{path_key}")
        
        path = config.get('Paths', path_key)
        if not os.path.exists(path):
            try:
                os.makedirs(path, exist_ok=True)
                logger.info(f"Created directory: {path}")
            except Exception as e:
                raise ValueError(f"Could not create directory for Paths.{path_key}: {e}")
        
        # Check write permissions
        if not os.access(path, os.W_OK):
            raise ValueError(f"No write permission for directory: {path}")

# Common test products
TEST_PRODUCTS = [
    "777쓰리쎄븐 TS-6500C 손톱깎이 13P세트",
    "휴대용 360도 회전 각도조절 접이식 핸드폰 거치대",
    "피에르가르뎅 3단 슬림 코지가든 우양산",
    "마루는강쥐 클리어미니케이스",
    "아테스토니 뱀부사 소프트 3P 타올 세트",
    "티드 텔유 Y타입 치실 60개입 연세대학교 치과대학"
]

# File semaphore for concurrent file operations
file_semaphore = asyncio.Semaphore(1)

async def test_kogift_crawler(browser, config, test_products):
    """Test Kogift crawler with image download."""
    logger.info("=== Testing Kogift Crawler ===")
    results = []
    
    for product in test_products:
        logger.info(f"Testing Kogift crawler for: {product}")
        try:
            result = await scrape_kogift(browser, product, config=config, fetch_price_tables=True)
            
            if not result.empty:
                logger.info(f"Found {len(result)} results for '{product}'")
                # Check for downloaded images
                if 'local_image_path' in result.columns:
                    downloaded = result['local_image_path'].notnull().sum()
                    logger.info(f"Downloaded {downloaded}/{len(result)} images")
                    
                    # Log first result details
                    first_row = result.iloc[0]
                    results.append({
                        'product': product,
                        'found_name': first_row.get('name', 'N/A'),
                        'image_url': first_row.get('image_url', 'N/A'),
                        'local_path': first_row.get('local_image_path', 'N/A'),
                        'price': first_row.get('price', 'N/A'),
                        'source': 'Kogift'
                    })
            else:
                logger.warning(f"No results found for '{product}'")
                results.append({
                    'product': product,
                    'found_name': 'Not found',
                    'image_url': 'N/A',
                    'local_path': 'N/A',
                    'price': 'N/A',
                    'source': 'Kogift'
                })
                
        except Exception as e:
            logger.error(f"Error testing Kogift for '{product}': {e}")
            results.append({
                'product': product,
                'found_name': f'Error: {str(e)}',
                'image_url': 'Error',
                'local_path': 'Error',
                'price': 'Error',
                'source': 'Kogift'
            })
    
    return results

async def test_haereum_crawler(browser, config, test_products):
    """Test Haereum crawler with image download."""
    logger.info("=== Testing Haereum Crawler ===")
    results = []
    
    for product in test_products:
        logger.info(f"Testing Haereum crawler for: {product}")
        try:
            result = await scrape_haereum_data(browser, product, config)
            
            if result and isinstance(result, dict):
                logger.info(f"Found result for '{product}'")
                results.append({
                    'product': product,
                    'found_name': product,  # Haereum doesn't return product name
                    'image_url': result.get('url', 'N/A'),
                    'local_path': result.get('local_path', 'N/A'),
                    'price': 'N/A',  # Haereum doesn't return price in current implementation
                    'source': 'Haereum'
                })
            else:
                logger.warning(f"No results found for '{product}'")
                results.append({
                    'product': product,
                    'found_name': 'Not found',
                    'image_url': 'N/A',
                    'local_path': 'N/A',
                    'price': 'N/A',
                    'source': 'Haereum'
                })
                
        except Exception as e:
            logger.error(f"Error testing Haereum for '{product}': {e}")
            results.append({
                'product': product,
                'found_name': f'Error: {str(e)}',
                'image_url': 'Error',
                'local_path': 'Error',
                'price': 'Error',
                'source': 'Haereum'
            })
    
    return results

async def test_naver_crawler(config, test_products):
    """Test Naver crawler with image download."""
    logger.info("=== Testing Naver Crawler ===")
    results = []
    
    # Create test DataFrame
    test_df = pd.DataFrame({
        '상품명': test_products,
        '판매단가(V포함)': [10000] * len(test_products),  # Example price
        '구분': ['A'] * len(test_products)  # Example category
    })
    
    try:
        async with get_async_httpx_client(config=config) as client:
            result_df = await crawl_naver_products(test_df, config)
            
            if not result_df.empty:
                logger.info(f"Found {len(result_df)} Naver results")
                
                # Process each product's results
                for idx, row in result_df.iterrows():
                    original_row = row.get('original_row', {})
                    product_name = original_row.get('상품명', 'Unknown')
                    
                    results.append({
                        'product': product_name,
                        'found_name': row.get('네이버_상품명', 'N/A'),
                        'image_url': row.get('네이버 이미지', 'N/A'),
                        'local_path': row.get('local_image_path', 'N/A') if 'local_image_path' in row else 'N/A',
                        'price': row.get('판매단가(V포함)(3)', 'N/A'),
                        'source': 'Naver'
                    })
            else:
                logger.warning("No results found from Naver API")
                # Add empty results for all products
                for product in test_products:
                    results.append({
                        'product': product,
                        'found_name': 'Not found',
                        'image_url': 'N/A',
                        'local_path': 'N/A',
                        'price': 'N/A',
                        'source': 'Naver'
                    })
                    
    except Exception as e:
        logger.error(f"Error during Naver crawl: {e}")
        # Add error results for all products
        for product in test_products:
            results.append({
                'product': product,
                'found_name': f'Error: {str(e)}',
                'image_url': 'Error',
                'local_path': 'Error',
                'price': 'Error',
                'source': 'Naver'
            })
    
    return results

async def run_integrated_test():
    """Run integrated test of all crawlers with image download."""
    logger.info("Starting integrated crawler test")
    
    # Load configuration
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.ini')
    config = load_config(config_path)
    
    if not config.sections():
        logger.error(f"Could not load config from {config_path}")
        return
        
    try:
        # Validate configuration and create directories
        validate_config(config)
    except ValueError as e:
        logger.error(f"Configuration validation failed: {e}")
        return
    
    # Create timestamp for this test run
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Get image directories from config
    main_image_dir = config.get('Paths', 'image_main_dir')
    test_image_dir = os.path.join(main_image_dir, f'test_{timestamp}')
    
    # Ensure test directory exists
    os.makedirs(test_image_dir, exist_ok=True)
    logger.info(f"Test images will be saved to: {test_image_dir}")
    
    # Update config with test directory
    if not config.has_section('Paths'):
        config.add_section('Paths')
    config.set('Paths', 'image_target_dir', test_image_dir)
    
    all_results = []
    
    async with async_playwright() as p:
        # Launch browser with config settings
        headless = config.getboolean('Playwright', 'playwright_headless', fallback=False)
        browser = await p.chromium.launch(headless=headless)
        
        try:
            # Test Playwright-based crawlers
            logger.info("Testing Kogift crawler...")
            kogift_results = await test_kogift_crawler(browser, config, TEST_PRODUCTS)
            all_results.extend(kogift_results)
            
            logger.info("Testing Haereum crawler...")
            haereum_results = await test_haereum_crawler(browser, config, TEST_PRODUCTS)
            all_results.extend(haereum_results)
            
            # Test Naver API crawler (doesn't need browser)
            logger.info("Testing Naver crawler...")
            naver_results = await test_naver_crawler(config, TEST_PRODUCTS)
            all_results.extend(naver_results)
            
        finally:
            await browser.close()
    
    # Create results DataFrame
    results_df = pd.DataFrame(all_results)
    
    # Print summary
    print("\n=== Test Results Summary ===")
    print(f"Total tests run: {len(all_results)}")
    print("\nResults by source:")
    print(results_df.groupby('source').size())
    
    print("\nResults by source and status:")
    status_counts = results_df.groupby(['source', results_df['found_name'].apply(
        lambda x: 'Error' if 'Error' in str(x) else 'Not found' if x == 'Not found' else 'Found'
    )]).size()
    print(status_counts)
    
    print("\nDetailed Results:")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(results_df)
    
    # Save results to CSV
    csv_path = os.path.join(test_image_dir, f'test_results_{timestamp}.csv')
    results_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"Results saved to: {csv_path}")
    
    # Print image download summary
    downloaded_images = results_df['local_path'].apply(lambda x: x != 'N/A' and x != 'Error').sum()
    print(f"\nImage Download Summary:")
    print(f"Total images downloaded: {downloaded_images}/{len(results_df)}")
    print(f"Images saved to: {test_image_dir}")
    
    return results_df

if __name__ == "__main__":
    asyncio.run(run_integrated_test()) 