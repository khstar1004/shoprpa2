import os
import sys
import pandas as pd
import logging
import asyncio
import time
from playwright.async_api import async_playwright
from utils import load_config
from crowling_kogift import scrape_data as scrape_kogift_data
from crawling_UPrice_v2 import main as uprice_main

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"crawler_test_{time.strftime('%Y%m%d_%H%M%S')}.log")
    ]
)
logger = logging.getLogger(__name__)

async def test_kogift_crawler():
    """고려기프트 크롤러 테스트"""
    logger.info("=== 고려기프트 크롤러 테스트 시작 ===")
    
    # 테스트할 검색어 목록
    test_products = [
        "쓰리쎄븐 399VC 손톱깍이 세트",
        "777 쓰리쎄븐 TS-16000VG 손톱깎이 세트",
        "777 쓰리쎄븐 손톱깎이 세트"
    ]
    
    # 설정 불러오기
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.ini')
    config = load_config(config_path)
    
    # Playwright 초기화
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=config.getboolean('Playwright', 'playwright_headless', fallback=False))
        
        all_results = []
        
        for product in test_products:
            logger.info(f"고려기프트 검색 테스트: '{product}'")
            
            # 크롤링 실행
            result = await scrape_kogift_data(browser, product, config=config)
            
            # 결과 출력
            logger.info(f"검색어 '{product}' 결과: {len(result)}개 항목")
            
            if not result.empty:
                logger.info(f"첫 3개 항목:")
                for i, row in result.head(3).iterrows():
                    logger.info(f"  {i+1}. {row['name'][:30]}...")
                    logger.info(f"     가격: {row['price']}")
                    logger.info(f"     공급사: {row.get('supplier', 'N/A')}")
                    logger.info(f"     이미지: {row['image_url']}")
                    logger.info(f"     링크: {row['link']}")
                
                # 결과 저장
                result['search_term'] = product
                all_results.append(result)
        
        await browser.close()
    
    # 모든 결과 결합 및 저장
    if all_results:
        combined_df = pd.concat(all_results, ignore_index=True)
        csv_path = os.path.join(os.path.dirname(__file__), f'kogift_test_results_{time.strftime("%Y%m%d_%H%M%S")}.csv')
        combined_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        logger.info(f"모든 결과가 저장됨: {csv_path}")
    
    logger.info("=== 고려기프트 크롤러 테스트 완료 ===")

def test_uprice_crawler():
    """단가 스크래퍼 테스트"""
    logger.info("=== 단가 스크래퍼 테스트 시작 ===")
    
    # 테스트 URL (네이버 쇼핑 상품 링크)
    test_urls = [
        'https://cr.shopping.naver.com/adcr.nhn?x=F6bdQ9qfHPwtpI2HrLt62%2F%2F%2F%2Fw%3D%3DswH9%2FfAhl792HEc3QWsbm8ciO0jx8ZUaesqSWbolX3jXoPuGgIu13OnfWMxqfzDXyAiOcvbHzqStYpJL2kRlHSZ16fvCeS1Hq2ZdBXIBpaWL2igVhxlRth5Mog9fr4irD3bb%2BUm4eH6wHJnurPFQemSf9sHLqXJg2XjyXDUYWGlfMHmjhjScrcESWN0LndjJlhbDC55qlMeuK5Y8fDLagb6S9FI7cK8IzH6Dg6%2FmGWJIHs%2F2PHkpmvWjL2qy1nHubU5MgDjHo1wbxBNEamXCVIIZa2nTLhT%2Fgj4z6L9HR88g0bCU1zCIYJwY5B%2FCzwr0CsIvKrW6xPOsI2ecGWPIk034fAL4Tw8PrVhquDw3arsw7kvaEHl0HGYPZYZkv9bVrZVhaMMtvk0F817DwJeWGcdAu1jpaMvq8iLAIrUvEPuo5t4wOigeRYH0s0cshmlDZbzdjQIoeUISzDCcx4%2BHBZZtBDPZz8DPbT4SICGP7eC7fdeEb4CY9AlrfXPteLbNfu27zNZsU3z%2FyoTj%2FUN7DVgF%2FriLXtuP5E%2FXBfyrWN6%2BINN9RwalG9jaYNyWn%2FJbyzY6of2Xpggjm6A4Z8HgJhg%3D%3D&nvMid=46807468690&catId=50003564'
    ]
    
    results = []
    
    for i, url in enumerate(test_urls):
        logger.info(f"URL {i+1} 테스트: {url[:50]}...")
        
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(os.path.dirname(__file__), f'uprice_test_results_{i+1}_{timestamp}.csv')
        
        status, result_df = uprice_main(url, output_path)
        
        logger.info(f"URL {i+1} 결과 상태: {status}")
        if result_df is not None:
            logger.info(f"추출된 데이터 ({len(result_df)}개 행):")
            logger.info(result_df)
            results.append(result_df)
    
    logger.info("=== 단가 스크래퍼 테스트 완료 ===")
    return results

async def run_all_tests():
    """모든 크롤러 테스트 실행"""
    logger.info("=== 크롤러 통합 테스트 시작 ===")
    
    # 고려기프트 크롤러 테스트
    await test_kogift_crawler()
    
    # 단가 스크래퍼 테스트
    uprice_results = test_uprice_crawler()
    
    logger.info("=== 크롤러 통합 테스트 완료 ===")

if __name__ == "__main__":
    # 통합 테스트 실행
    asyncio.run(run_all_tests()) 