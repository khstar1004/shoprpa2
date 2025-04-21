import os
import asyncio
import pandas as pd
import logging
import time
from playwright.async_api import async_playwright
from utils import load_config
from crowling_kogift import scrape_data, extract_price_table

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"kogift_price_test_{time.strftime('%Y%m%d_%H%M%S')}.log")
    ]
)
logger = logging.getLogger(__name__)

async def test_kogift_price_crawler():
    """고려기프트 단가 및 수량 정보 크롤링 테스트"""
    logger.info("=== 고려기프트 단가 및 수량 정보 크롤링 테스트 시작 ===")
    
    # 테스트할 검색어 목록
    test_products = [
        "777 쓰리쎄븐 손톱깎이 세트"
    ]
    
    # 설정 불러오기
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.ini')
    config = load_config(config_path)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # 테스트용으로 headless=False로 설정
        
        for product in test_products:
            logger.info(f"검색어: '{product}' 테스트 시작")
            
            # 크롤링 실행 (단가표 추출 옵션 활성화)
            result = await scrape_data(browser, product, config=config, fetch_price_tables=True)
            
            # 결과 출력
            logger.info(f"검색어 '{product}' 결과: {len(result)}개 항목")
            
            if not result.empty:
                # 단가표 정보 추출 결과 확인
                if 'price_table' in result.columns:
                    price_table_count = result['price_table'].count()
                    logger.info(f"단가표 추출 성공 횟수: {price_table_count}/{len(result)}")
                    
                    # 단가표 정보가 있는 상품들 출력
                    for idx, price_table in result['price_table'].items():
                        if isinstance(price_table, pd.DataFrame) and not price_table.empty:
                            product_name = result.loc[idx, 'name']
                            logger.info(f"\n상품: '{product_name}'")
                            logger.info(f"단가표:\n{price_table}")
                
                # 결과 저장
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                
                # 단가표 정보를 JSON으로 변환하여 저장
                if 'price_table' in result.columns:
                    result_copy = result.copy()
                    result_copy['price_table'] = result_copy['price_table'].apply(
                        lambda x: x.to_json(orient='records') if isinstance(x, pd.DataFrame) else None
                    )
                    csv_path = os.path.join(os.path.dirname(__file__), f'kogift_price_test_{timestamp}.csv')
                    result_copy.to_csv(csv_path, index=False, encoding='utf-8-sig')
                    logger.info(f"테스트 결과 저장됨: {csv_path}")
        
        await browser.close()
    
    logger.info("=== 고려기프트 단가 및 수량 정보 크롤링 테스트 완료 ===")

async def test_direct_price_extraction():
    """직접 상품 페이지에서 단가표 추출 테스트"""
    logger.info("=== 상품 페이지 직접 단가표 추출 테스트 시작 ===")
    
    # 테스트할 상품 URL 목록
    test_urls = [
        "https://koreagift.com/ez/mall.php?cat=004008002&query=view&no=23259",  # 777쓰리세븐 손톱깎이세트 TS-399VC
        "https://koreagift.com/ez/mall.php?cat=004008002&query=view&no=9621",   # 777쓰리세븐 손톱깎이세트 TS-950
        "https://koreagift.com/ez/mall.php?cat=004008002&query=view&no=125649"  # 언박싱팩토리 다기능 손톱깎이 15종 휴대용 세트 (사용자 제공 예시)
    ]
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        
        for url in test_urls:
            logger.info(f"URL 테스트: {url}")
            price_table = await extract_price_table(page, url)
            
            if price_table is not None and not price_table.empty:
                logger.info(f"단가표 추출 성공: {len(price_table)}개 행")
                logger.info(f"단가표:\n{price_table}")
                
                # 결과 저장
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                url_id = url.split("no=")[-1]
                csv_path = os.path.join(os.path.dirname(__file__), f'price_table_{url_id}_{timestamp}.csv')
                price_table.to_csv(csv_path, index=False, encoding='utf-8-sig')
                logger.info(f"단가표 저장됨: {csv_path}")
            else:
                logger.warning(f"단가표 추출 실패: {url}")
        
        await page.close()
        await context.close()
        await browser.close()
    
    logger.info("=== 상품 페이지 직접 단가표 추출 테스트 완료 ===")

async def main():
    """테스트 스크립트 메인 함수"""
    # 직접 상품 페이지에서 단가표 추출 테스트
    await test_direct_price_extraction()
    
    # 검색 결과에서 단가표 추출 테스트
    await test_kogift_price_crawler()

if __name__ == "__main__":
    asyncio.run(main()) 