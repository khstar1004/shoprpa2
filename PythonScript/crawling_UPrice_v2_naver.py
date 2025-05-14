from playwright.sync_api import sync_playwright, TimeoutError
from playwright.async_api import async_playwright, TimeoutError, Page, Browser
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
import time
import logging
import asyncio
from typing import Dict, Any, Optional, List, Tuple
from io import StringIO
import random

# Set up logging
logger = logging.getLogger(__name__)

dialog_message = ""

async def handle_dialog(dialog):
    global dialog_message
    # 경고창의 메시지 출력
    dialog_message = dialog.message
    # print(f"Dialog message: {dialog.message}")
    # 경고창을 수락하거나 거절
    await dialog.accept()  # 또는 dialog.dismiss()

def clean_quantity(qty):
    """수량 미만 처리 함수"""
    if '미만' in qty:
        return '0'
    else:
        # 모든 문자 및 특수문자 제거 (숫자만 남김)
        return ''.join(filter(str.isdigit, qty))

def remove_special_chars(value):
    """문자와 특수 문자를 제거하는 함수. 입력 값의 타입에 관계없이 처리"""
    try:
        return ''.join(filter(str.isdigit, str(value)))
    except TypeError as e:
        return value

def plus_vat(price):
    return int(price)*1.1

async def handle_login_one(soup: BeautifulSoup) -> pd.DataFrame:
    """첫 번째 로그인 방식 처리"""
    try:
        tables = soup.find_all('table')
        if tables:
            # Using StringIO to fix FutureWarning
            html_str = StringIO(str(tables[0]))
            df = pd.read_html(html_str)[0]
            df = df.T
            df.reset_index(drop=False, inplace=True)
            df.columns = df.iloc[0]
            df.drop(index=0, inplace=True)
            # Handle potential column issues
            if len(df.columns) > 2:
                logger.info(f"Found {len(df.columns)} columns, using first two for 수량 and 일반")
                df = df.iloc[:, 0:2]
            df.columns = ['수량', '일반']
            # Safely convert data
            df = df.apply(lambda col: col.astype(str).apply(remove_special_chars))
            # Convert to numeric, errors='coerce' will convert invalid values to NaN
            df['일반'] = pd.to_numeric(df['일반'], errors='coerce').fillna(0)
            # Fix: Apply exactly 10% VAT
            df['일반'] = df['일반'].apply(lambda x: round(float(x) * 1.1))
            df['수량'] = pd.to_numeric(df['수량'], errors='coerce').fillna(0).astype('int64')
            # Filter out rows with zero quantity
            df = df[df['수량'] > 0]
            if df.empty:
                logger.warning("After filtering invalid rows, dataframe is empty")
                return pd.DataFrame()
            df.sort_values(by='수량', inplace=True, ignore_index=True)
            return df
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error in handle_login_one: {e}")
        return pd.DataFrame()

async def handle_login_two(soup: BeautifulSoup) -> pd.DataFrame:
    """두 번째 로그인 방식 처리"""
    try:
        tables = soup.find_all('table')
        if tables:
            # Using StringIO to fix FutureWarning
            html_str = StringIO(str(tables[0]))
            df = pd.read_html(html_str)[0]
            
            # Log the original dataframe structure for debugging
            logger.debug(f"Original table structure:\n{df}")
            
            # Check if quantities are in column headers (like '200', '300', '500', etc.)
            quantity_cols = [col for col in df.columns if str(col).isdigit() or (isinstance(col, str) and col.isdigit())]
            
            if quantity_cols:
                logger.info(f"Found quantity columns as headers: {quantity_cols}")
                # Create result dataframe
                result_df = pd.DataFrame(columns=['수량', '일반'])
                
                # Get the price row (usually the first meaningful row)
                # Try multiple row indices since the price row might not be the first row
                for row_idx in range(min(3, len(df))):
                    try:
                        row_data = df.iloc[row_idx]
                        prices_found = False
                        
                        for qty in quantity_cols:
                            try:
                                price_val = row_data[qty]
                                if pd.notna(price_val) and price_val != '':
                                    # Clean and convert price to numeric
                                    if isinstance(price_val, str):
                                        price_val = ''.join(filter(str.isdigit, price_val.replace(',', '')))
                                    
                                    # Convert to int or use original if non-numeric
                                    try:
                                        price = int(price_val)
                                        if price > 0:
                                            result_df = pd.concat([result_df, pd.DataFrame({
                                                '수량': [int(qty)],
                                                '일반': [price]
                                            })], ignore_index=True)
                                            prices_found = True
                                            logger.debug(f"Found price {price} for quantity {qty}")
                                    except (ValueError, TypeError):
                                        logger.debug(f"Could not convert price value: {price_val}")
                            except Exception as e:
                                logger.debug(f"Error processing quantity {qty}: {e}")
                        
                        if prices_found:
                            break
                    except Exception as e:
                        logger.debug(f"Error processing row {row_idx}: {e}")
                
                if not result_df.empty:
                    # Sort by quantity
                    result_df = result_df.sort_values('수량')
                    logger.info(f"Successfully extracted {len(result_df)} quantity-price pairs")
                    
                    # Log the extracted data
                    logger.info("\n=== 수량별 가격 정보 ===")
                    for _, row in result_df.iterrows():
                        logger.info(f"수량: {row['수량']}, 가격: {row['일반']}")
                    
                    return result_df
                else:
                    logger.warning(f"No valid price data found for quantities: {quantity_cols}")
            else:
                # Original transpose approach for tables with other formats
                # Transpose and reset index
                df = df.T
                df.reset_index(drop=False, inplace=True)
                
                # Get column names from first row
                if len(df) > 0:
                    df.columns = df.iloc[0]
                    df = df.iloc[1:]  # Drop the first row which is now column names
                    
                    # Log available columns for debugging
                    logger.info(f"Available columns after transpose: {df.columns.tolist()}")
                    
                    # Check if we have quantity columns (numbers as column names)
                    quantity_cols = [col for col in df.columns if str(col).isdigit()]
                    if quantity_cols:
                        # Create a new DataFrame with quantity and price
                        result_df = pd.DataFrame(columns=['수량', '일반'])
                        
                        # Process each quantity column
                        for qty in quantity_cols:
                            try:
                                price = df[qty].iloc[0]  # Get the price for this quantity
                                if pd.notna(price) and price != '':
                                    # Convert price to numeric, removing any non-numeric characters
                                    price = pd.to_numeric(str(price).replace(',', ''), errors='coerce')
                                    if pd.notna(price) and price > 0:
                                        result_df = pd.concat([result_df, pd.DataFrame({
                                            '수량': [int(qty)],
                                            '일반': [price]
                                        })], ignore_index=True)
                            except Exception as e:
                                logger.warning(f"Error processing quantity {qty}: {e}")
                                continue
                        
                        if not result_df.empty:
                            # Sort by quantity
                            result_df = result_df.sort_values('수량')
                            return result_df
            
            # Try one more method: look for row with "수량" and associated price row
            for i in range(len(df)):
                row = df.iloc[i]
                if any('수량' in str(val).lower() for val in row.values):
                    logger.info(f"Found row with '수량' at index {i}")
                    if i + 1 < len(df):  # Check if there's a next row for prices
                        price_row = df.iloc[i + 1]
                        # Extract quantity-price pairs
                        result_df = pd.DataFrame(columns=['수량', '일반'])
                        for j, val in enumerate(row):
                            try:
                                if j < len(price_row):
                                    qty_val = str(val).strip()
                                    price_val = str(price_row.iloc[j]).strip()
                                    
                                    # Extract numeric parts
                                    qty_numeric = ''.join(filter(str.isdigit, qty_val))
                                    price_numeric = ''.join(filter(str.isdigit, price_val))
                                    
                                    if qty_numeric and price_numeric:
                                        qty = int(qty_numeric)
                                        price = int(price_numeric)
                                        if qty > 0 and price > 0:
                                            result_df = pd.concat([result_df, pd.DataFrame({
                                                '수량': [qty],
                                                '일반': [price]
                                            })], ignore_index=True)
                            except Exception as e:
                                logger.debug(f"Error processing cell pair: {e}")
                        
                        if not result_df.empty:
                            result_df = result_df.sort_values('수량')
                            return result_df
            
            logger.warning("Could not extract valid quantity-price data from table")
            return pd.DataFrame(columns=['수량', '일반'])
            
        return pd.DataFrame(columns=['수량', '일반'])
    except Exception as e:
        logger.error(f"Error in handle_login_two: {e}")
        return pd.DataFrame(columns=['수량', '일반'])

async def handle_login_three(soup: BeautifulSoup) -> pd.DataFrame:
    """세 번째 로그인 방식 처리"""
    try:
        tables = soup.find_all('table')
        if tables:
            # Try to find input tags with specific classes
            quantities = []
            prices = []
            
            # Safely extract quantities and prices
            for input_tag in soup.find_all('input', class_='qu'):
                try:
                    qty = input_tag.get('value', '0')
                    quantities.append(int(qty.replace(',', '')))
                except (ValueError, TypeError):
                    logger.debug(f"Could not convert quantity value: {input_tag.get('value', 'N/A')}")
            
            for input_tag in soup.find_all('input', class_='pr'):
                try:
                    price = input_tag.get('value', '0')
                    prices.append(int(price.replace(',', '')))
                except (ValueError, TypeError):
                    logger.debug(f"Could not convert price value: {input_tag.get('value', 'N/A')}")
            
            # Create dataframe if we have data
            if quantities and prices and len(quantities) == len(prices):
                df = pd.DataFrame({
                    '수량': quantities,
                    '일반': prices
                })
                # Fix: Apply exactly 10% VAT calculation
                df['일반'] = df['일반'].apply(lambda x: round(float(x) * 1.1))
                df.sort_values(by='수량', inplace=True, ignore_index=True)
                return df
            else:
                logger.warning(f"Mismatched quantities ({len(quantities)}) and prices ({len(prices)})")
                return pd.DataFrame()
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error in handle_login_three: {e}")
        return pd.DataFrame()

async def handle_login_four(soup: BeautifulSoup) -> pd.DataFrame:
    """네 번째 로그인 방식 처리"""
    try:
        tables = soup.find_all('table')
        if tables:
            # Using StringIO to fix FutureWarning
            html_str = StringIO(str(tables[0]))
            df = pd.read_html(html_str)[0]
            
            # Try to identify price and quantity columns
            for col in df.columns:
                if '수량' in str(col).lower():
                    df.rename(columns={col: '수량'}, inplace=True)
                elif any(term in str(col).lower() for term in ['가격', '단가', '일반']):
                    df.rename(columns={col: '일반'}, inplace=True)
            
            # Check if we have the necessary columns
            if '수량' in df.columns and '일반' in df.columns:
                # Convert to numeric safely
                df['수량'] = pd.to_numeric(df['수량'].astype(str).apply(remove_special_chars), errors='coerce').fillna(0).astype('int64')
                df['일반'] = pd.to_numeric(df['일반'].astype(str).apply(remove_special_chars), errors='coerce').fillna(0)
                # Filter out rows with zero quantity
                df = df[df['수량'] > 0]
                if df.empty:
                    logger.warning("After filtering invalid rows, dataframe is empty")
                    return pd.DataFrame()
                df.sort_values(by='수량', inplace=True, ignore_index=True)
                return df
            else:
                logger.warning(f"Required columns not found. Available columns: {df.columns.tolist()}")
                return pd.DataFrame()
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error in handle_login_four: {e}")
        return pd.DataFrame()

# Add a new function to detect promotional supplier sites
def is_promotional_supplier(supplier_name):
    """
    Check if a supplier is a promotional supplier based on keywords.
    
    Args:
        supplier_name (str): The name of the supplier
        
    Returns:
        bool: True if it's a promotional supplier, False otherwise
    """
    if not supplier_name or not isinstance(supplier_name, str):
        return False
        
    # Keywords that indicate a promotional supplier
    promo_keywords = [
        '온오프마켓', '답례품', '기프트', '판촉', '기념품', '인쇄', '각인', 
        '제작', '미스터몽키', '홍보', '호갱탈출'
    ]
    
    # Check if any keyword is in the supplier name
    for keyword in promo_keywords:
        if keyword in supplier_name:
            logger.info(f"Detected promotional supplier: {supplier_name} (contains keyword '{keyword}')")
            return True
            
    return False

async def extract_quantity_prices(page, url: str, target_quantities: List[int] = None) -> Dict[str, Any]:
    """수량별 가격 정보 추출 - 여러 사이트 구조에 대응하는 지능형 분석 알고리즘"""
    result = {
        "is_promotional_site": False,
        "has_quantity_pricing": False,
        "quantity_prices": {},
        "price_table": None,
        "vat_included": False,
        "supplier_name": "",
        "error": None,
        "is_naver_site": False,
        "has_captcha": False
    }
    
    if not page or not url:
        result["error"] = "Invalid page or URL provided"
        logger.error(result["error"])
        return result
    
    try:
        logger.info(f"Navigating to product page: {url}")
        
        # Set random viewport size to avoid detection
        viewport_sizes = [
            {"width": 1366, "height": 768},
            {"width": 1920, "height": 1080},
            {"width": 1440, "height": 900},
            {"width": 1536, "height": 864}
        ]
        await page.set_viewport_size(random.choice(viewport_sizes))
        
        # Set random user agent
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Edge/92.0.902.84 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0'
        ]
        await page.set_extra_http_headers({
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        })
        
        try:
            # Add random delay before navigation (1-3 seconds)
            await asyncio.sleep(random.uniform(1, 3))
            
            # Navigate with custom timeout and wait until
            await page.goto(url, timeout=30000, wait_until='networkidle')
            await page.wait_for_load_state('domcontentloaded')
            
            # 현재 URL 확인 (리다이렉트 후)
            current_url = page.url
            
            # 네이버 도메인 체크
            is_naver_domain = "naver.com" in current_url or "shopping.naver.com" in current_url
            result["is_naver_site"] = is_naver_domain
            
            # Enhanced anti-captcha measures
            await page.evaluate('''() => {
                // Hide automation flags
                Object.defineProperty(navigator, 'webdriver', { get: () => false });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                Object.defineProperty(navigator, 'languages', { get: () => ['ko-KR', 'ko', 'en-US', 'en'] });
                
                // Add random mouse movements
                const randomMove = () => {
                    const event = new MouseEvent('mousemove', {
                        bubbles: true,
                        cancelable: true,
                        clientX: Math.floor(Math.random() * window.innerWidth),
                        clientY: Math.floor(Math.random() * window.innerHeight)
                    });
                    document.dispatchEvent(event);
                };
                setInterval(randomMove, Math.random() * 2000 + 1000);
            }''')
        except Exception as e:
            logger.error(f"Error during page navigation and setup: {e}")
            result["error"] = f"Navigation error: {str(e)}"
            return result
            
        # 캡차 체크 (개선된 버전)
        captcha_selectors = [
            'form#captcha_form', 
            'img[alt*="captcha"]', 
            'div.captcha_wrap',
            'input[name="captchaBotKey"]',
            'div[class*="captcha"]',
            'iframe[src*="captcha"]',
            'div[class*="bot-check"]',
            'div[class*="security-check"]'
        ]
        
        for selector in captcha_selectors:
            if await page.locator(selector).count() > 0:
                logger.info(f"CAPTCHA detected on page: {current_url}")
                result["has_captcha"] = True
                return result  # Return immediately if captcha detected
            
        # 공급사 정보 수집
        supplier_selectors = [
            'div.basicInfo_mall_title__3IDPK a',
            'a.seller_name',
            'span.mall_txt',
            'div.shop_info a.txt',
            'div[class*="mall_title"] a',
            'div[class*="seller"] a',
            'a[class*="mall-name"]'
        ]
        
        for selector in supplier_selectors:
            if await page.locator(selector).count() > 0:
                supplier_name = await page.locator(selector).text_content()
                result["supplier_name"] = supplier_name.strip()
                logger.info(f"Found supplier name: {result['supplier_name']}")
                
                # 공급사가 네이버인지 확인
                if "네이버" in result["supplier_name"]:
                    result["is_naver_seller"] = True
                    logger.info("Detected Naver as the supplier")
                    
                # Enhanced lowest price button handling
                lowest_price_selectors = [
                    '//div[contains(@class, "lowestPrice_btn_box")]/div[contains(@class, "buyButton_compare_wrap")]/a[text()="최저가 사러가기"]',
                    '//a[contains(text(), "최저가 사러가기")]',
                    '//a[contains(text(), "최저가")]',
                    '//a[contains(@class, "lowest_price")]',
                    '//button[contains(text(), "최저가")]',
                    '//div[contains(@class, "lowest")]/a',
                    '//div[contains(@class, "price_compare")]/a',
                    '//a[contains(@class, "price_compare")]',
                    '//div[contains(@class, "compare")]/a[contains(@class, "link")]',
                    '//a[contains(@href, "search/gate")]'
                ]
                
                max_retries = 3
                retry_count = 0
                button_found = False
                
                while retry_count < max_retries and not button_found:
                    for selector in lowest_price_selectors:
                        try:
                            # Wait for selector with timeout
                            await page.wait_for_selector(selector, timeout=5000)
                            element = page.locator(selector).first
                            
                            if await element.is_visible():
                                logger.info(f"Found lowest price button with selector: {selector}")
                                
                                # Get button position and add slight random offset
                                box = await element.bounding_box()
                                if box:
                                    x = box['x'] + box['width'] / 2 + random.uniform(-5, 5)
                                    y = box['y'] + box['height'] / 2 + random.uniform(-5, 5)
                                    
                                    # Move mouse naturally to button
                                    await page.mouse.move(x, y, steps=random.randint(5, 10))
                                    await asyncio.sleep(random.uniform(0.1, 0.3))
                                
                                # Try to get href first
                                href = await element.get_attribute('href')
                                if href:
                                    logger.info(f"Navigating to lowest price URL: {href}")
                                    await page.goto(href, wait_until='networkidle', timeout=30000)
                                else:
                                    logger.info("Clicking lowest price button")
                                    await element.click()
                                    await page.wait_for_load_state('networkidle', timeout=30000)
                                
                                # Add random delay after click (2-4 seconds)
                                await asyncio.sleep(random.uniform(2, 4))
                                
                                button_found = True
                                current_url = page.url
                                logger.info(f"After clicking lowest price button, now at URL: {current_url}")
                                break
                        except Exception as e:
                            logger.warning(f"Error with lowest price selector {selector} (attempt {retry_count + 1}): {e}")
                            continue
                    
                    if not button_found:
                        retry_count += 1
                        if retry_count < max_retries:
                            # Add increasing delay between retries
                            await asyncio.sleep(random.uniform(2, 4) * retry_count)
                            # Reload page before retry
                            await page.reload(wait_until='networkidle', timeout=30000)
                
                if not button_found:
                    logger.warning(f"Could not find lowest price button after {max_retries} attempts")
                break
        
        # 네이버 직접 연결된 사이트이고 캡차가 없는 경우, 기본적으로 판촉물 사이트 아님
        if result["is_naver_site"] and not result["has_captcha"]:
            logger.info(f"Direct Naver shopping mall (not promotional site): {current_url}")
            result["is_promotional_site"] = False
            await page.wait_for_timeout(1000)
            return result
            
        # Add random delay before content analysis (2-4 seconds)
        await asyncio.sleep(random.uniform(2, 4))
            
        # Rest of the function remains the same...
        # ... existing code for price table detection and analysis ...

    except Exception as e:
        logger.error(f"Error in extract_quantity_prices: {e}")
        result["error"] = str(e)
        return result
    
    return result

async def detect_tables_by_content(html_content: str) -> Optional[Dict[str, Any]]:
    """HTML 내용 기반으로, 가격표가 있는 테이블 감지"""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 1. 모든 테이블 찾기
        all_tables = soup.find_all('table')
        logger.info(f"Found {len(all_tables)} tables in the HTML content")
        
        # 2. 잠재적 가격표 식별을 위한 스코어링 시스템 적용
        table_scores = []
        
        for table_idx, table in enumerate(all_tables):
            score = 0
            table_text = table.get_text().lower()
            
            # 관련 키워드 확인
            if '수량' in table_text:
                score += 5
            if '가격' in table_text or '단가' in table_text:
                score += 5
            if '일반' in table_text and ('가격' in table_text or '단가' in table_text):
                score += 3
            
            # 수량 패턴 확인 (200, 300, 500, 1000 등)
            common_qty = ['200', '300', '500', '1000', '2000', '3000']
            qty_matches = sum(1 for qty in common_qty if qty in table_text)
            score += qty_matches * 2
            
            # 테이블 구조 확인
            headers = table.find_all('th')
            digit_headers = sum(1 for header in headers if header.get_text().strip().isdigit())
            if digit_headers >= 3:
                score += digit_headers * 2
            
            table_scores.append((table_idx, score, table))
        
        # 가장 높은 점수의 테이블 사용
        table_scores.sort(key=lambda x: x[1], reverse=True)
        
        for table_idx, score, table in table_scores:
            if score >= 10:  # 임계값 설정
                logger.info(f"Table #{table_idx+1} has high score ({score}), analyzing it")
                result = await analyze_table_structure(table)
                if result and result.get("has_quantity_pricing"):
                    result["detection_method"] = "content_scoring"
                    return result
        
        return None
    except Exception as e:
        logger.error(f"Error in detect_tables_by_content: {e}")
        return None

async def detect_tables_by_structure(page) -> Optional[Dict[str, Any]]:
    """페이지 DOM 구조 기반으로 가격표 테이블 감지"""
    try:
        # 1. 테이블이 있는 셀렉터 목록 확인
        table_selectors = [
            'table.price_table', 'table.bg_table1', 'div.price_info table', 
            'div.quantity_price table', 'div.price-box table',
            'table.quantity_price__table', 'div.quantity_discount table',
            'div.quantity_pricing table', 'table.price_by_quantity'
        ]
        
        # 각 셀렉터 확인
        for selector in table_selectors:
            try:
                element = await page.query_selector(selector)
                if element:
                    logger.info(f"Found potential price table with selector: {selector}")
                    
                    # 테이블 HTML 가져오기
                    table_html = await element.inner_html()
                    soup = BeautifulSoup(f"<table>{table_html}</table>", 'html.parser')
                    
                    result = await analyze_table_structure(soup)
                    if result and result.get("has_quantity_pricing"):
                        result["detection_method"] = f"structure_selector:{selector}"
                        return result
            except Exception as e:
                logger.debug(f"Error processing selector {selector}: {e}")
                continue
        
        # 2. 프레임 내부 확인
        frames = page.frames
        for frame_idx, frame in enumerate(frames):
            try:
                frame_content = await frame.content()
                soup = BeautifulSoup(frame_content, 'html.parser')
                
                # 프레임 내부의 테이블 확인
                for table in soup.find_all('table'):
                    result = await analyze_table_structure(table)
                    if result and result.get("has_quantity_pricing"):
                        result["detection_method"] = f"frame_{frame_idx}_table"
                        return result
            except Exception as e:
                logger.debug(f"Error processing frame: {e}")
                continue
        
        return None
    except Exception as e:
        logger.error(f"Error in detect_tables_by_structure: {e}")
        return None

async def detect_with_input_fields(page, target_quantities: List[int] = None) -> Optional[Dict[str, Any]]:
    """수량 입력 필드 방식으로 가격 정보 감지"""
    try:
        qty_input_selectors = [
            'input#qty', 'input.buynum', 'input[name="quantity"]',
            'input.quantity', 'input.qty', 'input.amount',
            'input[name="opt_qty"]', 'input.opt_qty',
            'input[name="qty"]', 'input[type="number"]'
        ]
        
        # 기본 테스트 수량 설정
        if not target_quantities:
            target_quantities = [200, 300, 500, 1000, 3000, 8000, 15000]
        
        # 샘플 테스트를 위한 수량 선택 (성능 최적화)
        sample_quantities = sorted(target_quantities)[:3]  # 처음 3개 수량만 테스트
        
        for selector in qty_input_selectors:
            try:
                element = await page.query_selector(selector)
                if element:
                    price_table = []
                    
                    # 먼저 샘플 수량으로 테스트
                    for qty in sample_quantities:
                        try:
                            # 수량 입력
                            await element.fill(str(qty))
                            await element.press('Enter')
                            await page.wait_for_timeout(1000)
                            
                            # 가격 요소 찾기
                            price_selectors = [
                                'span.price', 'div.price', 'strong.price', 'p.price',
                                'span.total-price', 'div.total-price',
                                'span#price', 'div#price', 'span.amount', 'div.amount',
                                'span.item_price', 'div.item_price',
                                'strong.item_price', 'p.item_price',
                                'span.sale_price', 'div.sale_price'
                            ]
                            
                            for price_selector in price_selectors:
                                price_element = await page.query_selector(price_selector)
                                if price_element:
                                    price_text = await price_element.inner_text()
                                    price_str = ''.join(filter(str.isdigit, price_text))
                                    if price_str:
                                        price = int(price_str)
                                        if price > 0:
                                            price_table.append({"quantity": qty, "price": price})
                                            break
                        except Exception as e:
                            logger.debug(f"Error testing quantity {qty}: {e}")
                            continue
                    
                    # 3개 중 2개 이상의 가격을 찾으면 모든 수량 테스트
                    if len(price_table) >= 2:
                        logger.info(f"Found {len(price_table)} prices with input field. Testing all quantities.")
                        
                        # 나머지 수량 테스트
                        remaining_quantities = [q for q in target_quantities if q not in sample_quantities]
                        for qty in remaining_quantities:
                            try:
                                await element.fill(str(qty))
                                await element.press('Enter')
                                await page.wait_for_timeout(1000)
                                
                                for price_selector in price_selectors:
                                    price_element = await page.query_selector(price_selector)
                                    if price_element:
                                        price_text = await price_element.inner_text()
                                        price_str = ''.join(filter(str.isdigit, price_text))
                                        if price_str:
                                            price = int(price_str)
                                            if price > 0:
                                                price_table.append({"quantity": qty, "price": price})
                                                break
                            except Exception as e:
                                logger.debug(f"Error testing quantity {qty}: {e}")
                                continue
                        
                        if len(price_table) >= 3:
                            # 수량별 가격 정보 생성
                            result = {
                                "has_quantity_pricing": True,
                                "price_table": sorted(price_table, key=lambda x: x["quantity"]),
                                "quantity_prices": {},
                                "vat_included": False,
                                "detection_method": "input_field"
                            }
                            
                            for item in price_table:
                                qty = item["quantity"]
                                price = item["price"]
                                price_with_vat = price if result["vat_included"] else round(price * 1.1)
                                result["quantity_prices"][qty] = {
                                    "price": price,
                                    "price_with_vat": price_with_vat,
                                    "exact_match": True
                                }
                            
                            logger.info(f"Successfully extracted {len(price_table)} quantity-price pairs from input field")
                            return result
            except Exception as e:
                logger.debug(f"Error with selector {selector}: {e}")
                continue
        
        return None
    except Exception as e:
        logger.error(f"Error in detect_with_input_fields: {e}")
        return None

async def extract_all_numbers(page) -> List[Dict[str, Any]]:
    """페이지의 모든 숫자 추출 - 패턴 분석을 위해"""
    try:
        # JavaScript로 페이지의 모든 텍스트 노드에서 숫자 추출
        numbers = await page.evaluate('''() => {
            const result = [];
            const textWalker = document.createTreeWalker(
                document.body, 
                NodeFilter.SHOW_TEXT, 
                { acceptNode: node => node.textContent.trim() ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_REJECT }
            );
            
            let node;
            while (node = textWalker.nextNode()) {
                const text = node.textContent.trim();
                // 숫자만 있는 패턴 또는 숫자+콤마+원 패턴 찾기
                const matches = text.match(/\\b\\d{1,3}(,\\d{3})*\\b|\\b\\d+\\b/g);
                if (matches) {
                    // 노드의 정보 저장
                    const parentElement = node.parentElement;
                    const parentTagName = parentElement ? parentElement.tagName.toLowerCase() : 'unknown';
                    const parentClasses = parentElement ? Array.from(parentElement.classList).join(' ') : '';
                    
                    for (const match of matches) {
                        const cleanNumber = parseInt(match.replace(/[^0-9]/g, ''));
                        if (cleanNumber > 0) {
                            result.push({
                                number: cleanNumber,
                                text: text,
                                tagName: parentTagName,
                                classes: parentClasses,
                                row: parentElement ? parentElement.closest('tr') ? true : false : false
                            });
                        }
                    }
                }
            }
            return result;
        }''')
        
        logger.info(f"Extracted {len(numbers)} numbers from the page")
        return numbers
    except Exception as e:
        logger.error(f"Error extracting numbers: {e}")
        return []

def analyze_number_patterns(numbers: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """숫자 패턴 분석하여 수량-가격 관계 추론"""
    try:
        if not numbers or len(numbers) < 6:  # 최소 3쌍의 수량-가격 필요
            return None
            
        # 1. 일반적인 수량 패턴 정의
        common_quantities = [100, 200, 300, 500, 1000, 2000, 3000, 5000, 8000, 10000, 15000, 20000]
        potential_quantities = set(common_quantities)
        
        # 2. 페이지에서 발견된 수량 확인
        found_quantities = [num['number'] for num in numbers if num['number'] in potential_quantities]
        if len(found_quantities) < 3:
            logger.debug(f"Not enough common quantities found: {found_quantities}")
            return None
            
        # 중복 제거 및 정렬
        found_quantities = sorted(set(found_quantities))
        logger.info(f"Found potential quantities: {found_quantities}")
        
        # 3. 각 수량별로 가능한 가격 찾기
        quantity_price_map = {}
        
        # 같은 행에 있는 가격 우선 찾기
        for qty in found_quantities:
            qty_entries = [n for n in numbers if n['number'] == qty and n['row']]
            if not qty_entries:
                continue
                
            for qty_entry in qty_entries:
                # 같은 행에 있는 다른 숫자 찾기
                row_numbers = [n for n in numbers if n['row'] and n['number'] != qty]
                
                # 가격으로 적합한 숫자 찾기 (일반적인 범위 내)
                potential_prices = [n['number'] for n in row_numbers if 100 <= n['number'] < 100000]
                
                if potential_prices:
                    # 가장 적합한 가격 선택 (중간 범위)
                    mid_price = sorted(potential_prices)[len(potential_prices)//2]
                    quantity_price_map[qty] = mid_price
        
        # 충분한 수량-가격 쌍이 발견됐는지 확인
        if len(quantity_price_map) >= 3:
            logger.info(f"Found {len(quantity_price_map)} quantity-price pairs by pattern analysis")
            
            result = {
                "has_quantity_pricing": True,
                "price_table": [],
                "quantity_prices": {},
                "vat_included": False,
                "detection_method": "number_pattern_analysis"
            }
            
            for qty, price in sorted(quantity_price_map.items()):
                result["price_table"].append({"quantity": qty, "price": price})
                result["quantity_prices"][qty] = {
                    "price": price,
                    "price_with_vat": round(price * 1.1),
                    "exact_match": True
                }
                
            return result
            
        return None
    except Exception as e:
        logger.error(f"Error analyzing number patterns: {e}")
        return None

async def analyze_table_structure(table) -> Optional[Dict[str, Any]]:
    """
    테이블 구조를 분석하여 수량-가격 정보를 추출하는 범용적인 함수
    다양한 테이블 구조를 처리할 수 있도록 설계
    """
    try:
        # 1. 특별한 키워드가 테이블 내부에 있는지 확인
        table_text = table.get_text().lower()
        price_keywords = ['가격', '단가', 'price', '원']
        qty_keywords = ['수량', 'quantity', 'qty', '개']
        
        has_price_keyword = any(keyword in table_text for keyword in price_keywords)
        has_qty_keyword = any(keyword in table_text for keyword in qty_keywords)
        
        # 키워드가 없으면 일반 테이블로 간주
        if not (has_price_keyword and has_qty_keyword):
            # 특별한 경우 - 숫자만 있는 테이블 헤더 확인 (수량일 가능성)
            headers = table.find_all('th')
            digit_headers = 0
            for header in headers:
                if header.get_text().strip().isdigit():
                    digit_headers += 1
            
            # 숫자 헤더가 3개 이상이면 수량 테이블일 가능성 높음
            if digit_headers < 3:
                return None
        
        # 2. 테이블 구조 분석
        # 2.1 헤더 기반 분석
        quantities = []
        quantity_col_indices = []
        price_col_indices = []
        
        # 2.1.1 thead에서 수량 컬럼 찾기
        thead = table.find('thead')
        if thead:
            headers = thead.find_all('th')
            # 첫 번째 행에서 수량/가격 컬럼 식별
            for idx, header in enumerate(headers):
                text = header.get_text().strip().lower()
                # 수량 컬럼 식별
                if any(keyword in text for keyword in qty_keywords):
                    quantity_col_indices.append(idx)
                # 가격 컬럼 식별
                elif any(keyword in text for keyword in price_keywords):
                    price_col_indices.append(idx)
                # 숫자만 있는 경우 수량으로 간주
                elif text.isdigit():
                    try:
                        qty = int(text)
                        if qty > 0 and qty in [200, 300, 500, 1000, 2000, 3000, 5000, 8000, 10000, 15000, 20000]:
                            quantities.append(qty)
                    except ValueError:
                        continue
        
        # 2.1.2 tbody나 일반 tr에서도 검색
        rows = table.find_all('tr')
        if not quantity_col_indices:
            # 첫 번째 행에서 컬럼 찾기
            for row_idx, row in enumerate(rows):
                cells = row.find_all(['th', 'td'])
                for cell_idx, cell in enumerate(cells):
                    text = cell.get_text().strip().lower()
                    if any(keyword in text for keyword in qty_keywords):
                        quantity_col_indices.append(cell_idx)
                    elif any(keyword in text for keyword in price_keywords):
                        price_col_indices.append(cell_idx)
        
        # 2.2 일반 패턴 분석
        # 2.2.1 수량이 행으로 있는 경우
        prices = []
        quantity_price_map = {}
        
        if quantities:  # 이미 수량을 찾은 경우 (숫자 헤더)
            # 가격 행 찾기
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= len(quantities):
                    # "일반" 또는 "가격" 키워드가 있는지 확인
                    row_text = row.get_text().lower()
                    if any(keyword in row_text for keyword in price_keywords):
                        for i, cell in enumerate(cells):
                            if i < len(quantities):
                                try:
                                    price_str = ''.join(filter(str.isdigit, cell.get_text().strip()))
                                    if price_str:
                                        price = int(price_str)
                                        if price > 0:
                                            prices.append(price)
                                            quantity_price_map[quantities[i]] = price
                                except ValueError:
                                    continue
        
        # 2.2.2 일반적인 행 기반 구조 처리
        if not quantity_price_map and (quantity_col_indices or price_col_indices):
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if not cells:
                    continue
                
                # 각 행에서 수량과 가격 추출
                row_qty = None
                row_price = None
                
                # 수량 컬럼에서 값 추출
                for idx in quantity_col_indices:
                    if idx < len(cells):
                        qty_text = cells[idx].get_text().strip()
                        # 숫자만 추출
                        qty_num = ''.join(filter(str.isdigit, qty_text))
                        if qty_num:
                            try:
                                row_qty = int(qty_num)
                                break
                            except ValueError:
                                continue
                
                # 가격 컬럼에서 값 추출
                for idx in price_col_indices:
                    if idx < len(cells):
                        price_text = cells[idx].get_text().strip()
                        # 숫자만 추출
                        price_num = ''.join(filter(str.isdigit, price_text))
                        if price_num:
                            try:
                                row_price = int(price_num)
                                break
                            except ValueError:
                                continue
                
                # 둘 다 유효한 값이면 맵에 추가
                if row_qty and row_price and row_qty > 0 and row_price > 0:
                    quantity_price_map[row_qty] = row_price
        
        # 2.3 테이블 전체 분석 (일반 패턴 실패시)
        if not quantity_price_map:
            # 가능한 수량 패턴 (판촉물 사이트에서 자주 사용되는 값)
            common_quantities = [100, 200, 300, 500, 1000, 2000, 3000, 5000, 8000, 10000, 15000, 20000]
            
            # 각 행에서 숫자 찾기
            for row in rows:
                cells = row.find_all(['td', 'th'])
                row_numbers = []
                
                for cell in cells:
                    text = cell.get_text().strip()
                    # 숫자만 추출
                    num_str = ''.join(filter(str.isdigit, text))
                    if num_str:
                        try:
                            num = int(num_str)
                            if num > 0:
                                row_numbers.append((num, text))
                        except ValueError:
                            continue
                
                # 행에 2개 이상의 숫자가 있으면 가능한 수량-가격 쌍 확인
                if len(row_numbers) >= 2:
                    for i, (num1, text1) in enumerate(row_numbers):
                        for num2, text2 in row_numbers[i+1:]:
                            # 일반적인 수량 패턴에 맞는지 확인
                            if num1 in common_quantities and num2 not in common_quantities and num2 < 100000:
                                quantity_price_map[num1] = num2
                                break
                            elif num2 in common_quantities and num1 not in common_quantities and num1 < 100000:
                                quantity_price_map[num2] = num1
                                break
        
        # 3. 결과 생성
        if quantity_price_map:
            result = {
                "has_quantity_pricing": True,
                "price_table": [],
                "quantity_prices": {},
                "vat_included": False
            }
            
            # 정렬된 수량-가격 쌍 생성
            for qty, price in sorted(quantity_price_map.items()):
                if qty > 0 and price > 0:
                    result["price_table"].append({"quantity": qty, "price": price})
                    price_with_vat = price if result["vat_included"] else round(price * 1.1)
                    result["quantity_prices"][qty] = {
                        "price": price,
                        "price_with_vat": price_with_vat,
                        "exact_match": True
                    }
            
            if result["price_table"]:
                logger.info(f"Successfully extracted {len(result['price_table'])} quantity-price pairs using flexible pattern matching")
                return result
    
    except Exception as e:
        logger.error(f"Error analyzing table structure: {e}")
    
    return None

async def handle_quantities_in_columns(soup: BeautifulSoup) -> pd.DataFrame:
    """수량이 컬럼으로 있는 테이블을 처리하는 함수"""
    try:
        html_str = StringIO(str(soup))
        df = pd.read_html(html_str)[0]
        
        logger.debug(f"Table with quantities in columns structure:\n{df.head()}")
        
        # Check if quantities are in column headers
        quantity_cols = [col for col in df.columns if str(col).isdigit() or (
                           isinstance(col, str) and col.isdigit())]
        
        if not quantity_cols:
            logger.debug("No quantity columns found in headers")
            return pd.DataFrame()
            
        logger.info(f"Found {len(quantity_cols)} potential quantity columns: {quantity_cols}")
        
        # Create result dataframe
        result_df = pd.DataFrame(columns=['수량', '일반'])
        
        # Go through rows to find price values - only look at first few rows as price is usually there
        for row_idx in range(min(3, len(df))):
            row = df.iloc[row_idx]
            
            # Process each quantity column separately
            for qty_col in quantity_cols:
                try:
                    # Extract quantity from column name
                    qty = int(qty_col)
                    
                    # Get price from this cell
                    price_val = row[qty_col]
                    
                    # Only process if we have a value
                    if pd.notna(price_val) and price_val != '':
                        # Extract price from cell value
                        price = None
                        if isinstance(price_val, (int, float)):
                            price = int(price_val)
                        elif isinstance(price_val, str):
                            # Clean the string value - remove all non-numeric characters except commas
                            price_str = ''.join(c for c in price_val if c.isdigit() or c == ',')
                            # Remove commas and convert to integer
                            if price_str:
                                price_str = price_str.replace(',', '')
                                # Validate the length of the price string (prevent unreasonably long numbers)
                                if len(price_str) <= 8:  # Maximum 8 digits (up to 99,999,999)
                                    try:
                                        price = int(price_str)
                                    except ValueError:
                                        logger.debug(f"Invalid price value after cleaning: {price_str}")
                                else:
                                    logger.warning(f"Price value too long (> 8 digits): {price_str}")
                        
                        # Add to result if both values are valid and reasonable
                        if qty > 0 and price is not None and price > 0 and price < 10000000:  # Maximum price threshold
                            # Check if we already have this quantity to avoid duplicates
                            if qty not in result_df['수량'].values:
                                result_df = pd.concat([result_df, pd.DataFrame({
                                    '수량': [qty],
                                    '일반': [price]
                                })], ignore_index=True)
                                logger.debug(f"Found valid price {price} for quantity {qty}")
                except Exception as e:
                    logger.debug(f"Error processing column {qty_col} in row {row_idx}: {e}")
                    continue
        
        if not result_df.empty:
            # Sort by quantity and remove duplicates if any
            result_df = result_df.sort_values('수량').drop_duplicates(subset=['수량'])
            logger.info(f"Successfully extracted {len(result_df)} quantity-price pairs from columns")
            
            # Log the extracted data
            logger.info("\n=== 수량별 가격 정보 ===")
            for _, row in result_df.iterrows():
                logger.info(f"수량: {row['수량']}, 가격: {row['일반']}")
                
            return result_df
        else:
            logger.debug("No valid quantity-price pairs found in columns")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error in handle_quantities_in_columns: {e}")
        return pd.DataFrame()

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

async def main(URL: str, check_quantities: bool = True) -> Dict[str, Any]:
    """메인 함수"""
    result = {
        "status": "Error",
        "url": URL,
        "error": None,
        "quantity_pricing": None,
        "price_table": None,
        "dialog_message": None,
        "is_naver_seller": False
    }
    
    try:
        logger.info(f"Starting crawl for URL: {URL}")
        async with async_playwright() as p:
            # 브라우저 설정 개선
            browser = await p.chromium.launch(
                headless=False,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-features=IsolateOrigins,site-per-process',
                    '--disable-site-isolation-trials'
                ]
            )
            
            # 컨텍스트 설정 개선
            context = await browser.new_context(
                viewport={"width": 1366, "height": 768},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                java_script_enabled=True,
                has_touch=True,
                locale='ko-KR',
                timezone_id='Asia/Seoul',
                permissions=['geolocation']
            )

            # 자동화 감지 방지 스크립트 추가
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                window.navigator.chrome = {
                    runtime: {}
                };
            """)

            page = await context.new_page()
            page.on("dialog", handle_dialog)

            logger.info("Navigating to product page")
            await page.goto(URL, wait_until='networkidle')
            
            # 공급사가 네이버인지 확인 후, 네이버인 경우 최저가 버튼 클릭
            try:
                # 공급사명 확인
                supplier_selectors = [
                    'div.basicInfo_mall_title__3IDPK a',
                    'a.seller_name',
                    'span.mall_txt',
                    'div.shop_info a.txt'
                ]
                
                is_naver_seller = False
                for selector in supplier_selectors:
                    if await page.locator(selector).count() > 0:
                        supplier_name = await page.locator(selector).text_content()
                        logger.info(f"Found supplier name: {supplier_name.strip()}")
                        
                        if "네이버" in supplier_name:
                            is_naver_seller = True
                            result["is_naver_seller"] = True
                            logger.info("Detected Naver as the supplier")
                            break
                
                # 네이버 공급사인 경우 최저가 버튼 클릭
                if is_naver_seller:
                    logger.info("Looking for lowest price button since supplier is Naver")
                    lowest_price_selectors = [
                        '//div[contains(@class, "lowestPrice_btn_box")]/div[contains(@class, "buyButton_compare_wrap")]/a[text()="최저가 사러가기"]',
                        '//a[contains(text(), "최저가 사러가기")]',
                        '//a[contains(text(), "최저가")]',
                        '//a[contains(@class, "lowest_price")]',
                        '//button[contains(text(), "최저가")]',
                        '//div[contains(@class, "lowest")]/a',
                        '//div[contains(@class, "price_compare")]/a',
                        '//a[contains(@class, "price_compare")]',
                        '//div[contains(@class, "compare")]/a[contains(@class, "link")]',
                        '//a[contains(@href, "search/gate")]'
                    ]
                    
                    for selector in lowest_price_selectors:
                        try:
                            if await page.locator(selector).count() > 0:
                                logger.info(f"Found lowest price button with selector: {selector}")
                                
                                href = await page.locator(selector).get_attribute('href')
                                if href:
                                    logger.info(f"Navigating to lowest price URL: {href}")
                                    await page.goto(href, wait_until='networkidle', timeout=30000)
                                else:
                                    logger.info("Clicking lowest price button")
                                    await page.locator(selector).click()
                                    await page.wait_for_load_state('networkidle', timeout=30000)
                                    
                                current_url = page.url
                                logger.info(f"After clicking lowest price button, now at URL: {current_url}")
                                break
                        except Exception as e:
                            logger.warning(f"Error with lowest price selector {selector}: {e}")
                            continue
            except Exception as e:
                logger.warning(f"Error checking for Naver supplier or clicking lowest price button: {e}")

            try:
                await page.wait_for_load_state('networkidle')
            except TimeoutError:
                logger.info("No redirection occurred")

            current_url = page.url
            logger.info(f"Current URL: {current_url}")
            logger.info(f"Dialog message: {dialog_message}")
            result["dialog_message"] = dialog_message

            if check_quantities:
                logger.info("Checking for quantity-based pricing")
                quantity_pricing = await extract_quantity_prices(page, current_url, target_quantities=None)
                result["quantity_pricing"] = quantity_pricing
                
                if quantity_pricing["is_promotional_site"] or quantity_pricing["has_quantity_pricing"]:
                    logger.info(f"Detected promotional site: {quantity_pricing.get('supplier_name', 'Unknown')}")
                    logger.info(f"Has quantity pricing: {'Yes' if quantity_pricing['has_quantity_pricing'] else 'No'}")
                    
                    if quantity_pricing.get("price_table"):
                        logger.info("\nQuantity Price Table:")
                        for item in quantity_pricing["price_table"]:
                            price = item["price"]
                            price_with_vat = price if quantity_pricing.get("vat_included", False) else round(price * 1.1)
                            vat_info = "VAT included" if quantity_pricing.get("vat_included", False) else "VAT excluded"
                            logger.info(f"Quantity: {item['quantity']}, Price: {price} ({vat_info}), Price with VAT: {price_with_vat}")

            xpath_to_function = {
                '//div[@class = "price-box"]': handle_login_one,
                '//div[@class = "tbl02"]': handle_login_one,
                '//table[@class = "hompy1004_table_class hompy1004_table_list"]/ancestor::td[1]': handle_login_two,
                '//table[@class = "goods_option"]//td[@colspan = "4"]': handle_login_three,
                '//div[@class = "vi_info"]//div[@class = "tbl_frm01"]': handle_login_one,
                '//div[@class = "specArea"]//div[@class = "w100"]': handle_login_one,
                '//div[contains(@class, "price_info")]//table': handle_login_one,
                '//div[contains(@class, "quantity_price")]//table': handle_login_one,
                '//div[contains(@class, "product_info")]//table': handle_login_one
            }

            df_found = False
            for xpath, function in xpath_to_function.items():
                try:
                    element = await page.query_selector(xpath)
                    if element:
                        logger.info(f"Found element with XPath: {xpath}")
                        html_content = await element.inner_html()
                        soup = BeautifulSoup(html_content, 'html.parser')
                        df_result = await function(soup)
                        
                        if isinstance(df_result, pd.DataFrame) and not df_result.empty:
                            logger.info("Successfully extracted price table")
                            print(df_result)
                            
                            output_dir = os.environ.get('RPA_OUTPUT_DIR', 'C:\\RPA\\Image\\Target')
                            output_path = os.path.join(output_dir, "unit_price_list.csv")
                            df_result.to_csv(output_path, index=False)
                            logger.info(f"Saved price table to: {output_path}")
                            
                            df_found = True
                            result["price_table"] = df_result.to_dict('records')
                            break
                except Exception as e:
                    logger.warning(f"Error processing XPath {xpath}: {e}")
                    continue

            # Added pause to give user time to see results
            logger.info("Pausing for 10 seconds to allow observation of results...")
            await asyncio.sleep(10)
            
            await browser.close()

            if '상품' in dialog_message or '재고' in dialog_message or '품절' in dialog_message:
                logger.info("Product is out of stock or discontinued")
                result["status"] = '삭제'
            else:
                logger.info("Product is available")
                result["status"] = 'OK'
                result["url"] = current_url

            return result
            
    except Exception as e:
        error_msg = f"Error in main function: {str(e)}"
        logger.error(error_msg)
        result["error"] = error_msg
        return result

def get_quantities_from_excel(excel_file: str) -> List[int]:
    """
    엑셀 파일에서 수량 정보를 추출하는 함수
    
    Args:
        excel_file (str): 엑셀 파일 경로
        
    Returns:
        List[int]: 추출된 수량 리스트
    """
    try:
        # 엑셀 파일 읽기
        df = pd.read_excel(excel_file)
        
        # '수량' 또는 'quantity' 열 찾기
        quantity_columns = [col for col in df.columns if '수량' in str(col).lower() or 'quantity' in str(col).lower()]
        
        if not quantity_columns:
            logger.warning(f"No quantity column found in {excel_file}")
            return []
            
        # 첫 번째 수량 열 사용
        quantity_col = quantity_columns[0]
        
        # 수량 데이터 추출 및 정수로 변환
        quantities = []
        for val in df[quantity_col].dropna():
            try:
                qty = int(float(str(val).replace(',', '')))
                if qty > 0:
                    quantities.append(qty)
            except (ValueError, TypeError):
                continue
                
        # 중복 제거 및 정렬
        quantities = sorted(list(set(quantities)))
        
        logger.info(f"Extracted {len(quantities)} unique quantities from {excel_file}")
        return quantities
        
    except Exception as e:
        logger.error(f"Error reading quantities from excel {excel_file}: {e}")
        return []

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('naver_crawler.log'),
            logging.StreamHandler()
        ]
    )
    
    print("===== 네이버 수량별 가격 크롤링 테스트 =====")
    print("참고: 브라우저 창이 열리면 크롤링이 진행 중입니다. 창을 닫지 마세요.")
    print("각 URL 테스트에 약 30초가 소요됩니다.\n")
    
    # Test URLs with various product types
    test_urls = [
        'https://search.shopping.naver.com/catalog/47861603392',  # 일반 상품
        'https://search.shopping.naver.com/catalog/26827347522',  # 판촉물 상품 (수량별 가격)
        'https://search.shopping.naver.com/catalog/39792524949'   # 다른 형태의 판촉물 상품
    ]
    
    # Add more test URLs to the list
    additional_urls = [
        'https://search.shopping.naver.com/catalog/30176542618',  # 텀블러 (판촉물)
        'https://search.shopping.naver.com/catalog/28112237522',  # 볼펜 (판촉물 - 수량별 가격)
    ]
    
    # Combine all test URLs
    all_test_urls = test_urls + additional_urls
    
    async def run_tests():
        total_success = 0
        total_promo_sites = 0
        total_with_quantity_pricing = 0
        
        print(f"테스트할 URL 목록: {len(all_test_urls)}개")
        for i, url in enumerate(all_test_urls, 1):
            print(f"\n[{i}/{len(all_test_urls)}] URL 테스트: {url}")
            try:
                result = await main(url, check_quantities=True)
                
                # Check results
                success = result['status'] == 'OK'
                is_promo = False
                has_qty_pricing = False
                
                if 'quantity_pricing' in result and result['quantity_pricing']:
                    qty_pricing = result['quantity_pricing']
                    is_promo = qty_pricing.get('is_promotional_site', False)
                    has_qty_pricing = qty_pricing.get('has_quantity_pricing', False)
                    
                    # Update counters
                    if is_promo:
                        total_promo_sites += 1
                    if has_qty_pricing:
                        total_with_quantity_pricing += 1
                    
                    # Print detailed results
                    print("\n--- 크롤링 결과 ---")
                    print(f"판촉물 사이트: {'Yes' if is_promo else 'No'}")
                    print(f"수량별 가격 존재: {'Yes' if has_qty_pricing else 'No'}")
                    
                    if has_qty_pricing and 'price_table' in qty_pricing and qty_pricing['price_table']:
                        print("\n수량별 가격표:")
                        print("-" * 50)
                        print("| {:^8} | {:^12} | {:^12} |".format("수량", "단가", "VAT포함"))
                        print("-" * 50)
                        
                        for item in qty_pricing['price_table']:
                            qty = item.get('quantity', 0)
                            price = item.get('price', 0)
                            vat_included = qty_pricing.get('vat_included', False)
                            price_with_vat = price if vat_included else round(price * 1.1)
                            
                            print("| {:>8,d} | {:>12,d} | {:>12,d} |".format(qty, price, price_with_vat))
                        print("-" * 50)
                
                if success:
                    total_success += 1
                    print(f"✅ 테스트 성공: URL={result['url']}")
                else:
                    print(f"❌ 테스트 실패: {result.get('error', 'Unknown error')}")
                
            except Exception as e:
                print(f"❌ 에러 발생: {e}")
        
        # Print summary
        print("\n===== 테스트 결과 요약 =====")
        print(f"총 테스트 URL: {len(all_test_urls)}개")
        print(f"성공: {total_success}개")
        print(f"판촉물 사이트 감지: {total_promo_sites}개")
        print(f"수량별 가격 존재: {total_with_quantity_pricing}개")
        print("===== 테스트 완료 =====")
    
    asyncio.run(run_tests())