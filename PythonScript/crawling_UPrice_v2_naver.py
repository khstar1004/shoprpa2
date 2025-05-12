from playwright.sync_api import sync_playwright, TimeoutError
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
import time
import logging

# Set up logging
logger = logging.getLogger(__name__)

dialog_message = ""

def handle_dialog(dialog):
    global dialog_message
    # 경고창의 메시지 출력
    dialog_message = dialog.message
    # print(f"Dialog message: {dialog.message}")
    # 경고창을 수락하거나 거절
    dialog.accept()  # 또는 dialog.dismiss()

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

def handle_login_one(soup):
    try:
        # 테이블 찾기 시도
        tables = soup.find_all('table')
        if tables:
            df = pd.read_html(str(tables[0]))[0]  # 첫 번째 테이블을 DataFrame으로 변환
            df = df.T
            df.reset_index(drop=False, inplace=True)
            df.columns = df.iloc[0]
            df.drop(index=0, inplace=True)
            df.columns = ['수량', '일반']
            df = df.map(remove_special_chars)
            df['일반'] = df['일반'].apply(lambda x: int(x)*1.1)
            df['수량'] = df['수량'].astype('int64')
            df.sort_values(by='수량', inplace=True, ignore_index=True)
            return df
        else:
            return "No table found"  # 테이블이 없는 경우 메시지 반환
    except Exception as e:
        print('error')

def handle_login_two(soup):
    # 테이블 찾기 시도
    try:
        tables = soup.find_all('table')
        if tables:
            df = pd.read_html(str(tables[0]))[0]  # 첫 번째 테이블을 DataFrame으로 변환
            df = df.T
            df.reset_index(drop=False, inplace=True)
            df.columns = df.iloc[0]
            df.drop(index=0, inplace=True)
            df['수량'] = df['수량'].apply(clean_quantity)
            df = df.map(remove_special_chars)
            try:
                df.drop('회원', axis=1, inplace=True)
            except Exception as e:
                print('회원 인덱스 없음')
            df.sort_values(by='수량', inplace=True, ignore_index=True)
            return df
        else:
            return "No table found"  # 테이블이 없는 경우 메시지 반환
    except Exception as e:
        print('error')

def handle_login_three(soup):
    try:
        # 테이블 찾기 시도
        tables = soup.find_all('table')
        if tables:
            quantities = [int(input_tag['value']) for input_tag in soup.find_all('input', class_='qu')]
            prices = [int(input_tag['value'].replace(',', '')) for input_tag in soup.find_all('input', class_='pr')]

            # 데이터프레임 생성
            df = pd.DataFrame({
                '수량': quantities,
                '일반': prices
            })
            df['일반'] = df['일반'].apply(lambda x: int(x)*1.1)
            df.sort_values(by='수량', inplace=True, ignore_index=True)
            
            return df
        else:
            return "No table found"  # 테이블이 없는 경우 메시지 반환
    except Exception as e:
        print('error')

def handle_login_four(soup):
    try:
        # 테이블 찾기 시도
        tables = soup.find_all('table')
        if tables:
            df = pd.read_html(str(tables[0]))[0]
            return df
        else:
            return "No table found"  # 테이블이 없는 경우 메시지 반환
    except Exception as e:
        print('error')

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

# New function to extract quantity-based prices
def extract_quantity_prices(page, url):
    """
    Extract quantity-based prices from a product page.
    
    Args:
        page: Playwright page object
        url: URL of the product page
        
    Returns:
        dict: Dictionary with quantity price information
    """
    result = {
        "is_promotional_site": False,
        "has_quantity_pricing": False,
        "quantity_prices": {},
        "price_table": None,
        "vat_included": False,
        "supplier_name": "",
    }
    
    try:
        # Navigate to the page
        page.goto(url, wait_until='networkidle')
        
        # Extract supplier name
        supplier_selectors = [
            'div.mall_name', 'span.mall_name', 'a.mall_name',
            'div.seller_name', 'span.seller_name', 'a.seller_name',
            'div[class*="mall_title"] a', 'a[class*="seller"]'
        ]
        
        for selector in supplier_selectors:
            try:
                if page.query_selector(selector):
                    result["supplier_name"] = page.query_selector(selector).inner_text().strip()
                    break
            except:
                continue
                
        # Check if it's a promotional site based on supplier name
        if result["supplier_name"]:
            result["is_promotional_site"] = is_promotional_supplier(result["supplier_name"])
            
        # Look for quantity-based pricing tables
        quantity_table_selectors = [
            'table:has(tr:has(th:text("수량")))',
            'table:has(tr:has(td:text("수량")))',
            'table.quantity_price__table',
            'div.price-box table',
            'div.quantity_discount table',
            'div.quantity_pricing table',
            'table.price_by_quantity'
        ]
        
        for selector in quantity_table_selectors:
            if page.query_selector(selector):
                # Found a potential quantity price table
                table_html = page.query_selector(selector).inner_html()
                
                # Use BeautifulSoup to extract the table
                soup = BeautifulSoup(f"<table>{table_html}</table>", 'html.parser')
                
                # Try different methods to extract the table
                table_handlers = [handle_login_one, handle_login_two, handle_login_three, handle_login_four]
                
                for handler in table_handlers:
                    try:
                        df = handler(soup)
                        if isinstance(df, pd.DataFrame) and not df.empty:
                            # Successfully extracted price table
                            result["has_quantity_pricing"] = True
                            result["is_promotional_site"] = True
                            
                            # Convert to a list of dicts for easier use
                            price_table = []
                            for _, row in df.iterrows():
                                try:
                                    qty = int(row['수량']) if '수량' in row else None
                                    price = None
                                    
                                    # Try to find the price column
                                    for col in ['일반', '단가', '가격']:
                                        if col in row and row[col]:
                                            price = int(row[col])
                                            break
                                            
                                    if qty and price:
                                        price_table.append({"quantity": qty, "price": price})
                                except:
                                    continue
                                    
                            if price_table:
                                # Sort price table by quantity
                                price_table.sort(key=lambda x: x["quantity"])
                                result["price_table"] = price_table
                                
                                # Check for VAT info on the page
                                vat_texts = ["부가세별도", "부가세 별도", "VAT별도", "VAT 별도"]
                                page_text = page.content()
                                
                                for vat_text in vat_texts:
                                    if vat_text in page_text:
                                        result["vat_included"] = False
                                        break
                                    elif "부가세포함" in page_text or "VAT포함" in page_text:
                                        result["vat_included"] = True
                                        break
                                        
                                # Fill quantity_prices based on the table
                                for item in price_table:
                                    qty = item["quantity"]
                                    price = item["price"]
                                    price_with_vat = price if result["vat_included"] else round(price * 1.1)
                                    
                                    result["quantity_prices"][qty] = {
                                        "price": price,
                                        "price_with_vat": price_with_vat,
                                        "exact_match": True
                                    }
                                    
                                break  # Successfully processed the table
                    except Exception as e:
                        logger.warning(f"Error processing table with handler {handler.__name__}: {e}")
                        continue
                        
                if result["has_quantity_pricing"]:
                    break  # Found and processed a table successfully
                
        return result
            
    except Exception as e:
        logger.error(f"Error extracting quantity prices: {e}")
        return result

def main(URL, check_quantities=True):
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context()
            page = context.new_page()
            page.on("dialog", handle_dialog)

            # URL = 'https://cr.shopping.naver.com/adcr.nhn?x=F8yhiNRHlx%2Fq5Q5HuzZ1sf%2F%2F%2Fw%3D%3DsXvx0s141%2FJRS0j1HQc2Vylepznd7YwqFZpdDtYqHtX6AxkSjc3u9GGRp4KPLuZnUQc34ftwdl3qvQwOHxu8IjTb6yvTLpGgg3bFMUF6BqpS9sIlp9BQt%2F5yMGIZYXftYrqwXiy%2BANPqRop8WEAuRnFSkQns6Gt4QsLR9NbNNuyOhQbTDJc1lnV6IvqrutO2pknF60kc2vL5Bb5oA0uQcV4x0czrYzfsHrwFNHM30Ba654J8UO7LDGXdPU9Do8oyM%2ButNgbXZ3dsXpLc8YhINXHTz1hiY843N28Ka28FlWXi%2FjjYqqhQXkgp4IhXkaKASscM0CgwfJfk84wxZgU8a5L9nAWQx9qxYcXVru5G9kDitcUfinZpq1w9IhZAChYLWeIMjE83Ym368oliJRn4ho7%2Fe3KiTp0hRhnRQMb1mjB0iOc4Y9eXn8DLvyQYICSvhGVPw5%2BaBtbckSfWXEgomXxO0ecOe%2BoSooeDCZxJqZ292Zp1pQdPRxGyhWUJIDMRVWHme3dhbZnUnByuEMV3hvRueT0dJhzm7GVj3dV4%2BoTFjUSAB1KvX50d7UUWMp24kh0obTELoIlzYidN1XgmuPsQRcKHIH4rJaSi9U8azs7SSFgZKv3Z84NTkSeFQOs%2Bbzr0LUTF5AtKOY9dPCx4%2BzqEsTXFJDRv0l5OZioap8vbTlh590rl6%2BPa6VDOzyWpN&nvMid=39792524949&catId=50003344'
            page.goto(URL, wait_until='networkidle')
            
            href = page.locator('//div[contains(@class, "lowestPrice_btn_box")]/div[contains(@class, "buyButton_compare_wrap")]/a[text()="최저가 사러가기"]').get_attribute('href')
            print(href)
            page.goto(href, wait_until='networkidle')

            try:
                with page.expect_navigation(wait_until='networkidle'):
                    pass
            except TimeoutError:
                print("리다이렉션이 발생하지 않았습니다.")

            current_url = page.url
            print(current_url)
            print(dialog_message)

            # Check if it's a promotional site and extract quantity pricing
            result = {}
            if check_quantities:
                quantity_pricing = extract_quantity_prices(page, current_url)
                
                if quantity_pricing["is_promotional_site"] or quantity_pricing["has_quantity_pricing"]:
                    print(f"Detected promotional site: {quantity_pricing['supplier_name']}")
                    print(f"Has quantity pricing: {'Yes' if quantity_pricing['has_quantity_pricing'] else 'No'}")
                    
                    if quantity_pricing["price_table"]:
                        print("\nQuantity Price Table:")
                        for item in quantity_pricing["price_table"]:
                            price = item["price"]
                            price_with_vat = price if quantity_pricing["vat_included"] else round(price * 1.1)
                            vat_info = "VAT included" if quantity_pricing["vat_included"] else "VAT excluded"
                            print(f"Quantity: {item['quantity']}, Price: {price} ({vat_info}), Price with VAT: {price_with_vat}")
                
                result["quantity_pricing"] = quantity_pricing

            xpath_to_function = {
                '//div[@class = "price-box"]': handle_login_one, # 부가세 별도
                '//div[@class = "tbl02"]' : handle_login_one, # 부가세 별도
                '//table[@class = "hompy1004_table_class hompy1004_table_list"]/ancestor::td[1]' : handle_login_two, # 부가세 별도
                '//table[@class = "goods_option"]//td[@colspan = "4"]' : handle_login_three, # 부가세 별도
                '//div[@class = "vi_info"]//div[@class = "tbl_frm01"]' : handle_login_one, # 부가세 별도
                '//div[@class = "specArea"]//div[@class = "w100"]' : handle_login_one
            }

            # 각 XPath와 연결된 함수 실행
            df_found = False
            for xpath, function in xpath_to_function.items():
                element = page.query_selector(xpath)
                if element:
                    html_content = element.inner_html()
                    soup = BeautifulSoup(html_content, 'html.parser')
                    df_result = function(soup)  # soup을 함수로 전달
                    if isinstance(df_result, pd.DataFrame) and not df_result.empty:
                        print(df_result)
                        # Save to file if needed
                        output_dir = os.environ.get('RPA_OUTPUT_DIR', 'C:\\RPA\\Image\\Target')
                        df_result.to_csv(os.path.join(output_dir, "unit_price_list.csv"), index=False)
                        df_found = True
                        result["price_table"] = df_result.to_dict('records')
                        break

            browser.close()

            if '상품' in dialog_message or '재고' in dialog_message or '품절' in dialog_message:
                print('삭제')
                result["status"] = '삭제'
                return result
            else:
                print('None')
                result["status"] = 'OK'
                result["url"] = current_url
                return result
        except Exception as e:
            print(e)
            return {"status": "Error", "error": str(e)}

if __name__ == "__main__":
    # URL = 'https://search.shopping.naver.com/catalog/47861603392'
    URL = 'https://search.shopping.naver.com/catalog/26827347522'  # A product with potential quantity pricing
    result = main(URL, check_quantities=True)
    print("\nFinal result:", result)