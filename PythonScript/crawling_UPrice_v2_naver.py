from playwright.sync_api import sync_playwright, TimeoutError
from bs4 import BeautifulSoup
import pandas as pd

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

def main(URL):
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

            xpath_to_function = {
                '//div[@class = "price-box"]': handle_login_one, # 부가세 별도
                '//div[@class = "tbl02"]' : handle_login_one, # 부가세 별도
                '//table[@class = "hompy1004_table_class hompy1004_table_list"]/ancestor::td[1]' : handle_login_two, # 부가세 별도
                '//table[@class = "goods_option"]//td[@colspan = "4"]' : handle_login_three, # 부가세 별도
                '//div[@class = "vi_info"]//div[@class = "tbl_frm01"]' : handle_login_one, # 부가세 별도
                '//div[@class = "specArea"]//div[@class = "w100"]' : handle_login_one
            }

            # 각 XPath와 연결된 함수 실행
            for xpath, function in xpath_to_function.items():
                element = page.query_selector(xpath)
                if element:
                    html_content = element.inner_html()
                    soup = BeautifulSoup(html_content, 'html.parser')
                    result = function(soup)  # soup을 함수로 전달
                    print(result)  # 결과 출력
                    result.to_csv("C:\\RPA\\Image\\Target\\unit_price_list.csv", index=False)

            browser.close()

            if '상품' in dialog_message or '재고' in dialog_message or '품절' in dialog_message:
                print('삭제')
                return '삭제'
            else:
                print('None')
                return current_url
        except Exception as e:
            print(e)

if __name__ == "__main__":
    URL = 'https://search.shopping.naver.com/catalog/47861603392'
    main(URL)