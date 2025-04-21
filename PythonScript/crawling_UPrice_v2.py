from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
import logging
import time

# 로깅 설정
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
        logger.warning(f"TypeError in remove_special_chars: {e}")
        return value

def plus_vat(price):
    """부가세 추가"""
    try:
        return float(price) * 1.1
    except (ValueError, TypeError) as e:
        logger.warning(f"Error adding VAT to price {price}: {e}")
        return 0.0

def handle_login_one(soup):
    """첫 번째 유형의 테이블 처리"""
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
            df['일반'] = df['일반'].apply(lambda x: plus_vat(x))
            # 숫자로 변환 가능한 수량만 포함
            df = df[df['수량'].apply(lambda x: str(x).isdigit())]
            df['수량'] = df['수량'].astype('int64')
            df.sort_values(by='수량', inplace=True, ignore_index=True)
            logger.info(f"handle_login_one: 처리 완료, {len(df)}개 행 추출")
            return df
        else:
            logger.warning("handle_login_one: 테이블을 찾을 수 없음")
            return pd.DataFrame(columns=['수량', '일반'])
    except Exception as e:
        logger.error(f"handle_login_one 오류: {e}")
        return pd.DataFrame(columns=['수량', '일반'])

def handle_login_two(soup):
    """두 번째 유형의 테이블 처리"""
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
                logger.debug(f"회원 인덱스 없음: {e}")
            
            # 숫자로 변환 가능한 수량만 포함
            df = df[df['수량'].apply(lambda x: str(x).isdigit())]
            df['수량'] = df['수량'].astype('int64')
            df.sort_values(by='수량', inplace=True, ignore_index=True)
            logger.info(f"handle_login_two: 처리 완료, {len(df)}개 행 추출")
            return df
        else:
            logger.warning("handle_login_two: 테이블을 찾을 수 없음")
            return pd.DataFrame(columns=['수량', '일반'])
    except Exception as e:
        logger.error(f"handle_login_two 오류: {e}")
        return pd.DataFrame(columns=['수량', '일반'])

def handle_login_three(soup):
    """세 번째 유형의 테이블 처리 (입력 필드)"""
    try:
        # 테이블 찾기 시도
        tables = soup.find_all('table')
        if tables:
            quantities = []
            prices = []
            
            # 입력 필드 찾기
            qu_inputs = soup.find_all('input', class_='qu')
            pr_inputs = soup.find_all('input', class_='pr')
            
            for input_tag in qu_inputs:
                try:
                    quantities.append(int(input_tag['value']))
                except (ValueError, KeyError) as e:
                    logger.warning(f"수량 변환 오류: {e}")
            
            for input_tag in pr_inputs:
                try:
                    prices.append(int(input_tag['value'].replace(',', '')))
                except (ValueError, KeyError) as e:
                    logger.warning(f"가격 변환 오류: {e}")
            
            if len(quantities) == len(prices) and len(quantities) > 0:
                # 데이터프레임 생성
                df = pd.DataFrame({
                    '수량': quantities,
                    '일반': prices
                })
                df['일반'] = df['일반'].apply(lambda x: plus_vat(x))
                df.sort_values(by='수량', inplace=True, ignore_index=True)
                logger.info(f"handle_login_three: 처리 완료, {len(df)}개 행 추출")
                return df
            else:
                logger.warning(f"handle_login_three: 수량({len(quantities)})과 가격({len(prices)}) 개수 불일치")
                return pd.DataFrame(columns=['수량', '일반'])
        else:
            logger.warning("handle_login_three: 테이블을 찾을 수 없음")
            return pd.DataFrame(columns=['수량', '일반'])
    except Exception as e:
        logger.error(f"handle_login_three 오류: {e}")
        return pd.DataFrame(columns=['수량', '일반'])

def handle_login_four(soup):
    """네 번째 유형의 테이블 처리 (단순 테이블)"""
    try:
        # 테이블 찾기 시도
        tables = soup.find_all('table')
        if tables:
            df = pd.read_html(str(tables[0]))[0]
            
            # 표준 컬럼 형식으로 변환 시도
            if '수량' in df.columns and set(['가격', '단가', '일반']).intersection(set(df.columns)):
                price_col = [col for col in ['가격', '단가', '일반'] if col in df.columns][0]
                df = df[['수량', price_col]].copy()
                df.columns = ['수량', '일반']
                
                # 데이터 정제
                df['수량'] = df['수량'].astype(str).apply(clean_quantity)
                df = df[df['수량'].apply(lambda x: str(x).isdigit())]
                df['수량'] = df['수량'].astype('int64')
                
                # 가격 정제
                df['일반'] = df['일반'].astype(str).apply(remove_special_chars)
                df['일반'] = df['일반'].apply(lambda x: plus_vat(x) if x and str(x).isdigit() else 0.0)
                
                df.sort_values(by='수량', inplace=True, ignore_index=True)
                logger.info(f"handle_login_four: 처리 완료, {len(df)}개 행 추출")
                return df
            else:
                logger.warning(f"handle_login_four: 적절한 컬럼을 찾을 수 없음. 사용 가능한 컬럼: {df.columns.tolist()}")
                return pd.DataFrame(columns=['수량', '일반'])
        else:
            logger.warning("handle_login_four: 테이블을 찾을 수 없음")
            return pd.DataFrame(columns=['수량', '일반'])
    except Exception as e:
        logger.error(f"handle_login_four 오류: {e}")
        return pd.DataFrame(columns=['수량', '일반'])

def main(URL, output_path=None):
    """네이버 쇼핑 URL에서 단가 정보 스크래핑"""
    global dialog_message
    dialog_message = ""
    
    if not output_path:
        output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "unit_price_list.csv")
    
    logger.info(f"UPrice 스크래핑 시작: {URL}")
    
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context()
            page = context.new_page()
            page.on("dialog", handle_dialog)

            page.goto(URL, wait_until='networkidle', timeout=60000)
            logger.info("페이지 로딩 완료")
            
            # 대화상자 메시지 확인
            if dialog_message:
                logger.info(f"대화상자 메시지: {dialog_message}")

            # XPath와 처리 함수 매핑
            xpath_to_function = {
                '//div[@class = "price-box"]': handle_login_one, # 부가세 별도
                '//div[@class = "tbl02"]' : handle_login_one, # 부가세 별도
                '//table[@class = "hompy1004_table_class hompy1004_table_list"]/ancestor::td[1]' : handle_login_two, # 부가세 별도
                '//table[@class = "goods_option"]//td[@colspan = "4"]' : handle_login_three, # 부가세 별도
                '//div[@class = "vi_info"]//div[@class = "tbl_frm01"]' : handle_login_one, # 부가세 별도
                '//div[@class = "specArea"]//div[@class = "w100"]' : handle_login_one
            }

            # 페이지 내용 확인용 로깅 (디버깅용)
            page_content = page.content()
            logger.debug(f"페이지 제목: {page.title()}")
            
            result_df = None
            
            # 각 XPath와 연결된 함수 실행
            for xpath, function in xpath_to_function.items():
                element = page.query_selector(xpath)
                if element:
                    logger.info(f"찾은 요소 XPath: {xpath}")
                    html_content = element.inner_html()
                    soup = BeautifulSoup(html_content, 'html.parser')
                    result = function(soup)  # soup을 함수로 전달
                    
                    if isinstance(result, pd.DataFrame) and not result.empty:
                        logger.info(f"처리 결과: {len(result)}개 행")
                        logger.debug(f"첫 3개 행:\n{result.head(3)}")
                        result_df = result
                        
                        # 결과를 CSV로 저장
                        result.to_csv(output_path, index=False)
                        logger.info(f"결과 저장 완료: {output_path}")
                        break  # 첫 번째 성공한 결과 사용
                    else:
                        logger.warning(f"XPath {xpath}에서 결과를 추출할 수 없음")

            browser.close()
            logger.info("브라우저 종료됨")

            # 특수 메시지 확인
            if '상품' in dialog_message or '재고' in dialog_message or '품절' in dialog_message:
                logger.warning(f"상품 관련 경고: {dialog_message}")
                return '삭제', None
            else:
                return None, result_df
                
        except Exception as e:
            logger.error(f"UPrice 스크래핑 오류: {e}", exc_info=True)
            return f"오류: {str(e)}", None

if __name__ == "__main__":
    # 기본 로깅 설정
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # 테스트용 URL
    test_url = 'https://cr.shopping.naver.com/adcr.nhn?x=F6bdQ9qfHPwtpI2HrLt62%2F%2F%2F%2Fw%3D%3DswH9%2FfAhl792HEc3QWsbm8ciO0jx8ZUaesqSWbolX3jXoPuGgIu13OnfWMxqfzDXyAiOcvbHzqStYpJL2kRlHSZ16fvCeS1Hq2ZdBXIBpaWL2igVhxlRth5Mog9fr4irD3bb%2BUm4eH6wHJnurPFQemSf9sHLqXJg2XjyXDUYWGlfMHmjhjScrcESWN0LndjJlhbDC55qlMeuK5Y8fDLagb6S9FI7cK8IzH6Dg6%2FmGWJIHs%2F2PHkpmvWjL2qy1nHubU5MgDjHo1wbxBNEamXCVIIZa2nTLhT%2Fgj4z6L9HR88g0bCU1zCIYJwY5B%2FCzwr0CsIvKrW6xPOsI2ecGWPIk034fAL4Tw8PrVhquDw3arsw7kvaEHl0HGYPZYZkv9bVrZVhaMMtvk0F817DwJeWGcdAu1jpaMvq8iLAIrUvEPuo5t4wOigeRYH0s0cshmlDZbzdjQIoeUISzDCcx4%2BHBZZtBDPZz8DPbT4SICGP7eC7fdeEb4CY9AlrfXPteLbNfu27zNZsU3z%2FyoTj%2FUN7DVgF%2FriLXtuP5E%2FXBfyrWN6%2BINN9RwalG9jaYNyWn%2FJbyzY6of2Xpggjm6A4Z8HgJhg%3D%3D&nvMid=46807468690&catId=50003564'
    
    # 테스트 실행
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_path = f"UPrice_test_results_{timestamp}.csv"
    status, results = main(test_url, output_path)
    
    print(f"상태: {status}")
    if results is not None:
        print(f"결과 (총 {len(results)}개 행):")
        print(results)