import os
import subprocess
import threading
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import time
from bs4 import BeautifulSoup
import pandas as pd
import re

def fetch_html_content(url):
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.binary_location = "C:\\Users\\USER2\\AppData\\Local\\Programs\\Python\\Python39\\Tools\\chrome-win64\\chrome.exe"
    driver_path = 'C:\\Users\\USER2\\AppData\\Local\\Programs\\Python\\Python39\\Tools\\chromedriver-win64\\chromedriver.exe'
    service = Service(driver_path, creationflags=subprocess.CREATE_NO_WINDOW)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get(url)
    
    time.sleep(15)  # 15초 동안 대기
    try:
        html_content = driver.page_source
        return html_content
    
    except Exception as e:
        return 'url에러'

    finally:
        driver.quit()

def driver_thread_one(html_content, result, event):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        # 1. <table class="wfull"> 요소를 찾습니다.
        outer_table = soup.find('table', class_='wfull')
        
        if outer_table:
            # 2. 그 안에서 <div class="tbl02"> 요소를 찾습니다.
            div_tbl02 = outer_table.find('div')
            
            if div_tbl02:
                # 3. 해당 <div> 안에 있는 <table> 요소를 추출합니다.
                inner_table = div_tbl02.find('table')
                
                if inner_table:
                    # 첫 번째 행(수량)을 처리합니다.
                    rows = inner_table.find_all('tr')
                    quantity_data = []
                    for cell in rows[0].find_all('th')[1:]:  # 첫 번째 칸은 '수　량'이므로 스킵
                        quantity_data.append(int(re.sub(r'[^0-9]', '', cell.text.replace(",", ""))))
                    
                    # 두 번째 행(가격)을 처리합니다.
                    price_data = []
                    for cell in rows[1].find_all('td'):
                        price_data.append(int(re.sub(r'[^0-9]', '', cell.text.replace(",", ""))))

                    df_data = {
                        '수량': quantity_data,
                        '일반가': price_data
                    }
                    df = pd.DataFrame(df_data)

                    df['수량'] = df['수량'].astype(int)
                    df['일반가'] = df['일반가'].astype(float).apply(lambda x: x * 1.1)

                    df_sorted = df.sort_values(by='수량', ascending=True)
                    result['data'] = df_sorted  # 결과를 딕셔너리에 저장
                    
                    event.set()  # 결과가 준비되었음을 알림

                else:
                    print("스레드 1 오류가 발생했습니다: 내부 테이블을 찾지 못했습니다.")
                    event.set()  # 오류 발생 시에도 이벤트 설정
            else:
                print("스레드 1 오류가 발생했습니다: <div class='tbl02'>를 찾지 못했습니다.")
                event.set()  # 오류 발생 시에도 이벤트 설정
        else:
            print("스레드 1 오류가 발생했습니다: <table class='wfull'>를 찾지 못했습니다.")
            event.set()  # 오류 발생 시에도 이벤트 설정

    except Exception as e:
        print("스레드 1 오류가 발생했습니다:", e)
        event.set()  # 오류 발생 시에도 이벤트 설정

def driver_thread_two(html_content, result, event):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        # 원하는 <table> 태그 추출 (cellspacing, cellpadding, width, border 속성 확인)
        target_table = soup.find('table', attrs={'cellspacing': '0', 'cellpadding': '0', 'width': '100%', 'border': '1'})
        
        # <thead> 추출
        thead = target_table.find('thead')

        # <tbody> 추출
        tbody = target_table.find('tbody')

        # 행과 열 순회하며 데이터 추출
        data = []
        
        # <thead>의 행을 추출하여 컬럼명을 가져옴
        header_row = thead.find('tr')
        header_columns = header_row.find_all('th')
        header = [column.text.replace(',','').strip() for column in header_columns]
        data.append(header)
        
        # <tbody>의 행을 추출하여 데이터 가져옴
        for row in tbody.find_all('tr'):
            cells = row.find_all('td')
            row_data = [cell.text.replace(',','').strip() for cell in cells]
            data.append(row_data)

        quantity_data = data[0]
        price_data = data[1]

        quantity_data = [re.sub(r'[^\d]', '', value) for value in quantity_data]
        quantity_data = [value for value in quantity_data if value != '']
        price_data = [re.sub(r'[^\d]', '', value) for value in price_data]
        price_data = [value for value in price_data if value != '']

        df_data = {
            '수량': quantity_data,
            '일반가': price_data
        }
        df = pd.DataFrame(df_data)

        df['수량'] = df['수량'].astype(int)
        df['일반가'] = df['일반가'].astype(float).apply(lambda x: x * 1.1)

        df_sorted = df.sort_values(by='수량', ascending=True)

        result['data'] = df_sorted  # 결과를 딕셔너리에 저장
        event.set()  # 결과가 준비되었음을 알림

    except Exception as e:
        print("스레드 2 오류가 발생했습니다:", e)
        event.set()  # 오류 발생 시에도 이벤트 설정

def driver_thread_three(html_content, result, event):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        # 원하는 <table> 태그 추출 (class가 'bg_table1'인 <table> 태그)
        target_table = soup.find('table', class_='bg_table1')

        # <thead> 추출
        thead = target_table.find('thead')

        # <tbody> 추출
        tbody = target_table.find('tbody')

        # 행과 열 순회하며 데이터 추출
        data = []
        
        # <thead>의 행을 추출하여 컬럼명을 가져옴
        header_row = thead.find('tr')
        header_columns = header_row.find_all(['th', 'td'])
        header = [column.text.replace(',','').strip() for column in header_columns]
        data.append(header)
        
        # <tbody>의 행을 추출하여 데이터 가져옴
        for row in tbody.find_all('tr'):
            cells = row.find_all(['th', 'td'])
            row_data = [cell.text.replace(',','').strip() for cell in cells]
            data.append(row_data)

        quantity_data = data[0]
        price_data = data[1]

        quantity_data = [re.sub(r'[^\d]', '', value) for value in quantity_data]
        quantity_data = [value for value in quantity_data if value != '']
        price_data = [re.sub(r'[^\d]', '', value) for value in price_data]
        price_data = [value for value in price_data if value != '']

        df_data = {
            '수량': quantity_data,
            '일반가': price_data
        }
        df = pd.DataFrame(df_data)

        df['수량'] = df['수량'].astype(int)
        df['일반가'] = df['일반가'].astype(float).apply(lambda x: x * 1.1)

        df_sorted = df.sort_values(by='수량', ascending=True)

        result['data'] = df_sorted  # 결과를 딕셔너리에 저장
        event.set()  # 결과가 준비되었음을 알림

    except Exception as e:
        print("스레드 3 오류가 발생했습니다:", e)
        event.set()  # 오류 발생 시에도 이벤트 설정

def driver_thread_four(html_content, result, event):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        # 원하는 <table> 태그 추출
        target_table = soup.find('table', width='460', class_='pv_tbl')

        # <tbody> 추출
        tbody = target_table.find('tbody')

        # 행과 열 순회하며 데이터 추출
        data = []
        
        # <tbody>의 행을 추출하여 데이터 가져옴
        for row in tbody.find_all('tr'):
            cells = row.find_all(['th', 'td'])
            row_data = [cell.text.replace(',','').strip() for cell in cells]
            data.append(row_data)

        if len(data) >= 4:
            quantity_data = data[1]
            price_data = data[3]
        elif len(data) == 3:
            quantity_data = data[0]
            price_data = data[2]
        elif len(data) == 2:
            quantity_data = data[0]
            price_data = data[1]

        quantity_data = [re.sub(r'[^\d]', '', value) for value in quantity_data]
        quantity_data = [value for value in quantity_data if value != '']
        price_data = [re.sub(r'[^\d]', '', value) for value in price_data]
        price_data = [value for value in price_data if value != '']

        df_data = {
            '수량': quantity_data,
            '일반가': price_data
        }
        df = pd.DataFrame(df_data)

        df['수량'] = df['수량'].astype(int)
        df['일반가'] = df['일반가'].astype(float).apply(lambda x: x * 1.1)

        df_sorted = df.sort_values(by='수량', ascending=True)

        result['data'] = df_sorted  # 결과를 딕셔너리에 저장
        event.set()  # 결과가 준비되었음을 알림

    except Exception as e:
        print("스레드 4 오류가 발생했습니다:", e)
        event.set()  # 오류 발생 시에도 이벤트 설정

def driver_thread_five(html_content, result, event):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        # 원하는 <table> 태그 추출
        target_table = soup.find('table', class_='hompy1004_table_class hompy1004_table_list', width='100%')  # class와 width 속성을 이용해 <table> 태그 찾기

        # <tbody> 추출
        tbody = target_table.find('tbody')

        # 행과 열 순회하며 데이터 추출
        data = []
        
        # <tbody>의 행을 추출하여 데이터 가져옴
        for row in tbody.find_all('tr'):
            cells = row.find_all(['th', 'td'])
            row_data = [cell.text.replace(',','').replace('원','').replace('개','').replace('\n\t', '').replace('~','').strip() for cell in cells]
            data.append(row_data)

        quantity_data = data[0]

        for i in range(1, len(quantity_data)):
            if '미만' in quantity_data[i]:
                quantity_data[i] = str(int(quantity_data[i].split()[0]) - 1) + ' 미만'
            quantity_data[i] = quantity_data[i].replace(' 미만', '')

        price_data = data[2]

        quantity_data = [re.sub(r'[^\d]', '', value) for value in quantity_data]
        quantity_data = [value for value in quantity_data if value != '']
        price_data = [re.sub(r'[^\d]', '', value) for value in price_data]
        price_data = [value for value in price_data if value != '']

        df_data = {
            '수량': quantity_data,
            '일반가': price_data
        }
        df = pd.DataFrame(df_data)

        df['수량'] = df['수량'].astype(int)
        # df['일반가'] = df['일반가'].astype(float).apply(lambda x: x * 1.1)

        df_sorted = df.sort_values(by='수량', ascending=True)

        result['data'] = df_sorted  # 결과를 딕셔너리에 저장
        event.set()  # 결과가 준비되었음을 알림

    except Exception as e:
        print("스레드 5 오류가 발생했습니다:", e)
        event.set()  # 오류 발생 시에도 이벤트 설정

def driver_thread_six(html_content, result, event):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        # 첫 번째 테이블 찾기
        first_table = soup.find('table', {'id': 'prd_amt'})

        # 첫 번째 테이블 바로 다음에 위치한 테이블 찾기
        second_table = first_table.find_next_sibling('table') if first_table else None

        # 첫 번째 테이블의 데이터 추출
        quantity_data = [cell.text.replace('개',"").strip() for cell in first_table.find('tr').find_all('td')] if first_table else []

        # 두 번째 테이블의 데이터 추출
        price_data = [cell.text.replace('원','').replace(',','').strip() for cell in second_table.find('tr').find_all('td')] if second_table else []

        quantity_data = [re.sub(r'[^\d]', '', value) for value in quantity_data]
        quantity_data = [value for value in quantity_data if value != '']
        price_data = [re.sub(r'[^\d]', '', value) for value in price_data]
        price_data = [value for value in price_data if value != '']

        df_data = {
            '수량': quantity_data,
            '일반가': price_data
        }
        df = pd.DataFrame(df_data)

        df['수량'] = df['수량'].astype(int)

        df_sorted = df.sort_values(by='수량', ascending=True)

        result['data'] = df_sorted  # 결과를 딕셔너리에 저장
        event.set()  # 결과가 준비되었음을 알림

    except Exception as e:
        print("스레드 6 오류가 발생했습니다:", e)
        event.set()  # 오류 발생 시에도 이벤트 설정

def driver_thread_seven(html_content, result, event):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        # 지정된 ID를 사용하여 테이블 태그를 찾습니다.
        table = soup.find('table', {'id': 'price_table'})

        # 테이블의 모든 행을 순회하고, 각 행의 모든 셀 내부의 input 태그의 value 값을 추출합니다.
        data = []
        for row in table.find_all('tr'):
            row_data = []
            for cell in row.find_all('td'):
                input_tag = cell.find('input')
                if input_tag:
                    value = input_tag.get('value', '')  # 'value' 속성값을 가져옵니다. 만약 없다면 빈 문자열을 반환합니다.
                    row_data.append(value)
                else:
                    row_data.append(cell.text.strip())
            data.append(row_data)

        quantity_data = data[0]
        price_data = data[1]

        quantity_data = [re.sub(r'[^\d]', '', value) for value in quantity_data]
        quantity_data = [value for value in quantity_data if value != '']
        price_data = [re.sub(r'[^\d]', '', value) for value in price_data]
        price_data = [value for value in price_data if value != '']

        df_data = {
            '수량': quantity_data,
            '일반가': price_data
        }
        df = pd.DataFrame(df_data)

        df['수량'] = df['수량'].astype(int)
        df['일반가'] = df['일반가'].astype(float).apply(lambda x: x * 1.1)

        df_sorted = df.sort_values(by='수량', ascending=True)

        result['data'] = df_sorted  # 결과를 딕셔너리에 저장
        event.set()  # 결과가 준비되었음을 알림

    except Exception as e:
        print("스레드 7 오류가 발생했습니다:", e)
        event.set()  # 오류 발생 시에도 이벤트 설정

def driver_thread_eight(html_content, result, event):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        # <dl class="item_price"> 태그 검색
        price_tag = soup.find('dl', {'class': 'item_price'})
        if price_tag:
            # <strong> 태그에서 값을 가져옴
            strong_tag = price_tag.find('strong')
            if strong_tag:
                price = strong_tag.text
                # 콤마 제거 및 정수로 변환
                price = int(price.replace(',', '').strip())

        # optionSnoInput의 모든 옵션을 가져옴
        option_list = []  # 옵션 값을 저장할 리스트 초기화
        select = soup.find('select', {'name': 'optionSnoInput', 'class': 'chosen-select'})
        if select:
            options = select.find_all('option')
            for option in options:
                option_list.append(option.text)  # 옵션 값 리스트에 추가

        option_list[1] = option_list[1].replace('(기본수량)', ': -0원')

        filtered_texts = []
        for text in option_list:
            filtered_text = re.sub(r"[^0-9:\+\-]", "", text)
            filtered_texts.append(filtered_text)

        filtered_texts = [text for text in filtered_texts if text]

        # 빈 리스트를 생성하여 각 부분을 저장합니다.
        quantity_data = []
        price_data = []

        # 각 문자열을 ":"으로 분리하고, 각 부분을 적절한 리스트에 추가합니다.
        for text in filtered_texts:
            quantity, discount = text.split(":")
            quantity_data.append(int(quantity))
            price_data.append(int(discount))

        # 데이터프레임을 생성합니다.
        df_data = {
            '수량': quantity_data,
            '일반가': price_data
        }
        df = pd.DataFrame(df_data)

        # '일반가' 열의 모든 값에 price를 더합니다.
        df['일반가'] = df['일반가'] + price

        df_sorted = df.sort_values(by='수량', ascending=True)

        result['data'] = df_sorted  # 결과를 딕셔너리에 저장
        event.set()  # 결과가 준비되었음을 알림

    except Exception as e:
        print("스레드 8 오류가 발생했습니다:", e)
        event.set()  # 오류 발생 시에도 이벤트 설정

def driver_thread_nine(html_content, result, event):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        price_tag = soup.find('li', {'class': 'price'})

        strong_tag = price_tag.find_all('strong')[1]  # 첫 번째 <strong>은 '판매가'이므로 두 번째 <strong>을 선택
        if strong_tag:
            # 텍스트를 추출합니다.
            price = strong_tag.text.replace(',','')
        price = int(price.strip())

        # optionSnoInput의 모든 옵션을 가져옴
        option_list = []  # 옵션 값을 저장할 리스트 초기화
        select = soup.find('select', {'name': 'optionSnoInput', 'class': 'tune'})
        if select:
            options = select.find_all('option')
            for option in options:
                option_list.append(option.text)  # 옵션 값 리스트에 추가

        filtered_texts = []
        for text in option_list:
            filtered_text = re.sub(r"[^0-9:\+\-]", "", text)
            filtered_texts.append(filtered_text)

        filtered_texts = [text for text in filtered_texts if text]

        filtered_texts.pop(2)
        filtered_texts.pop(0)

        # 빈 리스트를 생성하여 각 부분을 저장합니다.
        quantity_data = []
        price_data = []

        # 각 문자열을 ":"으로 분리하고, 각 부분을 적절한 리스트에 추가합니다.
        for text in filtered_texts:
            quantity, discount = text.split(":")
            quantity_data.append(int(quantity))
            price_data.append(int(discount))

        # 데이터프레임을 생성합니다.
        df_data = {
            '수량': quantity_data,
            '일반가': price_data
        }
        df = pd.DataFrame(df_data)

        df['일반가'] = (df['일반가'] + price) / df['수량']

        df_sorted = df.sort_values(by='수량', ascending=True)

        result['data'] = df_sorted  # 결과를 딕셔너리에 저장
        event.set()  # 결과가 준비되었음을 알림

    except Exception as e:
        print("스레드 9 오류가 발생했습니다:", e)
        event.set()  # 오류 발생 시에도 이벤트 설정

def driver_thread_ten(html_content, result, event):
    try:

        def format_header_text(text):
            formatted_text = text
            if '천부' in formatted_text:
                formatted_text = re.sub('천부', '000', formatted_text)
            
            formatted_text = re.sub('[^0-9]', '', formatted_text)
            
            return formatted_text
        
        soup = BeautifulSoup(html_content, 'html.parser')

        header_list = []
        data_list = []

        target_table = soup.find('table', {'class': 't_basic gray'})

        if target_table:
            thead = target_table.find('thead')
            if thead:
                header_row = thead.find('tr')
                if header_row:
                    header_cols = header_row.find_all('th')
                    header_list = [format_header_text(col.text.strip()) for col in header_cols]
                    
            # Extract data rows
            tbody = target_table.find('tbody')
            if tbody:
                data_rows = tbody.find_all('tr')
                for row in data_rows:
                    data_cols = row.find_all('td')
                    data_list = [format_header_text(col.text.replace(',', '').strip()) for col in data_cols]

        header_list = [value for value in header_list if value != '']
        data_list = [value for value in data_list if value != '']

        df_data = {
            '수량': header_list,
            '일반가': data_list
        }
        df = pd.DataFrame(df_data)

        df['수량'] = df['수량'].astype(int)
        df['일반가'] = df['일반가'].astype(int)

        df_sorted = df.sort_values(by='수량', ascending=True)

        result['data'] = df_sorted  # 결과를 딕셔너리에 저장
        event.set()  # 결과가 준비되었음을 알림

    except Exception as e:
        print("스레드 10 오류가 발생했습니다:", e)
        event.set()  # 오류 발생 시에도 이벤트 설정

def driver_thread_eleven(html_content, result, event):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        # 해당 div 태그 내에서 table 태그를 찾기
        table_div = soup.find('div', class_='tablebox')
        table = table_div.find('table', {'cellspacing': '1', 'width': '100%', 'cellpadding': '0', 'border': '1'})

        # pandas를 이용하여 테이블을 DataFrame으로 변환
        df = pd.read_html(str(table))[0]

        # DataFrame 수정
        df.columns = df.iloc[0]  # 0번째 행을 컬럼명으로 사용
        df = df.drop(0)  # 0번째 행 제거

        # 컬럼명 변경
        df.columns = ['수량', '일반가']

        # ',' 제거 후 숫자로 변환
        df['수량'] = df['수량'].str.replace(',', '').str.extract('(\d+)').astype(int)  # 수량에서 숫자만 추출하여 정수로 변환
        df['일반가'] = df['일반가'].str.replace(',', '').str.extract('(\d+)').astype(int)  # 일반가에서 쉼표 제거 후 숫자만 추출하여 정수로 변환
        df['일반가'] = df['일반가'].astype(float).apply(lambda x: x * 1.1)
        
        df_sorted = df.sort_values(by='수량', ascending=True)

        result['data'] = df_sorted  # 결과를 딕셔너리에 저장
        event.set()  # 결과가 준비되었음을 알림

    except Exception as e:
        print("스레드 11 오류가 발생했습니다:", e)
        event.set()  # 오류 발생 시에도 이벤트 설정

def save_thread_result(thread_number, data):
    if "data" in data and not data["data"].empty:
        print(f"스레드 {thread_number} 데이터:")
        print(data["data"])
        data["data"].to_csv(f"C:\\RPA\\Image\\Target\\unit_price_list.csv", index=False)

def crawl_product_data(url: str):
    html_content = fetch_html_content(url)

    if html_content == 'url에러':
        return "URL 에러"

    results = [{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}]
    events = [threading.Event() for _ in range(11)]
    targets = [driver_thread_one, driver_thread_two, driver_thread_three, driver_thread_four, driver_thread_five, driver_thread_six, driver_thread_seven, driver_thread_eight, driver_thread_nine, driver_thread_ten, driver_thread_eleven]

    threads = [threading.Thread(target=targets[i], args=(html_content, results[i], events[i])) for i in range(11)]
    for t in threads:
        t.start()

    for e in events:
        e.wait()

    for idx, result in enumerate(results, 1):
        save_thread_result(idx, result)

    if not any(["data" in res for res in results]):  # 모든 결과 딕셔너리에서 'data' 키가 없는 경우
        print("스레드 모두 테이블 데이터를 가져오지 못했습니다.")


if __name__ == "__main__":
    a = crawl_product_data('https://cr.shopping.naver.com/adcr.nhn?x=h48dbJkNOhVES6sIRLbHhP%2F%2F%2Fw%3D%3Ds07gZ%2BDMOzBFj%2Fz5R4bzdzAHtQtD%2B%2BYeyNzsqiZ%2Fwt3lSb9I54VEktdd82UHurpvwIvZ6i9r3j%2BWAY2P0AbdEYTB%2BFN66Q4P4BitXvPttH7EiiJZt94KUP86oed18ERKOaraQwVw%2B5KN7DMoodxKp7UCjwV29C9xJnKLYNk0TujsOMXslOvJfHVJLp00o7xxIrONOFMCO6FzWtCn1qH8mAkPg%2F6PYJW9LpCSumzPJoigcWr1BVHDKRuZzTxzLzzc9ZzZOfJGGbM1UYHchI%2FMMJbzCd0lSdCESXEz4q3SKUyFFAXne8qRVhrqG7Dpe2NILscxzhL%2B1z58ukVEit8Jz6YwefsEME1qpc2agpsB5My9KQlsh3vaoP85nGlWorMk8B3%2FX5mkQ8x9IikynLQdC3AoEFUW9c66mGugH280C7QRDfx9xqmICRMmJ6cx%2FL2RANs02Ify9Jfp4d9D099jL%2BOo4gMKuypnGcV9yPfSFsMgqHVEaBjWga3OskBmXSIPwQdAiJdwXirBRdnaxfvrBbw%3D%3D&nvMid=41016206552&catId=50008588')
    print(a)