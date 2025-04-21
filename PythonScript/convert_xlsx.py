import pandas as pd
import re
import os

INPUT_DIRECTORY = 'C:\\RPA\\Input'
PATTERN = r'(\d{4}_[A-Z]\.)|(\d+\+\d+)|[^a-zA-Z0-9가-힣\s]|\s+'

def get_first_xls_file(directory):
    xls_files = [f for f in os.listdir(directory) if f.lower().endswith('.xls')]
    if not xls_files:
        print("xls 파일이 없습니다.")
        return None
    return xls_files[0]  # 파일의 전체 경로 대신 파일명만 반환

def preprocess_product_name(product_name):
    product_name = re.sub(PATTERN, ' ', product_name)
    product_name = product_name.replace('정품', '').replace('NEW', '').replace('특가', '').replace('주문제작타올', '').replace('주문제작수건', '').replace('결혼답례품 수건', '').replace('답례품수건', '').replace('주문제작 수건', '').replace('돌답례품수건', '').replace('명절선물세트', '').replace('각종행사수건','').strip()
    product_name = re.sub(' +', ' ', product_name)
    return product_name

def process_excel_file():
    file_name = get_first_xls_file(INPUT_DIRECTORY)
    if not file_name:
        return

    file_path = os.path.join(INPUT_DIRECTORY, file_name)
    tables = pd.read_html(file_path, encoding='cp949')
    df = tables[0]

    df.columns = df.iloc[0].str.strip()
    df = df.drop(0)

    df['상품명'] = df['상품명'].apply(lambda x: x.split("//")[0])
    df['상품명'] = df['상품명'].apply(preprocess_product_name)

    df['본사 이미지'] = ''
    df['고려기프트 이미지'] = ''  
    df['네이버 이미지'] = ''

    # 원본 .xls 파일명을 .xlsx 확장자로 변경하여 출력 파일 경로 설정
    output_file_name = file_name.replace('.xls', '.xlsx')
    output_file_path = os.path.join(INPUT_DIRECTORY, output_file_name)
    
    df.to_excel(output_file_path, index=False)
    print(f"데이터가 {output_file_path}에 저장되었습니다.")