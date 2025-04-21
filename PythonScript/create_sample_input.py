import os
import pandas as pd

# Path to input directory
INPUT_DIR = 'C:\\RPA\\Input'

# Make sure directory exists
os.makedirs(INPUT_DIR, exist_ok=True)

# Define the sample data
data = {
    '구분': ['A', 'A', 'A'],
    '담당자': ['김균아', '김균아', '김균아'],
    '업체명': ['세종기업(주)', '세종기업(주)', '세종기업(주)'],
    '업체코드': [517, 517, 517],
    'Code': [432849, 432811, 432787],
    '중분류카테고리': ['방향제/디퓨져', '방향제/디퓨져', '방향제/디퓨져'],
    '상품명': ['차량용디퓨저세트 NEO-2.', '센티드우드블럭 디퓨저세트(20ml) DWA-3202', '차량용 디퓨저세트 NEO-2'],
    '기본수량(1)': [30, 30, 50],
    '판매단가(V포함)': [15593, 15593, 15593],
    '본사상품링크': [
        'http://www.jclgift.com/product/product_view.asp?p_idx=432849',
        'http://www.jclgift.com/product/product_view.asp?p_idx=432811',
        'http://www.jclgift.com/product/product_view.asp?p_idx=432787'
    ]
}

# Create a DataFrame from the data
df = pd.DataFrame(data)

# Save to Excel
output_file = os.path.join(INPUT_DIR, 'test1.xlsx')
df.to_excel(output_file, index=False)

print(f"Sample input file created: {output_file}") 