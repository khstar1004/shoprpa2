import openpyxl

def clear_userconfig_data():
    # 엑셀 파일 경로
    filepath="C:\\RPA\\UserConfig.xlsx"

    # 엑셀 파일 열기
    wb = openpyxl.load_workbook(filepath)
    ws = wb.active

    # A2:C2 범위의 값을 지우기
    for col in ['A', 'B', 'C']:
        ws[f'{col}2'].value = None

    wb.save(filepath)