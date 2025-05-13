import os
import pandas as pd
from datetime import datetime
from excel_utils import create_final_output_excel, create_split_excel_outputs, finalize_dataframe_for_excel
from price_highlighter import apply_price_highlighting_to_files

def test_price_highlighting():
    """Test to ensure rows are highlighted when price difference is < -1"""
    print("Testing price difference highlighting...")
    
    # 엑셀 골든 예제와 유사한 테스트 데이터
    data = {
        '구분': ['A', 'A', 'A', 'A', 'A'],
        '담당자': ['김균아', '김균아', '김균아', '김균아', '김균아'],
        '업체명': ['엠제이(MJ)기획', '엠제이(MJ)기획', '엠제이(MJ)기획', '엠제이(MJ)기획', '엠제이(MJ)기획'],
        '업체코드': ['3941', '3941', '3941', '3941', '3941'],
        'Code': ['437611', '437593', '437583', '437570', '437551'],
        '중분류카테고리': ['가방(에코백/면)', '가방(에코백/면)', '가방(에코백/면)', '가방(에코백/면)', '어린이우산'],
        '상품명': ['사랑이 엔젤하트 투포켓 에코백', '사랑이 큐피트화살 투포켓 에코백', '행복이 스마일플라워 투포켓 에코백',
                 '행운이 네잎클로버 투포켓 에코백', '캐치티니핑 53 스무디 입체리본 투명 아동우산'],
        '기본수량(1)': [200, 200, 200, 200, 50],
        '판매단가(V포함)': [2970, 2970, 2970, 2970, 17820],
        '본사상품링크': ['http://www.jclgift.com/product/product_view.asp?p_idx=437611',
                     'http://www.jclgift.com/product/product_view.asp?p_idx=437593',
                     'http://www.jclgift.com/product/product_view.asp?p_idx=437583',
                     'http://www.jclgift.com/product/product_view.asp?p_idx=437570',
                     'http://www.jclgift.com/product/product_view.asp?p_idx=437551'],
        '기본수량(2)': ['-', 200, 200, 200, 50],
        '판매가(V포함)(2)': ['-', 570900, 570900, 570900, 842600],
        '판매단가(V포함)(2)': ['-', 2854.5, 2854.5, 2854.5, 16852],
        '가격차이(2)': ['-', -115.5, -115.5, -115.5, -968],  # 음수값
        '가격차이(2)(%)': ['-', -3.9, -3.9, -3.9, -5.4],  # 음수값
        '고려기프트 상품링크': ['가격 범위내에 없거나 텍스트 유사율을 가진 상품이 없음',
                         'http://koreagift.com/ez/mall.php?cat=003011001&query=view&no=170303',
                         'http://koreagift.com/ez/mall.php?cat=003011001&query=view&no=170300',
                         'http://koreagift.com/ez/mall.php?cat=003011001&query=view&no=170297',
                         'http://koreagift.com/ez/mall.php?cat=004002005&query=view&no=170277'],
        '기본수량(3)': ['-', '-', '-', '-', ''],
        '판매단가(V포함)(3)': ['-', '-', '-', '-', 14490],
        '가격차이(3)': ['-', '-', '-', '-', -3330],  # 음수값
        '가격차이(3)(%)': ['-', '-', '-', '-', -18.6],  # 음수값
        '공급사명': ['-', '-', '-', '-', '네이버'],
        '네이버 쇼핑 링크': ['가격이 범위내에 없거나 검색된 상품이 없음',
                       '일정 정확도 이상의 텍스트 유사율을 가진 상품이 없음',
                       '가격이 범위내에 없거나 검색된 상품이 없음',
                       '일정 정확도 이상의 텍스트 유사율을 가진 상품이 없음',
                       'https://search.shopping.naver.com/catalog/53165134501'],
        '공급사 상품링크': ['-', '-', '-', '-', '-'],
        '본사 이미지': ['-', 'http://i.jclgift.com/upload/product/bimg3/BBCH0009423b.png',
                    'http://i.jclgift.com/upload/product/bimg3/BBCH0009422b.png',
                    'http://i.jclgift.com/upload/product/bimg3/BBCH0009421b.png',
                    'http://i.jclgift.com/upload/product/bimg3/LLAG0003250b.jpg'],
        '고려기프트 이미지': ['-', 'http://koreagift.com/ez/upload/mall/shop_1744176711612699_0.png',
                        'http://koreagift.com/ez/upload/mall/shop_1744178375501354_0.png',
                        'http://koreagift.com/ez/upload/mall/shop_1744178312138728_0.png',
                        'http://koreagift.com/ez/upload/mall/shop_1744109588135407_0.jpg'],
        '네이버 이미지': ['-', '-', '-', '-', 'https://shopping-phinf.pstatic.net/main_5316513/53165134501.20250222203926.jpg'],
    }
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Create output directories if they don't exist
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_output')
    os.makedirs(output_dir, exist_ok=True)
    
    # Format current timestamp for filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create output paths
    result_path = os.path.join(output_dir, f'test_price_highlight_result_{timestamp}.xlsx')
    split_base_path = os.path.join(output_dir, f'test_price_highlight_split_{timestamp}')
    
    print(f"Creating test Excel files with price highlighting...")
    
    # Test create_final_output_excel
    print(f"Creating single output file: {result_path}")
    success = create_final_output_excel(df, result_path)
    if success:
        print(f"✓ Successfully created single output file.")
    else:
        print(f"✗ Failed to create single output file.")
    
    # Test create_split_excel_outputs
    print(f"Creating split output files: {split_base_path}")
    df_final = finalize_dataframe_for_excel(df)
    result_success, upload_success, result_file_path, upload_file_path = create_split_excel_outputs(df_final, split_base_path)
    
    if result_success:
        print(f"✓ Successfully created result file: {result_file_path}")
    else:
        print(f"✗ Failed to create result file.")
        
    if upload_success:
        print(f"✓ Successfully created upload file: {upload_file_path}")
    else:
        print(f"✗ Failed to create upload file.")
    
    # 가격차이 하이라이팅 테스트
    print("\n가격차이 하이라이팅 적용...")
    if result_success or upload_success:
        files_to_process = []
        if result_success and os.path.exists(result_file_path):
            files_to_process.append(("결과 파일", result_file_path))
        if upload_success and os.path.exists(upload_file_path):
            files_to_process.append(("업로드 파일", upload_file_path))
        
        if files_to_process:
            success_count, total_count = apply_price_highlighting_to_files(
                result_path=result_file_path if result_success else None,
                upload_path=upload_file_path if upload_success else None,
                threshold=-1
            )
            print(f"✓ 가격차이 하이라이팅 완료: {success_count}/{total_count} 파일 처리")
        else:
            print("✗ 하이라이팅할 파일이 없습니다.")
    else:
        print("✗ 하이라이팅을 적용할 파일이 생성되지 않았습니다.")
    
    print("\nTest completed. Please check the output files to verify highlighting of rows with negative price differences (< -1).")
    print("Expected highlights:")
    print("- Row 2: 사랑이 큐피트화살 투포켓 에코백 (가격차이(2) = -115.5, 가격차이(2)(%) = -3.9)")
    print("- Row 3: 행복이 스마일플라워 투포켓 에코백 (가격차이(2) = -115.5, 가격차이(2)(%) = -3.9)")
    print("- Row 4: 행운이 네잎클로버 투포켓 에코백 (가격차이(2) = -115.5, 가격차이(2)(%) = -3.9)")
    print("- Row 5: 캐치티니핑 53 스무디 입체리본 투명 아동우산 (가격차이(2) = -968, 가격차이(2)(%) = -5.4, 가격차이(3) = -3330, 가격차이(3)(%) = -18.6)")
    
    return result_path, result_file_path, upload_file_path

if __name__ == "__main__":
    test_price_highlighting() 