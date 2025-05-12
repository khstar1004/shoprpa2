import os
import pandas as pd
from datetime import datetime
from excel_utils import excel_generator, finalize_dataframe_for_excel
from price_highlighter import apply_price_highlighting_to_files
from pathlib import Path
import tempfile
import shutil

def test_price_highlighting():
    """Test to ensure rows are highlighted when price difference is < -1"""
    print("Testing price difference highlighting...")
    
    # Test data
    test_data = {
        '구분': ['P', 'P', 'P', 'P'],
        '담당자': ['테스트', '테스트', '테스트', '테스트'],
        '업체명': ['업체A', '업체B', '업체C', '업체D'],
        '상품명': ['상품A', '상품B', '상품C', '상품D'],
        '판매단가(V포함)': [1000, 2000, 3000, 4000],
        '판매가(V포함)(2)': [900, 2100, 2900, 4500],  
        '가격차이(2)': [-100, 100, -100, 500],
        '가격차이(2)(%)': [-10, 5, -3.3, 12.5],
        '판매단가(V포함)(3)': [950, 1900, 3100, 3800],
        '가격차이(3)': [-50, -100, 100, -200],
        '가격차이(3)(%)': [-5, -5, 3.3, -5]
    }
    
    # Create DataFrame
    df = pd.DataFrame(test_data)
    
    # Create temp directory for test files
    temp_dir = tempfile.mkdtemp(prefix="price_highlight_test_")
    print(f"Created temp directory: {temp_dir}")
    
    try:
        # Test single file output
        single_output_path = os.path.join(temp_dir, 'excel_test_single.xlsx')
        print(f"Creating single output file: {single_output_path}")
        df_final = finalize_dataframe_for_excel(df)
        result_success, _, result_path, _ = excel_generator.create_excel_output(
            df=df_final,
            output_path=single_output_path,
            create_upload_file=False
        )
        
        # Test create_split_excel_outputs
        split_base_path = os.path.join(temp_dir, 'excel_test_split')
        print(f"Creating split output files: {split_base_path}")
        df_final = finalize_dataframe_for_excel(df)
        result_success, upload_success, result_file_path, upload_file_path = excel_generator.create_excel_output(
            df=df_final, 
            output_path=split_base_path,
            create_upload_file=True
        )
        
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
        print("- Row 2: 상품B (가격차이(2) = 100, 가격차이(2)(%) = 5)")
        print("- Row 3: 상품C (가격차이(2) = -100, 가격차이(2)(%) = -3.3)")
        print("- Row 4: 상품D (가격차이(2) = 500, 가격차이(2)(%) = 12.5)")
        print("- Row 5: 상품D (가격차이(3) = -200, 가격차이(3)(%) = -5)")
        
        return result_path, result_file_path, upload_file_path
    finally:
        # Clean up temp directory
        shutil.rmtree(temp_dir)

if __name__ == "__main__":
    test_price_highlighting() 