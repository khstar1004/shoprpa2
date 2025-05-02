# Kogift Image Fix Solution

## 문제 설명 (Problem Description)
ShopRPA Excel 파일에서 고려기프트(Kogift) 이미지와 관련된 두 가지 주요 문제:
1. 일부 이미지가 링크만 표시되고 로컬에서 이미지를 불러오지 못함
2. 링크와 불러온 사진이 일치하지 않는 경우가 있음

## 해결 방안 (Solution)
이 솔루션은 다음과 같은 방법으로 문제를 해결합니다:

1. 이미지 매칭 알고리즘 개선:
   - 다양한 파일명 패턴 인식 (kogift_, shop_ 등)
   - URL과 로컬 파일 간의 매칭 개선
   - 해시 기반 이미지 식별 추가

2. 엑셀 출력 개선:
   - 이미지 크기 증가 (가시성 향상)
   - 하이퍼링크 보존 (원본 URL 참조 가능)
   - 행 높이 자동 조정

## 사용 방법 (Usage)

### 스크립트 실행 (Run Script)
이미 생성된 Excel 파일의 Kogift 이미지 문제를 수정하려면:

```bash
python fix_kogift_images.py --input [입력_엑셀파일] --output [출력_엑셀파일]
```

예시:
```bash
python fix_kogift_images.py --input C:\RPA\Output\result_20231201.xlsx --output C:\RPA\Output\result_20231201_fixed.xlsx
```

출력 파일을 지정하지 않으면 원본 파일 이름에 "_fixed"가 추가된 파일이 동일한 디렉토리에 생성됩니다.

### 이미지 통합 프로세스에 통합 (Integration)
데이터 처리 과정에서 Kogift 이미지 매칭을 개선하려면:

`image_integration.py`의 `integrate_and_filter_images` 함수가 자동으로 개선된 Kogift 이미지 매칭 프로세스를 포함합니다.

## 기술 상세 (Technical Details)

### 주요 기능 (Key Features)
- **확장된 이미지 검색**: 여러 디렉토리와 명명 패턴을 검색하여 최대한 많은 이미지 파일 식별
- **다중 매칭 알고리즘**: URL과 파일 이름 간의 다양한.매칭 방법 적용
  - 직접 파일명 매칭
  - 접두사/접미사 변형 처리 (kogift_, shop_)
  - 해시 기반 매칭
  - 유사도 기반 퍼지 매칭
- **Excel 통합**: 로컬 이미지 파일을 직접 Excel에 삽입하면서 원본 URL 하이퍼링크 보존

### 개선된 파일 (Modified Files)
- `image_integration.py`: 개선된 Kogift 이미지 매칭 통합
- `excel_utils.py`: 이미지 표시 크기 및 행 높이 조정
- `kogift_image_fix.py`: 포괄적인 Kogift 이미지 매칭 유틸리티
- `fix_kogift_images.py`: 독립 실행형 수정 도구

## 사용 예시 (Usage Examples)

### 명령줄 실행 (Command Line)
```bash
# 기본 실행 (출력 파일은 자동 생성)
python fix_kogift_images.py --input C:\RPA\Output\result.xlsx

# 출력 파일 지정
python fix_kogift_images.py --input C:\RPA\Output\result.xlsx --output C:\RPA\Output\fixed_result.xlsx
```

### 코드에서 사용 (In Code)
```python
from kogift_image_fix import fix_excel_kogift_images

# 엑셀 파일 수정
fixed_file = fix_excel_kogift_images(
    excel_file="C:/RPA/Output/result.xlsx",
    output_file="C:/RPA/Output/fixed_result.xlsx"
)

if fixed_file:
    print(f"성공: {fixed_file}에 수정된 파일 저장됨")
else:
    print("실패: 이미지 수정 중 오류 발생") 