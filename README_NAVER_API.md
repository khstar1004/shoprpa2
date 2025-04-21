# Naver Shopping API Integration Guide

이 문서는 ShopRPA의 네이버 쇼핑 API 통합에 대한 설정 방법을 설명합니다.

## 설정 방법

### 1. 네이버 API 키 발급받기

1. [네이버 개발자 센터](https://developers.naver.com/)에 접속합니다.
2. 로그인 후 "Application > 애플리케이션 등록" 메뉴로 이동합니다.
3. "애플리케이션 등록" 버튼을 클릭합니다.
4. 다음 정보를 입력합니다:
   - 애플리케이션 이름: "ShopRPA" (또는 원하는 이름)
   - 사용 API: "검색" 선택
   - 비로그인 오픈 API 서비스 환경: "Web 서비스 URL" 입력 (사용할 웹서버 URL 또는 localhost)
   - 로그인 오픈 API 서비스 환경: 사용하지 않음
5. 등록 완료 후 발급된 "Client ID"와 "Client Secret"을 확인합니다.

### 2. 설정 파일 구성하기

1. 프로그램 폴더에 있는 `config.txt` 파일을 `C:\RPA\` 폴더로 복사합니다.
2. 텍스트 편집기로 `C:\RPA\config.txt` 파일을 열어 다음 항목을 수정합니다:

```
# Naver API Credentials
naver_client_id=YOUR_NAVER_CLIENT_ID_HERE     # 발급받은 Client ID로 변경
naver_client_secret=YOUR_NAVER_CLIENT_SECRET_HERE  # 발급받은 Client Secret으로 변경
```

### 3. API 호출 제한 및 주의사항

- 네이버 API는 일일 호출 제한이 있습니다 (무료 플랜: 25,000회/일).
- 호출량이 많은 경우 API 사용량을 모니터링하세요.
- 네이버 개발자 센터의 API 사용 정책을 준수해야 합니다.

## 작동 방식

새로운 네이버 쇼핑 API 통합은 다음과 같이 작동합니다:

1. 각 상품에 대해 네이버 쇼핑 API를 사용하여 검색을 수행합니다.
2. 검색 결과는 가격 오름차순으로 정렬됩니다.
3. 검색어 최적화를 위해 상품명에서 처음 3단어를 사용합니다.
4. 결과가 없을 경우 첫 단어와 마지막 단어를 조합하여 재검색합니다.
5. 최대 3페이지까지 검색하며 각 페이지당 30개 결과를 가져옵니다.
6. 판촉물 관련 키워드가 포함된 상품은 특별히 표시됩니다.
7. 원본 가격과 10% 미만 차이가 나는 상품은 필터링됩니다.
8. 결과는 이미지와 함께 처리되며 배경 제거 기능이 적용됩니다.

## 문제 해결

- API 오류가 발생하면 로그 파일을 확인하세요.
- 네이버 API 키가 올바르게 설정되었는지 확인하세요.
- 인터넷 연결이 안정적인지 확인하세요.
- API 호출 제한에 도달했다면 잠시 후에 다시 시도하세요.

## 코드 사용법

네이버 API를 프로그래밍 방식으로 사용하려면:

```python
from main_rpa import crawl_naver

# 상품 검색 (기본 설정)
results = crawl_naver("검색어")

# 상세 설정으로 검색
results = crawl_naver(
    query="검색어", 
    max_items=50,  # 최대 항목 수
    reference_price=10000  # 참조 가격 (10% 룰 적용)
)

# 결과 처리
for product in results:
    print(f"상품명: {product['name']}")
    print(f"가격: {product['price']}")
    print(f"판매처: {product['seller']}")
    print(f"링크: {product['link']}")
    print("---") 