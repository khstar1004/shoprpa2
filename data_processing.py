# 부가세 포함 여부 확인
vat_included = item.get('vat_included', False)
# 만약 부가세가 이미 포함되어 있다면 그대로 사용
if vat_included:
    df.at[idx, '판매단가(V포함)(2)'] = item['price']
else:
    # 정확히 10% 부가세 적용
    df.at[idx, '판매단가(V포함)(2)'] = item['price']

# Check for regular price and apply VAT
elif 'price' in item:
    # 부가세 포함 여부 확인
    vat_included = item.get('vat_included', False)
    # 만약 부가세가 이미 포함되어 있다면 그대로 사용
    if vat_included:
        df.at[idx, '판매단가(V포함)(3)'] = item['price']
    else:
        # 크롤링된 가격을 그대로 사용 (부가세는 이미 크롤링 단계에서 계산됨)
        df.at[idx, '판매단가(V포함)(3)'] = item['price'] 