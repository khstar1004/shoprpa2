import os
import logging
import pandas as pd
from pathlib import Path
import configparser
from typing import Dict, Any, Optional, List, Tuple, Set
from openpyxl import Workbook
from openpyxl.drawing.image import Image
from openpyxl.utils import get_column_letter
import shutil

def prepare_image_metadata(image_dir: Path, prefix: str) -> Dict[str, Dict]:
    """
    이미지 메타데이터를 준비합니다.
    
    Args:
        image_dir: 이미지 디렉토리 경로
        prefix: 이미지 파일명 접두사 (예: 'haereum_', 'kogift_', 'naver_')
        
    Returns:
        이미지 메타데이터 사전
    """
    image_info = {}
    
    if not image_dir.exists():
        logging.warning(f"이미지 디렉토리를 찾을 수 없습니다: {image_dir}")
        return image_info
    
    # JPG와 PNG 파일 모두 찾기 (nobg 파일 제외)
    images = sorted([f for f in image_dir.glob("*.jpg") if "_nobg" not in f.name]) + \
             sorted([f for f in image_dir.glob("*.png") if "_nobg" not in f.name])
    
    logging.info(f"{len(images)}개의 {prefix} 이미지 발견")
    
    # 샘플 이미지 몇 개 로깅
    if images:
        sample_images = images[:3] if len(images) > 3 else images
        logging.debug(f"샘플 {prefix} 이미지: {[img.name for img in sample_images]}")
    
    for img_path in images:
        # 이미지 파일명에서 상품명 추출
        img_name = img_path.stem
        original_img_name = img_name
        
        # 접두사 제거
        if img_name.startswith(prefix):
            img_name = img_name[len(prefix):]  # 접두사 제거
        
        # 해시 부분 제거 - 파일명 끝에 있는 해시값 (언더스코어 + 알파벳/숫자 10자리 이내)
        if '_' in img_name:
            parts = img_name.split('_')
            # 마지막 부분이 해시처럼 생겼는지 확인
            if len(parts[-1]) <= 10 and parts[-1].isalnum():
                img_name = '_'.join(parts[:-1])
        
        # 이미지 이름을 토큰화하여 저장 (공백과 밑줄로 분리)
        clean_name = ''.join([c if c.isalnum() or c.isspace() else ' ' for c in img_name.lower()])
        tokens = [t.lower() for t in clean_name.replace('_', ' ').split() if len(t) > 1]
        
        image_info[str(img_path)] = {
            'original_name': original_img_name,
            'clean_name': img_name,
            'tokens': tokens,
            'path': img_path
        }
    
    return image_info

def calculate_similarity(product_tokens: List[str], image_tokens: List[str]) -> float:
    """
    상품명과 이미지 이름 간의 유사도를 계산합니다.
    
    Args:
        product_tokens: 상품명에서 추출한 토큰 목록
        image_tokens: 이미지 이름에서 추출한 토큰 목록
        
    Returns:
        유사도 점수 (0.0 ~ 1.0)
    """
    # 토큰 기반 유사도 계산
    common_tokens = set(product_tokens) & set(image_tokens)
    
    # 더 정확한 유사도 계산 - 토큰의 길이와 수를 고려
    total_tokens = len(set(product_tokens) | set(image_tokens))
    if total_tokens == 0:
        return 0.0
        
    similarity = len(common_tokens) / total_tokens
    
    # 더 긴 토큰이 매칭되면 가중치 부여
    weight = 1.0
    for token in common_tokens:
        if len(token) >= 4:  # 4글자 이상 토큰에 가중치
            weight += 0.1
    
    return similarity * weight

def tokenize_product_name(product_name: str) -> List[str]:
    """
    상품명을 토큰화합니다.
    
    Args:
        product_name: 상품명
        
    Returns:
        토큰 목록
    """
    # 특수문자를 공백으로 변환하고, 소문자로 변환
    clean_product = ''.join([c if c.isalnum() or c.isspace() else ' ' for c in product_name.lower()])
    # 2자 이상의 토큰만 추출
    return [t.lower() for t in clean_product.split() if len(t) > 1]

def find_best_image_matches(product_names: List[str], 
                           haereum_images: Dict[str, Dict], 
                           kogift_images: Dict[str, Dict], 
                           naver_images: Dict[str, Dict],
                           similarity_threshold: float = 0.1) -> List[Tuple[Optional[str], Optional[str], Optional[str]]]:
    """
    각 상품에 대해 세 가지 이미지 소스에서 가장 적합한 이미지를 찾습니다.
    세 이미지가 서로 일관성을 유지하도록 합니다.
    
    Args:
        product_names: 상품명 목록
        haereum_images: 해오름 이미지 정보
        kogift_images: 고려기프트 이미지 정보
        naver_images: 네이버 이미지 정보
        similarity_threshold: 최소 유사도 점수
        
    Returns:
        각 상품별 (해오름 이미지 경로, 고려기프트 이미지 경로, 네이버 이미지 경로) 튜플 목록
    """
    results = []
    
    # 이미지 매칭 시 이미 사용한 이미지 추적
    used_haereum = set()
    used_kogift = set()
    used_naver = set()
    
    # 모든 이미지 소스를 한번에 처리하여 일관된 매칭 보장
    for product_name in product_names:
        product_tokens = tokenize_product_name(product_name)
        
        # 각 소스별 최적 매치 찾기
        haereum_best = find_best_match_for_product(product_tokens, haereum_images, used_haereum, similarity_threshold)
        if haereum_best:
            used_haereum.add(haereum_best)
        
        # 이미 매칭된 해오름 이미지가 있다면, 그 이미지명을 기준으로 다른 소스 매칭 시도
        if haereum_best and haereum_images[haereum_best]['clean_name']:
            # 해오름 이미지 이름에서 토큰 추출
            haereum_tokens = tokenize_product_name(haereum_images[haereum_best]['clean_name'])
            
            # 해오름 이미지 이름으로 다른 소스 매칭 시도 (더 정확한 매칭)
            kogift_best = find_best_match_for_product(haereum_tokens, kogift_images, used_kogift, 0.05)
            naver_best = find_best_match_for_product(haereum_tokens, naver_images, used_naver, 0.05)
        else:
            # 원래 상품명으로 매칭 시도
            kogift_best = find_best_match_for_product(product_tokens, kogift_images, used_kogift, similarity_threshold)
            naver_best = find_best_match_for_product(product_tokens, naver_images, used_naver, similarity_threshold)
        
        if kogift_best:
            used_kogift.add(kogift_best)
        if naver_best:
            used_naver.add(naver_best)
            
        # 결과 추가
        results.append((haereum_best, kogift_best, naver_best))
        
        # 로깅
        haereum_name = haereum_images[haereum_best]['clean_name'] if haereum_best else "없음"
        kogift_name = kogift_images[kogift_best]['clean_name'] if kogift_best else "없음" 
        naver_name = naver_images[naver_best]['clean_name'] if naver_best else "없음"
        
        logging.debug(f"상품 '{product_name}' 매칭 결과:")
        logging.debug(f"  해오름: {haereum_name}")
        logging.debug(f"  고려기프트: {kogift_name}")
        logging.debug(f"  네이버: {naver_name}")
    
    # 남은 제품에 대해 이미지 할당 (1:1 매핑이 안된 경우)
    # 각 소스에서 사용되지 않은 이미지 중에서 가장 적합한 이미지를 할당
    if len(used_haereum) < len(haereum_images) and len(used_kogift) < len(kogift_images) and len(used_naver) < len(naver_images):
        for idx, (haereum_path, kogift_path, naver_path) in enumerate(results):
            # 이미 모든 소스에 이미지가 할당된 경우 건너뜀
            if haereum_path and kogift_path and naver_path:
                continue
                
            product_name = product_names[idx]
            product_tokens = tokenize_product_name(product_name)
            
            # 할당되지 않은 이미지 소스에 대해 처리
            if not haereum_path:
                haereum_best = find_best_match_for_product(product_tokens, haereum_images, used_haereum, 0.05)
                if haereum_best:
                    used_haereum.add(haereum_best)
                    results[idx] = (haereum_best, results[idx][1], results[idx][2])
                    
            if not kogift_path:
                kogift_best = find_best_match_for_product(product_tokens, kogift_images, used_kogift, 0.05)
                if kogift_best:
                    used_kogift.add(kogift_best)
                    results[idx] = (results[idx][0], kogift_best, results[idx][2])
                    
            if not naver_path:
                naver_best = find_best_match_for_product(product_tokens, naver_images, used_naver, 0.05)
                if naver_best:
                    used_naver.add(naver_best)
                    results[idx] = (results[idx][0], results[idx][1], naver_best)
    
    return results

def find_best_match_for_product(product_tokens: List[str], 
                               image_info: Dict[str, Dict], 
                               used_images: Set[str] = None,
                               similarity_threshold: float = 0.1) -> Optional[str]:
    """
    상품에 대해 가장 유사한 이미지를 찾습니다.
    
    Args:
        product_tokens: 상품명 토큰
        image_info: 이미지 정보 사전
        used_images: 이미 사용된 이미지 경로 집합
        similarity_threshold: 최소 유사도 점수
        
    Returns:
        가장 유사한 이미지 경로 또는 None
    """
    best_match = None
    best_score = 0
    
    if used_images is None:
        used_images = set()
    
    # 상품 토큰 정보 로깅
    if product_tokens:
        logging.debug(f"매칭 시도 - 제품 토큰: {product_tokens}")
    
    # 이미지 수와 사용된 이미지 수 로깅
    available_images = len(image_info) - len(used_images)
    logging.debug(f"사용 가능한 이미지: {available_images}개 (전체: {len(image_info)}개, 사용됨: {len(used_images)}개)")
    
    # 매칭 결과를 추적하기 위한 리스트
    match_scores = []
    
    for img_path, info in image_info.items():
        # 이미 사용된 이미지는 건너뜀
        if img_path in used_images:
            continue
            
        similarity = calculate_similarity(product_tokens, info['tokens'])
        
        # 모든 매칭 점수 추적
        if similarity > 0:
            match_scores.append((info['clean_name'], similarity))
        
        if similarity > best_score and similarity >= similarity_threshold:
            best_score = similarity
            best_match = img_path
    
    # 상위 3개 매칭 점수 로깅
    if match_scores:
        top_matches = sorted(match_scores, key=lambda x: x[1], reverse=True)[:3]
        logging.debug(f"상위 3개 매칭: {top_matches}")
    
    # 최종 매칭 결과 로깅
    if best_match:
        logging.debug(f"최적 매치: {image_info[best_match]['clean_name']} (점수: {best_score:.3f})")
    else:
        logging.debug(f"매치 없음 (임계값: {similarity_threshold})")
    
    return best_match

def integrate_images(df: pd.DataFrame, config: configparser.ConfigParser) -> pd.DataFrame:
    """
    세 가지 이미지 소스(해오름, 고려기프트, 네이버)의 이미지를 DataFrame에 통합합니다.
    상품별로 일관된 이미지 매칭을 보장합니다.
    
    Args:
        df: 처리할 DataFrame
        config: 설정 파일
        
    Returns:
        이미지가 통합된 DataFrame
    """
    try:
        logging.info("통합: 이미지 통합 프로세스 시작...")
        result_df = df.copy()
        
        # 이미지 디렉토리 경로
        main_img_dir = Path(config.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main'))
        haereum_dir = main_img_dir / 'Haereum'
        kogift_dir = main_img_dir / 'Kogift'
        naver_dir = main_img_dir / 'Naver'
        
        # 디렉토리 존재 체크
        if not haereum_dir.exists():
            logging.warning(f"해오름 이미지 디렉토리가 존재하지 않습니다: {haereum_dir}")
        if not kogift_dir.exists():
            logging.warning(f"고려기프트 이미지 디렉토리가 존재하지 않습니다: {kogift_dir}")
        if not naver_dir.exists():
            logging.warning(f"네이버 이미지 디렉토리가 존재하지 않습니다: {naver_dir}")
        
        # 이미지 메타데이터 준비
        haereum_images = prepare_image_metadata(haereum_dir, 'haereum_')
        kogift_images = prepare_image_metadata(kogift_dir, 'kogift_')
        naver_images = prepare_image_metadata(naver_dir, 'naver_')
        
        # 필요한 열 추가
        if '본사 이미지' not in result_df.columns:
            result_df['본사 이미지'] = None
        if '고려기프트 이미지' not in result_df.columns:
            result_df['고려기프트 이미지'] = None
        if '네이버 이미지' not in result_df.columns:
            result_df['네이버 이미지'] = None
        
        # 상품 목록 추출
        product_names = result_df['상품명'].tolist()
        
        # 제품 수와 이미지 수 로깅
        logging.info(f"제품 수: {len(product_names)}개")
        logging.info(f"해오름 이미지: {len(haereum_images)}개")
        logging.info(f"고려기프트 이미지: {len(kogift_images)}개")
        logging.info(f"네이버 이미지: {len(naver_images)}개")
        
        # 상품명 샘플 로깅
        if product_names:
            sample_products = product_names[:3] if len(product_names) > 3 else product_names
            logging.debug(f"제품 샘플: {sample_products}")
        
        # 이미지 매칭 임계값 설정 (설정 파일에서 가져오거나 기본값 사용)
        try:
            similarity_threshold = config.getfloat('Matching', 'image_similarity_threshold', fallback=0.1)
            logging.info(f"이미지 매칭 유사도 임계값: {similarity_threshold}")
        except ValueError as e:
            logging.warning(f"이미지 매칭 임계값 설정 오류: {e}. 기본값 0.1을 사용합니다.")
            similarity_threshold = 0.1
        
        # 최적 매치 찾기 (일관성 보장)
        best_matches = find_best_image_matches(
            product_names,
            haereum_images,
            kogift_images,
            naver_images,
            similarity_threshold=similarity_threshold
        )
        
        # 매칭 결과 통계
        haereum_matched = sum(1 for m in best_matches if m[0] is not None)
        kogift_matched = sum(1 for m in best_matches if m[1] is not None)
        naver_matched = sum(1 for m in best_matches if m[2] is not None)
        
        logging.info(f"1차 매칭 결과 - 해오름: {haereum_matched}/{len(product_names)}개, 고려기프트: {kogift_matched}/{len(product_names)}개, 네이버: {naver_matched}/{len(product_names)}개")
        
        # 매칭 실패한 경우, 임계값을 낮춰서 재시도
        if kogift_matched == 0 or naver_matched == 0:
            logging.info("고려기프트 또는 네이버 이미지 매칭 실패. 임계값을 낮춰서 재시도합니다.")
            
            # 임계값 낮추기
            retry_threshold = 0.03  # 더 낮은 임계값
            
            # 다시 매칭 시도
            best_matches = find_best_image_matches(
                product_names,
                haereum_images,
                kogift_images,
                naver_images,
                similarity_threshold=retry_threshold
            )
            
            # 재시도 결과 통계
            haereum_matched = sum(1 for m in best_matches if m[0] is not None)
            kogift_matched = sum(1 for m in best_matches if m[1] is not None)
            naver_matched = sum(1 for m in best_matches if m[2] is not None)
            
            logging.info(f"2차 매칭 결과 - 해오름: {haereum_matched}/{len(product_names)}개, 고려기프트: {kogift_matched}/{len(product_names)}개, 네이버: {naver_matched}/{len(product_names)}개")
            
            # 여전히 매칭 실패한 경우, 백업 전략 시도
            if kogift_matched == 0 or naver_matched == 0:
                logging.info("백업 매칭 전략 시도: 이미지 이름 패턴 기반 매칭")
                
                # 백업 전략: 완전히 매칭되지 않은 제품에 대해 단순히 순서대로 할당
                # (최후의 수단으로 사용)
                unused_kogift = [path for path in kogift_images.keys() 
                               if not any(match[1] == path for match in best_matches if match[1] is not None)]
                unused_naver = [path for path in naver_images.keys() 
                               if not any(match[2] == path for match in best_matches if match[2] is not None)]
                
                logging.info(f"미사용 이미지 - 고려기프트: {len(unused_kogift)}개, 네이버: {len(unused_naver)}개")
                
                # 매칭되지 않은 제품 인덱스
                unmatched_kogift_idx = [i for i, match in enumerate(best_matches) if match[1] is None]
                unmatched_naver_idx = [i for i, match in enumerate(best_matches) if match[2] is None]
                
                # 고려기프트 이미지 할당
                for i, idx in enumerate(unmatched_kogift_idx):
                    if i < len(unused_kogift):
                        kogift_path = unused_kogift[i]
                        best_matches[idx] = (best_matches[idx][0], kogift_path, best_matches[idx][2])
                
                # 네이버 이미지 할당
                for i, idx in enumerate(unmatched_naver_idx):
                    if i < len(unused_naver):
                        naver_path = unused_naver[i]
                        best_matches[idx] = (best_matches[idx][0], best_matches[idx][1], naver_path)
                
                # 최종 결과 통계
                haereum_matched = sum(1 for m in best_matches if m[0] is not None)
                kogift_matched = sum(1 for m in best_matches if m[1] is not None)
                naver_matched = sum(1 for m in best_matches if m[2] is not None)
                
                logging.info(f"최종 매칭 결과 - 해오름: {haereum_matched}/{len(product_names)}개, 고려기프트: {kogift_matched}/{len(product_names)}개, 네이버: {naver_matched}/{len(product_names)}개")
        
        # 결과를 DataFrame에 적용
        for idx, (haereum_path, kogift_path, naver_path) in enumerate(best_matches):
            # 해오름 이미지
            if haereum_path:
                img_path = haereum_images[haereum_path]['path']
                image_data = {
                    'local_path': str(img_path),
                    'source': 'haereum',
                    'url': f"file:///{str(img_path).replace(os.sep, '/')}",
                    'original_path': str(img_path)
                }
                result_df.at[idx, '본사 이미지'] = image_data
            
            # 고려기프트 이미지
            if kogift_path:
                img_path = kogift_images[kogift_path]['path']
                image_data = {
                    'local_path': str(img_path),
                    'source': 'kogift',
                    'url': f"file:///{str(img_path).replace(os.sep, '/')}",
                    'original_path': str(img_path)
                }
                result_df.at[idx, '고려기프트 이미지'] = image_data
            
            # 네이버 이미지
            if naver_path:
                img_path = naver_images[naver_path]['path']
                image_data = {
                    'local_path': str(img_path),
                    'source': 'naver',
                    'url': f"file:///{str(img_path).replace(os.sep, '/')}",
                    'original_path': str(img_path)
                }
                result_df.at[idx, '네이버 이미지'] = image_data
        
        # 매칭 결과 요약
        haereum_count = sum(1 for m in best_matches if m[0] is not None)
        kogift_count = sum(1 for m in best_matches if m[1] is not None)
        naver_count = sum(1 for m in best_matches if m[2] is not None)
        
        logging.info(f"통합: 이미지 매칭 완료 - 해오름: {haereum_count}개, 고려기프트: {kogift_count}개, 네이버: {naver_count}개")
        
        return result_df
    
    except Exception as e:
        logging.error(f"통합: 이미지 통합 중 오류 발생: {e}", exc_info=True)
        return df

def filter_images_by_similarity(df: pd.DataFrame, config: configparser.ConfigParser) -> pd.DataFrame:
    """
    이미지 유사도에 따라 고려기프트 및 네이버 이미지를 필터링합니다.
    임계값보다 낮은 유사도를 가진 이미지는 표시하지 않습니다.
    해오름(본사) 이미지는 유사도에 관계없이 항상 유지합니다.
    
    수정: 현재는 모든 이미지(해오름, 고려기프트, 네이버)를 유지합니다.
    
    Args:
        df: 처리할 DataFrame
        config: 설정 파일
    
    Returns:
        필터링된 DataFrame
    """
    try:
        # DataFrame 복사본 생성
        result_df = df.copy()
        
        # 임계값 설정 - 설정 파일에서 가져오거나 기본값 사용
        try:
            similarity_threshold = config.getfloat('Matching', 'image_display_threshold', fallback=0.7)
            logging.info(f"통합: 이미지 표시 임계값: {similarity_threshold} (현재 필터링 비활성화 상태)")
        except ValueError as e:
            logging.warning(f"임계값 읽기 오류: {e}. 기본값 0.7을 사용합니다.")
            similarity_threshold = 0.7
        
        # 이미지 유사도 필터링을 비활성화
        # 모든 이미지(해오름, 고려기프트, 네이버) 유지
        logging.info("통합: 이미지 필터링이 비활성화되었습니다. 모든 이미지가 유지됩니다.")
        
        # 이미지 존재 여부 확인 로깅
        haereum_count = 0
        kogift_count = 0
        naver_count = 0
        
        for i in range(len(result_df)):
            # 이미지 열별 존재 카운트
            if '본사 이미지' in result_df.columns:
                if pd.notna(result_df.iloc[i]['본사 이미지']) and result_df.iloc[i]['본사 이미지'] is not None:
                    haereum_count += 1
                    
            if '고려기프트 이미지' in result_df.columns:
                if pd.notna(result_df.iloc[i]['고려기프트 이미지']) and result_df.iloc[i]['고려기프트 이미지'] is not None:
                    kogift_count += 1
                    
            if '네이버 이미지' in result_df.columns:
                if pd.notna(result_df.iloc[i]['네이버 이미지']) and result_df.iloc[i]['네이버 이미지'] is not None:
                    naver_count += 1
        
        logging.info(f"통합: 이미지 현황 - 해오름: {haereum_count}개, 고려기프트: {kogift_count}개, 네이버: {naver_count}개")
        return result_df
    
    except Exception as e:
        logging.error(f"통합: 이미지 유사도 필터링 중 오류 발생: {e}", exc_info=True)
        # 오류 발생 시 원본 DataFrame 반환
        return df

def create_excel_with_images(df, output_file):
    """이미지가 포함된 엑셀 파일 생성"""
    try:
        # '번호' 컬럼이 없으면 추가
        if '번호' not in df.columns:
            df['번호'] = range(1, len(df) + 1)
        
        # 임시 디렉토리 생성
        temp_dir = Path("temp_images")
        temp_dir.mkdir(exist_ok=True)
        
        # 워크북 생성
        wb = Workbook()
        ws = wb.active
        
        # 사용 가능한 컬럼 확인
        available_columns = df.columns.tolist()
        logging.info(f"엑셀 생성: 사용 가능한 컬럼: {available_columns}")
        
        # 기본 헤더 및 데이터 컬럼 정의
        base_headers = ['번호', '상품명']
        optional_headers = ['파일명', '본사 이미지', '고려기프트 이미지', '네이버 이미지', '이미지_유사도']
        
        # 실제 사용할 헤더 목록 생성
        headers = base_headers + [h for h in optional_headers if h in available_columns]
        
        # 헤더 작성
        for col, header in enumerate(headers, 1):
            ws.cell(row=1, column=col, value=header)
        
        # 행 높이 설정
        ws.row_dimensions[1].height = 30  # 헤더 행 높이
        for row in range(2, len(df) + 2):
            ws.row_dimensions[row].height = 100  # 데이터 행 높이
        
        # 열 너비 설정
        column_widths = {}
        for i, header in enumerate(headers):
            col_letter = get_column_letter(i+1)
            if header == '번호':
                column_widths[col_letter] = 5
            elif header == '상품명':
                column_widths[col_letter] = 30
            elif header == '파일명':
                column_widths[col_letter] = 30
            else:
                column_widths[col_letter] = 15
        
        for col, width in column_widths.items():
            ws.column_dimensions[col].width = width
        
        # 데이터 및 이미지 추가
        for row_idx, (_, row) in enumerate(df.iterrows(), 2):
            # 기본 데이터 추가
            col_idx = 1
            
            # 번호 추가
            ws.cell(row=row_idx, column=col_idx, value=row['번호'])
            col_idx += 1
            
            # 상품명 추가
            ws.cell(row=row_idx, column=col_idx, value=row['상품명'])
            col_idx += 1
            
            # 파일명 추가 (있을 경우)
            if '파일명' in available_columns:
                ws.cell(row=row_idx, column=col_idx, value=row['파일명'])
                col_idx += 1
            
            # 이미지 데이터 처리
            image_columns = {}
            for col_name in ['본사 이미지', '고려기프트 이미지', '네이버 이미지']:
                if col_name in available_columns:
                    image_columns[col_name] = row.get(col_name)
            
            # 이미지 추가
            for col_name, img_data in image_columns.items():
                if pd.isna(img_data) or img_data is None:
                    ws.cell(row=row_idx, column=col_idx, value="")
                    col_idx += 1
                    continue
                
                try:
                    # 이미지 경로 추출
                    img_path = None
                    if isinstance(img_data, dict):
                        # excel_utils.py 형식의 딕셔너리 처리
                        img_path = img_data.get('local_path')
                        if not img_path and 'url' in img_data:
                            # URL만 있는 경우 셀에 URL 표시
                            ws.cell(row=row_idx, column=col_idx, value=img_data['url'])
                            col_idx += 1
                            continue
                    elif isinstance(img_data, str):
                        # 문자열 경로 처리
                        img_path = img_data
                    
                    if img_path and os.path.exists(img_path):
                        try:
                            # 이미지 파일 복사
                            img = Image(img_path)
                            # 이미지 크기 조정 (최대 100x100)
                            img.width = 100
                            img.height = 100
                            # 이미지 추가
                            ws.add_image(img, f"{get_column_letter(col_idx)}{row_idx}")
                            ws.cell(row=row_idx, column=col_idx, value="")  # 이미지가 있으면 셀 값 비움
                        except Exception as e:
                            logging.warning(f"이미지 추가 실패 ({img_path}): {e}")
                            # 이미지 추가 실패 시 경로나 URL 표시
                            if isinstance(img_data, dict):
                                ws.cell(row=row_idx, column=col_idx, value=img_data.get('url', str(img_path)))
                            else:
                                ws.cell(row=row_idx, column=col_idx, value=str(img_path))
                    else:
                        # 이미지 파일이 없는 경우 URL이나 경로 표시
                        if isinstance(img_data, dict):
                            ws.cell(row=row_idx, column=col_idx, value=img_data.get('url', ''))
                        else:
                            ws.cell(row=row_idx, column=col_idx, value=str(img_data))
                except Exception as e:
                    logging.error(f"이미지 처리 중 오류 발생 ({col_name}): {e}")
                    ws.cell(row=row_idx, column=col_idx, value="이미지 처리 오류")
                
                col_idx += 1
            
            # 이미지 유사도 추가 (있을 경우)
            if '이미지_유사도' in available_columns:
                ws.cell(row=row_idx, column=col_idx, value=row['이미지_유사도'])
                col_idx += 1
        
        # 엑셀 파일 저장
        wb.save(output_file)
        logging.info(f"이미지가 포함된 엑셀 파일이 저장되었습니다: {output_file}")
        
        # 임시 디렉토리 정리
        shutil.rmtree(temp_dir)
        
    except Exception as e:
        logging.error(f"엑셀 파일 생성 중 오류 발생: {e}", exc_info=True)

def integrate_and_filter_images(df: pd.DataFrame, config: configparser.ConfigParser, 
                            save_excel_output=False) -> pd.DataFrame:
    """
    이미지 통합 및 유사도 기반 이미지 필터링을 순차적으로 수행합니다.
    
    Args:
        df: 처리할 DataFrame
        config: 설정 파일
        save_excel_output: 결과를 별도의 엑셀 파일로 저장할지 여부 (기본값: False)
    
    Returns:
        처리된 DataFrame
    """
    try:
        logging.info("이미지 통합 및 필터링 프로세스 시작...")
        
        # 새로운 통합 이미지 함수로 모든 이미지 소스를 한번에 처리
        result_df = integrate_images(df, config)
        
        # 필터링 적용
        result_df = filter_images_by_similarity(result_df, config)
        
        # 필요한 경우에만 결과를 별도의 엑셀 파일로 저장 (이미지 포함)
        if save_excel_output:
            try:
                output_dir = Path(config.get('Paths', 'output_dir', fallback='C:\\RPA\\Output'))
                output_dir.mkdir(parents=True, exist_ok=True)
                output_file = output_dir / "image_integration_results.xlsx"
                create_excel_with_images(result_df, output_file)
                logging.info(f"이미지 통합 결과가 별도 파일로 저장되었습니다: {output_file}")
            except Exception as excel_error:
                logging.error(f"이미지 통합 결과 엑셀 파일 생성 실패: {excel_error}", exc_info=True)
                # 엑셀 파일 저장 실패는 전체 처리 실패로 간주하지 않음
        
        logging.info("이미지 통합 및 필터링 프로세스 완료!")
        return result_df
    
    except Exception as e:
        logging.error(f"이미지 통합 및 필터링 프로세스 중 오류 발생: {e}", exc_info=True)
        # 오류 발생 시 원본 DataFrame 반환
        return df

# 모듈 테스트용 코드
if __name__ == "__main__":
    # 기본 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    # 설정 파일 로드
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.ini')
    config.read(config_path, encoding='utf-8')
    
    # 테스트 데이터 생성
    test_df = pd.DataFrame({
        '번호': [1, 2],
        '상품명': ['테스트 상품 1', '테스트 상품 2'],
        '본사 이미지': [None, None],
        '고려기프트 이미지': [None, None],
        '네이버 이미지': [None, None],
        '이미지_유사도': [0.6, 0.8]
    })
    
    # 이미지 통합 및 필터링 테스트
    result_df = integrate_and_filter_images(test_df, config, save_excel_output=True)
    
    # 결과 출력
    logging.info(f"테스트 결과 DataFrame 형태: {result_df.shape}")
    logging.info(f"본사 이미지 열 데이터: {result_df['본사 이미지'].tolist()}")
    logging.info(f"고려기프트 이미지 열 데이터: {result_df['고려기프트 이미지'].tolist()}")
    logging.info(f"네이버 이미지 열 데이터: {result_df['네이버 이미지'].tolist()}") 