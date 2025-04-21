import cv2
import numpy as np
import os
import configparser
import logging
from typing import List, Tuple, Dict, Optional, Union
import time
from PIL import Image
import hashlib

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def sift_and_akaze_image_matching(main_folder_path=None, target_folder_path=None):
    """
    SIFT와 AKAZE 알고리즘을 결합한 향상된 이미지 매칭 함수
    
    Args:
        main_folder_path: 메인 이미지 폴더 경로
        target_folder_path: 대상 이미지 폴더 경로
        
    Returns:
        매칭된 이미지 파일명을 쉼표로 구분한 문자열
    """
    # 설정 파일 로드
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.ini')
    config.read(config_path, encoding='utf-8')
    
    # 이미지 폴더 경로 설정
    if target_folder_path is None:
        target_folder_path = config.get('Matching', 'images_dir', fallback='C:\\RPA\\Image\\Target')
    
    if main_folder_path is None:
        main_folder_path = config.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main')
    
    # 알고리즘 파라미터 설정 (설정 파일에서 로드 또는 기본값 사용)
    try:
        # SIFT 파라미터
        sift_ratio_threshold = config.getfloat('ImageMatching', 'sift_ratio_threshold', fallback=0.75)
        
        # AKAZE 파라미터
        akaze_distance_threshold = config.getint('ImageMatching', 'akaze_distance_threshold', fallback=50)
        
        # 공통 파라미터
        feature_match_threshold = config.getint('ImageMatching', 'feature_match_threshold', fallback=10)
        ransac_reproj_threshold = config.getfloat('ImageMatching', 'ransac_reproj_threshold', fallback=5.0)
        
        # 배경 제거 설정
        use_background_removal = config.getboolean('ImageMatching', 'use_background_removal_before_matching', fallback=False)
        
        # 캐시 설정
        cache_features = config.getboolean('ImageMatching', 'cache_extracted_features', fallback=True)
        cache_dir = config.get('ImageMatching', 'feature_cache_dir', fallback='C:\\RPA\\Temp\\feature_cache')
        if cache_features and not os.path.exists(cache_dir):
            os.makedirs(cache_dir, exist_ok=True)
    except Exception as e:
        logger.error(f"설정 로드 오류: {e}")
        # 기본값 설정
        sift_ratio_threshold = 0.75
        akaze_distance_threshold = 50
        feature_match_threshold = 10
        ransac_reproj_threshold = 5.0
        use_background_removal = False
        cache_features = False
        cache_dir = 'C:\\RPA\\Temp\\feature_cache'
    
    # 로깅 설정
    logger.info(f"SIFT/AKAZE 매칭 파라미터:")
    logger.info(f"  - SIFT 비율 임계값: {sift_ratio_threshold}")
    logger.info(f"  - AKAZE 거리 임계값: {akaze_distance_threshold}")
    logger.info(f"  - 특징점 매칭 임계값: {feature_match_threshold}")
    logger.info(f"  - RANSAC 재투영 임계값: {ransac_reproj_threshold}")
    logger.info(f"  - 배경 제거 사용: {use_background_removal}")
    logger.info(f"  - 특징점 캐싱 사용: {cache_features}")
    logger.info(f"대상 폴더 경로: {target_folder_path}")
    logger.info(f"메인 폴더 경로: {main_folder_path}")
    
    # 폴더 존재 확인
    if not os.path.exists(target_folder_path):
        logger.warning(f"대상 폴더가 존재하지 않습니다: {target_folder_path}")
        os.makedirs(target_folder_path, exist_ok=True)
        
    if not os.path.exists(main_folder_path):
        logger.warning(f"메인 폴더가 존재하지 않습니다: {main_folder_path}")
        os.makedirs(main_folder_path, exist_ok=True)
    
    # 배경 제거 함수 (선택적 사용)
    def remove_background(img_path: str) -> np.ndarray:
        """
        이미지에서 배경을 제거하고 전처리된 이미지 반환
        
        Args:
            img_path: 이미지 경로
            
        Returns:
            배경이 제거된 이미지 또는 원본 이미지
        """
        if not use_background_removal:
            return cv2.imread(img_path, cv2.IMREAD_GRAYSCALE)
            
        try:
            # rembg 라이브러리 사용 (필요시 설치)
            from rembg import remove, new_session
            
            # 출력 경로 생성
            cache_key = hashlib.md5(os.path.abspath(img_path).encode()).hexdigest()
            cache_path = os.path.join(cache_dir, f"nobg_{cache_key}.png")
            
            # 캐시된 결과가 있으면 사용
            if os.path.exists(cache_path):
                img = cv2.imread(cache_path, cv2.IMREAD_GRAYSCALE)
                if img is not None:
                    return img
            
            # 이미지 처리
            with open(img_path, 'rb') as f:
                img_data = f.read()
                
            # 세션 생성 및 배경 제거
            session = new_session("u2net")
            output_data = remove(img_data, session=session)
            
            # 결과 저장
            with open(cache_path, 'wb') as f:
                f.write(output_data)
                
            # 저장된 이미지 로드
            return cv2.imread(cache_path, cv2.IMREAD_GRAYSCALE)
        except Exception as e:
            logger.warning(f"배경 제거 오류: {e}")
            # 실패 시 원본 이미지 사용
            return cv2.imread(img_path, cv2.IMREAD_GRAYSCALE)
            
    # 특징점 캐시 클래스
    class FeatureCache:
        def __init__(self, cache_dir: str):
            self.cache_dir = cache_dir
            os.makedirs(self.cache_dir, exist_ok=True)
            
        def get_cache_path(self, img_path: str, feature_type: str) -> str:
            """캐시 파일 경로 생성"""
            img_hash = hashlib.md5(os.path.abspath(img_path).encode()).hexdigest()
            return os.path.join(self.cache_dir, f"{img_hash}_{feature_type}.npy")
            
        def get(self, img_path: str, feature_type: str) -> Optional[Tuple]:
            """캐시에서 특징점 가져오기"""
            if not cache_features:
                return None
                
            cache_path = self.get_cache_path(img_path, feature_type)
            if os.path.exists(cache_path):
                try:
                    return np.load(cache_path, allow_pickle=True)
                except Exception as e:
                    logger.warning(f"캐시 로드 오류: {e}")
            return None
            
        def put(self, img_path: str, feature_type: str, features: Tuple) -> None:
            """특징점 캐싱"""
            if not cache_features:
                return
                
            cache_path = self.get_cache_path(img_path, feature_type)
            try:
                np.save(cache_path, features)
            except Exception as e:
                logger.warning(f"캐시 저장 오류: {e}")
                
    # 특징점 캐시 초기화
    feature_cache = FeatureCache(cache_dir)
    
    # SIFT 객체 생성 (파라미터 최적화)
    sift = cv2.SIFT_create(
        nfeatures=0,  # 0: 모든 특징점 유지
        nOctaveLayers=3,  # 기본값
        contrastThreshold=0.04,  # 낮은 값: 더 많은 특징점
        edgeThreshold=10,  # 엣지 필터링
        sigma=1.6  # 가우시안 필터 시그마
    )
    
    # AKAZE descriptor 초기화 (파라미터 최적화)
    akaze = cv2.AKAZE_create(
        descriptor_type=cv2.AKAZE_DESCRIPTOR_MLDB,  # 기본 타입
        descriptor_size=0,  # 기본값
        descriptor_channels=3,  # 채널 수
        threshold=0.001,  # 낮은 값: 더 많은 특징점
        nOctaves=4,  # 옥타브 수
        nOctaveLayers=4,  # 옥타브 레이어 수
    )

    # 매칭 객체 초기화
    FLANN_INDEX_KDTREE = 1
    index_params = dict(algorithm=FLANN_INDEX_KDTREE, trees=5)
    search_params = dict(checks=50)
    flann = cv2.FlannBasedMatcher(index_params, search_params)
    bf = cv2.BFMatcher(cv2.NORM_HAMMING, crossCheck=True)

    # 메인 이미지와 타겟 이미지 목록 (이미지 파일만 필터링)
    image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp']
    main_images = [f for f in os.listdir(main_folder_path) 
                  if os.path.splitext(f)[1].lower() in image_extensions]
    target_images = [f for f in os.listdir(target_folder_path) 
                    if os.path.splitext(f)[1].lower() in image_extensions]

    matching_images_sift = set()
    matching_images_akaze = set()

    # 처리 시작 시간
    start_time = time.time()
    processed_count = 0

    for main_image in main_images:
        main_path = os.path.join(main_folder_path, main_image)
        
        try:
            # 메인 이미지 특징점 추출 (캐시 사용)
            main_sift_features = feature_cache.get(main_path, "sift")
            if main_sift_features is None:
                main_img = remove_background(main_path)
                if main_img is None or main_img.dtype != np.uint8:
                    logger.warning(f"메인 이미지 로드 실패: {main_image}")
                    continue
                    
                main_kp_sift, main_des_sift = sift.detectAndCompute(main_img, None)
                feature_cache.put(main_path, "sift", (main_kp_sift, main_des_sift))
            else:
                main_kp_sift, main_des_sift = main_sift_features
                
            main_akaze_features = feature_cache.get(main_path, "akaze")
            if main_akaze_features is None:
                if 'main_img' not in locals():
                    main_img = remove_background(main_path)
                    if main_img is None or main_img.dtype != np.uint8:
                        logger.warning(f"메인 이미지 로드 실패: {main_image}")
                        continue
                        
                main_kp_akaze, main_des_akaze = akaze.detectAndCompute(main_img, None)
                feature_cache.put(main_path, "akaze", (main_kp_akaze, main_des_akaze))
            else:
                main_kp_akaze, main_des_akaze = main_akaze_features
                
            # 특징점 없는 경우 처리
            if main_des_sift is None or main_des_akaze is None:
                logger.warning(f"메인 이미지 특징점 추출 실패: {main_image}")
                continue
        except Exception as e:
            logger.error(f"메인 이미지 처리 오류 {main_image}: {e}")
            continue
            
        for target_image in target_images:
            target_path = os.path.join(target_folder_path, target_image)
            
            try:
                # 타겟 이미지 특징점 추출 (캐시 사용)
                target_sift_features = feature_cache.get(target_path, "sift")
                if target_sift_features is None:
                    target_img = remove_background(target_path)
                    if target_img is None or target_img.dtype != np.uint8:
                        logger.warning(f"타겟 이미지 로드 실패: {target_image}")
                        continue
                        
                    target_kp_sift, target_des_sift = sift.detectAndCompute(target_img, None)
                    feature_cache.put(target_path, "sift", (target_kp_sift, target_des_sift))
                else:
                    target_kp_sift, target_des_sift = target_sift_features
                    
                target_akaze_features = feature_cache.get(target_path, "akaze")
                if target_akaze_features is None:
                    if 'target_img' not in locals():
                        target_img = remove_background(target_path)
                        if target_img is None or target_img.dtype != np.uint8:
                            logger.warning(f"타겟 이미지 로드 실패: {target_image}")
                            continue
                            
                    target_kp_akaze, target_des_akaze = akaze.detectAndCompute(target_img, None)
                    feature_cache.put(target_path, "akaze", (target_kp_akaze, target_des_akaze))
                else:
                    target_kp_akaze, target_des_akaze = target_akaze_features
                
                # 특징점 없는 경우 처리
                if target_des_sift is None or target_des_akaze is None:
                    logger.warning(f"타겟 이미지 특징점 추출 실패: {target_image}")
                    continue
                    
                # SIFT 매칭
                if len(main_des_sift) >= 2 and len(target_des_sift) >= 2:
                    try:
                        matches_sift = flann.knnMatch(main_des_sift, target_des_sift, k=2)
                        good_matches_sift = []
                        
                        # Lowe's ratio test
                        for match_pair in matches_sift:
                            if len(match_pair) == 2:
                                m, n = match_pair
                                if m.distance < sift_ratio_threshold * n.distance:
                                    good_matches_sift.append(m)
                                    
                        # RANSAC으로 호모그래피 계산
                        if len(good_matches_sift) > 4:
                            src_pts = np.float32([main_kp_sift[m.queryIdx].pt for m in good_matches_sift]).reshape(-1, 1, 2)
                            dst_pts = np.float32([target_kp_sift[m.trainIdx].pt for m in good_matches_sift]).reshape(-1, 1, 2)
                            
                            H, mask = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, ransac_reproj_threshold)
                            inliers = np.sum(mask) if mask is not None else 0
                            inlier_ratio = inliers / len(good_matches_sift) if len(good_matches_sift) > 0 else 0
                            
                            # 정확한 매칭으로 판단될 경우 저장
                            if inliers > feature_match_threshold or inlier_ratio > 0.5:
                                filename, _ = os.path.splitext(target_image)
                                matching_images_sift.add(filename)
                                logger.debug(f"SIFT - {target_image}: {inliers} inliers, {inlier_ratio:.2f} ratio")
                    except cv2.error as e:
                        logger.warning(f"SIFT 매칭 오류 {target_image}: {e}")
                        
                # AKAZE 매칭
                if len(main_des_akaze) >= 2 and len(target_des_akaze) >= 2:
                    try:
                        matches_akaze = bf.match(main_des_akaze, target_des_akaze)
                        matches_akaze = sorted(matches_akaze, key=lambda x: x.distance)
                        
                        # 거리 기반 필터링
                        good_matches_akaze = [m for m in matches_akaze if m.distance < akaze_distance_threshold]
                        
                        # 충분한 매칭점이 있는 경우 호모그래피 계산
                        if len(good_matches_akaze) > 4:
                            src_pts = np.float32([main_kp_akaze[m.queryIdx].pt for m in good_matches_akaze]).reshape(-1, 1, 2)
                            dst_pts = np.float32([target_kp_akaze[m.trainIdx].pt for m in good_matches_akaze]).reshape(-1, 1, 2)
                            
                            H, mask = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, ransac_reproj_threshold)
                            inliers = np.sum(mask) if mask is not None else 0
                            inlier_ratio = inliers / len(good_matches_akaze) if len(good_matches_akaze) > 0 else 0
                            
                            # 정확한 매칭으로 판단될 경우 저장
                            if inliers > feature_match_threshold or inlier_ratio > 0.5:
                                filename, _ = os.path.splitext(target_image)
                                matching_images_akaze.add(filename)
                                logger.debug(f"AKAZE - {target_image}: {inliers} inliers, {inlier_ratio:.2f} ratio")
                    except cv2.error as e:
                        logger.warning(f"AKAZE 매칭 오류 {target_image}: {e}")
            except Exception as e:
                logger.error(f"타겟 이미지 처리 오류 {target_image}: {e}")
                
        # 진행 상황 로깅
        processed_count += 1
        if processed_count % 5 == 0 or processed_count == len(main_images):
            elapsed = time.time() - start_time
            logger.info(f"진행 상황: {processed_count}/{len(main_images)} 이미지 처리 완료 (경과 시간: {elapsed:.1f}초)")

    # 두 알고리즘의 결과 합집합 (OR 조건)
    matching_results = matching_images_sift.union(matching_images_akaze)
    
    logger.info(f"매칭 완료: SIFT={len(matching_images_sift)}, AKAZE={len(matching_images_akaze)}, 합집합={len(matching_results)}")
    
    # 결과 반환
    return ",".join(matching_results)

if __name__ == "__main__":
    # 기본 로깅 설정
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # 설정 파일 로드
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.ini')
    config.read(config_path, encoding='utf-8')
    
    # 설정에서 경로 가져오기
    main_folder_path = config.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main')
    target_folder_path = config.get('Matching', 'images_dir', fallback='C:\\RPA\\Image\\Target')
    
    logger.info(f"이미지 매칭 시작:")
    logger.info(f"- 메인 폴더: {main_folder_path}")
    logger.info(f"- 타겟 폴더: {target_folder_path}")

    matches = sift_and_akaze_image_matching(main_folder_path, target_folder_path)

    if matches:
        print("매칭된 이미지가 발견되었습니다:")
        print(matches)
    else:
        print("매칭된 이미지가 없습니다.")
