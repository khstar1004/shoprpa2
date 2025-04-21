import os
import cv2
import numpy as np
import configparser
import logging

def image_matching(main_folder_path=None, target_folder_path=None):
    """
    이미지 매칭을 수행하는 함수
    
    Args:
        main_folder_path: 메인 이미지가 있는 폴더 경로 (None이면 config에서 값을 가져옴)
        target_folder_path: 타겟 이미지가 있는 폴더 경로 (None이면 config에서 값을 가져옴)
        
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
    
    # 로깅 설정
    logging.info(f"SIFT_DOG_RANSAC matching using target folder: {target_folder_path}")
    logging.info(f"SIFT_DOG_RANSAC matching using main folder: {main_folder_path}")
    
    # 폴더 존재 확인
    if not os.path.exists(target_folder_path):
        logging.warning(f"Target folder does not exist: {target_folder_path}")
        os.makedirs(target_folder_path, exist_ok=True)
        
    if not os.path.exists(main_folder_path):
        logging.warning(f"Main folder does not exist: {main_folder_path}")
        os.makedirs(main_folder_path, exist_ok=True)
    
    # 폴더 내 이미지 파일만 리스트로
    def get_image_files_from_folder(folder_path):
        image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp']  
        image_files = []

        for root, dirs, files in os.walk(folder_path):
            for file in files:
                _, ext = os.path.splitext(file)
                if ext.lower() in image_extensions:
                    image_files.append(os.path.join(root, file))

        return image_files

    # 이미지에서 DoG (Difference of Gaussian)을 계산
    def calculate_dog(image, ksize1, ksize2):
        blurred1 = cv2.GaussianBlur(image, (ksize1, ksize1), 0)
        blurred2 = cv2.GaussianBlur(image, (ksize2, ksize2), 0)
        dog = blurred1 - blurred2
        return dog

    # 이미지에서 SIFT(Scale-Invariant Feature Transform) 매칭 점수를 계산
    def calculate_sift_matching_score(image1, image2):
        sift = cv2.SIFT_create()
        kp1, des1 = sift.detectAndCompute(image1, None)
        kp2, des2 = sift.detectAndCompute(image2, None)

        bf = cv2.BFMatcher()
        matches = bf.knnMatch(des1, des2, k=2)

        good_matches = []
        for m, n in matches:
            if m.distance < 0.75 * n.distance:
                good_matches.append(m)

        src_pts = np.float32([kp1[m.queryIdx].pt for m in good_matches]).reshape(-1, 1, 2)
        dst_pts = np.float32([kp2[m.trainIdx].pt for m in good_matches]).reshape(-1, 1, 2)

        if len(good_matches) > 5:
            M, mask = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, 5.0)
            matches_mask = mask.ravel().tolist()
            inlier_matches = [m for i, m in enumerate(good_matches) if matches_mask[i] == 1]
            matching_score = len(inlier_matches) / len(good_matches)
        else:
            matching_score = 0.0

        return matching_score

    # 메인 폴더와 타겟 폴더 내의 이미지 파일들을 가져옵니다.
    main_image_files = get_image_files_from_folder(main_folder_path)
    target_image_files = get_image_files_from_folder(target_folder_path)
    
    logging.info(f"Found {len(main_image_files)} main images and {len(target_image_files)} target images")

    matching_results = set()  # 일치율이 0 이상인 target 이미지 파일 이름 저장을 위한 set
    for main_file in main_image_files:
        try:
            main_image = cv2.imread(main_file, cv2.IMREAD_GRAYSCALE)
            if main_image is None:
                raise Exception(f"이미지를 읽을 수 없습니다: {main_file}")

            for target_file in target_image_files:
                try:
                    target_image = cv2.imread(target_file, cv2.IMREAD_GRAYSCALE)
                    if target_image is None:
                        raise Exception(f"이미지를 읽을 수 없습니다: {target_file}")

                    ksize1 = 3
                    ksize2 = 11
                    main_dog = calculate_dog(main_image, ksize1, ksize2)
                    target_dog = calculate_dog(target_image, ksize1, ksize2)

                    # 두 이미지 간의 SIFT 매칭 점수를 계산합니다.
                    matching_score = calculate_sift_matching_score(main_dog, target_dog)

                    if matching_score > 0:
                        base_name = os.path.basename(target_file)
                        file_name, _ = os.path.splitext(base_name)
                        matching_results.add(file_name)
                except Exception as target_e:
                    logging.error(f"{target_file} 처리 중 오류 발생: {target_e}")
        except Exception as main_e:
            logging.error(f"{main_file} 처리 중 오류 발생: {main_e}")

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
    
    logging.info(f"Starting SIFT_DOG_RANSAC image matching using:")
    logging.info(f"- Main folder: {main_folder_path}")
    logging.info(f"- Target folder: {target_folder_path}")

    matching_files = image_matching(main_folder_path, target_folder_path)
    print(f"대상 이미지: {matching_files}")
