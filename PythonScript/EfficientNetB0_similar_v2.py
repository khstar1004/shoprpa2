import os
import csv
import numpy as np
import tensorflow as tf
from sklearn.metrics.pairwise import cosine_similarity
import re

def natural_sort(l): 
    """
    정규 표현식을 사용해 문자열과 숫자를 분리하고, 이를 기반으로 리스트를 정렬합니다.
    """
    convert = lambda text: int(text) if text.isdigit() else text.lower()
    alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
    return sorted(l, key=alphanum_key)

def calculate_highest_similarities():
    os.environ['CUDA_VISIBLE_DEVICES'] = '-1'
    main_dir = 'C:\\RPA\\Image\\Main'
    target_dir = 'C:\\RPA\\Image\\Target'
    output_path = 'C:\\RPA\\Image\\Target\\output.csv'

    base_model = tf.keras.applications.EfficientNetB0(weights='imagenet', include_top=False)
    global_avg_layer = tf.keras.layers.GlobalAveragePooling2D()(base_model.output)
    feature_extractor = tf.keras.Model(inputs=base_model.input, outputs=global_avg_layer)

    def extract_features_from_img(img_path):
        try:
            img = tf.keras.preprocessing.image.load_img(img_path, target_size=(224, 224))
            img_array = tf.keras.preprocessing.image.img_to_array(img)
            img_batch = tf.expand_dims(img_array, 0)
            img_preprocessed = tf.keras.applications.efficientnet.preprocess_input(img_batch)
            
            features = feature_extractor(img_preprocessed)
            return features.numpy().flatten()
        except Exception as e:
            print(f"An error occurred while processing {img_path}: {e}")
            return None

    main_image_paths = [os.path.join(main_dir, f) for f in os.listdir(main_dir) if f.endswith(('.jpg', '.jpeg', '.png'))]
    target_images = [os.path.join(target_dir, f) for f in natural_sort(os.listdir(target_dir)) if f.endswith(('.jpg', '.jpeg', '.png'))]
    
    highest_similarity_records = []

    for target_img_path in target_images:
        highest_similarity = -1  # 초기값 설정
        best_main_image = ""
        
        for main_img_path in main_image_paths:
            main_features = extract_features_from_img(main_img_path)
            target_features = extract_features_from_img(target_img_path)
            
            if main_features is None or target_features is None:
                continue  # 이미지를 처리하지 못했을 경우 다음 이미지로 넘어간다.
            
            similarity = cosine_similarity([main_features], [target_features])[0][0]
            
            if similarity > highest_similarity:
                highest_similarity = similarity
                best_main_image = main_img_path

        # 모든 메인 이미지와의 비교가 끝나면 최고 유사도 값을 저장한다.
        highest_similarity_records.append(highest_similarity)

    with open(output_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        for sim in highest_similarity_records:
            writer.writerow([sim])