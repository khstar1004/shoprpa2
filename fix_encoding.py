#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
이 스크립트는 config.ini 및 다른 필요한 파일들의 인코딩을 UTF-8으로 변환합니다.
Windows에서 CP949 인코딩으로 인한 문제를 해결하기 위함입니다.
"""

import os
import sys
import codecs
from pathlib import Path

def convert_file_to_utf8(file_path):
    """
    파일을 다양한 인코딩으로 읽으려고 시도하고 UTF-8로 저장합니다.
    """
    encodings = ['utf-8', 'cp949', 'euc-kr', 'latin1']
    file_content = None
    
    # 먼저 파일이 존재하는지 확인
    if not os.path.exists(file_path):
        print(f"오류: 파일이 존재하지 않습니다: {file_path}")
        return False
    
    # 다양한 인코딩으로 읽기 시도
    for encoding in encodings:
        try:
            with codecs.open(file_path, 'r', encoding=encoding) as f:
                file_content = f.read()
                print(f"파일을 {encoding} 인코딩으로 성공적으로 읽었습니다: {file_path}")
                break
        except UnicodeDecodeError:
            print(f"{encoding} 인코딩으로 읽기 실패, 다음 인코딩 시도 중...")
            continue
    
    if file_content is None:
        print(f"오류: 어떤 인코딩으로도 파일을 읽을 수 없습니다: {file_path}")
        return False
    
    # UTF-8로 파일 다시 저장
    try:
        with codecs.open(file_path, 'w', encoding='utf-8') as f:
            f.write(file_content)
        print(f"파일을 UTF-8로 저장했습니다: {file_path}")
        return True
    except Exception as e:
        print(f"파일 저장 중 오류 발생: {str(e)}")
        return False

def main():
    # 현재 디렉토리 확인
    current_dir = Path.cwd()
    print(f"현재 작업 디렉토리: {current_dir}")
    
    # config.ini 파일 변환
    config_path = current_dir / 'config.ini'
    if convert_file_to_utf8(config_path):
        print("config.ini 파일을 UTF-8로 변환했습니다.")
    else:
        print("config.ini 파일 변환에 실패했습니다.")
    
    # PythonScript 디렉토리의 data_processing.py 파일도 변환
    python_script_dir = current_dir / 'PythonScript'
    data_processing_path = python_script_dir / 'data_processing.py'
    if os.path.exists(data_processing_path):
        if convert_file_to_utf8(data_processing_path):
            print("data_processing.py 파일을 UTF-8로 변환했습니다.")
        else:
            print("data_processing.py 파일 변환에 실패했습니다.")
    
    print("변환 작업이 완료되었습니다.")

if __name__ == "__main__":
    main() 