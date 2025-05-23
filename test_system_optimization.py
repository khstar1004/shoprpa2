#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
시스템 최적화 및 성능 테스트 스크립트

이 스크립트는 개선된 RPA 시스템의 성능을 테스트하고 
최적화 제안을 제공합니다.

Usage:
    python test_system_optimization.py

Features:
    - 시스템 성능 모니터링
    - 설정 파일 검증
    - 임시 파일 정리
    - 성능 벤치마크
    - 메모리 최적화
"""

import os
import sys
import logging
import configparser
import time
from datetime import datetime
from pathlib import Path

# 프로젝트 루트 디렉토리를 Python 경로에 추가
script_dir = Path(__file__).resolve().parent
if str(script_dir) not in sys.path:
    sys.path.insert(0, str(script_dir))

# PythonScript 디렉토리 추가
pythonscript_dir = script_dir / 'PythonScript'
if str(pythonscript_dir) not in sys.path:
    sys.path.insert(0, str(pythonscript_dir))

def setup_logging():
    """로깅 설정"""
    log_format = '%(asctime)s - %(levelname)s - [%(name)s] - %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('system_optimization_test.log', encoding='utf-8')
        ]
    )

def load_config() -> configparser.ConfigParser:
    """설정 파일 로드"""
    config = configparser.ConfigParser()
    config_path = script_dir / 'config.ini'
    
    if config_path.exists():
        config.read(config_path, encoding='utf-8')
        logging.info(f"✅ 설정 파일 로드 완료: {config_path}")
    else:
        logging.warning(f"⚠️ 설정 파일 없음: {config_path}")
        
    return config

def test_hash_functions():
    """해시 함수 성능 테스트"""
    try:
        from PythonScript.utils import generate_product_name_hash, extract_product_hash_from_filename
        
        logging.info("🔧 해시 함수 테스트 시작...")
        
        # 해시 생성 테스트
        test_products = [
            "테스트 상품 123",
            "Test Product ABC",
            "  공백이   많은   상품  ",
            "특수문자!@#$%^&*()상품",
            "한글과English혼합Product",
            "",  # 빈 문자열
            None  # None 값
        ]
        
        results = []
        for product in test_products:
            try:
                hash_value = generate_product_name_hash(product)
                results.append({
                    'input': repr(product),
                    'hash': hash_value,
                    'success': bool(hash_value)
                })
            except Exception as e:
                results.append({
                    'input': repr(product),
                    'error': str(e),
                    'success': False
                })
        
        # 결과 출력
        success_count = sum(1 for r in results if r['success'])
        logging.info(f"해시 생성 테스트: {success_count}/{len(results)} 성공")
        
        for result in results:
            if result['success']:
                logging.debug(f"  ✅ {result['input']} -> {result['hash']}")
            else:
                logging.debug(f"  ❌ {result['input']} -> 오류: {result.get('error', '실패')}")
        
        # 해시 추출 테스트
        test_filenames = [
            "haereum_1234567890abcdef_12345678.jpg",
            "kogift_abcdef1234567890.png",
            "naver_invalidhash.jpg",
            "1234567890abcdef.jpg",
            "invalid_filename.jpg"
        ]
        
        extract_results = []
        for filename in test_filenames:
            try:
                extracted_hash = extract_product_hash_from_filename(filename)
                extract_results.append({
                    'filename': filename,
                    'extracted': extracted_hash,
                    'success': extracted_hash is not None
                })
            except Exception as e:
                extract_results.append({
                    'filename': filename,
                    'error': str(e),
                    'success': False
                })
        
        extract_success = sum(1 for r in extract_results if r['success'])
        logging.info(f"해시 추출 테스트: {extract_success}/{len(extract_results)} 성공")
        
        return {
            'hash_generation': results,
            'hash_extraction': extract_results,
            'overall_success': success_count > 0 and extract_success > 0
        }
        
    except ImportError as e:
        logging.error(f"❌ 해시 함수 import 실패: {e}")
        return {'error': str(e), 'overall_success': False}
    except Exception as e:
        logging.error(f"❌ 해시 함수 테스트 오류: {e}")
        return {'error': str(e), 'overall_success': False}

def test_performance_monitoring(config):
    """성능 모니터링 테스트"""
    try:
        from PythonScript.utils import (
            monitor_system_performance, 
            optimize_memory_usage,
            validate_configuration,
            cleanup_temp_files,
            benchmark_system_performance
        )
        
        logging.info("📊 성능 모니터링 테스트 시작...")
        
        # 1. 시스템 성능 모니터링
        perf_data = monitor_system_performance(config)
        if 'error' not in perf_data:
            logging.info(f"시스템 상태: {perf_data['emoji']} {perf_data['status']} "
                        f"(점수: {perf_data['performance_score']})")
            
            metrics = perf_data['system_metrics']
            logging.info(f"  메모리: {metrics['memory_percent']:.1f}% "
                        f"(가용: {metrics['memory_available_gb']}GB)")
            logging.info(f"  CPU: {metrics['cpu_percent']:.1f}% "
                        f"({metrics['cpu_count']}코어)")
            logging.info(f"  디스크: {metrics['disk_percent']:.1f}% "
                        f"(가용: {metrics['disk_free_gb']:.1f}GB)")
            logging.info(f"  GPU: {metrics['gpu_info']}")
            
            if perf_data['recommendations']:
                logging.info("권장사항:")
                for rec in perf_data['recommendations']:
                    logging.info(f"  {rec}")
        else:
            logging.error(f"성능 모니터링 오류: {perf_data['error']}")
        
        # 2. 메모리 최적화
        mem_result = optimize_memory_usage()
        if mem_result:
            logging.info(f"메모리 최적화: {mem_result['objects_collected']}개 객체 정리")
        
        # 3. 설정 검증
        suggestions = validate_configuration(config)
        logging.info(f"설정 검증 완료: {len(suggestions)}개 항목")
        for suggestion in suggestions[:5]:  # 처음 5개만 표시
            logging.info(f"  {suggestion}")
        
        # 4. 임시 파일 정리
        cleanup_result = cleanup_temp_files(config, max_age_days=1)
        if 'error' not in cleanup_result:
            if cleanup_result['deleted_files_count'] > 0:
                logging.info(f"임시 파일 정리: {cleanup_result['deleted_files_count']}개 파일 "
                           f"({cleanup_result['deleted_size_mb']}MB) 삭제")
            else:
                logging.info("정리할 임시 파일 없음")
        else:
            logging.warning(f"임시 파일 정리 오류: {cleanup_result['error']}")
        
        # 5. 성능 벤치마크
        benchmark_result = benchmark_system_performance(config)
        if 'error' not in benchmark_result:
            logging.info(f"성능 벤치마크: {benchmark_result['grade']} "
                        f"(점수: {benchmark_result['overall_score']})")
            
            tests = benchmark_result['tests']
            if 'hash_generation' in tests:
                hash_test = tests['hash_generation']
                logging.info(f"  해시 생성: {hash_test['time_seconds']}초 "
                           f"({hash_test['ops_per_second']} ops/sec)")
        else:
            logging.error(f"벤치마크 오류: {benchmark_result['error']}")
        
        return {
            'performance_monitoring': perf_data,
            'memory_optimization': mem_result,
            'config_validation': suggestions,
            'cleanup': cleanup_result,
            'benchmark': benchmark_result,
            'overall_success': True
        }
        
    except ImportError as e:
        logging.error(f"❌ 성능 모니터링 함수 import 실패: {e}")
        return {'error': str(e), 'overall_success': False}
    except Exception as e:
        logging.error(f"❌ 성능 모니터링 테스트 오류: {e}")
        return {'error': str(e), 'overall_success': False}

def test_image_integration():
    """이미지 통합 기능 테스트"""
    try:
        from PythonScript.image_integration import (
            get_system_status_summary,
            print_system_status
        )
        
        logging.info("🖼️ 이미지 통합 시스템 테스트 시작...")
        
        # 시스템 상태 요약
        status = get_system_status_summary()
        if 'error' not in status:
            logging.info(f"이미지 매칭 시스템: {status['matching_system']['version']}")
            logging.info(f"해시 알고리즘: {status['matching_system']['hash_algorithm']}")
            logging.info(f"고급 매처 사용 가능: {status['matching_system']['enhanced_matcher_available']}")
            
            improvements = status.get('improvements', [])
            logging.info(f"주요 개선사항: {len(improvements)}개")
            for improvement in improvements:
                logging.debug(f"  {improvement}")
        else:
            logging.error(f"이미지 시스템 상태 확인 오류: {status['error']}")
        
        return {
            'system_status': status,
            'overall_success': 'error' not in status
        }
        
    except ImportError as e:
        logging.error(f"❌ 이미지 통합 함수 import 실패: {e}")
        return {'error': str(e), 'overall_success': False}
    except Exception as e:
        logging.error(f"❌ 이미지 통합 테스트 오류: {e}")
        return {'error': str(e), 'overall_success': False}

def run_comprehensive_test():
    """종합 테스트 실행"""
    logging.info("🚀 시스템 최적화 종합 테스트 시작")
    logging.info("=" * 60)
    
    start_time = time.time()
    config = load_config()
    
    # 테스트 결과 수집
    test_results = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'tests': {}
    }
    
    # 1. 해시 함수 테스트
    logging.info("\n1️⃣ 해시 함수 테스트")
    hash_result = test_hash_functions()
    test_results['tests']['hash_functions'] = hash_result
    
    # 2. 성능 모니터링 테스트
    logging.info("\n2️⃣ 성능 모니터링 테스트")
    perf_result = test_performance_monitoring(config)
    test_results['tests']['performance_monitoring'] = perf_result
    
    # 3. 이미지 통합 테스트
    logging.info("\n3️⃣ 이미지 통합 시스템 테스트")
    image_result = test_image_integration()
    test_results['tests']['image_integration'] = image_result
    
    # 전체 결과 요약
    total_time = time.time() - start_time
    success_count = sum(1 for test in test_results['tests'].values() 
                       if test.get('overall_success', False))
    total_tests = len(test_results['tests'])
    
    test_results['summary'] = {
        'total_time_seconds': round(total_time, 2),
        'successful_tests': success_count,
        'total_tests': total_tests,
        'success_rate': round((success_count / total_tests * 100), 1) if total_tests > 0 else 0
    }
    
    logging.info("\n" + "=" * 60)
    logging.info("📊 테스트 완료 요약")
    logging.info("=" * 60)
    logging.info(f"실행 시간: {test_results['summary']['total_time_seconds']}초")
    logging.info(f"성공한 테스트: {success_count}/{total_tests}")
    logging.info(f"성공률: {test_results['summary']['success_rate']}%")
    
    # 개별 테스트 결과
    for test_name, result in test_results['tests'].items():
        status = "✅ 성공" if result.get('overall_success', False) else "❌ 실패"
        logging.info(f"  {test_name}: {status}")
        if 'error' in result:
            logging.info(f"    오류: {result['error']}")
    
    # 전체 평가
    if test_results['summary']['success_rate'] >= 80:
        logging.info("🎉 전체 평가: 우수 - 시스템이 정상적으로 최적화되었습니다!")
    elif test_results['summary']['success_rate'] >= 60:
        logging.info("👍 전체 평가: 양호 - 대부분의 기능이 정상 작동합니다.")
    else:
        logging.info("⚠️ 전체 평가: 개선 필요 - 일부 기능에 문제가 있습니다.")
    
    return test_results

def main():
    """메인 함수"""
    setup_logging()
    
    try:
        logging.info("시스템 최적화 및 성능 테스트 스크립트")
        logging.info(f"시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 종합 테스트 실행
        results = run_comprehensive_test()
        
        # 결과를 JSON 파일로 저장
        import json
        result_file = script_dir / f"test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        logging.info(f"\n📄 테스트 결과 저장: {result_file}")
        
        return results['summary']['success_rate'] >= 80
        
    except Exception as e:
        logging.error(f"❌ 테스트 실행 중 치명적 오류: {e}")
        import traceback
        logging.debug(traceback.format_exc())
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 