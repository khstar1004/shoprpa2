import os
import sys
import logging
import configparser
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from PythonScript.image_integration import (
    prepare_image_metadata, 
    find_best_image_matches,
    generate_product_name_hash,
    extract_product_hash_from_filename
)

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def test_hash_extraction():
    """Test hash extraction from filenames"""
    logging.info("=== Testing Hash Extraction ===")
    
    test_filenames = [
        "haereum_0792f8d36850a88b_6e3684b1.jpg",
        "kogift_241b288d9114ccab_e11fdfca.jpg",
        "naver_5acb0fbf5b7af799_2664ee24.jpg",
        "haereum_0792f8d36850a88b_6e3684b1_nobg.png"
    ]
    
    for filename in test_filenames:
        hash_value = extract_product_hash_from_filename(filename)
        logging.info(f"Filename: {filename} -> Hash: {hash_value}")
        
    return True

def test_hash_generation():
    """Test hash generation from product names"""
    logging.info("\n=== Testing Hash Generation ===")
    
    test_products = [
        "베이직스타일 하프 앞치마",
        "베이직 스타일 하프앞치마",  # Different spacing
        "베이직스타일하프앞치마",    # No spaces
        "BASIC STYLE HALF APRON"      # English
    ]
    
    for product in test_products:
        hash_value = generate_product_name_hash(product)
        logging.info(f"Product: '{product}' -> Hash: {hash_value}")
        
    return True

def test_metadata_preparation():
    """Test metadata preparation with hash extraction"""
    logging.info("\n=== Testing Metadata Preparation ===")
    
    # Create unique test directory to avoid conflicts
    import tempfile
    import shutil
    
    # Create temporary directory
    with tempfile.TemporaryDirectory(prefix="test_images_") as test_dir:
        test_dir_path = Path(test_dir)
        
        # Create test image files with different hash patterns
        test_files = [
            "haereum_0792f8d36850a88b_6e3684b1.jpg",      # 16-char hex
            "kogift_241b288d9114ccab_e11fdfca.jpg",        # 16-char hex
            "naver_5acb0fbf5b7af799_2664ee24.jpg",         # 16-char hex
            "kogift_1912824fba_2061e0f04f.jpg",            # 10-char hex
            "shop_1707873892937710_0.jpg"                  # shop pattern
        ]
        
        for filename in test_files:
            (test_dir_path / filename).touch()
        
        # Prepare metadata
        metadata = prepare_image_metadata(test_dir_path, 'test_')
        
        # Check if hash is extracted
        success = True
        expected_hashes = {
            "haereum_0792f8d36850a88b_6e3684b1.jpg": "0792f8d36850a88b",
            "kogift_241b288d9114ccab_e11fdfca.jpg": "241b288d9114ccab", 
            "naver_5acb0fbf5b7af799_2664ee24.jpg": "5acb0fbf5b7af799",
            "kogift_1912824fba_2061e0f04f.jpg": "1912824fba",
            "shop_1707873892937710_0.jpg": "1707873892937710"
        }
        
        for path, info in metadata.items():
            filename = info['filename']
            hash_value = info.get('product_hash')
            expected_hash = expected_hashes.get(filename)
            
            if expected_hash and hash_value == expected_hash:
                logging.info(f"✅ File: {filename} -> Hash: {hash_value}")
            elif expected_hash:
                logging.error(f"❌ File: {filename} -> Expected: {expected_hash}, Got: {hash_value}")
                success = False
            else:
                logging.warning(f"⚠️ File: {filename} -> No expected hash defined")
        
        # Cleanup is automatic with TemporaryDirectory
        
    return success

def test_hash_matching():
    """Test hash-based image matching"""
    logging.info("\n=== Testing Hash-Based Matching ===")
    
    # Create test config
    config = configparser.ConfigParser()
    config.add_section('ImageMatching')
    config.set('ImageMatching', 'similarity_threshold', '0.8')
    
    # Test product names
    product_names = [
        "베이직스타일 하프 앞치마",
        "테스트 상품",
        "매칭 안되는 상품"
    ]
    
    # Simulate image metadata with hashes
    # Hash for "베이직스타일 하프 앞치마" (without spaces and lowercase)
    expected_hash = generate_product_name_hash("베이직스타일 하프 앞치마")
    logging.info(f"Expected hash for '베이직스타일 하프 앞치마': {expected_hash}")
    
    haereum_images = {
        f"haereum_{expected_hash}_12345678.jpg": {
            'path': f"haereum_{expected_hash}_12345678.jpg",
            'product_hash': expected_hash,
            'filename': f"haereum_{expected_hash}_12345678.jpg"
        }
    }
    
    kogift_images = {
        f"kogift_{expected_hash}_87654321.jpg": {
            'path': f"kogift_{expected_hash}_87654321.jpg",
            'product_hash': expected_hash,
            'filename': f"kogift_{expected_hash}_87654321.jpg"
        }
    }
    
    naver_images = {
        f"naver_{expected_hash}_11111111.jpg": {
            'path': f"naver_{expected_hash}_11111111.jpg",
            'product_hash': expected_hash,
            'filename': f"naver_{expected_hash}_11111111.jpg"
        }
    }
    
    # Run matching
    matches = find_best_image_matches(
        product_names,
        haereum_images,
        kogift_images,
        naver_images,
        similarity_threshold=0.8,
        config=config
    )
    
    # Check results
    success = True
    for idx, (product_name, match_set) in enumerate(zip(product_names, matches)):
        haereum_match, kogift_match, naver_match = match_set
        
        if idx == 0:  # First product should match
            if haereum_match and kogift_match and naver_match:
                logging.info(f"✅ Product '{product_name}' matched successfully!")
                logging.info(f"   Haereum: {haereum_match[0] if haereum_match else None}")
                logging.info(f"   Kogift: {kogift_match[0] if kogift_match else None}")
                logging.info(f"   Naver: {naver_match[0] if naver_match else None}")
            else:
                logging.error(f"❌ Product '{product_name}' failed to match!")
                success = False
        else:  # Other products should not match
            if not haereum_match and not kogift_match and not naver_match:
                logging.info(f"✅ Product '{product_name}' correctly not matched (no hash match)")
            else:
                logging.error(f"❌ Product '{product_name}' incorrectly matched!")
                success = False
    
    return success

def main():
    """Run all tests"""
    logging.info("Starting Hash Matching Fix Tests")
    logging.info("="*60)
    
    tests = [
        ("Hash Extraction", test_hash_extraction),
        ("Hash Generation", test_hash_generation),
        ("Metadata Preparation", test_metadata_preparation),
        ("Hash Matching", test_hash_matching)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            logging.error(f"Test '{test_name}' failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    logging.info("\n" + "="*60)
    logging.info("Test Summary:")
    all_passed = True
    for test_name, result in results:
        status = "PASSED" if result else "FAILED"
        logging.info(f"  {test_name}: {status}")
        if not result:
            all_passed = False
    
    if all_passed:
        logging.info("\n✅ All tests passed! Hash matching should now work correctly.")
    else:
        logging.error("\n❌ Some tests failed. Please check the logs above.")
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 