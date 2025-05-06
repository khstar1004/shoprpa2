import pandas as pd
import logging
import configparser
import os
import openpyxl # Required for reading/writing .xlsx
import re
import aiohttp
import asyncio
from urllib.parse import urljoin
import datetime

# Define expected column names from the final Excel output
# Adjust these if the actual column names in the generated Excel differ
KOREAGIFT_LINK_COL = '고려 링크'
NAVER_LINK_COL = '네이버 링크'
# Price difference columns to filter
KOREAGIFT_PRICE_DIFF_COL = '고려 가격차이'
NAVER_PRICE_DIFF_COL = '네이버 가격차이'
# Optional: Add other columns to check if links alone are insufficient
# KOREAGIFT_PRICE_COL = '판매단가2(VAT포함)'
# NAVER_PRICE_COL = '판매단가3 (VAT포함)'

async def check_image_url_exists(url, session=None):
    """Check if an image URL actually exists by performing a HEAD request.
    Returns True if the URL exists, False otherwise.
    """
    if not url:
        return False
    
    close_session = False
    try:
        if session is None:
            session = aiohttp.ClientSession()
            close_session = True
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://www.jclgift.com/'
        }
        
        async with session.head(url, headers=headers, timeout=5, ssl=False) as response:
            return response.status == 200
    except Exception as e:
        logging.debug(f"Error checking URL {url}: {e}")
        return False
    finally:
        if close_session and session:
            await session.close()

async def convert_haereum_url(url):
    """Convert Korean text Haereum URLs to proper product code URLs.
    Tries different formats (jpg, png) and returns the first working URL.
    """
    if not url or not isinstance(url, str):
        return None
        
    if '/upload/product/simg3/' not in url:
        return url  # Not a Haereum product URL
        
    # If URL already contains product code format (capital letters followed by numbers), keep it
    if re.search(r'\/[A-Z]{4}\d{7}', url):
        return url
    
    # For Korean text URLs, try standard product code patterns with different extensions
    base_url = 'https://www.jclgift.com'
    async with aiohttp.ClientSession() as session:
        # Extract the Korean keyword from the URL
        korean_text = url.split('/')[-1].split('.')[0]
        logging.info(f"Converting Korean URL for product: {korean_text}")
        
        # First try the pattern from successful image downloads in logs
        # These are actual product codes seen in successful downloads
        product_codes = [
            "DDAC0007399s", "CCCH0008576s_3", "BBCA0009349s", 
            "CCBK0001873s", "LLAC0009357s"
        ]
        
        # Try with different extensions
        for code in product_codes:
            for ext in ['.jpg', '.png', '.jpeg']:
                test_url = urljoin(base_url, f"/upload/product/simg3/{code}{ext}")
                
                if await check_image_url_exists(test_url, session):
                    logging.info(f"Korean URL converted using known product code: {url} → {test_url}")
                    return test_url
        
        # Try more systematically with brand codes and number ranges
        brand_codes = ["DDAC", "BBCA", "CCCH", "AACJ", "CCBK", "LLAC", "GGBJ", "AAZZ", "EEBB"]
        for prefix in brand_codes:
            # Try different number ranges - checking specific ranges
            for num_range in [range(7000, 9500, 100), range(1000, 3000, 100)]:
                for num in num_range:
                    # Try both with and without size suffix
                    for suffix in ["s", ""]:
                        # Try different extensions
                        for ext in ['.jpg', '.png', '.jpeg']:
                            product_code = f"{prefix}{str(num).zfill(7)}{suffix}"
                            test_url = urljoin(base_url, f"/upload/product/simg3/{product_code}{ext}")
                            
                            # Use head request to quickly check if URL exists
                            if await check_image_url_exists(test_url, session):
                                logging.info(f"Korean URL converted using systematic search: {url} → {test_url}")
                                return test_url
        
        # If no pattern matches found, try checking if original URL with different extension works
        url_without_ext = url.rsplit('.', 1)[0]
        for ext in ['.jpg', '.png', '.jpeg']:
            test_url = f"{url_without_ext}{ext}"
            if await check_image_url_exists(test_url, session):
                logging.info(f"Extension changed: {url} → {test_url}")
                return test_url
    
    logging.warning(f"Could not find working URL alternative for: {url}")
    # Return original if no working alternatives found
    return url

def _is_data_missing(series: pd.Series) -> pd.Series:
    """Checks if data in a pandas Series is missing.
    Considers '-', '', None, NaN as missing.
    """
    # Using vectorized operations for efficiency
    is_missing = series.isna() | series.isin(['-', ''])
    return is_missing

def filter_upload_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters DataFrame based on three criteria:
    1. Removes rows where both Koreagift and Naver data are missing
    2. Removes rows where price difference is >= -1 for either Koreagift or Naver
       (keeps only rows where price difference is < -1)
    3. Removes rows with unreliable "front" URLs in the Naver image column
    """
    if df.empty:
        logging.warning("Input DataFrame for upload filtering is empty. Returning empty DataFrame.")
        return df.copy() # Return a copy

    # Check if necessary link columns exist
    required_cols = [KOREAGIFT_LINK_COL, NAVER_LINK_COL]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logging.error(f"Required columns for upload filtering missing: {missing_cols}. Cannot filter.")
        return df.copy() # Return a copy of the original if columns are missing

    # --- 1. Filter based on missing links ---
    # Determine missing status based on link columns
    is_kogift_missing = _is_data_missing(df[KOREAGIFT_LINK_COL])
    is_naver_missing = _is_data_missing(df[NAVER_LINK_COL])
    
    # Identify rows where BOTH sources are missing
    missing_links_mask = is_kogift_missing & is_naver_missing
    
    # --- 2. Filter based on price differences ---
    # Instead of identifying rows to remove, let's identify rows to KEEP
    # Initialize masks for price difference conditions (rows we want to KEEP)
    kogift_keep_mask = pd.Series(True, index=df.index)  # Default: keep all rows
    naver_keep_mask = pd.Series(True, index=df.index)   # Default: keep all rows
    
    # Check Koreagift price difference (if column exists)
    if KOREAGIFT_PRICE_DIFF_COL in df.columns:
        # Convert to numeric, coercing errors to NaN
        kogift_price_diff = pd.to_numeric(df[KOREAGIFT_PRICE_DIFF_COL], errors='coerce')
        # Keep rows where price difference is < -1 OR link is missing
        kogift_keep_mask = (kogift_price_diff < -1) | is_kogift_missing
        
    # Check Naver price difference (if column exists)
    if NAVER_PRICE_DIFF_COL in df.columns:
        # Convert to numeric, coercing errors to NaN
        naver_price_diff = pd.to_numeric(df[NAVER_PRICE_DIFF_COL], errors='coerce')
        # Keep rows where price difference is < -1 OR link is missing
        naver_keep_mask = (naver_price_diff < -1) | is_naver_missing
    
    # --- 3. Filter out rows with "front" URLs in Naver image column ---
    front_url_mask = pd.Series(False, index=df.index)  # Default: keep all rows
    
    # Check if the Naver image column exists
    if '네이버쇼핑(이미지링크)' in df.columns:
        # Create a mask for rows with "front" URLs
        front_url_mask = df['네이버쇼핑(이미지링크)'].astype(str).str.contains('pstatic.net/front/', case=False, na=False)
        
        # Log rows with problematic URLs (for debugging)
        front_url_count = front_url_mask.sum()
        if front_url_count > 0:
            logging.warning(f"Found {front_url_count} rows with unreliable 'front' URLs that will be filtered out")
            for idx in df[front_url_mask].index:
                logging.debug(f"Row {idx}: Filtering out due to 'front' URL: {df.at[idx, '네이버쇼핑(이미지링크)']}")
    
    # Combined mask for rows to keep:
    # 1. Either don't have both links missing AND
    # 2. Satisfy the price difference criteria for both Koreagift and Naver AND
    # 3. Don't have "front" URLs in the Naver image column
    rows_to_keep_mask = (~missing_links_mask) & kogift_keep_mask & naver_keep_mask & (~front_url_mask)

    # Apply the filter to keep only the rows we want
    initial_rows = len(df)
    filtered_df = df[rows_to_keep_mask].copy()
    removed_count = initial_rows - len(filtered_df)
    
    # Log the filtering results
    if removed_count > 0:
        logging.info(f"Upload filter: Removed {removed_count} rows total:")
        missing_links_count = missing_links_mask.sum()
        kogift_filtered = len(df) - kogift_keep_mask.sum()
        naver_filtered = len(df) - naver_keep_mask.sum()
        front_url_filtered = front_url_mask.sum()
        logging.info(f"  - {missing_links_count} rows with both Koreagift and Naver links missing")
        logging.info(f"  - {kogift_filtered} rows with Koreagift price difference >= -1")
        logging.info(f"  - {naver_filtered} rows with Naver price difference >= -1")
        logging.info(f"  - {front_url_filtered} rows with unreliable 'front' URLs in Naver image column")
    else:
        logging.info("Upload filter: No rows removed after applying all filtering criteria.")

    return filtered_df

def filter_front_urls_from_upload_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Specifically filters out rows with unreliable "front" URLs in the Naver image column.
    These URLs (like https://shopping-phinf.pstatic.net/front/...) often fail to load.
    
    Args:
        df: DataFrame to filter
        
    Returns:
        DataFrame with "front" URL rows removed or replaced
    """
    if df.empty:
        logging.warning("Input DataFrame for front URL filtering is empty. Returning empty DataFrame.")
        return df.copy()
    
    # Check if the Naver image column exists
    naver_img_col = '네이버쇼핑(이미지링크)'
    if naver_img_col not in df.columns:
        logging.warning(f"Column '{naver_img_col}' not found in DataFrame. Cannot filter front URLs.")
        return df.copy()
    
    # Create mask for rows with "front" URLs
    front_url_mask = df[naver_img_col].astype(str).str.contains('pstatic.net/front/', case=False, na=False)
    front_url_count = front_url_mask.sum()
    
    if front_url_count > 0:
        logging.warning(f"Found {front_url_count} rows with unreliable 'front' URLs")
        
        # Create a copy of the DataFrame
        result_df = df.copy()
        
        # Replace "front" URLs with empty strings
        result_df.loc[front_url_mask, naver_img_col] = ''
        
        logging.info(f"Removed {front_url_count} 'front' URLs from the upload data")
        return result_df
    else:
        logging.info("No 'front' URLs found in the upload data")
        return df

def apply_filter_to_upload_excel(upload_file_path: str, config: configparser.ConfigParser) -> bool:
    """Reads the upload Excel, applies filtering, and saves it back, overwriting the original.

    Args:
        upload_file_path: Path to the upload .xlsx file.
        config: The configuration object (unused currently, but passed for potential future use).

    Returns:
        True if filtering was applied successfully, False otherwise.
    """
    if not upload_file_path or not isinstance(upload_file_path, str):
        logging.error("Upload file path is invalid. Skipping filtering.")
        return False
    if not os.path.exists(upload_file_path):
        logging.error(f"Upload file does not exist: {upload_file_path}. Skipping filtering.")
        return False

    logging.info(f"Applying post-creation filter to upload file: {upload_file_path}")
    
    try:
        # Read the Excel file into a DataFrame
        import pandas as pd
        df = pd.read_excel(upload_file_path, engine='openpyxl')
        
        # Record original row count
        original_rows = len(df)
        
        # Apply filtering based on criteria
        df_filtered = filter_upload_data(df)
        
        # Also filter out "front" URLs
        df_filtered = filter_front_urls_from_upload_data(df_filtered)
        
        # Calculate rows removed
        rows_removed = original_rows - len(df_filtered)
        
        if rows_removed > 0:
            logging.info(f"Upload filtering removed {rows_removed} rows in total.")
        else:
            logging.info("No rows were removed during upload filtering.")
        
        # Add comment about filtering process to the DataFrame
        # (Not actually adding to the DataFrame, just logging)
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"Upload file filtered at {timestamp}. Original rows: {original_rows}, Final rows: {len(df_filtered)}")

        # Save the filtered data back to the same file path, overwriting it.
        # Use 'openpyxl' engine to ensure compatibility with .xlsx format features.
        df_filtered.to_excel(upload_file_path, index=False, engine='openpyxl')

        logging.info(f"Successfully filtered and overwrote upload file: {upload_file_path}")
        return True
    except FileNotFoundError:
         logging.error(f"Upload file not found during filtering read: {upload_file_path}")
         return False
    except ImportError:
         logging.error("`openpyxl` is required to read/write Excel .xlsx files. Please install it (`pip install openpyxl`).")
         return False
    except Exception as e:
        logging.error(f"Error filtering or saving upload Excel file {upload_file_path}: {e}", exc_info=True)
        return False

async def test_haereum_urls(urls):
    """Test function to check and fix Haereum URLs.
    
    Args:
        urls: List of URLs to test and fix
        
    Returns:
        Dictionary mapping original URLs to fixed URLs
    """
    results = {}
    valid_count = 0
    
    print(f"Testing {len(urls)} Haereum URLs...")
    print("-" * 80)
    
    for i, url in enumerate(urls, 1):
        print(f"[{i}/{len(urls)}] Testing: {url}")
        fixed_url = await convert_haereum_url(url)
        results[url] = fixed_url
        
        # Check if the fixed URL is valid
        is_valid = await check_image_url_exists(fixed_url)
        status = "✅ VALID" if is_valid else "❌ INVALID"
        if is_valid:
            valid_count += 1
            
        print(f"{status}: {url}")
        print(f"→ {fixed_url}")
        print("-" * 80)
    
    print(f"\nSummary: {valid_count}/{len(urls)} URLs valid after conversion")
    return results

async def fix_haereum_urls_in_excel(excel_path, config=None):
    """Fix Haereum URLs with Korean text in a single Excel file.
    
    Args:
        excel_path: Path to the Excel file
        config: ConfigParser object (optional)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logging.info(f"Processing Excel file: {excel_path}")
        
        # Read the Excel file
        df = pd.read_excel(excel_path, engine='openpyxl')
        
        if KOREAGIFT_LINK_COL not in df.columns:
            logging.warning(f"No Haereum link column found in {excel_path}")
            return False
            
        # Find rows with Korean text in Haereum URLs
        korean_url_mask = df[KOREAGIFT_LINK_COL].astype(str).str.contains(
            r'/upload/product/simg3/.*[가-힣].*\.gif', 
            regex=True, 
            na=False
        )
        
        korean_url_count = korean_url_mask.sum()
        if korean_url_count == 0:
            logging.info(f"No Korean Haereum URLs found in {excel_path}")
            return True
            
        logging.info(f"Found {korean_url_count} Haereum URLs with Korean text that need fixing")
        
        # Get the URLs that need fixing
        korean_urls = df.loc[korean_url_mask, KOREAGIFT_LINK_COL].tolist()
        
        # Process the URLs
        results = {}
        for url in korean_urls:
            fixed_url = await convert_haereum_url(url)
            results[url] = fixed_url
        
        # Update the DataFrame
        fixed_count = 0
        for old_url, new_url in results.items():
            if old_url != new_url and new_url is not None:
                logging.info(f"Replacing URL: {old_url} → {new_url}")
                df[KOREAGIFT_LINK_COL] = df[KOREAGIFT_LINK_COL].replace(old_url, new_url)
                fixed_count += 1
        
        # Save the updated DataFrame
        df.to_excel(excel_path, index=False, engine='openpyxl')
        logging.info(f"Fixed {fixed_count} URLs in {excel_path}")
        
        return True
    except Exception as e:
        logging.error(f"Error processing {excel_path}: {e}")
        return False

async def batch_fix_haereum_urls(directory, pattern="*.xlsx", config=None):
    """Fix Haereum URLs with Korean text in all Excel files in a directory.
    
    Args:
        directory: Directory containing Excel files
        pattern: File pattern to match (default: "*.xlsx")
        config: ConfigParser object (optional)
    
    Returns:
        Dict with results for each file
    """
    import glob
    import os
    
    results = {}
    
    # Find all Excel files in the directory
    excel_files = glob.glob(os.path.join(directory, pattern))
    
    if not excel_files:
        logging.warning(f"No Excel files found in {directory} matching pattern {pattern}")
        return results
    
    logging.info(f"Found {len(excel_files)} Excel files to process")
    
    # Process each file
    for excel_path in excel_files:
        success = await fix_haereum_urls_in_excel(excel_path, config)
        results[excel_path] = success
    
    # Print summary
    success_count = sum(1 for success in results.values() if success)
    logging.info(f"Successfully processed {success_count}/{len(excel_files)} Excel files")
    
    return results

def show_help():
    """Display help information for using this tool."""
    print("\nHaereum URL Converter Tool")
    print("========================\n")
    print("This tool helps fix Korean text URLs from Haereum by converting them to")
    print("proper product code URLs with supported file extensions.\n")
    print("Usage:")
    print("  python upload_filter.py                        - Test default URLs")
    print("  python upload_filter.py <url1> <url2> ...      - Test specific URLs")
    print("  python upload_filter.py --batch <directory>    - Process all Excel files in directory")
    print("  python upload_filter.py --batch <directory> <pattern>")
    print("                                                 - Process Excel files matching pattern")
    print("  python upload_filter.py --help                 - Show this help message\n")
    print("Examples:")
    print("  python upload_filter.py --batch C:\\RPA\\Excel")
    print("  python upload_filter.py --batch C:\\RPA\\Excel \"*Product*.xlsx\"\n")

if __name__ == "__main__":
    # This will run if the script is executed directly
    import sys
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    # Example URLs to test (or use command line arguments)
    test_urls = [
        "https://www.jclgift.com/upload/product/simg3/페이즐리반다나손수건스카프54cm대형.gif",
        "https://www.jclgift.com/upload/product/simg3/메쉬가방중형32X33X12비치가방망사.gif",
        "https://www.jclgift.com/upload/product/simg3/손톱깎이세트선물세트네일세트12p.gif",
        "https://www.jclgift.com/upload/product/simg3/고급3단자동양우산10k.gif",
        "https://www.jclgift.com/upload/product/simg3/메쉬가방대형비치백망사가방비치가방43X.gif"
    ]
    
    # Check command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] in ["--help", "-h", "/?"]:
            show_help()
        elif sys.argv[1] == "--batch" and len(sys.argv) > 2:
            # Batch process all Excel files in the specified directory
            directory = sys.argv[2]
            pattern = sys.argv[3] if len(sys.argv) > 3 else "*.xlsx"
            print(f"Batch processing Excel files in {directory} matching {pattern}")
            asyncio.run(batch_fix_haereum_urls(directory, pattern))
        else:
            # Use command line arguments as URLs to test
            test_urls = sys.argv[1:]
            print(f"Starting URL conversion test for {len(test_urls)} Haereum URLs...")
            asyncio.run(test_haereum_urls(test_urls))
    else:
        # Run the test with default URLs
        print(f"Starting URL conversion test for {len(test_urls)} Haereum URLs...")
        asyncio.run(test_haereum_urls(test_urls)) 