import asyncio
import logging
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup
import pandas as pd
from typing import Union, Optional, Dict, List, Tuple, Any

# Global variable to capture dialog messages (consider passing state differently if needed)
dialog_message_capture = ""

async def handle_dialog(dialog):
    """Handles unexpected dialogs/alerts during navigation."""
    global dialog_message_capture
    dialog_message_capture = dialog.message
    logging.warning(f"UPrice Scraper: Dialog encountered - '{dialog.message}'")
    await dialog.accept()

def _clean_quantity(qty_str):
    """Cleans quantity strings, handling '미만' and removing non-digits."""
    if not isinstance(qty_str, str):
        qty_str = str(qty_str)
    if '미만' in qty_str:
        return 0 # Treat 'less than X' as minimum quantity 0 or 1 depending on logic
    return int("".join(filter(str.isdigit, qty_str))) if qty_str else 0

def _clean_price(price_str):
    """Cleans price strings, removing non-digits."""
    if not isinstance(price_str, str):
        price_str = str(price_str)
    return int("".join(filter(str.isdigit, price_str))) if price_str else 0

def _parse_table_type1(soup):
    """Parses table structures like Type 1/2/5/6 (VAT exclusive)."""
    try:
        tables = soup.find_all('table')
        if not tables: return None
        df = pd.read_html(str(tables[0]), header=0)[0] # Assume header is row 0

        # Handle potential multi-level headers or transposed tables
        if '수량' not in df.columns and '수량' in df.iloc[0].to_list():
            df = df.T
            df.columns = df.iloc[0]
            df = df[1:].reset_index(drop=True)

        if '수량' not in df.columns or '일반' not in df.columns:
             logging.warning("UPrice Parser: Type 1/2/5/6 table structure unexpected. Missing '수량' or '일반'.")
             return None

        # Keep only necessary columns and rename
        df = df[['수량', '일반']].copy()
        df.columns = ['quantity', 'price_no_vat']

        df['quantity'] = df['quantity'].apply(_clean_quantity)
        df['price_no_vat'] = df['price_no_vat'].apply(_clean_price)
        df['price_vat'] = (df['price_no_vat'] * 1.1).round().astype(int)
        df = df.sort_values(by='quantity').reset_index(drop=True)
        return df
    except Exception as e:
        logging.error(f"UPrice Parser: Error parsing Type 1/2/5/6 table: {e}", exc_info=True)
        return None

def _parse_table_type3(soup):
    """Parses table structure Type 3 (VAT exclusive, input fields)."""
    try:
        quantities = [int(input_tag['value']) for input_tag in soup.find_all('input', class_='qu')]
        prices = [int(input_tag['value'].replace(',', '')) for input_tag in soup.find_all('input', class_='pr')]

        if not quantities or not prices or len(quantities) != len(prices):
            logging.warning("UPrice Parser: Type 3 structure unexpected. Mismatched quantities/prices.")
            return None

        df = pd.DataFrame({'quantity': quantities, 'price_no_vat': prices})
        df['price_vat'] = (df['price_no_vat'] * 1.1).round().astype(int)
        df = df.sort_values(by='quantity').reset_index(drop=True)
        return df
    except Exception as e:
        logging.error(f"UPrice Parser: Error parsing Type 3 table: {e}", exc_info=True)
        return None

# Map XPaths to parsing functions - Prioritize more specific paths
# These XPaths need verification against actual target site structures
XPATH_TO_PARSER_MAP = {
    '//table[@class = "goods_option"]//td[@colspan = "4"]': _parse_table_type3, # Type 3 (more specific)
    '//table[@class = "hompy1004_table_class hompy1004_table_list"]/ancestor::td[1]': _parse_table_type1, # Type 2 (more specific)
    '//div[@class = "price-box"]': _parse_table_type1, # Type 1
    '//div[@class = "tbl02"]': _parse_table_type1, # Type 1 variation?
    '//div[@class = "vi_info"]//div[@class = "tbl_frm01"]': _parse_table_type1, # Type 5
    '//div[@class = "specArea"]//div[@class = "w100"]': _parse_table_type1, # Type 6
    # Add more specific XPaths or parsing functions as needed
}

async def scrape_unit_price(initial_url: str, config: dict) -> Union[pd.DataFrame, None]:
    """
    Scrapes unit price information from a given URL using Playwright.
    Handles Naver redirects and attempts to parse known table structures.

    Args:
        initial_url: The starting URL (can be a direct link or Naver redirect).
        config: Dictionary containing configuration (timeouts, headless mode).

    Returns:
        A Pandas DataFrame with columns ['quantity', 'price_no_vat', 'price_vat']
        sorted by quantity, or None if scraping fails or no data is found.
    """
    global dialog_message_capture
    dialog_message_capture = "" # Reset dialog message capture
    playwright = None
    browser = None
    page = None
    final_df = None
    
    headless_mode = config.get('headless_playwright', True) # Default to headless
    timeout = config.get('playwright_timeout', 30000) # Page load timeout in ms

    logging.info(f"UPrice Scraper: Starting scrape for URL: {initial_url}")

    try:
        playwright = await async_playwright().start()
        browser = await playwright.chromium.launch(headless=headless_mode)
        context = await browser.new_context()
        page = await context.new_page()
        page.on("dialog", handle_dialog)

        # --- Navigation ---
        try:
            await page.goto(initial_url, wait_until='networkidle', timeout=timeout)
            logging.info(f"UPrice Scraper: Initial load successful: {page.url}")
            
            # Handle potential Naver redirects ("최저가 사러가기")
            if "shopping.naver.com" in page.url:
                 # Try finding the "최저가 사러가기" link specifically
                lowest_price_link_selector = '//div[contains(@class, "lowestPrice_btn_box")]//a[contains(text(),"최저가") and contains(text(),"사러가기")]'
                link_element = page.locator(lowest_price_link_selector).first
                
                if await link_element.is_visible(timeout=5000):
                    href = await link_element.get_attribute('href')
                    if href:
                        logging.info(f"UPrice Scraper: Found Naver '최저가 사러가기' link. Navigating to: {href}")
                        await page.goto(href, wait_until='networkidle', timeout=timeout)
                        logging.info(f"UPrice Scraper: Navigation after Naver redirect successful: {page.url}")
                    else:
                         logging.warning("UPrice Scraper: Found Naver '최저가 사러가기' element but no href.")
                else:
                     logging.info("UPrice Scraper: Naver page detected, but '최저가 사러가기' link not found/visible. Assuming current page is target or structure changed.")


            # Wait briefly for any final redirects or dynamic content loading after navigation
            await asyncio.sleep(2)
            final_url = page.url
            logging.info(f"UPrice Scraper: Final URL after navigation: {final_url}")

            # Check for dialog messages indicating issues (e.g., out of stock)
            if dialog_message_capture and ('상품' in dialog_message_capture or '재고' in dialog_message_capture or '품절' in dialog_message_capture):
                 logging.warning(f"UPrice Scraper: Detected potential out-of-stock dialog: '{dialog_message_capture}'. Aborting scrape for {final_url}")
                 return None # Indicate failure due to dialog

        except PlaywrightTimeoutError:
            logging.error(f"UPrice Scraper: Timeout loading URL: {initial_url} or subsequent navigation.")
            return None
        except Exception as nav_err:
            logging.error(f"UPrice Scraper: Error during navigation from {initial_url}: {nav_err}", exc_info=True)
            return None

        # --- Content Parsing ---
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        # Iterate through known XPath patterns to find and parse the table
        for xpath, parser_func in XPATH_TO_PARSER_MAP.items():
            # Use Playwright's locator to check element existence first (more reliable for dynamic content)
            element_exists = await page.locator(f"xpath={xpath}").first.is_visible(timeout=2000) # Short timeout check
            if element_exists:
                # If element exists via Playwright, get its HTML for BS4 parsing
                try:
                     element_html = await page.locator(f"xpath={xpath}").first.inner_html(timeout=5000)
                     element_soup = BeautifulSoup(element_html, 'html.parser')
                     logging.info(f"UPrice Scraper: Found matching element with XPath: {xpath}. Attempting parse...")
                     final_df = parser_func(element_soup)
                     if final_df is not None and not final_df.empty:
                         logging.info(f"UPrice Scraper: Successfully parsed unit price table using pattern for XPath: {xpath}")
                         break # Stop searching once a table is successfully parsed
                     else:
                         logging.warning(f"UPrice Scraper: Parser function for XPath '{xpath}' returned None or empty DataFrame.")
                except Exception as parse_locator_err:
                     logging.error(f"UPrice Scraper: Error locating/getting HTML or parsing for XPath '{xpath}': {parse_locator_err}", exc_info=True)
            # else:
            #     logging.debug(f"UPrice Scraper: XPath not found or not visible: {xpath}")


        if final_df is None or final_df.empty:
            logging.warning(f"UPrice Scraper: Could not find or parse any known unit price table structure for URL: {final_url}")
            # Optionally save HTML for debugging:
            # with open(f"debug_uprice_{hashlib.md5(final_url.encode()).hexdigest()}.html", "w", encoding="utf-8") as f:
            #     f.write(content)
            return None

        return final_df

    except Exception as e:
        logging.error(f"UPrice Scraper: General error during scraping {initial_url}: {e}", exc_info=True)
        return None
    finally:
        if page: await page.close()
        if browser: await browser.close()
        if playwright: await playwright.stop()
        logging.info(f"UPrice Scraper: Finished scrape attempt for URL: {initial_url}")

# Example usage (for testing purposes)
async def _test_main():
    # Example URL (replace with actual test URLs)
    # test_url_naver = "https://search.shopping.naver.com/catalog/47861603392"
    test_url_direct = "https://koreagift.com/ez/mall.php?cat=001001000&keyword=&sh=list&list_ord=4&p_idx=219840" # Replace with a direct site URL
    test_config = {'headless_playwright': False, 'playwright_timeout': 30000}

    logging.basicConfig(level=logging.INFO) # Setup logging for testing
    
    # print(f"--- Testing Direct URL: {test_url_direct} ---")
    # df_direct = await scrape_unit_price(test_url_direct, test_config)
    # if df_direct is not None:
    #     print("Direct URL Result:")
    #     print(df_direct)
    # else:
    #     print("Direct URL Scrape Failed or No Data Found.")

    # print(f"\n--- Testing Naver URL: {test_url_naver} ---")
    # df_naver = await scrape_unit_price(test_url_naver, test_config)
    # if df_naver is not None:
    #     print("Naver URL Result:")
    #     print(df_naver)
    # else:
    #     print("Naver URL Scrape Failed or No Data Found.")


# if __name__ == "__main__":
#    asyncio.run(_test_main()) 