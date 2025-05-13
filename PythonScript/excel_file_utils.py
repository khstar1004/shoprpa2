import os
import logging
from datetime import datetime
from typing import Optional, Tuple
import re

# Initialize logger
logger = logging.getLogger(__name__)

def generate_file_path(base_path: str, file_type: str, source_info: str, row_count: int, mgmt_type: str) -> str:
    """Generate appropriate file path based on type."""
    dir_path = os.path.dirname(base_path)
    date_part = datetime.now().strftime("%Y%m%d")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Ensure source_info is not empty and row_count is valid
    if not source_info or source_info == "Unknown":
        source_info = "쇼핑RPA" 
    
    # Ensure row_count is positive
    if row_count <= 0:
        # Try to get the actual row count from the filename if it contains the pattern
        match = re.search(r'\((\d+)개\)', os.path.basename(base_path))
        if match:
            try:
                row_count = int(match.group(1))
            except ValueError:
                row_count = 1
        else:
            row_count = 1
    
    # Format: {company}({count})-{mgmt_type}-{date}_{type}_{timestamp}.xlsx
    if file_type == 'result':
        filename = f"{source_info}({row_count}개)-{mgmt_type}-{date_part}_result_{timestamp}.xlsx"
    elif file_type == 'upload':
        filename = f"{source_info}({row_count}개)-{mgmt_type}-{date_part}_upload_{timestamp}.xlsx"
    else:
        filename = os.path.basename(base_path)
        
    return os.path.join(dir_path, filename)

def get_source_info(df) -> Tuple[str, str, int]:
    """Get source info, management type and row count from DataFrame."""
    source_info = "쇼핑RPA"  # Default company name
    mgmt_type = "승인관리"  # Default type
    row_count = len(df)

    try:
        # Get management type from '구분' column
        if '구분' in df.columns and not df['구분'].empty:
            most_common_type = df['구분'].value_counts().idxmax()
            if most_common_type == 'A':
                mgmt_type = "승인관리"
            elif most_common_type == 'P':
                mgmt_type = "가격관리"
            else:
                mgmt_type = str(most_common_type)

        # Try multiple columns for company name, in order of preference
        company_cols = ['업체명', '공급사명', '담당자']
        
        for col in company_cols:
            if col in df.columns and not df[col].empty:
                # Filter out None, NaN, empty strings
                valid_values = df[col].dropna()
                valid_values = valid_values[valid_values.astype(str).str.strip() != '']
                
                if not valid_values.empty:
                    # Get most common non-empty value
                    company_counts = valid_values.value_counts()
                    source_info = company_counts.index[0]
                    break
    
    except Exception as e:
        logger.warning(f"Error getting source info: {e}")

    # Final validation
    if not source_info or source_info.strip() == '':
        source_info = "쇼핑RPA"
    
    return source_info, mgmt_type, row_count 