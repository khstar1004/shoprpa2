import os
import logging
from datetime import datetime
from typing import Optional, Tuple

# Initialize logger
logger = logging.getLogger(__name__)

def generate_file_path(base_path: str, file_type: str, source_info: str, row_count: int, mgmt_type: str) -> str:
    """Generate appropriate file path based on type."""
    dir_path = os.path.dirname(base_path)
    date_part = datetime.now().strftime("%Y%m%d")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
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
    source_info = "Unknown"
    mgmt_type = "승인관리"  # Default type
    row_count = len(df)

    try:
        # Get management type from '구분' column
        if '구분' in df.columns:
            source_val = df['구분'].iloc[0]
            if source_val == 'A':
                mgmt_type = "승인관리"
            elif source_val == 'P':
                mgmt_type = "가격관리"
            else:
                mgmt_type = str(source_val)

        # Get company name from '업체명' or '공급사명' column
        if '업체명' in df.columns:
            company_counts = df['업체명'].value_counts()
            if not company_counts.empty:
                source_info = company_counts.index[0]
        elif '공급사명' in df.columns:
            company_counts = df['공급사명'].value_counts()
            if not company_counts.empty:
                source_info = company_counts.index[0]
    except Exception as e:
        logger.warning(f"Error getting source info: {e}")

    return source_info, mgmt_type, row_count 