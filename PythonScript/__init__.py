"""
ShopRPA Excel Utilities - Refactored for better maintainability
"""

# Import and expose the main functions
from excel_utils import (
    create_excel_output,
    apply_excel_styles,
    apply_excel_formatting,
    finalize_dataframe_for_excel,
    find_excel_file
)

# Constants that might be useful to expose
from excel_constants import (
    FINAL_COLUMN_ORDER,
    UPLOAD_COLUMN_ORDER,
    COLUMN_RENAME_MAP,
    COLUMN_MAPPING_FINAL_TO_UPLOAD,
    IMAGE_COLUMNS
)

__version__ = "2.0.0" 