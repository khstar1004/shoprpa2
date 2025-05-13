from openpyxl.styles import Alignment, Border, Side, Font, PatternFill

# --- Styling Constants ---
HEADER_FILL = PatternFill(start_color="E2EFDA", end_color="E2EFDA", fill_type="solid")  # Light green fill
HEADER_FONT = Font(bold=True, size=11, name='맑은 고딕')
HEADER_ALIGNMENT = Alignment(horizontal="center", vertical="center", wrap_text=True)

# Define alignments based on column type
LEFT_ALIGNMENT = Alignment(horizontal="left", vertical="center", wrap_text=True)
CENTER_ALIGNMENT = Alignment(horizontal="center", vertical="center", wrap_text=True)
RIGHT_ALIGNMENT = Alignment(horizontal="right", vertical="center", wrap_text=False)  # Numbers right-aligned

DEFAULT_FONT = Font(name='맑은 고딕', size=10)

THIN_BORDER_SIDE = Side(style='thin')
DEFAULT_BORDER = Border(left=THIN_BORDER_SIDE, right=THIN_BORDER_SIDE, top=THIN_BORDER_SIDE, bottom=THIN_BORDER_SIDE)

LINK_FONT = Font(color="0000FF", underline="single", name='맑은 고딕', size=10)
INVALID_LINK_FONT = Font(color="FF0000", name='맑은 고딕', size=10)  # Red for invalid links

NEGATIVE_PRICE_FILL = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")  # Yellow fill for negative diff < -1

# Dedicated styling for upload file format
UPLOAD_HEADER_FILL = PatternFill(start_color="D9D9D9", end_color="D9D9D9", fill_type="solid")  # Gray background
UPLOAD_HEADER_HEIGHT = 34.5
UPLOAD_DATA_ROW_HEIGHT = 16.9
UPLOAD_COLUMN_DEFAULT_WIDTH = 7

# Column width settings for different column types
COLUMN_WIDTH_SETTINGS = {
    'image': 21.44,      # Image columns
    'name': 45,          # Product name
    'link': 35,          # URLs and links
    'price': 14,         # Price columns
    'percent': 10,       # Percentage columns
    'quantity': 10,      # Quantity columns
    'code': 12,          # Code columns
    'category': 20,      # Category columns
    'text_short': 7,     # Short text columns (구분, 담당자 등)
    'default': 15        # Default width
}

# Result file specific formatting
RESULT_COLUMN_DEFAULT_WIDTH = 20
RESULT_ROW_HEIGHT = 380  # Row height for image rows
RESULT_IMAGE_WIDTH = 160  # Width of embedded images
RESULT_IMAGE_HEIGHT = 160  # Height of embedded images

# Upload file specific formatting
UPLOAD_HEADER_HEIGHT = 34.5
UPLOAD_DATA_ROW_HEIGHT = 16.9
UPLOAD_COLUMN_DEFAULT_WIDTH = 7 