import os
import sys
import logging
import glob
import pandas as pd
import argparse
from datetime import datetime
import traceback
import shutil
from pathlib import Path

# Import our fix script
try:
    from kogift_image_fix import fix_excel_kogift_images, download_image, extract_id_from_url
except ImportError:
    print("Error: Could not import kogift_image_fix module. Make sure it exists in the same directory.")
    sys.exit(1)

# Set up logging
log_filename = f"fix_result_files_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_filename)
    ]
)

def backup_file(file_path):
    """Create a backup of the file before modifying it"""
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return False
        
    backup_path = f"{file_path}.bak"
    try:
        shutil.copy2(file_path, backup_path)
        logging.info(f"Backed up file to: {backup_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to backup file: {e}")
        return False

def find_result_files(directory, days=None, pattern="*result*.xlsx"):
    """Find result Excel files in the given directory"""
    logging.info(f"Searching for result files in: {directory}")
    
    # Get all files matching the pattern
    file_pattern = os.path.join(directory, pattern)
    files = glob.glob(file_pattern)
    
    # Filter by modification date if days is specified
    if days is not None:
        cutoff_time = datetime.now().timestamp() - (days * 24 * 60 * 60)
        files = [f for f in files if os.path.getmtime(f) >= cutoff_time]
    
    logging.info(f"Found {len(files)} result files")
    return files

def process_files(files, dry_run=False):
    """Process all result files to fix Kogift images"""
    results = {
        'total': len(files),
        'processed': 0,
        'success': 0,
        'failed': 0,
        'skipped': 0
    }
    
    for file_path in files:
        logging.info(f"Processing file: {file_path}")
        
        if dry_run:
            logging.info(f"DRY RUN: Would process {file_path}")
            results['skipped'] += 1
            continue
            
        # Backup the file
        if not backup_file(file_path):
            logging.error(f"Skipping file due to backup failure: {file_path}")
            results['failed'] += 1
            continue
            
        try:
            # Apply the fix
            success = fix_excel_kogift_images(file_path)
            results['processed'] += 1
            
            if success:
                results['success'] += 1
                logging.info(f"Successfully fixed: {file_path}")
            else:
                results['failed'] += 1
                logging.error(f"Failed to fix: {file_path}")
                
        except Exception as e:
            results['failed'] += 1
            logging.error(f"Error processing file {file_path}: {e}")
            logging.error(traceback.format_exc())
    
    return results

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Fix Kogift images in result Excel files")
    parser.add_argument("--dir", "-d", default="C:\\RPA\\Output", help="Directory containing result files")
    parser.add_argument("--days", "-D", type=int, help="Only process files modified in the last N days")
    parser.add_argument("--pattern", "-p", default="*result*.xlsx", help="File pattern to match")
    parser.add_argument("--dry-run", action="store_true", help="Dry run, don't actually modify files")
    parser.add_argument("--file", "-f", help="Process a single file instead of searching a directory")
    
    args = parser.parse_args()
    
    logging.info("Starting Kogift image fix for result files")
    logging.info(f"Args: {args}")
    
    if args.file:
        # Process a single file
        if not os.path.exists(args.file):
            logging.error(f"File not found: {args.file}")
            return 1
            
        files = [args.file]
    else:
        # Process files in directory
        if not os.path.exists(args.dir):
            logging.error(f"Directory not found: {args.dir}")
            return 1
            
        files = find_result_files(args.dir, args.days, args.pattern)
        
    if not files:
        logging.info("No files found matching criteria")
        return 0
        
    # Process the files
    results = process_files(files, args.dry_run)
    
    # Print summary
    logging.info("=" * 50)
    logging.info("Processing Summary")
    logging.info("=" * 50)
    logging.info(f"Total files:       {results['total']}")
    logging.info(f"Processed:         {results['processed']}")
    logging.info(f"Successfully fixed: {results['success']}")
    logging.info(f"Failed:            {results['failed']}")
    logging.info(f"Skipped (dry run): {results['skipped']}")
    logging.info("=" * 50)
    
    return 0
    
if __name__ == "__main__":
    sys.exit(main()) 