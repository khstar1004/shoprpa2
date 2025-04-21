import sys
import os
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                           QHBoxLayout, QPushButton, QLabel, QFileDialog, 
                           QProgressBar, QComboBox, QMessageBox, QTabWidget,
                           QTextEdit, QGroupBox, QGridLayout, QSpinBox)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QSize
from PyQt6.QtGui import QIcon, QPixmap
from PyQt6.QtSvg import QSvgRenderer
from PyQt6.QtSvgWidgets import QSvgWidget
import pandas as pd
import configparser
import logging
from datetime import datetime
import traceback
import asyncio

# Import your existing modules
from excel_utils import create_final_output_excel, filter_dataframe
from matching_logic import process_matching
from data_processing import process_input_data, process_input_file
from utils import setup_logging, load_config
from main_rpa import main, initialize_environment

class WorkerThread(QThread):
    progress = pyqtSignal(str, str)  # (type, message)
    finished = pyqtSignal(bool, str)  # (success, output_path)
    
    def __init__(self, config_path):
        super().__init__()
        self.config_path = config_path
        
    def run(self):
        try:
            # Initialize environment
            CONFIG, gpu_available_detected, validation_passed = initialize_environment(self.config_path)
            
            if not validation_passed:
                self.progress.emit("error", "Environment validation failed")
                self.finished.emit(False, "")
                return

            # Set up progress queue
            self.progress_queue = self.progress

            # Run RPA process
            asyncio.run(main(config=CONFIG, gpu_available=gpu_available_detected, progress_queue=self.progress_queue))
            
        except Exception as e:
            self.progress.emit("error", f"An error occurred: {str(e)}")
            self.finished.emit(False, "")
        finally:
            self.progress.emit("finished", "True")

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("ShopRPA - 가격 비교 자동화 시스템")
        self.setMinimumSize(800, 600)
        
        # Load config using the utility function
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.ini')
        try:
            self.config = load_config(config_path)
            if not self.config.sections():
                QMessageBox.warning(self, "설정 오류", "설정 파일을 불러올 수 없습니다. 기본 설정을 사용합니다.")
                self.config = configparser.ConfigParser()
        except Exception as e:
            QMessageBox.warning(self, "설정 오류", f"설정 파일 로드 중 오류 발생: {str(e)}\n기본 설정을 사용합니다.")
            self.config = configparser.ConfigParser()
        
        # Setup logging
        try:
            setup_logging(self.config)
        except Exception as e:
            QMessageBox.warning(self, "로깅 오류", f"로깅 설정 중 오류 발생: {str(e)}")
            # Setup basic logging as fallback
            logging.basicConfig(level=logging.INFO)
        
        # Load SVG icons
        self.load_icons()
        
        # Create main widget and layout
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QVBoxLayout(main_widget)
        
        # Create tabs
        tabs = QTabWidget()
        layout.addWidget(tabs)
        
        # Add tabs
        tabs.addTab(self.create_process_tab(), "가격 비교")
        tabs.addTab(self.create_settings_tab(), "설정")
        tabs.addTab(self.create_help_tab(), "도움말")
        
        # Set window icon if available
        icon_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'assets', 'app_icon.png')
        if os.path.exists(icon_path):
            self.setWindowIcon(QIcon(icon_path))
        
        logging.info("MainWindow initialized successfully")
        
    def load_icons(self):
        """Load SVG icons with proper error handling."""
        try:
            # Get assets directory path - use absolute path from workspace root
            assets_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'assets')
            
            # Define icon paths
            icon_paths = {
                'file': os.path.join(assets_dir, 'file.svg'),
                'batch': os.path.join(assets_dir, 'batch.svg'),
                'settings': os.path.join(assets_dir, 'settings.svg'),
                'help': os.path.join(assets_dir, 'help.svg'),
                'loading': os.path.join(assets_dir, 'loading.svg'),
                'logo': os.path.join(assets_dir, 'logo.svg')
            }
            
            # Create SVG widgets with error handling
            self.icons = {}
            for name, path in icon_paths.items():
                if os.path.exists(path):
                    try:
                        icon_widget = QSvgWidget(path)
                        icon_widget.setFixedSize(24, 24)  # Set consistent size
                        self.icons[name] = icon_widget
                    except Exception as e:
                        logging.warning(f"Failed to load SVG icon {name} from {path}: {e}")
                else:
                    logging.warning(f"Icon file not found: {path}")
            
            logging.info(f"Successfully loaded {len(self.icons)} icons")
            
        except Exception as e:
            logging.error(f"Error during icon loading: {e}")
            self.icons = {}  # Empty dict as fallback
        
    def create_process_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Header with logo
        header = QHBoxLayout()
        if 'logo' in self.icons:
            logo = self.icons['logo']
            logo.setFixedSize(200, 50)
            header.addWidget(logo)
        header.addStretch()
        layout.addLayout(header)
        
        # File selection
        file_group = QGroupBox("입력 파일")
        file_layout = QGridLayout()
        
        self.file_label = QLabel("선택된 파일: 없음")
        file_layout.addWidget(self.file_label, 0, 0, 1, 2)
        
        browse_btn = QPushButton("파일 선택")
        if 'file' in self.icons:
            browse_btn.setIcon(QIcon(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'assets', 'file.svg')))
        browse_btn.clicked.connect(self.browse_file)
        file_layout.addWidget(browse_btn, 1, 0)
        
        file_group.setLayout(file_layout)
        layout.addWidget(file_group)
        
        # Process settings
        settings_group = QGroupBox("처리 설정")
        settings_layout = QGridLayout()
        
        # Process type selection
        settings_layout.addWidget(QLabel("처리 유형:"), 0, 0)
        self.process_type = QComboBox()
        self.process_type.addItems(["승인관리 (A)", "가격관리 (P)"])
        settings_layout.addWidget(self.process_type, 0, 1)
        
        # Batch size selection
        settings_layout.addWidget(QLabel("배치 크기:"), 1, 0)
        self.batch_size = QSpinBox()
        self.batch_size.setRange(1, 1000)
        self.batch_size.setValue(50)
        settings_layout.addWidget(self.batch_size, 1, 1)
        
        settings_group.setLayout(settings_layout)
        layout.addWidget(settings_group)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        layout.addWidget(self.progress_bar)
        
        # Start button
        self.start_btn = QPushButton("처리 시작")
        if 'batch' in self.icons:
            self.start_btn.setIcon(QIcon(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'assets', 'batch.svg')))
        self.start_btn.clicked.connect(self.start_processing)
        layout.addWidget(self.start_btn)
        
        # Status text area
        self.status_text = QTextEdit()
        self.status_text.setReadOnly(True)
        layout.addWidget(self.status_text)
        
        return tab
        
    def create_settings_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Add settings controls here
        settings_group = QGroupBox("시스템 설정")
        settings_layout = QGridLayout()
        
        # Add your settings controls here
        # Example:
        settings_layout.addWidget(QLabel("로그 레벨:"), 0, 0)
        log_level = QComboBox()
        log_level.addItems(["DEBUG", "INFO", "WARNING", "ERROR"])
        settings_layout.addWidget(log_level, 0, 1)
        
        settings_group.setLayout(settings_layout)
        layout.addWidget(settings_group)
        
        return tab
        
    def create_help_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Help content
        help_text = QTextEdit()
        help_text.setReadOnly(True)
        help_text.setHtml("""
        <h2>ShopRPA 사용 설명서</h2>
        
        <h3>1. 기본 사용법</h3>
        <p>1) '파일 선택' 버튼을 클릭하여 가격 비교를 수행할 엑셀 파일을 선택합니다.</p>
        <p>2) 처리 유형을 선택합니다 (승인관리 또는 가격관리).</p>
        <p>3) 배치 크기를 설정합니다 (기본값: 50).</p>
        <p>4) '처리 시작' 버튼을 클릭하여 작업을 시작합니다.</p>
        
        <h3>2. 처리 유형 설명</h3>
        <p><b>승인관리 (A):</b> 상품 승인을 위한 가격 비교를 수행합니다.</p>
        <p><b>가격관리 (P):</b> 일반적인 가격 비교를 수행합니다.</p>
        
        <h3>3. 결과 파일</h3>
        <p>처리가 완료되면 입력 파일과 동일한 폴더에 결과 파일이 생성됩니다.</p>
        <p>파일명 형식: output_YYYYMMDD_HHMMSS.xlsx</p>
        
        <h3>4. 주의사항</h3>
        <p>- 입력 파일은 반드시 지정된 형식을 따라야 합니다.</p>
        <p>- 대량의 데이터를 처리할 때는 배치 크기를 적절히 조정하세요.</p>
        <p>- 처리 중에는 프로그램을 종료하지 마세요.</p>
        """)
        layout.addWidget(help_text)
        
        return tab
        
    def browse_file(self):
        file_name, _ = QFileDialog.getOpenFileName(
            self,
            "엑셀 파일 선택",
            "",
            "Excel Files (*.xlsx *.xls)"
        )
        if file_name:
            self.file_label.setText(f"선택된 파일: {os.path.basename(file_name)}")
            self.input_file = file_name
            
    def start_processing(self):
        """Start the RPA process"""
        try:
            # Disable start button
            self.start_btn.setEnabled(False)
            
            # Clear status text
            self.status_text.clear()
            
            # Get config path
            config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.ini')
            
            # Create and start worker thread
            self.worker = WorkerThread(config_path)
            self.worker.progress.connect(self.update_progress)
            self.worker.finished.connect(self.processing_finished)
            self.worker.start()
            
        except Exception as e:
            QMessageBox.critical(self, "오류", f"처리 시작 중 오류 발생: {str(e)}")
            self.start_btn.setEnabled(True)
    
    def update_progress(self, type, message):
        """Update progress and status"""
        if type == "status":
            self.status_text.append(f"상태: {message}")
        elif type == "error":
            self.status_text.append(f"오류: {message}")
            QMessageBox.warning(self, "오류", message)
        elif type == "finished":
            self.status_text.append("처리 완료")
    
    def processing_finished(self, success, output_path):
        """Handle processing completion"""
        self.start_btn.setEnabled(True)
        if success:
            QMessageBox.information(self, "완료", f"처리가 완료되었습니다.\n출력 파일: {output_path}")
        else:
            QMessageBox.warning(self, "오류", "처리 중 오류가 발생했습니다.")

if __name__ == "__main__":
    try:
        app = QApplication(sys.argv)
        app.setStyle("Fusion")
        window = MainWindow()
        window.show()
        sys.exit(app.exec())
    except Exception as e:
        print(f"Error starting GUI: {str(e)}")
        logging.error(f"Error starting GUI: {str(e)}", exc_info=True) 