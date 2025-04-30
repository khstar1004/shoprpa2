import sys
import os
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                           QHBoxLayout, QPushButton, QLabel, QFileDialog, 
                           QProgressBar, QComboBox, QMessageBox, QTabWidget,
                           QTextEdit, QGroupBox, QGridLayout, QSpinBox, QScrollArea,
                           QDoubleSpinBox, QFrame, QStatusBar, QSplitter, QCheckBox,
                           QSlider, QToolButton)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QSize
from PyQt6.QtGui import QIcon, QPixmap, QPalette, QColor, QFont
from PyQt6.QtSvg import QSvgRenderer
from PyQt6.QtSvgWidgets import QSvgWidget
import pandas as pd
import configparser
import logging
from datetime import datetime
import traceback
import asyncio

# Import your existing modules
from excel_utils import create_final_output_excel
from matching_logic import process_matching
from data_processing import process_input_data, process_input_file
from utils import setup_logging, load_config
from main_rpa import main, initialize_environment

class WorkerThread(QThread):
    progress = pyqtSignal(str, str)  # (type, message)
    finished = pyqtSignal(bool, str)  # (success, output_path)
    
    def __init__(self, config_path, input_file=None, process_type=None, batch_size=None):
        super().__init__()
        self.config_path = config_path
        self.input_file = input_file
        self.process_type = process_type
        self.batch_size = batch_size
        self.running = False
        self.output_path = None
        
    def run(self):
        try:
            self.running = True
            # Initialize environment
            CONFIG, gpu_available_detected, validation_passed = initialize_environment(self.config_path)
            
            if not validation_passed:
                self.progress.emit("error", "Environment validation failed")
                self.finished.emit(False, "")
                return

            # Create a custom signal handler to capture paths
            class ProgressHandler:
                def __init__(self, worker_thread):
                    self.worker_thread = worker_thread
                
                def emit(self, signal_type, message):
                    # Forward the signal to the GUI
                    self.worker_thread.progress.emit(signal_type, message)
                    
                    # Special handling for final_path
                    if signal_type == "final_path" and message and not message.startswith("Error:"):
                        self.worker_thread.output_path = message
                        logging.info(f"Output path set: {message}")
            
            # Set up progress handler
            self.progress_handler = ProgressHandler(self)
            
            # Set input file path in config if provided
            if self.input_file and os.path.exists(self.input_file):
                if 'Paths' not in CONFIG:
                    CONFIG.add_section('Paths')
                CONFIG.set('Paths', 'input_file', self.input_file)
                self.progress.emit("status", f"Using input file: {os.path.basename(self.input_file)}")
            
            # Set process type if provided (A for approval, P for price management)
            if self.process_type:
                if 'Processing' not in CONFIG:
                    CONFIG.add_section('Processing')
                process_code = 'A' if '승인관리' in self.process_type else 'P'
                CONFIG.set('Processing', 'process_type', process_code)
                self.progress.emit("status", f"Process type: {self.process_type}")
            
            # Set batch size if provided
            if self.batch_size and self.batch_size > 0:
                if 'Processing' not in CONFIG:
                    CONFIG.add_section('Processing')
                CONFIG.set('Processing', 'batch_size', str(self.batch_size))
                self.progress.emit("status", f"Batch size: {self.batch_size}")

            # Run RPA process
            asyncio.run(main(config=CONFIG, gpu_available=gpu_available_detected, progress_queue=self.progress_handler))
            
            # Determine if successful based on presence of output_path
            success = self.output_path is not None and self.output_path != ""
            output_path = self.output_path if success else ""
            
            if not success:
                logging.warning("Processing completed but no output path was set")
                
            self.finished.emit(success, output_path)
            
        except Exception as e:
            error_msg = f"An error occurred: {str(e)}\n{traceback.format_exc()}"
            logging.error(error_msg)
            self.progress.emit("error", error_msg)
            self.finished.emit(False, "")
        finally:
            self.running = False
            
    def stop(self):
        if self.running:
            self.running = False
            self.terminate()
            self.wait()
            logging.info("Worker thread stopped")

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("ShopRPA - 가격 비교 자동화 시스템")
        self.setMinimumSize(900, 700)
        
        # Initialize instance variables
        self.worker = None
        self.input_files = []
        self.current_file_index = -1
        self.last_upload_path = None
        self.dark_mode = False
        self.file_start_times = {}  # 각 파일 처리 시작 시간 저장
        self.total_start_time = None  # 전체 처리 시작 시간
        self.file_durations = {}  # 각 파일 처리 소요 시간 저장
        
        # Create status bar
        self.statusBar = QStatusBar()
        self.setStatusBar(self.statusBar)
        self.status_label = QLabel("준비")
        self.statusBar.addWidget(self.status_label)
        
        # Load config using the utility function
        self.config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.ini')
        try:
            self.config = load_config(self.config_path)
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
            logging.basicConfig(level=logging.INFO)
        
        # Load SVG icons
        self.load_icons()
        
        # Create main widget and layout
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QVBoxLayout(main_widget)
        
        # Create tabs
        self.tabs = QTabWidget()
        self.tabs.addTab(self.create_process_tab(), "가격 비교")
        self.tabs.addTab(self.create_settings_tab(), "설정")
        self.tabs.addTab(self.create_appearance_tab(), "모양")
        self.tabs.addTab(self.create_help_tab(), "도움말")
        layout.addWidget(self.tabs)
        
        # Set window icon if available
        icon_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'assets', 'icon.ico')
        if os.path.exists(icon_path):
            self.setWindowIcon(QIcon(icon_path))
        
        # Show success message
        self.statusBar.showMessage("ShopRPA 시스템 초기화 완료", 3000)
        
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
                'logo': os.path.join(assets_dir, 'logo.svg'),
                'analytics': os.path.join(assets_dir, 'analytics.svg'),
                'chart': os.path.join(assets_dir, 'chart.svg'),
                'reports': os.path.join(assets_dir, 'reports.svg'),
                'file_upload': os.path.join(assets_dir, 'file-upload.svg'),
                'check': os.path.join(assets_dir, 'check-white.svg')
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
        """Create the main processing tab"""
        tab = QWidget()
        
        # 전체 레이아웃
        main_layout = QVBoxLayout(tab)
        main_layout.setSpacing(10)
        main_layout.setContentsMargins(15, 15, 15, 15)
        
        # 헤더 백그라운드 및 로고 부분
        header_frame = QFrame()
        header_frame.setObjectName("headerFrame")
        header_frame.setMinimumHeight(80)
        header_frame.setMaximumHeight(80)
        header_frame.setStyleSheet("""
            #headerFrame {
                background-color: #f5f5f5;
                border-radius: 8px;
                border: 1px solid #e0e0e0;
            }
        """)
        
        header_layout = QHBoxLayout(header_frame)
        header_layout.setContentsMargins(20, 5, 20, 5)
        
        # SVG 로고 이미지
        logo = QSvgWidget(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'assets', 'compact-logo.svg'))
        logo.setFixedSize(36, 36)
        header_layout.addWidget(logo)
        
        # 타이틀 레이아웃
        title_layout = QVBoxLayout()
        title_layout.setSpacing(0)
        
        # 타이틀과 버전 텍스트
        title = QLabel("ShopRPA")
        title.setStyleSheet("font-size: 18pt; font-weight: bold; color: #333;")
        subtitle = QLabel("가격 비교 자동화 시스템")
        subtitle.setStyleSheet("font-size: 9pt; color: #666;")
        version = QLabel("v1.0.2")
        version.setStyleSheet("font-size: 8pt; color: #888;")
        
        title_layout.addWidget(title)
        title_layout.addWidget(subtitle)
        title_layout.addWidget(version)
        header_layout.addLayout(title_layout)
        
        # 스페이서 추가 (왼쪽 정렬)
        header_layout.addStretch(1)
        
        # 상태 표시기
        self.status_layout = QHBoxLayout()
        status_indicator_icon = QSvgWidget(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'assets', 'status-ready.svg'))
        status_indicator_icon.setFixedSize(24, 24)
        self.status_indicator = QLabel("준비됨")
        self.status_indicator.setStyleSheet("""
            font-weight: bold;
            color: #4CAF50;
            padding: 5px;
        """)
        
        self.status_layout.addWidget(status_indicator_icon)
        self.status_layout.addWidget(self.status_indicator)
        header_layout.addLayout(self.status_layout)
        
        main_layout.addWidget(header_frame)
        
        # 컨텐츠 영역 (스플리터)
        content_splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # 왼쪽 패널 (컨트롤 영역)
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 10, 10, 0)
        
        # 파일 선택 섹션
        file_group = QGroupBox("입력 파일")
        file_group.setStyleSheet("""
            QGroupBox {
                font-weight: bold;
                border: 1px solid #cccccc;
                border-radius: 6px;
                margin-top: 10px;
                padding-top: 12px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px 0 5px;
            }
        """)
        
        file_layout = QVBoxLayout(file_group)
        file_layout.setSpacing(10)
        
        # 파일 선택 표시
        self.file_label = QLabel("선택된 파일: 없음")
        self.file_label.setWordWrap(True)
        self.file_label.setStyleSheet("color: #666;")
        
        # 파일 버튼 레이아웃
        file_buttons = QHBoxLayout()
        
        # 파일 선택 버튼
        browse_btn = QPushButton("파일 선택")
        browse_btn.setIcon(QIcon(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'assets', 'file-upload.svg')))
        browse_btn.setStyleSheet("""
            QPushButton {
                background-color: #2196F3;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
                min-width: 100px;
            }
            QPushButton:hover {
                background-color: #0b7dda;
            }
            QPushButton:pressed {
                background-color: #0a5999;
            }
        """)
        browse_btn.clicked.connect(self.browse_file)
        
        # 결과 파일 열기 버튼
        self.open_file_btn = QPushButton("결과 파일 열기")
        self.open_file_btn.setEnabled(False)
        self.open_file_btn.setIcon(QIcon(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'assets', 'file.svg')))
        self.open_file_btn.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
                min-width: 120px;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
            QPushButton:pressed {
                background-color: #398439;
            }
            QPushButton:disabled {
                background-color: #cccccc;
                color: #666666;
            }
        """)
        self.open_file_btn.setToolTip("마지막으로 성공적으로 생성된 업로드 파일을 엽니다.")
        self.open_file_btn.setText("업로드 파일 열기")
        self.open_file_btn.clicked.connect(self.open_result_file)
        
        file_buttons.addWidget(browse_btn)
        file_buttons.addWidget(self.open_file_btn)
        
        file_layout.addWidget(self.file_label)
        file_layout.addLayout(file_buttons)
        
        left_layout.addWidget(file_group)
        
        # 처리 설정 섹션
        settings_group = QGroupBox("처리 설정")
        settings_group.setStyleSheet("""
            QGroupBox {
                font-weight: bold;
                border: 1px solid #cccccc;
                border-radius: 6px;
                margin-top: 10px;
                padding-top: 12px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px 0 5px;
            }
        """)
        
        settings_layout = QGridLayout(settings_group)
        settings_layout.setColumnStretch(1, 1)
        settings_layout.setVerticalSpacing(10)
        
        # 처리 유형 설정
        settings_layout.addWidget(QLabel("처리 유형:"), 0, 0)
        self.process_type = QComboBox()
        self.process_type.addItems(["승인관리 (A)", "가격관리 (P)"])
        self.process_type.setStyleSheet("""
            QComboBox {
                border: 1px solid #ccc;
                border-radius: 4px;
                padding: 5px;
                background-color: white;
                min-height: 25px;
            }
            QComboBox::drop-down {
                subcontrol-origin: padding;
                subcontrol-position: top right;
                width: 15px;
                border-left-width: 1px;
                border-left-color: #ccc;
                border-left-style: solid;
            }
        """)
        settings_layout.addWidget(self.process_type, 0, 1)
        
        # 배치 크기 설정
        settings_layout.addWidget(QLabel("배치 크기:"), 1, 0)
        self.batch_size = QSpinBox()
        self.batch_size.setRange(1, 1000)
        self.batch_size.setValue(50)
        self.batch_size.setStyleSheet("""
            QSpinBox {
                border: 1px solid #ccc;
                border-radius: 4px;
                padding: 5px;
                background-color: white;
                min-height: 25px;
            }
        """)
        settings_layout.addWidget(self.batch_size, 1, 1)
        
        left_layout.addWidget(settings_group)
        
        # 진행 상태 섹션
        progress_group = QGroupBox("진행 상태")
        progress_group.setStyleSheet("""
            QGroupBox {
                font-weight: bold;
                border: 1px solid #cccccc;
                border-radius: 6px;
                margin-top: 10px;
                padding-top: 12px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px 0 5px;
            }
        """)
        
        progress_layout = QVBoxLayout(progress_group)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(100)
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setFormat("%p% 완료")
        self.progress_bar.setValue(0)
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: 1px solid #ccc;
                border-radius: 5px;
                text-align: center;
                height: 20px;
                background-color: #f5f5f5;
            }
            QProgressBar::chunk {
                background-color: #4CAF50;
                border-radius: 5px;
            }
        """)
        progress_layout.addWidget(self.progress_bar)
        
        left_layout.addWidget(progress_group)
        
        # 컨트롤 버튼
        control_layout = QHBoxLayout()
        control_layout.setSpacing(10)
        
        # 시작 버튼
        self.start_btn = QPushButton("처리 시작")
        self.start_btn.setIcon(QIcon(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'assets', 'batch.svg')))
        self.start_btn.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                border: none;
                padding: 10px 20px;
                font-weight: bold;
                border-radius: 4px;
                min-width: 120px;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
            QPushButton:pressed {
                background-color: #398439;
            }
        """)
        self.start_btn.clicked.connect(self.start_processing)
        
        # 중지 버튼
        self.stop_btn = QPushButton("처리 중단")
        self.stop_btn.setEnabled(False)
        self.stop_btn.setStyleSheet("""
            QPushButton {
                background-color: #f44336;
                color: white;
                border: none;
                padding: 10px 20px;
                font-weight: bold;
                border-radius: 4px;
                min-width: 120px;
            }
            QPushButton:hover {
                background-color: #d32f2f;
            }
            QPushButton:pressed {
                background-color: #b71c1c;
            }
            QPushButton:disabled {
                background-color: #cccccc;
                color: #666666;
            }
        """)
        self.stop_btn.clicked.connect(self.stop_processing)
        
        control_layout.addWidget(self.start_btn)
        control_layout.addWidget(self.stop_btn)
        left_layout.addLayout(control_layout)
        
        # 공간 늘리기
        left_layout.addStretch(1)
        
        # 오른쪽 패널 (로그 출력)
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(10, 10, 0, 0)
        
        # 로그 그룹
        log_group = QGroupBox("로그")
        log_group.setStyleSheet("""
            QGroupBox {
                font-weight: bold;
                border: 1px solid #cccccc;
                border-radius: 6px;
                margin-top: 10px;
                padding-top: 12px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px 0 5px;
            }
        """)
        
        log_layout = QVBoxLayout(log_group)
        
        # 로그 출력 영역
        self.status_text = QTextEdit()
        self.status_text.setReadOnly(True)
        self.status_text.setMinimumHeight(200)
        self.status_text.setStyleSheet("""
            QTextEdit {
                border: 1px solid #ccc;
                border-radius: 4px;
                background-color: #f9f9f9;
                font-family: 'Consolas', monospace;
                padding: 5px;
            }
        """)
        log_layout.addWidget(self.status_text)
        
        right_layout.addWidget(log_group)
        
        # 패널 추가
        content_splitter.addWidget(left_panel)
        content_splitter.addWidget(right_panel)
        content_splitter.setSizes([350, 650])
        
        main_layout.addWidget(content_splitter, 1)  # 스트레치 추가
        
        return tab
        
    def create_settings_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Create a scroll area to contain all settings
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_content = QWidget()
        scroll_layout = QVBoxLayout(scroll_content)
        
        # Load current config values
        try:
            self.config = load_config(self.config_path)
        except Exception as e:
            logging.error(f"Failed to load config for settings tab: {e}")
            self.config = configparser.ConfigParser()
        
        # --- Matching Thresholds Group ---
        thresholds_group = QGroupBox("매칭 임계값 설정")
        thresholds_layout = QGridLayout()
        
        # Text threshold
        thresholds_layout.addWidget(QLabel("텍스트 유사도 임계값:"), 0, 0)
        self.text_threshold = QDoubleSpinBox()
        self.text_threshold.setRange(0.01, 0.99)
        self.text_threshold.setSingleStep(0.01)
        self.text_threshold.setDecimals(2)
        self.text_threshold.setValue(self.config.getfloat('Matching', 'text_threshold', fallback=0.45))
        thresholds_layout.addWidget(self.text_threshold, 0, 1)
        
        # Image threshold
        thresholds_layout.addWidget(QLabel("이미지 유사도 임계값:"), 1, 0)
        self.image_threshold = QDoubleSpinBox()
        self.image_threshold.setRange(0.01, 0.99)
        self.image_threshold.setSingleStep(0.01)
        self.image_threshold.setDecimals(2)
        self.image_threshold.setValue(self.config.getfloat('Matching', 'image_threshold', fallback=0.42))
        thresholds_layout.addWidget(self.image_threshold, 1, 1)
        
        # Combined threshold
        thresholds_layout.addWidget(QLabel("통합 유사도 임계값:"), 2, 0)
        self.combined_threshold = QDoubleSpinBox()
        self.combined_threshold.setRange(0.01, 0.99)
        self.combined_threshold.setSingleStep(0.01)
        self.combined_threshold.setDecimals(2)
        self.combined_threshold.setValue(self.config.getfloat('Matching', 'combined_threshold', fallback=0.48))
        thresholds_layout.addWidget(self.combined_threshold, 2, 1)
        
        # Minimum combined score
        thresholds_layout.addWidget(QLabel("최소 통합 점수:"), 3, 0)
        self.min_combined_score = QDoubleSpinBox()
        self.min_combined_score.setRange(0.01, 0.99)
        self.min_combined_score.setSingleStep(0.01)
        self.min_combined_score.setDecimals(2)
        self.min_combined_score.setValue(self.config.getfloat('Matching', 'minimum_combined_score', fallback=0.40))
        thresholds_layout.addWidget(self.min_combined_score, 3, 1)
        
        # Image display threshold
        thresholds_layout.addWidget(QLabel("이미지 표시 임계값:"), 4, 0)
        self.image_display_threshold = QDoubleSpinBox()
        self.image_display_threshold.setRange(0.01, 0.99)
        self.image_display_threshold.setSingleStep(0.01)
        self.image_display_threshold.setDecimals(2)
        self.image_display_threshold.setValue(self.config.getfloat('Matching', 'image_display_threshold', fallback=0.7))
        thresholds_layout.addWidget(self.image_display_threshold, 4, 1)
        
        thresholds_group.setLayout(thresholds_layout)
        scroll_layout.addWidget(thresholds_group)
        
        # --- Weights Group ---
        weights_group = QGroupBox("가중치 설정")
        weights_layout = QGridLayout()
        
        # Text weight
        weights_layout.addWidget(QLabel("텍스트 가중치:"), 0, 0)
        self.text_weight = QDoubleSpinBox()
        self.text_weight.setRange(0.01, 0.99)
        self.text_weight.setSingleStep(0.05)
        self.text_weight.setDecimals(2)
        self.text_weight.setValue(self.config.getfloat('Matching', 'text_weight', fallback=0.65))
        weights_layout.addWidget(self.text_weight, 0, 1)
        
        # Image weight
        weights_layout.addWidget(QLabel("이미지 가중치:"), 1, 0)
        self.image_weight = QDoubleSpinBox()
        self.image_weight.setRange(0.01, 0.99)
        self.image_weight.setSingleStep(0.05)
        self.image_weight.setDecimals(2)
        self.image_weight.setValue(self.config.getfloat('Matching', 'image_weight', fallback=0.35))
        weights_layout.addWidget(self.image_weight, 1, 1)
        
        weights_group.setLayout(weights_layout)
        scroll_layout.addWidget(weights_group)
        
        # --- Feature Options Group ---
        features_group = QGroupBox("특성 설정")
        features_layout = QGridLayout()
        
        # Use ensemble models
        features_layout.addWidget(QLabel("앙상블 모델 사용:"), 0, 0)
        self.use_ensemble = QComboBox()
        self.use_ensemble.addItems(["True", "False"])
        ensemble_value = "True" if self.config.getboolean('Matching', 'use_ensemble_models', fallback=True) else "False"
        self.use_ensemble.setCurrentText(ensemble_value)
        features_layout.addWidget(self.use_ensemble, 0, 1)
        
        # Use TFIDF
        features_layout.addWidget(QLabel("TFIDF 사용:"), 1, 0)
        self.use_tfidf = QComboBox()
        self.use_tfidf.addItems(["True", "False"])
        tfidf_value = "True" if self.config.getboolean('Matching', 'use_tfidf', fallback=False) else "False"
        self.use_tfidf.setCurrentText(tfidf_value)
        features_layout.addWidget(self.use_tfidf, 1, 1)
        
        # Use multiple image models
        features_layout.addWidget(QLabel("다중 이미지 모델 사용:"), 2, 0)
        self.use_multi_img_models = QComboBox()
        self.use_multi_img_models.addItems(["True", "False"])
        multi_img_value = "True" if self.config.getboolean('ImageMatching', 'use_multiple_models', fallback=True) else "False"
        self.use_multi_img_models.setCurrentText(multi_img_value)
        features_layout.addWidget(self.use_multi_img_models, 2, 1)
        
        # Use background removal
        features_layout.addWidget(QLabel("배경 제거 사용:"), 3, 0)
        self.use_bg_removal = QComboBox()
        self.use_bg_removal.addItems(["True", "False"])
        bg_removal_value = "True" if self.config.getboolean('Matching', 'use_background_removal', fallback=True) else "False"
        self.use_bg_removal.setCurrentText(bg_removal_value)
        features_layout.addWidget(self.use_bg_removal, 3, 1)
        
        features_group.setLayout(features_layout)
        scroll_layout.addWidget(features_group)
        
        # --- Concurrency Settings Group ---
        concurrency_group = QGroupBox("병렬 처리 설정")
        concurrency_layout = QGridLayout()
        
        # Max crawl workers
        concurrency_layout.addWidget(QLabel("최대 크롤링 작업자:"), 0, 0)
        self.max_crawl_workers = QSpinBox()
        self.max_crawl_workers.setRange(1, 8)
        self.max_crawl_workers.setValue(self.config.getint('Concurrency', 'max_crawl_workers', fallback=2))
        concurrency_layout.addWidget(self.max_crawl_workers, 0, 1)
        
        # Max match workers
        concurrency_layout.addWidget(QLabel("최대 매칭 작업자:"), 1, 0)
        self.max_match_workers = QSpinBox()
        self.max_match_workers.setRange(1, 8)
        self.max_match_workers.setValue(self.config.getint('Concurrency', 'max_match_workers', fallback=4))
        concurrency_layout.addWidget(self.max_match_workers, 1, 1)
        
        concurrency_group.setLayout(concurrency_layout)
        scroll_layout.addWidget(concurrency_group)
        
        # --- Save Button ---
        save_btn = QPushButton("설정 저장")
        save_btn.clicked.connect(self.save_settings)
        scroll_layout.addWidget(save_btn)
        
        # Add stretch to push everything to the top
        scroll_layout.addStretch()
        
        # Set up scroll area
        scroll_area.setWidget(scroll_content)
        layout.addWidget(scroll_area)
        
        return tab
        
    def save_settings(self):
        """Save settings to config.ini file"""
        try:
            # Load current config to keep unmodified sections
            config = load_config(self.config_path)
            
            # Update Matching section
            if 'Matching' not in config:
                config.add_section('Matching')
                
            # Update threshold values
            config.set('Matching', 'text_threshold', str(self.text_threshold.value()))
            config.set('Matching', 'image_threshold', str(self.image_threshold.value()))
            config.set('Matching', 'combined_threshold', str(self.combined_threshold.value()))
            config.set('Matching', 'minimum_combined_score', str(self.min_combined_score.value()))
            config.set('Matching', 'image_display_threshold', str(self.image_display_threshold.value()))
            
            # Update weight values
            config.set('Matching', 'text_weight', str(self.text_weight.value()))
            config.set('Matching', 'image_weight', str(self.image_weight.value()))
            
            # Update feature options
            config.set('Matching', 'use_ensemble_models', self.use_ensemble.currentText())
            config.set('Matching', 'use_tfidf', self.use_tfidf.currentText())
            config.set('ImageMatching', 'use_multiple_models', self.use_multi_img_models.currentText())
            config.set('Matching', 'use_background_removal', self.use_bg_removal.currentText())
            
            # Update concurrency settings
            if 'Concurrency' not in config:
                config.add_section('Concurrency')
            config.set('Concurrency', 'max_crawl_workers', str(self.max_crawl_workers.value()))
            config.set('Concurrency', 'max_match_workers', str(self.max_match_workers.value()))
            
            # Save to file
            with open(self.config_path, 'w', encoding='utf-8') as f:
                config.write(f)
                
            # Show success message
            QMessageBox.information(self, "설정 저장", "설정이 성공적으로 저장되었습니다.")
            logging.info("Settings saved to config.ini")
            
        except Exception as e:
            error_msg = f"설정 저장 중 오류 발생: {str(e)}"
            QMessageBox.critical(self, "오류", error_msg)
            logging.error(f"Failed to save settings: {e}", exc_info=True)
        
    def create_help_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Help content
        help_text = QTextEdit()
        help_text.setReadOnly(True)
        help_text.setStyleSheet("""
            QTextEdit {
                background-color: #ffffff;
                border: 1px solid #dddddd;
                border-radius: 5px;
                font-family: 'Segoe UI', Arial, sans-serif;
            }
        """)
        
        help_text.setHtml("""
        <style>
            h2 { color: #2196F3; }
            h3 { color: #333; border-bottom: 1px solid #eee; padding-bottom: 5px; }
            h4 { color: #4CAF50; }
            b { color: #333; }
            .note { background-color: #e8f4fd; padding: 10px; border-left: 4px solid #2196F3; margin: 10px 0; }
            .warning { background-color: #ffebee; padding: 10px; border-left: 4px solid #f44336; margin: 10px 0; }
            .tip { background-color: #e8f5e9; padding: 10px; border-left: 4px solid #4CAF50; margin: 10px 0; }
            ul { margin-left: 20px; }
        </style>
        <h2>ShopRPA 사용 설명서</h2>
        
        <h3>1. 기본 사용법</h3>
        <ol>
            <li><b>파일 선택</b> 버튼을 클릭하여 가격 비교를 수행할 엑셀 파일을 선택합니다.</li>
            <li>처리 유형을 선택합니다 (<b>승인관리</b> 또는 <b>가격관리</b>).</li>
            <li>배치 크기를 설정합니다 (기본값: 50).</li>
            <li><b>처리 시작</b> 버튼을 클릭하여 작업을 시작합니다.</li>
        </ol>
        
        <div class="note">
            <b>참고:</b> 처리 중에는 상단의 상태 표시기가 파란색으로 변경되며, 완료시 녹색으로 표시됩니다.
        </div>
        
        <h3>2. 처리 유형 설명</h3>
        <p><b>승인관리 (A):</b> 상품 승인을 위한 가격 비교를 수행합니다.</p>
        <p><b>가격관리 (P):</b> 일반적인 가격 비교를 수행합니다.</p>
        
        <h3>3. 결과 파일</h3>
        <p>처리가 완료되면 입력 파일과 동일한 폴더에 결과 파일이 생성됩니다.</p>
        <p>파일명 형식: <code>output_YYYYMMDD_HHMMSS.xlsx</code></p>
        <p><b>결과 파일 열기</b> 버튼을 클릭하여 생성된 결과 파일을 직접 열 수 있습니다.</p>
        
        <h3>4. 설정 탭 사용법</h3>
        <p>설정 탭에서는 매칭 임계값 및 다양한 설정을 변경할 수 있습니다:</p>
        
        <h4>매칭 임계값 설정</h4>
        <ul>
            <li><b>텍스트 유사도 임계값</b>: 텍스트 비교 시 필요한 최소 유사도 (0.01-0.99)</li>
            <li><b>이미지 유사도 임계값</b>: 이미지 비교 시 필요한 최소 유사도 (0.01-0.99)</li>
            <li><b>통합 유사도 임계값</b>: 텍스트와 이미지를 함께 고려한 최소 유사도 (0.01-0.99)</li>
            <li><b>최소 통합 점수</b>: 매칭으로 간주하기 위한 최소 점수 (0.01-0.99)</li>
            <li><b>이미지 표시 임계값</b>: 결과에 이미지를 표시하기 위한 최소 유사도 (0.01-0.99)</li>
        </ul>
        
        <h4>가중치 설정</h4>
        <ul>
            <li><b>텍스트 가중치</b>: 전체 유사도에서 텍스트 유사도의 가중치 (0.01-0.99)</li>
            <li><b>이미지 가중치</b>: 전체 유사도에서 이미지 유사도의 가중치 (0.01-0.99)</li>
        </ul>
        
        <h4>특성 설정</h4>
        <ul>
            <li><b>앙상블 모델 사용</b>: 여러 모델을 함께 사용하여 정확도 향상 (True/False)</li>
            <li><b>TFIDF 사용</b>: 텍스트 매칭에 TFIDF 알고리즘 사용 (True/False)</li>
            <li><b>다중 이미지 모델 사용</b>: 여러 이미지 모델을 함께 사용 (True/False)</li>
            <li><b>배경 제거 사용</b>: 이미지 배경 제거 기능 사용 (True/False)</li>
        </ul>
        
        <h4>병렬 처리 설정</h4>
        <ul>
            <li><b>최대 크롤링 작업자</b>: 동시에 실행할 크롤링 작업자 수 (1-8)</li>
            <li><b>최대 매칭 작업자</b>: 동시에 실행할 매칭 작업자 수 (1-8)</li>
        </ul>
        
        <div class="tip">
            <b>팁:</b> 설정을 변경한 후에는 '설정 저장' 버튼을 클릭하여 변경 사항을 저장하세요.
        </div>
        
        <h3>5. 모양 탭 사용법</h3>
        <p>모양 탭에서는 애플리케이션의 디자인과 사용자 경험을 개인화할 수 있습니다:</p>
        
        <h4>테마 설정</h4>
        <ul>
            <li><b>다크 모드</b>: 화면의 밝기를 어둡게 하여 눈의 피로도를 줄이고 배터리 사용량을 줄입니다.</li>
        </ul>
        
        <h4>글꼴 설정</h4>
        <ul>
            <li><b>글꼴 크기</b>: 슬라이더를 사용하여 애플리케이션 전체의 글꼴 크기를 조정합니다 (8-16pt).</li>
        </ul>
        
        <div class="note">
            <b>참고:</b> 모양 설정은 즉시 적용되며, 프로그램을 재시작할 필요가 없습니다.
        </div>
        
        <h3>6. 주의사항</h3>
        <div class="warning">
            <ul>
                <li>입력 파일은 반드시 지정된 형식을 따라야 합니다.</li>
                <li>대량의 데이터를 처리할 때는 배치 크기를 적절히 조정하세요.</li>
                <li>처리 중에는 프로그램을 종료하지 마세요.</li>
                <li>임계값을 너무 높게 설정하면 매칭률이 낮아질 수 있습니다.</li>
                <li>임계값을 너무 낮게 설정하면 부정확한 매칭이 발생할 수 있습니다.</li>
            </ul>
        </div>
        """)
        layout.addWidget(help_text)
        
        return tab
        
    def create_appearance_tab(self):
        """Create the appearance tab with theme settings"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Create a frame for theme settings
        theme_frame = QFrame()
        theme_frame.setFrameShape(QFrame.Shape.StyledPanel)
        theme_frame.setFrameShadow(QFrame.Shadow.Raised)
        theme_layout = QVBoxLayout(theme_frame)
        
        # Add theme selection section
        theme_group = QGroupBox("테마 설정")
        theme_group_layout = QVBoxLayout(theme_group)
        
        # Dark mode toggle
        self.dark_mode_checkbox = QCheckBox("다크 모드")
        self.dark_mode_checkbox.setChecked(self.dark_mode)
        self.dark_mode_checkbox.stateChanged.connect(self.toggle_dark_mode)
        theme_group_layout.addWidget(self.dark_mode_checkbox)
        
        # Add description
        theme_desc = QLabel("테마를 변경하면 즉시 적용됩니다. 다크 모드는 눈의 피로를 줄이고 배터리 소모를 줄일 수 있습니다.")
        theme_desc.setWordWrap(True)
        theme_group_layout.addWidget(theme_desc)
        
        theme_layout.addWidget(theme_group)
        layout.addWidget(theme_frame)
        
        # Font size section
        font_frame = QFrame()
        font_frame.setFrameShape(QFrame.Shape.StyledPanel)
        font_frame.setFrameShadow(QFrame.Shadow.Raised)
        font_layout = QVBoxLayout(font_frame)
        
        font_group = QGroupBox("글꼴 설정")
        font_group_layout = QVBoxLayout(font_group)
        
        font_size_layout = QHBoxLayout()
        font_size_label = QLabel("글꼴 크기:")
        self.font_size_slider = QSlider(Qt.Orientation.Horizontal)
        self.font_size_slider.setMinimum(8)
        self.font_size_slider.setMaximum(16)
        self.font_size_slider.setValue(QApplication.font().pointSize())
        self.font_size_slider.setTickPosition(QSlider.TickPosition.TicksBelow)
        self.font_size_slider.setTickInterval(1)
        self.font_size_slider.valueChanged.connect(self.change_font_size)
        
        self.font_size_value = QLabel(str(self.font_size_slider.value()))
        
        font_size_layout.addWidget(font_size_label)
        font_size_layout.addWidget(self.font_size_slider)
        font_size_layout.addWidget(self.font_size_value)
        
        font_group_layout.addLayout(font_size_layout)
        font_layout.addWidget(font_group)
        layout.addWidget(font_frame)
        
        # Add spacer
        layout.addStretch()
        
        return tab
        
    def toggle_dark_mode(self, state):
        """Toggle dark mode on/off"""
        self.dark_mode = bool(state)
        if self.dark_mode:
            # Dark mode palette
            dark_palette = QPalette()
            dark_palette.setColor(QPalette.ColorRole.Window, QColor(53, 53, 53))
            dark_palette.setColor(QPalette.ColorRole.WindowText, QColor(255, 255, 255))
            dark_palette.setColor(QPalette.ColorRole.Base, QColor(25, 25, 25))
            dark_palette.setColor(QPalette.ColorRole.AlternateBase, QColor(53, 53, 53))
            dark_palette.setColor(QPalette.ColorRole.ToolTipBase, QColor(0, 0, 0))
            dark_palette.setColor(QPalette.ColorRole.ToolTipText, QColor(255, 255, 255))
            dark_palette.setColor(QPalette.ColorRole.Text, QColor(255, 255, 255))
            dark_palette.setColor(QPalette.ColorRole.Button, QColor(53, 53, 53))
            dark_palette.setColor(QPalette.ColorRole.ButtonText, QColor(255, 255, 255))
            dark_palette.setColor(QPalette.ColorRole.BrightText, QColor(255, 0, 0))
            dark_palette.setColor(QPalette.ColorRole.Link, QColor(42, 130, 218))
            dark_palette.setColor(QPalette.ColorRole.Highlight, QColor(42, 130, 218))
            dark_palette.setColor(QPalette.ColorRole.HighlightedText, QColor(0, 0, 0))
            
            QApplication.setPalette(dark_palette)
            self.statusBar.showMessage("다크 모드가 활성화되었습니다.", 3000)
        else:
            # Reset to default palette
            QApplication.setPalette(QApplication.style().standardPalette())
            self.statusBar.showMessage("라이트 모드가 활성화되었습니다.", 3000)
    
    def change_font_size(self, size):
        """Change application font size"""
        self.font_size_value.setText(str(size))
        font = QApplication.font()
        font.setPointSize(size)
        QApplication.setFont(font)
        self.statusBar.showMessage(f"글꼴 크기가 {size}로 변경되었습니다.", 3000)
    
    def browse_file(self):
        # Allow selecting multiple files
        file_names, _ = QFileDialog.getOpenFileNames(
            self,
            "엑셀 파일 선택 (여러 개 선택 가능)",
            "",
            "Excel Files (*.xlsx *.xls)"
        )
        if file_names:
            self.input_files = file_names
            if len(file_names) == 1:
                self.file_label.setText(f"선택된 파일: {os.path.basename(file_names[0])}")
            else:
                self.file_label.setText(f"{len(file_names)}개 파일 선택됨")
            # Disable open file button when new files are selected, until processing is complete
            self.open_file_btn.setEnabled(False)
            self.last_upload_path = None

    def start_processing(self):
        """Start the RPA process for the list of files sequentially"""
        try:
            # Check if files are selected
            if not self.input_files:
                QMessageBox.warning(self, "경고", "입력 파일을 먼저 선택해주세요.")
                return

            # Check if already processing
            if self.worker is not None and self.worker.isRunning():
                QMessageBox.warning(self, "알림", "현재 다른 파일 처리 중입니다.")
                return

            # Disable start button and enable stop button
            self.start_btn.setEnabled(False)
            if hasattr(self, 'stop_btn'):
                self.stop_btn.setEnabled(True)

            # Disable open file button during processing
            if hasattr(self, 'open_file_btn'):
                self.open_file_btn.setEnabled(False)

            # Clear status text and reset progress bar for the sequence
            self.status_text.clear()
            self.progress_bar.setValue(0)
            self.last_upload_path = None # Reset last path for the new sequence
            
            # 시간 측정 초기화
            self.file_start_times = {}
            self.file_durations = {}
            self.total_start_time = datetime.now()
            
            # 전체 처리 시작 메시지
            self.status_text.append(f"<span style='color:#2196F3;'>전체 처리 시작: {self.total_start_time.strftime('%Y-%m-%d %H:%M:%S')} ({len(self.input_files)}개 파일)</span>")

            # Start with the first file
            self.current_file_index = 0
            self.start_next_file_processing()

        except Exception as e:
            error_msg = f"처리 시작 중 오류 발생: {str(e)}\n{traceback.format_exc()}"
            QMessageBox.critical(self, "오류", error_msg)
            self.start_btn.setEnabled(True)
            if hasattr(self, 'stop_btn'):
                self.stop_btn.setEnabled(False)
            logging.error(error_msg)

    def start_next_file_processing(self):
        """Initiates processing for the file at self.current_file_index."""
        if 0 <= self.current_file_index < len(self.input_files):
            current_file = self.input_files[self.current_file_index]
            num_files = len(self.input_files)
            file_info = f"[파일 {self.current_file_index + 1}/{num_files}]"

            # 파일 처리 시작 시간 기록
            self.file_start_times[self.current_file_index] = datetime.now()
            start_time_str = self.file_start_times[self.current_file_index].strftime('%H:%M:%S')

            # Add status update for starting the specific file
            self.status_text.append(f"<span style='color:#2196F3;'>{file_info} 처리 시작: {start_time_str} - {os.path.basename(current_file)}</span>")
            self.progress_bar.setValue(0) # Reset progress bar for each file

            # Get current settings from UI (needed for each file)
            selected_process_type = self.process_type.currentText()
            selected_batch_size = self.batch_size.value()

            # Create and start worker thread for the current file
            self.worker = WorkerThread(
                self.config_path,
                current_file,
                selected_process_type,
                selected_batch_size
            )
            # Disconnect previous connections if any (important for sequential runs)
            try:
                self.worker.progress.disconnect()
                self.worker.finished.disconnect()
            except TypeError: # Raised if no connection exists
                pass
            # Connect signals for the current worker
            self.worker.progress.connect(self.update_progress)
            self.worker.finished.connect(self.processing_finished)
            self.worker.start()
        else:
            # Should not happen if logic is correct, but good to handle
            logging.error("Invalid current_file_index in start_next_file_processing")
            self.processing_finished(False, "") # Treat as overall failure

    def stop_processing(self):
        """Stop the RPA process (stops current file, cancels sequence)"""
        try:
            if self.worker and self.worker.isRunning():
                reply = QMessageBox.question(
                    self,
                    '처리 중단',
                    '현재 파일 처리를 중단하고 남은 파일 처리를 취소하시겠습니까?',
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                    QMessageBox.StandardButton.No
                )

                if reply == QMessageBox.StandardButton.Yes:
                    self.worker.stop()
                    
                    # 중단 시간과 처리 소요 시간 계산
                    if self.current_file_index in self.file_start_times:
                        stop_time = datetime.now()
                        duration = stop_time - self.file_start_times[self.current_file_index]
                        self.status_text.append(f"<span style='color:#FF9800;'>[파일 {self.current_file_index + 1}/{len(self.input_files)}] 작업이 사용자에 의해 중단되었습니다. (처리 시간: {self._format_duration(duration)})</span>")
                    else:
                        self.status_text.append(f"<span style='color:#FF9800;'>[파일 {self.current_file_index + 1}/{len(self.input_files)}] 작업이 사용자에 의해 중단되었습니다.</span>")
                    
                    # 전체 처리 중단 메시지와 소요 시간
                    if self.total_start_time:
                        total_duration = datetime.now() - self.total_start_time
                        self.status_text.append(f"<span style='color:#FF9800;'>전체 파일 처리가 중단되었습니다. (총 소요 시간: {self._format_duration(total_duration)})</span>")
                    else:
                        self.status_text.append("<span style='color:#FF9800;'>전체 파일 처리가 중단되었습니다.</span>")
                    
                    # Reset state
                    self.current_file_index = -1
                    self.start_btn.setEnabled(True)
                    if hasattr(self, 'stop_btn'):
                        self.stop_btn.setEnabled(False)
                    # Consider enabling open file button if a previous file succeeded
                    self.open_file_btn.setEnabled(self.last_upload_path is not None)

        except Exception as e:
            error_msg = f"작업 중단 중 오류 발생: {str(e)}"
            QMessageBox.critical(self, "오류", error_msg)
            logging.error(error_msg)

    def update_progress(self, type, message):
        """Update progress and status, prepending current file info"""
        try:
            # Prepend file progress info if processing multiple files
            file_info = ""
            if len(self.input_files) > 1 and self.current_file_index >= 0:
                file_info = f"[파일 {self.current_file_index + 1}/{len(self.input_files)}] "

            if type == "status":
                # Format timestamp
                timestamp = datetime.now().strftime("%H:%M:%S")
                self.status_text.append(f"[{timestamp}] ℹ️ {file_info}{message}")

                # Update status indicator
                self.status_indicator.setText("처리 중")
                self.status_indicator.setStyleSheet("font-weight: bold; color: #2196F3; padding: 5px;")

                # Update status icon
                status_icon_path = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    'assets', 'status-processing.svg'
                )
                if hasattr(self, 'status_layout') and self.status_layout.count() > 0:
                    old_icon = self.status_layout.itemAt(0).widget()
                    if old_icon:
                        # Check if the icon needs changing
                        if 'status-processing' not in old_icon.renderer().defaultSize().toString(): # Basic check
                           new_icon = QSvgWidget(status_icon_path)
                           new_icon.setFixedSize(24, 24)
                           self.status_layout.replaceWidget(old_icon, new_icon)
                           old_icon.deleteLater()

                # Pulse the progress bar for the current file
                if self.progress_bar.value() < 95: # Avoid hitting 100 prematurely
                    current_value = self.progress_bar.value()
                    # Simple pulse, might need refinement based on actual steps
                    new_value = min(current_value + 5, 95)
                    self.progress_bar.setValue(new_value)

                # Update status bar
                self.statusBar.showMessage(f"상태: {file_info}{message}", 3000)

            elif type == "error":
                # Format timestamp with error styling
                timestamp = datetime.now().strftime("%H:%M:%S")
                self.status_text.append(f"<span style='color:#f44336;'>[{timestamp}] ❌ 오류: {file_info}{message}</span>")

                # Update status indicator to show error (temporary, will be reset on next file or finish)
                self.status_indicator.setText("오류 발생")
                self.status_indicator.setStyleSheet("font-weight: bold; color: #f44336; padding: 5px;")

                # Update status icon
                status_icon_path = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    'assets', 'status-error.svg'
                )
                if hasattr(self, 'status_layout') and self.status_layout.count() > 0:
                    old_icon = self.status_layout.itemAt(0).widget()
                    if old_icon:
                        if 'status-error' not in old_icon.renderer().defaultSize().toString(): # Basic check
                            new_icon = QSvgWidget(status_icon_path)
                            new_icon.setFixedSize(24, 24)
                            self.status_layout.replaceWidget(old_icon, new_icon)
                            old_icon.deleteLater()

                # Show error message in status bar
                self.statusBar.showMessage(f"오류: {file_info}{message}", 5000)
                QMessageBox.warning(self, f"오류 ({file_info.strip()})", message)

            elif type == "finished":
                # This 'finished' signal comes from main_rpa for *one file*
                # We handle the final completion logic in processing_finished
                timestamp = datetime.now().strftime("%H:%M:%S")
                # Determine if it finished successfully based on the message content
                success = message.lower() == "true"
                if success:
                    self.status_text.append(f"<span style='color:#4CAF50;'>[{timestamp}] ✅ {file_info}처리 완료</span>")
                    self.progress_bar.setValue(100) # Mark current file as 100%
                else:
                    # Error likely occurred, already logged via 'error' signal
                     self.status_text.append(f"<span style='color:#f44336;'>[{timestamp}] ⚠️ {file_info}처리 중 오류 발생 또는 결과 없음</span>")
                     self.progress_bar.setValue(0) # Reset progress on failure? Or leave as is? Resetting.

                # Don't update overall status indicator here, wait for processing_finished

            elif type == "final_path":
                # Handle the output file path for the *current* file
                if message and not message.startswith("Error:"):
                    timestamp = datetime.now().strftime("%H:%M:%S")
                    self.status_text.append(f"[{timestamp}] 📄 {file_info}출력 파일 (업로드용): {message}")
                    # Store this path as the *potential* last successful path
                    self.last_upload_path = message
                    # Enable the button immediately if desired, or wait until all files finish
                    # self.open_file_btn.setEnabled(True) # Option 1: Enable now
                    logging.info(f"Upload path for file {self.current_file_index + 1} received: {message}")
                elif message.startswith("Error:"):
                    timestamp = datetime.now().strftime("%H:%M:%S")
                    self.status_text.append(f"<span style='color:#f44336;'>[{timestamp}] ❌ {file_info}출력 파일 생성 실패: {message}</span>")

        except Exception as e:
            logging.error(f"Progress update error: {str(e)}", exc_info=True)

    def _format_duration(self, duration):
        """시간을 읽기 쉬운 형식으로 변환"""
        total_seconds = duration.total_seconds()
        
        # 시간, 분, 초 계산
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        # 적절한 형식으로 포맷팅
        if hours > 0:
            return f"{int(hours)}시간 {int(minutes)}분 {int(seconds)}초"
        elif minutes > 0:
            return f"{int(minutes)}분 {int(seconds)}초"
        else:
            return f"{int(seconds)}초"

    def processing_finished(self, success, output_path):
        """Handle completion of a single file processing, then start next or finish all."""
        try:
            file_index = self.current_file_index
            file_info = f"[파일 {file_index + 1}/{len(self.input_files)}]"
            
            # 현재 처리한 파일의 소요 시간 계산
            if file_index in self.file_start_times:
                end_time = datetime.now()
                duration = end_time - self.file_start_times[file_index]
                self.file_durations[file_index] = duration
                
                # 소요 시간 출력
                duration_str = self._format_duration(duration)
                if success:
                    self.status_text.append(f"<span style='color:#4CAF50;'>{file_info} 처리 완료: 소요 시간 {duration_str}</span>")
                else:
                    self.status_text.append(f"<span style='color:#FF9800;'>{file_info} 처리 실패: 소요 시간 {duration_str}</span>")
            
            if success:
                logging.info(f"{file_info} processing finished successfully. Output: {output_path}")
                # output_path is now the upload_path due to previous changes
                if output_path:
                    self.last_upload_path = output_path # Update last known good path
            else:
                logging.warning(f"{file_info} processing failed or produced no output.")
                # Optionally add a specific log message here if not already covered by 'error' signals
                # self.status_text.append(f"<span style='color:#f44336;'>{file_info} 처리 실패 또는 결과 없음</span>")

            # Move to the next file
            self.current_file_index += 1

            if self.current_file_index < len(self.input_files):
                # More files to process, start the next one
                self.start_next_file_processing()
            else:
                # All files processed - 전체 처리 시간 계산
                total_duration = datetime.now() - self.total_start_time
                
                # 각 파일별 소요 시간 요약
                self.status_text.append("<span style='color:#2196F3;'>----- 파일별 처리 시간 요약 -----</span>")
                for idx, duration in sorted(self.file_durations.items()):
                    file_name = os.path.basename(self.input_files[idx])
                    status = "성공" if idx in self.file_durations else "실패"
                    duration_str = self._format_duration(duration)
                    color = "#4CAF50" if status == "성공" else "#FF9800"
                    self.status_text.append(f"<span style='color:{color};'>[파일 {idx + 1}] {file_name}: {duration_str}</span>")
                
                # 전체 소요 시간 출력
                total_files = len(self.input_files)
                success_files = len([f for f in self.file_durations.values() if f])
                self.status_text.append("<span style='color:#2196F3;'>-------------------------------</span>")
                total_duration_str = self._format_duration(total_duration)
                self.status_text.append(f"<span style='color:#4CAF50;'>전체 처리 완료: 총 {total_files}개 파일 중 {success_files}개 성공, 소요 시간 {total_duration_str}</span>")
                
                # 평균 처리 시간 계산 및 출력
                if success_files > 0:
                    avg_seconds = sum(d.total_seconds() for d in self.file_durations.values()) / success_files
                    avg_duration = datetime.timedelta(seconds=avg_seconds)
                    avg_duration_str = self._format_duration(avg_duration)
                    self.status_text.append(f"<span style='color:#2196F3;'>파일당 평균 처리 시간: {avg_duration_str}</span>")
                
                self.statusBar.showMessage(f"모든 파일 처리 완료: {total_files}개 중 {success_files}개 성공, 총 소요 시간 {total_duration_str}", 5000)

                # Reset state and buttons
                self.start_btn.setEnabled(True)
                if hasattr(self, 'stop_btn'):
                    self.stop_btn.setEnabled(False)
                self.current_file_index = -1 # Reset index

                # Update final status indicator and icon
                self.status_indicator.setText("준비됨")
                self.status_indicator.setStyleSheet("font-weight: bold; color: #4CAF50; padding: 5px;")
                status_icon_path = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    'assets', 'status-ready.svg'
                )
                if hasattr(self, 'status_layout') and self.status_layout.count() > 0:
                    old_icon = self.status_layout.itemAt(0).widget()
                    if old_icon:
                        new_icon = QSvgWidget(status_icon_path)
                        new_icon.setFixedSize(24, 24)
                        self.status_layout.replaceWidget(old_icon, new_icon)
                        old_icon.deleteLater()

                # Enable the "Open File" button ONLY if a successful path was recorded
                self.open_file_btn.setEnabled(self.last_upload_path is not None)

                if self.last_upload_path:
                     QMessageBox.information(self, "완료", f"모든 파일 처리가 완료되었습니다.\n총 {total_files}개 파일 중 {success_files}개 성공, 총 소요 시간 {total_duration_str}\n마지막 성공 업로드 파일: {self.last_upload_path}")
                else:
                     QMessageBox.warning(self, "완료", "모든 파일 처리가 완료되었지만, 성공적으로 생성된 업로드 파일이 없습니다.")

        except Exception as e:
            logging.error(f"Processing finished handler error: {str(e)}", exc_info=True)
            # Attempt to reset state partially on error
            self.start_btn.setEnabled(True)
            if hasattr(self, 'stop_btn'):
                self.stop_btn.setEnabled(False)
            self.current_file_index = -1

    def open_result_file(self):
        """Open the last successfully generated upload file"""
        try:
            # Use self.last_upload_path which stores the path from the last successful run
            if self.last_upload_path and os.path.exists(self.last_upload_path):
                if sys.platform == 'win32':
                    os.startfile(self.last_upload_path)
                elif sys.platform == 'darwin':  # macOS
                    import subprocess
                    subprocess.call(('open', self.last_upload_path))
                else:  # Linux
                    import subprocess
                    subprocess.call(('xdg-open', self.last_upload_path))
            elif self.last_upload_path:
                 QMessageBox.warning(self, "파일 열기 오류", f"파일을 찾을 수 없습니다: {self.last_upload_path}")
            else:
                QMessageBox.warning(self, "경고", "성공적으로 생성된 업로드 파일이 없습니다.")
        except Exception as e:
            logging.error(f"Error opening result file: {str(e)}")
            QMessageBox.warning(self, "오류", f"업로드 파일을 열 수 없습니다: {str(e)}")

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