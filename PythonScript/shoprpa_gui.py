import os
import sys
import time
import pandas as pd
import threading
import logging
<<<<<<< HEAD
import configparser
import shutil

from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                             QPushButton, QLabel, QProgressBar, QFileDialog, QComboBox, 
                             QTextEdit, QTabWidget, QGridLayout, QGroupBox, QCheckBox,
                             QSpinBox, QDoubleSpinBox, QMessageBox, QSplashScreen, QFrame)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer, QSize
from PyQt5.QtGui import QPixmap, QFont, QIcon, QColor, QPalette

# Import Queue for progress updates
from multiprocessing import Queue, freeze_support
from queue import Empty as QueueEmpty # To handle empty queue exception

# Import the main RPA process
try:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from main_rpa import main as run_rpa
except ImportError as e:
    logging.error(f"Failed to import main_rpa module: {e}")
    run_rpa = None
    
# Worker thread for executing RPA process
class RPAWorker(QThread):
    update_progress = pyqtSignal(int, str)
    finished = pyqtSignal(str)
    error = pyqtSignal(str)
    
    def __init__(self, settings, parent=None):
        super().__init__(parent)
        self.running = False
        self.settings = settings
        # Create a queue for progress updates
        self.progress_queue = Queue()
        self.progress_timer = QTimer(self)
        self.progress_timer.timeout.connect(self.check_progress_queue)
=======
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
>>>>>>> hotfix
        
    def run(self):
        """Execute the RPA process in a separate thread"""
        self.running = True
        
        try:
<<<<<<< HEAD
            if run_rpa is None:
                self.error.emit("main_rpa 모듈을 로드할 수 없습니다. 설치를 확인하세요.")
                return
=======
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
>>>>>>> hotfix

            # Pass settings to the backend via environment variables
            os.environ['SHOPRPA_TEXT_THRESHOLD'] = str(self.settings.get('text_threshold', 0.7))
            os.environ['SHOPRPA_IMAGE_THRESHOLD'] = str(self.settings.get('image_threshold', 0.6))
            os.environ['SHOPRPA_TEXT_WEIGHT'] = str(self.settings.get('text_weight', 0.7))
            os.environ['SHOPRPA_IMAGE_WEIGHT'] = str(self.settings.get('image_weight', 0.3))
            os.environ['SHOPRPA_USE_BG_REMOVAL'] = str(int(self.settings.get('use_background_removal', True)))
            os.environ['SHOPRPA_PROCESS_TYPE'] = self.settings.get('process_type', 'A')
            
            # Start checking the queue periodically (e.g., every 200ms)
            self.progress_timer.start(200)
            
            # Run the actual RPA process, passing the queue
            logging.info("Starting main RPA process in worker thread...")
            result_path = run_rpa(progress_queue=self.progress_queue)
            logging.info(f"Main RPA process finished in worker thread. Result: {result_path}")
            
            # Ensure queue is checked one last time after process finishes
            self.check_progress_queue()
            
            if result_path and os.path.exists(result_path):
                # Final update might come from queue, but ensure 100% is set
                self.update_progress.emit(100, "완료!") 
                self.finished.emit(result_path)
            elif result_path is None:
                 # Error likely already emitted via queue (-1), but handle case where it returns None without error
                 if self.running: # Check if error wasn't already emitted
                     self.error.emit("RPA 처리가 완료되었으나 결과 경로를 받지 못했습니다.")
            else: # Path returned but doesn't exist
                 self.error.emit(f"RPA 처리 완료 후 결과 파일을 찾을 수 없습니다: {result_path}")
                
        except Exception as e:
            logging.error(f"Error in RPA thread run method: {e}", exc_info=True)
            # Ensure timer stops on unexpected error
            if self.progress_timer.isActive():
                self.progress_timer.stop()
            if self.running: # Avoid emitting error twice if already emitted via queue
                 self.error.emit(f"RPA 스레드 오류: {str(e)}")
        
        finally:
            self.running = False
            # Stop the timer if it's still active
            if self.progress_timer.isActive():
                self.progress_timer.stop()
                logging.debug("Progress timer stopped.")
            # Clean up environment variables
            for key in list(os.environ.keys()):
                if key.startswith('SHOPRPA_'):
                    del os.environ[key]

    def check_progress_queue(self):
        """Check the progress queue for updates from the backend process."""
        try:
            while not self.progress_queue.empty():
                progress_data = self.progress_queue.get_nowait()
                if isinstance(progress_data, tuple) and len(progress_data) == 2:
                    percent, message = progress_data
                    if percent == -1: # Error signal from backend
                        logging.error(f"Error received from RPA process: {message}")
                        if self.progress_timer.isActive():
                             self.progress_timer.stop()
                        if self.running: # Prevent duplicate error signals
                            self.error.emit(f"오류: {message}")
                        self.running = False # Mark as not running on error
                        break # Stop checking queue on error
                    else:
                        logging.debug(f"Progress update received: {percent}% - {message}")
                        self.update_progress.emit(percent, message)
                else:
                    logging.warning(f"Received unexpected data from progress queue: {progress_data}")
        except QueueEmpty:
            pass # Queue is empty, nothing to do
        except Exception as e:
            logging.error(f"Error checking progress queue: {e}", exc_info=True)
            # Stop timer on error
            if self.progress_timer.isActive():
                self.progress_timer.stop()

class ShopRPAApp(QMainWindow):
    def __init__(self):
        super().__init__()
        
        # Set window title and dimensions
        self.setWindowTitle("ShopRPA - 해오름기프트 RPA 시스템 v1.1")
        self.setMinimumSize(900, 600)
        
        # Initialize UI
        self.init_ui()
        
        # Initialize worker thread (will be created on start)
        self.worker = None
        
        # Load settings on startup
        self.load_settings()
        
    def init_ui(self):
        """Initialize the user interface"""
        # Create central widget and main layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        # Create tabs
        tabs = QTabWidget()
        main_tab = QWidget()
        settings_tab = QWidget()
        log_tab = QWidget()
        
        tabs.addTab(main_tab, "메인")
        tabs.addTab(settings_tab, "설정")
        tabs.addTab(log_tab, "로그")
        
        # Set up main tab
        main_layout_tab = QVBoxLayout(main_tab)
        
        # Header with title and logo
        header_layout = QHBoxLayout()
        logo_label = QLabel()
        logo_path = os.path.join(os.path.dirname(__file__), "logo.png")
        logo_pixmap = QPixmap(logo_path)
        if not logo_pixmap.isNull():
            logo_label.setPixmap(logo_pixmap.scaled(80, 80, Qt.KeepAspectRatio))
        else:
            logo_label.setText("ShopRPA")
            logo_label.setFont(QFont("Arial", 20, QFont.Bold))
        
        header_layout.addWidget(logo_label)
        
        title_label = QLabel("해오름기프트 상품 가격비교 RPA 시스템")
        title_label.setFont(QFont("Arial", 16, QFont.Bold))
        header_layout.addWidget(title_label)
        header_layout.addStretch()
        
        main_layout_tab.addLayout(header_layout)
        
        # Add separator line
        separator = QFrame()
        separator.setFrameShape(QFrame.HLine)
        separator.setFrameShadow(QFrame.Sunken)
        main_layout_tab.addWidget(separator)
        
        # File input section
        file_group = QGroupBox("입력 파일")
        file_layout = QHBoxLayout(file_group)
        
        self.file_path_label = QLabel("C:\RPA\Input 폴더의 첫 번째 Excel 파일을 사용합니다.")
        file_layout.addWidget(self.file_path_label)
        
        browse_button = QPushButton("입력 폴더 열기")
        browse_button.clicked.connect(self.open_input_folder)
        file_layout.addWidget(browse_button)
        
        main_layout_tab.addWidget(file_group)
        
        # Options section (Simplified on Main Tab)
        options_group = QGroupBox("주요 옵션")
        options_layout = QGridLayout(options_group)
        
        # Process type selection
        options_layout.addWidget(QLabel("처리 유형:"), 0, 0)
        self.process_type = QComboBox()
        self.process_type.addItems(["승인관리 (A)", "가격관리 (P)"])
        options_layout.addWidget(self.process_type, 0, 1)
        
        options_layout.addWidget(QLabel("텍스트 유사도 임계값:"), 1, 0)
        self.text_threshold = QDoubleSpinBox()
        self.text_threshold.setRange(0.1, 1.0)
        self.text_threshold.setSingleStep(0.05)
        self.text_threshold.setValue(0.7)
        options_layout.addWidget(self.text_threshold, 1, 1)
        
        options_layout.addWidget(QLabel("이미지 유사도 임계값:"), 2, 0)
        self.image_threshold = QDoubleSpinBox()
        self.image_threshold.setRange(0.1, 1.0)
        self.image_threshold.setSingleStep(0.05)
        self.image_threshold.setValue(0.6)
        options_layout.addWidget(self.image_threshold, 2, 1)
        
        main_layout_tab.addWidget(options_group)
        
        # Progress section
        progress_group = QGroupBox("진행 상황")
        progress_layout = QVBoxLayout(progress_group)
        
        self.progress_bar = QProgressBar()
        progress_layout.addWidget(self.progress_bar)
        
        self.status_label = QLabel("대기 중...")
        progress_layout.addWidget(self.status_label)
        
        main_layout_tab.addWidget(progress_group)
        
        # Action buttons
        button_layout = QHBoxLayout()
        
        self.start_button = QPushButton("시작")
        self.start_button.setFont(QFont("Arial", 12, QFont.Bold))
        self.start_button.setMinimumHeight(40)
        self.start_button.clicked.connect(self.start_process)
        button_layout.addWidget(self.start_button)
        
        self.stop_button = QPushButton("중지")
        self.stop_button.setFont(QFont("Arial", 12))
        self.stop_button.setMinimumHeight(40)
        self.stop_button.setEnabled(False)
        self.stop_button.clicked.connect(self.stop_process)
        button_layout.addWidget(self.stop_button)
        
        main_layout_tab.addLayout(button_layout)
        
        # --- Set up settings tab ---
        settings_layout = QVBoxLayout(settings_tab)
        
        paths_group = QGroupBox("경로 설정")
        paths_layout = QGridLayout(paths_group)
        
        paths_layout.addWidget(QLabel("입력 폴더:"), 0, 0)
        self.input_dir_edit = QTextEdit("C:\\RPA\\Input")
        self.input_dir_edit.setMaximumHeight(30)
        paths_layout.addWidget(self.input_dir_edit, 0, 1)
        
        paths_layout.addWidget(QLabel("출력 폴더:"), 1, 0)
        self.output_dir_edit = QTextEdit("C:\\RPA\\Output")
        self.output_dir_edit.setMaximumHeight(30)
        paths_layout.addWidget(self.output_dir_edit, 1, 1)

        paths_layout.addWidget(QLabel("임시 폴더:"), 2, 0)
        self.temp_dir_edit = QTextEdit("C:\\RPA\\Temp")
        self.temp_dir_edit.setMaximumHeight(30)
        paths_layout.addWidget(self.temp_dir_edit, 2, 1)

        paths_layout.addWidget(QLabel("메인 이미지 폴더:"), 3, 0)
        self.main_img_dir_edit = QTextEdit("C:\\RPA\\Image\\Main")
        self.main_img_dir_edit.setMaximumHeight(30)
        paths_layout.addWidget(self.main_img_dir_edit, 3, 1)

        paths_layout.addWidget(QLabel("타겟 이미지 폴더:"), 4, 0)
        self.target_img_dir_edit = QTextEdit("C:\\RPA\\Image\\Target")
        self.target_img_dir_edit.setMaximumHeight(30)
        paths_layout.addWidget(self.target_img_dir_edit, 4, 1)
        
        settings_layout.addWidget(paths_group)
        
        advanced_group = QGroupBox("매칭 상세 설정")
        advanced_layout = QGridLayout(advanced_group)
        
        advanced_layout.addWidget(QLabel("배경 제거 사용:"), 0, 0)
        self.use_background_removal = QCheckBox()
        self.use_background_removal.setChecked(True)
        advanced_layout.addWidget(self.use_background_removal, 0, 1)
        
        advanced_layout.addWidget(QLabel("텍스트 가중치 (0.1 ~ 1.0):"), 1, 0)
        self.text_weight = QDoubleSpinBox()
        self.text_weight.setRange(0.1, 1.0)
        self.text_weight.setSingleStep(0.05)
        self.text_weight.setValue(0.7)
        advanced_layout.addWidget(self.text_weight, 1, 1)
        
        advanced_layout.addWidget(QLabel("이미지 가중치 (0.0 ~ 0.9):"), 2, 0)
        self.image_weight = QDoubleSpinBox()
        self.image_weight.setRange(0.0, 0.9)
        self.image_weight.setSingleStep(0.05)
        self.image_weight.setValue(0.3)
        advanced_layout.addWidget(self.image_weight, 2, 1)
        
<<<<<<< HEAD
        settings_layout.addWidget(advanced_group)
        
        # Save settings button
        save_settings_button = QPushButton("설정 저장")
        save_settings_button.clicked.connect(self.save_settings)
        settings_layout.addWidget(save_settings_button)
        
        settings_layout.addStretch()
        
        # --- Set up log tab ---
        log_layout = QVBoxLayout(log_tab)
        
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        log_layout.addWidget(self.log_text)
        
        clear_log_button = QPushButton("로그 지우기")
        clear_log_button.clicked.connect(self.clear_log)
        log_layout.addWidget(clear_log_button)
        
        # Add tabs to main layout
        main_layout.addWidget(tabs)
        
        # Footer with version information
        footer_layout = QHBoxLayout()
        footer_layout.addStretch()
        version_label = QLabel("ShopRPA v1.1")
        footer_layout.addWidget(version_label)
        
        main_layout.addLayout(footer_layout)
        
    def open_input_folder(self):
        """Opens the input folder in File Explorer"""
        input_dir = self.input_dir_edit.toPlainText()
        if os.path.isdir(input_dir):
            try:
                os.startfile(input_dir)
            except Exception as e:
                self.log_message(f"입력 폴더를 열 수 없습니다: {str(e)}")
                QMessageBox.warning(self, "폴더 열기 실패", f"폴더를 열 수 없습니다: {input_dir}")
        else:
            QMessageBox.warning(self, "폴더 없음", f"설정된 입력 폴더를 찾을 수 없습니다: {input_dir}")
    
    def start_process(self):
        """Start the RPA process in a worker thread."""
        if self.worker and self.worker.isRunning():
            QMessageBox.warning(self, "실행 중", "RPA 프로세스가 이미 실행 중입니다.")
            return
            
        # Save current settings before starting
        self.save_settings()
        
        # Disable start button, enable stop button
        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        self.status_label.setText("RPA 프로세스 시작 중...")
        self.progress_bar.setValue(0)
        
        # Create and start the worker thread
        current_settings = self.load_settings() # Load potentially updated settings
        self.worker = RPAWorker(current_settings)
        self.worker.update_progress.connect(self.update_progress)
        self.worker.finished.connect(self.process_finished)
        self.worker.error.connect(self.show_error)
        
        self.worker.start()
        self.status_label.setText("RPA 프로세스 실행 중...")
    
    def stop_process(self):
        """Stop the RPA process"""
        if not self.worker or not self.worker.isRunning():
            return
            
        reply = QMessageBox.question(
            self, '처리 중지', 
            "RPA 작업을 중지하시겠습니까? 진행 중인 모든 작업이 취소됩니다.\n(주의: 강제 종료는 불안정할 수 있습니다)",
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            self.log_message("RPA 작업 중지 요청...")
            # Terminate is forceful and might leave resources hanging or corrupt data.
            # A cleaner approach involves the worker checking a flag periodically.
            self.worker.terminate()
            self.worker.wait() # Wait for termination
            
            # Update UI
            self.start_button.setEnabled(True)
            self.stop_button.setEnabled(False)
            self.progress_bar.setValue(0)
            self.status_label.setText("중지됨")
            
            self.log_message("RPA 작업이 중지되었습니다.")
            self.worker = None # Clear the worker
    
    def update_progress(self, value, message):
        """Update progress bar and status label"""
        self.progress_bar.setValue(value)
        self.status_label.setText(message)
        self.log_message(message)
    
    def process_finished(self, result_path):
        """Handle successful completion of the RPA process."""
        # Stop the progress timer if the worker is done
        if self.worker and self.worker.progress_timer.isActive():
             self.worker.progress_timer.stop()
             
        self.status_label.setText(f"완료! 결과 파일: {result_path}")
        self.progress_bar.setValue(100)
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        QMessageBox.information(self, "완료", f"RPA 처리가 성공적으로 완료되었습니다.\n결과 파일: {result_path}")
        # Optionally open the output directory
        try:
            output_dir = os.path.dirname(result_path)
            os.startfile(output_dir)
        except Exception as e:
            logging.warning(f"Could not open output directory '{output_dir}': {e}")
            
        self.worker = None # Clear worker reference
    
    def show_error(self, message):
        """Display an error message."""
        # Stop the progress timer if the worker is done or errored
        if self.worker and self.worker.progress_timer.isActive():
             self.worker.progress_timer.stop()
             
        self.status_label.setText(f"오류 발생: {message}")
        self.progress_bar.setValue(0) # Or keep last value?
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        QMessageBox.critical(self, "오류", message)
        self.worker = None # Clear worker reference
    
    def save_settings(self):
        """Save settings to config file (INI format)"""
        config = configparser.ConfigParser()
        config['Paths'] = {
            'input_dir': self.input_dir_edit.toPlainText(),
            'output_dir': self.output_dir_edit.toPlainText(),
            'temp_dir': self.temp_dir_edit.toPlainText(),
            'main_img_dir': self.main_img_dir_edit.toPlainText(),
            'target_img_dir': self.target_img_dir_edit.toPlainText(),
        }
        config['Matching'] = {
            'text_threshold': str(self.text_threshold.value()),
            'image_threshold': str(self.image_threshold.value()),
            'text_weight': str(self.text_weight.value()),
            'image_weight': str(self.image_weight.value()),
            'use_background_removal': str(int(self.use_background_removal.isChecked()))
        }
        config['General'] = {
            'process_type': 'A' if self.process_type.currentText().startswith('승인관리') else 'P'
        }
        
        try:
            config_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(config_dir, "shoprpa_gui_config.ini")
            
            with open(config_path, 'w', encoding='utf-8') as configfile:
                config.write(configfile)
            
            QMessageBox.information(self, "설정 저장", "설정이 shoprpa_gui_config.ini 파일에 저장되었습니다.")
            self.log_message("설정이 저장되었습니다.")
            
        except Exception as e:
            QMessageBox.critical(self, "설정 저장 오류", f"설정을 저장하는 중 오류가 발생했습니다: {str(e)}")
            self.log_message(f"설정 저장 오류: {str(e)}")
    
    def load_settings(self):
        """Load settings from config file (INI format)"""
        config = configparser.ConfigParser()
        config_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(config_dir, "shoprpa_gui_config.ini")
        
        if not os.path.exists(config_path):
            self.log_message("설정 파일(shoprpa_gui_config.ini)을 찾을 수 없습니다. 기본값을 사용합니다.")
            return
            
        try:
            config.read(config_path, encoding='utf-8')
            
            # Load Paths
            if 'Paths' in config:
                self.input_dir_edit.setText(config['Paths'].get('input_dir', "C:\\RPA\\Input"))
                self.output_dir_edit.setText(config['Paths'].get('output_dir', "C:\\RPA\\Output"))
                self.temp_dir_edit.setText(config['Paths'].get('temp_dir', "C:\\RPA\\Temp"))
                self.main_img_dir_edit.setText(config['Paths'].get('main_img_dir', "C:\\RPA\\Image\\Main"))
                self.target_img_dir_edit.setText(config['Paths'].get('target_img_dir', "C:\\RPA\\Image\\Target"))
            
            # Load Matching settings
            if 'Matching' in config:
                self.text_threshold.setValue(config['Matching'].getfloat('text_threshold', 0.7))
                self.image_threshold.setValue(config['Matching'].getfloat('image_threshold', 0.6))
                self.text_weight.setValue(config['Matching'].getfloat('text_weight', 0.7))
                self.image_weight.setValue(config['Matching'].getfloat('image_weight', 0.3))
                self.use_background_removal.setChecked(config['Matching'].getboolean('use_background_removal', True))
                
            # Load General settings
            if 'General' in config:
                process_type = config['General'].get('process_type', 'A')
                self.process_type.setCurrentIndex(0 if process_type == 'A' else 1)
            
            self.log_message("설정을 로드했습니다.")
            
        except Exception as e:
            self.log_message(f"설정 로드 오류: {str(e)}")
            QMessageBox.warning(self, "설정 로드 오류", "설정 파일을 읽는 중 오류가 발생했습니다. 기본값이 사용될 수 있습니다.")
    
    def log_message(self, message):
        """Add a message to the log"""
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        self.log_text.append(f"[{timestamp}] {message}")
        
        # Also log to file (ensure logger is configured)
        try:
            logging.info(message)
        except NameError:
             # Handle case where logging might not be fully set up yet
             print(f"[{timestamp}] {message}") 
    
    def clear_log(self):
        """Clear the log display"""
        self.log_text.clear()

    def closeEvent(self, event):
        """Handle window close event"""
        if self.worker and self.worker.isRunning():
            reply = QMessageBox.question(
                self, '확인',
                "RPA 작업이 진행 중입니다. 종료하시겠습니까?",
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No
            )
            if reply == QMessageBox.Yes:
                self.stop_process() # Attempt to stop gracefully
                event.accept()
            else:
                event.ignore()
        else:
            event.accept()


def main():
    # Configure logging (ensure it's configured before use)
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s',
                        handlers=[logging.StreamHandler()]) # Log to console for GUI app startup

    # Required for multiprocessing support when running as executable
    freeze_support()
    
    app = QApplication(sys.argv)
    
    # Set application style
    app.setStyle("Fusion")
    
    # Show splash screen
    splash_path = os.path.join(os.path.dirname(__file__), "splash.png")
    splash_pixmap = QPixmap(splash_path)
    if splash_pixmap.isNull():
        # Create a default splash screen if image is not available
        splash_pixmap = QPixmap(500, 300)
        splash_pixmap.fill(QColor(50, 100, 250))
    
    splash = QSplashScreen(splash_pixmap)
    splash.show()
    splash.showMessage("ShopRPA 로딩 중...", Qt.AlignBottom | Qt.AlignCenter, Qt.white)
    app.processEvents()
    
    # Create main window
    window = ShopRPAApp()
    
    # Show main window and close splash screen after a delay
    QTimer.singleShot(1500, lambda: (window.show(), splash.finish(window)))
    
    # Run application
    sys.exit(app.exec_())

if __name__ == "__main__":
    main() 
=======
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

>>>>>>> hotfix
