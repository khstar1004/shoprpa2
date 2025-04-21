import os
import time
import openpyxl
import pandas as pd
import shutil
import psutil
import pyautogui
import pygetwindow as gw
from plyer import notification
from natsort import natsorted

# 트레이 알림
def send_tray_notification(title, message):
    notification.notify(
        title=title,
        message=message,
        app_name='RPA Notification',
        timeout=10  # 알림 시간 10
    )

# 대기시간 알람
def notify_with_interval(title, message, total_duration, interval):
    elapsed_time = 0
    while elapsed_time < total_duration:
        send_tray_notification(title, f"{message} - 남은 시간: {total_duration - elapsed_time}초")
        time.sleep(interval)
        elapsed_time += interval

# 엑셀파일 이동
def move_first_xlsx(source_dir, target_dir):
    files = [f for f in os.listdir(source_dir) if f.endswith('.xlsx')]
    files = natsorted(files)  # 파일 이름으로 정렬합니다.

    if files:
        source_path = os.path.join(source_dir, files[0])  # 이동할 파일의 전체 경로
        target_path = os.path.join(target_dir, files[0])  # 대상 경로
        
        shutil.move(source_path, target_path)  # 파일 이동
        print(f"'{files[0]}' 파일을 '{source_dir}'에서 '{target_dir}'로 이동했습니다.")
        return True
    else:
        print(f"'{source_dir}'에 .xlsx 파일이 없습니다.")
        return False

# 재시작 행 번호 입력
def restart_index_num(file_path, config_workbook_path):
    # 지정된 폴더에서 .xlsx 파일 목록을 가져옵니다.
    xlsx_files = [f for f in os.listdir(file_path) if f.endswith('.xlsx')]

    # 파일이 존재하는지 확인하고 첫 번째 파일을 pandas DataFrame으로 로드합니다.
    if xlsx_files:
        first_xlsx = os.path.join(file_path, xlsx_files[0])
        df = pd.read_excel(first_xlsx, engine='openpyxl')
        
        # 컬럼 이름의 공백 제거
        df.columns = df.columns.str.strip()
        
        # "고려기프트 상품링크"와 "네이버 쇼핑 링크" 컬럼에서 값이 공백인 행을 찾습니다.
        empty_link_rows = df[df["고려기프트 상품링크"].isna() & df["네이버 쇼핑 링크"].isna()]

        if not empty_link_rows.empty:
            first_empty_link_row_index = empty_link_rows.index[0]
            print(f"'고려기프트 상품링크'와 '네이버 쇼핑 링크'가 빈 첫 번째 행의 인덱스는: {first_empty_link_row_index}입니다.")
        else:
            print("'고려기프트 상품링크'와 '네이버 쇼핑 링크'가 빈 행이 없습니다.")
    else:
        print("지정된 디렉토리에 .xlsx 파일이 없습니다.")

    if first_empty_link_row_index != 0:
        # 행 번호 입력
        excel_row_index = first_empty_link_row_index + 2
        # 첫번째 시트
        config_workbook = openpyxl.load_workbook(config_workbook_path)
        config_sheet = config_workbook.active
        # C2에 입력
        config_sheet['C2'] = excel_row_index
        # 저장
        config_workbook.save(config_workbook_path)
        # 종료
        config_workbook.close()

        print(f"값 {excel_row_index}이(가) {config_workbook_path}의 C2에 성공적으로 입력되었습니다.")
    else:
        print('입력된 값이 없습니다.')

# 프로세스 강제종료
def kill_process_by_name(process_name):
    for process in psutil.process_iter():
        try:
            if process_name.lower() in process.name().lower():
                process.terminate()  # 프로세스 종료
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

# rpa 실행
def rpa_run(list_image_path, rpa_process_image_path, rpa_run_image_path, program_title, confidence_value=0.8):
    # 프로그램 창의 위치와 크기를 얻습니다.
    windows = gw.getWindowsWithTitle(program_title)
    if windows:
        window = windows[0]
        region = (window.left, window.top, window.width, window.height)
        
        # 이동한 후 클릭하려는 이미지 위치를 찾습니다.
        click_location = pyautogui.locateOnScreen(list_image_path, region=region, confidence=confidence_value)
        if click_location:
            click_center = pyautogui.center(click_location)
            pyautogui.click(click_center)  # rpa_list 이미지의 중앙 위치를 클릭합니다.
            
            # 클릭한 후 일정 시간 동안 대기합니다.
            time.sleep(2)
            
            # 대기 시간 후 rpa_process 이미지의 위치를 찾아서 마우스를 이동합니다.
            hover_location = pyautogui.locateOnScreen(rpa_process_image_path, region=region, confidence=confidence_value)
            if hover_location:
                hover_center = pyautogui.center(hover_location)  
                pyautogui.moveTo(hover_center)  # rpa_process 이미지의 중앙 위치로 마우스를 이동합니다.

                # 클릭한 후 일정 시간 동안 대기합니다.
                time.sleep(2)
                
                # 마우스 이동이 끝난 후 rpa_run 이미지를 클릭합니다.
                run_location = pyautogui.locateOnScreen(rpa_run_image_path, region=region, confidence=confidence_value)
                if run_location:
                    run_center = pyautogui.center(run_location)
                    pyautogui.click(run_center)  # run 이미지의 중앙 위치를 클릭합니다.
                else:
                    print(f"{program_title} 프로그램 내에서 {rpa_run_image_path} 이미지를 찾을 수 없습니다!")
            else:
                print(f"{program_title} 프로그램 내에서 {rpa_process_image_path} 이미지 클릭 후에 찾을 수 없습니다!")
        else:
            print(f"{program_title} 프로그램 내에서 {list_image_path} 이미지를 찾을 수 없습니다!")
    else:
        print(f"{program_title} 프로그램 창을 찾을 수 없습니다!")

def rpa_restart():
    source_dir = r"C:\RPA\Input\Queue"
    target_dir = r"C:\RPA\Input"
    config_workbook_path = 'C:\\RPA\\UserConfig.xlsx'
    process_name = "aworks_bot"
    list_image_path = r"C:\RPA\Image\Restart\rpa_list.jpg"
    rpa_process_image_path = r"C:\RPA\Image\Restart\rpa_process.jpg"
    rpa_run_image_path = r"C:\RPA\Image\Restart\rpa_run.jpg"
    program_title = "aworks_mini" # 확인하고 싶은 프로세스의 이름


    notify_with_interval('RPA Restart', '60초 대기 중', 60, 10)
    send_tray_notification('RPA Restart', 'rpa 실행')
    kill_process_by_name(process_name)
    if not move_first_xlsx(source_dir, target_dir):  # 파일이 없으면 False를 반환합니다.
        send_tray_notification('RPA Notification', '필요한 .xlsx 파일이 없어 작업을 종료합니다.')
        time.sleep(5)
        return  # 파일이 없으므로 함수를 종료합니다.
    restart_index_num(target_dir, config_workbook_path)
    rpa_run(list_image_path, rpa_process_image_path, rpa_run_image_path, program_title)

if __name__ == "__main__":
    rpa_restart()