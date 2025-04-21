import subprocess

def error_restart_rpa_bg():
    subprocess.Popen(["python", "C:/Users/USER2/AppData/Local/Programs/Python/PythonScript/Error_Restart_Rpa.py"], shell=True)
    print("함수를 종료합니다.")
    return