# rthook_workdir.py — PyInstaller runtime hook
# exe 실행 시 작업 디렉토리를 exe 파일 위치로 설정
import sys, os
if getattr(sys, "frozen", False):
    os.environ.setdefault("ELT_RUNNER_WORKDIR", os.path.dirname(sys.executable))
