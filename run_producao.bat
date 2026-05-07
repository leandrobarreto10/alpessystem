@echo off
cd /d "%~dp0"
if not exist ".venv" (
    python -m venv .venv
)
call ".venv\Scripts\activate.bat"
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
streamlit run app.py --server.address 0.0.0.0 --server.port 8502 --server.fileWatcherType poll
