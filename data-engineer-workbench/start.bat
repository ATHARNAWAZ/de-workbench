@echo off
echo Starting Senior Data Engineer Workbench...
echo.

echo [1/2] Starting Backend (FastAPI)...
cd backend
start /B venv\Scripts\uvicorn main:app --host 0.0.0.0 --port 8000 --reload
cd ..

echo [2/2] Starting Frontend (Vite)...
cd frontend
start npm run dev
cd ..

echo.
echo Backend:  http://localhost:8000
echo API Docs: http://localhost:8000/docs
echo Frontend: http://localhost:5173
echo.
pause
