@echo off
echo ========================================
echo 重复文件查找工具 - 开发环境启动脚本
echo ========================================
echo.

REM 检查Python是否安装
python --version >nul 2>&1
if errorlevel 1 (
    echo 错误: 未找到Python，请先安装Python
    pause
    exit /b 1
)

REM 检查Node.js是否安装
node --version >nul 2>&1
if errorlevel 1 (
    echo 错误: 未找到Node.js，请先安装Node.js
    pause
    exit /b 1
)

echo 启动服务...
echo.

REM 启动后端服务
echo [1/2] 启动后端服务...
start "Backend - Flask Server" cmd /k "cd /d E:\workspace\python-tool\backend && echo 正在后端目录... && pip install -r requirements.txt && python app.py && echo 后端服务已启动，按任意键关闭..."

REM 等待3秒确保后端启动
timeout /t 3 /nobreak >nul

REM 启动前端服务
echo [2/2] 启动前端服务...
start "Frontend - Vue Server" cmd /k "cd /d E:\workspace\python-tool\frontend && echo 正在前端目录... && npm install && npm run dev && echo 前端服务已启动，按任意键关闭..."

echo.
echo ========================================
echo 服务启动完成！
echo.
echo 前端地址: http://localhost:3000
echo 后端地址: http://localhost:5000
echo.
echo 按任意键关闭此窗口...
pause >nul