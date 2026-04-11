# PowerShell 启动脚本
Write-Host "========================================" -ForegroundColor Green
Write-Host "重复文件查找工具 - 开发环境启动" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""

# 检查Python
python --version > $null 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "错误: 未找到Python，请先安装Python" -ForegroundColor Red
    Read-Host "按任意键退出"
    exit
}

# 检查Node.js
node --version > $null 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "错误: 未找到Node.js，请先安装Node.js" -ForegroundColor Red
    Read-Host "按任意键退出"
    exit
}

Write-Host "正在启动服务..." -ForegroundColor Yellow
Write-Host ""

# 启动后端
Start-Process -FilePath "cmd.exe" -ArgumentList "/k cd backend && pip install -r requirements.txt && python app.py && echo 后端服务已启动，按任意键关闭..." -NoNewWindow -WindowStyle Normal

Start-Sleep -Seconds 3

# 启动前端
Start-Process -FilePath "cmd.exe" -ArgumentList "/k cd frontend && npm install && npm run dev && echo 前端服务已启动，按任意键关闭..." -NoNewWindow -WindowStyle Normal

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "服务启动完成！" -ForegroundColor Green
Write-Host "前端地址: http://localhost:3004" -ForegroundColor Cyan
Write-Host "后端地址: http://localhost:5000" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "按任意键关闭此窗口..." -ForegroundColor Gray
Read-Host