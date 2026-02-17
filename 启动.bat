@echo off
title QQQ Market Analyser
echo 正在启动 QQQ 市场分析器...

:: 启动 Flask 服务器（后台运行）
start /B "" "C:\Users\li\.conda\envs\kagg\python.exe" "%~dp0app.py"

:: 等待服务器就绪
timeout /t 3 /nobreak >nul

:: 自动打开浏览器
start "" "http://localhost:5000"

echo.
echo 服务器已启动，浏览器已打开。
echo 关闭此窗口将停止服务器。
echo.
pause
