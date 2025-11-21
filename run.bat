@echo off
chcp 65001 >nul
title Запуск пайплайна обработки фидбеков
cd /d "%~dp0"

echo ========================================
echo Запуск пайплайна обработки фидбеков
echo ========================================
echo.

echo [1/5] Запуск Docker Compose...
docker-compose up -d
if errorlevel 1 (
    echo ❌ Ошибка при запуске Docker Compose!
    pause
    exit /b 1
)
echo ✅ Docker Compose запущен успешно
echo.

echo [2/5] Ожидание запуска сервисов (10 секунд)...
timeout /t 10 /nobreak >nul
echo.

echo [3/5] Запуск Python скриптов...
uv run python ingest.py && (
    echo ✅ ingest.py завершен успешно
    echo.

    uv run python generate.py && (
        echo ✅ generate.py завершен успешно
        echo.

        uv run python send.py
        if errorlevel 1 (
            echo ❌ Ошибка в send.py!
        ) else (
            echo ✅ send.py завершен успешно
        )
    ) || (
        echo ❌ Ошибка в generate.py!
    )
) || (
    echo ❌ Ошибка в ingest.py!
)

echo.
echo [6/6] Остановка Docker Compose...
docker-compose down
echo ✅ Docker Compose остановлен
echo.

echo ========================================
echo Процесс завершен!
echo ========================================
pause