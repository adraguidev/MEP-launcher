@echo off
chcp 65001 >nul 2>&1
setlocal EnableDelayedExpansion

:: ===================================================================
:: MEP Gatherer Launcher (BAT) — Alternativa al .exe
:: Integratel Peru — Stefanini Group
::
:: Replica la misma funcionalidad de MEP_Gatherer.exe para entornos
:: donde el antivirus bloquea el ejecutable PyInstaller.
:: Requiere: scripts\gather_sqlserver.ps1 y scripts\export_etl.ps1
:: ===================================================================

:: ---------------------------------------------------------------------------
:: Verificar elevacion de administrador
:: ---------------------------------------------------------------------------
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo.
    echo   [!] Se requieren permisos de Administrador.
    echo       Relanzando con elevacion UAC...
    powershell -Command "Start-Process cmd -ArgumentList '/c \"%~f0\"' -Verb RunAs"
    exit /b
)

:: ---------------------------------------------------------------------------
:: Verificar que los scripts existan
:: ---------------------------------------------------------------------------
set "SCRIPT_DIR=%~dp0scripts"
set "GATHER_PS1=%SCRIPT_DIR%\gather_sqlserver.ps1"
set "ETL_PS1=%SCRIPT_DIR%\export_etl.ps1"

if not exist "%GATHER_PS1%" (
    echo.
    echo   [ERROR] Script no encontrado: gather_sqlserver.ps1
    echo           Esperado en: %GATHER_PS1%
    echo.
    pause
    exit /b 1
)
if not exist "%ETL_PS1%" (
    echo.
    echo   [ERROR] Script no encontrado: export_etl.ps1
    echo           Esperado en: %ETL_PS1%
    echo.
    pause
    exit /b 1
)

:: ---------------------------------------------------------------------------
:: Header
:: ---------------------------------------------------------------------------
cls
echo ==============================================================
echo   MEP Gatherer — Stefanini Group
echo   Recolector automatizado de metadata SQL Server
echo ==============================================================
echo.
echo   [OK] Ejecutando como Administrador
echo.

:: ---------------------------------------------------------------------------
:: Deteccion automatica de instancias SQL Server (via registro)
:: ---------------------------------------------------------------------------
set INST_COUNT=0
for /f "tokens=1" %%A in ('reg query "HKLM\SOFTWARE\Microsoft\Microsoft SQL Server\Instance Names\SQL" 2^>nul ^| findstr /i "REG_SZ"') do (
    set /a INST_COUNT+=1
    if /i "%%A"=="MSSQLSERVER" (
        set "INST_!INST_COUNT!=%COMPUTERNAME%"
    ) else (
        set "INST_!INST_COUNT!=%COMPUTERNAME%\%%A"
    )
)

if %INST_COUNT% gtr 0 (
    echo   Instancias SQL Server detectadas:
    for /l %%i in (1,1,%INST_COUNT%) do (
        echo     %%i^) !INST_%%i!
    )
    echo     O^) Otra (ingresar manualmente^)
    echo.
    :choose_instance
    set "INST_CHOICE="
    set /p "INST_CHOICE=  Seleccione (1-%INST_COUNT%) o O para otra: "
    if /i "!INST_CHOICE!"=="O" goto manual_instance
    set /a "INST_NUM=INST_CHOICE" 2>nul
    if !INST_NUM! geq 1 if !INST_NUM! leq %INST_COUNT% (
        set "SERVER_INSTANCE=!INST_!INST_NUM!!"
        goto got_instance
    )
    echo   Opcion invalida.
    goto choose_instance
) else (
    goto manual_instance
)

:manual_instance
set "SERVER_INSTANCE="
set /p "SERVER_INSTANCE=  Instancia SQL Server (ej: MISERVIDOR, MISERVIDOR\INST1, 10.0.1.5,1433): "

:got_instance
if "%SERVER_INSTANCE%"=="" (
    echo.
    echo   [ERROR] Debe ingresar una instancia de SQL Server.
    echo.
    pause
    exit /b 1
)

:: ---------------------------------------------------------------------------
:: Header con instancia seleccionada
:: ---------------------------------------------------------------------------
cls
echo ==============================================================
echo   MEP Gatherer para %SERVER_INSTANCE%
echo   Fase 1: Metadata SQL Server  (gather_sqlserver.ps1)
echo   Fase 2: ETL/SSIS packages    (export_etl.ps1)
echo   Compatible con SQL Server 2008 R2 a 2022
echo ==============================================================
echo.

:: ---------------------------------------------------------------------------
:: Autenticacion
:: ---------------------------------------------------------------------------
echo   AUTENTICACION:
echo     W^) Windows (cuenta actual del equipo^)
echo     S^) SQL Server (usuario y password^)
echo.

:choose_auth
set "AUTH_CHOICE="
set /p "AUTH_CHOICE=  Seleccione W o S: "
if /i "%AUTH_CHOICE%"=="W" goto auth_windows
if /i "%AUTH_CHOICE%"=="S" goto auth_sql
echo   Opcion invalida. Ingrese W o S.
goto choose_auth

:auth_windows
echo.
echo   [OK] Se usara autenticacion Windows (integrada)
set "AUTH_PARAMS="
goto choose_action

:auth_sql
echo.
set "SQL_USER="
set "SQL_PASS="
set /p "SQL_USER=  Usuario SQL: "
set /p "SQL_PASS=  Password:    "
echo.
echo   [OK] Se usara autenticacion SQL Server
set "AUTH_PARAMS=-UseWindowsAuth false -SqlUser "%SQL_USER%" -SqlPassword "%SQL_PASS%""
goto choose_action

:: ---------------------------------------------------------------------------
:: Que ejecutar
:: ---------------------------------------------------------------------------
:choose_action
echo.
echo   QUE EJECUTAR:
echo     1^) Recolectar TODO - metadata + ETL (RECOMENDADO^)
echo     2^) Solo metadata (gather_sqlserver.ps1^)
echo     3^) Solo ETL/SSIS (export_etl.ps1^)
echo     4^) Custom (especificar BDs/schemas^)
echo     5^) Cancelar
echo.

:action_input
set "ACTION="
set /p "ACTION=  Seleccione (1-5): "
if "%ACTION%"=="1" goto run_all
if "%ACTION%"=="2" goto run_gather
if "%ACTION%"=="3" goto run_etl
if "%ACTION%"=="4" goto run_custom
if "%ACTION%"=="5" goto cancelled
echo   Opcion invalida.
goto action_input

:: ---------------------------------------------------------------------------
:: Ejecucion
:: ---------------------------------------------------------------------------
:run_all
echo.
echo === Fase 1/2: Recolectando metadata de SQL Server... ===
echo --------------------------------------------------------------
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%GATHER_PS1%" -ServerInstance "%SERVER_INSTANCE%" %AUTH_PARAMS%
echo --------------------------------------------------------------
echo.
echo === Fase 2/2: Exportando paquetes ETL/SSIS... ===
echo --------------------------------------------------------------
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%ETL_PS1%" -ServerInstance "%SERVER_INSTANCE%" %AUTH_PARAMS%
echo --------------------------------------------------------------
goto done

:run_gather
echo.
echo   Ejecutando solo metadata...
echo --------------------------------------------------------------
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%GATHER_PS1%" -ServerInstance "%SERVER_INSTANCE%" %AUTH_PARAMS%
echo --------------------------------------------------------------
goto done

:run_etl
echo.
echo   Ejecutando solo ETL/SSIS...
echo --------------------------------------------------------------
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%ETL_PS1%" -ServerInstance "%SERVER_INSTANCE%" %AUTH_PARAMS%
echo --------------------------------------------------------------
goto done

:run_custom
echo.
set "CUSTOM_DBS="
set "CUSTOM_SCHEMAS="
set /p "CUSTOM_DBS=  Bases de datos (separadas por coma, ENTER para todas): "
set /p "CUSTOM_SCHEMAS=  Schemas (separados por coma, ENTER para todos):      "
set "CUSTOM_PARAMS="
if not "%CUSTOM_DBS%"=="" set "CUSTOM_PARAMS=-Databases "%CUSTOM_DBS%""
if not "%CUSTOM_SCHEMAS%"=="" set "CUSTOM_PARAMS=%CUSTOM_PARAMS% -Schemas "%CUSTOM_SCHEMAS%""
echo.
echo === Fase 1/2: Metadata (custom)... ===
echo --------------------------------------------------------------
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%GATHER_PS1%" -ServerInstance "%SERVER_INSTANCE%" %AUTH_PARAMS% %CUSTOM_PARAMS%
echo --------------------------------------------------------------
echo.
echo === Fase 2/2: ETL/SSIS... ===
echo --------------------------------------------------------------
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%ETL_PS1%" -ServerInstance "%SERVER_INSTANCE%" %AUTH_PARAMS%
echo --------------------------------------------------------------
goto done

:cancelled
echo.
echo   Cancelado.
goto end

:done
echo.
echo ==============================================================
echo   COMPLETADO. Comprima las carpetas mep_* y entregue.
echo ==============================================================

:end
echo.
pause
