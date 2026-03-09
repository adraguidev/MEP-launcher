@echo off

chcp 65001 >nul 2>&1

setlocal EnableDelayedExpansion



:: ===================================================================

:: MEP Gatherer Launcher (BAT) - Alternativa al .exe

:: Integratel Peru - Stefanini Group

::

:: Usar cuando MEP_Gatherer.exe no arranca (Win7/Server2008R2,

:: DLL faltante, o antivirus bloqueando PyInstaller).

:: Requiere: carpeta scripts\ con gather_sqlserver.ps1 y export_etl.ps1

:: ===================================================================



title MEP Gatherer - Stefanini Group



:: ---------------------------------------------------------------------------

:: Verificar elevacion de administrador

:: Anti-loop: si se relanza con argumento ELEVATED, saltar el chequeo.

:: Esto evita el loop infinito en servidores donde el servicio LanmanServer

:: (requerido por "net session") esta detenido, haciendo que net session

:: siempre falle aunque el proceso ya sea administrador.

:: ---------------------------------------------------------------------------

if /i "%~1"=="ELEVATED" goto elevation_ok

:: Usar PowerShell para chequear admin (lee el token de seguridad directamente,

:: no depende de servicios como LanmanServer). Compatible con PS 2.0+.

powershell -NoProfile -Command "if(-not([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)){exit 1}" >nul 2>&1

if %errorlevel% neq 0 (

    echo.

    echo   [!] Se requieren permisos de Administrador.

    echo       Relanzando con elevacion UAC...

    powershell -Command "Start-Process cmd -ArgumentList '/c \"%~f0\" ELEVATED' -Verb RunAs" >nul 2>&1

    if %errorlevel% neq 0 (

        echo.

        echo   [ERROR] No se pudo elevar permisos automaticamente.

        echo           Haga clic derecho en MEP_Gatherer.bat

        echo           y seleccione "Ejecutar como administrador".

        echo.

        pause

        exit /b 1

    )

    exit /b

)

:elevation_ok



:: ---------------------------------------------------------------------------

:: Verificar PowerShell disponible

:: ---------------------------------------------------------------------------

where powershell.exe >nul 2>&1

if %errorlevel% neq 0 (

    echo.

    echo   [ERROR] PowerShell no encontrado en el PATH.

    echo.

    echo   Este equipo requiere Windows PowerShell 3.0 o superior.

    echo   Descargue WMF 4.0 (compatible con Windows 7 / Server 2008 R2):

    echo   https://www.microsoft.com/en-us/download/details.aspx?id=40855

    echo.

    pause

    exit /b 1

)



:: Obtener version de PowerShell

for /f "delims=" %%V in ('powershell -NoProfile -Command "$PSVersionTable.PSVersion.Major" 2^>nul') do set "PS_MAJOR=%%V"

if "!PS_MAJOR!"=="" set "PS_MAJOR=0"



:: ---------------------------------------------------------------------------

:: Verificar scripts

:: ---------------------------------------------------------------------------

set "SCRIPT_DIR=%~dp0scripts"

set "GATHER_PS1=%SCRIPT_DIR%\gather_sqlserver.ps1"

set "ETL_PS1=%SCRIPT_DIR%\export_etl.ps1"



if not exist "%GATHER_PS1%" (

    echo.

    echo   [ERROR] Script no encontrado: gather_sqlserver.ps1

    echo           Esperado en: %GATHER_PS1%

    echo.

    echo   Asegurese de que la carpeta "scripts\" este en el mismo

    echo   directorio que este .bat con ambos archivos .ps1 adentro.

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



:: Verificar sqlcmd (informativo, no bloquea)

set "HAS_SQLCMD=NO"

where sqlcmd >nul 2>&1

if %errorlevel% equ 0 set "HAS_SQLCMD=SI"



:: ---------------------------------------------------------------------------

:: Header de diagnostico

:: ---------------------------------------------------------------------------

cls

echo ==============================================================

echo   MEP Gatherer - Stefanini Group

echo   Recolector automatizado de metadata SQL Server

echo   Compatible con SQL Server 2008 R2 a 2022

echo ==============================================================

echo.

echo   [OK] Ejecutando como Administrador

echo   [OK] PowerShell !PS_MAJOR!.x detectado



if !PS_MAJOR! LSS 3 (

    echo   [WARN] PowerShell !PS_MAJOR!.x es muy antiguo. Se recomienda 3.0+.

    echo          Instale WMF 4.0: https://www.microsoft.com/download/details.aspx?id=40855

)



if "%HAS_SQLCMD%"=="SI" (

    echo   [OK] sqlcmd disponible

) else (

    echo   [WARN] sqlcmd no encontrado en PATH

    echo          Los scripts usaran Invoke-Sqlcmd como alternativa.

    echo          Si hay errores de conexion instale SQL Server Command Line Tools.

)

echo.



:: ---------------------------------------------------------------------------

:: Deteccion automatica de instancias SQL Server

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

    for /l %%i in (1,1,%INST_COUNT%) do echo     %%i^) !INST_%%i!

    echo     O^) Otra (ingresar manualmente^)

    echo.

    :choose_instance

    set "INST_CHOICE="

    set /p "INST_CHOICE=  Seleccione (1-%INST_COUNT%) o O para otra: "

    if /i "!INST_CHOICE!"=="O" goto manual_instance

    set /a "INST_NUM=INST_CHOICE" 2>nul

    if !INST_NUM! geq 1 if !INST_NUM! leq %INST_COUNT% (

        call set "SERVER_INSTANCE=%%INST_!INST_NUM!%%"

        goto got_instance

    )

    echo   Opcion invalida.

    goto choose_instance

) else (

    goto manual_instance

)



:manual_instance

set "SERVER_INSTANCE="

set /p "SERVER_INSTANCE=  Instancia SQL Server (ej: SERVIDOR, SERVIDOR\INST, 10.0.1.5,1433): "



:got_instance

if "%SERVER_INSTANCE%"=="" (

    echo.

    echo   [ERROR] Debe ingresar una instancia de SQL Server.

    echo.

    pause

    exit /b 1

)



:: ---------------------------------------------------------------------------

:: Header con instancia

:: ---------------------------------------------------------------------------

cls

echo ==============================================================

echo   MEP Gatherer para %SERVER_INSTANCE%

echo   Fase 1: Metadata SQL Server  (gather_sqlserver.ps1)

echo   Fase 2: ETL/SSIS packages    (export_etl.ps1)

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

echo   [OK] Autenticacion Windows

set "USE_WIN_AUTH=true"

set "SQL_USER="

set "SQL_PASS="

goto choose_action



:auth_sql

echo.

set "SQL_USER="

set "SQL_PASS="

set /p "SQL_USER=  Usuario SQL: "

set /p "SQL_PASS=  Password:    "

echo.

echo   [OK] Autenticacion SQL Server

set "USE_WIN_AUTH=false"

goto choose_action



:: ---------------------------------------------------------------------------

:: Que ejecutar

:: ---------------------------------------------------------------------------

:choose_action

echo.

echo   QUE EJECUTAR:

echo     1^) Recolectar TODO - metadata + ETL  [RECOMENDADO]

echo     2^) Solo metadata    (gather_sqlserver.ps1^)

echo     3^) Solo ETL/SSIS    (export_etl.ps1^)

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

call :exec_gather

echo --------------------------------------------------------------

if %EXEC_RC% neq 0 (

    echo.

    echo   [WARN] Fase 1 termino con codigo %EXEC_RC%.

    echo          Puede haber errores parciales. Continuando con Fase 2...

)

echo.

echo === Fase 2/2: Exportando paquetes ETL/SSIS... ===

echo --------------------------------------------------------------

call :exec_etl

echo --------------------------------------------------------------

if %EXEC_RC% neq 0 (

    echo.

    echo   [WARN] Fase 2 termino con codigo %EXEC_RC%.

    echo          Revise el log en la carpeta de salida mep_etl_*\

)

goto done



:run_gather

echo.

echo === Fase 1: Metadata SQL Server... ===

echo --------------------------------------------------------------

call :exec_gather

echo --------------------------------------------------------------

if %EXEC_RC% neq 0 (

    echo.

    echo   [WARN] Fase 1 termino con codigo %EXEC_RC%.

    echo          Revise el log en la carpeta mep_sqlserver_*\

)

goto done



:run_etl

echo.

echo === Fase 2: ETL/SSIS... ===

echo --------------------------------------------------------------

call :exec_etl

echo --------------------------------------------------------------

if %EXEC_RC% neq 0 (

    echo.

    echo   [WARN] Fase 2 termino con codigo %EXEC_RC%.

    echo          Revise el log en la carpeta mep_etl_*\

)

goto done



:run_custom

echo.

set "CUSTOM_DBS="

set "CUSTOM_SCHEMAS="

set /p "CUSTOM_DBS=  Bases de datos (separadas por coma, ENTER para todas): "

set /p "CUSTOM_SCHEMAS=  Schemas (separados por coma, ENTER para todos):      "

echo.

echo === Fase 1/2: Metadata (custom)... ===

echo --------------------------------------------------------------

call :exec_gather_custom

echo --------------------------------------------------------------

echo.

echo === Fase 2/2: ETL/SSIS... ===

echo --------------------------------------------------------------

call :exec_etl

echo --------------------------------------------------------------

goto done



:: ===========================================================================

:: SUBRUTINAS DE EJECUCION POWERSHELL

:: Separa auth Windows vs SQL para evitar problemas de comillas anidadas.

:: La contrasena se pasa directamente al proceso PS sin quedar visible

:: en el historial de comandos del sistema.

:: ===========================================================================



:exec_gather

set EXEC_RC=0

if "%USE_WIN_AUTH%"=="true" (

    powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%GATHER_PS1%" -ServerInstance "%SERVER_INSTANCE%"

) else (

    powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%GATHER_PS1%" -ServerInstance "%SERVER_INSTANCE%" -UseWindowsAuth false -SqlUser "%SQL_USER%" -SqlPassword "%SQL_PASS%"

)

set EXEC_RC=%errorlevel%

exit /b



:exec_gather_custom

set EXEC_RC=0

if "%USE_WIN_AUTH%"=="true" (

    if not "%CUSTOM_DBS%"=="" (

        if not "%CUSTOM_SCHEMAS%"=="" (

            powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%GATHER_PS1%" -ServerInstance "%SERVER_INSTANCE%" -Databases "%CUSTOM_DBS%" -Schemas "%CUSTOM_SCHEMAS%"

        ) else (

            powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%GATHER_PS1%" -ServerInstance "%SERVER_INSTANCE%" -Databases "%CUSTOM_DBS%"

        )

    ) else (

        powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%GATHER_PS1%" -ServerInstance "%SERVER_INSTANCE%"

    )

) else (

    if not "%CUSTOM_DBS%"=="" (

        if not "%CUSTOM_SCHEMAS%"=="" (

            powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%GATHER_PS1%" -ServerInstance "%SERVER_INSTANCE%" -UseWindowsAuth false -SqlUser "%SQL_USER%" -SqlPassword "%SQL_PASS%" -Databases "%CUSTOM_DBS%" -Schemas "%CUSTOM_SCHEMAS%"

        ) else (

            powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%GATHER_PS1%" -ServerInstance "%SERVER_INSTANCE%" -UseWindowsAuth false -SqlUser "%SQL_USER%" -SqlPassword "%SQL_PASS%" -Databases "%CUSTOM_DBS%"

        )

    ) else (

        powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%GATHER_PS1%" -ServerInstance "%SERVER_INSTANCE%" -UseWindowsAuth false -SqlUser "%SQL_USER%" -SqlPassword "%SQL_PASS%"

    )

)

set EXEC_RC=%errorlevel%

exit /b



:exec_etl

set EXEC_RC=0

if "%USE_WIN_AUTH%"=="true" (

    powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%ETL_PS1%" -ServerInstance "%SERVER_INSTANCE%"

) else (

    powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%ETL_PS1%" -ServerInstance "%SERVER_INSTANCE%" -UseWindowsAuth false -SqlUser "%SQL_USER%" -SqlPassword "%SQL_PASS%"

)

set EXEC_RC=%errorlevel%

exit /b



:: ---------------------------------------------------------------------------

:cancelled

echo.

echo   Cancelado.

goto end



:done

echo.

echo ==============================================================

echo   COMPLETADO. Comprima las carpetas mep_* y entreguela.

echo ==============================================================



:end

echo.

set "SQL_PASS="

pause

endlocal

