@echo off

chcp 65001 >nul 2>&1

setlocal EnableDelayedExpansion
set "LAUNCHER_LOG=%~dp0MEP_Gatherer_launcher.log"
set "LAUNCHER_FALLBACK_LOG=%TEMP%\MEP_Gatherer_launcher.log"
call :log_line INFO "Inicio de launcher desde %~dp0"



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

:: Verificar PowerShell disponible

:: ---------------------------------------------------------------------------

where powershell.exe >nul 2>&1

if %errorlevel% neq 0 (

    call :log_error "PowerShell no encontrado en PATH."

    echo.

    echo   [ERROR] PowerShell no encontrado en el PATH.

    echo.

    echo   Este equipo requiere Windows PowerShell 2.0 o superior.

    echo   En Windows 7 / Server 2008 R2 normalmente viene instalado por defecto.

    echo   Si falta, repare la instalacion de PowerShell/WMF antes de continuar.

    echo.

    pause

    exit /b 1

)



:: ---------------------------------------------------------------------------

:: Verificar elevacion de administrador

:: NO intenta auto-elevarse (causa ventanas que cierran solas en WS2008R2).

:: Si no es admin, muestra instrucciones claras y espera.

:: ---------------------------------------------------------------------------

powershell -NoProfile -Command "if(-not([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)){exit 1}" >nul 2>&1

if %errorlevel% neq 0 (

    call :log_error "El launcher no se ejecuto como Administrador."

    echo.

    echo   ============================================================

    echo   ERROR: Se requieren permisos de Administrador.

    echo   ============================================================

    echo.

    echo   Cierre esta ventana y ejecute el BAT de una de estas formas:

    echo.

    echo   OPCION 1 - Clic derecho sobre MEP_Gatherer.bat

    echo             Seleccionar "Ejecutar como administrador"

    echo.

    echo   OPCION 2 - Abrir cmd.exe como Administrador y escribir:

    echo             cd /d "%~dp0"

    echo             MEP_Gatherer.bat

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

    call :log_error "No se encontro gather_sqlserver.ps1 en %GATHER_PS1%"

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

    call :log_error "No se encontro export_etl.ps1 en %ETL_PS1%"

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

set "EXEC_RC=0"
set "OVERALL_RC=0"
set "SQL_PASS_B64="
set "MEP_SQLPASSWORD_B64="



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

    call :log_info "sqlcmd detectado en PATH."

    echo   [OK] sqlcmd disponible

) else (

    call :log_warn "sqlcmd no encontrado en PATH. Se intentara fallback a Invoke-Sqlcmd."

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

    call :log_error "No se ingreso instancia SQL Server."

    echo.

    echo   [ERROR] Debe ingresar una instancia de SQL Server.

    echo.

    pause

    exit /b 1

)

call :log_info "Instancia SQL seleccionada: %SERVER_INSTANCE%"



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

call :log_info "Autenticacion seleccionada: Windows"

set "USE_WIN_AUTH=true"

set "SQL_USER="

set "SQL_PASS_B64="
set "MEP_SQLPASSWORD_B64="

goto choose_action



:auth_sql

echo.

set "SQL_USER="

set "SQL_PASS_B64="
set "MEP_SQLPASSWORD_B64="

set /p "SQL_USER=  Usuario SQL: "

call :prompt_sql_password_b64

if %errorlevel% neq 0 (

    call :log_error "Fallo al capturar password SQL en modo seguro."

    echo.

    echo   [ERROR] No se pudo capturar el password SQL.

    echo          Intente nuevamente.

    goto auth_sql

)

echo.

echo   [OK] Autenticacion SQL Server

call :log_info "Autenticacion seleccionada: SQL Server para usuario %SQL_USER%"

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

set "OVERALL_RC=0"
call :log_info "Inicio opcion 1: metadata + ETL"

echo.

echo === Fase 1/2: Recolectando metadata de SQL Server... ===

echo --------------------------------------------------------------

call :exec_gather

echo --------------------------------------------------------------

if %EXEC_RC% neq 0 (

    set "OVERALL_RC=%EXEC_RC%"
    call :log_warn "Fase 1 (metadata) termino con codigo %EXEC_RC%."

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

    set "OVERALL_RC=%EXEC_RC%"
    call :log_warn "Fase 2 (ETL) termino con codigo %EXEC_RC%."

    echo.

    echo   [WARN] Fase 2 termino con codigo %EXEC_RC%.

    echo          Revise el log en la carpeta de salida mep_etl_*\

)

goto done



:run_gather

set "OVERALL_RC=0"
call :log_info "Inicio opcion 2: solo metadata"

echo.

echo === Fase 1: Metadata SQL Server... ===

echo --------------------------------------------------------------

call :exec_gather

echo --------------------------------------------------------------

if %EXEC_RC% neq 0 (

    set "OVERALL_RC=%EXEC_RC%"
    call :log_warn "Metadata termino con codigo %EXEC_RC%."

    echo.

    echo   [WARN] Fase 1 termino con codigo %EXEC_RC%.

    echo          Revise el log en la carpeta mep_sqlserver_*\

)

goto done



:run_etl

set "OVERALL_RC=0"
call :log_info "Inicio opcion 3: solo ETL"

echo.

echo === Fase 2: ETL/SSIS... ===

echo --------------------------------------------------------------

call :exec_etl

echo --------------------------------------------------------------

if %EXEC_RC% neq 0 (

    set "OVERALL_RC=%EXEC_RC%"
    call :log_warn "ETL termino con codigo %EXEC_RC%."

    echo.

    echo   [WARN] Fase 2 termino con codigo %EXEC_RC%.

    echo          Revise el log en la carpeta mep_etl_*\

)

goto done



:run_custom

set "OVERALL_RC=0"
call :log_info "Inicio opcion 4: custom"

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

if %EXEC_RC% neq 0 set "OVERALL_RC=%EXEC_RC%"
if %EXEC_RC% neq 0 call :log_warn "Metadata custom termino con codigo %EXEC_RC%."

echo.

echo === Fase 2/2: ETL/SSIS... ===

echo --------------------------------------------------------------

call :exec_etl

echo --------------------------------------------------------------

if %EXEC_RC% neq 0 set "OVERALL_RC=%EXEC_RC%"
if %EXEC_RC% neq 0 call :log_warn "ETL custom termino con codigo %EXEC_RC%."

goto done



:: ===========================================================================

:: SUBRUTINAS DE EJECUCION POWERSHELL

:: Separa auth Windows vs SQL para evitar problemas de comillas anidadas.

:: La contrasena se pasa directamente al proceso PS sin quedar visible

:: en el historial de comandos del sistema.

:: ===========================================================================



:exec_gather

set EXEC_RC=0
call :log_info "Ejecutando gather_sqlserver.ps1 para %SERVER_INSTANCE%"

if "%USE_WIN_AUTH%"=="true" (

    powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%GATHER_PS1%" -ServerInstance "%SERVER_INSTANCE%"

) else (

    powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%GATHER_PS1%" -ServerInstance "%SERVER_INSTANCE%" -UseWindowsAuth false -SqlUser "%SQL_USER%"

)

set EXEC_RC=%errorlevel%

if %EXEC_RC% neq 0 call :log_error "gather_sqlserver.ps1 devolvio %EXEC_RC%"

exit /b



:exec_gather_custom

set EXEC_RC=0
call :log_info "Ejecutando gather_sqlserver.ps1 (custom) para %SERVER_INSTANCE%"

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

            powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%GATHER_PS1%" -ServerInstance "%SERVER_INSTANCE%" -UseWindowsAuth false -SqlUser "%SQL_USER%" -Databases "%CUSTOM_DBS%" -Schemas "%CUSTOM_SCHEMAS%"

        ) else (

            powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%GATHER_PS1%" -ServerInstance "%SERVER_INSTANCE%" -UseWindowsAuth false -SqlUser "%SQL_USER%" -Databases "%CUSTOM_DBS%"

        )

    ) else (

        powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%GATHER_PS1%" -ServerInstance "%SERVER_INSTANCE%" -UseWindowsAuth false -SqlUser "%SQL_USER%"

    )

)

set EXEC_RC=%errorlevel%

if %EXEC_RC% neq 0 call :log_error "gather_sqlserver.ps1 (custom) devolvio %EXEC_RC%"

exit /b



:exec_etl

set EXEC_RC=0
call :log_info "Ejecutando export_etl.ps1 para %SERVER_INSTANCE%"

if "%USE_WIN_AUTH%"=="true" (

    powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%ETL_PS1%" -ServerInstance "%SERVER_INSTANCE%"

) else (

    powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%ETL_PS1%" -ServerInstance "%SERVER_INSTANCE%" -UseWindowsAuth false -SqlUser "%SQL_USER%"

)

set EXEC_RC=%errorlevel%

if %EXEC_RC% neq 0 call :log_error "export_etl.ps1 devolvio %EXEC_RC%"

exit /b



:: ---------------------------------------------------------------------------

:prompt_sql_password_b64

set "SQL_PASS_B64="

for /f "usebackq delims=" %%P in (`powershell.exe -NoProfile -Command "$sec = Read-Host '  Password SQL' -AsSecureString; $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($sec); try { $plain = [Runtime.InteropServices.Marshal]::PtrToStringAuto($bstr); [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($plain)) } finally { if ($bstr -ne [IntPtr]::Zero) { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) } }"`) do set "SQL_PASS_B64=%%P"

if not defined SQL_PASS_B64 exit /b 1

set "MEP_SQLPASSWORD_B64=%SQL_PASS_B64%"
call :log_info "Password SQL capturada en modo seguro."

exit /b 0



:: ---------------------------------------------------------------------------

:cancelled

echo.

echo   Cancelado.

set "OVERALL_RC=0"
call :log_warn "Ejecucion cancelada por usuario."

goto end



:done

echo.

echo ==============================================================

echo   COMPLETADO. Comprima las carpetas mep_* y entreguela.

echo ==============================================================

if "%OVERALL_RC%"=="0" (
    call :log_info "Ejecucion completada sin errores fatales."
) else (
    call :log_warn "Ejecucion completada con codigo final %OVERALL_RC%."
)



:end

echo.

if not defined OVERALL_RC set "OVERALL_RC=0"
set "FINAL_RC=%OVERALL_RC%"

set "SQL_PASS_B64="
set "MEP_SQLPASSWORD_B64="

pause

endlocal & exit /b %FINAL_RC%



:log_info
call :log_line INFO "%~1"
exit /b 0



:log_warn
call :log_line WARN "%~1"
exit /b 0



:log_error
call :log_line ERROR "%~1"
exit /b 0



:log_line
>>"%LAUNCHER_LOG%" echo [%date% %time%] [%~1] %~2
>>"%LAUNCHER_FALLBACK_LOG%" echo [%date% %time%] [%~1] %~2
exit /b 0

