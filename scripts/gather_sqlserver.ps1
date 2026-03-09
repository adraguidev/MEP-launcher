<#
.SYNOPSIS
    MEP Gatherer - SQL Server Automated Metadata Extraction
    Integratel Peru - Stefanini Group

.DESCRIPTION
    Recolecta TODA la metadata de una instancia SQL Server en una sola
    ejecucion. Detecta la version de SQL Server y adapta las queries
    automaticamente (compatible SQL Server 2008 R2 a 2022).

    Organiza output por Base de Datos > Schema:
      output/
      |-- _instance/           (info a nivel de instancia)
      |-- DB_Name/
      |   |-- _database/       (info a nivel de BD)
      |   |-- SchemaName/      (data dictionary, SPs, triggers, views...)
      |   \-- ...
      \-- ...

.PARAMETER ServerInstance
    Instancia SQL Server (ej: "WINDBPVLI0017", "WINDBPVLI0017\INST1", "10.0.1.5,1433")

.PARAMETER Databases
    Lista de bases de datos a procesar (separadas por coma).
    Si se omite, procesa TODAS las bases de datos de usuario.

.PARAMETER Schemas
    Lista de schemas a procesar (separados por coma).
    Si se omite, procesa TODOS los schemas con objetos.

.PARAMETER OutputDir
    Directorio de salida. Default: .\mep_sqlserver_YYYYMMDD_HHMMSS

.PARAMETER UseWindowsAuth
    Usar autenticacion Windows (default). Si es $false, solicita usuario/password SQL.

.EXAMPLE
    # Todo automatico - descubre todas las BDs y schemas
    .\gather_sqlserver.ps1 -ServerInstance "WINDBPVLI0017"

    # Solo ciertas bases de datos
    .\gather_sqlserver.ps1 -ServerInstance "WINDBPVLI0017" -Databases "DWH_Red,DWH_Staging"

    # Solo ciertos schemas dentro de todas las BDs
    .\gather_sqlserver.ps1 -ServerInstance "WINDBPVLI0017" -Schemas "dbo,etl,staging,dim,fact"

    # Combinado: BDs y schemas especificos
    .\gather_sqlserver.ps1 -ServerInstance "WINDBPVLI0017" -Databases "DWH_Red" -Schemas "dbo,fact,dim,stg"

    # Con autenticacion SQL
    .\gather_sqlserver.ps1 -ServerInstance "10.0.1.5,1433" -UseWindowsAuth $false
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [string]$ServerInstance,

    [string]$Databases = "",
    [string]$Schemas = "",
    [string]$OutputDir = "",
    [string]$UseWindowsAuth = "true",
    [string]$SqlUser = "",
    [string]$SqlPassword = ""
)

# ============================================================
# CONFIGURACION
# ============================================================
$ErrorActionPreference = "Continue"
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$FallbackLogFile = Join-Path ([System.IO.Path]::GetTempPath()) "MEP_Gatherer_gather_fallback_${timestamp}.log"

# === FIX v4.1: PROVIDER-SAFE PATH RESOLUTION ===
# When SQLPS module is loaded, PowerShell's current location may be
# SQLSERVER:\  - a non-filesystem provider where New-Item, Add-Content,
# Export-Csv, Out-File, Get-ChildItem, and Split-Path all fail.
# We anchor EVERY path to the filesystem using $PSScriptRoot.
$_scriptDir = $null
if ($PSScriptRoot) { $_scriptDir = $PSScriptRoot }
elseif ($MyInvocation.MyCommand.Path) { $_scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path -ErrorAction SilentlyContinue }
if (-not $_scriptDir -or ($_scriptDir -notmatch '^[A-Za-z]:\\' -and $_scriptDir -notmatch '^\\\\')) {
    $_scriptDir = [System.IO.Directory]::GetCurrentDirectory()
}
# Ensure we're on a filesystem provider for all relative operations
Push-Location $_scriptDir

if (-not $OutputDir) {
    $safeServer = $ServerInstance -replace '[\\/:*?"<>|]', '_'
    $OutputDir = Join-Path $_scriptDir "mep_sqlserver_${safeServer}_${timestamp}"
}

# Resolve to absolute filesystem path (prevents SQLSERVER:\ provider trap)
if (-not [System.IO.Path]::IsPathRooted($OutputDir)) {
    $OutputDir = Join-Path $_scriptDir $OutputDir
}
$OutputDir = [System.IO.Path]::GetFullPath($OutputDir)

# Create output dir BEFORE any writes (so log file can be written)
[System.IO.Directory]::CreateDirectory($OutputDir) | Out-Null

# Force TCP protocol so ODBC Driver 11 doesn't fall back to Named Pipes
# (Named Pipes fails on remote connections and some local configurations)
if ($ServerInstance -notmatch '^(tcp:|np:|lpc:|via:|admin:)') {
    $ServerInstance = "tcp:$ServerInstance"
}
$LogFile = Join-Path $OutputDir "gather_sqlserver.log"
$script:FallbackLogFile = $FallbackLogFile

# ============================================================
# FUNCIONES AUXILIARES
# ============================================================
function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$ts] [$Level] $Message"
    Write-Host $line
    foreach ($target in @($LogFile, $script:FallbackLogFile) | Where-Object { $_ } | Select-Object -Unique) {
        try {
            Add-Content -Path $target -Value $line -Encoding UTF8 -ErrorAction Stop
        } catch {}
    }
}

function Finalize-Execution {
    $env:SQLCMDPASSWORD = $null
    try { Pop-Location } catch {}
}

function Get-PlainSqlPassword {
    param([string]$EncodedPassword)

    if (-not $EncodedPassword) { return "" }

    try {
        $bytes = [System.Convert]::FromBase64String($EncodedPassword)
        return [System.Text.Encoding]::UTF8.GetString($bytes)
    }
    catch {
        return $null
    }
}

trap {
    $errMessage = $_.Exception.Message
    if (-not $errMessage) { $errMessage = "$_" }
    Write-Log "UNHANDLED ERROR: $errMessage" "ERROR"
    if ($_.ScriptStackTrace) {
        Write-Log "STACK: $($_.ScriptStackTrace)" "ERROR"
    }
    Finalize-Execution
    exit 1
}

function Run-Query {
    <#
    .SYNOPSIS
        Ejecuta query SQL y guarda resultado como texto.
        Usa sqlcmd si disponible, fallback a Invoke-Sqlcmd.
    #>
    param(
        [string]$Label,
        [string]$SQL,
        [string]$OutFile,
        [string]$Database = "master",
        [int]$MinVersion = 0
    )

    if ($script:sqlMajorVersion -lt $MinVersion -and $MinVersion -gt 0) {
        Write-Log "  [$Label] SKIP (requiere SQL Server >= $MinVersion, actual = $($script:sqlMajorVersion))" "WARN"
        return
    }

    $startTime = Get-Date
    Write-Log "  [$Label] ejecutando..." "INFO"

    try {
        $outDir = Split-Path $OutFile -Parent
        if ($outDir -and -not (Test-Path $outDir)) {
            New-Item -ItemType Directory -Path $outDir -Force | Out-Null
        }

        if ($script:_hasSqlcmd) {
            $sqlcmdArgs = @(
                "-S", $ServerInstance,
                "-d", $Database,
                "-Q", $SQL,
                "-s", ",",
                "-W",
                "-h", "-1",
                "-w", "65535",
                "-o", $OutFile
            )
            if ($_useWinAuth) { $sqlcmdArgs += "-E" }
            else { $sqlcmdArgs += @("-U", $script:_credUser) }

            & sqlcmd @sqlcmdArgs 2>>$LogFile

            if ($LASTEXITCODE -ne 0) {
                Write-Log "  [$Label] sqlcmd returned exit code $LASTEXITCODE" "WARN"
            }
        } else {
            # Fallback: Invoke-Sqlcmd
            $connParams = @{
                ServerInstance = $ServerInstance; Database = $Database;
                Query = $SQL; QueryTimeout = 600; MaxCharLength = 1000000
            }
            if (-not $_useWinAuth) {
                $cmdInfo = Get-Command Invoke-Sqlcmd
                if ($cmdInfo.Parameters.ContainsKey('Credential')) {
                    $secPass = ConvertTo-SecureString $script:_credPass -AsPlainText -Force
                    $cred = New-Object System.Management.Automation.PSCredential($script:_credUser, $secPass)
                    $connParams["Credential"] = $cred
                } else {
                    $connParams["Username"] = $script:_credUser
                    $connParams["Password"] = $script:_credPass
                }
            }
            $results = Invoke-Sqlcmd @connParams -ErrorAction Stop
            if ($results) {
                $lines = @()
                foreach ($row in @($results)) {
                    $lines += ($row.PSObject.Properties | Where-Object { $_.MemberType -eq 'NoteProperty' } | ForEach-Object {
                        if ($_.Value -ne $null) { [string]$_.Value } else { "" }
                    }) -join ","
                }
                $lines | Out-File $OutFile -Encoding UTF8
            }
        }

        $elapsed = ((Get-Date) - $startTime).TotalSeconds
        if (Test-Path $OutFile) {
            $lines = (Get-Content $OutFile | Measure-Object -Line).Lines
            $sizeMB = [math]::Round((Get-Item $OutFile).Length / 1MB, 2)
            Write-Log "  [$Label] OK ($lines filas, ${sizeMB} MB, ${elapsed}s)"
        } else {
            Write-Log "  [$Label] WARNING: archivo no generado" "WARN"
        }
    }
    catch {
        $_msg = "$_"
        if ($_msg -match "permission was denied|does not have permission|Login failed|Cannot open database|access denied") {
            Write-Log "  [$Label] PERMISO DENEGADO: la cuenta no tiene acceso suficiente para esta consulta. No es un error del script." "ERROR"
        } else {
            Write-Log "  [$Label] ERROR: $_msg" "ERROR"
        }
    }
}

function Run-QueryCSV {
    <#
    .SYNOPSIS
        Ejecuta query y genera CSV limpio usando BCP o PowerShell.
        Mas robusto que sqlcmd para datos con comas/newlines.
    #>
    param(
        [string]$Label,
        [string]$SQL,
        [string]$OutFile,
        [string]$Database = "master",
        [int]$MinVersion = 0
    )

    if ($script:sqlMajorVersion -lt $MinVersion -and $MinVersion -gt 0) {
        Write-Log "  [$Label] SKIP (requiere SQL >= $MinVersion)" "WARN"
        return
    }

    $startTime = Get-Date
    Write-Log "  [$Label] ejecutando..." "INFO"

    try {
        $outDir = Split-Path $OutFile -Parent
        if ($outDir -and -not (Test-Path $outDir)) {
            New-Item -ItemType Directory -Path $outDir -Force | Out-Null
        }

        # Use Invoke-Sqlcmd if available (better CSV), fallback to sqlcmd
        $hasInvokeSqlcmd = $false
        try {
            Get-Command Invoke-Sqlcmd -ErrorAction Stop | Out-Null
            $hasInvokeSqlcmd = $true
        } catch {}

        if ($hasInvokeSqlcmd) {
            $connParams = @{
                ServerInstance = $ServerInstance
                Database       = $Database
                Query          = $SQL
                QueryTimeout   = 600
                MaxCharLength  = 1000000
            }
            if (-not $_useWinAuth) {
                $cmdInfo = Get-Command Invoke-Sqlcmd
                if ($cmdInfo.Parameters.ContainsKey('Credential')) {
                    $secPass = ConvertTo-SecureString $script:_credPass -AsPlainText -Force
                    $cred = New-Object System.Management.Automation.PSCredential($script:_credUser, $secPass)
                    $connParams["Credential"] = $cred
                } else {
                    # SQLPS module uses -Username/-Password instead of -Credential
                    $connParams["Username"] = $script:_credUser
                    $connParams["Password"] = $script:_credPass
                }
            }
            $results = Invoke-Sqlcmd @connParams -ErrorAction Stop
            if ($results) {
                $results | Export-Csv -Path $OutFile -NoTypeInformation -Encoding UTF8
            } else {
                "-- No results --" | Out-File $OutFile
            }
        } else {
            # Fallback: sqlcmd with -s and post-process
            $sqlcmdArgs = @(
                "-S", $ServerInstance, "-d", $Database,
                "-Q", $SQL, "-s", "`t", "-W", "-h", "-1",
                "-w", "65535", "-o", $OutFile
            )
            if ($_useWinAuth) { $sqlcmdArgs += "-E" }
            else { $sqlcmdArgs += @("-U", $script:_credUser) }
            & sqlcmd @sqlcmdArgs 2>>$LogFile
            if ($LASTEXITCODE -ne 0) {
                Write-Log "  [$Label] sqlcmd returned exit code $LASTEXITCODE" "ERROR"
            }
        }

        $elapsed = [math]::Round(((Get-Date) - $startTime).TotalSeconds, 1)
        if (Test-Path $OutFile) {
            $lines = (Get-Content $OutFile -ErrorAction SilentlyContinue | Measure-Object -Line).Lines
            $sizeMB = [math]::Round((Get-Item $OutFile).Length / 1MB, 2)
            Write-Log "  [$Label] OK ($lines filas, ${sizeMB} MB, ${elapsed}s)"
        }
    }
    catch {
        $_msg = "$_"
        if ($_msg -match "permission was denied|does not have permission|Login failed|Cannot open database|access denied") {
            Write-Log "  [$Label] PERMISO DENEGADO: la cuenta no tiene acceso suficiente para esta consulta. No es un error del script." "ERROR"
        } else {
            Write-Log "  [$Label] ERROR: $_msg" "ERROR"
        }
    }
}


# ============================================================
# INICIO
# ============================================================
New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $OutputDir "_instance") -Force | Out-Null

Write-Log "============================================================"
Write-Log "MEP SQL Server Gatherer v4.1 (provider-safe)"
Write-Log "Servidor: $ServerInstance"
Write-Log "Output:   $OutputDir"
Write-Log "============================================================"

# --- Credentials ---
$script:_credUser = ""
$script:_credPass = ""
$_useWinAuth = -not (@("false","0","False","no") -contains $UseWindowsAuth)
if (-not $_useWinAuth) {
    # Accept pre-passed credentials (from launcher) or prompt interactively
    if ($SqlUser) {
        $script:_credUser = $SqlUser
        if ($SqlPassword) {
            $script:_credPass = $SqlPassword
            Write-Log "Auth: SQL Server (credentials pre-passed)"
        } elseif ($env:MEP_SQLPASSWORD_B64) {
            $script:_credPass = Get-PlainSqlPassword $env:MEP_SQLPASSWORD_B64
            if ($null -eq $script:_credPass) {
                Write-Log "ERROR: No se pudo decodificar la password SQL desde la variable de entorno." "ERROR"
                Pop-Location
                exit 1
            }
            Write-Log "Auth: SQL Server (password from environment)"
        } else {
            $secPassInput = Read-Host "Password" -AsSecureString
            $script:_credPass = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
                [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secPassInput)
            )
            Write-Log "Auth: SQL Server (user pre-passed, password interactive)"
        }
    } else {
        $script:_credUser = Read-Host "Usuario SQL"
        $secPassInput = Read-Host "Password" -AsSecureString
        $script:_credPass = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
            [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secPassInput)
        )
        Write-Log "Auth: SQL Server (interactive)"
    }
} else {
    Write-Log "Auth: Windows (integrated)"
}

# Set env var for sqlcmd (avoids -P argument parsing issues with special chars)
if (-not $_useWinAuth -and $script:_credPass) {
    $env:SQLCMDPASSWORD = $script:_credPass
}

# --- Validate SQL tools availability ---
$script:_hasSqlcmd = $false
$script:_hasInvokeSqlcmd = $false
try { Get-Command sqlcmd -ErrorAction Stop | Out-Null; $script:_hasSqlcmd = $true } catch {}
try { Get-Command Invoke-Sqlcmd -ErrorAction Stop | Out-Null; $script:_hasInvokeSqlcmd = $true } catch {}

if (-not $script:_hasSqlcmd -and -not $script:_hasInvokeSqlcmd) {
    Write-Log "============================================================" "ERROR"
    Write-Log "ERROR CRITICO: No se encontro 'sqlcmd' ni 'Invoke-Sqlcmd'" "ERROR"
    Write-Log "" "ERROR"
    Write-Log "Instale alguno de los siguientes:" "ERROR"
    Write-Log "  1) sqlcmd: https://learn.microsoft.com/sql/tools/sqlcmd/sqlcmd-utility" "ERROR"
    Write-Log "     O instalar 'SQL Server Command Line Utilities' desde el instalador de SQL Server" "ERROR"
    Write-Log "  2) Modulo SqlServer de PowerShell:" "ERROR"
    Write-Log "     Install-Module -Name SqlServer -Scope CurrentUser" "ERROR"
    Write-Log "  3) Importar SQLPS (si SQL Server esta instalado):" "ERROR"
    Write-Log "     Import-Module SQLPS" "ERROR"
    Write-Log "============================================================" "ERROR"
    Write-Host ""
    Write-Host "  [ERROR] No se encontro sqlcmd ni Invoke-Sqlcmd."
    Write-Host "  Sin estas herramientas no es posible conectarse a SQL Server."
    Write-Host "  Consulte el log para opciones de instalacion: $LogFile"
    Pop-Location
    exit 1
}

Write-Log "SQL Tools: sqlcmd=$(if($script:_hasSqlcmd){'SI'}else{'NO'}), Invoke-Sqlcmd=$(if($script:_hasInvokeSqlcmd){'SI'}else{'NO'})"

# ============================================================
# FASE 0: DETECCION DE VERSION
# ============================================================
Write-Log ""
Write-Log "=== FASE 0: Deteccion de version ==="

$versionQuery = "SET NOCOUNT ON; SELECT SERVERPROPERTY('ProductVersion') AS ver, SERVERPROPERTY('ProductLevel') AS sp, SERVERPROPERTY('Edition') AS ed, SERVERPROPERTY('ProductMajorVersion') AS major, @@VERSION AS full_ver"

$versionFile = Join-Path $OutputDir "_instance\00_version.txt"
Run-Query "Version" $versionQuery $versionFile

# Parse version
$script:sqlMajorVersion = 10  # default to 2008
$script:sqlVersionFull = "Unknown"

if (Test-Path $versionFile) {
    $vContent = [System.IO.File]::ReadAllText($versionFile)
    # Try to extract major version number
    if ($vContent -match "(\d+)\.(\d+)\.(\d+)") {
        $majorNum = [int]$Matches[1]
        $script:sqlMajorVersion = $majorNum
        $versionMap = @{
            10 = "2008/2008R2"; 11 = "2012"; 12 = "2014";
            13 = "2016"; 14 = "2017"; 15 = "2019"; 16 = "2022"
        }
        $friendlyName = if ($versionMap.ContainsKey($majorNum)) { $versionMap[$majorNum] } else { "Unknown" }
        $script:sqlVersionFull = "$majorNum ($friendlyName)"
    }
}

Write-Log "SQL Server Version: $($script:sqlVersionFull) (major=$($script:sqlMajorVersion))"
Write-Log "STRING_AGG:    $(if($script:sqlMajorVersion -ge 14){'SI (2017+)'}else{'NO - usando FOR XML PATH'})"
Write-Log "Temporal:       $(if($script:sqlMajorVersion -ge 13){'SI (2016+)'}else{'NO'})"

# ============================================================
# FASE 1: INFO A NIVEL DE INSTANCIA
# ============================================================
Write-Log ""
Write-Log "=== FASE 1: Informacion de instancia ==="
$instDir = Join-Path $OutputDir "_instance"

# Memory expression: physical_memory_kb was added in SQL 2012 (v11)
# SQL 2008 R2 (v10) uses physical_memory_in_bytes
$_memExpr = if ($script:sqlMajorVersion -ge 11) {
    '(SELECT physical_memory_kb/1024 FROM sys.dm_os_sys_info) AS memory_mb'
} else {
    '(SELECT CAST(physical_memory_in_bytes/1048576 AS BIGINT) FROM sys.dm_os_sys_info) AS memory_mb'
}

# Instance config
Run-QueryCSV "Server Config" @"
SET NOCOUNT ON;
SELECT
    SERVERPROPERTY('ServerName')          AS server_name,
    SERVERPROPERTY('MachineName')         AS machine_name,
    SERVERPROPERTY('InstanceName')        AS instance_name,
    SERVERPROPERTY('ProductVersion')      AS product_version,
    SERVERPROPERTY('ProductLevel')        AS service_pack,
    SERVERPROPERTY('Edition')             AS edition,
    SERVERPROPERTY('EngineEdition')       AS engine_edition,
    SERVERPROPERTY('Collation')           AS collation,
    SERVERPROPERTY('IsClustered')         AS is_clustered,
    SERVERPROPERTY('IsHadrEnabled')       AS is_hadr,
    SERVERPROPERTY('IsFullTextInstalled') AS fulltext,
    (SELECT COUNT(*) FROM sys.databases WHERE database_id > 4) AS user_db_count,
    (SELECT sqlserver_start_time FROM sys.dm_os_sys_info) AS start_time,
    $_memExpr,
    (SELECT cpu_count FROM sys.dm_os_sys_info) AS cpu_count;
"@ (Join-Path $instDir "01_server_config.csv")

# Linked servers
Run-QueryCSV "Linked Servers" @"
SET NOCOUNT ON;
SELECT name, product, provider, data_source, catalog,
       is_remote_login_enabled, is_data_access_enabled,
       modify_date
FROM sys.servers WHERE is_linked = 1
ORDER BY name;
"@ (Join-Path $instDir "02_linked_servers.csv")

# SQL Agent Jobs (instance-level)
Run-QueryCSV "SQL Agent Jobs" @"
SET NOCOUNT ON;
SELECT
    j.name           AS job_name,
    j.enabled        AS job_enabled,
    j.description,
    c.name           AS category,
    jh.step_id,
    jh.step_name,
    jh.subsystem,
    jh.command       AS step_command,
    jh.database_name,
    jh.output_file_name,
    s.name           AS schedule_name,
    s.enabled        AS schedule_enabled,
    s.freq_type,
    s.freq_interval,
    s.freq_subday_type,
    s.freq_subday_interval,
    s.active_start_time
FROM msdb.dbo.sysjobs j
LEFT JOIN msdb.dbo.syscategories c ON j.category_id = c.category_id
LEFT JOIN msdb.dbo.sysjobsteps jh ON j.job_id = jh.job_id
LEFT JOIN msdb.dbo.sysjobschedules js ON j.job_id = js.job_id
LEFT JOIN msdb.dbo.sysschedules s ON js.schedule_id = s.schedule_id
ORDER BY j.name, jh.step_id;
"@ (Join-Path $instDir "03_agent_jobs.csv")

# Logins
Run-QueryCSV "Server Logins" @"
SET NOCOUNT ON;
SELECT
    name, type_desc, is_disabled, create_date,
    modify_date, default_database_name, default_language_name
FROM sys.server_principals
WHERE type IN ('S','U','G')
AND name NOT LIKE '##%'
AND name NOT IN ('sa','NT AUTHORITY\SYSTEM','NT SERVICE\MSSQLSERVER')
ORDER BY name;
"@ (Join-Path $instDir "04_logins.csv")


# ============================================================
# FASE 2: DESCUBRIR BASES DE DATOS
# ============================================================
Write-Log ""
Write-Log "=== FASE 2: Descubrimiento de bases de datos ==="

$dbListQuery = "SET NOCOUNT ON; SELECT name FROM sys.databases WHERE database_id > 4 AND state_desc = 'ONLINE' ORDER BY name"

if ($Databases) {
    $dbList = $Databases -split "," | ForEach-Object { $_.Trim() }
    Write-Log "BDs especificadas: $($dbList -join ', ')"
} else {
    # Discover all user databases
    if ($script:_hasSqlcmd) {
        $dbFile = Join-Path $OutputDir "_tmp_dbs.txt"
        $sqlcmdArgs = @("-S", $ServerInstance, "-Q", $dbListQuery, "-h", "-1", "-W", "-o", $dbFile)
        if ($_useWinAuth) { $sqlcmdArgs += "-E" }
        else { $sqlcmdArgs += @("-U", $script:_credUser) }
        & sqlcmd @sqlcmdArgs 2>>$LogFile
        $dbSqlcmdRc = $LASTEXITCODE

        $dbLines = @(Get-Content $dbFile -ErrorAction SilentlyContinue |
            ForEach-Object { $_.Trim() } |
            Where-Object { $_ -ne "" })
        $dbAccessError = $dbLines |
            Where-Object { $_ -match "^(Sqlcmd:|Msg |Warning:|Error:)" } |
            Select-Object -First 1
        $dbList = @($dbLines |
            Where-Object { $_ -notmatch "^\(" -and $_ -notmatch "rows affected" -and $_ -notmatch "^(Sqlcmd:|Msg |Warning:|Error:)" })
        Remove-Item $dbFile -ErrorAction SilentlyContinue
        if ($dbAccessError -or ($dbSqlcmdRc -ne 0 -and $dbList.Count -eq 0)) {
            Write-Log "ERROR CRITICO: No se pudo descubrir las bases de datos de usuario." "ERROR"
            if ($dbAccessError) {
                Write-Log "Detalle: $dbAccessError" "ERROR"
            } else {
                Write-Log "sqlcmd devolvio exit code $dbSqlcmdRc durante el descubrimiento de bases de datos." "ERROR"
            }
            Finalize-Execution
            exit 1
        }
    } else {
        # Fallback: Invoke-Sqlcmd
        try {
            $connParams = @{ ServerInstance = $ServerInstance; Query = $dbListQuery; QueryTimeout = 120 }
            if (-not $_useWinAuth) {
                $cmdInfo = Get-Command Invoke-Sqlcmd
                if ($cmdInfo.Parameters.ContainsKey('Credential')) {
                    $secPass = ConvertTo-SecureString $script:_credPass -AsPlainText -Force
                    $connParams["Credential"] = New-Object System.Management.Automation.PSCredential($script:_credUser, $secPass)
                } else {
                    $connParams["Username"] = $script:_credUser
                    $connParams["Password"] = $script:_credPass
                }
            }
            $dbResults = Invoke-Sqlcmd @connParams -ErrorAction Stop
            $dbList = @($dbResults | ForEach-Object { $_.name })
        } catch {
            Write-Log "ERROR CRITICO: No se pudo descubrir las bases de datos de usuario." "ERROR"
            Write-Log "Detalle: $_" "ERROR"
            Finalize-Execution
            exit 1
        }
    }

    Write-Log "BDs descubiertas: $($dbList -join ', ')"
}

if ($dbList.Count -eq 0) {
    Write-Log "WARN: No se descubrieron bases de datos de usuario accesibles." "WARN"
}

# DB summary
Run-QueryCSV "Database Summary" @"
SET NOCOUNT ON;
SELECT
    d.name, d.create_date, d.compatibility_level,
    d.recovery_model_desc, d.state_desc, d.collation_name,
    d.is_read_only, d.is_auto_shrink_on,
    (SELECT SUM(size)*8.0/1024 FROM sys.master_files WHERE database_id = d.database_id AND type = 0) AS data_mb,
    (SELECT SUM(size)*8.0/1024 FROM sys.master_files WHERE database_id = d.database_id AND type = 1) AS log_mb
FROM sys.databases d
WHERE d.database_id > 4
ORDER BY d.name;
"@ (Join-Path $instDir "05_databases.csv")


# ============================================================
# FASE 3: POR CADA BASE DE DATOS
# ============================================================
$dbCount = 0
$totalSchemas = 0

# authentication_type_desc was added in SQL 2012 (v11); not available in 2008 R2
$_authTypeCol = if ($script:sqlMajorVersion -ge 11) {
    "dp.authentication_type_desc"
} else {
    "'N/A' AS authentication_type_desc"
}

foreach ($db in $dbList) {

    $dbCount++
    $dbSafeName = $db -replace '[\\/:*?"<>|]', '_'
    $dbDir = Join-Path $OutputDir $dbSafeName
    $dbMetaDir = Join-Path $dbDir "_database"
    New-Item -ItemType Directory -Path $dbMetaDir -Force | Out-Null

    Write-Log ""
    Write-Log "=========================================="
    Write-Log "  BD $dbCount/$($dbList.Count): $db"
    Write-Log "=========================================="

    # ----------------------------------------------------------
    # 3a: Discover schemas in this database
    # ----------------------------------------------------------
    $schemaQuery = @"
SET NOCOUNT ON;
SELECT DISTINCT s.name
FROM sys.schemas s
INNER JOIN sys.objects o ON s.schema_id = o.schema_id
WHERE s.name NOT IN ('sys','INFORMATION_SCHEMA','guest','db_owner',
    'db_accessadmin','db_securityadmin','db_ddladmin','db_backupoperator',
    'db_datareader','db_datawriter','db_denydatareader','db_denydatawriter')
AND o.type IN ('U','V','P','FN','IF','TF','TR')
ORDER BY s.name;
"@
    if ($Schemas) {
        $schemaList = $Schemas -split "," | ForEach-Object { $_.Trim() }
        Write-Log "  Schemas especificados: $($schemaList -join ', ')"
    } else {
        if ($script:_hasSqlcmd) {
            $schemaFile = Join-Path $OutputDir "_tmp_schemas.txt"
            $sqlcmdArgs = @("-S", $ServerInstance, "-d", $db, "-Q", $schemaQuery, "-h", "-1", "-W", "-o", $schemaFile)
            if ($_useWinAuth) { $sqlcmdArgs += "-E" }
            else { $sqlcmdArgs += @("-U", $script:_credUser) }
            & sqlcmd @sqlcmdArgs 2>>$LogFile
            $schemaSqlcmdRc = $LASTEXITCODE

            $_rawSchemaLines = @(Get-Content $schemaFile -ErrorAction SilentlyContinue |
                ForEach-Object { $_.Trim() } |
                Where-Object { $_ -ne "" })
            $_dbAccessError = $_rawSchemaLines |
                Where-Object { $_ -match "^(Sqlcmd:|Msg |Warning:|Error:).*(Login failed|Cannot open database|permission was denied|does not have permission|access denied)" } |
                Select-Object -First 1
            $schemaList = @($_rawSchemaLines |
                Where-Object { $_ -notmatch "^\(" -and $_ -notmatch "rows affected" -and $_ -notmatch "^(Sqlcmd:|Msg |Warning:|Error:)" })
            Remove-Item $schemaFile -ErrorAction SilentlyContinue
            if ($_dbAccessError -or ($schemaSqlcmdRc -ne 0 -and $schemaList.Count -eq 0)) {
                Write-Log "  [ACCESO DENEGADO] BD '$db' omitida: la cuenta utilizada no tiene permisos sobre esta base de datos."
                Write-Log "  [ACCESO DENEGADO] Accion requerida: otorgue acceso a la cuenta ejecutante o use 'sa' / cuenta sysadmin."
                continue
            }
        } else {
            # Fallback: Invoke-Sqlcmd
            $connParams = @{ ServerInstance = $ServerInstance; Database = $db; Query = $schemaQuery; QueryTimeout = 120 }
            if (-not $_useWinAuth) {
                $cmdInfo = Get-Command Invoke-Sqlcmd
                if ($cmdInfo.Parameters.ContainsKey('Credential')) {
                    $secPass = ConvertTo-SecureString $script:_credPass -AsPlainText -Force
                    $connParams["Credential"] = New-Object System.Management.Automation.PSCredential($script:_credUser, $secPass)
                } else {
                    $connParams["Username"] = $script:_credUser
                    $connParams["Password"] = $script:_credPass
                }
            }
            try {
                $schemaResults = Invoke-Sqlcmd @connParams -ErrorAction Stop
                $schemaList = @($schemaResults | ForEach-Object { $_.name })
            } catch {
                Write-Log "  [ACCESO DENEGADO] BD '$db' omitida: la cuenta utilizada no tiene permisos sobre esta base de datos."
                Write-Log "  [ACCESO DENEGADO] Accion requerida: otorgue acceso a la cuenta ejecutante o use 'sa' / cuenta sysadmin."
                continue
            }
        }

        Write-Log "  Schemas descubiertos: $($schemaList -join ', ')"
    }

    $totalSchemas += $schemaList.Count

    # ----------------------------------------------------------
    # 3b: Database-level extractions (cross-schema)
    # ----------------------------------------------------------
    Write-Log "  --- Info a nivel de base de datos ---"

    # DB principals & roles
    Run-QueryCSV "[$db] DB Principals" @"
SET NOCOUNT ON;
SELECT
    dp.name, dp.type_desc, dp.default_schema_name,
    dp.create_date, dp.modify_date,
    $_authTypeCol
FROM sys.database_principals dp
WHERE dp.type IN ('S','U','G','R')
AND dp.name NOT IN ('dbo','guest','INFORMATION_SCHEMA','sys','public')
ORDER BY dp.type_desc, dp.name;
"@ (Join-Path $dbMetaDir "01_principals.csv") $db

    # Role memberships
    Run-QueryCSV "[$db] Role Members" @"
SET NOCOUNT ON;
SELECT
    r.name AS role_name,
    m.name AS member_name,
    m.type_desc AS member_type
FROM sys.database_role_members rm
JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
JOIN sys.database_principals m ON rm.member_principal_id = m.principal_id
ORDER BY r.name, m.name;
"@ (Join-Path $dbMetaDir "02_role_members.csv") $db

    # Object permissions
    Run-QueryCSV "[$db] Permissions" @"
SET NOCOUNT ON;
SELECT
    dp.name              AS principal,
    dp.type_desc         AS principal_type,
    p.permission_name,
    p.state_desc         AS permission_state,
    p.class_desc,
    OBJECT_SCHEMA_NAME(p.major_id) AS obj_schema,
    OBJECT_NAME(p.major_id)        AS obj_name
FROM sys.database_principals dp
LEFT JOIN sys.database_permissions p ON dp.principal_id = p.grantee_principal_id
WHERE dp.type IN ('S','U','G','R')
AND dp.name NOT IN ('dbo','guest','INFORMATION_SCHEMA','sys','public')
ORDER BY dp.name, p.permission_name;
"@ (Join-Path $dbMetaDir "03_permissions.csv") $db

    # Database-level object summary
    Run-QueryCSV "[$db] Object Summary" @"
SET NOCOUNT ON;
SELECT
    SCHEMA_NAME(o.schema_id) AS [schema],
    o.type_desc,
    COUNT(*)                 AS quantity,
    MAX(o.modify_date)       AS last_modified
FROM sys.objects o
WHERE o.is_ms_shipped = 0
GROUP BY SCHEMA_NAME(o.schema_id), o.type_desc
ORDER BY [schema], o.type_desc;
"@ (Join-Path $dbMetaDir "04_object_summary.csv") $db

    # ----------------------------------------------------------
    # 3c: PER-SCHEMA extractions
    # ----------------------------------------------------------
    $schemaNum = 0

    foreach ($schema in $schemaList) {

        $schemaNum++
        $schDir = Join-Path $dbDir $schema
        New-Item -ItemType Directory -Path $schDir -Force | Out-Null

        Write-Log ""
        Write-Log "  --- [$db].[$schema] ($schemaNum/$($schemaList.Count)) ---"

        # ======================================================
        # S01: TABLES + COLUMNS (data dictionary)
        # ======================================================
        Run-QueryCSV "[$db].[$schema] Tables+Columns" @"
SET NOCOUNT ON;
SELECT
    s.name               AS [schema],
    t.name               AS table_name,
    t.create_date,
    t.modify_date,
    p.rows               AS row_count,
    c.name               AS column_name,
    c.column_id,
    ty.name              AS data_type,
    c.max_length,
    c.precision,
    c.scale,
    c.is_nullable,
    c.is_identity,
    dc.definition        AS default_value,
    ep.value             AS column_description
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.columns c ON t.object_id = c.object_id
JOIN sys.types ty ON c.user_type_id = ty.user_type_id
LEFT JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
LEFT JOIN sys.extended_properties ep ON ep.major_id = c.object_id
    AND ep.minor_id = c.column_id AND ep.name = 'MS_Description'
WHERE s.name = '$schema'
ORDER BY t.name, c.column_id;
"@ (Join-Path $schDir "S01_tables_columns.csv") $db

        # ======================================================
        # S02: PRIMARY KEYS
        # ======================================================
        Run-QueryCSV "[$db].[$schema] Primary Keys" @"
SET NOCOUNT ON;
SELECT
    OBJECT_SCHEMA_NAME(i.object_id) AS [schema],
    OBJECT_NAME(i.object_id)        AS table_name,
    i.name                          AS pk_name,
    c.name                          AS column_name,
    ic.key_ordinal                  AS position
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.is_primary_key = 1
AND OBJECT_SCHEMA_NAME(i.object_id) = '$schema'
ORDER BY OBJECT_NAME(i.object_id), ic.key_ordinal;
"@ (Join-Path $schDir "S02_primary_keys.csv") $db

        # ======================================================
        # S03: TABLES WITHOUT PK (CDC risk)
        # ======================================================
        Run-QueryCSV "[$db].[$schema] No-PK Tables" @"
SET NOCOUNT ON;
SELECT
    s.name AS [schema],
    t.name AS table_name,
    p.rows AS row_count,
    'SIN PK - RIESGO CDC' AS alerta
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
LEFT JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
WHERE s.name = '$schema'
AND NOT EXISTS (
    SELECT 1 FROM sys.indexes i
    WHERE i.object_id = t.object_id AND i.is_primary_key = 1
)
ORDER BY p.rows DESC;
"@ (Join-Path $schDir "S03_tables_no_pk.csv") $db

        # ======================================================
        # S04: FOREIGN KEYS
        # ======================================================
        Run-QueryCSV "[$db].[$schema] Foreign Keys" @"
SET NOCOUNT ON;
SELECT
    OBJECT_SCHEMA_NAME(fk.parent_object_id) AS child_schema,
    OBJECT_NAME(fk.parent_object_id)        AS child_table,
    fk.name                                 AS fk_name,
    cp.name                                 AS fk_column,
    fkc.constraint_column_id                AS position,
    OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS parent_schema,
    OBJECT_NAME(fk.referenced_object_id)    AS parent_table,
    cr.name                                 AS parent_column,
    fk.delete_referential_action_desc,
    fk.update_referential_action_desc,
    fk.is_disabled
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) = '$schema'
ORDER BY OBJECT_NAME(fk.parent_object_id), fk.name, fkc.constraint_column_id;
"@ (Join-Path $schDir "S04_foreign_keys.csv") $db

        # ======================================================
        # S05: INDEXES (version-adaptive column list)
        # ======================================================
        # FOR XML PATH works on SQL 2005+
        Run-QueryCSV "[$db].[$schema] Indexes" @"
SET NOCOUNT ON;
SELECT
    OBJECT_SCHEMA_NAME(i.object_id) AS [schema],
    OBJECT_NAME(i.object_id)        AS table_name,
    i.name                          AS index_name,
    i.type_desc,
    i.is_unique,
    i.is_primary_key,
    i.filter_definition,
    STUFF((
        SELECT ', ' + c2.name
        FROM sys.index_columns ic2
        JOIN sys.columns c2 ON ic2.object_id = c2.object_id AND ic2.column_id = c2.column_id
        WHERE ic2.object_id = i.object_id AND ic2.index_id = i.index_id AND ic2.is_included_column = 0
        ORDER BY ic2.key_ordinal
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS key_columns,
    STUFF((
        SELECT ', ' + c3.name
        FROM sys.index_columns ic3
        JOIN sys.columns c3 ON ic3.object_id = c3.object_id AND ic3.column_id = c3.column_id
        WHERE ic3.object_id = i.object_id AND ic3.index_id = i.index_id AND ic3.is_included_column = 1
        ORDER BY ic3.key_ordinal
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS included_columns
FROM sys.indexes i
WHERE OBJECT_SCHEMA_NAME(i.object_id) = '$schema'
AND i.object_id > 100
AND i.name IS NOT NULL
ORDER BY OBJECT_NAME(i.object_id), i.name;
"@ (Join-Path $schDir "S05_indexes.csv") $db

        # ======================================================
        # S06: CHECK CONSTRAINTS
        # ======================================================
        Run-QueryCSV "[$db].[$schema] Check Constraints" @"
SET NOCOUNT ON;
SELECT
    OBJECT_SCHEMA_NAME(cc.parent_object_id) AS [schema],
    OBJECT_NAME(cc.parent_object_id) AS table_name,
    cc.name AS constraint_name,
    cc.definition,
    cc.is_disabled
FROM sys.check_constraints cc
WHERE OBJECT_SCHEMA_NAME(cc.parent_object_id) = '$schema'
ORDER BY OBJECT_NAME(cc.parent_object_id), cc.name;
"@ (Join-Path $schDir "S06_check_constraints.csv") $db

        # ======================================================
        # S07: STORED PROCEDURES - FULL CODE
        # ======================================================
        Run-QueryCSV "[$db].[$schema] SP Code" @"
SET NOCOUNT ON;
SELECT
    OBJECT_SCHEMA_NAME(o.object_id) AS [schema],
    o.name         AS sp_name,
    o.create_date,
    o.modify_date,
    m.definition   AS codigo_completo
FROM sys.objects o
JOIN sys.sql_modules m ON o.object_id = m.object_id
WHERE o.type = 'P'
AND OBJECT_SCHEMA_NAME(o.object_id) = '$schema'
ORDER BY o.name;
"@ (Join-Path $schDir "S07_sp_code.csv") $db

        # ======================================================
        # S08: FUNCTIONS - FULL CODE
        # ======================================================
        Run-QueryCSV "[$db].[$schema] Function Code" @"
SET NOCOUNT ON;
SELECT
    OBJECT_SCHEMA_NAME(o.object_id) AS [schema],
    o.name         AS function_name,
    o.type_desc,
    o.create_date,
    o.modify_date,
    m.definition   AS codigo_completo
FROM sys.objects o
JOIN sys.sql_modules m ON o.object_id = m.object_id
WHERE o.type IN ('FN','IF','TF')
AND OBJECT_SCHEMA_NAME(o.object_id) = '$schema'
ORDER BY o.name;
"@ (Join-Path $schDir "S08_function_code.csv") $db

        # ======================================================
        # S09: TRIGGERS - FULL CODE
        # ======================================================
        Run-QueryCSV "[$db].[$schema] Trigger Code" @"
SET NOCOUNT ON;
SELECT
    OBJECT_SCHEMA_NAME(t.parent_id) AS table_schema,
    OBJECT_NAME(t.parent_id)        AS table_name,
    t.name                          AS trigger_name,
    t.is_disabled,
    t.is_instead_of_trigger,
    m.definition                    AS codigo_completo
FROM sys.triggers t
JOIN sys.sql_modules m ON t.object_id = m.object_id
WHERE t.parent_id > 0
AND OBJECT_SCHEMA_NAME(t.parent_id) = '$schema'
ORDER BY OBJECT_NAME(t.parent_id), t.name;
"@ (Join-Path $schDir "S09_trigger_code.csv") $db

        # ======================================================
        # S10: VIEWS - FULL CODE
        # ======================================================
        Run-QueryCSV "[$db].[$schema] View Code" @"
SET NOCOUNT ON;
SELECT
    OBJECT_SCHEMA_NAME(o.object_id) AS [schema],
    o.name         AS view_name,
    o.create_date,
    o.modify_date,
    m.definition   AS codigo_completo
FROM sys.objects o
JOIN sys.sql_modules m ON o.object_id = m.object_id
WHERE o.type = 'V'
AND OBJECT_SCHEMA_NAME(o.object_id) = '$schema'
ORDER BY o.name;
"@ (Join-Path $schDir "S10_view_code.csv") $db

        # ======================================================
        # S11: TABLE SIZES
        # ======================================================
        Run-QueryCSV "[$db].[$schema] Table Sizes" @"
SET NOCOUNT ON;
SELECT
    s.name AS [schema],
    t.name AS table_name,
    p.rows AS row_count,
    CAST(ROUND(SUM(a.total_pages) * 8.0 / 1024, 2) AS DECIMAL(18,2)) AS total_mb,
    CAST(ROUND(SUM(a.used_pages) * 8.0 / 1024, 2) AS DECIMAL(18,2)) AS used_mb,
    CAST(ROUND(SUM(a.data_pages) * 8.0 / 1024, 2) AS DECIMAL(18,2)) AS data_mb
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.indexes i ON t.object_id = i.object_id
JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
JOIN sys.allocation_units a ON p.partition_id = a.container_id
WHERE s.name = '$schema'
GROUP BY s.name, t.name, p.rows
ORDER BY SUM(a.total_pages) DESC;
"@ (Join-Path $schDir "S11_table_sizes.csv") $db

        # ======================================================
        # S12: EXTENDED PROPERTIES (descriptions)
        # ======================================================
        Run-QueryCSV "[$db].[$schema] Extended Properties" @"
SET NOCOUNT ON;
SELECT
    OBJECT_SCHEMA_NAME(ep.major_id) AS [schema],
    OBJECT_NAME(ep.major_id)        AS object_name,
    ep.name                         AS property_name,
    CAST(ep.value AS NVARCHAR(4000)) AS property_value,
    ep.minor_id,
    CASE WHEN ep.minor_id > 0
         THEN COL_NAME(ep.major_id, ep.minor_id)
         ELSE NULL END              AS column_name
FROM sys.extended_properties ep
WHERE ep.class = 1
AND OBJECT_SCHEMA_NAME(ep.major_id) = '$schema'
ORDER BY OBJECT_NAME(ep.major_id), ep.minor_id;
"@ (Join-Path $schDir "S12_extended_properties.csv") $db

        # ======================================================
        # S13: DEPENDENCIES (cross-schema also)
        # ======================================================
        Run-QueryCSV "[$db].[$schema] Dependencies" @"
SET NOCOUNT ON;
SELECT DISTINCT
    OBJECT_SCHEMA_NAME(d.referencing_id) AS referencing_schema,
    OBJECT_NAME(d.referencing_id)        AS referencing_object,
    d.referencing_minor_id,
    COALESCE(d.referenced_schema_name, OBJECT_SCHEMA_NAME(d.referenced_id)) AS referenced_schema,
    COALESCE(d.referenced_entity_name, OBJECT_NAME(d.referenced_id))        AS referenced_object,
    d.referenced_server_name,
    d.referenced_database_name
FROM sys.sql_expression_dependencies d
WHERE OBJECT_SCHEMA_NAME(d.referencing_id) = '$schema'
   OR d.referenced_schema_name = '$schema'
ORDER BY referencing_schema, referencing_object, referenced_schema, referenced_object;
"@ (Join-Path $schDir "S13_dependencies.csv") $db

        # ======================================================
        # S14: USER-DEFINED TYPES (if any)
        # ======================================================
        Run-QueryCSV "[$db].[$schema] User Types" @"
SET NOCOUNT ON;
SELECT
    SCHEMA_NAME(t.schema_id)  AS [schema],
    t.name                    AS type_name,
    TYPE_NAME(t.system_type_id) AS base_type,
    t.max_length,
    t.precision,
    t.scale,
    t.is_nullable,
    t.is_table_type
FROM sys.types t
WHERE t.is_user_defined = 1
AND SCHEMA_NAME(t.schema_id) = '$schema'
ORDER BY t.name;
"@ (Join-Path $schDir "S14_user_types.csv") $db

    } # end foreach schema

} # end foreach database


# ============================================================
# FASE 4: RESUMEN
# ============================================================
Write-Log ""
Write-Log "============================================================"
Write-Log "RECOLECCION COMPLETADA - $(Get-Date)"
Write-Log "============================================================"
Write-Log ""
Write-Log "SQL Server:  $($script:sqlVersionFull)"
Write-Log "Bases datos: $dbCount"
Write-Log "Schemas:     $totalSchemas (total across all DBs)"
Write-Log ""
Write-Log "Archivos generados:"

$allFiles = @(Get-ChildItem -Path $OutputDir -Recurse | Where-Object { -not $_.PSIsContainer })
$totalSize = ($allFiles | Measure-Object -Property Length -Sum).Sum
$totalFiles = $allFiles.Count

Write-Log "  Total archivos: $totalFiles"
Write-Log "  Tamano total:   $([math]::Round($totalSize / 1MB, 2)) MB"
Write-Log ""
Write-Log "Estructura:"
Get-ChildItem -Path $OutputDir | Where-Object { $_.PSIsContainer } | ForEach-Object {
    $subFiles = (Get-ChildItem -Path $_.FullName -Recurse | Where-Object { -not $_.PSIsContainer } | Measure-Object).Count
    $subSize = [math]::Round((Get-ChildItem -Path $_.FullName -Recurse | Where-Object { -not $_.PSIsContainer } |
        Measure-Object -Property Length -Sum).Sum / 1MB, 2)
    Write-Log "  $($_.Name)/  ($subFiles files, $subSize MB)"
}

Write-Log ""
Write-Log "SIGUIENTE PASO:"
Write-Log "  Comprimir: Compress-Archive -Path '$OutputDir\*' -DestinationPath 'mep_evidence.zip'"
Write-Log "  Entregar el .zip al equipo Stefanini"

Finalize-Execution
