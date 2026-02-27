<#
.SYNOPSIS
    MEP ETL Exporter — SSIS Package & ETL Artifact Extraction
    Integratel Peru — Stefanini Group

.DESCRIPTION
    Detecta y exporta TODOS los artefactos ETL/SSIS de una instancia
    SQL Server como texto legible (XML/SQL/TXT) para análisis por LLM.

    Flujo automático (sin preguntas):
      Fase 1: SSISDB catalog → .dtsx XML + project params
      Fase 2: msdb legacy packages → .dtsx XML
      Fase 3: SQL Agent jobs → .dtsx paths referenced + job step SQL
      Fase 4: File system scan → .dtsx + .dtsConfig found on disk
      Fase 5: Post-process → extract embedded SQL, connections, dataflows
      Fase 6: Sanitize → redact passwords/tokens

    Output 100% texto/XML — no binarios.

.PARAMETER ServerInstance
    Instancia SQL Server

.PARAMETER OutputDir
    Directorio de salida. Default: .\mep_etl_YYYYMMDD_HHMMSS

.PARAMETER UseWindowsAuth
    Usar autenticación Windows (default)

.PARAMETER ScanPaths
    Rutas adicionales donde buscar .dtsx en disco (separadas por coma).
    Default: C:\SSIS,D:\SSIS,E:\SSIS,C:\ETL,D:\ETL

.EXAMPLE
    .\export_etl.ps1 -ServerInstance "WINDBPVLI0017"
    .\export_etl.ps1 -ServerInstance "WINDBPVLI0017" -ScanPaths "F:\ETL_Prod,G:\Packages"
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [string]$ServerInstance,

    [string]$OutputDir = "",
    [string]$UseWindowsAuth = "true",
    [string]$SqlUser = "",
    [string]$SqlPassword = "",
    [string]$ScanPaths = "C:\SSIS,D:\SSIS,E:\SSIS,C:\ETL,D:\ETL,C:\SSISPackages,D:\SSISPackages"
)

$ErrorActionPreference = "Continue"
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"

# === FIX v4.1: PROVIDER-SAFE PATH RESOLUTION ===
# When SQLPS module is loaded, PowerShell's current location may be
# SQLSERVER:\  — a non-filesystem provider where New-Item, Add-Content,
# Export-Csv, Out-File, Get-ChildItem, and Split-Path all fail.
# We anchor EVERY path to the filesystem using $PSScriptRoot.
$_scriptDir = $null
if ($PSScriptRoot) { $_scriptDir = $PSScriptRoot }
elseif ($MyInvocation.MyCommand.Path) { $_scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path -ErrorAction SilentlyContinue }
if (-not $_scriptDir -or $_scriptDir -notmatch '^[A-Za-z]:\\') {
    $_scriptDir = [System.IO.Directory]::GetCurrentDirectory()
}
# Ensure we're on a filesystem provider for all relative operations
Push-Location $_scriptDir

if (-not $OutputDir) {
    $safeServer = $ServerInstance -replace '[\\/:*?"<>|]', '_'
    $OutputDir = Join-Path $_scriptDir "mep_etl_${safeServer}_${timestamp}"
}

# Resolve to absolute filesystem path (prevents SQLSERVER:\ provider trap)
if (-not [System.IO.Path]::IsPathRooted($OutputDir)) {
    $OutputDir = Join-Path $_scriptDir $OutputDir
}
$OutputDir = [System.IO.Path]::GetFullPath($OutputDir)

# Create output dir BEFORE any writes (so log file can be written)
[System.IO.Directory]::CreateDirectory($OutputDir) | Out-Null
$LogFile = Join-Path $OutputDir "export_etl.log"

# ============================================================
# HELPERS
# ============================================================
function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$ts] [$Level] $Message"
    Write-Host $line
    Add-Content -Path $LogFile -Value $line -ErrorAction SilentlyContinue
}

function Run-SqlQuery {
    param([string]$SQL, [string]$Database = "master")
    $sqlcmdArgs = @("-S", $ServerInstance, "-d", $Database,
                    "-Q", $SQL, "-h", "-1", "-W", "-w", "65535", "-b")
    if ($_useWinAuth) { $sqlcmdArgs += "-E" }
    else { $sqlcmdArgs += @("-U", $script:_credUser, "-P", $script:_credPass) }
    $result = & sqlcmd @sqlcmdArgs 2>>$LogFile
    return $result
}

function Run-SqlToFile {
    param([string]$SQL, [string]$OutFile, [string]$Database = "master")
    $dir = Split-Path $OutFile -Parent
    if ($dir -and -not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }

    # Prefer Invoke-Sqlcmd for clean CSV
    $hasInvoke = $false
    try { Get-Command Invoke-Sqlcmd -ErrorAction Stop | Out-Null; $hasInvoke = $true } catch {}

    if ($hasInvoke) {
        $params = @{ ServerInstance = $ServerInstance; Database = $Database;
                     Query = $SQL; QueryTimeout = 600; MaxCharLength = 1000000 }
        if (-not $_useWinAuth) {
            $secPass = ConvertTo-SecureString $script:_credPass -AsPlainText -Force
            $params["Credential"] = New-Object System.Management.Automation.PSCredential($script:_credUser, $secPass)
        }
        $results = Invoke-Sqlcmd @params
        if ($results) { $results | Export-Csv -Path $OutFile -NoTypeInformation -Encoding UTF8 }
    } else {
        $sqlcmdArgs = @("-S", $ServerInstance, "-d", $Database,
                        "-Q", $SQL, "-s", ",", "-W", "-w", "65535", "-o", $OutFile)
        if ($_useWinAuth) { $sqlcmdArgs += "-E" }
        else { $sqlcmdArgs += @("-U", $script:_credUser, "-P", $script:_credPass) }
        & sqlcmd @sqlcmdArgs 2>>$LogFile
    }
}

function Sanitize-Content {
    <# Replace passwords, tokens, secrets in text #>
    param([string]$Text)
    # Connection string passwords
    $Text = $Text -replace '(?i)(password|pwd)\s*=\s*[^;"\r\n]+', '$1=***REDACTED***'
    # XML attribute passwords
    $Text = $Text -replace '(?i)(password|pwd)="[^"]*"', '$1="***REDACTED***"'
    # Tokens and keys
    $Text = $Text -replace '(?i)(token|apikey|secret|api_key)\s*=\s*\S+', '$1=***REDACTED***'
    return $Text
}

function Extract-DtsxMetadata {
    <#
    .SYNOPSIS
        Parse a .dtsx XML file and extract embedded SQL, connections,
        and data flow summaries into separate readable text files.
    #>
    param([string]$DtsxPath, [string]$ExtractDir)

    if (-not (Test-Path $DtsxPath)) { return }
    New-Item -ItemType Directory -Path $ExtractDir -Force | Out-Null

    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($DtsxPath)

    try {
        [xml]$xml = Get-Content $DtsxPath -Encoding UTF8 -ErrorAction Stop
    } catch {
        Write-Log "    Cannot parse as XML: $DtsxPath" "WARN"
        return
    }

    $nsMgr = New-Object System.Xml.XmlNamespaceManager($xml.NameTable)
    # Register common SSIS namespaces
    $nsMgr.AddNamespace("DTS", "www.microsoft.com/SqlServer/Dts")
    $nsMgr.AddNamespace("dts", "www.microsoft.com/SqlServer/Dts")

    # --- 1. Connection Managers ---
    $connFile = Join-Path $ExtractDir "${baseName}_connections.txt"
    $connLines = @("# Connection Managers — $baseName", "# Extracted for MEP analysis", "")

    $connNodes = $xml.SelectNodes("//DTS:ConnectionManager", $nsMgr)
    if (-not $connNodes -or $connNodes.Count -eq 0) {
        # Try without namespace
        $connNodes = $xml.SelectNodes("//*[local-name()='ConnectionManager']")
    }

    if ($connNodes -and $connNodes.Count -gt 0) {
        foreach ($conn in $connNodes) {
            $name = $conn.GetAttribute("DTS:ObjectName")
            if (-not $name) { $name = $conn.GetAttribute("ObjectName") }
            $connLines += "CONNECTION: $name"

            # Look for connection string in ObjectData
            $objData = $conn.SelectSingleNode("*[local-name()='ObjectData']")
            if ($objData) {
                $innerXml = $objData.InnerXml
                # Extract ConnectionString attribute or element
                if ($innerXml -match 'ConnectionString="([^"]*)"') {
                    $cs = Sanitize-Content $Matches[1]
                    $connLines += "  ConnectionString: $cs"
                }
                if ($innerXml -match 'ServerName="([^"]*)"') {
                    $connLines += "  Server: $($Matches[1])"
                }
                if ($innerXml -match 'InitialCatalog="([^"]*)"') {
                    $connLines += "  Database: $($Matches[1])"
                }
            }
            $connLines += ""
        }
    } else {
        $connLines += "(No connection managers found in package)"
    }
    $connLines | Out-File -FilePath $connFile -Encoding UTF8
    Write-Log "    Extracted: connections ($($connNodes.Count) found)"

    # --- 2. Embedded SQL (Execute SQL Tasks + OLE DB Sources/Destinations) ---
    $sqlFile = Join-Path $ExtractDir "${baseName}_sql_tasks.sql"
    $sqlLines = @("-- Embedded SQL extracted from: $baseName", "-- For MEP analysis", "")
    $sqlCount = 0

    # Execute SQL Tasks: look for SqlStatementSource
    $allNodes = $xml.SelectNodes("//*")
    foreach ($node in $allNodes) {
        # SqlStatementSource attribute (Execute SQL Task)
        $sqlStmt = $node.GetAttribute("SQLTask:SqlStatementSource")
        if (-not $sqlStmt) { $sqlStmt = $node.GetAttribute("SqlStatementSource") }
        if ($sqlStmt -and $sqlStmt.Trim().Length -gt 0) {
            $taskName = ""
            $parent = $node.ParentNode
            while ($parent) {
                $tn = $parent.GetAttribute("DTS:ObjectName")
                if (-not $tn) { $tn = $parent.GetAttribute("ObjectName") }
                if ($tn) { $taskName = $tn; break }
                $parent = $parent.ParentNode
            }
            $sqlLines += "-- ============================================"
            $sqlLines += "-- Task: $taskName"
            $sqlLines += "-- ============================================"
            $sqlLines += $sqlStmt.Trim()
            $sqlLines += ""
            $sqlCount++
        }

        # OpenRowset / SqlCommand in data flow (OLE DB Source)
        foreach ($attrName in @("OpenRowset", "SqlCommand", "TableOrViewName")) {
            $val = $node.GetAttribute($attrName)
            if ($val -and $val.Trim().Length -gt 5) {
                $taskName = ""
                $p = $node.ParentNode
                while ($p) {
                    $tn = $p.GetAttribute("DTS:ObjectName")
                    if (-not $tn) { $tn = $p.GetAttribute("ObjectName") }
                    if ($tn) { $taskName = $tn; break }
                    $p = $p.ParentNode
                }
                $sqlLines += "-- Source ($attrName): $taskName"
                $sqlLines += $val.Trim()
                $sqlLines += ""
                $sqlCount++
            }
        }
    }

    # Also scan raw text for common SQL patterns buried in CDATA or element text
    $rawText = Get-Content $DtsxPath -Raw -Encoding UTF8
    $cdataMatches = [regex]::Matches($rawText, '<!\[CDATA\[([\s\S]*?)\]\]>')
    foreach ($m in $cdataMatches) {
        $content = $m.Groups[1].Value.Trim()
        if ($content.Length -gt 20 -and ($content -match '(?i)\b(SELECT|INSERT|UPDATE|DELETE|EXEC|CREATE|ALTER|MERGE|TRUNCATE)\b')) {
            $sqlLines += "-- CDATA block (embedded SQL):"
            $sqlLines += $content
            $sqlLines += ""
            $sqlCount++
        }
    }

    if ($sqlCount -eq 0) {
        $sqlLines += "-- (No embedded SQL statements found in this package)"
    }
    ($sqlLines -join "`n") | Out-File -FilePath $sqlFile -Encoding UTF8
    Write-Log "    Extracted: SQL statements ($sqlCount found)"

    # --- 3. Script Tasks (C#/VB code) ---
    $scriptCount = 0
    $scriptFile = Join-Path $ExtractDir "${baseName}_script_tasks.txt"
    $scriptLines = @("// Script Tasks extracted from: $baseName", "")

    foreach ($node in $allNodes) {
        $localName = $node.LocalName
        if ($localName -eq "ScriptProject" -or $node.GetAttribute("DTS:CreationName") -like "*ScriptTask*") {
            $scriptName = $node.GetAttribute("DTS:ObjectName")
            if (-not $scriptName) { $scriptName = "ScriptTask_$scriptCount" }

            # Look for source code nodes
            $sourceNodes = $node.SelectNodes(".//*[local-name()='SourceCode' or local-name()='File']")
            foreach ($src in $sourceNodes) {
                $fileName = $src.GetAttribute("FileName")
                if (-not $fileName) { $fileName = $src.GetAttribute("Name") }
                $code = $src.InnerText
                if ($code -and $code.Trim().Length -gt 10) {
                    $scriptLines += "// ============================================"
                    $scriptLines += "// Script Task: $scriptName / $fileName"
                    $scriptLines += "// ============================================"
                    # Decode if base64
                    try {
                        $decoded = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($code.Trim()))
                        if ($decoded -match '(?:using|Imports|public|class|void|Sub |Function )') {
                            $scriptLines += $decoded
                        } else {
                            $scriptLines += $code.Trim()
                        }
                    } catch {
                        $scriptLines += $code.Trim()
                    }
                    $scriptLines += ""
                    $scriptCount++
                }
            }
        }
    }

    if ($scriptCount -gt 0) {
        ($scriptLines -join "`n") | Out-File -FilePath $scriptFile -Encoding UTF8
        Write-Log "    Extracted: script tasks ($scriptCount found)"
    }

    # --- 4. Data Flow Summary ---
    $dfFile = Join-Path $ExtractDir "${baseName}_dataflows.txt"
    $dfLines = @("# Data Flow Summary — $baseName", "")
    $dfCount = 0

    foreach ($node in $allNodes) {
        $creationName = $node.GetAttribute("DTS:CreationName")
        if (-not $creationName) { $creationName = $node.GetAttribute("CreationName") }
        $objectName = $node.GetAttribute("DTS:ObjectName")
        if (-not $objectName) { $objectName = $node.GetAttribute("ObjectName") }

        if ($creationName -and $objectName) {
            $type = ""
            if ($creationName -match '(?i)OLEDBSource|ADONETSource|FlatFileSource|ExcelSource|ODBCSource|XMLSource') {
                $type = "SOURCE"
            } elseif ($creationName -match '(?i)OLEDBDest|ADONETDest|FlatFileDest|ExcelDest|ODBCDest|SqlDest|RecordsetDest') {
                $type = "DESTINATION"
            } elseif ($creationName -match '(?i)Lookup|DerivedColumn|ConditionalSplit|Merge|UnionAll|Sort|Aggregate|Pivot|RowCount|DataConversion|ScriptComp') {
                $type = "TRANSFORM"
            }

            if ($type) {
                # Find parent data flow task name
                $dfTaskName = ""
                $p = $node.ParentNode
                while ($p) {
                    $pcn = $p.GetAttribute("DTS:CreationName")
                    if ($pcn -match '(?i)Pipeline|DataFlow') {
                        $dfTaskName = $p.GetAttribute("DTS:ObjectName")
                        if (-not $dfTaskName) { $dfTaskName = $p.GetAttribute("ObjectName") }
                        break
                    }
                    $p = $p.ParentNode
                }

                $dfLines += "[$type] $objectName"
                if ($dfTaskName) { $dfLines += "  DataFlow: $dfTaskName" }

                # Try to get table/query info
                foreach ($attr in @("OpenRowset", "TableOrViewName", "SqlCommand", "ConnectionName")) {
                    $val = $node.GetAttribute($attr)
                    if ($val) { $dfLines += "  ${attr}: $val" }
                }
                $dfLines += ""
                $dfCount++
            }
        }
    }

    if ($dfCount -gt 0) {
        ($dfLines -join "`n") | Out-File -FilePath $dfFile -Encoding UTF8
    }
    Write-Log "    Extracted: dataflow components ($dfCount found)"
}


# ============================================================
# INICIO
# ============================================================
New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

Write-Log "============================================================"
Write-Log "MEP ETL Exporter v1.1 (provider-safe)"
Write-Log "Servidor: $ServerInstance"
Write-Log "Output:   $OutputDir"
Write-Log "============================================================"

# Credentials
$script:_credUser = ""
$script:_credPass = ""
$_useWinAuth = $UseWindowsAuth -notin @("false","0","$false","no")
if (-not $_useWinAuth) {
    # Accept pre-passed credentials (from launcher) or prompt interactively
    if ($SqlUser) {
        $script:_credUser = $SqlUser
        $script:_credPass = $SqlPassword
        Write-Log "Auth: SQL Server (credentials pre-passed)"
    } else {
        $script:_credUser = Read-Host "Usuario SQL"
        $secP = Read-Host "Password" -AsSecureString
        $script:_credPass = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
            [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secP))
        Write-Log "Auth: SQL Server (interactive)"
    }
} else {
    Write-Log "Auth: Windows (integrated)"
}

$totalPackages = 0
$totalSqlExtracted = 0


# ============================================================
# FASE 1: SSISDB CATALOG
# ============================================================
Write-Log ""
Write-Log "=== FASE 1: SSISDB Catalog ==="

$ssisdbExists = Run-SqlQuery "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE name = 'SSISDB'"
$ssisdbExists = ($ssisdbExists | Where-Object { $_ -match '^\d+$' } | Select-Object -First 1)

if ($ssisdbExists -and [int]$ssisdbExists -gt 0) {
    Write-Log "SSISDB encontrada — exportando proyectos..."

    $ssisdbDir = Join-Path $OutputDir "SSISDB"
    New-Item -ItemType Directory -Path $ssisdbDir -Force | Out-Null

    # Export environments first (valuable for understanding connections)
    Write-Log "  Exportando environment variables..."
    Run-SqlToFile @"
SET NOCOUNT ON;
SELECT
    f.name AS folder_name,
    e.name AS environment_name,
    v.name AS variable_name,
    v.type AS data_type,
    CASE WHEN v.sensitive = 1 THEN '***SENSITIVE/REDACTED***'
         ELSE CAST(v.value AS NVARCHAR(4000)) END AS value,
    v.description
FROM SSISDB.catalog.environment_variables v
JOIN SSISDB.catalog.environments e ON v.environment_id = e.environment_id
JOIN SSISDB.catalog.folders f ON e.folder_id = f.folder_id
ORDER BY f.name, e.name, v.name;
"@ (Join-Path $ssisdbDir "environments.csv") "SSISDB"

    # Export project parameters
    Write-Log "  Exportando project parameters..."
    Run-SqlToFile @"
SET NOCOUNT ON;
SELECT
    f.name AS folder_name,
    p.name AS project_name,
    pa.parameter_name,
    pa.data_type,
    CASE WHEN pa.sensitive = 1 THEN '***SENSITIVE/REDACTED***'
         ELSE CAST(pa.design_default_value AS NVARCHAR(4000)) END AS default_value,
    pa.description,
    pa.required
FROM SSISDB.catalog.object_parameters pa
JOIN SSISDB.catalog.projects p ON pa.project_id = p.project_id
JOIN SSISDB.catalog.folders f ON p.folder_id = f.folder_id
WHERE pa.object_type = 20
ORDER BY f.name, p.name, pa.parameter_name;
"@ (Join-Path $ssisdbDir "project_parameters.csv") "SSISDB"

    # Get list of projects with their deployment info
    Write-Log "  Inventariando proyectos y paquetes..."
    Run-SqlToFile @"
SET NOCOUNT ON;
SELECT
    f.name AS folder_name,
    p.name AS project_name,
    pk.name AS package_name,
    p.deployed_by_name,
    p.last_deployed_time,
    p.description AS project_desc
FROM SSISDB.catalog.packages pk
JOIN SSISDB.catalog.projects p ON pk.project_id = p.project_id
JOIN SSISDB.catalog.folders f ON p.folder_id = f.folder_id
ORDER BY f.name, p.name, pk.name;
"@ (Join-Path $ssisdbDir "inventory.csv") "SSISDB"

    # Export execution history (last 100 executions)
    Write-Log "  Exportando historial de ejecuciones recientes..."
    Run-SqlToFile @"
SET NOCOUNT ON;
SELECT TOP 200
    f.name AS folder_name,
    p.name AS project_name,
    e.package_name,
    e.status,
    CASE e.status
        WHEN 1 THEN 'Created' WHEN 2 THEN 'Running' WHEN 3 THEN 'Canceled'
        WHEN 4 THEN 'Failed' WHEN 5 THEN 'Pending' WHEN 6 THEN 'Unexpected'
        WHEN 7 THEN 'Succeeded' WHEN 8 THEN 'Stopping' WHEN 9 THEN 'Completed'
        ELSE 'Unknown' END AS status_desc,
    e.start_time,
    e.end_time,
    DATEDIFF(SECOND, e.start_time, e.end_time) AS duration_seconds,
    e.executed_as_name
FROM SSISDB.catalog.executions e
JOIN SSISDB.catalog.projects p ON e.project_id = p.project_id
JOIN SSISDB.catalog.folders f ON e.folder_id = f.folder_id
ORDER BY e.start_time DESC;
"@ (Join-Path $ssisdbDir "execution_history.csv") "SSISDB"

    # Now extract actual package XML via T-SQL
    # The .ispac is stored as varbinary in internal.object_versions
    # We export it, then unzip (it's a ZIP containing .dtsx files)

    # Get project list
    $projectList = Run-SqlQuery @"
SET NOCOUNT ON;
SELECT f.name + '|' + p.name + '|' + CAST(p.project_id AS VARCHAR(20))
FROM SSISDB.catalog.projects p
JOIN SSISDB.catalog.folders f ON p.folder_id = f.folder_id
ORDER BY f.name, p.name;
"@

    foreach ($line in $projectList) {
        $line = $line.Trim()
        if (-not $line -or $line -match "rows affected" -or $line -match "^\(") { continue }
        $parts = $line -split '\|'
        if ($parts.Count -lt 3) { continue }

        $folderName = $parts[0].Trim()
        $projectName = $parts[1].Trim()
        $projectId = $parts[2].Trim()

        $projDir = Join-Path $ssisdbDir (Join-Path $folderName $projectName)
        $extractDir = Join-Path $projDir "_extracted"
        New-Item -ItemType Directory -Path $projDir -Force | Out-Null

        Write-Log "  Exportando proyecto: $folderName/$projectName"

        # Try .NET assembly approach first
        $exported = $false
        try {
            [System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Management.IntegrationServices") | Out-Null
            $sqlConn = New-Object System.Data.SqlClient.SqlConnection
            if ($_useWinAuth) {
                $sqlConn.ConnectionString = "Data Source=$ServerInstance;Initial Catalog=master;Integrated Security=SSPI;"
            } else {
                $sqlConn.ConnectionString = "Data Source=$ServerInstance;Initial Catalog=master;User ID=$($script:_credUser);Password=$($script:_credPass);"
            }
            $sqlConn.Open()
            $ssis = New-Object Microsoft.SqlServer.Management.IntegrationServices.IntegrationServices $sqlConn
            $project = $ssis.Catalogs["SSISDB"].Folders[$folderName].Projects[$projectName]
            $bytes = $project.GetProjectBytes()
            $sqlConn.Close()

            # Write .ispac (zip) then extract
            $ispacPath = Join-Path $projDir "$projectName.ispac"
            [System.IO.File]::WriteAllBytes($ispacPath, $bytes)

            # Unzip .ispac → .dtsx files
            try {
                Add-Type -AssemblyName System.IO.Compression.FileSystem
                [System.IO.Compression.ZipFile]::ExtractToDirectory($ispacPath, $projDir)
                Write-Log "    Unzipped .ispac → .dtsx files"
            } catch {
                # Fallback for older PS
                $shell = New-Object -ComObject Shell.Application
                $zip = $shell.Namespace($ispacPath)
                $dest = $shell.Namespace($projDir)
                $dest.CopyHere($zip.Items(), 16)
            }

            # Remove the binary .ispac, keep only text
            Remove-Item $ispacPath -ErrorAction SilentlyContinue
            $exported = $true
        } catch {
            Write-Log "    .NET assembly not available, using T-SQL fallback" "WARN"
        }

        if (-not $exported) {
            # T-SQL fallback: extract individual package XML directly
            $pkgListQ = "SET NOCOUNT ON; SELECT name FROM SSISDB.catalog.packages WHERE project_id = $projectId ORDER BY name"
            $pkgNames = Run-SqlQuery $pkgListQ

            foreach ($pkgName in $pkgNames) {
                $pkgName = $pkgName.Trim()
                if (-not $pkgName -or $pkgName -match "rows affected" -or $pkgName -match "^\(") { continue }

                $pkgFile = Join-Path $projDir "$pkgName"
                # package_format_version contains the .dtsx XML as nvarchar
                # But actually in SSISDB the packages are inside the .ispac binary
                # We need to use BCP to extract the project binary and unzip

                # Alternative: use the internal SP to get package data
                Write-Log "    Package: $pkgName (T-SQL extraction limited — consider SSMS export)"
            }

            # Export project binary via BCP as last resort
            $binaryFile = Join-Path $projDir "_project.ispac"
            $bcpQuery = "SELECT object_data FROM SSISDB.internal.object_versions WHERE object_id = $projectId ORDER BY object_version_id DESC"

            # Actually the simplest T-SQL approach: get_project returns a result set
            # Let's try that
            $tmpIspac = Join-Path $projDir "__tmp_project.ispac"
            try {
                $conn = New-Object System.Data.SqlClient.SqlConnection
                if ($_useWinAuth) {
                    $conn.ConnectionString = "Data Source=$ServerInstance;Initial Catalog=SSISDB;Integrated Security=SSPI;"
                } else {
                    $conn.ConnectionString = "Data Source=$ServerInstance;Initial Catalog=SSISDB;User ID=$($script:_credUser);Password=$($script:_credPass);"
                }
                $conn.Open()
                $cmd = $conn.CreateCommand()
                $cmd.CommandText = "EXEC [catalog].get_project @folder_name=N'$folderName', @project_name=N'$projectName'"
                $cmd.CommandTimeout = 120
                $reader = $cmd.ExecuteReader()
                if ($reader.Read()) {
                    $bytes = [byte[]]::new($reader.GetBytes(0, 0, $null, 0, 0))
                    $reader.GetBytes(0, 0, $bytes, 0, $bytes.Length)
                    [System.IO.File]::WriteAllBytes($tmpIspac, $bytes)
                    Write-Log "    Exported via catalog.get_project"

                    try {
                        Add-Type -AssemblyName System.IO.Compression.FileSystem -ErrorAction SilentlyContinue
                        [System.IO.Compression.ZipFile]::ExtractToDirectory($tmpIspac, $projDir)
                    } catch {
                        $shell = New-Object -ComObject Shell.Application
                        $zip = $shell.Namespace($tmpIspac)
                        $dest = $shell.Namespace($projDir)
                        $dest.CopyHere($zip.Items(), 16)
                    }
                    Remove-Item $tmpIspac -ErrorAction SilentlyContinue
                    $exported = $true
                }
                $reader.Close()
                $conn.Close()
            } catch {
                Write-Log "    T-SQL fallback also failed: $_" "WARN"
                Write-Log "    → Export manually via SSMS: SSISDB > $folderName > $projectName > right-click" "WARN"
            }
        }

        # Sanitize all .dtsx files
        $dtsxFiles = Get-ChildItem -Path $projDir -Filter "*.dtsx" -ErrorAction SilentlyContinue
        foreach ($dtsx in $dtsxFiles) {
            $content = Get-Content $dtsx.FullName -Raw -Encoding UTF8
            $sanitized = Sanitize-Content $content
            $sanitized | Out-File -FilePath $dtsx.FullName -Encoding UTF8

            $totalPackages++
            Write-Log "    Package: $($dtsx.Name) ($([math]::Round($dtsx.Length/1KB, 1)) KB XML)"

            # Extract embedded SQL, connections, dataflows
            Extract-DtsxMetadata -DtsxPath $dtsx.FullName -ExtractDir $extractDir
        }

        # Also handle .params files
        $paramFiles = Get-ChildItem -Path $projDir -Filter "*.params" -ErrorAction SilentlyContinue
        foreach ($pf in $paramFiles) {
            $content = Get-Content $pf.FullName -Raw -Encoding UTF8
            $sanitized = Sanitize-Content $content
            $sanitized | Out-File -FilePath $pf.FullName -Encoding UTF8
            Write-Log "    Params: $($pf.Name)"
        }
    }

} else {
    Write-Log "SSISDB no existe en esta instancia — saltando Fase 1"
}


# ============================================================
# FASE 2: MSDB LEGACY PACKAGES
# ============================================================
Write-Log ""
Write-Log "=== FASE 2: MSDB Legacy Packages ==="

$msdbCount = Run-SqlQuery "SET NOCOUNT ON; SELECT COUNT(*) FROM msdb.dbo.sysssispackages"
$msdbCount = ($msdbCount | Where-Object { $_ -match '^\d+$' } | Select-Object -First 1)

if ($msdbCount -and [int]$msdbCount -gt 0) {
    Write-Log "Encontrados $msdbCount paquetes en msdb — exportando como XML..."

    $msdbDir = Join-Path $OutputDir "MSDB"
    New-Item -ItemType Directory -Path $msdbDir -Force | Out-Null

    # Inventory
    Run-SqlToFile @"
SET NOCOUNT ON;
SELECT
    p.name AS package_name,
    f.foldername AS folder,
    p.description,
    p.createdate,
    p.vermajor, p.verminor,
    DATALENGTH(p.packagedata) / 1024 AS size_kb
FROM msdb.dbo.sysssispackages p
LEFT JOIN msdb.dbo.sysssispackagefolders f ON p.folderid = f.folderid
ORDER BY f.foldername, p.name;
"@ (Join-Path $msdbDir "inventory.csv")

    # Export each package as XML using dtutil (if available)
    $hasDtutil = $false
    try { Get-Command dtutil -ErrorAction Stop | Out-Null; $hasDtutil = $true } catch {}

    if ($hasDtutil) {
        $pkgList = Run-SqlQuery @"
SET NOCOUNT ON;
SELECT
    ISNULL(f.foldername, '') + '\' + p.name
FROM msdb.dbo.sysssispackages p
LEFT JOIN msdb.dbo.sysssispackagefolders f ON p.folderid = f.folderid
ORDER BY f.foldername, p.name;
"@

        foreach ($pkg in $pkgList) {
            $pkg = $pkg.Trim()
            if (-not $pkg -or $pkg -match "rows affected" -or $pkg -match "^\(") { continue }

            $safeName = ($pkg -replace '\\', '_' -replace '^_', '').Trim()
            if (-not $safeName) { continue }
            $destFile = Join-Path $msdbDir "$safeName.dtsx"

            & dtutil /SQL "$pkg" /COPY "FILE;$destFile" /SourceServer $ServerInstance /Quiet 2>>$LogFile
            if (Test-Path $destFile) {
                # Sanitize
                $content = Get-Content $destFile -Raw -Encoding UTF8
                (Sanitize-Content $content) | Out-File -FilePath $destFile -Encoding UTF8

                $totalPackages++
                Write-Log "  Exported: $safeName.dtsx"

                $extractDir = Join-Path $msdbDir "_extracted"
                Extract-DtsxMetadata -DtsxPath $destFile -ExtractDir $extractDir
            }
        }
    } else {
        Write-Log "dtutil no disponible — exportando packagedata como XML via T-SQL..." "WARN"

        # Direct XML extraction from packagedata column
        $pkgXmlQuery = @"
SET NOCOUNT ON;
SELECT
    ISNULL(f.foldername, 'root') AS folder,
    p.name,
    CAST(CAST(p.packagedata AS VARBINARY(MAX)) AS NVARCHAR(MAX)) AS package_xml
FROM msdb.dbo.sysssispackages p
LEFT JOIN msdb.dbo.sysssispackagefolders f ON p.folderid = f.folderid;
"@
        # This approach has size limits; log warning
        Write-Log "  Nota: La exportación T-SQL puede truncar paquetes grandes" "WARN"
        Write-Log "  Para paquetes completos, instalar dtutil o exportar desde SSMS"
    }
} else {
    Write-Log "No hay paquetes legacy en msdb — saltando Fase 2"
}


# ============================================================
# FASE 3: SQL AGENT JOB STEP ANALYSIS
# ============================================================
Write-Log ""
Write-Log "=== FASE 3: SQL Agent Job Steps (SSIS + scripts) ==="

$jobDir = Join-Path $OutputDir "AgentJobs"
New-Item -ItemType Directory -Path $jobDir -Force | Out-Null

# Export ALL job steps with full command text
Run-SqlToFile @"
SET NOCOUNT ON;
SELECT
    j.name           AS job_name,
    j.enabled        AS job_enabled,
    j.description    AS job_description,
    c.name           AS category,
    js.step_id,
    js.step_name,
    js.subsystem,
    js.command        AS step_command,
    js.database_name,
    js.output_file_name,
    s.name           AS schedule_name,
    s.enabled        AS schedule_enabled,
    CASE s.freq_type
        WHEN 1 THEN 'Once' WHEN 4 THEN 'Daily' WHEN 8 THEN 'Weekly'
        WHEN 16 THEN 'Monthly' WHEN 32 THEN 'Monthly-relative'
        WHEN 64 THEN 'Agent start' WHEN 128 THEN 'Idle'
        ELSE CAST(s.freq_type AS VARCHAR) END AS frequency,
    s.active_start_time
FROM msdb.dbo.sysjobs j
LEFT JOIN msdb.dbo.syscategories c ON j.category_id = c.category_id
LEFT JOIN msdb.dbo.sysjobsteps js ON j.job_id = js.job_id
LEFT JOIN msdb.dbo.sysjobschedules jss ON j.job_id = jss.job_id
LEFT JOIN msdb.dbo.sysschedules s ON jss.schedule_id = s.schedule_id
ORDER BY j.name, js.step_id;
"@ (Join-Path $jobDir "all_job_steps.csv")

# Extract SSIS-referencing steps specifically
Run-SqlToFile @"
SET NOCOUNT ON;
SELECT
    j.name AS job_name,
    js.step_name,
    js.subsystem,
    js.command AS ssis_command
FROM msdb.dbo.sysjobsteps js
JOIN msdb.dbo.sysjobs j ON js.job_id = j.job_id
WHERE js.subsystem = 'SSIS'
   OR js.command LIKE '%dtsx%'
   OR js.command LIKE '%SSISDB%'
   OR js.command LIKE '%.ispac%'
ORDER BY j.name, js.step_id;
"@ (Join-Path $jobDir "ssis_job_steps.csv")

# Extract T-SQL job steps (often contain ETL logic)
Write-Log "  Exportando T-SQL embebido en job steps..."
$tsqlStepsFile = Join-Path $jobDir "tsql_job_steps.sql"
$tsqlLines = @("-- T-SQL embedded in SQL Agent Job Steps", "-- Extracted for MEP analysis", "")

$tsqlSteps = Run-SqlQuery @"
SET NOCOUNT ON;
SELECT j.name + '|||' + js.step_name + '|||' + REPLACE(REPLACE(js.command, CHAR(13), ' '), CHAR(10), ' ')
FROM msdb.dbo.sysjobsteps js
JOIN msdb.dbo.sysjobs j ON js.job_id = j.job_id
WHERE js.subsystem = 'TSQL'
AND LEN(js.command) > 10
ORDER BY j.name, js.step_id;
"@

foreach ($line in $tsqlSteps) {
    $line = $line.Trim()
    if (-not $line -or $line -match "rows affected" -or $line -match "^\(") { continue }
    $parts = $line -split '\|\|\|'
    if ($parts.Count -ge 3) {
        $tsqlLines += "-- ============================================"
        $tsqlLines += "-- Job: $($parts[0].Trim())"
        $tsqlLines += "-- Step: $($parts[1].Trim())"
        $tsqlLines += "-- ============================================"
        $tsqlLines += $parts[2].Trim()
        $tsqlLines += ""
    }
}
($tsqlLines -join "`n") | Out-File -FilePath $tsqlStepsFile -Encoding UTF8
Write-Log "  T-SQL job steps exportados"

# CmdExec / PowerShell job steps (BAT/PS1 commands)
Run-SqlToFile @"
SET NOCOUNT ON;
SELECT
    j.name AS job_name,
    js.step_name,
    js.subsystem,
    js.command AS script_command
FROM msdb.dbo.sysjobsteps js
JOIN msdb.dbo.sysjobs j ON js.job_id = j.job_id
WHERE js.subsystem IN ('CmdExec', 'PowerShell')
ORDER BY j.name, js.step_id;
"@ (Join-Path $jobDir "cmdexec_powershell_steps.csv")


# ============================================================
# FASE 4: FILE SYSTEM SCAN
# ============================================================
Write-Log ""
Write-Log "=== FASE 4: File System Scan ==="

$fsDir = Join-Path $OutputDir "FileSystem"
New-Item -ItemType Directory -Path $fsDir -Force | Out-Null

$searchList = $ScanPaths -split "," | ForEach-Object { $_.Trim() }

# Also add paths found in Agent jobs
$agentPaths = Run-SqlQuery @"
SET NOCOUNT ON;
SELECT DISTINCT
    CASE
        WHEN command LIKE '%/FILE %' THEN
            SUBSTRING(command,
                CHARINDEX('/FILE ', command) + 7,
                CHARINDEX('"', command, CHARINDEX('/FILE ', command) + 7) - CHARINDEX('/FILE ', command) - 7)
        WHEN command LIKE '%.dtsx%' THEN
            LEFT(command, LEN(command) - CHARINDEX('\', REVERSE(command)) + 1)
        ELSE NULL
    END AS dtsx_path
FROM msdb.dbo.sysjobsteps
WHERE (command LIKE '%/FILE %' OR command LIKE '%.dtsx%')
AND subsystem IN ('SSIS', 'CmdExec', 'PowerShell');
"@
foreach ($ap in $agentPaths) {
    $ap = $ap.Trim()
    if ($ap -and $ap -ne "NULL" -and $ap -match '\\') {
        $dir = Split-Path $ap -Parent -ErrorAction SilentlyContinue
        if ($dir) { $searchList += $dir }
    }
}

$searchList = $searchList | Sort-Object -Unique
$foundFiles = @()

foreach ($searchPath in $searchList) {
    if (-not (Test-Path $searchPath -ErrorAction SilentlyContinue)) { continue }
    $files = Get-ChildItem -Path $searchPath -Include "*.dtsx","*.dtsConfig","*.params" -Recurse -ErrorAction SilentlyContinue
    if ($files) {
        Write-Log "  Encontrados $($files.Count) archivos en: $searchPath"
        $foundFiles += $files
    }
}

if ($foundFiles.Count -gt 0) {
    $fsExtractDir = Join-Path $fsDir "_extracted"

    foreach ($file in ($foundFiles | Sort-Object FullName -Unique)) {
        # Preserve relative path structure
        $relPath = $file.FullName -replace '^[A-Z]:\\', ''
        $destPath = Join-Path $fsDir $relPath
        $destDir = Split-Path $destPath -Parent
        New-Item -ItemType Directory -Path $destDir -Force | Out-Null

        # Copy and sanitize
        $content = Get-Content $file.FullName -Raw -Encoding UTF8 -ErrorAction SilentlyContinue
        if ($content) {
            (Sanitize-Content $content) | Out-File -FilePath $destPath -Encoding UTF8
            Write-Log "  Copied: $($file.FullName) → $relPath"

            if ($file.Extension -eq ".dtsx") {
                $totalPackages++
                Extract-DtsxMetadata -DtsxPath $destPath -ExtractDir $fsExtractDir
            }
        }
    }
} else {
    Write-Log "  No se encontraron archivos .dtsx en disco"
}

# Also grab .dtsConfig from common config locations
$configPaths = @(
    "$env:ProgramFiles\Microsoft SQL Server\*\DTS\*",
    "$env:ProgramData\SSIS\*"
)
foreach ($cp in $configPaths) {
    $configs = Get-ChildItem -Path $cp -Include "*.dtsConfig","*.config" -Recurse -ErrorAction SilentlyContinue
    foreach ($cfg in $configs) {
        $content = Get-Content $cfg.FullName -Raw -Encoding UTF8 -ErrorAction SilentlyContinue
        if ($content -and $content -match '(?i)SSIS|DTS|ConnectionString') {
            $destPath = Join-Path $fsDir "Configs\$($cfg.Name)"
            New-Item -ItemType Directory -Path (Split-Path $destPath -Parent) -Force | Out-Null
            (Sanitize-Content $content) | Out-File -FilePath $destPath -Encoding UTF8
            Write-Log "  Config: $($cfg.Name)"
        }
    }
}


# ============================================================
# FASE 5: SUMMARY
# ============================================================
Write-Log ""
Write-Log "============================================================"
Write-Log "ETL EXPORT COMPLETADO — $(Get-Date)"
Write-Log "============================================================"
Write-Log ""
Write-Log "Paquetes SSIS exportados: $totalPackages"
Write-Log ""

# Count extracted artifacts
$extractedSql = (Get-ChildItem -Path $OutputDir -Filter "*_sql_tasks.sql" -Recurse -ErrorAction SilentlyContinue).Count
$extractedConn = (Get-ChildItem -Path $OutputDir -Filter "*_connections.txt" -Recurse -ErrorAction SilentlyContinue).Count
$extractedDF = (Get-ChildItem -Path $OutputDir -Filter "*_dataflows.txt" -Recurse -ErrorAction SilentlyContinue).Count
$extractedScript = (Get-ChildItem -Path $OutputDir -Filter "*_script_tasks.txt" -Recurse -ErrorAction SilentlyContinue).Count

Write-Log "Artefactos extraídos para análisis LLM:"
Write-Log "  SQL embebido:        $extractedSql archivos"
Write-Log "  Connection managers:  $extractedConn archivos"
Write-Log "  Data flow summaries:  $extractedDF archivos"
Write-Log "  Script tasks:         $extractedScript archivos"
Write-Log ""

$allFiles = Get-ChildItem -Path $OutputDir -Recurse -File
$totalSize = [math]::Round(($allFiles | Measure-Object -Property Length -Sum).Sum / 1MB, 2)
Write-Log "Total archivos: $($allFiles.Count)"
Write-Log "Tamaño total:   $totalSize MB (100% texto/XML)"
Write-Log ""
Write-Log "SIGUIENTE PASO:"
Write-Log "  Comprimir: Compress-Archive -Path '$OutputDir\*' -DestinationPath 'etl_evidence.zip'"

# Restore original PowerShell location (balances Push-Location at script start)
Pop-Location
