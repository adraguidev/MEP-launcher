# MEP Gatherer

Herramienta automatizada para recolectar metadata de SQL Server y artefactos ETL/SSIS, empaquetada como un `.exe` portable que no requiere instalacion.

Desarrollado por **Integratel Peru - Stefanini Group**.

## Caracteristicas

- Recoleccion completa de metadata SQL Server (schemas, tablas, SPs, funciones, triggers, vistas, indices, constraints, permisos, jobs, etc.)
- Exportacion de paquetes SSIS desde SSISDB, msdb y filesystem
- Extraccion de SQL embebido, connection managers y data flows de paquetes DTSX
- Compatible con **SQL Server 2008 R2 a 2022**
- **Deteccion automatica de instancias** SQL Server instaladas en el servidor
- Autenticacion Windows o SQL Server
- Seleccion inteligente de dtutil (prefiere la version mas reciente disponible)
- Output 100% texto/XML, optimizado para analisis por LLM
- Ejecutable portable (.exe) - no requiere Python ni PowerShell modules

## Uso

### Opcion 1: Ejecutable (recomendado)

Copiar `MEP_Gatherer.exe` al servidor y ejecutar como Administrador:

```
MEP_Gatherer.exe
```

El exe detecta automaticamente las instancias SQL Server instaladas y las muestra para seleccion:

```
==============================================================
  MEP Gatherer -- Stefanini Group
  Recolector automatizado de metadata SQL Server
==============================================================

  [OK] Ejecutando como Administrador

  Instancias SQL Server detectadas:
    1) MISERVIDOR
    2) MISERVIDOR\DEVTEST
    O) Otra (ingresar manualmente)

  Seleccione (1-2) o O para otra: _
```

Si no se detectan instancias (o se elige `O`), se solicita el nombre manualmente.

Formatos aceptados: `MISERVIDOR`, `MISERVIDOR\INSTANCIA`, `10.0.1.5,1433`

Menu interactivo:

| Opcion | Descripcion |
|--------|-------------|
| **1** | Recolectar TODO - metadata + ETL (recomendado) |
| **2** | Solo metadata (`gather_sqlserver.ps1`) |
| **3** | Solo ETL/SSIS (`export_etl.ps1`) |
| **4** | Custom (especificar BDs/schemas) |
| **5** | Cancelar |

### Opcion 2: PowerShell directo

```powershell
# Ejecucion basica:
.\gather_sqlserver.ps1 -ServerInstance "MISERVIDOR"

# Solo ciertas bases de datos:
.\gather_sqlserver.ps1 -ServerInstance "MISERVIDOR" -Databases "MiDB1,MiDB2"

# Solo ciertos schemas:
.\gather_sqlserver.ps1 -ServerInstance "MISERVIDOR" -Schemas "dbo,etl,staging"

# Combinado: BDs + schemas especificos:
.\gather_sqlserver.ps1 -ServerInstance "MISERVIDOR" -Databases "DWH" -Schemas "dbo,fact,dim,stg"

# Con autenticacion SQL (en vez de Windows):
.\gather_sqlserver.ps1 -ServerInstance "MISERVIDOR" -UseWindowsAuth $false

# Solo ETL/SSIS:
.\export_etl.ps1 -ServerInstance "MISERVIDOR"
```

## Scripts Internos

| Script | Version | Funcion |
|--------|---------|---------|
| `gather_sqlserver.ps1` | v4.1 | Metadata de instancia, BDs, schemas, objetos |
| `export_etl.ps1` | v1.1 | Exportacion de paquetes SSIS y artefactos ETL |

## Como funciona internamente

### Parametros de entrada

#### `gather_sqlserver.ps1`

| Parametro | Obligatorio | Default | Descripcion |
|-----------|:-----------:|---------|-------------|
| `-ServerInstance` | **SI** | - | Instancia SQL Server. Ej: `MISERVIDOR`, `MISERVIDOR\INST1`, `10.0.1.5,1433` |
| `-Databases` | no | *(todas)* | Lista de BDs separadas por coma. Si se omite, descubre automaticamente todas las BDs de usuario |
| `-Schemas` | no | *(todos)* | Lista de schemas separados por coma. Si se omite, descubre automaticamente todos los schemas con objetos |
| `-OutputDir` | no | `.\mep_sqlserver_SERVIDOR_YYYYMMDD_HHMMSS` | Carpeta de salida. Se crea automaticamente |
| `-UseWindowsAuth` | no | `true` | `true` = Windows integrada, `false` = SQL Auth |
| `-SqlUser` | no | - | Usuario SQL (solo si `UseWindowsAuth=false`) |
| `-SqlPassword` | no | - | Password SQL (solo si `UseWindowsAuth=false`) |

#### `export_etl.ps1`

| Parametro | Obligatorio | Default | Descripcion |
|-----------|:-----------:|---------|-------------|
| `-ServerInstance` | **SI** | - | Instancia SQL Server |
| `-OutputDir` | no | `.\mep_etl_SERVIDOR_YYYYMMDD_HHMMSS` | Carpeta de salida |
| `-UseWindowsAuth` | no | `true` | Metodo de autenticacion |
| `-SqlUser` / `-SqlPassword` | no | - | Credenciales SQL |
| `-ScanPaths` | no | `C:\SSIS,D:\SSIS,E:\SSIS,C:\ETL,D:\ETL,...` | Rutas donde buscar `.dtsx` en disco |

### Seleccion de bases de datos

```
Si -Databases fue proporcionado:
    Usar exactamente esas BDs (separadas por coma)

Si -Databases esta vacio (default):
    Ejecutar: SELECT name FROM sys.databases
              WHERE database_id > 4          -- excluye master, model, msdb, tempdb
              AND state_desc = 'ONLINE'      -- solo BDs accesibles
              ORDER BY name
    -> Procesar TODAS las BDs de usuario descubiertas
```

**Ejemplo**: un servidor con 3 BDs de usuario (`DWH`, `Staging`, `AppData`) procesara las 3 automaticamente.
Con `-Databases "DWH,Staging"` solo procesaria esas 2.

### Seleccion de schemas dentro de cada BD

```
Si -Schemas fue proporcionado:
    Usar exactamente esos schemas en TODAS las BDs

Si -Schemas esta vacio (default):
    Por cada BD, ejecutar:
        SELECT DISTINCT s.name
        FROM sys.schemas s
        INNER JOIN sys.objects o ON s.schema_id = o.schema_id
        WHERE s.name NOT IN ('sys','INFORMATION_SCHEMA','guest',
              'db_owner','db_accessadmin','db_securityadmin',
              'db_ddladmin','db_backupoperator','db_datareader',
              'db_datawriter','db_denydatareader','db_denydatawriter')
        AND o.type IN ('U','V','P','FN','IF','TF','TR')
    -> Solo schemas que TIENEN objetos (tablas, vistas, SPs, funciones, triggers)
    -> Excluye schemas de sistema y roles built-in
```

**Ejemplo**: una BD con schemas `dbo`, `etl`, `staging`, `dim`, `fact` (todos con tablas) generara 14 CSVs x 5 schemas = 70 archivos para esa BD. Un schema vacio se omite.

### Flujo de ejecucion completo

```
1. VALIDACION
   - Verifica admin (UAC)
   - Detecta instancias SQL Server via registro de Windows
   - Valida que exista sqlcmd o Invoke-Sqlcmd
   - Establece autenticacion (Windows o SQL)

2. GATHER (gather_sqlserver.ps1)
   Fase 0: Detecta version SQL Server (2008R2-2022) y adapta queries
   Fase 1: Info de instancia          -> _instance/ (4-5 CSVs)
   Fase 2: Descubre BDs              -> lista de BDs a procesar
   Fase 3: Por cada BD:
           - Descubre schemas         -> lista de schemas con objetos
           - Info a nivel de BD       -> BD/_database/ (4 CSVs)
           - Por cada schema:         -> BD/schema/ (14 CSVs)
             S01-S14: tablas, PKs, FKs, indexes, SPs, funciones,
                      triggers, vistas, sizes, dependencias, etc.
   Fase 4: Resumen + conteo de archivos

3. EXPORT ETL (export_etl.ps1)
   Fase 1: SSISDB catalog    -> proyectos .ispac, .dtsx, parametros, historial
   Fase 2: MSDB legacy       -> paquetes SSIS almacenados en msdb (via dtutil)
   Fase 3: Agent Jobs        -> SQL embebido en job steps, referencias a SSIS
   Fase 4: File system scan  -> .dtsx y .dtsConfig encontrados en disco
   Fase 5: Post-proceso      -> extrae SQL embebido, connections, dataflows de cada .dtsx
   Sanitizacion: redacta passwords/tokens en todo el output
```

### Trazabilidad del run

Cada ejecucion genera logs detallados con timestamp de cada operacion:

```
[2026-03-02 10:15:23] [INFO] MEP SQL Server Gatherer v4.1
[2026-03-02 10:15:23] [INFO] Servidor: MISERVIDOR\PROD
[2026-03-02 10:15:23] [INFO] Auth: Windows (integrated)
[2026-03-02 10:15:23] [INFO] SQL Tools: sqlcmd=SI, Invoke-Sqlcmd=SI
[2026-03-02 10:15:23] [INFO] SQL Server Version: 15 (2019)
[2026-03-02 10:15:24] [INFO] BDs descubiertas: DWH, Staging, AppData
[2026-03-02 10:15:24] [INFO]   Schemas descubiertos: dbo, etl, dim, fact
[2026-03-02 10:15:24] [INFO]   [Server Config] OK (2 filas, 0.01 MB, 0.2s)
...
[2026-03-02 10:18:45] [INFO] RECOLECCION COMPLETADA
[2026-03-02 10:18:45] [INFO]   Total archivos: 163
[2026-03-02 10:18:45] [INFO]   Tamano total: 4.52 MB
```

- **`gather_sqlserver.log`**: dentro de la carpeta de output del gather
- **`export_etl.log`**: dentro de la carpeta de output del ETL
- Cada query individual registra: filas retornadas, tamano del archivo, tiempo de ejecucion
- Los errores se registran con nivel `[ERROR]` o `[WARN]` sin detener la ejecucion

### Prerequisitos en el servidor

- Windows Server 2012 R2 o superior
- PowerShell 3.0+ (incluido en Windows Server 2012 R2+)
- **Al menos uno** de los siguientes para conectarse a SQL Server:
  - `sqlcmd.exe` (incluido con SQL Server Client Tools)
  - `Invoke-Sqlcmd` (modulo SQLPS o SqlServer de PowerShell)
- Permisos: el login debe tener acceso de lectura a `sys.*` views y a las BDs objetivo
- Ejecutar como Administrador (recomendado para acceso completo a registry y Agent Jobs)

## Que genera

Detecta la version de SQL Server automaticamente y adapta las queries.

### Fase 1: Metadata SQL Server (`gather_sqlserver.ps1`)

```
mep_sqlserver_SERVIDOR_YYYYMMDD_HHMMSS/
├── _instance/                  <- Info del servidor
│   ├── 01_server_config.csv
│   ├── 02_linked_servers.csv
│   ├── 03_agent_jobs.csv       <- Jobs con codigo COMPLETO de cada step
│   └── 04_logins.csv
├── NombreBD/
│   ├── _database/              <- Info a nivel de BD
│   │   ├── 01_principals.csv
│   │   ├── 02_role_members.csv
│   │   ├── 03_permissions.csv
│   │   └── 04_object_summary.csv
│   ├── dbo/                    <- Por cada schema
│   │   ├── S01_tables_columns.csv
│   │   ├── S02_primary_keys.csv
│   │   ├── S03_tables_no_pk.csv    <- Tablas sin PK = riesgo CDC
│   │   ├── S04_foreign_keys.csv
│   │   ├── S05_indexes.csv
│   │   ├── S06_check_constraints.csv
│   │   ├── S07_sp_code.csv         <- Codigo COMPLETO de SPs
│   │   ├── S08_function_code.csv   <- Codigo COMPLETO de functions
│   │   ├── S09_trigger_code.csv    <- Codigo COMPLETO de triggers
│   │   ├── S10_view_code.csv       <- Codigo COMPLETO de vistas
│   │   ├── S11_table_sizes.csv
│   │   ├── S12_extended_properties.csv
│   │   ├── S13_dependencies.csv
│   │   └── S14_user_types.csv
│   ├── etl/                    <- Otro schema
│   │   └── (mismos 14 archivos)
│   └── ...
└── gather_sqlserver.log
```

- Por instancia: 4 CSVs
- Por BD: 4 CSVs
- Por schema: 14 CSVs
- Total por servidor tipico (3 BDs, 5 schemas c/u): ~80 CSVs

### Fase 2: ETL/SSIS (`export_etl.ps1`)

```
mep_etl_SERVIDOR_YYYYMMDD_HHMMSS/
├── 01_ssisdb_catalog/          <- Proyectos SSISDB (si existe)
│   └── Folder/Project/
│       ├── project.ispac
│       ├── *.dtsx
│       ├── *.params
│       ├── *.conmgr
│       └── sql_statements/
├── 02_msdb_packages/           <- Paquetes legacy msdb
│   └── *.dtsx + analisis
├── 03_agent_jobs/              <- SQL embebido de jobs
│   └── *.sql
├── 04_filesystem/              <- DTSX del filesystem
│   └── *.dtsx + configs
└── export_etl.log
```

## Adaptacion por version de SQL Server

| Version | Adaptaciones |
|---------|-------------|
| 2008/2008R2 | Usa FOR XML PATH (no STRING_AGG), sin filter_definition |
| 2012/2014 | Usa FOR XML PATH, soporte parcial de DMVs |
| 2016 | Temporal tables detection disponible |
| 2017+ | Puede usar STRING_AGG (pero el script usa FOR XML PATH por seguridad) |
| 2019/2022 | Todas las features disponibles |

## Resultado esperado

Comprimir las carpetas de output y entregar:

```powershell
Compress-Archive -Path ".\mep_*" -DestinationPath "evidencia_MISERVIDOR.zip"
```

## Limitaciones conocidas

| Componente | Limitacion | Workaround |
|------------|-----------|------------|
| SSISDB `.ispac` export | Con SQL Auth, los archivos `.ispac` no se pueden exportar porque Microsoft los almacena encriptados internamente. Solo el CLR stored procedure `catalog.get_project` puede desencriptarlos, y requiere Windows Authentication. | Usar Windows Auth para obtener los `.ispac`/`.dtsx` completos. Con SQL Auth, el script exporta automaticamente un inventario de paquetes (`_package_inventory.txt`) como fallback. |

---

## Changelog

### v2.8 (2026-03-02)
- Fix: SQL auth ahora se pasa correctamente a los scripts (antes se ignoraba silenciosamente).
- Mejora: SSISDB .ispac export con SQL auth muestra mensaje claro en vez de error; exporta inventario de paquetes como fallback.
- Compatibilidad: `Invoke-Sqlcmd` detecta automaticamente si usar `-Credential` o `-Username`/`-Password` segun el modulo instalado.

### v2.6 (2026-02-28)
- Fix: Compatibilidad SQL Auth con modulo SQLPS (antiguo) - detecta si `Invoke-Sqlcmd` soporta `-Credential` (SqlServer module) o `-Username`/`-Password` (SQLPS module) y usa el parametro correcto automaticamente.

### v2.5 (2026-02-28)
- Fix: Query de historial de ejecuciones SSISDB - `catalog.executions` ya tiene `folder_name` y `project_name` directamente; eliminados JOINs incorrectos contra columnas inexistentes (`project_id`, `folder_id`).

### v2.4 (2026-02-27)
- Deteccion automatica de instancias SQL Server instaladas via registro de Windows.
- El usuario elige de una lista en vez de escribir el nombre manualmente.
- Opcion manual sigue disponible para servidores remotos (IP, instancias no locales).

### v2.3 (2026-02-27)
- Fix: Seleccion inteligente de dtutil - busca en orden descendente (v160, v150, v140, v130) antes de usar `Get-Command`, eliminando errores "This application requires Integration Services" cuando dtutil de Express se encontraba primero en PATH.

### v2.2 (2026-02-27)
- Fix: Eliminacion de todos los caracteres non-ASCII y BOM UTF-8 para compatibilidad con Windows Server 2016.

### v2.1 (2026-02-27)
- Fix: Reemplazo de em-dash por guion ASCII para compatibilidad de encoding en Windows.

### v2.0
- Fix: Parametro `UseWindowsAuth`, colision de variables, compatibilidad cross-platform de parametros PowerShell.

### v1.0
- Release inicial con `gather_sqlserver.ps1` y `export_etl.ps1`.
