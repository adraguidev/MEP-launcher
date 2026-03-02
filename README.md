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

### Que necesita el usuario para ejecutar

1. Copiar `MEP_Gatherer.exe` al servidor
2. Ejecutar como Administrador
3. El exe pide 3 cosas interactivamente:

| Paso | Pregunta | Opciones |
|------|----------|----------|
| 1 | Instancia SQL Server | Autodetecta las instaladas, o ingresar manualmente |
| 2 | Autenticacion | **W** = Windows (cuenta actual) / **S** = SQL Server (usuario + password) |
| 3 | Que ejecutar | **1** = Todo (recomendado) / **2** = Solo metadata / **3** = Solo ETL / **4** = Custom / **5** = Cancelar |

La **opcion 4 (Custom)** es la unica que pide inputs adicionales:
- Bases de datos (separadas por coma, o ENTER para todas)
- Schemas (separados por coma, o ENTER para todos)

### Como selecciona bases de datos

| Opcion elegida | Comportamiento |
|----------------|---------------|
| **1, 2, 3** (Todo/Metadata/ETL) | Descubre **automaticamente** todas las BDs de usuario |
| **4** (Custom) sin especificar BDs | Igual: descubre todas automaticamente |
| **4** (Custom) con BDs especificas | Solo procesa las BDs indicadas |

El autodescubrimiento ejecuta:
```sql
SELECT name FROM sys.databases
WHERE database_id > 4       -- excluye master, model, msdb, tempdb
AND state_desc = 'ONLINE'   -- solo BDs accesibles
```

**Ejemplo**: un servidor con BDs `DWH`, `Staging`, `AppData` -> las 3 se procesan automaticamente.
Con Custom + `DWH,Staging` -> solo esas 2.

### Como selecciona schemas dentro de cada BD

| Opcion elegida | Comportamiento |
|----------------|---------------|
| **1, 2, 3** (Todo/Metadata/ETL) | Descubre **automaticamente** todos los schemas con objetos |
| **4** (Custom) sin especificar schemas | Igual: descubre todos automaticamente |
| **4** (Custom) con schemas especificos | Solo procesa los schemas indicados (en todas las BDs) |

El autodescubrimiento ejecuta por cada BD:
```sql
SELECT DISTINCT s.name
FROM sys.schemas s
INNER JOIN sys.objects o ON s.schema_id = o.schema_id
WHERE s.name NOT IN ('sys','INFORMATION_SCHEMA','guest',
      'db_owner','db_accessadmin', ... otros roles built-in ...)
AND o.type IN ('U','V','P','FN','IF','TF','TR')
```

Es decir: **solo schemas que tienen al menos una tabla, vista, SP, funcion o trigger**. Los schemas vacios y de sistema se omiten.

**Ejemplo**: BD con schemas `dbo`, `etl`, `staging`, `dim`, `fact` (todos con tablas) -> genera 14 CSVs por schema = 70 archivos para esa BD.

### Flujo de ejecucion completo

```
1. INICIO (exe)
   - Solicita permisos de Administrador (UAC)
   - Detecta instancias SQL Server instaladas (registro de Windows)
   - Pide autenticacion y accion a ejecutar
   - Valida que exista sqlcmd o Invoke-Sqlcmd en el servidor

2. METADATA (opcion 1 o 2)
   Fase 0: Detecta version SQL Server (2008R2-2022) y adapta queries
   Fase 1: Info de instancia             -> _instance/ (config, jobs, logins)
   Fase 2: Descubre BDs de usuario       -> lista de BDs a procesar
   Fase 3: Por cada BD:
           a) Descubre schemas con objetos
           b) Info a nivel de BD          -> BD/_database/ (principals, roles, permisos)
           c) Por cada schema             -> BD/schema/ (14 CSVs: tablas, PKs, FKs,
              indexes, SPs, funciones, triggers, vistas, sizes, dependencias...)
   Fase 4: Resumen con conteo de archivos

3. ETL/SSIS (opcion 1 o 3)
   Fase 1: SSISDB catalog     -> proyectos .ispac, .dtsx, parametros, historial
   Fase 2: MSDB legacy        -> paquetes SSIS almacenados en msdb
   Fase 3: Agent Jobs         -> SQL embebido en job steps, referencias a SSIS
   Fase 4: File system scan   -> .dtsx y .dtsConfig encontrados en disco
   Post-proceso: extrae SQL embebido, connections, dataflows de cada .dtsx
   Sanitizacion: redacta passwords/tokens en todo el output
```

### Trazabilidad del run

Cada ejecucion genera **logs detallados** con timestamp de cada operacion dentro de las carpetas de output:

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

| Archivo de log | Ubicacion | Contenido |
|----------------|-----------|-----------|
| `gather_sqlserver.log` | Dentro de `mep_sqlserver_*/` | Cada query: filas, tamano, tiempo. Errores con `[ERROR]`/`[WARN]` |
| `export_etl.log` | Dentro de `mep_etl_*/` | Paquetes exportados, artefactos extraidos, errores |

Los errores **no detienen** la ejecucion: si una query falla, se registra y continua con la siguiente.

### Prerequisitos en el servidor

- Windows Server 2012 R2 o superior
- SQL Server instalado (2008 R2 a 2022)
- Ejecutar como **Administrador**
- El exe detecta automaticamente las herramientas SQL disponibles (`sqlcmd` o `Invoke-Sqlcmd`). Si no encuentra ninguna, muestra instrucciones de instalacion y se detiene.

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

### v2.9 (2026-03-02)
- Fix: Validacion al inicio de que `sqlcmd` o `Invoke-Sqlcmd` existan en el servidor. Si no hay ninguno, muestra instrucciones de instalacion y aborta (Riesgo #1).
- Fix: Passwords con caracteres especiales (`$`, `` ` ``, `"`, `!`) ya no fallan. Se usa `$env:SQLCMDPASSWORD` (variable oficial Microsoft) en vez de pasar el password via `-P` en la linea de comandos (Riesgo #2).
- Mejora: Fallback automatico a `Invoke-Sqlcmd` cuando `sqlcmd` no esta disponible en ambos scripts.
- Seguridad: El password ya no aparece en la linea de comandos del proceso. Variable de entorno se limpia al finalizar.

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
