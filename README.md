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

---

## Changelog

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
