#!/bin/bash
# ============================================================
# MEP Gatherer — Docker Test Runner
# ============================================================
set -e

MSSQL_HOST="${MSSQL_HOST:-mssql}"
MSSQL_PASS="${MSSQL_SA_PASSWORD:-MepTest#2024!}"
OUTPUT_DIR="/mep/output"

echo "============================================================"
echo "  MEP Gatherer — Docker Test"
echo "  Host: $MSSQL_HOST"
echo "============================================================"

echo ""
echo "=== Verificando conexion a SQL Server... ==="
sqlcmd -S "$MSSQL_HOST" -U sa -P "$MSSQL_PASS" -Q "SELECT @@VERSION" -h -1 -W
echo "[OK] Conexion exitosa"

# Run gather_sqlserver.ps1
echo ""
echo "============================================================"
echo "=== FASE 1: gather_sqlserver.ps1 ==="
echo "============================================================"
pwsh -NoProfile -Command "& /mep/scripts/gather_sqlserver.ps1 -ServerInstance '$MSSQL_HOST' -UseWindowsAuth 'false' -SqlUser 'sa' -SqlPassword '$MSSQL_PASS' -OutputDir '/mep/output/mep_sqlserver'"

# Run export_etl.ps1
echo ""
echo "============================================================"
echo "=== FASE 2: export_etl.ps1 ==="
echo "============================================================"
pwsh -NoProfile -Command "& /mep/scripts/export_etl.ps1 -ServerInstance '$MSSQL_HOST' -UseWindowsAuth 'false' -SqlUser 'sa' -SqlPassword '$MSSQL_PASS' -OutputDir '/mep/output/mep_etl'" || true

# Validate output
echo ""
echo "============================================================"
echo "=== VALIDACION DE RESULTADOS ==="
echo "============================================================"

ERRORS=0

if [ -d "$OUTPUT_DIR/mep_sqlserver" ]; then
    FILE_COUNT=$(find "$OUTPUT_DIR/mep_sqlserver" -type f | wc -l)
    echo "[OK] gather_sqlserver genero $FILE_COUNT archivos"

    for f in "_instance/01_server_config.csv" "_instance/05_databases.csv"; do
        if [ -f "$OUTPUT_DIR/mep_sqlserver/$f" ]; then
            LINES=$(wc -l < "$OUTPUT_DIR/mep_sqlserver/$f")
            echo "  [OK] $f ($LINES lineas)"
        else
            echo "  [FAIL] $f NO ENCONTRADO"
            ERRORS=$((ERRORS + 1))
        fi
    done

    for db in MEP_TestDB MEP_Staging; do
        if [ -d "$OUTPUT_DIR/mep_sqlserver/$db" ]; then
            DB_FILES=$(find "$OUTPUT_DIR/mep_sqlserver/$db" -type f | wc -l)
            echo "  [OK] $db/ ($DB_FILES archivos)"
        else
            echo "  [FAIL] $db/ NO ENCONTRADO"
            ERRORS=$((ERRORS + 1))
        fi
    done

    for schema in dbo staging dim fact; do
        SCHEMA_DIR="$OUTPUT_DIR/mep_sqlserver/MEP_TestDB/$schema"
        if [ -d "$SCHEMA_DIR" ]; then
            SCHEMA_FILES=$(find "$SCHEMA_DIR" -type f | wc -l)
            echo "  [OK] MEP_TestDB/$schema/ ($SCHEMA_FILES archivos)"
        else
            echo "  [WARN] MEP_TestDB/$schema/ no encontrado"
        fi
    done
else
    echo "[FAIL] Directorio mep_sqlserver NO EXISTE"
    ERRORS=$((ERRORS + 1))
fi

if [ -d "$OUTPUT_DIR/mep_etl" ]; then
    ETL_FILES=$(find "$OUTPUT_DIR/mep_etl" -type f | wc -l)
    echo "[OK] export_etl genero $ETL_FILES archivos"
else
    echo "[WARN] export_etl sin output (esperado: Azure SQL Edge no tiene SSIS/Agent)"
fi

echo ""
echo "=== Estructura de output ==="
find "$OUTPUT_DIR" -type f | sort | head -80

echo ""
echo "============================================================"
if [ $ERRORS -eq 0 ]; then
    echo "  RESULTADO: OK — Todos los checks pasaron"
else
    echo "  RESULTADO: $ERRORS errores encontrados"
fi
echo "============================================================"

exit $ERRORS
