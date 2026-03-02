# MEP Gatherer - Riesgos para Produccion

Listado de riesgos identificados para cuando el exe se ejecute en servidores de cliente.
Priorizar de arriba hacia abajo.

---

## CRITICO

### 1. `sqlcmd` no existe en el servidor
- **Impacto**: El script falla silenciosamente. No recolecta nada.
- **Causa**: Servidores con solo el engine SQL instalado (sin "Command Line Utilities") no tienen `sqlcmd` en PATH.
- **Solucion propuesta**: Validar al inicio del script que `sqlcmd` o `Invoke-Sqlcmd` existan. Si no hay ninguno, mostrar mensaje claro con instrucciones de instalacion y abortar.
- **Estado**: RESUELTO (v2.9) - Ambos scripts validan al inicio y abortan con instrucciones claras. Funciones con fallback a Invoke-Sqlcmd cuando sqlcmd no existe.

---

## ALTO

### 2. Passwords con caracteres especiales
- **Impacto**: Falla de autenticacion silenciosa (el password llega alterado).
- **Causa**: Caracteres como `$`, `` ` ``, `"`, `!` son interpretados por PowerShell antes de llegar a sqlcmd via `-P`.
- **Ejemplo**: Password `Pa$$w0rd` -> PowerShell interpreta `$$` como variable -> sqlcmd recibe `Pa0rd`.
- **Solucion propuesta**: Escapar el password o usar variables de entorno (`$env:SQLCMDPASSWORD`) en lugar de `-P`.
- **Estado**: Pendiente

---

## MEDIO

### 3. Sin permisos a `msdb`
- **Impacto**: Queries de Agent Jobs, paquetes SSIS legacy y schedules fallan con error de permisos.
- **Causa**: El login SQL proporcionado puede no tener acceso a msdb (comun en ambientes restringidos).
- **Solucion propuesta**: Envolver queries a msdb en try/catch con mensaje claro: "Sin acceso a msdb - se omite recoleccion de Jobs/SSIS legacy".
- **Estado**: Pendiente

### 4. Nombres de BD con caracteres especiales
- **Impacto**: Queries fallan para BDs con espacios, guiones o puntos en el nombre.
- **Causa**: Los nombres de BD se inyectan en queries sin `[brackets]`. Ej: `My Database` genera SQL invalido.
- **Ejemplo**: `SELECT ... FROM My Database.sys.objects` -> error de sintaxis.
- **Solucion propuesta**: Envolver todos los nombres de BD en `[$db]` en las queries generadas.
- **Estado**: Pendiente

### 5. Sin permisos de escritura en disco
- **Impacto**: El script falla al intentar crear carpetas o escribir CSVs.
- **Causa**: El exe se ejecuta en una ubicacion protegida, o el usuario no tiene permisos de escritura.
- **Solucion propuesta**: Validar escritura al inicio (crear archivo temporal en OutputDir). Si falla, sugerir mover el exe a `C:\temp` o especificar `-OutputDir`.
- **Estado**: Pendiente

---

## BAJO

### 6. Timeouts en BDs muy grandes
- **Impacto**: Queries de metadata (especialmente SP code, indexes) pueden demorar mas de lo esperado en BDs con miles de objetos.
- **Causa**: `Invoke-Sqlcmd` tiene `QueryTimeout = 600` (10 min), pero `sqlcmd` no tiene timeout explicito (espera indefinidamente).
- **Solucion propuesta**: Agregar `-t 600` a todas las invocaciones de sqlcmd para consistencia.
- **Estado**: Pendiente

### 7. Antivirus bloquea el exe
- **Impacto**: El exe no se ejecuta o es eliminado automaticamente.
- **Causa**: PyInstaller genera ejecutables que algunos antivirus marcan como sospechosos (falso positivo comun).
- **Solucion propuesta**: Documentar en README que puede requerir exclusion en antivirus. Alternativa: ofrecer ejecucion directa via PowerShell sin exe.
- **Estado**: Documentar

---

## YA RESUELTO

| Riesgo | Version |
|--------|---------|
| SQLPS module vs SqlServer module (`-Credential` vs `-Username`) | v2.6 |
| SQL Auth no se pasaba a los scripts (`$false` literal) | v2.8 |
| SSISDB .ispac requiere Windows Auth | v2.8 (fallback con inventario) |
| dtutil de Express se encontraba primero en PATH | v2.3 |
| Caracteres non-ASCII / BOM UTF-8 en Windows Server 2016 | v2.2 |
| Provider SQLSERVER:\ rompe paths de filesystem | v4.1 (gather) |
| Execution Policy bloquea scripts | Siempre usa `-ExecutionPolicy Bypass` |
| Deteccion de version SQL Server | Automatica desde v1.0 |
| `sqlcmd` no existe en el servidor | v2.9 (validacion + fallback Invoke-Sqlcmd) |
