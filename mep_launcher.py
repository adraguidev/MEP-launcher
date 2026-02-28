"""
MEP Gatherer Launcher — Self-contained executable wrapper
Integratel Peru — Stefanini Group

Empaqueta los scripts PowerShell (gather_sqlserver.ps1 + export_etl.ps1)
y los ejecuta con -ExecutionPolicy Bypass y elevación de administrador.
Compatible con Windows Server 2012 R2 a 2022.
"""

import ctypes
import os
import subprocess
import sys
import tempfile
import shutil

# ---------------------------------------------------------------------------
# Embedded scripts — PyInstaller bundled files
# ---------------------------------------------------------------------------

def get_resource_path(relative_path: str) -> str:
    """Get path to bundled resource (works for PyInstaller --onefile)."""
    if hasattr(sys, "_MEIPASS"):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), relative_path)


# ---------------------------------------------------------------------------
# UAC elevation
# ---------------------------------------------------------------------------

def is_admin() -> bool:
    """Check if the process is running with administrator privileges."""
    try:
        return ctypes.windll.shell32.IsUserAnAdmin() != 0
    except Exception:
        return False


def elevate():
    """Re-launch the current process with UAC elevation."""
    if is_admin():
        return
    params = " ".join(f'"{a}"' for a in sys.argv)
    executable = sys.executable
    # ShellExecuteW with 'runas' triggers the UAC prompt
    ret = ctypes.windll.shell32.ShellExecuteW(
        None, "runas", executable, params, None, 1
    )
    if ret <= 32:
        print("\n[ERROR] No se pudo obtener permisos de administrador.")
        print("        Ejecute el programa haciendo clic derecho > 'Ejecutar como administrador'.")
        input("\nPresione ENTER para salir...")
        sys.exit(1)
    sys.exit(0)


# ---------------------------------------------------------------------------
# Console helpers
# ---------------------------------------------------------------------------

def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


def detect_sql_instances() -> list:
    """Detect locally installed SQL Server instances via the Windows registry.

    Returns a list of instance connection strings (e.g. ['MYSERVER', 'MYSERVER\\DEV']).
    Falls back to an empty list on any failure (non-Windows, no registry access, etc.).
    """
    if os.name != "nt":
        return []
    try:
        import winreg
        hostname = os.environ.get("COMPUTERNAME", "")
        if not hostname:
            return []
        instances = []
        # HKLM\SOFTWARE\Microsoft\Microsoft SQL Server\Instance Names\SQL
        # Each value name is an instance name, e.g. "MSSQLSERVER" (default) or "DEVTEST"
        key = winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            r"SOFTWARE\Microsoft\Microsoft SQL Server\Instance Names\SQL",
        )
        i = 0
        while True:
            try:
                name, _, _ = winreg.EnumValue(key, i)
                if name.upper() == "MSSQLSERVER":
                    instances.append(hostname)
                else:
                    instances.append(f"{hostname}\\{name}")
                i += 1
            except OSError:
                break
        winreg.CloseKey(key)
        return instances
    except Exception:
        return []


def print_header(server_instance: str):
    clear_screen()
    print("=" * 62)
    print(f"  MEP Gatherer para {server_instance}")
    print("  Fase 1: Metadata SQL Server  (gather_sqlserver.ps1)")
    print("  Fase 2: ETL/SSIS packages    (export_etl.ps1)")
    print("  Compatible con SQL Server 2008 R2 a 2022")
    print("=" * 62)
    print()


def ask_auth() -> dict:
    """Prompt for authentication method. Returns dict with auth params."""
    print("  AUTENTICACION:")
    print("    W) Windows (cuenta actual del equipo)")
    print("    S) SQL Server (usuario y password)")
    print()
    while True:
        choice = input("  Seleccione W o S: ").strip().upper()
        if choice in ("W", "S"):
            break
        print("  Opcion invalida. Ingrese W o S.")

    if choice == "W":
        print()
        print("  [OK] Se usara autenticacion Windows (integrada)")
        return {}

    print()
    sql_user = input("  Usuario SQL: ").strip()
    sql_pass = input("  Password:    ").strip()
    print()
    print("  [OK] Se usara autenticacion SQL Server")
    return {
        "UseWindowsAuth": "$false",
        "SqlUser": sql_user,
        "SqlPassword": sql_pass,
    }


def ask_action() -> str:
    """Prompt for what to execute. Returns choice string."""
    print()
    print("  QUE EJECUTAR:")
    print("    1) Recolectar TODO - metadata + ETL (RECOMENDADO)")
    print("    2) Solo metadata (gather_sqlserver.ps1)")
    print("    3) Solo ETL/SSIS (export_etl.ps1)")
    print("    4) Custom (especificar BDs/schemas)")
    print("    5) Cancelar")
    print()
    while True:
        choice = input("  Seleccione (1-5): ").strip()
        if choice in ("1", "2", "3", "4", "5"):
            return choice
        print("  Opcion invalida.")


def ask_custom() -> dict:
    """Prompt for custom databases and schemas."""
    print()
    dbs = input("  Bases de datos (separadas por coma, ENTER para todas): ").strip()
    schemas = input("  Schemas (separados por coma, ENTER para todos):      ").strip()
    result = {}
    if dbs:
        result["Databases"] = dbs
    if schemas:
        result["Schemas"] = schemas
    return result


# ---------------------------------------------------------------------------
# Script execution
# ---------------------------------------------------------------------------

def run_ps1(script_path: str, server_instance: str, extra_params=None):
    """Execute a PowerShell script with -ExecutionPolicy Bypass."""
    args = [
        "powershell.exe",
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", script_path,
        "-ServerInstance", server_instance,
    ]
    if extra_params:
        for key, value in extra_params.items():
            args.extend([f"-{key}", value])

    print(f"\n  Ejecutando: {os.path.basename(script_path)}...")
    print("-" * 62)
    result = subprocess.run(args, cwd=os.path.dirname(script_path))
    print("-" * 62)
    if result.returncode != 0:
        print(f"  [WARN] El script termino con codigo {result.returncode}")
    return result.returncode


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # Step 0: Ensure admin
    if os.name == "nt":
        elevate()

    # Step 1: Extract bundled scripts to a working directory
    if getattr(sys, "frozen", False):
        exe_dir = os.path.dirname(sys.executable)
    else:
        exe_dir = os.path.dirname(os.path.abspath(__file__))

    work_dir = os.path.join(exe_dir, "mep_scripts")
    os.makedirs(work_dir, exist_ok=True)

    scripts = ["gather_sqlserver.ps1", "export_etl.ps1"]
    for script in scripts:
        src = get_resource_path(os.path.join("scripts", script))
        dst = os.path.join(work_dir, script)
        if os.path.exists(src):
            shutil.copy2(src, dst)
        else:
            print(f"  [ERROR] Script no encontrado: {script}")
            print(f"          Buscado en: {src}")
            input("\nPresione ENTER para salir...")
            sys.exit(1)

    # Step 2: Ask for server instance
    clear_screen()
    print("=" * 62)
    print("  MEP Gatherer — Stefanini Group")
    print("  Recolector automatizado de metadata SQL Server")
    print("=" * 62)
    print()
    if os.name == "nt" and is_admin():
        print("  [OK] Ejecutando como Administrador")
        print()

    # Auto-detect installed SQL Server instances
    detected = detect_sql_instances()
    if detected:
        print("  Instancias SQL Server detectadas:")
        for idx, inst in enumerate(detected, 1):
            print(f"    {idx}) {inst}")
        print(f"    O) Otra (ingresar manualmente)")
        print()
        while True:
            choice = input(f"  Seleccione (1-{len(detected)}) o O para otra: ").strip().upper()
            if choice == "O":
                server_instance = input("  Instancia SQL Server: ").strip()
                break
            if choice.isdigit() and 1 <= int(choice) <= len(detected):
                server_instance = detected[int(choice) - 1]
                break
            print("  Opcion invalida.")
    else:
        server_instance = input("  Instancia SQL Server (ej: MISERVIDOR, MISERVIDOR\\INST1, 10.0.1.5,1433): ").strip()

    if not server_instance:
        print("\n  [ERROR] Debe ingresar una instancia de SQL Server.")
        input("\nPresione ENTER para salir...")
        sys.exit(1)

    # Step 3: Interactive menu
    print_header(server_instance)

    auth_params = ask_auth()
    action = ask_action()

    if action == "5":
        print("\n  Cancelado.")
        sys.exit(0)

    gather_path = os.path.join(work_dir, "gather_sqlserver.ps1")
    etl_path = os.path.join(work_dir, "export_etl.ps1")

    if action == "1":
        print("\n=== Fase 1/2: Recolectando metadata de SQL Server... ===")
        run_ps1(gather_path, server_instance, auth_params)
        print("\n=== Fase 2/2: Exportando paquetes ETL/SSIS... ===")
        run_ps1(etl_path, server_instance, auth_params)

    elif action == "2":
        print("\n  Ejecutando solo metadata...")
        run_ps1(gather_path, server_instance, auth_params)

    elif action == "3":
        print("\n  Ejecutando solo ETL/SSIS...")
        run_ps1(etl_path, server_instance, auth_params)

    elif action == "4":
        custom = ask_custom()
        params = {**auth_params, **custom}
        print("\n=== Fase 1/2: Metadata (custom)... ===")
        run_ps1(gather_path, server_instance, params)
        print("\n=== Fase 2/2: ETL/SSIS... ===")
        run_ps1(etl_path, server_instance, auth_params)

    # Done
    print()
    print("=" * 62)
    print("  COMPLETADO. Comprima las carpetas mep_* y entregue.")
    print("=" * 62)
    input("\nPresione ENTER para salir...")


if __name__ == "__main__":
    main()
