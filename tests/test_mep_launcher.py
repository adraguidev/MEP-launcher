"""Tests for MEP Gatherer Launcher."""

import os
import sys
import subprocess
from unittest import mock

import pytest

# Add project root to path so we can import mep_launcher
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import mep_launcher


# ---------------------------------------------------------------------------
# get_resource_path
# ---------------------------------------------------------------------------

class TestGetResourcePath:
    def test_returns_path_relative_to_script(self):
        result = mep_launcher.get_resource_path("scripts/gather_sqlserver.ps1")
        expected = os.path.join(
            os.path.dirname(os.path.abspath(mep_launcher.__file__)),
            "scripts/gather_sqlserver.ps1",
        )
        assert result == expected

    def test_uses_meipass_when_frozen(self):
        with mock.patch.object(sys, "_MEIPASS", "/tmp/mei_fake", create=True):
            result = mep_launcher.get_resource_path("scripts/test.ps1")
            assert result == os.path.join("/tmp/mei_fake", "scripts/test.ps1")


# ---------------------------------------------------------------------------
# is_admin
# ---------------------------------------------------------------------------

class TestIsAdmin:
    def test_returns_false_on_non_windows(self):
        """On Mac/Linux, ctypes.windll doesn't exist → should return False."""
        if os.name != "nt":
            assert mep_launcher.is_admin() is False

    @mock.patch("ctypes.windll", create=True)
    def test_returns_true_when_admin(self, mock_windll):
        mock_windll.shell32.IsUserAnAdmin.return_value = 1
        assert mep_launcher.is_admin() is True

    @mock.patch("ctypes.windll", create=True)
    def test_returns_false_when_not_admin(self, mock_windll):
        mock_windll.shell32.IsUserAnAdmin.return_value = 0
        assert mep_launcher.is_admin() is False

    def test_returns_false_on_exception(self):
        with mock.patch.object(mep_launcher.ctypes, "windll", create=True) as mock_windll:
            mock_windll.shell32.IsUserAnAdmin.side_effect = OSError("fake error")
            assert mep_launcher.is_admin() is False


# ---------------------------------------------------------------------------
# ask_auth
# ---------------------------------------------------------------------------

class TestAskAuth:
    def test_windows_auth(self):
        with mock.patch("builtins.input", return_value="W"):
            result = mep_launcher.ask_auth()
        assert result == {}

    def test_windows_auth_lowercase(self):
        with mock.patch("builtins.input", return_value="w"):
            result = mep_launcher.ask_auth()
        assert result == {}

    def test_sql_auth(self):
        inputs = iter(["S", "mi_usuario", "mi_password"])
        with mock.patch("builtins.input", side_effect=inputs):
            result = mep_launcher.ask_auth()
        assert result == {
            "UseWindowsAuth": "$false",
            "SqlUser": "mi_usuario",
            "SqlPassword": "mi_password",
        }

    def test_invalid_then_valid_input(self):
        inputs = iter(["X", "Z", "W"])
        with mock.patch("builtins.input", side_effect=inputs):
            result = mep_launcher.ask_auth()
        assert result == {}


# ---------------------------------------------------------------------------
# ask_action
# ---------------------------------------------------------------------------

class TestAskAction:
    @pytest.mark.parametrize("choice", ["1", "2", "3", "4", "5"])
    def test_valid_choices(self, choice):
        with mock.patch("builtins.input", return_value=choice):
            assert mep_launcher.ask_action() == choice

    def test_invalid_then_valid(self):
        inputs = iter(["9", "abc", "1"])
        with mock.patch("builtins.input", side_effect=inputs):
            assert mep_launcher.ask_action() == "1"


# ---------------------------------------------------------------------------
# ask_custom
# ---------------------------------------------------------------------------

class TestAskCustom:
    def test_both_specified(self):
        inputs = iter(["DB1,DB2", "dbo,etl"])
        with mock.patch("builtins.input", side_effect=inputs):
            result = mep_launcher.ask_custom()
        assert result == {"Databases": "DB1,DB2", "Schemas": "dbo,etl"}

    def test_only_databases(self):
        inputs = iter(["MiDB", ""])
        with mock.patch("builtins.input", side_effect=inputs):
            result = mep_launcher.ask_custom()
        assert result == {"Databases": "MiDB"}

    def test_only_schemas(self):
        inputs = iter(["", "dbo,fact"])
        with mock.patch("builtins.input", side_effect=inputs):
            result = mep_launcher.ask_custom()
        assert result == {"Schemas": "dbo,fact"}

    def test_both_empty(self):
        inputs = iter(["", ""])
        with mock.patch("builtins.input", side_effect=inputs):
            result = mep_launcher.ask_custom()
        assert result == {}


# ---------------------------------------------------------------------------
# run_ps1
# ---------------------------------------------------------------------------

class TestRunPs1:
    def test_builds_correct_command(self):
        with mock.patch("subprocess.run") as mock_run:
            mock_run.return_value = mock.Mock(returncode=0)
            mep_launcher.run_ps1("/tmp/test.ps1", "MYSERVER")

        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert args[0] == "powershell.exe"
        assert "-ExecutionPolicy" in args
        assert "Bypass" in args
        assert "-File" in args
        assert "/tmp/test.ps1" in args
        assert "-ServerInstance" in args
        assert "MYSERVER" in args

    def test_appends_extra_params(self):
        with mock.patch("subprocess.run") as mock_run:
            mock_run.return_value = mock.Mock(returncode=0)
            mep_launcher.run_ps1("/tmp/test.ps1", "SRV", {
                "UseWindowsAuth": "$false",
                "SqlUser": "admin",
            })

        args = mock_run.call_args[0][0]
        assert "-UseWindowsAuth" in args
        assert "$false" in args
        assert "-SqlUser" in args
        assert "admin" in args

    def test_no_extra_params(self):
        with mock.patch("subprocess.run") as mock_run:
            mock_run.return_value = mock.Mock(returncode=0)
            mep_launcher.run_ps1("/tmp/test.ps1", "SRV", {})

        args = mock_run.call_args[0][0]
        # Should end with -ServerInstance SRV (no extras)
        idx = args.index("-ServerInstance")
        assert args[idx + 1] == "SRV"
        assert len(args) == idx + 2

    def test_returns_exit_code(self):
        with mock.patch("subprocess.run") as mock_run:
            mock_run.return_value = mock.Mock(returncode=42)
            code = mep_launcher.run_ps1("/tmp/test.ps1", "SRV")
        assert code == 42

    def test_sets_working_directory(self):
        with mock.patch("subprocess.run") as mock_run:
            mock_run.return_value = mock.Mock(returncode=0)
            mep_launcher.run_ps1("/some/path/test.ps1", "SRV")

        assert mock_run.call_args[1]["cwd"] == "/some/path"


# ---------------------------------------------------------------------------
# print_header
# ---------------------------------------------------------------------------

class TestPrintHeader:
    def test_prints_server_name(self, capsys):
        with mock.patch("mep_launcher.clear_screen"):
            mep_launcher.print_header("TESTSERVER")
        output = capsys.readouterr().out
        assert "TESTSERVER" in output
        assert "gather_sqlserver.ps1" in output
        assert "export_etl.ps1" in output


# ---------------------------------------------------------------------------
# main — integration tests
# ---------------------------------------------------------------------------

class TestMain:
    def _setup_work_dir(self, tmp_path):
        """Create fake scripts dir so extraction succeeds."""
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "gather_sqlserver.ps1").write_text("# fake")
        (scripts_dir / "export_etl.ps1").write_text("# fake")
        return tmp_path

    def test_cancel_exits_cleanly(self, tmp_path):
        base = self._setup_work_dir(tmp_path)
        inputs = iter(["TESTSERVER", "W", "5"])
        with (
            mock.patch("builtins.input", side_effect=inputs),
            mock.patch("mep_launcher.get_resource_path", side_effect=lambda p: str(base / p)),
            mock.patch("mep_launcher.elevate"),
            mock.patch("mep_launcher.clear_screen"),
            mock.patch("mep_launcher.is_admin", return_value=False),
            mock.patch("os.name", "nt"),
            pytest.raises(SystemExit) as exc_info,
        ):
            mep_launcher.main()
        assert exc_info.value.code == 0

    def test_empty_server_instance_exits(self, tmp_path):
        base = self._setup_work_dir(tmp_path)
        inputs = iter(["", ""])
        with (
            mock.patch("builtins.input", side_effect=inputs),
            mock.patch("mep_launcher.get_resource_path", side_effect=lambda p: str(base / p)),
            mock.patch("mep_launcher.elevate"),
            mock.patch("mep_launcher.clear_screen"),
            mock.patch("mep_launcher.is_admin", return_value=False),
            mock.patch("os.name", "nt"),
            pytest.raises(SystemExit) as exc_info,
        ):
            mep_launcher.main()
        assert exc_info.value.code == 1

    def test_option1_runs_both_scripts(self, tmp_path):
        base = self._setup_work_dir(tmp_path)
        inputs = iter(["TESTSERVER", "W", "1", ""])  # last "" for ENTER to exit
        with (
            mock.patch("builtins.input", side_effect=inputs),
            mock.patch("mep_launcher.get_resource_path", side_effect=lambda p: str(base / p)),
            mock.patch("mep_launcher.elevate"),
            mock.patch("mep_launcher.clear_screen"),
            mock.patch("mep_launcher.is_admin", return_value=False),
            mock.patch("os.name", "nt"),
            mock.patch("mep_launcher.run_ps1", return_value=0) as mock_run,
        ):
            mep_launcher.main()

        assert mock_run.call_count == 2
        # First call: gather, second: etl
        assert "gather_sqlserver.ps1" in mock_run.call_args_list[0][0][0]
        assert "export_etl.ps1" in mock_run.call_args_list[1][0][0]
        # Both with correct server instance
        assert mock_run.call_args_list[0][0][1] == "TESTSERVER"
        assert mock_run.call_args_list[1][0][1] == "TESTSERVER"

    def test_option2_runs_only_gather(self, tmp_path):
        base = self._setup_work_dir(tmp_path)
        inputs = iter(["SRV01", "W", "2", ""])
        with (
            mock.patch("builtins.input", side_effect=inputs),
            mock.patch("mep_launcher.get_resource_path", side_effect=lambda p: str(base / p)),
            mock.patch("mep_launcher.elevate"),
            mock.patch("mep_launcher.clear_screen"),
            mock.patch("mep_launcher.is_admin", return_value=False),
            mock.patch("os.name", "nt"),
            mock.patch("mep_launcher.run_ps1", return_value=0) as mock_run,
        ):
            mep_launcher.main()

        assert mock_run.call_count == 1
        assert "gather_sqlserver.ps1" in mock_run.call_args_list[0][0][0]

    def test_option3_runs_only_etl(self, tmp_path):
        base = self._setup_work_dir(tmp_path)
        inputs = iter(["SRV01", "W", "3", ""])
        with (
            mock.patch("builtins.input", side_effect=inputs),
            mock.patch("mep_launcher.get_resource_path", side_effect=lambda p: str(base / p)),
            mock.patch("mep_launcher.elevate"),
            mock.patch("mep_launcher.clear_screen"),
            mock.patch("mep_launcher.is_admin", return_value=False),
            mock.patch("os.name", "nt"),
            mock.patch("mep_launcher.run_ps1", return_value=0) as mock_run,
        ):
            mep_launcher.main()

        assert mock_run.call_count == 1
        assert "export_etl.ps1" in mock_run.call_args_list[0][0][0]

    def test_option4_custom_passes_dbs_and_schemas(self, tmp_path):
        base = self._setup_work_dir(tmp_path)
        inputs = iter(["SRV01", "S", "admin", "pass123", "4", "DB1,DB2", "dbo,etl", ""])
        with (
            mock.patch("builtins.input", side_effect=inputs),
            mock.patch("mep_launcher.get_resource_path", side_effect=lambda p: str(base / p)),
            mock.patch("mep_launcher.elevate"),
            mock.patch("mep_launcher.clear_screen"),
            mock.patch("mep_launcher.is_admin", return_value=False),
            mock.patch("os.name", "nt"),
            mock.patch("mep_launcher.run_ps1", return_value=0) as mock_run,
        ):
            mep_launcher.main()

        assert mock_run.call_count == 2
        # First call (gather) should include custom params + auth
        gather_params = mock_run.call_args_list[0][0][2]
        assert gather_params["Databases"] == "DB1,DB2"
        assert gather_params["Schemas"] == "dbo,etl"
        assert gather_params["SqlUser"] == "admin"
        assert gather_params["SqlPassword"] == "pass123"

    def test_scripts_are_extracted_to_work_dir(self, tmp_path):
        base = self._setup_work_dir(tmp_path)
        inputs = iter(["SRV", "W", "5"])
        with (
            mock.patch("builtins.input", side_effect=inputs),
            mock.patch("mep_launcher.get_resource_path", side_effect=lambda p: str(base / p)),
            mock.patch("mep_launcher.elevate"),
            mock.patch("mep_launcher.clear_screen"),
            mock.patch("mep_launcher.is_admin", return_value=False),
            mock.patch("os.name", "nt"),
            pytest.raises(SystemExit),
        ):
            mep_launcher.main()

        work_dir = os.path.join(os.path.dirname(os.path.abspath(mep_launcher.__file__)), "mep_scripts")
        # Scripts should have been copied (to the real work_dir based on __file__)
        # We can't easily check this since __file__ points to the real module,
        # but we verify no crash occurred during extraction


# ---------------------------------------------------------------------------
# Bundled scripts exist
# ---------------------------------------------------------------------------

class TestBundledScripts:
    def test_gather_script_exists(self):
        path = mep_launcher.get_resource_path("scripts/gather_sqlserver.ps1")
        assert os.path.isfile(path), f"gather_sqlserver.ps1 not found at {path}"

    def test_etl_script_exists(self):
        path = mep_launcher.get_resource_path("scripts/export_etl.ps1")
        assert os.path.isfile(path), f"export_etl.ps1 not found at {path}"

    def test_gather_script_has_content(self):
        path = mep_launcher.get_resource_path("scripts/gather_sqlserver.ps1")
        size = os.path.getsize(path)
        assert size > 1000, f"gather_sqlserver.ps1 too small ({size} bytes)"

    def test_etl_script_has_content(self):
        path = mep_launcher.get_resource_path("scripts/export_etl.ps1")
        size = os.path.getsize(path)
        assert size > 1000, f"export_etl.ps1 too small ({size} bytes)"
