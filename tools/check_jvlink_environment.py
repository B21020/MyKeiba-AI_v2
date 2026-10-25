"""Read only JV-Link installation metadata; never read service keys or accounts."""
import argparse
import importlib.util
import json
from pathlib import Path
import struct
import sys
import winreg


def inspect():
    result = {
        "python_bits": struct.calcsize("P") * 8, "python": sys.executable,
        "pywin32_available": importlib.util.find_spec("win32com") is not None,
        "com_registration": [], "installations": [], "registry_errors": [],
        "sdk_dlls": [], "service_keys_read": False,
    }
    for view, label in [(winreg.KEY_WOW64_64KEY, "64bit"), (winreg.KEY_WOW64_32KEY, "32bit")]:
        for root, root_name in [(winreg.HKEY_LOCAL_MACHINE, "HKLM"), (winreg.HKEY_CURRENT_USER, "HKCU")]:
            for progid in ["JVDTLab.JVLink", "JVDTLab.JVLink.1"]:
                try:
                    with winreg.OpenKey(root, "SOFTWARE\\Classes\\" + progid, 0, winreg.KEY_READ | view):
                        result["com_registration"].append({"view": label, "root": root_name, "progid": progid})
                except FileNotFoundError:
                    pass
                except PermissionError:
                    result["registry_errors"].append({"view": label, "root": root_name, "scope": "COM registration"})
            try:
                with winreg.OpenKey(root, "SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Uninstall", 0, winreg.KEY_READ | view) as base:
                    for i in range(winreg.QueryInfoKey(base)[0]):
                        try:
                            with winreg.OpenKey(base, winreg.EnumKey(base, i)) as child:
                                name = winreg.QueryValueEx(child, "DisplayName")[0]
                                if not any(s in name.lower() for s in ["jra-van", "jv-link", "jvdatalab"]):
                                    continue
                                item = {"name": name, "view": label, "root": root_name}
                                for key in ["DisplayVersion", "InstallLocation"]:
                                    try:
                                        item[key] = winreg.QueryValueEx(child, key)[0]
                                    except FileNotFoundError:
                                        pass
                                result["installations"].append(item)
                        except (FileNotFoundError, PermissionError, OSError):
                            pass
            except FileNotFoundError:
                pass
            except PermissionError:
                result["registry_errors"].append({"view": label, "root": root_name, "scope": "JV-Link installation lookup"})
    roots = [Path(item["InstallLocation"]) for item in result["installations"] if item.get("InstallLocation")]
    roots += [Path(p) for p in [r"C:\Program Files\JRA-VAN", r"C:\Program Files (x86)\JRA-VAN"]]
    for root in set(roots):
        if root.is_dir():
            result["sdk_dlls"].extend(str(p) for p in root.rglob("JVLink*.dll"))
    result["ready_for_collection"] = bool(result["com_registration"] or result["sdk_dlls"])
    # Installation detection is not a subscription or connectivity check.
    result["subscription_verified"] = False
    return result


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="data/odds/20261023_setup/environment_check.json")
    args = parser.parse_args()
    result = inspect()
    path = Path(args.out)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
