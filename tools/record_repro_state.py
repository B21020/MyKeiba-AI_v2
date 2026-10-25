"""実験開始時点の作業状態（コード・環境・データ）を記録する。

目的:
- 再開計画 4.1「再現できる作業状態を保存する」の実装。
- 未コミットの差分と未追跡ソースを保存し、どのコードで実験したかを後から特定できるようにする。
- Python・ライブラリのバージョン、マシンの空き容量・メモリを記録する。
- データファイルごとに件数・サイズ・更新日時・SHA-256 を台帳にする（原本と .bak を区別）。

使い方例:
  .venv/Scripts/python.exe tools/record_repro_state.py --out reproducibility/20261023_baseline
  # 件数の集計を省き、ハッシュだけ取る場合
  .venv/Scripts/python.exe tools/record_repro_state.py --out ... --no-profile

出力（--out 配下）:
- git_state.txt             : ブランチ・HEAD・git status
- tracked_changes.patch     : HEAD に対する未コミット差分（git apply で復元可能）
- untracked_sources.tar.gz  : 未追跡（.gitignore 対象外）ファイル一式
- untracked_sources.csv     : 上記の一覧と SHA-256
- environment.json / pip_freeze.txt : 実行環境
- data_manifest.csv         : データ台帳（パス・役割・サイズ・更新日時・SHA-256・行数など）
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import pickle
import platform
import subprocess
import sys
import tarfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# 台帳に載せる場所（data/html は件数が膨大なので数とサイズの集計だけにする）
MANIFEST_GLOBS = [
    "data/raw/*",
    "data/tmp/*",
    "data/tmp/**/*",
    "data/master/*",
    "data/score_tables/**/*",
    "data/score_20260207/**/*",
    "data/*.csv",
    "models/**/*.pickle",
]
# 中身を読んで件数を数えるファイルの上限サイズ（horse_results.pickle は別途 Parquet 化の際に数える）
PROFILE_MAX_BYTES = 2 * 1024**3


def run(cmd: list[str]) -> str:
    return subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, encoding="utf-8", check=True).stdout


def sha256(path: Path, chunk: int = 16 * 1024**2) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while block := f.read(chunk):
            h.update(block)
    return h.hexdigest()


def role_of(rel: str) -> str:
    name = Path(rel).name
    if ".bak" in name or "backup" in name:
        return "backup"
    if rel.startswith("data/raw/"):
        return "raw"
    if rel.startswith("data/tmp/featured_data"):
        return "feature_cache"
    if rel.startswith("data/master/"):
        return "master"
    if rel.startswith("models/"):
        return "model"
    return "derived"


def save_git_state(out: Path) -> None:
    lines = [
        f"recorded_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"branch: {run(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).strip()}",
        f"head: {run(['git', 'rev-parse', 'HEAD']).strip()}",
        f"head_log: {run(['git', 'log', '-1', '--format=%h %ad %s', '--date=iso']).strip()}",
        "",
        "## git status --short",
        run(["git", "status", "--short"]),
    ]
    (out / "git_state.txt").write_text("\n".join(lines), encoding="utf-8")

    patch = subprocess.run(["git", "diff", "HEAD", "--binary"], cwd=REPO_ROOT, capture_output=True, check=True).stdout
    (out / "tracked_changes.patch").write_bytes(patch)

    untracked = [p for p in run(["git", "ls-files", "--others", "--exclude-standard", "-z"]).split("\0") if p]
    # 出力先自身は除く
    out_rel = out.resolve().relative_to(REPO_ROOT).as_posix() + "/"
    untracked = [p for p in untracked if not p.startswith(out_rel)]
    with tarfile.open(out / "untracked_sources.tar.gz", "w:gz") as tar, \
            open(out / "untracked_sources.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["path", "bytes", "sha256"])
        for rel in sorted(untracked):
            p = REPO_ROOT / rel
            tar.add(p, arcname=rel)
            w.writerow([rel, p.stat().st_size, sha256(p)])
    print(f"git: patch {len(patch):,} bytes, untracked {len(untracked)} files")


def save_environment(out: Path) -> None:
    import psutil

    freeze = subprocess.run([sys.executable, "-m", "pip", "freeze"], capture_output=True, text=True, check=True).stdout
    (out / "pip_freeze.txt").write_text(freeze, encoding="utf-8")
    vm = psutil.virtual_memory()
    disk = psutil.disk_usage(str(REPO_ROOT))
    env = {
        "python": sys.version,
        "executable": sys.executable,
        "platform": platform.platform(),
        "processor": platform.processor(),
        "cpu_count_logical": psutil.cpu_count(),
        "ram_total_gib": round(vm.total / 1024**3, 1),
        "ram_available_gib": round(vm.available / 1024**3, 1),
        "disk_total_gib": round(disk.total / 1024**3, 1),
        "disk_free_gib": round(disk.free / 1024**3, 1),
        "key_packages": {
            line.split("==")[0]: line.split("==")[1]
            for line in freeze.splitlines()
            if "==" in line and line.split("==")[0].lower()
            in {"numpy", "pandas", "lightgbm", "scikit-learn", "optuna", "pyarrow", "scipy", "dill", "matplotlib"}
        },
    }
    (out / "environment.json").write_text(json.dumps(env, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"environment: python {platform.python_version()}, free disk {env['disk_free_gib']} GiB")


def profile_pickle(path: Path) -> dict:
    """DataFrame の pickle なら行数・列数・インデックスのユニーク数・日付範囲を返す。"""
    import pandas as pd

    try:
        obj = pd.read_pickle(path)
    except Exception:
        with open(path, "rb") as f:
            obj = pickle.load(f)
    info: dict = {"type": type(obj).__name__}
    if isinstance(obj, pd.DataFrame):
        info["rows"] = len(obj)
        info["cols"] = obj.shape[1]
        info["index_name"] = obj.index.name
        info["index_nunique"] = int(obj.index.nunique())
        for col in ("date", "日付"):
            if col in obj.columns:
                d = pd.to_datetime(obj[col], errors="coerce")
                info["date_min"], info["date_max"] = str(d.min().date()), str(d.max().date())
                break
    elif isinstance(obj, dict):
        info["keys"] = len(obj)
    return info


def save_data_manifest(out: Path, profile: bool) -> None:
    import pandas as pd

    paths = sorted({p for g in MANIFEST_GLOBS for p in REPO_ROOT.glob(g) if p.is_file() and p.name != ".gitkeep"})
    rows = []
    for p in paths:
        rel = p.relative_to(REPO_ROOT).as_posix()
        st = p.stat()
        row = {
            "path": rel,
            "role": role_of(rel),
            "bytes": st.st_size,
            "mtime": dt.datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds"),
            "sha256": sha256(p),
        }
        if profile and p.suffix in {".pickle", ".pkl"} and st.st_size <= PROFILE_MAX_BYTES and row["role"] != "model":
            try:
                row.update(profile_pickle(p))
            except Exception as e:  # 読めないものは理由だけ残す
                row["profile_error"] = repr(e)[:200]
        elif profile and p.suffix == ".csv" and st.st_size <= PROFILE_MAX_BYTES:
            try:
                row["rows"] = sum(1 for _ in open(p, "rb")) - 1
            except Exception as e:
                row["profile_error"] = repr(e)[:200]
        rows.append(row)
        print(f"  {rel} {st.st_size:,} bytes", flush=True)

    # data/html は件数とサイズだけ
    for sub in sorted((REPO_ROOT / "data/html").glob("*")):
        if sub.is_dir():
            n = total = 0
            for f in sub.rglob("*"):
                if f.is_file():
                    n += 1
                    total += f.stat().st_size
            rows.append({"path": sub.relative_to(REPO_ROOT).as_posix() + "/", "role": "html_cache",
                         "bytes": total, "files": n})

    pd.DataFrame(rows).to_csv(out / "data_manifest.csv", index=False, encoding="utf-8")
    print(f"data manifest: {len(rows)} entries")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", required=True, help="出力ディレクトリ（リポジトリ内）")
    parser.add_argument("--no-profile", action="store_true", help="pickle の中身を読まない（ハッシュのみ）")
    parser.add_argument("--skip-data", action="store_true", help="データ台帳を作らない")
    args = parser.parse_args()

    out = (REPO_ROOT / args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    save_git_state(out)
    save_environment(out)
    if not args.skip_data:
        save_data_manifest(out, profile=not args.no_profile)


if __name__ == "__main__":
    main()
