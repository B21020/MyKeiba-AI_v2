"""コマンドを子プロセスで実行し、指定した時間帯だけ一時停止（suspend）する。

鮮度計測（tools/odds_freshness_probe.py）の取得時刻に、重い処理がPCを占有しないようにするため。

使い方:
  .venv/Scripts/python.exe tools/run_paused_during.py --pause 12:33-13:05 --pause 14:18-15:45 -- \
      .venv/Scripts/python.exe tools/build_features.py --tag 20261024_stage1
"""
from __future__ import annotations

import argparse
from datetime import datetime, time as dtime
from pathlib import Path
import subprocess
import sys
import time

import psutil


def parse_window(s: str) -> tuple[dtime, dtime]:
    a, b = s.split('-')
    return dtime.fromisoformat(a), dtime.fromisoformat(b)


def in_window(now: dtime, windows) -> bool:
    return any(a <= now < b for a, b in windows)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--pause', action='append', default=[], help='HH:MM-HH:MM (local time)')
    ap.add_argument('cmd', nargs=argparse.REMAINDER)
    args = ap.parse_args()
    cmd = args.cmd[1:] if args.cmd and args.cmd[0] == '--' else args.cmd
    windows = [parse_window(w) for w in args.pause]
    # Windows の CreateProcess は相対パスの実行ファイルを解決しないので絶対パスにする
    exe = Path(cmd[0])
    if exe.exists():
        cmd[0] = str(exe.resolve())
    proc = subprocess.Popen(cmd)
    ps = psutil.Process(proc.pid)
    paused = False
    while proc.poll() is None:
        should = in_window(datetime.now().time(), windows)
        try:
            if should and not paused:
                for p in [ps, *ps.children(recursive=True)]:
                    p.suspend()
                paused = True
                print(f'[{datetime.now():%H:%M:%S}] paused', flush=True)
            elif not should and paused:
                for p in [ps, *ps.children(recursive=True)]:
                    p.resume()
                paused = False
                print(f'[{datetime.now():%H:%M:%S}] resumed', flush=True)
        except psutil.NoSuchProcess:
            pass
        time.sleep(5)
    sys.exit(proc.returncode)


if __name__ == '__main__':
    main()
