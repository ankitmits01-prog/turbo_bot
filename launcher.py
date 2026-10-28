"""
Tiny supervisor for tgbot.py (never auto-updated, so it stays simple and safe).

  * Starts tgbot.py and starts it again whenever it exits (that is what /restart and /update use).
  * If tgbot.py keeps crashing right after starting (3 times in a row, e.g. after a bad update),
    it puts back the files saved by the last /update and tries again - so you never need a terminal.
"""
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
FAST = float(os.getenv("LAUNCH_FAST_SECS", "25"))        # a run shorter than this counts as a crash
LIMIT = int(os.getenv("LAUNCH_CRASH_LIMIT", "3"))
PAUSE = float(os.getenv("LAUNCH_PAUSE", "3"))


def restore_latest_backup() -> bool:
    base = HERE / "backup_code"
    dirs = sorted(d for d in base.glob("*") if d.is_dir() and not d.name.endswith(".used")) if base.exists() else []
    if not dirs:
        return False
    last = dirs[-1]
    for f in last.glob("*.py"):
        shutil.copy2(f, HERE / f.name)
    last.rename(last.with_name(last.name + ".used"))
    print(f"[launcher] crash loop: restored backup {last.name}", flush=True)
    return True


def main():
    fails = 0
    while True:
        t0 = time.monotonic()
        rc = subprocess.call([sys.executable, str(HERE / "tgbot.py")], cwd=str(HERE))
        ran = time.monotonic() - t0
        fails = fails + 1 if ran < FAST else 0
        print(f"[launcher] tgbot exited (code {rc}) after {ran:.0f}s", flush=True)
        if fails >= LIMIT:
            if restore_latest_backup():
                fails = 0
            else:
                print("[launcher] crash loop and no backup to restore; waiting 30s", flush=True)
                time.sleep(30)
        time.sleep(PAUSE)


if __name__ == "__main__":
    main()
