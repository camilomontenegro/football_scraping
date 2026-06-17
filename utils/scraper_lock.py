"""
utils/scraper_lock.py
=====================
Bloqueo de proceso para evitar varios scrapers Selenium en paralelo.
"""
from __future__ import annotations

import atexit
import os
import sys
from pathlib import Path

LOCK_PATH = Path(__file__).resolve().parent.parent / "data" / ".scraper.lock"
_acquired = False


def _pid_alive(pid: int) -> bool:
    if sys.platform == "win32":
        import ctypes

        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if handle:
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _read_lock() -> tuple[int | None, str]:
    try:
        lines = LOCK_PATH.read_text(encoding="utf-8").splitlines()
        if not lines:
            return None, ""
        pid = int(lines[0])
        cmd = lines[1] if len(lines) > 1 else ""
        return pid, cmd
    except (OSError, ValueError):
        return None, ""


def acquire_scraper_lock(name: str) -> None:
    """Sale con código 1 si otro scraper sigue activo."""
    global _acquired
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)

    if LOCK_PATH.exists():
        pid, cmd = _read_lock()
        if pid is not None and pid != os.getpid() and _pid_alive(pid):
            print(
                f"[!] Ya hay un scraper en ejecución (PID {pid}): {cmd or name}\n"
                "    Espera a que termine antes de lanzar otro.\n"
                "    Parar: Get-Process -Id {pid} | Stop-Process -Force".format(pid=pid),
                file=sys.stderr,
            )
            sys.exit(1)
        try:
            LOCK_PATH.unlink()
        except OSError:
            pass

    try:
        fd = os.open(LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, f"{os.getpid()}\n{name}\n".encode("utf-8"))
        os.close(fd)
    except FileExistsError:
        pid, cmd = _read_lock()
        print(
            f"[!] Otro scraper acaba de arrancar (PID {pid}): {cmd or '?'}\n"
            "    Reintenta en unos segundos.",
            file=sys.stderr,
        )
        sys.exit(1)

    _acquired = True
    atexit.register(release_scraper_lock)


def release_scraper_lock() -> None:
    global _acquired
    if not _acquired or not LOCK_PATH.exists():
        return
    pid, _ = _read_lock()
    if pid == os.getpid():
        try:
            LOCK_PATH.unlink()
        except OSError:
            pass
    _acquired = False
