"""Backwards-compatible shim — re-exports the wizard.wizard surface.

The canonical definition lives in `wizard/wizard.py`. Running
`python -m scripts.wizard` continues to work by delegating to the canonical
CLI entry point with log capture enabled.
"""
from wizard.wizard import *  # noqa: F401,F403
from wizard.wizard import main as _main, run_with_log_capture as _run_with_log_capture


if __name__ == "__main__":
    _run_with_log_capture(_main)
