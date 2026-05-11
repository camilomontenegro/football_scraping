"""Backwards-compatible shim — re-exports the wizard.pipeline_runner surface.

The canonical definition lives in `wizard/pipeline_runner.py`. Existing
documentation and tooling that invokes `python -m scripts.pipeline_runner`
continues to work by delegating to the canonical CLI.
"""
from wizard.pipeline_runner import *  # noqa: F401,F403
from wizard.pipeline_runner import main as _main


if __name__ == "__main__":
    _main()
