from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def test_claude_span_batching_import_does_not_mask_helper_errors(tmp_path):
    package_dir = tmp_path / "sii_opik_plugin"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "span_batching.py").write_text(
        "raise RuntimeError('broken span batching helper')\n",
        encoding="utf-8",
    )
    (tmp_path / "span_batching.py").write_text(
        "\n".join(
            [
                "def flush_span_batch(*args, **kwargs): return 'stub'",
                "def queue_span_snapshot(*args, **kwargs): return False",
                "def span_batch_env_names(): return ('OPIK_SPAN_BATCH_ENABLED',)",
                "def update_queued_span(*args, **kwargs): return False",
                "",
            ]
        ),
        encoding="utf-8",
    )

    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(tmp_path)

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import runpy; runpy.run_path('src/sii_opik_plugin/claude_code/claude_realtime_trace.py')",
        ],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "broken span batching helper" in result.stderr
