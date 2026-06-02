#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Shanghai Innovation Institute
"""Export Opik traces to per-trace flat-span JSON files.

Pulls traces (and all their spans) out of an Opik project and writes one JSON
file per trace, shaped as a flat span list ``[root_trace, span1, span2, ...]``.
This is the inverse of the realtime tracer and is byte-compatible with the
downstream converters (``opik_trace_to_codetracer.py``,
``convert_trace_to_session_parquet.py``), which read exactly this format.

Usage:
  python -m sii_opik_plugin.export.export_opik_traces \\
    --out-dir ./export \\
    [--project NAME] [--opik-url URL] [--workspace NAME] [--api-key KEY] \\
    [--filter 'tags contains "tb-task"'] \\
    [--max-results 1000] [--max-spans 5000] [--overwrite]

Target resolution:
  - URL:     --opik-url  > $OPIK_URL          > error
  - project: --project   > $OPIK_PROJECT_NAME > error
  - workspace / api-key are optional (fall back to ~/.opik.config defaults).
"""

from __future__ import annotations

import argparse
import sys


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="export_opik_traces",
        description="Export Opik traces to per-trace flat-span JSON files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--out-dir",
        required=True,
        help="Directory to write <trace_id>.json files and manifest.json into.",
    )
    parser.add_argument(
        "--project",
        default=None,
        help="Opik project to export. Falls back to $OPIK_PROJECT_NAME; required.",
    )
    parser.add_argument(
        "--opik-url",
        default=None,
        help="Opik server URL (maps to host). Falls back to $OPIK_URL; required.",
    )
    parser.add_argument(
        "--workspace",
        default=None,
        help="Opik workspace. Optional; defaults to ~/.opik.config / 'default'.",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="Opik API key. Optional; needed only for cloud / authed servers.",
    )
    parser.add_argument(
        "--filter",
        default=None,
        dest="filter_string",
        help="Optional OQL filter, e.g. 'tags contains \"tb-task\"'.",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=1000,
        help="Maximum number of traces to export (default: 1000).",
    )
    parser.add_argument(
        "--max-spans",
        type=int,
        default=5000,
        help="Maximum number of spans to fetch per trace (default: 5000).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-export traces whose JSON file already exists.",
    )
    return parser


def run(args: argparse.Namespace) -> int:
    """Execute the export. Filled in across subsequent steps."""
    raise NotImplementedError


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
