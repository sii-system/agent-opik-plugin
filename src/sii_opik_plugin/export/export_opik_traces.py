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
import json
import os
import sys
from pathlib import Path
from typing import Any


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


def _resolve_required(flag_value: str | None, env_var: str, flag_name: str) -> str:
    """Resolve a required setting: explicit flag > env var > hard error."""
    value = flag_value or os.environ.get(env_var)
    if not value:
        raise SystemExit(
            f"error: no value for {flag_name}. "
            f"Pass {flag_name} or set ${env_var}."
        )
    return value


def build_client(args: argparse.Namespace) -> Any:
    """Resolve the target and construct an Opik client.

    URL and project are mandatory (flag > env > error); workspace and api-key
    are optional and fall back to the SDK's own config resolution.
    """
    try:
        from opik import Opik
    except ImportError as exc:  # pragma: no cover - depends on install
        raise SystemExit(
            "error: the 'opik' package is not installed. "
            "Install it with: pip install opik"
        ) from exc

    url = _resolve_required(args.opik_url, "OPIK_URL", "--opik-url")
    project = _resolve_required(args.project, "OPIK_PROJECT_NAME", "--project")

    client = Opik(
        host=url,
        project_name=project,
        workspace=args.workspace,
        api_key=args.api_key,
    )

    cfg = client.config
    print(
        "[export] target resolved: "
        f"url={cfg.url_override} workspace={cfg.workspace} "
        f"project={cfg.project_name}",
        file=sys.stderr,
    )
    return client


def _to_jsonable(obj: Any) -> Any:
    """Convert an Opik pydantic model (TracePublic / SpanPublic) to a plain dict."""
    if hasattr(obj, "dict"):
        return obj.dict()
    return obj


def _span_sort_key(span: Any) -> str:
    """Sort spans chronologically; downstream converters expect llm order."""
    return str(getattr(span, "start_time", "") or "")


def _write_trace_file(out_dir: Path, trace: Any, spans: list[Any]) -> Path:
    """Write one trace as a flat span list: [root_trace, span1, span2, ...].

    The root (TracePublic) has no ``type`` field, so downstream converters pick
    it up as the conversation root; spans carry ``type`` (e.g. "llm").
    """
    flat = [_to_jsonable(trace)] + [_to_jsonable(s) for s in spans]
    path = out_dir / f"{trace.id}.json"
    with path.open("w", encoding="utf-8") as handle:
        json.dump(flat, handle, ensure_ascii=False, indent=2, default=str)
    return path


def run(args: argparse.Namespace) -> int:
    """Execute the export."""
    client = build_client(args)
    project = client.config.project_name
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    traces = client.search_traces(
        project_name=project,
        filter_string=args.filter_string,
        max_results=args.max_results,
        truncate=False,
    )
    print(f"[export] {len(traces)} trace(s) matched", file=sys.stderr)

    exported = skipped = 0
    for trace in traces:
        path = out_dir / f"{trace.id}.json"
        if path.exists() and not args.overwrite:
            skipped += 1
            continue
        spans = client.search_spans(
            project_name=project,
            trace_id=trace.id,
            max_results=args.max_spans,
            truncate=False,
        )
        spans.sort(key=_span_sort_key)
        _write_trace_file(out_dir, trace, spans)
        exported += 1
        print(f"[export] wrote {trace.id} ({len(spans)} spans)", file=sys.stderr)

    print(f"[export] done: exported={exported} skipped={skipped}", file=sys.stderr)
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
