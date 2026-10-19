#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VGI_PYTHON="${VGI_PYTHON:-$HOME/Development/vgi-python}"
cd "$VGI_PYTHON"
exec uv run --project . python "$ROOT/scripts/generate_protocol.py" --output-dir "$ROOT" "$@"
