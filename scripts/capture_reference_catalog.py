"""Capture declarative fixture metadata from the C++ worker over its public RPC API.

The captured Arrow file is build-time fixture data, never a runtime dependency
on the C++ worker. Function implementations live in the C example worker.
Run using the vgi-python development environment.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
import pyarrow as pa
from check_rpc import encode, invoke, payload, request
from vgi.protocol import CatalogAttachRequest
from vgi_rpc.rpc._reflection import Reflection


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("worker", type=Path)
    parser.add_argument(
        "--output", type=Path, default=ROOT / "example-worker/generated/fixtures.arrow"
    )
    args = parser.parse_args()
    binary = args.worker.resolve()

    def rpc(method, values=None, **kwargs):
        return payload(
            invoke(binary, request(method, values, **kwargs))[0]
        ).to_pylist()[0]

    description = rpc("describe", {"protocol": "vgi.v2"}, protocol=Reflection)
    version = description["protocol_version"]
    rows = []
    schema = None
    for catalog_name in (
        "example",
        "twin_a",
        "twin_b",
        "contents_probe",
        "contents_broken",
        "contents_legacy",
        "projection_repro",
        "narrow_bind",
        "accumulate",
        "versioned_tables",
        "attach_options",
        "ticket_probe",
    ):
        catalog = rpc(
            "catalog_attach",
            {
                "request": CatalogAttachRequest(
                    name=catalog_name,
                    options=encode(
                        pa.RecordBatch.from_pylist([{"api_key": "capture-placeholder"}])
                    )
                    if catalog_name == "ticket_probe"
                    else None,
                    data_version_spec=None,
                    implementation_version=None,
                ).serialize_to_bytes()
            },
            version=version,
        )
        cookie = catalog["attach_opaque_data"]
        schemas = rpc(
            "catalog_schemas", {"attach_opaque_data": cookie}, version=version
        )["items"]
        for encoded in schemas:
            path = pa.ipc.open_stream(encoded).read_all().to_pylist()[0]["path"]
            for kind in ("SCALAR", "TABLE", "TABLE_BUFFERING", "AGGREGATE"):
                functions = rpc(
                    "catalog_schema_contents_functions",
                    {"attach_opaque_data": cookie, "path": path, "type": kind},
                    version=version,
                )["items"]
                for item in functions:
                    table = pa.ipc.open_stream(item).read_all()
                    schema = table.schema.append(
                        pa.field("fixture_catalog", pa.string(), nullable=False)
                    )
                    rows.extend(
                        dict(row, fixture_catalog=catalog_name)
                        for row in table.to_pylist()
                    )
    rows.sort(
        key=lambda row: (
            row["fixture_catalog"],
            row["schema_path"],
            row["name"],
            row["arguments"],
        )
    )
    batch = pa.RecordBatch.from_pylist(rows, schema=schema)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(encode(batch))
    manifest = {
        "protocol_version": version,
        "protocol_hash": description["protocol_hash"],
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "function_count": len(rows),
        "functions": [".".join([*row["schema_path"], row["name"]]) for row in rows],
    }
    args.output.with_suffix(".json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Captured {len(rows)} function signatures from protocol {version}")


if __name__ == "__main__":
    main()
