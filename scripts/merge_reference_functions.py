"""Add reference function descriptors to the example worker's captured fixtures.

Fetches the named table functions from a reference worker over the public RPC API
and inserts them into ``fixtures.arrow`` and every captured catalog that lists
``--after``. It also brings the captured descriptors to the current records: every
descriptor is re-encoded with its current record schema, and enum fields carry
the member names the wire uses. Execution of the
added functions must be implemented in C; startup rejects one that is not.
Run using the vgi-python development environment.
"""

from __future__ import annotations

import argparse
import dataclasses
import enum
import hashlib
import json
import sys
import types
import typing
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
import pyarrow as pa
from check_rpc import encode, invoke, payload, request
from vgi.catalog.catalog_interface import (
    FunctionInfo,
    IndexInfo,
    MacroInfo,
    SchemaInfo,
    TableInfo,
    ViewInfo,
)
from vgi.protocol import CatalogAttachRequest, VgiProtocol
from vgi_rpc.rpc._reflection import Reflection

GENERATED = ROOT / "example-worker/generated"
RECORDS = {
    "tables": TableInfo,
    "views": ViewInfo,
    "indexes": IndexInfo,
    "scalar_functions": FunctionInfo,
    "aggregate_functions": FunctionInfo,
    "table_functions": FunctionInfo,
    "scalar_macros": MacroInfo,
    "table_macros": MacroInfo,
}


def enum_fields(record) -> dict[str, type[enum.Enum]]:
    """The record's enum-typed fields, which the wire carries as member names."""
    hints = typing.get_type_hints(record, include_extras=True)
    out = {}
    for item in dataclasses.fields(record):
        kind = hints[item.name]
        while typing.get_origin(kind) is typing.Annotated or (
            typing.get_origin(kind) in (typing.Union, types.UnionType)
            and len([a for a in typing.get_args(kind) if a is not type(None)]) == 1
        ):
            kind = next(a for a in typing.get_args(kind) if a is not type(None))
        if isinstance(kind, type) and issubclass(kind, enum.Enum):
            out[item.name] = kind
    return out


def current_rows(record, table: pa.Table) -> pa.RecordBatch:
    """Re-encode rows as the current record schema, matching fields by name."""
    rows = table.to_pylist()
    for row in rows:
        normalize(record, row)
    return pa.RecordBatch.from_pylist(rows, schema=record.ARROW_SCHEMA)


def normalize(record, row: dict) -> None:
    for name, kind in enum_fields(record).items():
        value = row.get(name)
        if value is not None and value not in kind.__members__:
            row[name] = kind(value).name


def current_item(record, item: bytes) -> bytes:
    return encode(current_rows(record, pa.ipc.open_stream(item).read_all()))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("worker", type=Path, help="reference worker, e.g. vgi-fixture-worker")
    parser.add_argument("names", nargs="*", help="table functions to add, in order")
    parser.add_argument("--after", default="blended_explode")
    args = parser.parse_args()

    def rpc(method, values=None, **kwargs):
        return payload(invoke(args.worker, request(method, values, **kwargs))[0])

    found = {}
    attach = None if not args.names else rpc(
        "catalog_attach",
        {
            "request": CatalogAttachRequest(
                name="example", options=None, data_version_spec=None, implementation_version=None
            ).serialize_to_bytes()
        },
    ).to_pylist()[0]
    listed = [] if attach is None else rpc(
        "catalog_schema_contents_functions",
        {"attach_opaque_data": attach["attach_opaque_data"], "path": ["main"], "type": "TABLE_FUNCTION"},
    ).to_pylist()[0]["items"]
    for item in listed:
        table = pa.ipc.open_stream(item).read_all()
        name = table.column("name")[0].as_py()
        if name in args.names:
            found[name] = current_item(FunctionInfo, item)
    missing = [name for name in args.names if name not in found]
    if missing:
        raise SystemExit(f"reference worker does not list {missing}")
    added = [found[name] for name in args.names]

    # Signatures the worker registers.
    fixtures_path = GENERATED / "fixtures.arrow"
    fixtures = pa.ipc.open_stream(fixtures_path.read_bytes()).read_all()
    schema = FunctionInfo.ARROW_SCHEMA.append(pa.field("fixture_catalog", pa.string(), nullable=False))
    rows = [row for row in fixtures.to_pylist() if row["name"] not in args.names]
    for item in added:
        rows.extend(dict(row, fixture_catalog="example") for row in pa.ipc.open_stream(item).read_all().to_pylist())
    for row in rows:
        normalize(FunctionInfo, row)
    rows.sort(key=lambda row: (row["fixture_catalog"], row["schema_path"], row["name"], row["arguments"]))
    fixtures_path.write_bytes(encode(pa.RecordBatch.from_pylist(rows, schema=schema)))
    manifest_path = fixtures_path.with_suffix(".json")
    manifest = json.loads(manifest_path.read_text())
    description = payload(
        invoke(args.worker, request("describe", {"protocol": "vgi.v2"}, protocol=Reflection))[0]
    ).to_pylist()[0]
    manifest.update(
        protocol_version=VgiProtocol.protocol_version,
        protocol_hash=description["protocol_hash"],
        function_count=len(rows),
        functions=[".".join([*row["schema_path"], row["name"]]) for row in rows],
    )
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    # Catalog contents the worker serves.
    catalogs_path = GENERATED / "catalogs.arrow"
    catalogs = pa.ipc.open_stream(catalogs_path.read_bytes()).read_all()
    out = []
    for row in catalogs.to_pylist():
        contents_table = pa.ipc.open_stream(row["contents"]).read_all()
        contents = contents_table.to_pylist()[0]
        for entry in contents["schemas"]:
            entry["schema"] = current_item(SchemaInfo, entry["schema"])
            for kind, record in RECORDS.items():
                items = []
                for item in entry[kind]:
                    name = pa.ipc.open_stream(item).read_all().column("name")[0].as_py()
                    if record is FunctionInfo and name in args.names:
                        continue
                    items.append(current_item(record, item))
                    if kind == "table_functions" and name == args.after:
                        items.extend(added)
                entry[kind] = items
        row["contents"] = encode(pa.RecordBatch.from_pylist([contents], schema=contents_table.schema))
        attach_table = pa.ipc.open_stream(row["attach"]).read_all()
        attach = attach_table.to_pylist()[0]
        attach["global_functions"] = [current_item(FunctionInfo, item) for item in attach["global_functions"]]
        row["attach"] = encode(pa.RecordBatch.from_pylist([attach], schema=attach_table.schema))
        out.append(row)
    catalogs_path.write_bytes(encode(pa.RecordBatch.from_pylist(out, schema=catalogs.schema)))
    print(f"Added {', '.join(args.names)}; {len(rows)} signatures; "
          f"catalogs.arrow sha256 {hashlib.sha256(catalogs_path.read_bytes()).hexdigest()[:12]}")


if __name__ == "__main__":
    main()
