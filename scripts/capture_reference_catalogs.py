"""Capture declarative catalogs through public reference RPCs; no executable code."""

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
import pyarrow as pa
from check_rpc import encode, invoke, payload, request
from vgi.codegen.c_schemas import schemas
from vgi.protocol import CatalogAttachRequest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("worker", type=Path)
    parser.add_argument("--primary", default="example")
    parser.add_argument("--data-version")
    parser.add_argument("--all-configurations", action="store_true")
    parser.add_argument(
        "--output", type=Path, default=ROOT / "example-worker/generated/catalogs.arrow"
    )
    args = parser.parse_args()
    if args.all_configurations:
        configurations = [
            ("example", None),
            ("attach_options", None),
            ("versioned", None),
            ("versioned_tables", None),
            *[("versioned_tables", v) for v in ("1.0.0", "1.1.0", "2.0.0")],
        ]
        tables = []
        with tempfile.TemporaryDirectory() as directory:
            for index, (primary, version) in enumerate(configurations):
                output = Path(directory) / f"{index}.arrow"
                command = [
                    sys.executable,
                    __file__,
                    str(args.worker),
                    "--primary",
                    primary,
                    "--output",
                    str(output),
                ]
                if version:
                    command += ["--data-version", version]
                subprocess.run(command, check=True)
                tables.append(pa.ipc.open_stream(output).read_all())
        table = pa.concat_tables(tables).combine_chunks()
        args.output.write_bytes(encode(table.to_batches()[0]))
        args.output.with_suffix(".json").write_text(
            json.dumps(
                {
                    "configurations": configurations,
                    "catalogs": table.column("name").to_pylist(),
                },
                indent=2,
            )
            + "\n"
        )
        return
    os.environ["VGI_WORKER_CATALOG_NAME"] = args.primary

    def rpc(method, values=None):
        response = invoke(args.worker, request(method, values))[0]
        if response[0][1] and b"vgi_rpc.log_message" in response[0][1]:
            raise AssertionError("Reference rejected request")
        if response[0][0].column("result")[0].as_py() is None:
            raise AssertionError("No descriptor")
        return payload(response)

    def one(table):
        return table.to_pylist()[0]

    contents_schema = next(
        s.schema for s in schemas() if s.name == "CatalogContentsResult"
    )
    rows = []
    catalogs = one(rpc("catalog_catalogs"))["items"]
    for catalog in catalogs:
        name = one(pa.ipc.open_stream(catalog).read_all())["name"]
        if name in ("contents_hash", "contents_memory", "contents_reval"):
            continue
        options = (
            encode(pa.RecordBatch.from_pylist([{"api_key": "capture-placeholder"}]))
            if name in ("attach_options_required", "ticket_probe")
            else None
        )
        attach = rpc(
            "catalog_attach",
            {
                "request": CatalogAttachRequest(
                    name=name,
                    options=options,
                    data_version_spec=args.data_version,
                    implementation_version=None,
                ).serialize_to_bytes()
            },
        )
        cookie = one(attach)["attach_opaque_data"]
        entries = []
        extras = []
        for schema_bytes in one(rpc("catalog_schemas", {"attach_opaque_data": cookie}))[
            "items"
        ]:
            path = one(pa.ipc.open_stream(schema_bytes).read_all())["path"]
            entry = {"path": path, "schema": schema_bytes}
            for kind in (
                "tables",
                "views",
                "indexes",
                "scalar_macros",
                "table_macros",
                "scalar_functions",
                "aggregate_functions",
                "table_functions",
            ):
                method = "catalog_schema_contents_" + kind.split("_")[-1]
                values = {"attach_opaque_data": cookie, "path": path}
                if "_" in kind:
                    values["type"] = kind.split("_")[0].upper()
                entry[kind] = one(rpc(method, values))["items"]
            # Buffering registrations travel with the table surface.
            entry["table_functions"] += one(
                rpc(
                    "catalog_schema_contents_functions",
                    {
                        "attach_opaque_data": cookie,
                        "path": path,
                        "type": "TABLE_BUFFERING",
                    },
                )
            )["items"]
            for table in entry["tables"]:
                table_name = one(pa.ipc.open_stream(table).read_all())["name"]
                values = {
                    "attach_opaque_data": cookie,
                    "schema_path": path,
                    "name": table_name,
                }
                for method in (
                    "catalog_table_get",
                    "catalog_table_scan_function_get",
                    "catalog_table_scan_branches_get",
                    "catalog_table_column_statistics_get",
                ):
                    try:
                        value = rpc(method, values)
                    except AssertionError:
                        continue
                    extras.append(
                        {
                            "method": method,
                            "path": path,
                            "name": table_name,
                            "value": encode(value.to_batches()[0]),
                            "accepts_at": table_name
                            in ("tt_pushdown_fn", "cache_versioned")
                            or table_name.startswith("multi_branch_"),
                        }
                    )
                # Version snapshots are declarative responses, never execution results.
                version_count = (
                    {
                        "versioned_data": 3,
                        "versioned_constraints": 3,
                        "tt_pushdown_cols": 2,
                    }.get(table_name, 0)
                    if name == "example"
                    else 0
                )
                for version in range(1, version_count + 1):
                    for method in (
                        "catalog_table_get",
                        "catalog_table_scan_function_get",
                        "catalog_table_scan_branches_get",
                    ):
                        try:
                            value = rpc(
                                method,
                                dict(values, at_unit="VERSION", at_value=str(version)),
                            )
                        except AssertionError:
                            continue
                        extras.append(
                            {
                                "method": method,
                                "path": path,
                                "name": table_name,
                                "value": encode(value.to_batches()[0]),
                                "version": version,
                                "timestamp_year": (2000 if version == 1 else 2021)
                                if table_name == "tt_pushdown_cols"
                                else 2019 + version,
                            }
                        )
            entries.append(entry)
        content = pa.RecordBatch.from_pylist(
            [
                {
                    "catalog_version": one(attach)["catalog_version"],
                    "etag": None,
                    "not_modified": False,
                    "schemas": entries,
                }
            ],
            schema=contents_schema,
        )
        rows.append(
            {
                "name": name,
                "info": catalog,
                "attach": encode(attach.to_batches()[0]),
                "contents": encode(content),
                "extras": extras,
                "fixture_primary": args.primary,
                "data_version": args.data_version,
            }
        )
        print(name, len(entries), sum(len(e["tables"]) for e in entries), flush=True)
    extra_type = pa.list_(
        pa.struct(
            [
                ("method", pa.string()),
                ("path", pa.list_(pa.string())),
                ("name", pa.string()),
                ("value", pa.binary()),
                ("version", pa.int64()),
                ("timestamp_year", pa.int32()),
                ("accepts_at", pa.bool_()),
            ]
        )
    )
    result = pa.RecordBatch.from_pylist(
        rows,
        schema=pa.schema(
            [
                ("name", pa.string()),
                ("info", pa.binary()),
                ("attach", pa.binary()),
                ("contents", pa.binary()),
                ("extras", extra_type),
                ("fixture_primary", pa.string()),
                ("data_version", pa.string()),
            ]
        ),
    )
    output = args.output
    output.write_bytes(encode(result))
    output.with_suffix(".json").write_text(
        json.dumps({"catalogs": [r["name"] for r in rows]}, indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
