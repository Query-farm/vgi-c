"""Wire tests against the standalone C worker (Python is only the test peer)."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, os.environ.get("VGI_PYTHON", str(ROOT.parent / "vgi-python")))
from vgi.codegen._registry import registry_methods
from vgi.protocol import VgiProtocol
from vgi_rpc.rpc._reflection import Reflection


def encode(batch, metadata=None):
    output = pa.BufferOutputStream()
    with pa.ipc.new_stream(output, batch.schema) as writer:
        writer.write_batch(batch, custom_metadata=metadata)
    return output.getvalue().to_pybytes()


def request(method, values=None, *, protocol=VgiProtocol, version=None, schema=None):
    methods = {m.name: m for m in registry_methods(protocol)}
    if schema is None:
        schema = pa.schema([p.field for p in methods[method].params])
    batch = pa.RecordBatch.from_pylist([values or {}], schema=schema)
    metadata = {
        "vgi_rpc.method": method,
        "vgi_rpc.protocol": protocol.protocol_name,
        "vgi_rpc.request_version": "1",
        "vgi_rpc.request_id": "c-test-request",
    }
    declared = (
        version if version is not None else getattr(protocol, "protocol_version", "")
    )
    if declared:
        metadata["vgi_rpc.protocol_version"] = declared
    return encode(batch, metadata)


def invoke(binary, data):
    process = subprocess.run(
        [str(binary)], input=data, capture_output=True, check=False, timeout=20
    )
    assert process.returncode == 0, process.stderr.decode(errors="replace")
    reader = pa.BufferReader(process.stdout)
    replies = []
    while reader.tell() < len(process.stdout):
        with pa.ipc.open_stream(reader) as stream:
            replies.append(list(stream.iter_batches_with_custom_metadata()))
    return replies


def payload(reply):
    batch, metadata = reply[0]
    if metadata and b"vgi_rpc.log_message" in metadata:
        raise AssertionError(dict(metadata.items()))
    return pa.ipc.open_stream(batch.column("result")[0].as_py()).read_all()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--worker", type=Path, default=ROOT / "build/example-worker/vgi-example-worker"
    )
    args = parser.parse_args()
    requests = [
        request("list_protocols", protocol=Reflection),
        request("describe", {"protocol": "vgi.v2"}, protocol=Reflection),
        request("catalog_catalogs", version="99.0.0"),
        request("missing", schema=pa.schema([])),
        request(
            "describe",
            {"protocol": 42},
            protocol=Reflection,
            schema=pa.schema([("protocol", pa.int64())]),
        ),
        request("catalog_catalogs"),
    ]
    replies = invoke(args.worker, b"".join(requests))
    assert len(replies) == len(requests)
    listing = payload(replies[0]).to_pylist()[0]
    assert {p["protocol"] for p in listing["protocols"]} == {
        VgiProtocol.protocol_name,
        Reflection.protocol_name,
        "conformance.Secondary.v1",
    }
    description = payload(replies[1]).to_pylist()[0]
    methods = {m.name: m for m in registry_methods(VgiProtocol)}
    assert description["protocol_version"] == VgiProtocol.protocol_version
    assert {m["name"] for m in description["methods"]} == methods.keys()
    for actual in description["methods"]:
        expected = methods[actual["name"]]
        assert pa.ipc.read_schema(pa.BufferReader(actual["params_schema_ipc"])).equals(
            pa.schema([p.field for p in expected.params]), check_metadata=True
        ), actual["name"]
    for reply, expected in zip(
        replies[2:5], ["ProtocolVersionError", "MethodNotImplementedError", "TypeError"]
    ):
        batch, metadata = reply[0]
        assert batch.num_rows == 0
        details = json.loads(metadata[b"vgi_rpc.log_extra"])
        assert details["exception_type"] == expected, details
        assert metadata[b"vgi_rpc.request_id"] == b"c-test-request"
    assert len(payload(replies[-1]).column("items")[0].as_py()) >= 1
    from vgi.protocol import CatalogAttachRequest

    for catalog in ("example", "an_arbitrary_attach_alias"):
        attach = payload(
            invoke(
                args.worker,
                request(
                    "catalog_attach",
                    {
                        "request": CatalogAttachRequest(
                            name=catalog,
                            data_version_spec=None,
                            implementation_version=None,
                        ).serialize_to_bytes()
                    },
                ),
            )[0]
        ).to_pylist()[0]
        assert attach["attach_opaque_data"]
        contents = payload(
            invoke(
                args.worker,
                request(
                    "catalog_contents",
                    {"attach_opaque_data": attach["attach_opaque_data"]},
                ),
            )[0]
        ).to_pylist()[0]
        assert contents["schemas"]
    print(
        f"Passed: {len(methods)} reflection signatures, version and dispatch errors, connection reuse"
    )


if __name__ == "__main__":
    main()
