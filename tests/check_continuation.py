"""Exercise continued table-in-out output and its refusals against the C worker.

blended_paged answers one input with several replies: each but the last says
vgi.status: HAVE_MORE_OUTPUT, and a zero-row tick carrying vgi.status: CONTINUE
asks for the next. A tick with nothing pending, a tick with rows, and real input
while output is pending each fail the stream, and the connection stays usable.
"""

from __future__ import annotations

import argparse
import base64
import struct
from pathlib import Path

import pyarrow as pa
from check_rpc import invoke, payload, request
from vgi.protocol import Arguments, BindRequest, FunctionType, InitRequest

SCHEMA = pa.schema([("n", pa.int64())])
TICK = {"vgi.status": "CONTINUE"}


def batch(counts):
    return pa.RecordBatch.from_pylist([{"n": n} for n in counts], schema=SCHEMA)


def exchange(worker, init, inputs):
    """Send init plus (batch, metadata) inputs; return the replies and a reuse probe."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, SCHEMA) as writer:
        for data, metadata in inputs:
            writer.write_batch(data, custom_metadata=metadata)
    wire = (
        request("init", {"request": init.serialize_to_bytes()})
        + sink.getvalue().to_pybytes()
        + request("catalog_catalogs")
    )
    replies = invoke(worker, wire)
    assert len(replies) == 3, replies
    assert payload(replies[2]).column("items")[0].as_py(), "connection was not reusable"
    return [(data, dict(metadata or {})) for data, metadata in replies[1]]


def parents(metadata):
    raw = base64.b64decode(metadata.get(b"vgi_rpc.parent_row#b64", b""))
    return list(struct.unpack(f"<{len(raw) // 4}i", raw))


def refused(replies, text):
    _, metadata = replies[-1]
    message = metadata.get(b"vgi_rpc.log_message", b"").decode()
    assert text in message, (text, message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", type=Path, required=True)
    args = parser.parse_args()
    bind = BindRequest(
        function_name="blended_paged",
        function_type=FunctionType.TABLE,
        arguments=Arguments(positional=(), named={"page_size": pa.scalar(2)}),
        input_schema=SCHEMA,
    )
    bound = payload(
        invoke(args.worker, request("bind", {"request": bind.serialize_to_bytes()}))[0]
    ).to_pylist()[0]
    init = InitRequest(
        bind_call=bind,
        output_schema=pa.ipc.read_schema(pa.BufferReader(bound["output_schema"])),
        bind_opaque_data=bound.get("opaque_data"),
    )
    empty = batch([])

    # Rows 0..n-1 per input row, two per reply, then a second input.
    replies = exchange(
        args.worker,
        init,
        [(batch([3, 0, 2]), None), (empty, TICK), (empty, TICK), (batch([1]), None)],
    )
    pages = [
        (data.column("i").to_pylist(), parents(m), m.get(b"vgi.status")) for data, m in replies
    ]
    assert pages == [
        ([0, 1], [0, 0], b"HAVE_MORE_OUTPUT"),
        ([2, 0], [0, 2], b"HAVE_MORE_OUTPUT"),
        ([1], [2], None),
        ([0], [0], None),
    ], pages

    # The protocol is strict: no capability negotiation, no lenient fallbacks.
    refused(
        exchange(args.worker, init, [(empty, TICK)]),
        "continuation tick received, but no output is pending",
    )
    refused(
        exchange(args.worker, init, [(batch([3]), None), (batch([1]), None)]),
        "input received while output is pending; expected a continuation tick",
    )
    refused(
        exchange(args.worker, init, [(batch([3]), None), (batch([1]), TICK)]),
        "a continuation tick must have no rows",
    )
    print("Passed: continued output, ticks, refusals, and connection reuse")


if __name__ == "__main__":
    main()
