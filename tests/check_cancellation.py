"""Exercise producer/exchange cancellation and subsequent connection reuse."""

from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow as pa
from check_rpc import invoke, payload, request
from vgi.protocol import Arguments, BindRequest, FunctionType, InitRequest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", type=Path, required=True)
    args = parser.parse_args()
    for exchange in (False, True):
        name = "slow_cancellable_inout" if exchange else "slow_cancellable"
        input_schema = pa.schema([("v", pa.int64())]) if exchange else pa.schema([])
        bind = BindRequest(
            function_name=name,
            function_type=FunctionType.TABLE,
            arguments=Arguments(
                positional=(pa.scalar("/tmp/unused-cancel-probe"),),
                named={"sleep_ms": pa.scalar(0), "count": pa.scalar(3)}
                if not exchange
                else {"sleep_ms": pa.scalar(0)},
            ),
            input_schema=input_schema if exchange else None,
        )
        bound = payload(
            invoke(
                args.worker, request("bind", {"request": bind.serialize_to_bytes()})
            )[0]
        ).to_pylist()[0]
        output_schema = pa.ipc.read_schema(pa.BufferReader(bound["output_schema"]))
        init = InitRequest(
            bind_call=bind,
            output_schema=output_schema,
            bind_opaque_data=bound.get("opaque_data"),
        )
        for cancel in (False, True):
            sink = pa.BufferOutputStream()
            tick = (
                pa.RecordBatch.from_pylist([{"v": 42}], schema=input_schema)
                if exchange
                else pa.record_batch([], schema=input_schema)
            )
            with pa.ipc.new_stream(sink, input_schema) as writer:
                writer.write_batch(tick)
                if cancel:
                    writer.write_batch(tick, custom_metadata={"vgi_rpc.cancel": "true"})
                else:
                    for _ in range(3):
                        writer.write_batch(tick)
            wire = (
                request("init", {"request": init.serialize_to_bytes()})
                + sink.getvalue().to_pybytes()
                + request("catalog_catalogs")
            )
            replies = invoke(args.worker, wire)
            assert len(replies) == 3, replies
            data = pa.Table.from_batches([batch for batch, _ in replies[1]])
            expected = (
                ([42] if exchange else [0])
                if cancel
                else ([42] * 4 if exchange else [0, 1, 2])
            )
            assert data.column(0).to_pylist() == expected
            assert payload(replies[2]).column("items")[0].as_py()
    print("Passed: producer/exchange completion, cancellation, and connection reuse")


if __name__ == "__main__":
    main()
