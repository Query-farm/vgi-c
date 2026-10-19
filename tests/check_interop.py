"""Check compiled C schemas and IPC against the canonical Python implementation."""

from __future__ import annotations

import argparse
import ctypes
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pyarrow as pa
from pyarrow.cffi import ffi


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, default=Path("build"))
    parser.add_argument(
        "--roundtrip", type=Path, help="Override IPC executable, e.g. a sanitizer build"
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, os.environ.get("VGI_PYTHON", str(root.parent / "vgi-python")))
    from vgi.codegen._registry import registry_methods
    sys.path.insert(0, str(root / "scripts"))
    from generate_protocol import schemas

    library_path = next(
        (
            args.build_dir / name
            for name in ("libvgi.dylib", "libvgi.so", "vgi.dll")
            if (args.build_dir / name).is_file()
        ),
        None,
    )
    if library_path is None:
        parser.error("Build with -DBUILD_SHARED_LIBS=ON to check the compiled schemas")
    library = ctypes.CDLL(str(library_path.resolve()))

    def import_schema(factory: object) -> pa.Schema:
        out = ffi.new("struct ArrowSchema*")
        address = int(ffi.cast("uintptr_t", out))
        error = ctypes.create_string_buffer(1024)
        code = factory(address, ctypes.addressof(error))
        assert code == 0, error.value
        return pa.Schema._import_from_c(address)

    schema_factory = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
    all_schemas = schemas()
    for item in all_schemas:
        factory = schema_factory((f"VgiSchema{item.name}", library))
        actual = import_schema(factory)
        assert actual.equals(item.schema, check_metadata=True), (
            f"Schema drift: {item.name}"
        )

    class Method(ctypes.Structure):
        _fields_ = [
            ("name", ctypes.c_char_p),
            ("kind", ctypes.c_int),
            ("params", schema_factory),
            ("result", schema_factory),
            ("payload", schema_factory),
            ("header", schema_factory),
            ("stream_kind", ctypes.c_int),
        ]

    methods = registry_methods()
    count = ctypes.c_size_t.in_dll(library, "VGI_METHOD_COUNT").value
    assert count == len(methods)
    actual_methods = (Method * count).in_dll(library, "VGI_METHODS")
    for actual, expected in zip(actual_methods, methods, strict=True):
        assert actual.name.decode() == expected.name
        assert actual.kind == {"Unary": 0, "Void": 1, "Stream": 2}[expected.kind.value]
        assert import_schema(actual.params).equals(
            pa.schema([p.field for p in expected.params]), check_metadata=True
        )
        assert bool(actual.result) == (expected.result_field is not None)
        if actual.result:
            assert import_schema(actual.result).equals(
                pa.schema([expected.result_field]), check_metadata=True
            )
        assert bool(actual.header) == (expected.header_schema is not None)
        if actual.header:
            assert import_schema(actual.header).equals(
                expected.header_schema, check_metadata=True
            )

    executable = (
        args.roundtrip or (args.build_dir / "tests/vgi-ipc-roundtrip")
    ).resolve()
    metadata = {
        b"vgi_rpc.request_id": b"0123456789abcdef",
        b"binary": b"before\x00after\xff",
    }
    cases = []
    for item in all_schemas:
        cases.append(
            (
                item.name,
                pa.RecordBatch.from_arrays(
                    [pa.array([], type=field.type) for field in item.schema],
                    schema=item.schema,
                ),
            )
        )
    cases.extend(
        [
            (
                "values",
                pa.record_batch(
                    [
                        pa.array([1, None, -2], type=pa.int64()),
                        pa.array(["alpha", None, ""], type=pa.string()),
                        pa.array([b"\x00\xff", b"", None], type=pa.large_binary()),
                        pa.array([[1, None], [], None], type=pa.list_(pa.int32())),
                        pa.array(
                            [[("a", 1)], [], None],
                            type=pa.map_(pa.string(), pa.int64()),
                        ),
                        pa.array(
                            ["a", None, "b"],
                            type=pa.dictionary(pa.int16(), pa.string()),
                        ),
                    ],
                    names=["integer", "string", "binary", "list", "map", "dictionary"],
                ),
            ),
            (
                "sliced",
                pa.record_batch(
                    [
                        pa.array([1, 2, None, 4]),
                        pa.array(["ignored", "first", None, "last"]),
                    ],
                    names=["integer", "text"],
                ).slice(1, 2),
            ),
        ]
    )
    with tempfile.TemporaryDirectory() as directory:
        incoming, outgoing = Path(directory) / "in.arrow", Path(directory) / "out.arrow"

        def encode(
            batch: pa.RecordBatch, count: int = 1, compression: str | None = None
        ) -> bytes:
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(
                sink,
                batch.schema,
                options=pa.ipc.IpcWriteOptions(compression=compression),
            ) as writer:
                for _ in range(count):
                    writer.write_batch(batch, custom_metadata=metadata)
            return sink.getvalue().to_pybytes()

        for name, batch in cases:
            incoming.write_bytes(encode(batch))
            process = subprocess.run(
                [executable, incoming, outgoing],
                capture_output=True,
                check=False,
                timeout=15,
            )
            assert process.returncode == 0, (name, process.stderr.decode())
            with pa.ipc.open_stream(outgoing) as reader:
                actual, custom = reader.read_next_batch_with_custom_metadata()
                assert actual.equals(batch, check_metadata=True), name
                assert dict(custom.items()) == metadata, name
                assert list(reader) == [], name

        for codec in ("lz4", "zstd"):
            incoming.write_bytes(encode(cases[-2][1], compression=codec))
            process = subprocess.run(
                [executable, incoming, outgoing],
                capture_output=True,
                check=False,
                timeout=15,
            )
            assert process.returncode == 0, (codec, process.stderr.decode())
            assert (
                pa.ipc.open_stream(outgoing)
                .read_all()
                .equals(pa.Table.from_batches([cases[-2][1]]))
            ), codec

        valid = encode(cases[-1][1])
        malformed = [
            b"",
            b"garbage",
            valid[:-8],
            valid[:-9],
            valid + b"trailing",
            valid + valid,
            encode(cases[-1][1], 0),
            encode(cases[-1][1], 2),
            b"\xff\xff\xff\xff\xff\xff\xff\x7f",
        ]
        for index, data in enumerate(malformed):
            incoming.write_bytes(data)
            process = subprocess.run(
                [executable, incoming, outgoing],
                capture_output=True,
                check=False,
                timeout=15,
            )
            assert process.returncode == 1, (index, process.returncode, process.stderr)
    print(
        f"Passed: {len(all_schemas)} exact schemas, {count} method signatures, "
        f"{len(cases)} IPC round trips, {len(malformed)} malformed streams"
    )


if __name__ == "__main__":
    main()
