# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Generate nanoarrow C schemas and the VGI method inventory from the Python model.

Export through Arrow's C Data Interface so formats, flags, field metadata and
dictionary value types stay exact without maintaining another type mapping.
Run ``scripts/regenerate_protocol.sh`` from the C SDK to regenerate.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import os
import struct
import tempfile
from pathlib import Path
from typing import Any, TextIO

import pyarrow as pa
from pyarrow.cffi import ffi
from vgi.attach_ticket import AttachTickets, SealAttachRequest
from vgi.codegen._common import (
    EXTRA_RESPONSE_TYPES,
    REQUEST_TYPES,
    EmittedSchema,
    collect_schemas,
)
from vgi.codegen._registry import preimage_hash, registry_methods
from vgi.protocol import VgiProtocol
from vgi_rpc.rpc import rpc_methods


def _literal(value: bytes | str | None) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, str):
        value = value.encode("utf-8")
    # Fixed-width octal escapes cannot consume a subsequent hexadecimal digit.
    return '"' + "".join(chr(b) if 32 <= b < 127 and b not in (34, 92) else f"\\{b:03o}" for b in value) + '"'


def _metadata_bytes(ptr: Any) -> bytes | None:
    if ptr == ffi.NULL:
        return None
    offset = 4
    count = struct.unpack("=i", ffi.buffer(ptr, 4))[0]
    for _ in range(count):
        for _ in range(2):
            length = struct.unpack("=i", ffi.buffer(ptr + offset, 4))[0]
            offset += 4 + length
    return bytes(ffi.buffer(ptr, offset))


def schemas() -> list[EmittedSchema]:
    """Collect named payloads plus each method's complete wire envelopes."""
    out = collect_schemas(extra_response_types=(*EXTRA_RESPONSE_TYPES, *REQUEST_TYPES))
    out.append(EmittedSchema("Empty", pa.schema([]), "void response"))
    for method in registry_methods():
        if method.result_field is not None:
            out.append(
                EmittedSchema(
                    method.schema_names.params.removesuffix("Params") + "Envelope",
                    pa.schema([method.result_field]),
                    f"{method.name} response envelope",
                )
            )
        if method.header_type is not None:
            name = method.header_type.__name__
            if not any(s.name == name for s in out):
                header_schema = method.header_schema
                assert header_schema is not None
                out.append(EmittedSchema(name, header_schema, f"{method.name} header"))
    for protocol, prefix, extra in (
        (AttachTickets, "Tickets", (SealAttachRequest,)),
    ):
        for item in collect_schemas(protocol, info_types=(), extra_response_types=extra, check_info_subclasses=False):
            out.append(EmittedSchema(prefix + item.name, item.schema, item.origin))
        for method in registry_methods(protocol):
            if method.result_field is not None:
                out.append(
                    EmittedSchema(
                        prefix + method.schema_names.params.removesuffix("Params") + "Envelope",
                        pa.schema([method.result_field]),
                        method.name,
                    )
                )
    return out


def render() -> dict[str, str]:
    """Render both artifacts deterministically, without writing any files."""
    all_schemas = schemas()
    methods = registry_methods()
    header = io.StringIO()
    source = io.StringIO()
    header.write("""#ifndef VGI_GENERATED_PROTOCOL_H
#define VGI_GENERATED_PROTOCOL_H
#include <stddef.h>
#include "nanoarrow/nanoarrow.h"
#ifdef __cplusplus
extern "C" {
#endif

#include "vgi_rpc/method.h"
size_t VgiSchemaCount(void);
const char *VgiSchemaName(size_t index);
int VgiSchemaInit(const char *name, struct ArrowSchema *out, struct ArrowError *error);
const struct VgiMethod *VgiMethodFind(const char *name);
extern const struct VgiMethod VGI_METHODS[];
extern const size_t VGI_METHOD_COUNT;
""")
    header.write(f"#define VGI_PROTOCOL_NAME {_literal(VgiProtocol.protocol_name)}\n")
    header.write(f"#define VGI_PROTOCOL_VERSION {_literal(VgiProtocol.protocol_version)}\n")
    header.write(f"#define VGI_PROTOCOL_HASH {_literal(preimage_hash(VgiProtocol.protocol_name, methods))}\n")
    for protocol, _prefix, constant in (
        (AttachTickets, "Tickets", "TICKETS"),
    ):
        header.write(f"#define VGI_{constant}_NAME {_literal(protocol.protocol_name)}\n")
        header.write(f"#define VGI_{constant}_VERSION {_literal(getattr(protocol, 'protocol_version', ''))}\n")
        protocol_hash = preimage_hash(protocol.protocol_name, registry_methods(protocol))
        header.write(f"#define VGI_{constant}_HASH {_literal(protocol_hash)}\n")
        header.write(f"extern const struct VgiMethod VGI_{constant}_METHODS[];\n")
        header.write(f"extern const size_t VGI_{constant}_METHOD_COUNT;\n")
    source.write(
        '#include "vgi/generated/protocol.h"\n#include "schema_internal.h"\n#include <errno.h>\n#include <string.h>\n\n'
    )
    nodes: dict[tuple[Any, ...], int] = {}

    def emit_node(node: Any) -> int:
        children = tuple(emit_node(node.children[i]) for i in range(node.n_children))
        dictionary = emit_node(node.dictionary) if node.dictionary != ffi.NULL else None
        key = (
            ffi.string(node.format),
            ffi.string(node.name) if node.name != ffi.NULL else None,
            _metadata_bytes(node.metadata),
            int(node.flags),
            children,
            dictionary,
        )
        if key in nodes:
            return nodes[key]
        index = len(nodes)
        nodes[key] = index
        child_ptr = "NULL"
        if children:
            child_ptr = f"children_{index}"
            source.write(
                f"static const struct VgiSchemaNode *const {child_ptr}[] = {{"
                + ", ".join(f"&node_{i}" for i in children)
                + "};\n"
            )
        dictionary_ptr = "NULL" if dictionary is None else f"&node_{dictionary}"
        source.write(
            f"static const struct VgiSchemaNode node_{index} = {{"
            f"{_literal(key[0])}, {_literal(key[1])}, {_literal(key[2])}, "
            f"{key[3]}, {len(children)}, {child_ptr}, {dictionary_ptr}" + "};\n"
        )
        return index

    for schema in all_schemas:
        exported = ffi.new("struct ArrowSchema*")
        schema.schema._export_to_c(int(ffi.cast("uintptr_t", exported)))
        try:
            index = emit_node(exported)
        finally:
            exported.release(exported)
        factory = f"VgiSchema{schema.name}"
        header.write(f"int {factory}(struct ArrowSchema *out, struct ArrowError *error);\n")
        source.write(
            f"int {factory}(struct ArrowSchema *out, struct ArrowError *error) {{\n"
            f"    return VgiSchemaBuild(&node_{index}, out, error);\n}}\n\n"
        )

    source.write("static const struct { const char *name; VgiSchemaFactory factory; } schemas[] = {\n")
    for schema in all_schemas:
        source.write(f"    {{{_literal(schema.name)}, VgiSchema{schema.name}}},\n")
    source.write("""};
size_t VgiSchemaCount(void) { return sizeof(schemas) / sizeof(schemas[0]); }
const char *VgiSchemaName(size_t index) {
    return index < VgiSchemaCount() ? schemas[index].name : NULL;
}
int VgiSchemaInit(const char *name, struct ArrowSchema *out, struct ArrowError *error) {
    if (!name || !out || out->release) return EINVAL;
    for (size_t i = 0; i < VgiSchemaCount(); ++i)
        if (strcmp(name, schemas[i].name) == 0) return schemas[i].factory(out, error);
    ArrowErrorSet(error, "Unknown VGI schema: %s", name);
    return ENOENT;
}
const struct VgiMethod VGI_METHODS[] = {
""")
    schema_names = {schema.name for schema in all_schemas}
    rpc_inventory = rpc_methods(VgiProtocol)
    for method in methods:
        result = (
            f"VgiSchema{method.schema_names.params.removesuffix('Params')}Envelope"
            if method.result_field is not None
            else "NULL"
        )
        payload = f"VgiSchema{method.schema_names.result}" if method.schema_names.result in schema_names else "NULL"
        stream_header = f"VgiSchema{method.schema_names.header}" if method.schema_names.header else "NULL"
        is_exchange = rpc_inventory[method.name].is_exchange
        stream_kind = (
            "UNKNOWN" if not method.is_stream or is_exchange is None else ("EXCHANGE" if is_exchange else "PRODUCER")
        )
        source.write(
            f"    {{{_literal(method.name)}, VGI_METHOD_{method.kind.value.upper()}, "
            f"VgiSchema{method.schema_names.params}, {result}, {payload}, {stream_header}, VGI_STREAM_{stream_kind}}},\n"
        )
    source.write("""};
const size_t VGI_METHOD_COUNT = sizeof(VGI_METHODS) / sizeof(VGI_METHODS[0]);
const struct VgiMethod *VgiMethodFind(const char *name) {
    if (!name) return NULL;
    for (size_t i = 0; i < VGI_METHOD_COUNT; ++i)
        if (strcmp(name, VGI_METHODS[i].name) == 0) return &VGI_METHODS[i];
    return NULL;
}
""")
    for protocol, prefix, constant in (
        (AttachTickets, "Tickets", "TICKETS"),
    ):
        source.write(f"const struct VgiMethod VGI_{constant}_METHODS[] = {{\n")
        for method in registry_methods(protocol):
            stem = prefix + method.schema_names.params.removesuffix("Params")
            source.write(
                f"    {{{_literal(method.name)}, VGI_METHOD_UNARY, VgiSchema{stem}Params, "
                f"VgiSchema{stem}Envelope, VgiSchema{stem}Result, NULL, VGI_STREAM_UNKNOWN}},\n"
            )
        source.write(
            f"}};\nconst size_t VGI_{constant}_METHOD_COUNT = "
            f"sizeof(VGI_{constant}_METHODS) / sizeof(VGI_{constant}_METHODS[0]);\n"
        )
    header.write("#ifdef __cplusplus\n}\n#endif\n#endif\n")
    artifacts: dict[str, str] = {}
    for filename, body in (
        ("include/vgi/generated/protocol.h", header.getvalue()),
        ("src/generated/protocol.c", source.getvalue()),
    ):
        digest = hashlib.sha256(body.encode()).hexdigest()[:12]
        artifacts[filename] = (
            "/* Generated by vgi.codegen.c_schemas. DO NOT EDIT.\n"
            f" * Content hash: {digest}\n"
            " * Regenerate: scripts/regenerate_protocol.sh\n */\n" + body
        )
    return artifacts


def emit(out: TextIO, *, record: str = "header") -> None:
    """Support the shared multi-repository regeneration script."""
    target = {"header": "include/vgi/generated/protocol.h", "source": "src/generated/protocol.c"}[record]
    out.write(render()[target])


def main() -> int:
    """Regenerate atomically, or check for drift without changing files."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    stale = False
    for relative, content in render().items():
        target = args.output_dir / relative
        if target.is_file() and target.read_text() == content:
            continue
        if args.check:
            print(f"STALE {target}")
            stale = True
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(dir=target.parent)
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "w") as tmp:
                tmp.write(content)
            os.chmod(temporary, 0o644)
            temporary.replace(target)
        finally:
            temporary.unlink(missing_ok=True)
        print(f"wrote {target}")
    return int(stale)


if __name__ == "__main__":
    raise SystemExit(main())
