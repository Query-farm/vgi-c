#include <errno.h>
#include <math.h>
#include <sodium.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "fixtures.h"
static int named(const struct ExampleFunction *f, const char *name) {
    return !strcmp(f->name, name);
}
int ExampleBlendedSupported(const char *name) {
    return !strcmp(name, "blended_explode") || !strcmp(name, "blended_any") ||
           !strcmp(name, "blended_any_varargs") || !strcmp(name, "geo_encode") ||
           !strcmp(name, "projectable_blended") || !strcmp(name, "hostile_provenance") ||
           !strcmp(name, "unnest_tensor_rows");
}
int ExampleBlendedBind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *out) {
    struct ExampleFunction *f = data;
    if (named(f, "unnest_tensor_rows")) {
        if (call->input_schema.n_children != 1)
            return VgiRpcFail(call->rpc, "RuntimeError", "INVALID_ARGUMENT", "",
                              "unnest_tensor_rows: input table must have exactly one column (the "
                              "nest_tensor struct)");
        struct ArrowSchema scalar = {0};
        int rc = ExampleTensorBind(call, &scalar);
        if (!rc) rc = ArrowSchemaDeepCopy(scalar.children[0]->children[0], out);
        if (scalar.release) ArrowSchemaRelease(&scalar);
        return rc;
    }
    int64_t n = named(f, "blended_any_varargs")   ? call->input_schema.n_children
                : named(f, "projectable_blended") ? 2
                                                  : 1;
    ArrowSchemaInit(out);
    int rc = ArrowSchemaSetTypeStruct(out, n);
    for (int64_t i = 0; !rc && i < n; ++i) {
        const char *name = named(f, "geo_encode")            ? "geohash"
                           : named(f, "hostile_provenance")  ? "hv"
                           : named(f, "blended_explode")     ? "i"
                           : named(f, "projectable_blended") ? (i ? "b" : "a")
                                                             : "value";
        char name_buffer[64];
        if (named(f, "blended_any") || named(f, "blended_any_varargs")) {
            if (i >= call->input_schema.n_children) {
                rc = EINVAL;
                break;
            }
            ArrowSchemaRelease(out->children[i]);
            rc = ArrowSchemaDeepCopy(call->input_schema.children[i], out->children[i]);
            if (named(f, "blended_any_varargs")) {
                snprintf(name_buffer, sizeof(name_buffer), "col%lld", (long long)i);
                name = name_buffer;
            }
        } else
            rc =
                ArrowSchemaSetType(out->children[i], named(f, "geo_encode") ? NANOARROW_TYPE_STRING
                                                                            : NANOARROW_TYPE_INT64);
        if (!rc) rc = ArrowSchemaSetName(out->children[i], name);
    }
    return rc;
}
int ExampleParentRow(struct ArrowBuffer *rows, int64_t row) {
    if (row < 0 || row > INT32_MAX) return EOVERFLOW;
    unsigned char bytes[4];
    for (int i = 0; i < 4; ++i) bytes[i] = (uint8_t)((uint64_t)row >> (8 * i));
    return ArrowBufferAppend(rows, bytes, 4);
}
int ExampleParentsMetadata(struct ArrowBuffer *metadata, const struct ArrowBuffer *rows) {
    size_t length =
        sodium_base64_ENCODED_LEN((size_t)rows->size_bytes, sodium_base64_VARIANT_ORIGINAL);
    char *encoded = malloc(length);
    if (!encoded) return ENOMEM;
    sodium_bin2base64(encoded, length, rows->data, (size_t)rows->size_bytes,
                      sodium_base64_VARIANT_ORIGINAL);
    int rc = VgiMetadataSet(metadata, "vgi_rpc.parent_row#b64", encoded);
    free(encoded);
    return rc;
}
static int tensor_rows(struct VgiFunctionCall *call, const struct VgiBatch *input,
                       struct VgiBatch *out, struct ArrowBuffer *parents) {
    struct ArrowSchema schema = {0};
    struct VgiBatch cells = {0};
    int rc = ExampleTensorBind(call, &schema);
    if (!rc) rc = VgiBatchInit(&cells, &schema, &call->rpc->error);
    for (int64_t row = 0; !rc && row < input->array.length; ++row) {
        rc = ExampleTensorRow(call, input, &cells, row);
        if (!rc) rc = ArrowArrayFinishElement(&cells.array);
    }
    if (!rc) rc = VgiBatchFinish(&cells, &call->rpc->error);
    const struct ArrowArrayView *list = rc ? NULL : cells.view.children[0];
    for (int64_t row = 0; !rc && row < input->array.length; ++row) {
        if (ArrowArrayViewIsNull(list, row)) continue;
        int64_t start = ArrowArrayViewListChildOffset(list, row),
                end = ArrowArrayViewListChildOffset(list, row + 1);
        for (int64_t i = start; !rc && i < end; ++i) {
            rc = VgiArrayAppendValue(&out->array, &out->schema, list->children[0],
                                     cells.schema.children[0]->children[0], i, &call->rpc->error);
            if (!rc) rc = ExampleParentRow(parents, row);
        }
    }
    VgiBatchReset(&cells);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}
int ExampleBlendedProcess(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                          struct VgiBatch *out, struct ArrowBuffer *metadata, int *finished) {
    (void)finished;
    struct ExampleFunction *f = data;
    int rc = VgiBatchInit(out, &call->output_schema, &call->rpc->error);
    struct ArrowBuffer parents;
    ArrowBufferInit(&parents);
    if (!rc && named(f, "unnest_tensor_rows")) rc = tensor_rows(call, input, out, &parents);
    for (int64_t row = 0; !rc && !named(f, "unnest_tensor_rows") && row < input->array.length;
         ++row) {
        if (named(f, "blended_explode")) {
            if (ArrowArrayViewIsNull(input->view.children[0], row)) continue;
            int64_t count = ArrowArrayViewGetIntUnsafe(input->view.children[0], row);
            for (int64_t i = 0; !rc && i < count; ++i) {
                rc = ArrowArrayAppendInt(out->array.children[0], i);
                if (!rc) rc = ArrowArrayFinishElement(&out->array);
                if (!rc) rc = ExampleParentRow(&parents, row);
            }
            continue;
        }
        for (int64_t col = 0; !rc && col < out->schema.n_children; ++col) {
            if (named(f, "blended_any") || named(f, "blended_any_varargs")) {
                int64_t source = named(f, "blended_any")
                                     ? 0
                                     : strtoll(out->schema.children[col]->name + 3, NULL, 10);
                if (source < 0 || source >= input->view.n_children) {
                    rc = EINVAL;
                    break;
                }
                rc = VgiArrayAppendValue(out->array.children[col], out->schema.children[col],
                                         input->view.children[source],
                                         input->schema.children[source], row, &call->rpc->error);
            } else if (named(f, "projectable_blended")) {
                if (ArrowArrayViewIsNull(input->view.children[0], row))
                    rc = ArrowArrayAppendNull(out->array.children[col], 1);
                else
                    rc = ArrowArrayAppendInt(
                        out->array.children[col],
                        (int64_t)(
                            (uint64_t)ArrowArrayViewGetIntUnsafe(input->view.children[0], row) *
                            (!strcmp(out->schema.children[col]->name, "b") ? 100 : 10)));
            } else if (named(f, "hostile_provenance"))
                rc = ArrowArrayAppendInt(out->array.children[col], row);
            else if (named(f, "geo_encode")) {
                struct ArrowBuffer text;
                ArrowBufferInit(&text);
                int null = 0;
                int64_t precision = VgiArgumentInt(call, -1, "precision", 4);
                if (precision < -300 || precision > 300) {
                    rc = EINVAL;
                    ArrowBufferReset(&text);
                    break;
                }
                for (int64_t i = 0; !rc && i < input->view.n_children && i < 3; ++i) {
                    if (ArrowArrayViewIsNull(input->view.children[i], row)) {
                        null = 1;
                        break;
                    }
                    double value;
                    rc = VgiValueDouble(input->view.children[i], input->schema.children[i], row,
                                        &value, &call->rpc->error);
                    double factor = pow(10, (double)precision);
                    value = round(value * factor) / factor;
                    char rendered[512];
                    if (isfinite(value) && trunc(value) == value)
                        snprintf(rendered, sizeof(rendered), "%.1f", value);
                    else
                        for (int p = 1; p <= 17; ++p) {
                            snprintf(rendered, sizeof(rendered), "%.*g", p, value);
                            if (strtod(rendered, NULL) == value) break;
                        }
                    if (!rc && i) rc = ArrowBufferAppend(&text, ":", 1);
                    if (!rc) rc = ArrowBufferAppend(&text, rendered, strlen(rendered));
                }
                if (!rc)
                    rc = null ? ArrowArrayAppendNull(out->array.children[col], 1)
                              : ArrowArrayAppendString(
                                    out->array.children[col],
                                    (struct ArrowStringView){(const char *)text.data,
                                                             text.size_bytes});
                ArrowBufferReset(&text);
            }
        }
        if (!rc) rc = ArrowArrayFinishElement(&out->array);
        if (!rc && named(f, "hostile_provenance")) {
            struct ArrowStringView mode = VgiArgumentString(call, -1, "mode");
            int range = !mode.data || (mode.size_bytes == 5 && !memcmp(mode.data, "range", 5));
            rc = ExampleParentRow(&parents, range ? input->array.length : row);
        }
    }
    if (!rc && named(f, "hostile_provenance")) {
        struct ArrowStringView mode = VgiArgumentString(call, -1, "mode");
        if (mode.data && mode.size_bytes == 6 && !memcmp(mode.data, "length", 6))
            rc = ExampleParentRow(&parents, 0);
        if (!rc && mode.data && mode.size_bytes == 6 && !memcmp(mode.data, "base64", 6))
            rc = VgiMetadataSet(metadata, "vgi_rpc.parent_row#b64", "!!!not base64!!!");
        else if (!rc)
            rc = ExampleParentsMetadata(metadata, &parents);
    } else if (!rc && (named(f, "blended_explode") || named(f, "unnest_tensor_rows")))
        rc = ExampleParentsMetadata(metadata, &parents);
    if (!rc) rc = VgiBatchFinish(out, &call->rpc->error);
    ArrowBufferReset(&parents);
    return rc;
}
