#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "fixtures.h"

static int nest_error(struct VgiFunctionCall *call, const char *message) {
    ArrowErrorSet(&call->rpc->error, "NestTensorError: nest_tensor: %s", message);
    call->rpc->error_type = "NestTensorError";
    call->rpc->error_code = "INVALID_ARGUMENT";
    return EINVAL;
}
int ExampleNestBind(struct VgiFunctionCall *call, struct ArrowSchema *out) {
    if (call->input_schema.n_children < 2)
        return nest_error(call, "expected 2 arguments (value, axes struct)");
    const struct ArrowSchema *value = call->input_schema.children[0],
                             *axes = call->input_schema.children[1];
    if (strcmp(axes->format, "+s")) return nest_error(call, "second argument must be a struct");
    if (!axes->n_children) return nest_error(call, "axes struct must have at least one field");
    ArrowSchemaInit(out);
    int rc = ArrowSchemaSetTypeStruct(out, 1);
    struct ArrowSchema *result = rc ? NULL : out->children[0];
    if (!rc) rc = ArrowSchemaSetName(result, "result");
    if (!rc) rc = ArrowSchemaSetTypeStruct(result, 2);
    if (!rc) rc = ArrowSchemaSetName(result->children[0], "tensor");
    struct ArrowSchema *leaf = rc ? NULL : result->children[0];
    for (int64_t i = 0; !rc && i < axes->n_children; ++i) {
        rc = ArrowSchemaSetType(leaf, NANOARROW_TYPE_LIST);
        if (!rc) leaf = leaf->children[0];
    }
    if (!rc) {
        ArrowSchemaRelease(leaf);
        rc = ArrowSchemaDeepCopy(value, leaf);
    }
    if (!rc) rc = ArrowSchemaSetName(leaf, "item");
    if (!rc) rc = ArrowSchemaSetTypeStruct(result->children[1], axes->n_children);
    if (!rc) rc = ArrowSchemaSetName(result->children[1], "axes");
    for (int64_t i = 0; !rc && i < axes->n_children; ++i) {
        struct ArrowSchemaView type;
        rc = ArrowSchemaViewInit(&type, axes->children[i], &call->rpc->error);
        if (rc) break;
        if (type.type == NANOARROW_TYPE_FLOAT || type.type == NANOARROW_TYPE_DOUBLE ||
            type.type == NANOARROW_TYPE_HALF_FLOAT) {
            rc = nest_error(call, "floating-point coord types are not supported");
            break;
        }
        if (axes->children[i]->n_children) {
            rc = nest_error(call, "only scalar coord types are supported");
            break;
        }
        struct ArrowSchema *axis = result->children[1]->children[i];
        rc = ArrowSchemaSetType(axis, NANOARROW_TYPE_LIST);
        if (!rc) rc = ArrowSchemaSetName(axis, axes->children[i]->name);
        if (!rc) {
            ArrowSchemaRelease(axis->children[0]);
            rc = ArrowSchemaDeepCopy(axes->children[i], axis->children[0]);
        }
        if (!rc) rc = ArrowSchemaSetName(axis->children[0], "item");
    }
    if (rc && out->release) ArrowSchemaRelease(out);
    return rc;
}
static int64_t argument_index(const struct VgiBatch *batch, int position) {
    for (int64_t i = 0; i < batch->schema.n_children; ++i) {
        if (!strcmp(batch->schema.children[i]->name, "__vgi_group_id")) continue;
        if (!position--) return i;
    }
    return -1;
}
int ExampleNestUpdate(struct VgiFunctionCall *call, const struct VgiBatch *input, int64_t row,
                      struct ArrowBuffer *state, int *present) {
    int64_t axes = argument_index(input, 1);
    if (axes < 0) return nest_error(call, "expected 2 arguments (value, axes struct)");
    if (ArrowArrayViewIsNull(input->view.children[axes], row)) return 0;
    struct VgiBatch one = {0};
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    int rc = VgiBatchInit(&one, &input->schema, &call->rpc->error);
    if (!rc)
        rc = VgiArrayAppendValue(&one.array, &one.schema, &input->view, &input->schema, row,
                                 &call->rpc->error);
    if (!rc) rc = VgiBatchFinish(&one, &call->rpc->error);
    if (!rc) rc = VgiIpcWriteBatch(&one.schema, &one.array, NULL, &bytes, &call->rpc->error);
    uint8_t length[8];
    for (int i = 0; i < 8; ++i) length[i] = (uint8_t)((uint64_t)bytes.size_bytes >> (i * 8));
    if (!rc) rc = ArrowBufferAppend(state, length, sizeof(length));
    if (!rc) rc = ArrowBufferAppend(state, bytes.data, bytes.size_bytes);
    if (!rc) *present = 1;
    VgiBatchReset(&one);
    ArrowBufferReset(&bytes);
    return rc;
}
static int compare(const struct ArrowArrayView *value, int64_t a, int64_t b) {
    struct ArrowStringView x = VgiValueString(value, a), y = VgiValueString(value, b);
    if (x.data && y.data) {
        int64_t size = x.size_bytes < y.size_bytes ? x.size_bytes : y.size_bytes;
        int cmp = memcmp(x.data, y.data, (size_t)size);
        return cmp ? cmp : (x.size_bytes > y.size_bytes) - (x.size_bytes < y.size_bytes);
    }
    if (value->storage_type == NANOARROW_TYPE_UINT64) {
        uint64_t x = ArrowArrayViewGetUIntUnsafe(value, a),
                 y = ArrowArrayViewGetUIntUnsafe(value, b);
        return (x > y) - (x < y);
    }
    int64_t left = ArrowArrayViewGetIntUnsafe(value, a),
            right = ArrowArrayViewGetIntUnsafe(value, b);
    return (left > right) - (left < right);
}
static void sort(const struct ArrowArrayView *value, int64_t *order, int64_t *scratch,
                 int64_t length) {
    if (length < 2) return;
    int64_t middle = length / 2;
    sort(value, order, scratch, middle);
    sort(value, order + middle, scratch + middle, length - middle);
    int64_t i = 0, j = middle;
    for (int64_t k = 0; k < length; ++k)
        scratch[k] = j == length || (i < middle && compare(value, order[i], order[j]) <= 0)
                         ? order[i++]
                         : order[j++];
    memcpy(order, scratch, (size_t)length * sizeof(*order));
}
struct TensorBuild {
    const struct ArrowArrayView *values;
    const struct ArrowSchema *value_schema;
    int64_t *shape;
    int64_t *source;
    int64_t dimensions;
    int64_t offset;
    struct ArrowError *error;
};
static int build_tensor(struct TensorBuild *build, struct ArrowArray *out,
                        const struct ArrowSchema *schema, int64_t dimension) {
    if (dimension == build->dimensions) {
        int64_t source = build->source[build->offset++];
        return source < 0 ? ArrowArrayAppendNull(out, 1)
                          : VgiArrayAppendValue(out, schema, build->values, build->value_schema,
                                                source, build->error);
    }
    for (int64_t i = 0; i < build->shape[dimension]; ++i)
        NANOARROW_RETURN_NOT_OK(
            build_tensor(build, out->children[0], schema->children[0], dimension + 1));
    return ArrowArrayFinishElement(out);
}
int ExampleNestFinalize(struct VgiFunctionCall *call, const struct ArrowBufferView *state,
                        struct VgiBatch *output) {
    struct VgiBatch rows = {0};
    int rc = 0;
    for (int64_t offset = 0; state && !rc && offset < state->size_bytes;) {
        if (state->size_bytes - offset < 8) {
            rc = EINVAL;
            break;
        }
        uint64_t size = 0;
        for (int i = 0; i < 8; ++i) size |= (uint64_t)state->data.as_uint8[offset + i] << (i * 8);
        offset += 8;
        if (size > (uint64_t)(state->size_bytes - offset)) {
            rc = EINVAL;
            break;
        }
        struct VgiBatch part = {0};
        rc = VgiBatchDecode(
            &part, (struct ArrowBufferView){{state->data.as_uint8 + offset}, (int64_t)size},
            &call->rpc->error);
        if (!rc && !rows.schema.release) rc = VgiBatchInit(&rows, &part.schema, &call->rpc->error);
        for (int64_t i = 0; !rc && i < part.array.length; ++i)
            rc = VgiArrayAppendValue(&rows.array, &rows.schema, &part.view, &part.schema, i,
                                     &call->rpc->error);
        VgiBatchReset(&part);
        offset += (int64_t)size;
    }
    if (!rc && rows.schema.release) rc = VgiBatchFinish(&rows, &call->rpc->error);
    struct ArrowArray *result = output->array.children[0], *out_axes = result->children[1];
    const struct ArrowSchema *result_schema = output->schema.children[0];
    int64_t dimensions = out_axes->n_children, count = rows.array.length;
    int64_t *shape = calloc((size_t)dimensions, sizeof(*shape));
    int64_t *order = count ? malloc((size_t)count * sizeof(*order)) : NULL,
            *scratch = count ? malloc((size_t)count * sizeof(*scratch)) : NULL;
    int64_t *ranks = count ? calloc((size_t)count * (size_t)dimensions, sizeof(*ranks)) : NULL;
    if (!shape || (count && (!order || !scratch || !ranks))) rc = ENOMEM;
    int64_t axes_index = count ? argument_index(&rows, 1) : -1,
            value_index = count ? argument_index(&rows, 0) : -1;
    const struct ArrowArrayView *axes = axes_index >= 0 ? rows.view.children[axes_index] : NULL;
    const struct ArrowSchema *axes_schema =
        axes_index >= 0 ? rows.schema.children[axes_index] : NULL;
    for (int64_t a = 0; !rc && a < dimensions; ++a) {
        const struct ArrowArrayView *axis = axes ? axes->children[a] : NULL;
        for (int64_t i = 0; i < count; ++i) {
            if (ArrowArrayViewIsNull(axis, i)) {
                char message[256];
                snprintf(message, sizeof(message), "null coord value for axis '%s'",
                         axes_schema->children[a]->name);
                rc = nest_error(call, message);
                break;
            }
            order[i] = i;
        }
        if (rc) break;
        sort(axis, order, scratch, count);
        for (int64_t i = 0; !rc && i < count; ++i) {
            if (!i || compare(axis, order[i - 1], order[i])) {
                ++shape[a];
                rc = VgiArrayAppendValue(out_axes->children[a]->children[0],
                                         result_schema->children[1]->children[a]->children[0], axis,
                                         axes_schema->children[a], order[i], &call->rpc->error);
            }
            ranks[a * count + order[i]] = shape[a] - 1;
        }
        if (!rc) rc = ArrowArrayFinishElement(out_axes->children[a]);
    }
    int64_t cells = 1;
    for (int64_t a = 0; !rc && a < dimensions; ++a)
        if (__builtin_mul_overflow(cells, shape[a], &cells) || cells > INT32_MAX) rc = EOVERFLOW;
    int64_t *sources = !rc && cells ? malloc((size_t)cells * sizeof(*sources)) : NULL;
    if (!rc && cells && !sources) rc = ENOMEM;
    for (int64_t i = 0; !rc && i < cells; ++i) sources[i] = -1;
    for (int64_t i = 0; !rc && i < count; ++i) {
        int64_t flat = 0;
        for (int64_t a = 0; a < dimensions; ++a) flat = flat * shape[a] + ranks[a * count + i];
        if (sources[flat] >= 0) {
            rc = nest_error(call, "duplicate coordinate at cell");
            break;
        }
        sources[flat] = i;
    }
    struct TensorBuild build = {value_index >= 0 ? rows.view.children[value_index] : NULL,
                                value_index >= 0 ? rows.schema.children[value_index] : NULL,
                                shape,
                                sources,
                                dimensions,
                                0,
                                &call->rpc->error};
    if (!rc) rc = build_tensor(&build, result->children[0], result_schema->children[0], 0);
    if (!rc) rc = ArrowArrayFinishElement(out_axes);
    if (!rc) rc = ArrowArrayFinishElement(result);
    free(shape);
    free(order);
    free(scratch);
    free(ranks);
    free(sources);
    VgiBatchReset(&rows);
    return rc;
}
