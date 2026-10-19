#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "fixtures.h"

static int tensor_error(struct VgiFunctionCall *call, const char *message) {
    ArrowErrorSet(&call->rpc->error, "unnest_tensor: %s", message);
    call->rpc->error_type = "RuntimeError";
    call->rpc->error_code = "INVALID_ARGUMENT";
    return EINVAL;
}
static int list(const struct ArrowSchema *schema) {
    return !strcmp(schema->format, "+l") || !strcmp(schema->format, "+L");
}
int ExampleTensorBind(struct VgiFunctionCall *call, struct ArrowSchema *output) {
    if (!call->input_schema.n_children || strcmp(call->input_schema.children[0]->format, "+s"))
        return tensor_error(call, "argument must be a struct");
    const struct ArrowSchema *input = call->input_schema.children[0];
    int64_t ti = VgiFieldIndex(input, "tensor"), ai = VgiFieldIndex(input, "axes");
    if (ti < 0 || ai < 0) return tensor_error(call, "struct must have 'tensor' and 'axes' fields");
    const struct ArrowSchema *axes = input->children[ai], *leaf = input->children[ti];
    if (strcmp(axes->format, "+s")) return tensor_error(call, "'axes' field must be a struct");
    int64_t depth = 0;
    while (list(leaf)) {
        ++depth;
        leaf = leaf->children[0];
    }
    if (depth != axes->n_children) {
        char message[160];
        snprintf(message, sizeof(message),
                 "tensor nesting depth %lld does not match number of axes %lld", (long long)depth,
                 (long long)axes->n_children);
        return tensor_error(call, message);
    }
    ArrowSchemaInit(output);
    int rc = ArrowSchemaSetTypeStruct(output, 1);
    struct ArrowSchema *result = rc ? NULL : output->children[0];
    if (!rc) rc = ArrowSchemaSetType(result, NANOARROW_TYPE_LIST);
    if (!rc) rc = ArrowSchemaSetName(result, "result");
    if (!rc) rc = ArrowSchemaSetTypeStruct(result->children[0], 2);
    struct ArrowSchema *row = rc ? NULL : result->children[0];
    if (!rc) {
        ArrowSchemaRelease(row->children[0]);
        rc = ArrowSchemaDeepCopy(leaf, row->children[0]);
    }
    if (!rc) rc = ArrowSchemaSetName(row->children[0], "value");
    if (!rc) rc = ArrowSchemaSetTypeStruct(row->children[1], axes->n_children);
    if (!rc) rc = ArrowSchemaSetName(row->children[1], "axes");
    for (int64_t i = 0; !rc && i < axes->n_children; ++i) {
        if (!list(axes->children[i])) {
            rc = tensor_error(call, "each axis must be a list of coordinates");
            break;
        }
        struct ArrowSchema *field = row->children[1]->children[i];
        ArrowSchemaRelease(field);
        rc = ArrowSchemaDeepCopy(axes->children[i]->children[0], field);
        if (!rc) rc = ArrowSchemaSetName(field, axes->children[i]->name);
        field->flags |= ARROW_FLAG_NULLABLE;
    }
    if (rc && output->release) ArrowSchemaRelease(output);
    return rc;
}
int ExampleTensorRow(struct VgiFunctionCall *call, const struct VgiBatch *input,
                     struct VgiBatch *output, int64_t row) {
    const struct ArrowSchema *input_schema = input->schema.children[0];
    const struct ArrowArrayView *cells = input->view.children[0];
    struct ArrowArray *result = output->array.children[0];
    if (ArrowArrayViewIsNull(cells, row)) return ArrowArrayAppendNull(result, 1);
    int64_t ti = VgiFieldIndex(input_schema, "tensor"), ai = VgiFieldIndex(input_schema, "axes");
    if (ti < 0 || ai < 0) return tensor_error(call, "input must have 'tensor' and 'axes' fields");
    const struct ArrowArrayView *axes = cells->children[ai], *tensor = cells->children[ti];
    const struct ArrowSchema *axes_schema = input_schema->children[ai],
                             *tensor_schema = input_schema->children[ti];
    int64_t dimensions = axes->n_children, total = 1;
    int64_t *shape = dimensions ? calloc((size_t)dimensions * 3, sizeof(int64_t)) : NULL;
    if (dimensions && !shape) return ENOMEM;
    int64_t *starts = shape ? shape + dimensions : NULL,
            *coordinates = shape ? shape + dimensions * 2 : NULL;
    int rc = 0;
    for (int64_t i = 0; i < dimensions; ++i) {
        const struct ArrowArrayView *axis = axes->children[i];
        if (ArrowArrayViewIsNull(axis, row)) {
            total = 0;
            break;
        }
        starts[i] = ArrowArrayViewListChildOffset(axis, row);
        shape[i] = ArrowArrayViewListChildOffset(axis, row + 1) - starts[i];
        if (__builtin_mul_overflow(total, shape[i], &total)) {
            rc = EOVERFLOW;
            break;
        }
    }
    struct ArrowArray *item = result->children[0];
    const struct ArrowSchema *item_schema = output->schema.children[0]->children[0];
    for (int64_t k = 0; !rc && k < total; ++k) {
        int64_t rest = k;
        for (int64_t i = dimensions; i-- > 0;) {
            coordinates[i] = rest % shape[i];
            rest /= shape[i];
        }
        const struct ArrowArrayView *value = tensor;
        const struct ArrowSchema *value_schema = tensor_schema;
        int64_t index = row;
        for (int64_t i = 0; i < dimensions; ++i) {
            int64_t start = ArrowArrayViewListChildOffset(value, index),
                    end = ArrowArrayViewListChildOffset(value, index + 1);
            if (end - start <= coordinates[i]) {
                rc = tensor_error(call, "tensor shape does not match axes");
                break;
            }
            index = start + coordinates[i];
            value = value->children[0];
            value_schema = value_schema->children[0];
        }
        if (!rc)
            rc = VgiArrayAppendValue(item->children[0], item_schema->children[0], value,
                                     value_schema, index, &call->rpc->error);
        for (int64_t i = 0; !rc && i < dimensions; ++i)
            rc = VgiArrayAppendValue(
                item->children[1]->children[i], item_schema->children[1]->children[i],
                axes->children[i]->children[0], axes_schema->children[i]->children[0],
                starts[i] + coordinates[i], &call->rpc->error);
        if (!rc) rc = ArrowArrayFinishElement(item->children[1]);
        if (!rc) rc = ArrowArrayFinishElement(item);
    }
    if (!rc) rc = ArrowArrayFinishElement(result);
    free(shape);
    return rc;
}
