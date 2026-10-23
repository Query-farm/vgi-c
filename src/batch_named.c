#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "vgi/batch.h"

int VgiBatchInitNamed(struct VgiBatch *batch, const char *name, struct ArrowError *error) {
    struct ArrowSchema schema = {0};
    int rc = VgiSchemaInit(name, &schema, error);
    if (!rc) rc = VgiBatchInit(batch, &schema, error);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}
static int is_decimal(enum ArrowType type) {
    switch (type) {
        case NANOARROW_TYPE_DECIMAL32:
        case NANOARROW_TYPE_DECIMAL64:
        case NANOARROW_TYPE_DECIMAL128:
        case NANOARROW_TYPE_DECIMAL256:
            return 1;
        default:
            return 0;
    }
}
int VgiValueIsNumber(const struct ArrowArrayView *value) {
    return value && (VgiValueIsNumeric(value) || is_decimal(value->storage_type));
}
static int unconvertible(struct ArrowError *error) {
    ArrowErrorSet(error, "Cannot convert Arrow value to double");
    return EINVAL;
}
/* VgiValueDouble (vgi-rpc-c) reads ArrowSchemaView.decimal_bitwidth for every
 * non-numeric type, which nanoarrow sets only for decimals, so it is handed
 * numbers and decimals only. Text is parsed here. */
int VgiValueNumber(const struct ArrowArrayView *value, const struct ArrowSchema *schema,
                   int64_t index, double *out, struct ArrowError *error) {
    if (!value || !out || index < 0 || index >= value->length || ArrowArrayViewIsNull(value, index))
        return EINVAL;
    if (value->dictionary) {
        if (!schema || !schema->dictionary) return unconvertible(error);
        return VgiValueNumber(value->dictionary, schema->dictionary,
                              ArrowArrayViewGetIntUnsafe(value, index), out, error);
    }
    if (VgiValueIsNumeric(value)) return VgiValueDouble(value, schema, index, out, error);
    if (VgiValueIsNumber(value))
        return schema ? VgiValueDouble(value, schema, index, out, error) : unconvertible(error);
    struct ArrowStringView text = VgiValueString(value, index);
    if (!text.data || text.size_bytes <= 0 || text.size_bytes >= 4096) return unconvertible(error);
    char *copy = malloc((size_t)text.size_bytes + 1);
    if (!copy) return ENOMEM;
    memcpy(copy, text.data, (size_t)text.size_bytes);
    copy[text.size_bytes] = 0;
    char *end = NULL;
    *out = strtod(copy, &end);
    int ok = end != copy && end == copy + text.size_bytes;
    free(copy);
    return ok ? 0 : unconvertible(error);
}
