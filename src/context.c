#include "vgi/context.h"

#include <errno.h>
#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <string.h>
const struct ArrowArrayView *VgiStructField(const struct ArrowArrayView *value,
                                            const struct ArrowSchema *schema, const char *name) {
    if (!value || !schema || value->storage_type != NANOARROW_TYPE_STRUCT || !value->length ||
        ArrowArrayViewIsNull(value, 0))
        return NULL;
    int64_t index = VgiFieldIndex(schema, name);
    return index >= 0 ? value->children[index] : NULL;
}
static int matches(struct ArrowStringView value, const char *text) {
    return value.data && value.size_bytes == (int64_t)strlen(text) &&
           !memcmp(value.data, text, (size_t)value.size_bytes);
}
const struct ArrowArrayView *VgiSecretSelect(const struct VgiFunctionCall *call, const char *type,
                                             const struct ArrowStringView *path,
                                             const struct ArrowSchema **out_schema) {
    const struct ArrowArrayView *best = NULL;
    int64_t score = -2;
    if (out_schema) *out_schema = NULL;
    for (int64_t i = 0; i < call->secrets.schema.n_children; ++i) {
        const struct ArrowSchema *schema = call->secrets.schema.children[i];
        const struct ArrowArrayView *value = call->secrets.view.children[i];
        if (value->storage_type != NANOARROW_TYPE_STRUCT || !value->length ||
            ArrowArrayViewIsNull(value, 0))
            continue;
        struct ArrowStringView actual = VgiValueString(VgiStructField(value, schema, "type"), 0);
        if (!actual.data) actual = VgiMetadataGet(schema->metadata, "secret_type");
        if (!actual.data) actual = ArrowCharView(schema->name);
        if (!matches(actual, type)) continue;
        if (!path) {
            if (out_schema) *out_schema = schema;
            return value;
        }
        struct ArrowStringView scopes = VgiValueString(VgiStructField(value, schema, "scope"), 0);
        int64_t candidate = scopes.size_bytes ? -2 : -1;
        for (int64_t start = 0; start < scopes.size_bytes;) {
            int64_t end = start;
            while (end < scopes.size_bytes && scopes.data[end] != '\n') ++end;
            int64_t length = end - start;
            if (length && length <= path->size_bytes &&
                !memcmp(path->data, scopes.data + start, (size_t)length) && length > candidate)
                candidate = length;
            start = end + 1;
        }
        if (candidate > score) {
            best = value;
            score = candidate;
            if (out_schema) *out_schema = schema;
        }
    }
    return best;
}
int VgiValueRender(const struct ArrowArrayView *value, const struct ArrowSchema *schema,
                   int64_t row, struct ArrowBuffer *text, struct ArrowError *error) {
    if (!value || row < 0 || row >= value->length || ArrowArrayViewIsNull(value, row)) return 0;
    if (value->dictionary)
        return VgiValueRender(value->dictionary, schema->dictionary,
                              ArrowArrayViewGetIntUnsafe(value, row), text, error);
    struct ArrowStringView string = VgiValueString(value, row);
    if (string.data) return ArrowBufferAppend(text, string.data, string.size_bytes);
    char rendered[128];
    if (value->storage_type == NANOARROW_TYPE_BOOL)
        snprintf(rendered, sizeof(rendered), "%s",
                 ArrowArrayViewGetIntUnsafe(value, row) ? "true" : "false");
    else if (value->storage_type == NANOARROW_TYPE_UINT64)
        snprintf(rendered, sizeof(rendered), "%" PRIu64, ArrowArrayViewGetUIntUnsafe(value, row));
    else if (value->storage_type == NANOARROW_TYPE_FLOAT ||
             value->storage_type == NANOARROW_TYPE_DOUBLE) {
        double n = ArrowArrayViewGetDoubleUnsafe(value, row);
        snprintf(rendered, sizeof(rendered), isfinite(n) && trunc(n) == n ? "%.1f" : "%.17g", n);
    } else if (VgiValueIsNumeric(value))
        snprintf(rendered, sizeof(rendered), "%" PRId64, ArrowArrayViewGetIntUnsafe(value, row));
    else {
        double n;
        NANOARROW_RETURN_NOT_OK(VgiValueDouble(value, schema, row, &n, error));
        snprintf(rendered, sizeof(rendered), "%.17g", n);
    }
    return ArrowBufferAppend(text, rendered, strlen(rendered));
}
