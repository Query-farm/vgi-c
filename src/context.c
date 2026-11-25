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
/* Byte offset of the first byte in text[from, size) that is one of `set`, or
 * size. A NUL byte never matches (strchr would find the terminator). */
static int64_t find_first_of(struct ArrowStringView text, int64_t from, const char *set) {
    for (int64_t i = from; i < text.size_bytes; ++i)
        if (text.data[i] && strchr(set, text.data[i])) return i;
    return text.size_bytes;
}
/* Byte offset of the first "://" in text, or -1. */
static int64_t find_scheme_end(struct ArrowStringView text) {
    for (int64_t i = 0; i + 3 <= text.size_bytes; ++i)
        if (!memcmp(text.data + i, "://", 3)) return i;
    return -1;
}
/* Length of the URI authority of `text`, or 0 when it has none; DuckDB's
 * StringUtil::GetURIAuthority. `text` counts as a URI only if it contains
 * "://" after a non-empty scheme and its first '/' or '\' is the one that
 * starts that "://". The authority runs from after "://" to the first '/',
 * '\', '?' or '#' (or the end). The authority starts at scheme_end + 3. */
static int64_t uri_authority_length(struct ArrowStringView text, int64_t scheme_end) {
    if (scheme_end <= 0 || find_first_of(text, 0, "/\\") != scheme_end + 1) return 0;
    int64_t start = scheme_end + 3;
    return find_first_of(text, start, "/\\?#") - start;
}
int VgiScopeMatches(struct ArrowStringView prefix, struct ArrowStringView path) {
    if (!prefix.data || prefix.size_bytes <= 0 || !path.data ||
        prefix.size_bytes > path.size_bytes ||
        memcmp(path.data, prefix.data, (size_t)prefix.size_bytes))
        return 0;
    int64_t scheme_end = find_scheme_end(prefix);
    int64_t authority = uri_authority_length(prefix, scheme_end);
    int authority_only = authority > 0 && prefix.size_bytes == scheme_end + 3 + authority;
    if (!authority_only) return 1;
    return path.size_bytes == prefix.size_bytes || path.data[prefix.size_bytes] == '/';
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
            struct ArrowStringView prefix = {scopes.data + start, length};
            if (length > candidate && VgiScopeMatches(prefix, *path)) candidate = length;
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
        NANOARROW_RETURN_NOT_OK(VgiValueNumber(value, schema, row, &n, error));
        snprintf(rendered, sizeof(rendered), "%.17g", n);
    }
    return ArrowBufferAppend(text, rendered, strlen(rendered));
}
