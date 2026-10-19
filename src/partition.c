#include "vgi/partition.h"

#include <errno.h>
#include <math.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>
int VgiPartitionField(struct ArrowSchema *field) {
    struct ArrowBuffer metadata;
    ArrowBufferInit(&metadata);
    int rc = ArrowMetadataBuilderInit(&metadata, field->metadata);
    if (!rc) rc = VgiMetadataSet(&metadata, "vgi.partition_column", "true");
    if (!rc) rc = ArrowSchemaSetMetadata(field, (const char *)metadata.data);
    ArrowBufferReset(&metadata);
    return rc;
}
static int marked(const struct ArrowSchema *schema) {
    struct ArrowStringView value = VgiMetadataGet(schema->metadata, "vgi.partition_column");
    return value.size_bytes == 4 && !memcmp(value.data, "true", 4);
}
static int compare(const struct ArrowArrayView *array, const struct ArrowSchema *schema, int64_t a,
                   int64_t b, int *result, struct ArrowError *error) {
    if (array->dictionary) {
        return compare(array->dictionary, schema->dictionary, ArrowArrayViewGetIntUnsafe(array, a),
                       ArrowArrayViewGetIntUnsafe(array, b), result, error);
    }
    switch (array->storage_type) {
        case NANOARROW_TYPE_UINT8:
        case NANOARROW_TYPE_UINT16:
        case NANOARROW_TYPE_UINT32:
        case NANOARROW_TYPE_UINT64: {
            uint64_t x = ArrowArrayViewGetUIntUnsafe(array, a),
                     y = ArrowArrayViewGetUIntUnsafe(array, b);
            *result = (x > y) - (x < y);
            return 0;
        }
        case NANOARROW_TYPE_BOOL:
        case NANOARROW_TYPE_INT8:
        case NANOARROW_TYPE_INT16:
        case NANOARROW_TYPE_INT32:
        case NANOARROW_TYPE_INT64: {
            int64_t x = ArrowArrayViewGetIntUnsafe(array, a),
                    y = ArrowArrayViewGetIntUnsafe(array, b);
            *result = (x > y) - (x < y);
            return 0;
        }
        case NANOARROW_TYPE_HALF_FLOAT:
        case NANOARROW_TYPE_FLOAT:
        case NANOARROW_TYPE_DOUBLE: {
            double x = ArrowArrayViewGetDoubleUnsafe(array, a),
                   y = ArrowArrayViewGetDoubleUnsafe(array, b);
            *result = isnan(y) ? -1 : isnan(x) ? 1 : (x > y) - (x < y);
            return 0;
        }
        case NANOARROW_TYPE_STRING:
        case NANOARROW_TYPE_LARGE_STRING:
        case NANOARROW_TYPE_STRING_VIEW:
        case NANOARROW_TYPE_BINARY:
        case NANOARROW_TYPE_LARGE_BINARY:
        case NANOARROW_TYPE_BINARY_VIEW:
        case NANOARROW_TYPE_FIXED_SIZE_BINARY: {
            struct ArrowBufferView x = ArrowArrayViewGetBytesUnsafe(array, a),
                                   y = ArrowArrayViewGetBytesUnsafe(array, b);
            int64_t n = x.size_bytes < y.size_bytes ? x.size_bytes : y.size_bytes;
            int c = n ? memcmp(x.data.data, y.data.data, (size_t)n) : 0;
            *result = c ? c : (x.size_bytes > y.size_bytes) - (x.size_bytes < y.size_bytes);
            return 0;
        }
        case NANOARROW_TYPE_DECIMAL32:
        case NANOARROW_TYPE_DECIMAL64:
        case NANOARROW_TYPE_DECIMAL128:
        case NANOARROW_TYPE_DECIMAL256: {
            struct ArrowSchemaView type;
            NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&type, schema, error));
            struct ArrowDecimal x, y;
            ArrowDecimalInit(&x, type.decimal_bitwidth, type.decimal_precision, type.decimal_scale);
            ArrowDecimalInit(&y, type.decimal_bitwidth, type.decimal_precision, type.decimal_scale);
            ArrowArrayViewGetDecimalUnsafe(array, a, &x);
            ArrowArrayViewGetDecimalUnsafe(array, b, &y);
            int sign_x = ArrowDecimalSign(&x), sign_y = ArrowDecimalSign(&y);
            if (sign_x != sign_y) {
                *result = (sign_x > sign_y) - (sign_x < sign_y);
                return 0;
            }
            if (!x.n_words) {
                int64_t a32 = ArrowDecimalGetIntUnsafe(&x), b32 = ArrowDecimalGetIntUnsafe(&y);
                *result = (a32 > b32) - (a32 < b32);
                return 0;
            }
            for (int i = x.n_words - 1; i >= 0; --i) {
                int j = x.high_word_index ? i : x.n_words - i - 1;
                if (x.words[j] != y.words[j]) {
                    *result = x.words[j] > y.words[j] ? 1 : -1;
                    return 0;
                }
            }
            *result = 0;
            return 0;
        }
        default:
            ArrowErrorSet(error, "Partition column '%s' has unsupported type %s", schema->name,
                          schema->format);
            return ENOTSUP;
    }
}
int VgiPartitionMetadata(const struct ArrowSchema *full_schema, const struct VgiBatch *batch,
                         struct ArrowBuffer *metadata, struct ArrowError *error) {
    if (!full_schema || !batch || !metadata) return EINVAL;
    if (!batch->array.length) return 0;
    int64_t count = 0;
    for (int64_t i = 0; i < full_schema->n_children; ++i)
        if (marked(full_schema->children[i]) &&
            VgiFieldIndex(&batch->schema, full_schema->children[i]->name) >= 0)
            ++count;
    if (!count) return 0;
    struct ArrowSchema schema = {0};
    struct VgiBatch bounds = {0};
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    ArrowSchemaInit(&schema);
    int rc = ArrowSchemaSetTypeStruct(&schema, count);
    for (int64_t i = 0, j = 0; !rc && i < full_schema->n_children; ++i) {
        if (!marked(full_schema->children[i])) continue;
        int64_t id = VgiFieldIndex(&batch->schema, full_schema->children[i]->name);
        if (id < 0) continue;
        ArrowSchemaRelease(schema.children[j]);
        rc = ArrowSchemaDeepCopy(batch->schema.children[id], schema.children[j]);
        if (!rc) rc = ArrowSchemaSetMetadata(schema.children[j], NULL);
        ++j;
    }
    if (!rc) rc = VgiBatchInit(&bounds, &schema, error);
    for (int64_t col = 0; !rc && col < count; ++col) {
        int64_t id = VgiFieldIndex(&batch->schema, schema.children[col]->name), low = -1, high = -1;
        const struct ArrowArrayView *array = batch->view.children[id];
        for (int64_t row = 0; !rc && row < batch->array.length; ++row) {
            if (ArrowArrayViewIsNull(array, row)) continue;
            if (low < 0) {
                low = high = row;
                continue;
            }
            int c;
            rc = compare(array, batch->schema.children[id], row, low, &c, error);
            if (!rc && c < 0) low = row;
            if (!rc) rc = compare(array, batch->schema.children[id], row, high, &c, error);
            if (!rc && c > 0) high = row;
        }
        for (int i = 0; !rc && i < 2; ++i)
            rc = low < 0
                     ? ArrowArrayAppendNull(bounds.array.children[col], 1)
                     : VgiArrayAppendValue(bounds.array.children[col], schema.children[col], array,
                                           batch->schema.children[id], i ? high : low, error);
    }
    if (!rc) bounds.array.length = 2;
    if (!rc) rc = VgiBatchFinish(&bounds, error);
    if (!rc) rc = VgiIpcWriteBatch(&bounds.schema, &bounds.array, NULL, &bytes, error);
    if (!rc) {
        size_t size =
            sodium_base64_ENCODED_LEN((size_t)bytes.size_bytes, sodium_base64_VARIANT_ORIGINAL);
        char *text = malloc(size);
        if (!text)
            rc = ENOMEM;
        else {
            sodium_bin2base64(text, size, bytes.data, (size_t)bytes.size_bytes,
                              sodium_base64_VARIANT_ORIGINAL);
            rc = VgiMetadataSet(metadata, "vgi_partition_values#b64", text);
            free(text);
        }
    }
    ArrowBufferReset(&bytes);
    VgiBatchReset(&bounds);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}
