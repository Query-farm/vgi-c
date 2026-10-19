#include "vgi/statistics.h"

#include <errno.h>
int VgiStatisticsInit(struct VgiBatch *batch, struct ArrowError *error) {
    struct ArrowSchema schema = {0};
    ArrowSchemaInit(&schema);
    int rc = ArrowSchemaSetTypeStruct(&schema, 8);
    const char *names[] = {"column_name",
                           "min",
                           "max",
                           "has_null",
                           "has_not_null",
                           "distinct_count",
                           "contains_unicode",
                           "max_string_length"};
    enum ArrowType types[] = {NANOARROW_TYPE_STRING,       NANOARROW_TYPE_SPARSE_UNION,
                              NANOARROW_TYPE_SPARSE_UNION, NANOARROW_TYPE_BOOL,
                              NANOARROW_TYPE_BOOL,         NANOARROW_TYPE_INT64,
                              NANOARROW_TYPE_BOOL,         NANOARROW_TYPE_UINT64};
    enum ArrowType bound_types[] = {NANOARROW_TYPE_INT64, NANOARROW_TYPE_DOUBLE,
                                    NANOARROW_TYPE_STRING, NANOARROW_TYPE_BINARY};
    for (int i = 0; !rc && i < 8; ++i) {
        if (i == 1 || i == 2) {
            rc = ArrowSchemaSetTypeUnion(schema.children[i], NANOARROW_TYPE_SPARSE_UNION, 4);
            for (int j = 0; !rc && j < 4; ++j) {
                char name[2] = {(char)('0' + j), 0};
                rc = ArrowSchemaSetType(schema.children[i]->children[j], bound_types[j]);
                if (!rc) rc = ArrowSchemaSetName(schema.children[i]->children[j], name);
            }
        } else
            rc = ArrowSchemaSetType(schema.children[i], types[i]);
        if (!rc) rc = ArrowSchemaSetName(schema.children[i], names[i]);
    }
    if (!rc) rc = VgiBatchInit(batch, &schema, error);
    ArrowSchemaRelease(&schema);
    return rc;
}
static int append_bound(struct ArrowArray *array, const struct VgiStatValue *value,
                        enum VgiStatKind kind) {
    if (kind < VGI_STAT_INT64 || kind > VGI_STAT_BINARY) return EINVAL;
    struct ArrowArray *child = array->children[kind];
    int rc;
    if (!value->present)
        rc = ArrowArrayAppendNull(child, 1);
    else if (value->kind != kind)
        return EINVAL;
    else
        switch (kind) {
            case VGI_STAT_INT64:
                rc = ArrowArrayAppendInt(child, value->integer);
                break;
            case VGI_STAT_DOUBLE:
                rc = ArrowArrayAppendDouble(child, value->real);
                break;
            case VGI_STAT_STRING:
                rc = ArrowArrayAppendString(child, value->bytes);
                break;
            default:
                rc = ArrowArrayAppendBytes(
                    child, (struct ArrowBufferView){{value->bytes.data}, value->bytes.size_bytes});
                break;
        }
    return rc ? rc : ArrowArrayFinishUnionElement(array, (int8_t)kind);
}
int VgiStatisticsAppend(struct VgiBatch *batch, const struct VgiColumnStatistics *column,
                        struct ArrowError *error) {
    enum VgiStatKind kind = column->min.present   ? column->min.kind
                            : column->max.present ? column->max.kind
                                                  : VGI_STAT_INT64;
    NANOARROW_RETURN_NOT_OK(VgiBatchSetString(batch, "column_name", column->name));
    NANOARROW_RETURN_NOT_OK(append_bound(VgiBatchColumn(batch, "min"), &column->min, kind));
    NANOARROW_RETURN_NOT_OK(append_bound(VgiBatchColumn(batch, "max"), &column->max, kind));
    NANOARROW_RETURN_NOT_OK(VgiBatchSetInt(batch, "has_null", column->has_null));
    NANOARROW_RETURN_NOT_OK(VgiBatchSetInt(batch, "has_not_null", column->has_not_null));
    NANOARROW_RETURN_NOT_OK(column->distinct_count < 0
                                ? VgiBatchSetNull(batch, "distinct_count")
                                : VgiBatchSetInt(batch, "distinct_count", column->distinct_count));
    NANOARROW_RETURN_NOT_OK(
        column->contains_unicode < 0
            ? VgiBatchSetNull(batch, "contains_unicode")
            : VgiBatchSetInt(batch, "contains_unicode", column->contains_unicode));
    NANOARROW_RETURN_NOT_OK(
        column->max_string_length < 0
            ? VgiBatchSetNull(batch, "max_string_length")
            : VgiBatchSetInt(batch, "max_string_length", column->max_string_length));
    return VgiBatchFinishRow(batch, error);
}
