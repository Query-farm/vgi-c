#include <errno.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

#include "fixtures.h"
#include "vgi/context.h"
static int named(const struct ExampleFunction *f, const char *name) {
    return !strcmp(f->name, name);
}
struct FilterState {
    int64_t index, count, batch;
    struct ArrowSchema schema;
    struct ArrowBuffer report, columns, values;
};
int ExampleFilterSupported(const char *name) {
    const char *names[] = {"filter_echo",
                           "bool_filter_echo",
                           "dict_filter_echo",
                           "value_prune",
                           "filtered_columns_echo",
                           "dynamic_filter_echo",
                           "expression_filter_test",
                           "spatial_filter_example"};
    for (size_t i = 0; i < sizeof(names) / sizeof(*names); ++i)
        if (!strcmp(name, names[i])) return 1;
    return 0;
}
int ExampleFilterBind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *out) {
    (void)call;
    struct ExampleFunction *f = data;
    int count = named(f, "dict_filter_echo") || named(f, "value_prune")                    ? 2
                : named(f, "filtered_columns_echo")                                        ? 6
                : named(f, "expression_filter_test") || named(f, "spatial_filter_example") ? 4
                                                                                           : 3;
    ArrowSchemaInit(out);
    int rc = ArrowSchemaSetTypeStruct(out, count);
    for (int i = 0; !rc && i < count; ++i) {
        const char *name = i == 0 ? "n" : i == 1 ? "s" : "pushed_filters", *format = i ? "u" : "l";
        if (named(f, "bool_filter_echo") && i == 1) {
            name = "flag";
            format = "b";
        }
        if (named(f, "dict_filter_echo") && i == 1) {
            format = "c";
            rc = ArrowSchemaAllocateDictionary(out->children[i]);
            if (!rc) {
                ArrowSchemaInit(out->children[i]->dictionary);
                rc = ArrowSchemaSetType(out->children[i]->dictionary, NANOARROW_TYPE_STRING);
            }
        }
        if (named(f, "value_prune") && i == 1) name = "resolved";
        if (named(f, "filtered_columns_echo")) {
            const char *names[] = {"n", "tag", "filtered_cols", "has_n", "has_tag", "tag_values"};
            name = names[i];
            format = i == 0 ? "l" : i == 3 || i == 4 ? "b" : "u";
        }
        if (named(f, "expression_filter_test")) {
            const char *names[] = {"id", "name", "tags", "score"},
                       *formats[] = {"l", "u", "+l", "g"};
            name = names[i];
            format = formats[i];
            if (i == 2) {
                rc = ArrowSchemaSetType(out->children[i], NANOARROW_TYPE_LIST);
                if (!rc)
                    rc = ArrowSchemaSetType(out->children[i]->children[0], NANOARROW_TYPE_STRING);
            }
        }
        if (named(f, "spatial_filter_example")) {
            const char *names[] = {"n", "x", "y", "geom"}, *formats[] = {"l", "g", "g", "z"};
            name = names[i];
            format = formats[i];
            if (i == 3) {
                struct ArrowBuffer metadata;
                ArrowBufferInit(&metadata);
                rc = VgiMetadataSet(&metadata, "ARROW:extension:name", "geoarrow.wkb");
                if (!rc) rc = VgiMetadataSet(&metadata, "ARROW:extension:metadata", "{}");
                if (!rc) rc = ArrowSchemaSetMetadata(out->children[i], (const char *)metadata.data);
                ArrowBufferReset(&metadata);
            }
        }
        if (!rc) rc = ArrowSchemaSetFormat(out->children[i], format);
        if (!rc) rc = ArrowSchemaSetName(out->children[i], name);
    }
    return rc;
}
static int join_values(const struct VgiBatch *values, struct ArrowBuffer *out,
                       struct ArrowError *error) {
    const struct ArrowArrayView *view = values->view.children[0];
    const struct ArrowSchema *schema = values->schema.children[0];
    int64_t *indices = calloc((size_t)view->length, sizeof(*indices));
    if (view->length && !indices) return ENOMEM;
    int64_t count = 0;
    int rc = 0;
    for (int64_t row = 0; row < view->length; ++row)
        if (!ArrowArrayViewIsNull(view, row)) {
            int64_t pos = count;
            while (pos > 0) {
                int comparison;
                if (VgiValueIsNumeric(view)) {
                    int64_t a = ArrowArrayViewGetIntUnsafe(view, indices[pos - 1]),
                            b = ArrowArrayViewGetIntUnsafe(view, row);
                    comparison = (a > b) - (a < b);
                } else {
                    struct ArrowStringView a = VgiValueString(view, indices[pos - 1]),
                                           b = VgiValueString(view, row);
                    int64_t n = a.size_bytes < b.size_bytes ? a.size_bytes : b.size_bytes;
                    comparison = n ? memcmp(a.data, b.data, (size_t)n) : 0;
                    if (!comparison)
                        comparison = (a.size_bytes > b.size_bytes) - (a.size_bytes < b.size_bytes);
                }
                if (comparison <= 0) break;
                indices[pos] = indices[pos - 1];
                --pos;
            }
            indices[pos] = row;
            ++count;
        }
    for (int64_t i = 0; !rc && i < count; ++i) {
        if (i) rc = ArrowBufferAppend(out, ",", 1);
        if (!rc) rc = VgiValueRender(view, schema, indices[i], out, error);
    }
    free(indices);
    return rc;
}
int ExampleFilterInit(void *data, struct VgiFunctionCall *call) {
    struct ExampleFunction *f = data;
    struct FilterState *s = calloc(1, sizeof(*s));
    if (!s) return ENOMEM;
    call->state = s;
    ArrowBufferInit(&s->report);
    ArrowBufferInit(&s->columns);
    ArrowBufferInit(&s->values);
    int rc = ExampleFilterBind(data, call, &s->schema);
    s->count = VgiArgumentInt(call, 0, NULL, 0);
    if (s->count < 0) s->count = 0;
    s->batch = VgiArgumentInt(call, -1, "batch_size", 2048);
    if (s->batch < 1) s->batch = 1;
    if (!rc) rc = VgiFiltersFormat(call->filters, 0, &s->report, &call->rpc->error);
    if (!rc && (named(f, "value_prune") || named(f, "filtered_columns_echo"))) {
        struct VgiBatch values = {0};
        rc = VgiFiltersColumnValues(call->filters, named(f, "value_prune") ? "n" : "tag", &values,
                                    &call->rpc->error);
        if (rc == ENODATA)
            rc = ArrowBufferAppend(&s->values, named(f, "value_prune") ? "(scan)" : "(none)", 6);
        else if (!rc)
            rc = join_values(&values, &s->values, &call->rpc->error);
        VgiBatchReset(&values);
        if (!rc) rc = VgiFiltersColumns(call->filters, &values, &call->rpc->error);
        if (!rc) rc = join_values(&values, &s->columns, &call->rpc->error);
        VgiBatchReset(&values);
    }
    return rc;
}
void ExampleFilterFree(void *data, struct VgiFunctionCall *call) {
    (void)data;
    struct FilterState *s = call->state;
    if (s) {
        if (s->schema.release) ArrowSchemaRelease(&s->schema);
        ArrowBufferReset(&s->report);
        ArrowBufferReset(&s->columns);
        ArrowBufferReset(&s->values);
    }
    free(s);
    call->state = NULL;
}
int ExampleFilterProcess(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                         struct VgiBatch *out, struct ArrowBuffer *metadata, int *finished) {
    (void)input;
    (void)metadata;
    struct ExampleFunction *f = data;
    struct FilterState *s = call->state;
    if (s->index >= s->count) {
        *finished = 1;
        return 0;
    }
    int rc = VgiBatchInit(out, &s->schema, &call->rpc->error);
    if (!rc && named(f, "dynamic_filter_echo")) {
        s->report.size_bytes = 0;
        rc = VgiFiltersFormat(call->filters, 1, &s->report, &call->rpc->error);
    }
    if (!rc && named(f, "dict_filter_echo")) {
        const char *colors[] = {"red", "green", "blue"};
        for (int i = 0; !rc && i < 3; ++i)
            rc = ArrowArrayAppendString(out->array.children[1]->dictionary,
                                        ArrowCharView(colors[i]));
    }
    int64_t end = s->count - s->index > s->batch ? s->index + s->batch : s->count;
    for (int64_t cursor = s->index; !rc && cursor < end; ++cursor) {
        int64_t row = named(f, "dynamic_filter_echo") ? s->count - 1 - cursor : cursor;
        for (int64_t col = 0; !rc && col < out->schema.n_children; ++col) {
            const char *name = out->schema.children[col]->name;
            struct ArrowArray *a = out->array.children[col];
            char text[128];
            if (!strcmp(name, "n") || !strcmp(name, "id"))
                rc = ArrowArrayAppendInt(a, row);
            else if (!strcmp(name, "flag"))
                rc = row % 3 == 2 ? ArrowArrayAppendNull(a, 1)
                                  : ArrowArrayAppendInt(a, row % 3 == 0);
            else if (!strcmp(name, "pushed_filters"))
                rc = ArrowArrayAppendString(
                    a,
                    (struct ArrowStringView){(const char *)s->report.data, s->report.size_bytes});
            else if (!strcmp(name, "filtered_cols"))
                rc = ArrowArrayAppendString(
                    a,
                    (struct ArrowStringView){(const char *)s->columns.data, s->columns.size_bytes});
            else if (!strcmp(name, "resolved") || !strcmp(name, "tag_values"))
                rc = ArrowArrayAppendString(
                    a,
                    (struct ArrowStringView){(const char *)s->values.data, s->values.size_bytes});
            else if (!strcmp(name, "has_n") || !strcmp(name, "has_tag"))
                rc = ArrowArrayAppendInt(a, VgiFiltersHasColumn(call->filters, name + 4));
            else if (!strcmp(name, "tags")) {
                for (int i = 0; !rc && i < 2; ++i) {
                    snprintf(text, sizeof(text), "tag_%lld", (long long)((row + i) % 5));
                    rc = ArrowArrayAppendString(a->children[0], ArrowCharView(text));
                }
                if (!rc) rc = ArrowArrayFinishElement(a);
            } else if (!strcmp(name, "score"))
                rc = ArrowArrayAppendDouble(a, (double)row * 1.1);
            else if (named(f, "spatial_filter_example")) {
                int64_t side = (int64_t)ceil(sqrt((double)s->count));
                if (side < 1) side = 1;
                double x = (double)(row % side) / (double)side,
                       y = (double)(row / side) / (double)side;
                if (!strcmp(name, "geom")) {
                    uint8_t wkb[21] = {1, 1, 0, 0, 0};
                    double values[] = {x, y};
                    for (int j = 0; j < 2; ++j) {
                        uint64_t bits;
                        memcpy(&bits, &values[j], 8);
                        for (int i = 0; i < 8; ++i) wkb[5 + j * 8 + i] = (uint8_t)(bits >> (8 * i));
                    }
                    rc = ArrowArrayAppendBytes(a, (struct ArrowBufferView){{wkb}, 21});
                } else
                    rc = ArrowArrayAppendDouble(a, !strcmp(name, "x") ? x : y);
            } else if (named(f, "dict_filter_echo"))
                rc = ArrowArrayAppendInt(a, row % 3);
            else {
                snprintf(text, sizeof(text),
                         !strcmp(name, "tag")    ? "t%lld"
                         : !strcmp(name, "name") ? "item_%lld"
                                                 : "row_%lld",
                         (long long)row);
                rc = ArrowArrayAppendString(a, ArrowCharView(text));
            }
        }
        if (!rc) rc = ArrowArrayFinishElement(&out->array);
    }
    s->index = end;
    if (!rc) rc = VgiBatchFinish(out, &call->rpc->error);
    return rc;
}
