#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "fixtures.h"
#include "vgi/cache.h"
#include "vgi/partition.h"
#include "vgi/split.h"
static int named(const struct ExampleFunction *f, const char *s) { return !strcmp(f->name, s); }
static uint64_t read64(const unsigned char *p) {
    uint64_t n = 0;
    for (int i = 0; i < 8; ++i) n |= (uint64_t)p[i] << (8 * i);
    return n;
}
static void write64(unsigned char *p, int64_t value) {
    for (int i = 0; i < 8; ++i) p[i] = (unsigned char)((uint64_t)value >> (8 * i));
}
static int64_t boundary(int64_t rows, int64_t i, int64_t count) {
    __extension__ typedef unsigned __int128 UInt128;
    return (int64_t)((UInt128)(uint64_t)rows * (uint64_t)i / (uint64_t)count);
}
int ExampleSplitSupported(const char *name) {
    const char *names[] = {"split_sequence",       "split_empty_ranges",   "split_zero",
                           "split_skewed",         "split_many",           "split_stale_plan",
                           "split_short_ttl",      "split_cacheable",      "split_paginated",
                           "split_batch_index",    "split_partitioned",    "split_fail_at",
                           "split_endless_cursor", "split_dynamic_filter", "split_echo_filters"};
    for (size_t i = 0; i < sizeof(names) / sizeof(*names); ++i)
        if (!strcmp(name, names[i])) return 1;
    return 0;
}
int ExampleSplitBind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *out) {
    (void)call;
    struct ExampleFunction *f = data;
    int n = named(f, "split_echo_filters")                                      ? 3
            : named(f, "split_partitioned") || named(f, "split_dynamic_filter") ? 2
                                                                                : 1;
    ArrowSchemaInit(out);
    int rc = ArrowSchemaSetTypeStruct(out, n);
    for (int i = 0; !rc && i < n; ++i) {
        const char *name = i ? "pushed_filters" : "n", *format = i ? "u" : "l";
        if (named(f, "split_echo_filters")) {
            const char *names[] = {"split_ordinal", "saw_filters", "n_projection"};
            name = names[i];
            format = i == 1 ? "b" : "l";
        } else if (named(f, "split_partitioned")) {
            name = i ? "sales" : "country";
            format = i ? "l" : "u";
            if (!i) rc = VgiPartitionField(out->children[i]);
        }
        if (!rc) rc = ArrowSchemaSetFormat(out->children[i], format);
        if (!rc) rc = ArrowSchemaSetName(out->children[i], name);
        if (!named(f, "split_partitioned")) out->children[i]->flags &= ~ARROW_FLAG_NULLABLE;
    }
    return rc;
}
int ExampleSplitPlan(void *data, struct VgiFunctionCall *call, const char *method,
                     struct VgiBatch *result) {
    struct ExampleFunction *f = data;
    if (!strcmp(method, "table_function_cardinality")) {
        int64_t count = VgiArgumentInt(call, -1, "n", 0);
        NANOARROW_RETURN_NOT_OK(VgiBatchSetInt(result, "estimate", count));
        return VgiBatchSetInt(result, "max", count);
    }
    if (strcmp(method, "table_function_plan")) return ENODATA;
    int64_t rows =
        VgiArgumentInt(call, -1, named(f, "split_partitioned") ? "rows_per_country" : "n", 0);
    if (rows < 0) rows = 0;
    int64_t count = VgiArgumentInt(call, -1, "splits", 1);
    if (count < 1) count = 1;
    if (named(f, "split_partitioned")) count = 4;
    if (count > 1000000) return EOVERFLOW;
    struct ArrowBufferView cursor = VgiBatchBytes(&call->init_request, "cursor");
    int64_t first = 0, last = count, page = 0;
    if (named(f, "split_paginated")) {
        if (cursor.size_bytes == 8) page = (int64_t)read64(cursor.data.as_uint8);
        if (page < 0 || page > INT64_MAX / 4) return EINVAL;
        first = page * 4;
        last = count - first < 4 ? count : first + 4;
    }
    if (named(f, "split_endless_cursor")) last = 1;
    if (named(f, "split_skewed") && rows == 0) last = 1;
    struct VgiColumnBounds bounds = {0};
    int rc = VgiFiltersColumnBounds(call->filters, "split_ordinal", &bounds, &call->rpc->error);
    const struct ArrowArrayView *projection = VgiBatchGet(&call->init_request, "projection_ids");
    int64_t projected = projection && !ArrowArrayViewIsNull(projection, 0)
                            ? ArrowArrayViewListChildOffset(projection, 1) -
                                  ArrowArrayViewListChildOffset(projection, 0)
                            : 0;
    if (projected >= 3) projected = 0;
    struct ArrowBuffer report;
    ArrowBufferInit(&report);
    if (!rc) rc = VgiFiltersFormat(call->filters, 0, &report, &call->rpc->error);
    int has_filters = report.size_bytes != 6 || memcmp(report.data, "(none)", 6);
    ArrowBufferReset(&report);
    for (int64_t i = first; !rc && i < last; ++i) {
        int64_t begin = boundary(rows, i, count), end = boundary(rows, i + 1, count),
                estimated = end - begin;
        int exact = 1;
        if (named(f, "split_zero"))
            begin = end = estimated = 0;
        else if (named(f, "split_empty_ranges")) {
            int64_t real = (count + 1) / 2;
            begin = boundary(rows, (i + 1) / 2, real);
            end = i % 2 ? begin : boundary(rows, i / 2 + 1, real);
            estimated = end - begin;
        } else if (named(f, "split_skewed") && count > 1) {
            int64_t head = rows / 2;
            begin = i ? head + boundary(rows - head, i - 1, count - 1) : 0;
            end = i ? head + boundary(rows - head, i, count - 1) : head;
            estimated = end - begin;
        } else if (named(f, "split_partitioned")) {
            begin = i;
            end = rows;
            estimated = rows;
        } else if (named(f, "split_endless_cursor")) {
            begin = 0;
            end = rows;
            exact = 0;
        } else if (named(f, "split_fail_at"))
            exact = 0;
        else if (named(f, "split_echo_filters")) {
            if ((bounds.has_min && i < bounds.min) || (bounds.has_max && i > bounds.max)) continue;
            begin = i;
            end = has_filters * 1000000 + projected;
            exact = 0;
        }
        unsigned char payload[24];
        int offset = named(f, "split_batch_index") ? 8 : 0;
        if (offset) write64(payload, i);
        write64(payload + offset, begin);
        write64(payload + offset + 8, end);
        struct VgiBatch split = {0};
        rc = VgiBatchInitNamed(&split, "ScanSplit", &call->rpc->error);
        if (!rc) rc = VgiBatchSetBytes(&split, "payload", payload, 16 + offset);
        if (!rc && exact) rc = VgiBatchSetInt(&split, "estimated_rows", estimated);
        if (!rc) rc = VgiBatchSetInt(&split, "rows_exact", exact);
        if (!rc) rc = VgiBatchFinishRow(&split, &call->rpc->error);
        if (!rc) rc = VgiPlanAppendSplit(result, &split, &call->rpc->error);
        VgiBatchReset(&split);
    }
    if (!rc) rc = ArrowArrayFinishElement(VgiBatchColumn(result, "splits"));
    if (!rc &&
        ((named(f, "split_paginated") && last < count) || named(f, "split_endless_cursor"))) {
        struct ArrowArray *list = VgiBatchColumn(result, "next_cursors");
        if (named(f, "split_endless_cursor")) {
            if (cursor.size_bytes > 1024 * 1024)
                rc = EOVERFLOW;
            else {
                struct ArrowBuffer next;
                ArrowBufferInit(&next);
                rc = ArrowBufferResize(&next, cursor.size_bytes + 1, 0);
                if (!rc) {
                    memset(next.data, 'x', (size_t)next.size_bytes);
                    rc = ArrowArrayAppendBytes(
                        list->children[0], (struct ArrowBufferView){{next.data}, next.size_bytes});
                }
                ArrowBufferReset(&next);
            }
        } else {
            unsigned char next[8];
            write64(next, page + 1);
            rc = ArrowArrayAppendBytes(list->children[0], (struct ArrowBufferView){{next}, 8});
        }
        if (!rc) rc = ArrowArrayFinishElement(list);
    }
    if (!rc && !named(f, "split_fail_at") && !named(f, "split_echo_filters") &&
        !named(f, "split_endless_cursor")) {
        int64_t total = rows;
        if (named(f, "split_partitioned") && __builtin_mul_overflow(rows, count, &total))
            rc = EOVERFLOW;
        if (named(f, "split_zero")) total = 0;
        if (!rc) rc = VgiBatchSetInt(result, "estimated_total_rows", total);
        if (!rc) rc = VgiBatchSetInt(result, "estimated_total_splits", count);
    }
    if (!rc && (named(f, "split_sequence") || named(f, "split_stale_plan")))
        rc =
            VgiBatchSetInt(result, "catalog_version", named(f, "split_stale_plan") ? 987654321 : 1);
    return rc;
}
struct SplitRange {
    int64_t ordinal, begin, end;
};
struct SplitState {
    struct SplitRange *ranges;
    int64_t count, at, cursor, batch;
    struct ArrowSchema schema;
};
int ExampleSplitInit(void *data, struct VgiFunctionCall *call) {
    struct ExampleFunction *f = data;
    if (!call->split_payloads.array.release)
        return VgiRpcFail(call->rpc, "RuntimeError", "INVALID_ARGUMENT", "",
                          "table function is split-only: it has no unplanned scan path");
    struct SplitState *s = calloc(1, sizeof(*s));
    if (!s) return ENOMEM;
    call->state = s;
    int rc = ExampleSplitBind(data, call, &s->schema);
    s->count = call->split_payloads.array.length;
    s->ranges = calloc((size_t)s->count, sizeof(*s->ranges));
    if (s->count && !s->ranges) return ENOMEM;
    int64_t fail = VgiArgumentInt(call, -1, "fail_at", -1);
    for (int64_t i = 0; !rc && i < s->count; ++i) {
        struct ArrowStringView bytes = VgiValueString(call->split_payloads.view.children[0], i);
        int offset = named(f, "split_batch_index") ? 8 : 0;
        if (bytes.size_bytes != 16 + offset) {
            rc = EINVAL;
            break;
        }
        const unsigned char *p = (const unsigned char *)bytes.data;
        s->ranges[i].ordinal = offset ? (int64_t)read64(p) : i;
        s->ranges[i].begin = (int64_t)read64(p + offset);
        s->ranges[i].end = (int64_t)read64(p + offset + 8);
        if (s->ranges[i].begin < 0 || s->ranges[i].end < 0) {
            rc = EINVAL;
            break;
        }
        if (named(f, "split_partitioned") && s->ranges[i].begin >= 4) {
            rc = EINVAL;
            break;
        }
        if (named(f, "split_fail_at") && VgiArgumentInt(call, -1, "fail_in_init", 0) &&
            fail >= s->ranges[i].begin && fail < s->ranges[i].end) {
            char message[160];
            snprintf(message, sizeof(message),
                     "split_fail_at refuses to initialize the split covering row %lld",
                     (long long)fail);
            rc = VgiRpcFail(call->rpc, "RuntimeError", "INTERNAL", "", message);
        }
    }
    if (s->count) s->cursor = s->ranges[0].begin;
    return rc;
}
void ExampleSplitFree(void *data, struct VgiFunctionCall *call) {
    (void)data;
    struct SplitState *s = call->state;
    if (s) {
        free(s->ranges);
        if (s->schema.release) ArrowSchemaRelease(&s->schema);
    }
    free(s);
    call->state = NULL;
}
int ExampleSplitProcess(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                        struct VgiBatch *out, struct ArrowBuffer *metadata, int *finished) {
    (void)input;
    struct ExampleFunction *f = data;
    struct SplitState *s = call->state;
    int partition = named(f, "split_partitioned"), echo = named(f, "split_echo_filters");
    if (!partition && !echo)
        while (s->at < s->count && s->cursor >= s->ranges[s->at].end) {
            ++s->at;
            s->batch = 0;
            if (s->at < s->count) s->cursor = s->ranges[s->at].begin;
        }
    if (s->at >= s->count) {
        *finished = 1;
        return 0;
    }
    struct SplitRange *range = &s->ranges[s->at];
    int64_t begin = s->cursor, end = range->end;
    if (partition) {
        begin = 0;
        end = range->end;
    } else if (echo) {
        begin = s->at;
        end = s->count;
    } else {
        int64_t size = named(f, "split_batch_index")      ? 8
                       : named(f, "split_dynamic_filter") ? 4
                                                          : 2048;
        if (end - begin > size) end = begin + size;
    }
    if (named(f, "split_fail_at") && !VgiArgumentInt(call, -1, "fail_in_init", 0)) {
        int64_t fail = VgiArgumentInt(call, -1, "fail_at", -1);
        if (fail >= begin && fail < end) {
            char message[128];
            snprintf(message, sizeof(message), "split_fail_at failed mid-stream at row %lld",
                     (long long)fail);
            return VgiRpcFail(call->rpc, "RuntimeError", "INTERNAL", "", message);
        }
    }
    int rc = VgiBatchInit(out, &s->schema, &call->rpc->error);
    struct ArrowBuffer report;
    ArrowBufferInit(&report);
    if (!rc && named(f, "split_dynamic_filter")) {
        struct VgiColumnBounds bounds = {0};
        rc = VgiFiltersColumnBounds(call->filters, "n", &bounds, &call->rpc->error);
        char text[96];
        if (!rc && bounds.has_min) {
            snprintf(text, sizeof(text), "n>=%lld", (long long)bounds.min);
            rc = ArrowBufferAppend(&report, text, strlen(text));
        }
        if (!rc && bounds.has_max) {
            snprintf(text, sizeof(text), "%sn<=%lld", bounds.has_min ? "," : "",
                     (long long)bounds.max);
            rc = ArrowBufferAppend(&report, text, strlen(text));
        }
        if (!rc && !report.size_bytes) rc = ArrowBufferAppend(&report, "(none)", 6);
    }
    const char *countries[] = {"US", "DE", "JP", "BR"};
    for (int64_t row = begin; !rc && row < end; ++row) {
        for (int64_t col = 0; !rc && col < out->schema.n_children; ++col) {
            const char *name = out->schema.children[col]->name;
            struct ArrowArray *a = out->array.children[col];
            if (partition)
                rc = !strcmp(name, "country")
                         ? ArrowArrayAppendString(a, ArrowCharView(countries[range->begin]))
                         : ArrowArrayAppendInt(a, range->begin * 100 + row + 1);
            else if (echo) {
                struct SplitRange *value = &s->ranges[row];
                rc = ArrowArrayAppendInt(a, !strcmp(name, "split_ordinal") ? value->begin
                                            : !strcmp(name, "saw_filters") ? value->end >= 1000000
                                                                           : value->end % 1000000);
            } else if (!strcmp(name, "pushed_filters"))
                rc = ArrowArrayAppendString(
                    a, (struct ArrowStringView){(const char *)report.data, report.size_bytes});
            else
                rc = ArrowArrayAppendInt(a, row);
        }
        if (!rc) rc = ArrowArrayFinishElement(&out->array);
    }
    if (!rc) rc = VgiBatchFinish(out, &call->rpc->error);
    if (!rc && partition) rc = VgiPartitionMetadata(&s->schema, out, metadata, &call->rpc->error);
    if (!rc && named(f, "split_batch_index")) {
        char text[64];
        snprintf(text, sizeof(text), "%lld", (long long)(range->ordinal * 1000 + s->batch++));
        rc = VgiMetadataSet(metadata, "vgi_batch_index", text);
    }
    if (!rc && named(f, "split_cacheable")) {
        struct VgiCacheControl c = {.has_ttl = 1, .ttl_seconds = 300};
        rc = VgiCacheControlMetadata(&c, metadata);
    }
    if (echo)
        s->at = s->count;
    else if (partition)
        ++s->at;
    else
        s->cursor = end;
    ArrowBufferReset(&report);
    return rc;
}
