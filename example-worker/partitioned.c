#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "fixtures.h"
#include "vgi/partition.h"

static int named(const struct ExampleFunction *f, const char *name) {
    return !strcmp(f->name, name);
}
static int range_kind(const struct ExampleFunction *f) {
    return named(f, "disjoint_range_partitioned") || named(f, "overlapping_range_partitioned");
}
static int country_kind(const struct ExampleFunction *f) {
    return named(f, "country_partitioned_sales") || named(f, "trailing_partition_sales") ||
           strstr(f->name, "broken_partition") || named(f, "broken_missing_partition_values");
}
static int broken(const struct ExampleFunction *f) { return !strncmp(f->name, "broken_", 7); }
static int partitions(const struct ExampleFunction *f) {
    return country_kind(f) || range_kind(f) || named(f, "region_year_partitioned") ||
           named(f, "partitioned_with_explicit_override");
}
static struct ArrowBufferView scope(struct VgiFunctionCall *call) {
    return (struct ArrowBufferView){{call->execution_id.data}, call->execution_id.size_bytes};
}
int ExamplePartitionSupported(const char *name) {
    const char *names[] = {"partitioned_sequence",
                           "partitioned_preserves_order",
                           "partitioned_no_order_guarantee",
                           "partitioned_fixed_order",
                           "partitioned_batch_index",
                           "partitioned_batch_index_marked",
                           "filter_echo_partitioned",
                           "country_partitioned_sales",
                           "trailing_partition_sales",
                           "region_year_partitioned",
                           "partitioned_with_explicit_override",
                           "disjoint_range_partitioned",
                           "overlapping_range_partitioned",
                           "broken_missing_partition_values",
                           "broken_partition_min_neq_max",
                           "broken_partition_values_no_annotation",
                           "broken_partition_column_absent_from_batch",
                           "broken_missing_batch_index_tag",
                           "broken_non_monotone_batch_index",
                           "broken_batch_index_overflow"};
    for (size_t i = 0; i < sizeof(names) / sizeof(*names); ++i)
        if (!strcmp(name, names[i])) return 1;
    return 0;
}
int ExamplePartitionBind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *out) {
    (void)call;
    struct ExampleFunction *f = data;
    const char *names[4] = {"n"}, *formats[4] = {"l"};
    int n = 1, marked = 0;
    if (country_kind(f)) {
        n = 2;
        names[0] = "country";
        names[1] = "sales";
        formats[0] = "u";
        formats[1] = "l";
        marked = 1;
        if (named(f, "trailing_partition_sales")) {
            n = 4;
            names[0] = "seq";
            names[1] = "label";
            names[2] = "sales";
            names[3] = "country";
            formats[0] = "l";
            formats[1] = "u";
            formats[2] = "l";
            formats[3] = "u";
            marked = 8;
        }
        if (named(f, "broken_partition_values_no_annotation")) marked = 0;
    } else if (range_kind(f)) {
        n = 2;
        names[0] = "key";
        names[1] = "value";
        formats[1] = "l";
        marked = 1;
    } else if (named(f, "region_year_partitioned")) {
        n = 3;
        names[0] = "region";
        names[1] = "year";
        names[2] = "value";
        formats[0] = "u";
        formats[1] = "l";
        formats[2] = "g";
        marked = 3;
    } else if (named(f, "partitioned_with_explicit_override")) {
        n = 2;
        names[0] = "category";
        names[1] = "revenue";
        formats[0] = "u";
        formats[1] = "l";
        marked = 1;
    } else if (named(f, "partitioned_batch_index_marked")) {
        n = 2;
        names[0] = "partition_id";
        names[1] = "seq";
        formats[1] = "l";
    } else if (named(f, "filter_echo_partitioned")) {
        n = 3;
        names[1] = "worker_pid";
        names[2] = "pushed_filters";
        formats[1] = "l";
        formats[2] = "u";
    }
    ArrowSchemaInit(out);
    int rc = ArrowSchemaSetTypeStruct(out, n);
    for (int i = 0; !rc && i < n; ++i) {
        rc = ArrowSchemaSetFormat(out->children[i], formats[i]);
        if (!rc) rc = ArrowSchemaSetName(out->children[i], names[i]);
        if (!rc && (marked & (1 << i))) rc = VgiPartitionField(out->children[i]);
    }
    return rc;
}
struct PartitionState {
    struct ArrowSchema schema;
    int64_t id, start, end, cursor, emitted;
};
static int push(struct VgiFunctionCall *call, int64_t id, int64_t start, int64_t end) {
    unsigned char bytes[24];
    int64_t values[] = {id, start, end};
    for (int i = 0; i < 3; ++i)
        for (int j = 0; j < 8; ++j)
            bytes[8 * i + j] = (unsigned char)((uint64_t)values[i] >> (8 * j));
    return VgiStorageAppend(call->storage, scope(call), "partition.chunks",
                            (struct ArrowBufferView){{bytes}, 24}, NULL, &call->rpc->error);
}
static int pop(struct VgiFunctionCall *call, struct PartitionState *s) {
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    int rc =
        VgiStoragePop(call->storage, scope(call), "partition.chunks", &bytes, &call->rpc->error);
    if (!rc && bytes.size_bytes != 24) rc = EINVAL;
    if (!rc) {
        int64_t *dest[] = {&s->id, &s->start, &s->end};
        for (int i = 0; i < 3; ++i) {
            uint64_t value = 0;
            for (int j = 0; j < 8; ++j) value |= (uint64_t)bytes.data[8 * i + j] << (8 * j);
            *dest[i] = (int64_t)value;
        }
        s->cursor = s->start;
    }
    ArrowBufferReset(&bytes);
    return rc;
}
int64_t ExamplePartitionWorkers(void *data, struct VgiFunctionCall *call) {
    (void)call;
    return broken(data) ? 1 : 4;
}
int ExamplePartitionInit(void *data, struct VgiFunctionCall *call) {
    struct ExampleFunction *f = data;
    struct PartitionState *s = calloc(1, sizeof(*s));
    if (!s) return ENOMEM;
    call->state = s;
    int rc = ExamplePartitionBind(data, call, &s->schema);
    if (rc || VgiBatchBytes(&call->init_request, "execution_id").size_bytes) return rc;
    int64_t count = VgiArgumentInt(call, 0, NULL, 0);
    if (count < 0) count = 0;
    if (broken(f)) {
        s->end = country_kind(f) && count < 1 ? 1 : count;
        return 0;
    }
    int64_t n = 0, rows = count, size = 1000;
    if (partitions(f)) {
        n = country_kind(f)                                  ? 5
            : named(f, "region_year_partitioned")            ? 6
            : named(f, "partitioned_with_explicit_override") ? 3
                                                             : count;
        if (range_kind(f)) rows = VgiArgumentInt(call, -1, "rows_per_partition", 10);
        if (rows < 1) rows = 1;
    } else if (named(f, "partitioned_sequence") || named(f, "filter_echo_partitioned")) {
        size = count / 24 + (count % 24 != 0);
        if (size < 1) size = 1;
    } else if (named(f, "partitioned_batch_index_marked")) {
        size = VgiArgumentInt(call, -1, "chunk_size", 1000);
        if (size < 1) size = 1;
    }
    rc = VgiStorageBegin(call->storage, &call->rpc->error);
    if (!rc) {
        if (partitions(f)) {
            for (int64_t i = 0; !rc && i < n; ++i) rc = push(call, i, 0, rows);
        } else
            for (int64_t start = 0, id = 0; !rc && start < count; ++id) {
                int64_t end = count - start > size ? start + size : count;
                rc = push(call, id, start, end);
                start = end;
            }
    }
    if (!rc) rc = VgiStorageCommit(call->storage, &call->rpc->error);
    if (rc) VgiStorageRollback(call->storage);
    return rc;
}
void ExamplePartitionFree(void *data, struct VgiFunctionCall *call) {
    (void)data;
    struct PartitionState *s = call->state;
    if (s && s->schema.release) ArrowSchemaRelease(&s->schema);
    free(s);
    call->state = NULL;
}
int ExamplePartitionProcess(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                            struct VgiBatch *out, struct ArrowBuffer *metadata, int *finished) {
    (void)input;
    struct ExampleFunction *f = data;
    struct PartitionState *s = call->state;
    if (broken(f)) {
        if (s->emitted && (!named(f, "broken_non_monotone_batch_index") || s->emitted > 1)) {
            *finished = 1;
            return 0;
        }
        if (named(f, "broken_partition_values_no_annotation"))
            return VgiRpcFail(
                call->rpc, "RuntimeError", "INTERNAL", "",
                "EmitPartitioned requires partition-annotated fields, but none were declared");
        if (named(f, "broken_partition_column_absent_from_batch"))
            return VgiRpcFail(call->rpc, "RuntimeError", "INTERNAL", "",
                              "partition column \"country\" is partition-annotated but absent from "
                              "emitted batch");
        if (s->emitted) {
            s->cursor = 0;
            s->end = 1;
        }
    } else
        while (s->cursor >= s->end) {
            int rc = pop(call, s);
            if (rc == ENOENT) {
                *finished = 1;
                return 0;
            }
            if (rc) return rc;
        }
    int64_t end = s->end;
    if (!broken(f) && end - s->cursor > 1000) end = s->cursor + 1000;
    int rc = VgiBatchInit(out, &s->schema, &call->rpc->error);
    struct ArrowBuffer report;
    ArrowBufferInit(&report);
    if (!rc && named(f, "filter_echo_partitioned"))
        rc = VgiFiltersFormat(call->filters, 0, &report, &call->rpc->error);
    const char *countries[] = {"AU", "BR", "CA", "FR", "US"}, *regions[] = {"AMER", "EMEA", "APAC"},
               *categories[] = {"books", "music", "video"};
    for (int64_t row = s->cursor; !rc && row < end; ++row) {
        int64_t offset = row - s->start;
        for (int64_t col = 0; !rc && col < out->schema.n_children; ++col) {
            const char *name = out->schema.children[col]->name;
            struct ArrowArray *a = out->array.children[col];
            int64_t value = row;
            if (!strcmp(name, "country")) {
                rc = ArrowArrayAppendString(
                    a, ArrowCharView(
                           broken(f)
                               ? (named(f, "broken_partition_min_neq_max") && row % 2 ? "BR" : "AU")
                               : countries[s->id]));
                continue;
            }
            if (!strcmp(name, "label")) {
                char text[96];
                snprintf(text, sizeof(text), "%s-%lld", countries[s->id], (long long)offset);
                rc = ArrowArrayAppendString(a, ArrowCharView(text));
                continue;
            }
            if (!strcmp(name, "region")) {
                rc = ArrowArrayAppendString(a, ArrowCharView(regions[s->id / 2]));
                continue;
            }
            if (!strcmp(name, "category")) {
                rc = ArrowArrayAppendString(a, ArrowCharView(categories[s->id]));
                continue;
            }
            if (!strcmp(name, "pushed_filters")) {
                rc = ArrowArrayAppendString(
                    a, (struct ArrowStringView){(const char *)report.data, report.size_bytes});
                continue;
            }
            if (named(f, "region_year_partitioned") && !strcmp(name, "value")) {
                rc = ArrowArrayAppendDouble(a, (double)(s->id * 1000 + offset));
                continue;
            }
            if (!strcmp(name, "worker_pid"))
                value = getpid();
            else if (!strcmp(name, "partition_id"))
                value = s->id;
            else if (!strcmp(name, "seq"))
                value = offset;
            else if (!strcmp(name, "sales") && !broken(f)) {
                if (__builtin_mul_overflow(s->id, INT64_C(1000000), &value) ||
                    __builtin_add_overflow(value, offset, &value)) {
                    rc = EOVERFLOW;
                    break;
                }
            } else if (!strcmp(name, "year"))
                value = 2023 + s->id % 2;
            else if (!strcmp(name, "revenue"))
                value = (s->id + 1) * 100 + offset;
            else if (range_kind(f)) {
                int64_t step = !strcmp(name, "key")
                                   ? (named(f, "disjoint_range_partitioned") ? 1000 : 500)
                                   : 10;
                if (__builtin_mul_overflow(s->id, step, &value) ||
                    __builtin_add_overflow(value, offset, &value)) {
                    rc = EOVERFLOW;
                    break;
                }
            } else if (named(f, "partitioned_sequence") &&
                       __builtin_mul_overflow(row, VgiArgumentInt(call, -1, "increment", 1),
                                              &value)) {
                rc = EOVERFLOW;
                break;
            }
            rc = ArrowArrayAppendInt(a, value);
        }
        if (!rc) rc = ArrowArrayFinishElement(&out->array);
    }
    if (!rc) rc = VgiBatchFinish(out, &call->rpc->error);
    if (!rc && partitions(f) && (!broken(f) || named(f, "broken_partition_min_neq_max"))) {
        if (named(f, "partitioned_with_explicit_override")) {
            struct VgiBatch stated = {0};
            rc = VgiBatchInit(&stated, &s->schema, &call->rpc->error);
            if (!rc) rc = VgiBatchSetString(&stated, "category", categories[s->id]);
            if (!rc) rc = VgiBatchFinishRow(&stated, &call->rpc->error);
            if (!rc) rc = VgiPartitionMetadata(&s->schema, &stated, metadata, &call->rpc->error);
            VgiBatchReset(&stated);
        } else
            rc = VgiPartitionMetadata(&s->schema, out, metadata, &call->rpc->error);
    }
    if (!rc && (strstr(f->name, "batch_index") && !named(f, "broken_missing_batch_index_tag"))) {
        int64_t index = broken(f) ? (named(f, "broken_batch_index_overflow") ? INT64_C(1) << 60
                                     : s->emitted                            ? 3
                                                                             : 10)
                                  : s->id;
        char text[32];
        snprintf(text, sizeof(text), "%lld", (long long)index);
        rc = VgiMetadataSet(metadata, "vgi_batch_index", text);
    }
    s->cursor = end;
    ++s->emitted;
    ArrowBufferReset(&report);
    return rc;
}
int ExamplePartitionPlan(void *data, struct VgiFunctionCall *call, const char *method,
                         struct VgiBatch *result) {
    if (strcmp(method, "table_function_cardinality")) return ENODATA;
    struct ExampleFunction *f = data;
    int64_t count = VgiArgumentInt(call, 0, NULL, 0),
            multiplier = country_kind(f) && !broken(f)                    ? 5
                         : named(f, "region_year_partitioned")            ? 6
                         : named(f, "partitioned_with_explicit_override") ? 3
                         : range_kind(f) ? VgiArgumentInt(call, -1, "rows_per_partition", 10)
                                         : 1;
    if (__builtin_mul_overflow(count, multiplier, &count)) return EOVERFLOW;
    NANOARROW_RETURN_NOT_OK(VgiBatchSetInt(result, "estimate", count));
    return VgiBatchSetInt(result, "max", count);
}
