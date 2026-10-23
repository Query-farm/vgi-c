#include <errno.h>
#include <inttypes.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

#include "fixtures.h"
#include "vgi/context.h"

static int named(const struct ExampleFunction *f, const char *name) {
    return !strcmp(f->name, name);
}
int ExampleAggregateSupported(const char *name) {
    if (!strcmp(name, "nest_tensor")) return 1;
    const char *names[] = {"vgi_sum",           "vgi_count",          "vgi_avg",
                           "vgi_weighted_sum",  "vgi_generic_sum",    "vgi_sum_all",
                           "vgi_listagg",       "global_agg",         "test_same_name_agg",
                           "vgi_percentile",    "vgi_window_sum",     "vgi_window_sum_batch",
                           "vgi_window_median", "vgi_window_listagg", "vgi_streaming_sum",
                           "secret_typed_sum"};
    for (size_t i = 0; i < sizeof(names) / sizeof(*names); ++i)
        if (!strcmp(name, names[i])) return 1;
    return 0;
}
static int listagg(const struct ExampleFunction *f) {
    return named(f, "vgi_listagg") || named(f, "vgi_window_listagg");
}
static int vector_state(const struct ExampleFunction *f) {
    return named(f, "vgi_percentile") || named(f, "vgi_window_median");
}
static int floating(const struct ExampleFunction *f) {
    return named(f, "vgi_avg") || named(f, "vgi_weighted_sum") || named(f, "vgi_generic_sum") ||
           named(f, "vgi_sum_all") || vector_state(f) || named(f, "secret_typed_sum");
}
int ExampleAggregateBind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *out) {
    struct ExampleFunction *f = data;
    if (named(f, "nest_tensor")) return ExampleNestBind(call, out);
    if (named(f, "vgi_percentile")) {
        const struct ArrowArrayView *constant = VgiArgument(call, 1, "percentile");
        double value = VgiArgumentDouble(call, 1, "percentile", .5);
        if (constant && constant->length && ArrowArrayViewIsNull(constant, 0))
            return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "",
                              "vgi_percentile: percentile must not be NULL");
        if (!isfinite(value))
            return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "",
                              "vgi_percentile: percentile must be a finite number");
        if (value < 0 || value > 1)
            return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "",
                              "vgi_percentile: percentile must be in [0, 1]");
    }
    if (named(f, "vgi_sum_all") && !call->input_schema.n_children)
        return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "",
                          "vgi_sum_all requires at least 1 value");
    ArrowSchemaInit(out);
    int rc = ArrowSchemaSetTypeStruct(out, 1);
    if (!rc && named(f, "vgi_generic_sum") && call->input_schema.n_children) {
        ArrowSchemaRelease(out->children[0]);
        rc = ArrowSchemaDeepCopy(call->input_schema.children[0], out->children[0]);
    } else if (!rc)
        rc = ArrowSchemaSetType(out->children[0], listagg(f) || named(f, "test_same_name_agg")
                                                      ? NANOARROW_TYPE_STRING
                                                  : floating(f) ? NANOARROW_TYPE_DOUBLE
                                                                : NANOARROW_TYPE_INT64);
    if (!rc && named(f, "secret_typed_sum")) {
        const struct ArrowSchema *schema = NULL;
        const struct ArrowArrayView *secret = VgiSecretSelect(call, "vgi_example", NULL, &schema);
        const struct ArrowArrayView *ssl = VgiStructField(secret, schema, "use_ssl");
        rc = ArrowSchemaSetType(out->children[0], ssl && !ArrowArrayViewIsNull(ssl, 0) &&
                                                          ArrowArrayViewGetIntUnsafe(ssl, 0)
                                                      ? NANOARROW_TYPE_DOUBLE
                                                      : NANOARROW_TYPE_INT64);
    }
    if (!rc) rc = ArrowSchemaSetName(out->children[0], "result");
    return rc;
}
static uint64_t get_u64(struct ArrowBufferView value, int offset) {
    uint64_t out = 0;
    for (int i = 0; i < 8 && offset + i < value.size_bytes; ++i)
        out |= (uint64_t)value.data.as_uint8[offset + i] << (i * 8);
    return out;
}
static double get_double(struct ArrowBufferView value) {
    uint64_t bits = get_u64(value, 0);
    double out;
    memcpy(&out, &bits, sizeof(out));
    return out;
}
static int set_state(struct ArrowBuffer *state, uint64_t total, uint64_t count) {
    NANOARROW_RETURN_NOT_OK(ArrowBufferResize(state, 16, 0));
    for (int i = 0; i < 8; ++i) {
        state->data[i] = (uint8_t)(total >> (i * 8));
        state->data[i + 8] = (uint8_t)(count >> (i * 8));
    }
    return 0;
}
static struct ArrowBufferView view(const struct ArrowBuffer *b) {
    return (struct ArrowBufferView){{b->data}, b->size_bytes};
}
int ExampleAggregateUpdate(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                           int64_t row, struct ArrowBuffer *state, int *present) {
    struct ExampleFunction *f = data;
    if (named(f, "nest_tensor")) return ExampleNestUpdate(call, input, row, state, present);
    const struct ArrowArrayView *first = NULL, *second = NULL;
    const struct ArrowSchema *first_schema = NULL, *second_schema = NULL;
    for (int64_t i = 0; i < input->view.n_children; ++i) {
        if (!strcmp(input->schema.children[i]->name, "__vgi_group_id")) continue;
        if (!first) {
            first = input->view.children[i];
            first_schema = input->schema.children[i];
        } else if (!second) {
            second = input->view.children[i];
            second_schema = input->schema.children[i];
        }
    }
    if (named(f, "vgi_count")) {
        *present = 1;
        return set_state(state, get_u64(view(state), 0) + 1, 0);
    }
    if (!first) return EINVAL;
    if (listagg(f)) {
        struct ArrowStringView value = VgiValueString(first, row);
        if (!value.data) return 0;
        if (state->size_bytes) NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(state, ",", 1));
        *present = 1;
        return ArrowBufferAppend(state, value.data, value.size_bytes);
    }
    if (!named(f, "vgi_sum_all") && ArrowArrayViewIsNull(first, row)) return 0;
    if (vector_state(f)) {
        double value;
        NANOARROW_RETURN_NOT_OK(
            VgiValueNumber(first, first_schema, row, &value, &call->rpc->error));
        *present = 1;
        return ArrowBufferAppend(state, &value, sizeof(value));
    }
    if (named(f, "vgi_weighted_sum") && (!second || ArrowArrayViewIsNull(second, row))) return 0;
    uint64_t count = get_u64(view(state), 8) + 1, total;
    if (floating(f)) {
        double value = 0;
        if (named(f, "vgi_sum_all")) {
            for (int64_t i = 0; i < input->view.n_children; ++i)
                if (strcmp(input->schema.children[i]->name, "__vgi_group_id") &&
                    !ArrowArrayViewIsNull(input->view.children[i], row)) {
                    double column;
                    NANOARROW_RETURN_NOT_OK(VgiValueNumber(input->view.children[i],
                                                           input->schema.children[i], row, &column,
                                                           &call->rpc->error));
                    value += column;
                }
        } else
            NANOARROW_RETURN_NOT_OK(
                VgiValueNumber(first, first_schema, row, &value, &call->rpc->error));
        if (named(f, "vgi_weighted_sum")) {
            double weight;
            NANOARROW_RETURN_NOT_OK(
                VgiValueNumber(second, second_schema, row, &weight, &call->rpc->error));
            value *= weight;
        }
        value += get_double(view(state));
        memcpy(&total, &value, sizeof(total));
    } else
        total = get_u64(view(state), 0) + (uint64_t)ArrowArrayViewGetIntUnsafe(first, row);
    *present = 1;
    return set_state(state, total, count);
}
int ExampleAggregateCombine(void *data, struct VgiFunctionCall *call, struct ArrowBuffer *target,
                            struct ArrowBufferView source) {
    (void)call;
    struct ExampleFunction *f = data;
    if (named(f, "nest_tensor"))
        return ArrowBufferAppend(target, source.data.data, source.size_bytes);
    if (vector_state(f)) return ArrowBufferAppend(target, source.data.data, source.size_bytes);
    if (listagg(f)) {
        if (target->size_bytes && source.size_bytes)
            NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(target, ",", 1));
        return ArrowBufferAppend(target, source.data.data, source.size_bytes);
    }
    uint64_t total;
    if (floating(f)) {
        double value = get_double(view(target)) + get_double(source);
        memcpy(&total, &value, sizeof(total));
    } else
        total = get_u64(view(target), 0) + get_u64(source, 0);
    return set_state(target, total, get_u64(view(target), 8) + get_u64(source, 8));
}
static int compare_double(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}
int ExampleAggregateFinalize(void *data, struct VgiFunctionCall *call,
                             const struct ArrowBufferView *state, struct VgiBatch *output) {
    (void)call;
    struct ExampleFunction *f = data;
    if (named(f, "nest_tensor")) return ExampleNestFinalize(call, state, output);
    struct ArrowArray *out = output->array.children[0];
    if (named(f, "test_same_name_agg")) {
        char label[320];
        snprintf(label, sizeof(label), "%s:%" PRId64, f->path,
                 state ? (int64_t)get_u64(*state, 0) : 0);
        return ArrowArrayAppendString(out, ArrowCharView(label));
    }
    if (named(f, "vgi_count"))
        return ArrowArrayAppendInt(out, state ? (int64_t)get_u64(*state, 0) : 0);
    if (!state && named(f, "vgi_streaming_sum")) return ArrowArrayAppendInt(out, 0);
    if (!state || (listagg(f) && !state->size_bytes)) return ArrowArrayAppendNull(out, 1);
    if (vector_state(f)) {
        size_t count = (size_t)state->size_bytes / sizeof(double);
        if (!count) return ArrowArrayAppendNull(out, 1);
        double *values = malloc(count * sizeof(double));
        if (!values) return ENOMEM;
        memcpy(values, state->data.data, count * sizeof(double));
        qsort(values, count, sizeof(double), compare_double);
        if (named(f, "vgi_window_median")) {
            double median =
                count % 2 ? values[count / 2] : (values[count / 2 - 1] + values[count / 2]) / 2;
            int rc = ArrowArrayAppendDouble(out, median);
            free(values);
            return rc;
        }
        size_t index = (size_t)(VgiArgumentDouble(call, 1, "percentile", .5) * (double)count);
        int rc = ArrowArrayAppendDouble(out, values[index < count ? index : count - 1]);
        free(values);
        return rc;
    }
    if (listagg(f))
        return ArrowArrayAppendString(
            out, (struct ArrowStringView){state->data.as_char, state->size_bytes});
    if (floating(f)) {
        double value = get_double(*state);
        if (named(f, "vgi_avg")) {
            uint64_t count = get_u64(*state, 8);
            if (!count) return ArrowArrayAppendNull(out, 1);
            value /= (double)count;
        }
        struct ArrowSchemaView type;
        NANOARROW_RETURN_NOT_OK(
            ArrowSchemaViewInit(&type, output->schema.children[0], &call->rpc->error));
        if (type.type == NANOARROW_TYPE_FLOAT || type.type == NANOARROW_TYPE_DOUBLE)
            return ArrowArrayAppendDouble(out, value);
        return ArrowArrayAppendInt(out, (int64_t)value);
    }
    return ArrowArrayAppendInt(out, (int64_t)get_u64(*state, 0));
}
