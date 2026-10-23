#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>

#include "fixtures.h"
#include "vgi/statistics.h"

static int named(const struct ExampleFunction *f, const char *name) {
    return !strcmp(f->name, name);
}
int ExampleTableSupported(const char *name) {
    if (ExampleCancellableSupported(name)) return 1;
    if (!strcmp(name, "echo_attach_options") || !strcmp(name, "ticket_probe")) return 1;
    if (ExampleAccumulateSupported(name) || ExampleCopyReader(name) ||
        ExampleStaticSupported(name) || ExamplePartitionSupported(name) ||
        ExampleSplitSupported(name) || ExampleFilterSupported(name) ||
        ExampleTableExtraSupported(name) || ExampleCacheSupported(name) ||
        ExampleBlendedSupported(name) || ExampleContextSupported(name) ||
        ExamplePagedSupported(name))
        return 1;
    const char *names[] = {"sequence",
                           "double_sequence",
                           "make_series",
                           "make_pairs",
                           "constant_columns",
                           "repeat_value",
                           "nested_sequence",
                           "projected_data",
                           "ten_thousand",
                           "generator_exception",
                           "global_table",
                           "test_same_name_table_scan",
                           "echo",
                           "echo_witness",
                           "repeat_inputs",
                           "test_same_name_transform",
                           "row_sum",
                           "blended_drop"};
    for (size_t i = 0; i < sizeof(names) / sizeof(*names); ++i)
        if (!strcmp(names[i], name)) return 1;
    return 0;
}
int ExampleTableIsExchange(const char *name) {
    return !strcmp(name, "slow_cancellable_inout") || ExampleCacheExchange(name) ||
           !strcmp(name, "filter_by_setting") || !strcmp(name, "secret_in_out") ||
           ExampleBlendedSupported(name) || ExamplePagedSupported(name) || !strcmp(name, "echo") ||
           !strcmp(name, "echo_witness") || !strcmp(name, "repeat_inputs") ||
           !strcmp(name, "test_same_name_transform") || !strcmp(name, "row_sum") ||
           !strcmp(name, "blended_drop");
}
static const struct ArrowSchema *argument_schema(const struct VgiFunctionCall *call,
                                                 int64_t position) {
    const struct ArrowSchema *schema = &call->arguments.schema;
    if (schema->n_children == 1 && !strcmp(schema->children[0]->name, "args"))
        schema = schema->children[0];
    char key[64];
    snprintf(key, sizeof(key), "positional_%lld", (long long)position);
    int64_t id = VgiFieldIndex(schema, key);
    return id >= 0 ? schema->children[id] : NULL;
}
static int64_t positional_count(const struct VgiFunctionCall *call) {
    int64_t n = 0;
    while (argument_schema(call, n)) ++n;
    return n;
}
int ExampleTableBind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *out) {
    struct ExampleFunction *f = data;
    if (ExampleCancellableSupported(f->name)) return ExampleCancellableBind(data, call, out);
    if (named(f, "ticket_probe")) {
        ArrowSchemaInit(out);
        int rc = ArrowSchemaSetTypeStruct(out, 2);
        for (int i = 0; !rc && i < 2; ++i) {
            rc = ArrowSchemaSetType(out->children[i], NANOARROW_TYPE_STRING);
            if (!rc) rc = ArrowSchemaSetName(out->children[i], i ? "api_key_sha256" : "region");
        }
        return rc;
    }
    if (named(f, "echo_attach_options")) {
        if (!call->attach_options.schema.release)
            return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "",
                              "echo_attach_options: this attachment carries no options");
        return ArrowSchemaDeepCopy(&call->attach_options.schema, out);
    }
    if (ExampleCopyReader(f->name)) return ExampleCopyBind(data, call, out);
    if (ExampleStaticSupported(f->name)) return ExampleStaticBind(data, call, out);
    if (ExamplePartitionSupported(f->name)) return ExamplePartitionBind(data, call, out);
    if (ExampleSplitSupported(f->name)) return ExampleSplitBind(data, call, out);
    if (ExampleFilterSupported(f->name)) return ExampleFilterBind(data, call, out);
    if (ExampleTableExtraSupported(f->name)) return ExampleTableExtraBind(data, call, out);
    if (ExampleCacheSupported(f->name)) return ExampleCacheBind(data, call, out);
    if (ExampleContextSupported(f->name)) return ExampleContextBind(data, call, out);
    if (ExampleBlendedSupported(f->name)) return ExampleBlendedBind(data, call, out);
    if (named(f, "sequence")) {
        const struct ArrowArrayView *values[] = {VgiArgument(call, 0, NULL),
                                                 VgiArgument(call, -1, "batch_size"),
                                                 VgiArgument(call, -1, "increment")};
        for (int i = 0; i < 3; ++i)
            if (values[i] && ArrowArrayViewIsNull(values[i], 0))
                return VgiRpcFail(call->rpc, "ArgumentValidationError", "INVALID_ARGUMENT", "",
                                  "sequence argument cannot be NULL");
        if (VgiArgumentInt(call, 0, NULL, 0) < 0)
            return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "",
                              "sequence: count must be >= 0");
        if (VgiArgumentInt(call, -1, "batch_size", 1000) < 1)
            return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "",
                              "sequence: batch_size must be >= 1");
        if (VgiArgumentInt(call, -1, "increment", 1) < 1)
            return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "",
                              "sequence: increment must be >= 1");
    }
    if (named(f, "make_series") && VgiArgumentInt(call, 0, NULL, 0) < 0)
        return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "", "count must be >= 0");
    if (named(f, "repeat_inputs") && VgiArgumentInt(call, 0, NULL, 1) < 1)
        return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "",
                          "Repeat count must be at least 1");
    if (named(f, "echo") || named(f, "echo_witness") || named(f, "repeat_inputs")) {
        if (!call->input_schema.release)
            return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "",
                              "input_schema is required but was None");
        return ArrowSchemaDeepCopy(&call->input_schema, out);
    }
    if (named(f, "constant_columns") || named(f, "repeat_value")) {
        int64_t count = positional_count(call) - 1;
        if (count < 0) count = 0;
        ArrowSchemaInit(out);
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeStruct(out, count));
        for (int64_t i = 0; i < count; ++i) {
            const struct ArrowSchema *source = named(f, "constant_columns")
                                                   ? argument_schema(call, i + 1)
                                                   : f->arguments.children[1];
            ArrowSchemaRelease(out->children[i]);
            NANOARROW_RETURN_NOT_OK(ArrowSchemaDeepCopy(source, out->children[i]));
            char name[64];
            snprintf(name, sizeof(name), named(f, "constant_columns") ? "col_%lld" : "v%lld",
                     (long long)i);
            NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(out->children[i], name));
            NANOARROW_RETURN_NOT_OK(ArrowSchemaSetMetadata(
                out->children[i], named(f, "constant_columns") ? source->metadata : NULL));
        }
        return 0;
    }
    if (named(f, "sequence") || named(f, "double_sequence") || named(f, "generator_exception") ||
        named(f, "ten_thousand") || named(f, "make_series") || named(f, "make_pairs") ||
        named(f, "projected_data") || named(f, "nested_sequence") || named(f, "global_table") ||
        named(f, "test_same_name_table_scan") || named(f, "test_same_name_transform") ||
        named(f, "row_sum") || named(f, "blended_drop")) {
        int64_t count = named(f, "projected_data")                             ? 4
                        : named(f, "nested_sequence")                          ? 3
                        : (named(f, "make_pairs") || named(f, "global_table")) ? 2
                                                                               : 1;
        ArrowSchemaInit(out);
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeStruct(out, count));
        for (int64_t i = 0; i < count; ++i) {
            const char *name = "n";
            enum ArrowType type = NANOARROW_TYPE_INT64;
            if (named(f, "double_sequence")) type = NANOARROW_TYPE_DOUBLE;
            if (named(f, "make_series")) {
                name = "value";
                if (f->arguments.n_children == 1 && !strcmp(f->arguments.children[0]->format, "g"))
                    type = NANOARROW_TYPE_DOUBLE;
            }
            if (named(f, "make_pairs")) {
                name = i ? "b" : "a";
                if (!strcmp(f->arguments.children[i]->format, "u")) type = NANOARROW_TYPE_STRING;
            }
            if (named(f, "projected_data")) {
                const char *names[] = {"id", "name", "value", "extra"};
                name = names[i];
                type = i == 1   ? NANOARROW_TYPE_STRING
                       : i == 2 ? NANOARROW_TYPE_DOUBLE
                                : NANOARROW_TYPE_INT64;
            }
            if (named(f, "global_table") && i) {
                name = "label";
                type = NANOARROW_TYPE_STRING;
            }
            if (named(f, "test_same_name_table_scan") || named(f, "test_same_name_transform")) {
                name = "tag";
                type = NANOARROW_TYPE_STRING;
            }
            if (named(f, "blended_drop")) name = "v";
            if (named(f, "row_sum")) {
                name = "row_sum";
                type = NANOARROW_TYPE_DOUBLE;
            }
            if (named(f, "nested_sequence") && i == 1) {
                name = "metadata";
                NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeStruct(out->children[i], 2));
                NANOARROW_RETURN_NOT_OK(
                    ArrowSchemaSetType(out->children[i]->children[0], NANOARROW_TYPE_INT64));
                NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(out->children[i]->children[0], "index"));
                NANOARROW_RETURN_NOT_OK(
                    ArrowSchemaSetType(out->children[i]->children[1], NANOARROW_TYPE_STRING));
                NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(out->children[i]->children[1], "label"));
            } else if (named(f, "nested_sequence") && i == 2) {
                name = "history";
                NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(out->children[i], NANOARROW_TYPE_LIST));
                NANOARROW_RETURN_NOT_OK(
                    ArrowSchemaSetType(out->children[i]->children[0], NANOARROW_TYPE_INT64));
            } else
                NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(out->children[i], type));
            NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(out->children[i], name));
        }
        return 0;
    }
    return ArrowSchemaDeepCopy(&f->output, out);
}
struct TableState {
    int64_t index, count, batch, start, step;
    double increment;
    struct ArrowSchema schema;
};
int ExampleTableInit(void *data, struct VgiFunctionCall *call) {
    struct ExampleFunction *f = data;
    if (ExampleCancellableSupported(f->name)) return ExampleCancellableInit(data, call);
    if (ExampleCopyReader(f->name)) return ExampleCopyInit(data, call);
    if (ExampleStaticSupported(f->name)) return ExampleStaticInit(data, call);
    if (ExamplePartitionSupported(f->name)) return ExamplePartitionInit(data, call);
    if (ExampleSplitSupported(f->name)) return ExampleSplitInit(data, call);
    if (ExampleFilterSupported(f->name)) return ExampleFilterInit(data, call);
    if (ExampleTableExtraSupported(f->name)) return ExampleTableExtraInit(data, call);
    if (ExampleCacheSupported(f->name)) return ExampleCacheInit(data, call);
    if (ExampleContextSupported(f->name)) {
        call->state = calloc(1, sizeof(int));
        return call->state ? 0 : ENOMEM;
    }
    if (ExampleTableIsExchange(f->name)) return 0;
    struct TableState *s = calloc(1, sizeof(*s));
    if (!s) return ENOMEM;
    call->state = s;
    NANOARROW_RETURN_NOT_OK(ExampleTableBind(data, call, &s->schema));
    s->count = VgiArgumentInt(call, 0, NULL, 0);
    if (s->count < 0) s->count = 0;
    s->batch = VgiArgumentInt(call, -1, "batch_size", 1000);
    if (s->batch < 1) s->batch = 1;
    s->increment = VgiArgumentDouble(call, -1, "increment", 1);
    s->step = 1;
    if (named(f, "ten_thousand")) s->count = 10000;
    if (named(f, "test_same_name_table_scan")) s->count = 1;
    if (named(f, "echo_attach_options") || named(f, "ticket_probe")) s->count = 1;
    if (named(f, "global_table")) s->count = 3;
    if (named(f, "generator_exception")) s->batch = 1;
    if (named(f, "make_series")) {
        int64_t n = positional_count(call);
        if (n > 1) {
            s->start = VgiArgumentInt(call, 0, NULL, 0);
            int64_t stop = VgiArgumentInt(call, 1, NULL, 0);
            s->step = n > 2 ? VgiArgumentInt(call, 2, NULL, 1) : 1;
            if (s->step < 1)
                return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "",
                                  "step must be >= 1");
            if (stop > s->start) {
                uint64_t span = (uint64_t)stop - (uint64_t)s->start;
                s->count = (int64_t)(span / (uint64_t)s->step + (span % (uint64_t)s->step != 0));
            } else
                s->count = 0;
        } else if (f->arguments.n_children && !strcmp(f->arguments.children[0]->format, "g")) {
            s->count = 10;
            s->increment = VgiArgumentDouble(call, 0, NULL, 1);
        } else if (VgiArgumentString(call, 0, NULL).data)
            s->count = 1;
    }
    if (named(f, "make_pairs")) {
        if (!strcmp(f->arguments.children[0]->format, "u") ||
            !strcmp(f->arguments.children[1]->format, "u"))
            s->count = 5;
        else {
            s->start = VgiArgumentInt(call, 0, NULL, 0);
            int64_t stop = VgiArgumentInt(call, 1, NULL, 0);
            s->count = stop > s->start ? stop - s->start : 0;
        }
    }
    return 0;
}
void ExampleTableFinalize(void *data, struct VgiFunctionCall *call) {
    if (ExampleCancellableSupported(((struct ExampleFunction *)data)->name)) {
        ExampleCancellableFinalize(data, call);
        return;
    }
    if (ExampleCopyReader(((struct ExampleFunction *)data)->name)) {
        ExampleCopyFree(data, call);
        return;
    }
    if (ExampleStaticSupported(((struct ExampleFunction *)data)->name)) {
        ExampleStaticFree(data, call);
        return;
    }
    if (ExamplePartitionSupported(((struct ExampleFunction *)data)->name)) {
        ExamplePartitionFree(data, call);
        return;
    }
    if (ExampleSplitSupported(((struct ExampleFunction *)data)->name)) {
        ExampleSplitFree(data, call);
        return;
    }
    if (ExampleFilterSupported(((struct ExampleFunction *)data)->name)) {
        ExampleFilterFree(data, call);
        return;
    }
    if (ExampleTableExtraSupported(((struct ExampleFunction *)data)->name)) {
        ExampleTableExtraFree(data, call);
        return;
    }
    if (ExampleCacheSupported(((struct ExampleFunction *)data)->name)) {
        ExampleCacheFree(data, call);
        return;
    }
    if (ExampleContextSupported(((struct ExampleFunction *)data)->name)) {
        free(call->state);
        call->state = NULL;
        return;
    }
    struct TableState *s = call->state;
    if (s && s->schema.release) ArrowSchemaRelease(&s->schema);
    free(s);
    call->state = NULL;
}
static int producer_value(struct ExampleFunction *f, struct VgiFunctionCall *call,
                          struct TableState *state, const struct ArrowSchema *schema,
                          struct ArrowArray *out, int64_t row, int64_t column) {
    const char *name = schema->name;
    char text[512];
    if (named(f, "constant_columns") || named(f, "repeat_value")) {
        int64_t index = strtoll(name + (named(f, "constant_columns") ? 4 : 1), NULL, 10) + 1;
        return VgiArrayAppendValue(out, schema, VgiArgument(call, index, NULL),
                                   argument_schema(call, index), 0, &call->rpc->error);
    }
    if (named(f, "test_same_name_table_scan"))
        return ArrowArrayAppendString(out, ArrowCharView(f->path));
    if (named(f, "global_table") && !strcmp(name, "label")) {
        snprintf(text, sizeof(text), "global_table:%lld", (long long)row);
        return ArrowArrayAppendString(out, ArrowCharView(text));
    }
    if (named(f, "projected_data")) {
        if (!strcmp(name, "name")) {
            snprintf(text, sizeof(text), "item_%lld", (long long)row);
            return ArrowArrayAppendString(out, ArrowCharView(text));
        }
        if (!strcmp(name, "value")) return ArrowArrayAppendDouble(out, (double)row * 1.5);
        return ArrowArrayAppendInt(
            out, !strcmp(name, "extra") ? (int64_t)((uint64_t)row * (uint64_t)row) : row);
    }
    if (named(f, "nested_sequence")) {
        if (!strcmp(name, "metadata")) {
            for (int64_t i = 0; i < schema->n_children; ++i) {
                int rc;
                if (!strcmp(schema->children[i]->name, "label")) {
                    snprintf(text, sizeof(text), "row_%lld", (long long)row);
                    rc = ArrowArrayAppendString(out->children[i], ArrowCharView(text));
                } else
                    rc = ArrowArrayAppendInt(out->children[i], row);
                if (rc) return rc;
            }
            return ArrowArrayFinishElement(out);
        }
        if (!strcmp(name, "history")) {
            int64_t history = VgiArgumentInt(call, -1, "history_size", 20);
            if (history < 1) history = 1;
            int64_t start = row >= history ? row - history + 1 : 0;
            for (int64_t i = start; i <= row; ++i)
                NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(out->children[0], i));
            return ArrowArrayFinishElement(out);
        }
    }
    if (named(f, "make_pairs")) {
        int64_t i = !strcmp(name, "b");
        if (!strcmp(schema->format, "u")) {
            struct ArrowStringView value = VgiArgumentString(call, i, NULL);
            struct ArrowBuffer bytes;
            ArrowBufferInit(&bytes);
            int rc = ArrowBufferAppend(&bytes, value.data, value.size_bytes);
            int n = snprintf(text, sizeof(text), "%lld", (long long)row);
            if (!rc) rc = ArrowBufferAppend(&bytes, text, n);
            if (!rc)
                rc = ArrowArrayAppendString(
                    out, (struct ArrowStringView){(const char *)bytes.data, bytes.size_bytes});
            ArrowBufferReset(&bytes);
            return rc;
        }
        return ArrowArrayAppendInt(
            out, ((!strcmp(f->arguments.children[1]->format, "u") ? VgiArgumentInt(call, 0, NULL, 0)
                                                                  : state->start) +
                  row) *
                     (i ? 2 : 1));
    }
    if (!strcmp(schema->format, "g"))
        return ArrowArrayAppendDouble(out, (double)row * state->increment);
    (void)column;
    return ArrowArrayAppendInt(
        out, named(f, "sequence")
                 ? (int64_t)((uint64_t)row * (uint64_t)VgiArgumentInt(call, -1, "increment", 1))
                 : (int64_t)((uint64_t)state->start + (uint64_t)row * (uint64_t)state->step));
}
static int exchange(struct ExampleFunction *f, struct VgiFunctionCall *call,
                    const struct VgiBatch *input, struct VgiBatch *out) {
    NANOARROW_RETURN_NOT_OK(VgiBatchInit(out, &call->output_schema, &call->rpc->error));
    int64_t repeats = named(f, "repeat_inputs") ? VgiArgumentInt(call, 0, NULL, 1) : 1;
    if (named(f, "blended_drop")) repeats = 0;
    for (int64_t repeat = 0; repeat < repeats; ++repeat)
        for (int64_t row = 0; row < input->array.length; ++row) {
            if (named(f, "echo") || named(f, "repeat_inputs"))
                NANOARROW_RETURN_NOT_OK(VgiArrayAppendValue(&out->array, &out->schema, &input->view,
                                                            &input->schema, row,
                                                            &call->rpc->error));
            else {
                for (int64_t i = 0; i < out->schema.n_children; ++i) {
                    if (named(f, "echo_witness")) {
                        struct ArrowSchemaView type;
                        NANOARROW_RETURN_NOT_OK(
                            ArrowSchemaViewInit(&type, out->schema.children[i], &call->rpc->error));
                        if (type.type == NANOARROW_TYPE_STRING) {
                            char n[32];
                            snprintf(n, sizeof(n), "%lld", (long long)out->schema.n_children);
                            NANOARROW_RETURN_NOT_OK(
                                ArrowArrayAppendString(out->array.children[i], ArrowCharView(n)));
                        } else
                            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(out->array.children[i],
                                                                        out->schema.n_children));
                    } else if (named(f, "test_same_name_transform")) {
                        if (ArrowArrayViewIsNull(input->view.children[0], row)) {
                            NANOARROW_RETURN_NOT_OK(
                                ArrowArrayAppendNull(out->array.children[i], 1));
                            continue;
                        }
                        struct ArrowStringView value = VgiValueString(input->view.children[0], row);
                        char number[64];
                        if (!value.data) {
                            snprintf(number, sizeof(number), "%lld",
                                     (long long)ArrowArrayViewGetIntUnsafe(input->view.children[0],
                                                                           row));
                            value = ArrowCharView(number);
                        }
                        struct ArrowBuffer text;
                        ArrowBufferInit(&text);
                        int rc = ArrowBufferAppend(&text, f->path, strlen(f->path));
                        if (!rc) rc = ArrowBufferAppend(&text, ":", 1);
                        if (!rc) rc = ArrowBufferAppend(&text, value.data, value.size_bytes);
                        if (!rc)
                            rc = ArrowArrayAppendString(
                                out->array.children[i],
                                (struct ArrowStringView){(const char *)text.data, text.size_bytes});
                        ArrowBufferReset(&text);
                        if (rc) return rc;
                    } else if (named(f, "row_sum")) {
                        double sum = 0;
                        int null = 0;
                        for (int64_t col = 0; col < input->view.n_children; ++col) {
                            double value;
                            if (ArrowArrayViewIsNull(input->view.children[col], row)) {
                                null = 1;
                                break;
                            }
                            NANOARROW_RETURN_NOT_OK(VgiValueNumber(input->view.children[col],
                                                                   input->schema.children[col], row,
                                                                   &value, &call->rpc->error));
                            sum += VgiArgumentInt(call, -1, "absolute", 0) ? fabs(value) : value;
                        }
                        NANOARROW_RETURN_NOT_OK(
                            null ? ArrowArrayAppendNull(out->array.children[i], 1)
                                 : ArrowArrayAppendDouble(out->array.children[i], sum));
                    }
                }
                NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(&out->array));
            }
        }
    return VgiBatchFinish(out, &call->rpc->error);
}
int ExampleTableProcess(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                        struct VgiBatch *out, struct ArrowBuffer *metadata, int *finished) {
    (void)metadata;
    struct ExampleFunction *f = data;
    if (ExampleCancellableSupported(f->name))
        return ExampleCancellableProcess(data, call, input, out, metadata, finished);
    if (ExampleCopyReader(f->name))
        return ExampleCopyRead(data, call, input, out, metadata, finished);
    if (ExampleStaticSupported(f->name))
        return ExampleStaticProcess(data, call, input, out, metadata, finished);
    if (ExamplePartitionSupported(f->name))
        return ExamplePartitionProcess(data, call, input, out, metadata, finished);
    if (ExampleSplitSupported(f->name))
        return ExampleSplitProcess(data, call, input, out, metadata, finished);
    if (ExampleFilterSupported(f->name))
        return ExampleFilterProcess(data, call, input, out, metadata, finished);
    if (ExampleTableExtraSupported(f->name))
        return ExampleTableExtraProcess(data, call, input, out, metadata, finished);
    if (ExampleCacheSupported(f->name))
        return ExampleCacheProcess(data, call, input, out, metadata, finished);
    if (ExampleContextSupported(f->name))
        return ExampleContextProcess(data, call, input, out, metadata, finished);
    if (ExampleBlendedSupported(f->name))
        return ExampleBlendedProcess(data, call, input, out, metadata, finished);
    if (ExampleTableIsExchange(f->name)) return exchange(f, call, input, out);
    struct TableState *s = call->state;
    if (!s) return EINVAL;
    if (s->index >= s->count) {
        if (named(f, "generator_exception")) {
            char message[128];
            snprintf(message, sizeof(message), "Intentional failure after %lld batches",
                     (long long)s->count);
            return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "", message);
        }
        *finished = 1;
        return 0;
    }
    /* Auto filtering needs all predicate columns before the SDK projects. */
    if (named(f, "ticket_probe")) {
        struct ArrowStringView key = VgiBatchString(&call->attach_options, "api_key"),
                               region = VgiBatchString(&call->attach_options, "region");
        if (!region.size_bytes) region = ArrowCharView("us-east-1");
        unsigned char digest[32];
        char hex[65];
        crypto_hash_sha256(digest, (const unsigned char *)key.data,
                           (unsigned long long)key.size_bytes);
        sodium_bin2hex(hex, sizeof(hex), digest, 32);
        hex[12] = 0;
        int rc = VgiBatchInit(out, &call->output_schema, &call->rpc->error);
        for (int64_t i = 0; !rc && i < out->schema.n_children; ++i)
            rc = ArrowArrayAppendString(
                out->array.children[i],
                !strcmp(out->schema.children[i]->name, "region") ? region : ArrowCharView(hex));
        if (!rc) rc = VgiBatchFinishRow(out, &call->rpc->error);
        s->index = 1;
        return rc;
    }
    if (named(f, "echo_attach_options")) {
        s->index = 1;
        return VgiBatchCoerce(&call->attach_options, &call->output_schema, out, &call->rpc->error);
    }
    const struct ArrowSchema *schema =
        (named(f, "sequence") || named(f, "double_sequence")) ? &s->schema : &call->output_schema;
    NANOARROW_RETURN_NOT_OK(VgiBatchInit(out, schema, &call->rpc->error));
    if (named(f, "make_series") && VgiArgumentString(call, 0, NULL).data) {
        struct ArrowStringView csv = VgiArgumentString(call, 0, NULL);
        char *text = malloc((size_t)csv.size_bytes + 1);
        if (!text) return ENOMEM;
        memcpy(text, csv.data, (size_t)csv.size_bytes);
        text[csv.size_bytes] = 0;
        char *start = text;
        int rc = 0;
        for (;;) {
            char *comma = strchr(start, ',');
            if (comma) *comma = 0;
            char *end = NULL;
            errno = 0;
            long long n = strtoll(start, &end, 10);
            if (end != start && !errno) {
                rc = ArrowArrayAppendInt(out->array.children[0], n);
                if (!rc) rc = ArrowArrayFinishElement(&out->array);
            }
            if (rc || !comma) break;
            start = comma + 1;
        }
        free(text);
        s->index = s->count;
        if (rc) return rc;
    } else {
        int64_t end = s->count - s->index > s->batch ? s->index + s->batch : s->count;
        for (int64_t row = s->index; row < end; ++row) {
            for (int64_t i = 0; i < schema->n_children; ++i)
                NANOARROW_RETURN_NOT_OK(producer_value(f, call, s, schema->children[i],
                                                       out->array.children[i], row, i));
            NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(&out->array));
        }
        s->index = end;
    }
    return VgiBatchFinish(out, &call->rpc->error);
}

int ExampleTablePlan(void *data, struct VgiFunctionCall *call, const char *method,
                     struct VgiBatch *result) {
    struct ExampleFunction *f = data;
    if (ExampleStaticSupported(f->name)) return ExampleStaticPlan(data, call, method, result);
    if (ExamplePartitionSupported(f->name)) return ExamplePartitionPlan(data, call, method, result);
    if (ExampleSplitSupported(f->name)) return ExampleSplitPlan(data, call, method, result);
    if (ExampleTableExtraSupported(f->name))
        return ExampleTableExtraPlan(data, call, method, result);
    int counted = ExampleFilterSupported(f->name) || named(f, "sequence") ||
                  named(f, "double_sequence") || named(f, "nested_sequence") ||
                  named(f, "projected_data") || named(f, "constant_columns") ||
                  named(f, "ten_thousand") || named(f, "global_table");
    int64_t count = named(f, "ten_thousand")   ? 10000
                    : named(f, "global_table") ? 3
                                               : VgiArgumentInt(call, 0, NULL, 0);
    if (!strcmp(method, "table_function_cardinality") && counted) {
        NANOARROW_RETURN_NOT_OK(VgiBatchSetInt(result, "estimate", count));
        return VgiBatchSetInt(result, "max", count);
    }
    if (!strcmp(method, "table_function_statistics") &&
        (named(f, "sequence") || named(f, "double_sequence"))) {
        NANOARROW_RETURN_NOT_OK(VgiStatisticsInit(result, &call->rpc->error));
        if (count <= 0) return VgiBatchFinish(result, &call->rpc->error);
        struct VgiColumnStatistics stat = {.name = "n",
                                           .has_not_null = 1,
                                           .distinct_count = count,
                                           .contains_unicode = -1,
                                           .max_string_length = -1};
        stat.min.present = stat.max.present = 1;
        if (named(f, "sequence")) {
            stat.min.kind = stat.max.kind = VGI_STAT_INT64;
            int64_t increment = VgiArgumentInt(call, -1, "increment", 1);
            if (__builtin_mul_overflow(count - 1, increment, &stat.max.integer)) return EOVERFLOW;
        } else {
            stat.min.kind = stat.max.kind = VGI_STAT_DOUBLE;
            stat.max.real = (double)(count - 1) * VgiArgumentDouble(call, -1, "increment", 1);
        }
        return VgiStatisticsAppend(result, &stat, &call->rpc->error);
    }
    return ENODATA;
}
