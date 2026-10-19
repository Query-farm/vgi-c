#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "fixtures.h"
#include "vgi/context.h"
static int named(const struct ExampleFunction *f, const char *name) {
    return !strcmp(f->name, name);
}
struct ExtraState {
    int64_t index, count, batch, value, batches, started_ms;
    struct ArrowSchema schema;
};
static int64_t now_ms(void) {
    struct timespec value;
    timespec_get(&value, TIME_UTC);
    return (int64_t)value.tv_sec * 1000 + value.tv_nsec / 1000000;
}
static int transaction_value(struct VgiFunctionCall *call, int64_t *out) {
    *out = VgiArgumentInt(call, 1, NULL, 0);
    struct ArrowBufferView scope = VgiBatchBytes(&call->bind_request, "transaction_opaque_data");
    if (!scope.size_bytes) return 0;
    struct ArrowStringView name = VgiArgumentString(call, 0, NULL);
    if (name.size_bytes < 0 || (uint64_t)name.size_bytes > SIZE_MAX - 9) return EOVERFLOW;
    char *key = malloc((size_t)name.size_bytes + 9);
    if (!key) return ENOMEM;
    memcpy(key, "txcache:", 8);
    memcpy(key + 8, name.data, (size_t)name.size_bytes);
    key[name.size_bytes + 8] = 0;
    struct VgiStorage *storage = NULL;
    struct ArrowBuffer value;
    ArrowBufferInit(&value);
    int rc = VgiStorageOpen(NULL, &storage, &call->rpc->error);
    if (!rc) rc = VgiStorageBegin(storage, &call->rpc->error);
    if (!rc) {
        rc = VgiStorageGet(storage, scope, key, &value, &call->rpc->error);
        if (rc == ENOENT) {
            unsigned char bytes[8];
            for (int i = 0; i < 8; ++i) bytes[i] = (unsigned char)((uint64_t)*out >> (8 * i));
            rc = VgiStoragePut(storage, scope, key, (struct ArrowBufferView){{bytes}, 8},
                               &call->rpc->error);
        } else if (!rc) {
            if (value.size_bytes != 8)
                rc = EINVAL;
            else {
                uint64_t n = 0;
                for (int i = 0; i < 8; ++i) n |= (uint64_t)value.data[i] << (8 * i);
                *out = (int64_t)n;
            }
        }
    }
    if (!rc)
        rc = VgiStorageCommit(storage, &call->rpc->error);
    else if (storage)
        VgiStorageRollback(storage);
    VgiStorageClose(storage);
    ArrowBufferReset(&value);
    free(key);
    return rc;
}
int ExampleTableExtraSupported(const char *name) {
    const char *names[] = {"named_params_echo", "union_varargs",  "typed_probe",
                           "logging_generator", "order_echo",     "sample_echo",
                           "profiling_demo",    "tx_cached_value"};
    for (size_t i = 0; i < sizeof(names) / sizeof(*names); ++i)
        if (!strcmp(name, names[i])) return 1;
    return 0;
}
int ExampleTableExtraBind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *out) {
    struct ExampleFunction *f = data;
    if (named(f, "tx_cached_value")) {
        int64_t value;
        NANOARROW_RETURN_NOT_OK(transaction_value(call, &value));
    }
    int count = named(f, "named_params_echo")                       ? 5
                : named(f, "union_varargs")                         ? 3
                : named(f, "typed_probe") || named(f, "order_echo") ? 6
                : named(f, "sample_echo")                           ? 4
                                                                    : 1;
    ArrowSchemaInit(out);
    int rc = ArrowSchemaSetTypeStruct(out, count);
    for (int i = 0; !rc && i < count; ++i) {
        const char *name = named(f, "tx_cached_value") ? "v" : "n", *format = "l";
        if (named(f, "named_params_echo")) {
            const char *names[] = {"id", "greeting", "value", "float_value", "enabled"},
                       *formats[] = {"l", "u", "l", "g", "b"};
            name = names[i];
            format = formats[i];
        } else if (named(f, "union_varargs")) {
            const char *names[] = {"idx", "tag", "value"};
            name = names[i];
            format = i ? "u" : "l";
        } else if (named(f, "typed_probe")) {
            const char *names[] = {"idx", "ts_us", "iv_ms", "payload", "ub", "f"},
                       *formats[] = {"L", "l", "l", "z", "L", "g"};
            name = names[i];
            format = formats[i];
        } else if (named(f, "order_echo")) {
            const char *names[] = {
                "n", "s", "order_column", "order_direction", "order_null_order", "order_limit"};
            name = names[i];
            format = i > 0 && i < 5 ? "u" : "l";
        } else if (named(f, "sample_echo")) {
            const char *names[] = {"n", "s", "sample_percentage", "sample_seed"},
                       *formats[] = {"l", "u", "g", "l"};
            name = names[i];
            format = formats[i];
        }
        rc = ArrowSchemaSetFormat(out->children[i], format);
        if (!rc) rc = ArrowSchemaSetName(out->children[i], name);
        if (named(f, "tx_cached_value")) out->children[i]->flags &= ~ARROW_FLAG_NULLABLE;
    }
    return rc;
}
int ExampleTableExtraInit(void *data, struct VgiFunctionCall *call) {
    struct ExampleFunction *f = data;
    struct ExtraState *s = calloc(1, sizeof(*s));
    if (!s) return ENOMEM;
    call->state = s;
    int rc = ExampleTableExtraBind(data, call, &s->schema);
    s->count = VgiArgumentInt(call, 0, NULL, 0);
    if (s->count < 0) s->count = 0;
    s->batch = VgiArgumentInt(call, -1, "batch_size", 2048);
    if (s->batch < 1) s->batch = 1;
    s->started_ms = now_ms();
    if (named(f, "logging_generator")) s->batch = 1;
    if (named(f, "union_varargs")) {
        s->count = 0;
        while (VgiArgument(call, s->count, NULL)) ++s->count;
    }
    if (named(f, "tx_cached_value")) {
        s->count = 1;
        if (!rc) rc = transaction_value(call, &s->value);
    }
    return rc;
}
void ExampleTableExtraFree(void *data, struct VgiFunctionCall *call) {
    (void)data;
    struct ExtraState *s = call->state;
    if (s && s->schema.release) ArrowSchemaRelease(&s->schema);
    free(s);
    call->state = NULL;
}
static int append_value(struct ExampleFunction *f, struct VgiFunctionCall *call,
                        struct ExtraState *s, const struct ArrowSchema *schema,
                        struct ArrowArray *dest, int64_t row) {
    const char *name = schema->name;
    char text[128];
    if (named(f, "named_params_echo")) {
        if (!strcmp(name, "greeting")) {
            struct ArrowStringView v = VgiArgumentString(call, -1, "greeting");
            return ArrowArrayAppendString(dest, v.data ? v : ArrowCharView("hello"));
        }
        if (!strcmp(name, "float_value"))
            return ArrowArrayAppendDouble(dest,
                                          (double)row * VgiArgumentDouble(call, -1, "scale", 1));
        if (!strcmp(name, "enabled"))
            return ArrowArrayAppendInt(dest, VgiArgumentInt(call, -1, "enabled", 1));
        return ArrowArrayAppendInt(
            dest, !strcmp(name, "value") ? (int64_t)((uint64_t)row * (uint64_t)VgiArgumentInt(
                                                                         call, -1, "multiplier", 1))
                                         : row);
    }
    if (named(f, "union_varargs")) {
        const struct ArrowArrayView *arg = VgiArgument(call, row, NULL);
        const struct ArrowSchema *type = VgiArgumentSchema(call, row, NULL);
        if (!arg || !type ||
            (arg->storage_type != NANOARROW_TYPE_SPARSE_UNION &&
             arg->storage_type != NANOARROW_TYPE_DENSE_UNION))
            return EINVAL;
        int8_t id = ArrowArrayViewUnionChildIndex(arg, 0);
        int64_t offset = ArrowArrayViewUnionChildOffset(arg, 0);
        if (id < 0 || id >= arg->n_children) return EINVAL;
        if (!strcmp(name, "idx")) return ArrowArrayAppendInt(dest, row);
        if (!strcmp(name, "tag"))
            return ArrowArrayAppendString(dest, ArrowCharView(type->children[id]->name));
        if (ArrowArrayViewIsNull(arg->children[id], offset))
            return ArrowArrayAppendString(dest, ArrowCharView("NULL"));
        struct ArrowBuffer value;
        ArrowBufferInit(&value);
        int rc = VgiValueRender(arg->children[id], type->children[id], offset, &value,
                                &call->rpc->error);
        if (!rc)
            rc = ArrowArrayAppendString(
                dest, (struct ArrowStringView){(const char *)value.data, value.size_bytes});
        ArrowBufferReset(&value);
        return rc;
    }
    if (named(f, "typed_probe")) {
        if (!strcmp(name, "idx")) return ArrowArrayAppendUInt(dest, (uint64_t)row);
        if (!strcmp(name, "ub")) {
            const struct ArrowArrayView *arg = VgiArgument(call, -1, "ub");
            return ArrowArrayAppendUInt(dest, arg && !ArrowArrayViewIsNull(arg, 0)
                                                  ? ArrowArrayViewGetUIntUnsafe(arg, 0)
                                                  : 9);
        }
        if (!strcmp(name, "payload")) {
            struct ArrowStringView v = VgiArgumentString(call, -1, "blob");
            if (!v.data) v = ArrowCharView("vgi");
            return ArrowArrayAppendBytes(dest, (struct ArrowBufferView){{v.data}, v.size_bytes});
        }
        if (!strcmp(name, "f"))
            return ArrowArrayAppendDouble(dest,
                                          VgiArgumentDouble(call, -1, "f", 2.5) + (double)row);
        if (!strcmp(name, "ts_us"))
            return ArrowArrayAppendInt(dest,
                                       VgiArgumentInt(call, -1, "ts", INT64_C(1767323045000000)));
        const struct ArrowArrayView *arg = VgiArgument(call, -1, "iv");
        int64_t value = 1500;
        if (arg && !ArrowArrayViewIsNull(arg, 0) &&
            arg->storage_type == NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO) {
            struct ArrowInterval interval;
            ArrowIntervalInit(&interval, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO);
            ArrowArrayViewGetIntervalUnsafe(arg, 0, &interval);
            value =
                ((int64_t)interval.months * 30 + interval.days) * 86400000 + interval.ns / 1000000;
        }
        return ArrowArrayAppendInt(dest, value);
    }
    if (named(f, "order_echo") || named(f, "sample_echo")) {
        if (!strcmp(name, "n")) return ArrowArrayAppendInt(dest, row);
        if (!strcmp(name, "s")) {
            snprintf(text, sizeof(text), "row_%lld", (long long)row);
            return ArrowArrayAppendString(dest, ArrowCharView(text));
        }
        if (!strcmp(name, "order_limit"))
            return ArrowArrayAppendInt(dest,
                                       VgiBatchInt(&call->init_request, "order_by_limit", -1));
        if (!strcmp(name, "sample_seed"))
            return ArrowArrayAppendInt(dest,
                                       VgiBatchInt(&call->init_request, "tablesample_seed", -1));
        if (!strcmp(name, "sample_percentage")) {
            const struct ArrowArrayView *v =
                VgiBatchGet(&call->init_request, "tablesample_percentage");
            return ArrowArrayAppendDouble(
                dest, v && !ArrowArrayViewIsNull(v, 0) ? ArrowArrayViewGetDoubleUnsafe(v, 0) : -1);
        }
        const char *key = !strcmp(name, "order_column")      ? "order_by_column_name"
                          : !strcmp(name, "order_direction") ? "order_by_direction"
                                                             : "order_by_null_order";
        struct ArrowStringView value = VgiBatchString(&call->init_request, key);
        return ArrowArrayAppendString(dest, value.data ? value : ArrowCharView("(none)"));
    }
    if (named(f, "tx_cached_value")) return ArrowArrayAppendInt(dest, s->value);
    return ArrowArrayAppendInt(
        dest, named(f, "profiling_demo")
                  ? (int64_t)((uint64_t)row * (uint64_t)VgiArgumentInt(call, -1, "increment", 1))
                  : row);
}
int ExampleTableExtraProcess(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                             struct VgiBatch *out, struct ArrowBuffer *metadata, int *finished) {
    (void)input;
    (void)metadata;
    struct ExampleFunction *f = data;
    struct ExtraState *s = call->state;
    int rc = 0;
    if (named(f, "logging_generator") && s->index == 0) {
        char message[128];
        snprintf(message, sizeof(message), "Starting generation of %lld values",
                 (long long)s->count);
        rc = VgiRpcLog(call->rpc, "INFO", message);
    }
    if (s->index >= s->count) {
        *finished = 1;
        if (!rc && named(f, "logging_generator"))
            rc = VgiRpcLog(call->rpc, "INFO", "Generation complete");
        return rc;
    }
    if (!rc)
        rc = VgiBatchInit(out, named(f, "order_echo") ? &s->schema : &call->output_schema,
                          &call->rpc->error);
    int64_t end = s->count - s->index > s->batch ? s->index + s->batch : s->count;
    for (int64_t row = s->index; !rc && row < end; ++row) {
        for (int64_t col = 0; !rc && col < out->schema.n_children; ++col)
            rc = append_value(f, call, s, out->schema.children[col], out->array.children[col], row);
        if (!rc) rc = ArrowArrayFinishElement(&out->array);
    }
    s->index = end;
    ++s->batches;
    if (!rc && named(f, "profiling_demo")) {
        uint8_t counters[24];
        uint64_t values[] = {(uint64_t)s->index, (uint64_t)s->batches, (uint64_t)s->started_ms};
        for (int j = 0; j < 3; ++j)
            for (int i = 0; i < 8; ++i) counters[j * 8 + i] = (uint8_t)(values[j] >> (8 * i));
        rc = VgiStoragePut(
            call->storage,
            (struct ArrowBufferView){{call->execution_id.data}, call->execution_id.size_bytes},
            "profiling", (struct ArrowBufferView){{counters}, 24}, &call->rpc->error);
    }
    if (!rc) rc = VgiBatchFinish(out, &call->rpc->error);
    return rc;
}
int ExampleTableExtraPlan(void *data, struct VgiFunctionCall *call, const char *method,
                          struct VgiBatch *result) {
    struct ExampleFunction *f = data;
    if (!strcmp(method, "table_function_cardinality")) {
        int64_t n = named(f, "tx_cached_value") ? 1 : VgiArgumentInt(call, 0, NULL, 0);
        if (named(f, "union_varargs")) {
            n = 0;
            while (VgiArgument(call, n, NULL)) ++n;
        }
        NANOARROW_RETURN_NOT_OK(VgiBatchSetInt(result, "estimate", n));
        return VgiBatchSetInt(result, "max", n);
    }
    if (!strcmp(method, "table_function_dynamic_to_string") && named(f, "profiling_demo")) {
        struct ArrowBuffer bytes;
        ArrowBufferInit(&bytes);
        int rc =
            VgiStorageGet(call->storage, VgiBatchBytes(&call->init_request, "global_execution_id"),
                          "profiling", &bytes, &call->rpc->error);
        uint64_t values[] = {0, 0, (uint64_t)now_ms()};
        if (rc == ENOENT)
            rc = 0;
        else if (!rc && bytes.size_bytes != 24)
            rc = EINVAL;
        else if (!rc) {
            for (int j = 0; j < 3; ++j) {
                values[j] = 0;
                for (int i = 0; i < 8; ++i) values[j] |= (uint64_t)bytes.data[j * 8 + i] << (8 * i);
            }
        }
        const char *keys[] = {"rows_produced", "batches_emitted", "elapsed_ms"};
        values[2] = (uint64_t)now_ms() - values[2];
        for (int i = 0; !rc && i < 3; ++i) {
            char number[32];
            snprintf(number, sizeof(number), "%llu", (unsigned long long)values[i]);
            rc = ArrowArrayAppendString(VgiBatchColumn(result, "keys")->children[0],
                                        ArrowCharView(keys[i]));
            if (!rc)
                rc = ArrowArrayAppendString(VgiBatchColumn(result, "values")->children[0],
                                            ArrowCharView(number));
        }
        if (!rc) rc = ArrowArrayFinishElement(VgiBatchColumn(result, "keys"));
        if (!rc) rc = ArrowArrayFinishElement(VgiBatchColumn(result, "values"));
        ArrowBufferReset(&bytes);
        return rc;
    }
    return ENODATA;
}
