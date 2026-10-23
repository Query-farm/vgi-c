#include <errno.h>
#include <inttypes.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "fixtures.h"

static int named(const struct ExampleFunction *f, const char *name) {
    return !strcmp(f->name, name);
}
static int summing(const struct ExampleFunction *f) {
    return named(f, "sum_all_columns") || named(f, "sum_all_columns_simple_distributed") ||
           named(f, "cached_sum_all") || named(f, "exception_finalize") ||
           named(f, "exception_process");
}
static int partial(const struct ExampleFunction *f) {
    return named(f, "substream_partial_sum") || named(f, "multi_batch_finish");
}
int ExampleBufferSupported(const char *name) {
    if (ExampleCopyWriter(name) || !strcmp(name, "accumulate")) return 1;
    const char *names[] = {"large_state",
                           "ordered_source",
                           "buffer_emit_wide",
                           "sum_all_columns",
                           "sum_all_columns_simple_distributed",
                           "cached_sum_all",
                           "exception_finalize",
                           "exception_process",
                           "buffer_input",
                           "echo_buffering",
                           "ordered_buffer_input",
                           "batch_index_buffer_input",
                           "slow_cancellable_buffering",
                           "hang_on_process",
                           "crash_on_process",
                           "crash_on_combine",
                           "crash_on_finalize",
                           "global_buffered",
                           "test_same_name_buffered",
                           "substream_partial_sum",
                           "multi_batch_finish"};
    for (size_t i = 0; i < sizeof(names) / sizeof(*names); ++i)
        if (!strcmp(name, names[i])) return 1;
    return 0;
}
static struct ArrowBufferView scope(struct VgiFunctionCall *call) {
    struct ArrowBufferView substream = VgiBatchBytes(&call->init_request, "substream_id");
    return substream.size_bytes
               ? substream
               : (struct ArrowBufferView){{call->execution_id.data}, call->execution_id.size_bytes};
}
static int sum_type(const struct ArrowSchema *schema) {
    struct ArrowSchemaView type;
    if (ArrowSchemaViewInit(&type, schema, NULL)) return 0;
    switch (type.type) {
        case NANOARROW_TYPE_INT8:
        case NANOARROW_TYPE_UINT8:
        case NANOARROW_TYPE_INT16:
        case NANOARROW_TYPE_UINT16:
        case NANOARROW_TYPE_INT32:
        case NANOARROW_TYPE_UINT32:
        case NANOARROW_TYPE_INT64:
        case NANOARROW_TYPE_UINT64:
            return 1;
        default:
            break;
    }
    if (type.type == NANOARROW_TYPE_HALF_FLOAT || type.type == NANOARROW_TYPE_FLOAT ||
        type.type == NANOARROW_TYPE_DOUBLE || type.type == NANOARROW_TYPE_DECIMAL128 ||
        type.type == NANOARROW_TYPE_DECIMAL256 || type.type == NANOARROW_TYPE_DECIMAL32 ||
        type.type == NANOARROW_TYPE_DECIMAL64)
        return 2;
    return 0;
}
int ExampleBufferBind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *out) {
    struct ExampleFunction *f = data;
    if (named(f, "ordered_source") || named(f, "buffer_emit_wide")) {
        ArrowSchemaInit(out);
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeStruct(out, 1));
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(out->children[0], NANOARROW_TYPE_INT64));
        return ArrowSchemaSetName(out->children[0], named(f, "ordered_source") ? "v" : "n");
    }
    if (!call->input_schema.release)
        return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "",
                          "Buffering requires an input schema");
    if (!summing(f) && !partial(f) && !named(f, "test_same_name_buffered"))
        return ArrowSchemaDeepCopy(&call->input_schema, out);
    int64_t count = 0;
    if (partial(f) || named(f, "test_same_name_buffered"))
        count = 1;
    else
        for (int64_t i = 0; i < call->input_schema.n_children; ++i)
            if (sum_type(call->input_schema.children[i])) ++count;
    if (!count || !call->input_schema.n_children)
        return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "",
                          "sum_all_columns requires at least one numeric (integer, floating-point, "
                          "or decimal) input column");
    ArrowSchemaInit(out);
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeStruct(out, count));
    int64_t target = 0;
    for (int64_t i = 0; i < call->input_schema.n_children && target < count; ++i) {
        int type = sum_type(call->input_schema.children[i]);
        if (!type && !partial(f) && !named(f, "test_same_name_buffered")) continue;
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(
            out->children[target], named(f, "test_same_name_buffered") ? NANOARROW_TYPE_STRING
                                   : partial(f) || type == 1           ? NANOARROW_TYPE_INT64
                                                                       : NANOARROW_TYPE_DOUBLE));
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(
            out->children[target],
            named(f, "test_same_name_buffered") ? "tag" : call->input_schema.children[i]->name));
        ++target;
    }
    return 0;
}
static int sum_batch(const struct VgiBatch *input, const struct ArrowSchema *schema,
                     struct VgiBatch *out, struct ArrowError *error) {
    NANOARROW_RETURN_NOT_OK(VgiBatchInit(out, schema, error));
    for (int64_t i = 0; i < schema->n_children; ++i) {
        int64_t id = VgiFieldIndex(&input->schema, schema->children[i]->name);
        uint64_t integer = 0;
        double floating = 0;
        int present = 0;
        if (id >= 0)
            for (int64_t row = 0; row < input->array.length; ++row) {
                if (ArrowArrayViewIsNull(input->view.children[id], row)) continue;
                present = 1;
                if (!strcmp(schema->children[i]->format, "l"))
                    integer += (uint64_t)ArrowArrayViewGetIntUnsafe(input->view.children[id], row);
                else {
                    double value;
                    NANOARROW_RETURN_NOT_OK(VgiValueNumber(
                        input->view.children[id], input->schema.children[id], row, &value, error));
                    floating += value;
                }
            }
        NANOARROW_RETURN_NOT_OK(!present ? ArrowArrayAppendNull(out->array.children[i], 1)
                                : !strcmp(schema->children[i]->format, "l")
                                    ? ArrowArrayAppendInt(out->array.children[i], (int64_t)integer)
                                    : ArrowArrayAppendDouble(out->array.children[i], floating));
    }
    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(&out->array));
    return VgiBatchFinish(out, error);
}
int ExampleBufferSink(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                      struct ArrowBuffer *state_id) {
    struct ExampleFunction *f = data;
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    struct VgiBatch prepared = {0};
    int rc = 0;
    if (VgiArgumentInt(call, -1, "logging", 0)) {
        char message[128];
        snprintf(message, sizeof(message), "Processing batch with %lld rows",
                 (long long)input->array.length);
        rc = VgiRpcLog(call->rpc, "INFO", message);
        if (rc) return rc;
    }
    if (named(f, "crash_on_process")) {
        kill(getpid(), SIGKILL);
        return EIO;
    }
    if (named(f, "hang_on_process")) {
        for (;;) {
            struct timespec delay = {0, 50000000};
            nanosleep(&delay, NULL);
        }
    }
    if (named(f, "ordered_source") || named(f, "buffer_emit_wide")) {
        /* These fixtures derive their output during source initialization. */
    } else if (named(f, "large_state")) {
        rc = ArrowBufferResize(&bytes, 1024 * 1024, 0);
        if (!rc) {
            memset(bytes.data, 0, (size_t)bytes.size_bytes);
            rc = VgiStorageAppend(call->storage, scope(call), "buffer.large",
                                  (struct ArrowBufferView){{bytes.data}, bytes.size_bytes}, NULL,
                                  &call->rpc->error);
        }
    } else if (named(f, "exception_process")) {
        int64_t id;
        rc = VgiStorageAppend(call->storage, scope(call), "buffer.count",
                              (struct ArrowBufferView){{""}, 0}, &id, &call->rpc->error);
        int64_t after = 0, count = 0;
        while (!rc) {
            rc = VgiStorageNext(call->storage, scope(call), "buffer.count", after, &after, &bytes,
                                &call->rpc->error);
            if (rc == ENOENT) {
                rc = 0;
                break;
            }
            ++count;
            ArrowBufferReset(&bytes);
        }
        if (!rc && count % 2 == 0) {
            char text[80];
            snprintf(text, sizeof(text), "Intentional exception on batch %lld", (long long)count);
            rc = VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "", text);
        }
    } else if (input->array.length) {
        if (summing(f))
            rc = sum_batch(input, &call->output_schema, &prepared, &call->rpc->error);
        else if (named(f, "test_same_name_buffered")) {
            struct ExampleFunction tag = *f;
            snprintf(tag.name, sizeof(tag.name), "test_same_name_transform");
            int finished = 0;
            struct ArrowBuffer metadata;
            ArrowBufferInit(&metadata);
            rc = ExampleTableProcess(&tag, call, input, &prepared, &metadata, &finished);
            ArrowBufferReset(&metadata);
        }
        const struct VgiBatch *value = prepared.array.release ? &prepared : input;
        if (!rc)
            rc = VgiIpcWriteBatch(&value->schema, &value->array, NULL, &bytes, &call->rpc->error);
        if (!rc && named(f, "batch_index_buffer_input")) {
            if (VgiBatchIsNull(&call->init_request, "batch_index"))
                rc = VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "",
                                "batch_index_buffer_input requires batch_index");
            int64_t serial = 0;
            if (!rc)
                rc =
                    VgiStorageAppend(call->storage, scope(call), "buffer.order",
                                     (struct ArrowBufferView){{""}, 0}, &serial, &call->rpc->error);
            char key[96];
            snprintf(key, sizeof(key), "buffer.index.%020lld.%020lld",
                     (long long)VgiBatchInt(&call->init_request, "batch_index", 0),
                     (long long)serial);
            if (!rc)
                rc = VgiStoragePut(call->storage, scope(call), key,
                                   (struct ArrowBufferView){{bytes.data}, bytes.size_bytes},
                                   &call->rpc->error);
            ArrowBufferReset(&bytes);
            if (!rc) rc = ArrowBufferAppend(&bytes, key, strlen(key));
            if (!rc)
                rc = VgiStorageAppend(call->storage, scope(call), "buffer.indices",
                                      (struct ArrowBufferView){{bytes.data}, bytes.size_bytes},
                                      NULL, &call->rpc->error);
        } else if (!rc)
            rc = VgiStorageAppend(call->storage, scope(call), "buffer.chunks",
                                  (struct ArrowBufferView){{bytes.data}, bytes.size_bytes}, NULL,
                                  &call->rpc->error);
    }
    struct ArrowBufferView execution = scope(call);
    if (!rc) rc = ArrowBufferAppend(state_id, execution.data.data, execution.size_bytes);
    ArrowBufferReset(&bytes);
    VgiBatchReset(&prepared);
    return rc;
}
static int key_order(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}
int ExampleBufferCombine(void *data, struct VgiFunctionCall *call, struct VgiBatch *result) {
    struct ExampleFunction *f = data;
    if (VgiArgumentInt(call, -1, "logging", 0)) {
        const struct ArrowArrayView *ids = VgiBatchGet(&call->init_request, "state_ids");
        int64_t count =
            ids ? ArrowArrayViewListChildOffset(ids, 1) - ArrowArrayViewListChildOffset(ids, 0) : 0;
        char message[96];
        snprintf(message, sizeof(message), "Combining %lld state_ids", (long long)count);
        NANOARROW_RETURN_NOT_OK(VgiRpcLog(call->rpc, "INFO", message));
    }
    if (named(f, "crash_on_combine"))
        return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "",
                          "Intentional exception during combine()");
    if (named(f, "ordered_source")) {
        struct ArrowArray *ids = VgiBatchColumn(result, "finalize_state_ids");
        for (int64_t i = 0; i < 16; ++i) {
            unsigned char bytes[8];
            for (int j = 0; j < 8; ++j) bytes[j] = (uint8_t)((uint64_t)i >> (8 * j));
            NANOARROW_RETURN_NOT_OK(
                ArrowArrayAppendBytes(ids->children[0], (struct ArrowBufferView){{bytes}, 8}));
        }
        return ArrowArrayFinishElement(ids);
    }
    if (named(f, "large_state")) {
        struct ArrowBuffer bytes;
        ArrowBufferInit(&bytes);
        int64_t after = 0, total = 0;
        int rc = 0;
        for (;;) {
            rc = VgiStorageNext(call->storage, scope(call), "buffer.large", after, &after, &bytes,
                                &call->rpc->error);
            if (rc == ENOENT) {
                rc = 0;
                break;
            }
            if (rc) break;
            total += bytes.size_bytes;
            ArrowBufferReset(&bytes);
        }
        struct VgiBatch batch = {0};
        if (!rc) rc = VgiBatchInit(&batch, &call->output_schema, &call->rpc->error);
        for (int64_t i = 0; !rc && i < batch.schema.n_children; ++i)
            rc = ArrowArrayAppendInt(batch.array.children[i], total);
        if (!rc) rc = ArrowArrayFinishElement(&batch.array);
        if (!rc) rc = VgiBatchFinish(&batch, &call->rpc->error);
        if (!rc)
            rc = VgiIpcWriteBatch(&batch.schema, &batch.array, NULL, &bytes, &call->rpc->error);
        if (!rc)
            rc = VgiStorageAppend(call->storage, scope(call), "buffer.chunks",
                                  (struct ArrowBufferView){{bytes.data}, bytes.size_bytes}, NULL,
                                  &call->rpc->error);
        VgiBatchReset(&batch);
        ArrowBufferReset(&bytes);
        if (rc) return rc;
    }
    if (named(f, "batch_index_buffer_input")) {
        char **keys = NULL;
        size_t n = 0;
        int64_t after = 0;
        struct ArrowBuffer bytes;
        ArrowBufferInit(&bytes);
        int rc = 0;
        while (!rc) {
            rc = VgiStorageNext(call->storage, scope(call), "buffer.indices", after, &after, &bytes,
                                &call->rpc->error);
            if (rc == ENOENT) {
                rc = 0;
                break;
            }
            if (rc) break;
            char **next = realloc(keys, (n + 1) * sizeof(*keys));
            if (!next) {
                rc = ENOMEM;
                break;
            }
            keys = next;
            keys[n] = malloc((size_t)bytes.size_bytes + 1);
            if (!keys[n]) {
                rc = ENOMEM;
                break;
            }
            memcpy(keys[n], bytes.data, (size_t)bytes.size_bytes);
            keys[n][bytes.size_bytes] = 0;
            ++n;
            ArrowBufferReset(&bytes);
        }
        if (!rc && n) qsort(keys, n, sizeof(*keys), key_order);
        for (size_t i = 0; !rc && i < n; ++i) {
            if (i && !strcmp(keys[i - 1], keys[i])) continue;
            ArrowBufferReset(&bytes);
            rc = VgiStorageGet(call->storage, scope(call), keys[i], &bytes, &call->rpc->error);
            if (!rc)
                rc = VgiStorageAppend(call->storage, scope(call), "buffer.chunks",
                                      (struct ArrowBufferView){{bytes.data}, bytes.size_bytes},
                                      NULL, &call->rpc->error);
        }
        for (size_t i = 0; i < n; ++i) free(keys[i]);
        free(keys);
        ArrowBufferReset(&bytes);
        if (rc) return rc;
    }
    struct ArrowArray *ids = VgiBatchColumn(result, "finalize_state_ids");
    NANOARROW_RETURN_NOT_OK(ArrowArrayAppendBytes(ids->children[0], scope(call)));
    return ArrowArrayFinishElement(ids);
}
struct BufferState {
    int done;
    int64_t index, rows;
    uint64_t total;
};
int ExampleBufferInit(void *data, struct VgiFunctionCall *call) {
    struct ExampleFunction *f = data;
    struct ArrowStringView phase = VgiBatchString(&call->init_request, "phase");
    if ((phase.size_bytes == 24 && !memcmp(phase.data, "TABLE_BUFFERING_FINALIZE", 24)) &&
        (named(f, "exception_finalize") || named(f, "crash_on_finalize")))
        return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "",
                          "Intentional exception during finalize()");
    call->state = calloc(1, sizeof(struct BufferState));
    return call->state ? 0 : ENOMEM;
}
void ExampleBufferRelease(void *data, struct VgiFunctionCall *call) {
    (void)data;
    free(call->state);
    call->state = NULL;
}
int ExampleBufferProcess(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                         struct VgiBatch *out, struct ArrowBuffer *metadata, int *finished) {
    struct ExampleFunction *f = data;
    struct BufferState *state = call->state;
    struct ArrowStringView phase = VgiBatchString(&call->init_request, "phase");
    int finalize = phase.size_bytes == 8 && !memcmp(phase.data, "FINALIZE", 8);
    if (partial(f) && !finalize) {
        struct ArrowBuffer ignored;
        ArrowBufferInit(&ignored);
        int rc = ExampleBufferSink(data, call, input, &ignored);
        ArrowBufferReset(&ignored);
        if (!rc) rc = VgiBatchInit(out, &call->output_schema, &call->rpc->error);
        if (!rc) rc = VgiBatchFinish(out, &call->rpc->error);
        return rc;
    }
    if (state->done) {
        *finished = 1;
        return 0;
    }
    if (named(f, "exception_finalize") || named(f, "crash_on_finalize"))
        return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "",
                          "Intentional exception during finalize()");
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    struct VgiBatch batch = {0}, all = {0};
    int rc = 0;
    if (named(f, "ordered_source") || named(f, "buffer_emit_wide")) {
        int64_t rows = named(f, "ordered_source") ? 1 : VgiArgumentInt(call, 0, NULL, 0), start = 0;
        if (rows < 0) rc = EINVAL;
        if (named(f, "ordered_source")) {
            struct ArrowBufferView id = VgiBatchBytes(&call->init_request, "finalize_state_id");
            if (id.size_bytes != 8)
                rc = EINVAL;
            else
                for (int i = 0; i < 8; ++i)
                    start |= (int64_t)((uint64_t)id.data.as_uint8[i] << (8 * i));
        }
        if (!rc) rc = VgiBatchInit(out, &call->output_schema, &call->rpc->error);
        for (int64_t i = 0; !rc && i < rows; ++i) {
            rc = ArrowArrayAppendInt(out->array.children[0], start + i);
            if (!rc) rc = ArrowArrayFinishElement(&out->array);
        }
        if (!rc) rc = VgiBatchFinish(out, &call->rpc->error);
        state->done = 1;
    } else if (summing(f) || partial(f)) {
        if (!state->index) {
            rc = VgiBatchInit(&all, &call->output_schema, &call->rpc->error);
            for (;;) {
                if (rc) break;
                rc = VgiStoragePop(call->storage, scope(call), "buffer.chunks", &bytes,
                                   &call->rpc->error);
                if (rc == ENOENT) {
                    rc = 0;
                    break;
                }
                if (rc) break;
                rc =
                    VgiBatchDecode(&batch, (struct ArrowBufferView){{bytes.data}, bytes.size_bytes},
                                   &call->rpc->error);
                if (!rc && partial(f)) {
                    state->rows += batch.array.length;
                    for (int64_t row = 0; row < batch.array.length; ++row)
                        if (!ArrowArrayViewIsNull(batch.view.children[0], row))
                            state->total +=
                                (uint64_t)ArrowArrayViewGetIntUnsafe(batch.view.children[0], row);
                } else
                    for (int64_t row = 0; !rc && row < batch.array.length; ++row)
                        rc = VgiArrayAppendValue(&all.array, &all.schema, &batch.view,
                                                 &batch.schema, row, &call->rpc->error);
                VgiBatchReset(&batch);
                ArrowBufferReset(&bytes);
            }
            if (!rc) rc = VgiBatchFinish(&all, &call->rpc->error);
            if (!rc && !partial(f)) {
                if (all.array.length)
                    rc = sum_batch(&all, &call->output_schema, out, &call->rpc->error);
                else {
                    rc = VgiBatchInit(out, &call->output_schema, &call->rpc->error);
                    for (int64_t i = 0; !rc && i < out->schema.n_children; ++i)
                        rc = ArrowArrayAppendInt(out->array.children[i], 0);
                    if (!rc) rc = ArrowArrayFinishElement(&out->array);
                    if (!rc) rc = VgiBatchFinish(out, &call->rpc->error);
                }
            }
        }
        if (!rc && partial(f)) {
            if (named(f, "multi_batch_finish") && state->index >= state->rows)
                *finished = 1;
            else {
                rc = VgiBatchInit(out, &call->output_schema, &call->rpc->error);
                if (!rc)
                    rc = ArrowArrayAppendInt(out->array.children[0],
                                             state->index ? 0 : (int64_t)state->total);
                if (!rc) rc = ArrowArrayFinishElement(&out->array);
                if (!rc) rc = VgiBatchFinish(out, &call->rpc->error);
            }
        }
        ++state->index;
        if (!named(f, "multi_batch_finish")) state->done = 1;
        if (!rc && named(f, "cached_sum_all"))
            rc = VgiMetadataSet(metadata, "vgi.cache.ttl", "300");
    } else {
        rc = VgiStoragePop(call->storage, scope(call), "buffer.chunks", &bytes, &call->rpc->error);
        if (rc == ENOENT) {
            rc = 0;
            *finished = 1;
            state->done = 1;
        } else if (!rc) {
            rc = VgiBatchDecode(&batch, (struct ArrowBufferView){{bytes.data}, bytes.size_bytes},
                                &call->rpc->error);
            if (!rc) rc = VgiBatchCoerce(&batch, &call->output_schema, out, &call->rpc->error);
        }
    }
    VgiBatchReset(&batch);
    VgiBatchReset(&all);
    ArrowBufferReset(&bytes);
    return rc;
}
