/* The blob_test storage group (vgi docs/testing/blob-test-catalog.md): each
 * function computes its output from what worker storage returns, so the
 * storage round trip (and, with VGI_STORAGE_EXTERNALIZE, the vendor) is what
 * is tested.
 *
 *   blob_buffer_echo(t)   table-buffering: process appends each input batch
 *                         (an IPC stream) to the execution's log as one value;
 *                         finalize emits each stored batch, one per tick
 *   blob_tio(t)           table-in-out: store each batch under a fresh key,
 *                         read it back, delete it, emit what was read
 *   blob_store_values(t)  table-in-out: store each non-NULL first-column value
 *                         as its own value, read them back, delete them
 *
 * Outputs carry payload_len / payload_sha256 of the first column as text. */
#include <errno.h>
#include <inttypes.h>
#include <sodium.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "fixtures.h"

enum StorageKind { BLOB_BUFFER_ECHO, BLOB_TIO, BLOB_STORE_VALUES };
struct StorageFunction {
    enum StorageKind kind;
    struct ArrowSchema arguments;
};
struct StorageState {
    int64_t after; /* blob_buffer_echo finalize: last log id emitted */
    int64_t counter;
};

/* The first column's value as text (strings as UTF-8, integers in decimal);
 * text->data NULL for NULL. */
static int value_text(const struct ArrowArrayView *column, int64_t row, char scratch[32],
                      struct ArrowStringView *text) {
    text->data = NULL;
    text->size_bytes = 0;
    if (ArrowArrayViewIsNull(column, row)) return 0;
    switch (column->storage_type) {
        case NANOARROW_TYPE_STRING:
        case NANOARROW_TYPE_LARGE_STRING:
        case NANOARROW_TYPE_STRING_VIEW:
        case NANOARROW_TYPE_BINARY:
        case NANOARROW_TYPE_LARGE_BINARY:
        case NANOARROW_TYPE_BINARY_VIEW:
            *text = VgiValueString(column, row);
            if (!text->data) text->data = "";
            return 0;
        case NANOARROW_TYPE_INT8:
        case NANOARROW_TYPE_INT16:
        case NANOARROW_TYPE_INT32:
        case NANOARROW_TYPE_INT64:
            text->size_bytes =
                snprintf(scratch, 32, "%" PRId64, ArrowArrayViewGetIntUnsafe(column, row));
            text->data = scratch;
            return 0;
        case NANOARROW_TYPE_UINT8:
        case NANOARROW_TYPE_UINT16:
        case NANOARROW_TYPE_UINT32:
        case NANOARROW_TYPE_UINT64:
            text->size_bytes =
                snprintf(scratch, 32, "%" PRIu64, ArrowArrayViewGetUIntUnsafe(column, row));
            text->data = scratch;
            return 0;
        default:
            return ENOTSUP;
    }
}
static int append_checksums(struct VgiBatch *output, int64_t first_checksum,
                            struct ArrowStringView text) {
    struct ArrowArray *len = output->array.children[first_checksum],
                      *sha = output->array.children[first_checksum + 1];
    if (!text.data) {
        int rc = ArrowArrayAppendNull(len, 1);
        return rc ? rc : ArrowArrayAppendNull(sha, 1);
    }
    unsigned char digest[crypto_hash_sha256_BYTES];
    char hex[65];
    crypto_hash_sha256(digest, (const unsigned char *)text.data,
                       (unsigned long long)text.size_bytes);
    sodium_bin2hex(hex, sizeof(hex), digest, sizeof(digest));
    int rc = ArrowArrayAppendInt(len, text.size_bytes);
    return rc ? rc : ArrowArrayAppendString(sha, ArrowCharView(hex));
}
/* Emit `source` columns [skip..) plus the checksums of its first column. */
static int emit_with_checksums(struct VgiFunctionCall *call, const struct VgiBatch *source,
                               int64_t skip, struct VgiBatch *output) {
    int rc = VgiBatchInit(output, &call->output_schema, &call->rpc->error);
    int64_t copied = source->schema.n_children - skip;
    char scratch[32];
    for (int64_t row = 0; !rc && row < source->array.length; ++row) {
        for (int64_t i = 0; !rc && i < copied; ++i)
            rc = VgiArrayAppendValue(output->array.children[i], output->schema.children[i],
                                     source->view.children[i + skip],
                                     source->schema.children[i + skip], row, &call->rpc->error);
        struct ArrowStringView text;
        if (!rc && value_text(source->view.children[0], row, scratch, &text))
            rc = VgiRpcFail(call->rpc, "RuntimeError", "INVALID_ARGUMENT", "",
                            "blob_test: the first column must be a string or an integer");
        if (!rc) rc = append_checksums(output, copied, text);
        if (!rc) rc = ArrowArrayFinishElement(&output->array);
    }
    if (!rc) rc = VgiBatchFinish(output, &call->rpc->error);
    return rc;
}
static struct ArrowBufferView execution(struct VgiFunctionCall *call) {
    return (struct ArrowBufferView){{call->execution_id.data}, call->execution_id.size_bytes};
}

static int storage_bind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *out) {
    struct StorageFunction *f = data;
    if (!call->input_schema.release || !call->input_schema.n_children)
        return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "",
                          "blob_test storage functions take one table input");
    int64_t skip = f->kind == BLOB_STORE_VALUES ? 1 : 0,
            copied = call->input_schema.n_children - skip;
    ArrowSchemaInit(out);
    int rc = ArrowSchemaSetTypeStruct(out, copied + 2);
    for (int64_t i = 0; !rc && i < copied; ++i) {
        ArrowSchemaRelease(out->children[i]);
        rc = ArrowSchemaDeepCopy(call->input_schema.children[i + skip], out->children[i]);
    }
    if (!rc) rc = ArrowSchemaSetType(out->children[copied], NANOARROW_TYPE_INT64);
    if (!rc) rc = ArrowSchemaSetName(out->children[copied], "payload_len");
    if (!rc) rc = ArrowSchemaSetType(out->children[copied + 1], NANOARROW_TYPE_STRING);
    if (!rc) rc = ArrowSchemaSetName(out->children[copied + 1], "payload_sha256");
    return rc;
}
static int storage_init(void *data, struct VgiFunctionCall *call) {
    (void)data;
    call->state = calloc(1, sizeof(struct StorageState));
    return call->state ? 0 : ENOMEM;
}
static void storage_free(void *data, struct VgiFunctionCall *call) {
    (void)data;
    free(call->state);
    call->state = NULL;
}
/* blob_buffer_echo, process phase: one storage value per input batch. */
static int buffer_sink(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                       struct ArrowBuffer *state_id) {
    (void)data;
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    int rc = VgiIpcWriteBatch(&input->schema, &input->array, NULL, &bytes, &call->rpc->error);
    if (!rc)
        rc = VgiStorageAppend(call->storage, execution(call), "blob.log",
                              (struct ArrowBufferView){{bytes.data}, bytes.size_bytes}, NULL,
                              &call->rpc->error);
    if (!rc)
        rc = ArrowBufferAppend(state_id, call->execution_id.data, call->execution_id.size_bytes);
    ArrowBufferReset(&bytes);
    return rc;
}
static int buffer_combine(void *data, struct VgiFunctionCall *call, struct VgiBatch *result) {
    (void)data;
    struct ArrowArray *ids = VgiBatchColumn(result, "finalize_state_ids");
    int rc = ArrowArrayAppendBytes(ids->children[0], execution(call));
    return rc ? rc : ArrowArrayFinishElement(ids);
}
static int storage_process(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                           struct VgiBatch *output, struct ArrowBuffer *metadata, int *finished) {
    (void)metadata;
    struct StorageFunction *f = data;
    struct StorageState *state = call->state;
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    struct VgiBatch stored = {0};
    int rc = 0;
    if (f->kind == BLOB_BUFFER_ECHO) {
        /* Finalize: one stored batch per tick, in log order. */
        struct ArrowBufferView scope = VgiBatchBytes(&call->init_request, "finalize_state_id");
        if (!scope.size_bytes) scope = execution(call);
        rc = VgiStorageNext(call->storage, scope, "blob.log", state->after, &state->after, &bytes,
                            &call->rpc->error);
        if (rc == ENOENT) {
            *finished = 1;
            return 0;
        }
        if (!rc)
            rc = VgiBatchDecode(&stored, (struct ArrowBufferView){{bytes.data}, bytes.size_bytes},
                                &call->rpc->error);
        if (!rc) rc = emit_with_checksums(call, &stored, 0, output);
    } else if (f->kind == BLOB_TIO) {
        char key[48];
        snprintf(key, sizeof(key), "tio.%" PRId64, state->counter++);
        rc = VgiIpcWriteBatch(&input->schema, &input->array, NULL, &bytes, &call->rpc->error);
        if (!rc)
            rc = VgiStoragePut(call->storage, execution(call), key,
                               (struct ArrowBufferView){{bytes.data}, bytes.size_bytes},
                               &call->rpc->error);
        ArrowBufferReset(&bytes);
        ArrowBufferInit(&bytes);
        if (!rc) {
            rc = VgiStorageGet(call->storage, execution(call), key, &bytes, &call->rpc->error);
            if (rc == ENOENT)
                rc = VgiRpcFail(call->rpc, "RuntimeError", "INTERNAL", "",
                                "blob_tio: the value just written is missing");
        }
        if (!rc) rc = VgiStorageDelete(call->storage, execution(call), key, &call->rpc->error);
        if (!rc)
            rc = VgiBatchDecode(&stored, (struct ArrowBufferView){{bytes.data}, bytes.size_bytes},
                                &call->rpc->error);
        if (!rc) rc = emit_with_checksums(call, &stored, 0, output);
    } else {
        /* blob_store_values: each non-NULL value its own storage value. */
        int64_t rows = input->array.length, base = state->counter;
        state->counter += rows;
        char key[48], scratch[32];
        for (int64_t row = 0; !rc && row < rows; ++row) {
            struct ArrowStringView text;
            if (value_text(input->view.children[0], row, scratch, &text))
                rc = VgiRpcFail(call->rpc, "RuntimeError", "INVALID_ARGUMENT", "",
                                "blob_test: the first column must be a string or an integer");
            if (rc || !text.data) continue;
            snprintf(key, sizeof(key), "value.%" PRId64, base + row);
            rc = VgiStoragePut(call->storage, execution(call), key,
                               (struct ArrowBufferView){{text.data}, text.size_bytes},
                               &call->rpc->error);
        }
        if (!rc) rc = VgiBatchInit(output, &call->output_schema, &call->rpc->error);
        int64_t copied = input->schema.n_children - 1;
        for (int64_t row = 0; !rc && row < rows; ++row) {
            for (int64_t i = 0; !rc && i < copied; ++i)
                rc = VgiArrayAppendValue(output->array.children[i], output->schema.children[i],
                                         input->view.children[i + 1], input->schema.children[i + 1],
                                         row, &call->rpc->error);
            struct ArrowStringView read = {NULL, 0};
            ArrowBufferReset(&bytes);
            ArrowBufferInit(&bytes);
            if (!rc && !ArrowArrayViewIsNull(input->view.children[0], row)) {
                snprintf(key, sizeof(key), "value.%" PRId64, base + row);
                rc = VgiStorageGet(call->storage, execution(call), key, &bytes, &call->rpc->error);
                if (rc == ENOENT)
                    rc = VgiRpcFail(call->rpc, "RuntimeError", "INTERNAL", "",
                                    "blob_store_values: a value just written is missing");
                if (!rc)
                    rc = VgiStorageDelete(call->storage, execution(call), key, &call->rpc->error);
                read = (struct ArrowStringView){bytes.data ? (const char *)bytes.data : "",
                                                bytes.size_bytes};
            }
            if (!rc) rc = append_checksums(output, copied, read);
            if (!rc) rc = ArrowArrayFinishElement(&output->array);
        }
        if (!rc) rc = VgiBatchFinish(output, &call->rpc->error);
    }
    VgiBatchReset(&stored);
    ArrowBufferReset(&bytes);
    return rc;
}

struct StorageSpec {
    const char *name;
    enum StorageKind kind;
    enum VgiFunctionKind function_kind;
};
static const struct StorageSpec storage_specs[] = {
    {"blob_buffer_echo", BLOB_BUFFER_ECHO, VGI_BUFFERING},
    {"blob_tio", BLOB_TIO, VGI_TABLE_IN_OUT},
    {"blob_store_values", BLOB_STORE_VALUES, VGI_TABLE_IN_OUT},
};
static void release_storage(void *data) {
    struct StorageFunction *f = data;
    if (f->arguments.release) ArrowSchemaRelease(&f->arguments);
    free(f);
}
int ExampleBlobStorageRegister(struct VgiWorker *worker, struct ArrowError *error) {
    const char *primary = getenv("VGI_WORKER_CATALOG_NAME");
    if (primary && *primary && strcmp(primary, "example")) return 0;
    int rc = 0;
    for (size_t i = 0; !rc && i < sizeof(storage_specs) / sizeof(*storage_specs); ++i) {
        struct StorageFunction *data = calloc(1, sizeof(*data));
        if (!data) return ENOMEM;
        data->kind = storage_specs[i].kind;
        struct ArrowSchema output = {0};
        struct VgiFunction fn = {0};
        ArrowSchemaInit(&data->arguments);
        rc = ArrowSchemaSetTypeStruct(&data->arguments, 1);
        if (!rc) {
            ArrowSchemaRelease(data->arguments.children[0]);
            rc = VgiArgumentInit(data->arguments.children[0], "t", "table", 0, 0, 0, error);
        }
        /* The real output depends on the input (bind); register a placeholder. */
        ArrowSchemaInit(&output);
        if (!rc) rc = ArrowSchemaSetTypeStruct(&output, 2);
        if (!rc) rc = ArrowSchemaSetType(output.children[0], NANOARROW_TYPE_INT64);
        if (!rc) rc = ArrowSchemaSetName(output.children[0], "payload_len");
        if (!rc) rc = ArrowSchemaSetType(output.children[1], NANOARROW_TYPE_STRING);
        if (!rc) rc = ArrowSchemaSetName(output.children[1], "payload_sha256");
        if (!rc)
            rc = VgiFunctionInit(&fn, storage_specs[i].name, storage_specs[i].function_kind,
                                 &data->arguments, &output, error);
        if (output.release) ArrowSchemaRelease(&output);
        if (rc) {
            release_storage(data);
            return rc;
        }
        fn.userdata = data;
        fn.release = release_storage;
        fn.catalog = "blob_test";
        fn.schema_path = "main";
        fn.bind = storage_bind;
        fn.init = storage_init;
        fn.process = storage_process;
        fn.finalize = storage_free;
        if (storage_specs[i].kind == BLOB_BUFFER_ECHO) {
            fn.buffer_process = buffer_sink;
            fn.buffer_combine = buffer_combine;
        }
        rc = VgiWorkerRegisterFunction(worker, &fn, error);
        VgiFunctionReset(&fn);
    }
    return rc;
}
