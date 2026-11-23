/* The blob_test catalog (vgi docs/testing/blob-test-catalog.md): functions
 * sized by their arguments, for the blob-vendor externalization lane and the
 * protocol 3.4 argument-reference tests. The declarative catalog is captured
 * from vgi-python's fixture worker (generated/blob_catalog.arrow); the
 * functions run here, natively.
 *
 *   blob_repeat(s, n)                 scalar  s repeated n times (large outputs)
 *   blob_len(s)                       scalar  UTF-8 byte length (large inputs)
 *   blob_rows(rows, width)            table   row i = repeat(chr(97 + i % 26), width)
 *   blob_param_echo(payload)          table   one row: strlen, sha256 of payload
 *   blob_param_batches(payload, n)    table   n one-row batches, one per turn
 *   blob_param_streams(payload, n)    table   n splits, one row each
 *   blob_param_scalar(i, payload)     scalar  sha256(payload) per non-NULL i; payload const
 *   blob_param_table_scan(payload)    table   the scan behind blob_param_table
 *                                             (STABLE_WITHIN_QUERY, table_only)
 *
 * The storage-group functions (blob_buffer_echo, blob_tio,
 * blob_store_values) are in blob_storage.c. */
#include <errno.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>

#include "fixtures.h"
#include "vgi/catalog.h"
#include "vgi/split.h"

#define BLOB_CATALOG "blob_test"
#define BLOB_ROWS_PER_BATCH 2048

enum BlobKind {
    BLOB_REPEAT,
    BLOB_LEN,
    BLOB_ROWS,
    BLOB_PARAM_ECHO,
    BLOB_PARAM_BATCHES,
    BLOB_PARAM_STREAMS,
    BLOB_PARAM_SCALAR,
    BLOB_PARAM_TABLE_SCAN
};
/* userdata: struct BlobData, whose first member is this. */
struct BlobFunction {
    enum BlobKind kind;
};
struct BlobState {
    int64_t next;     /* next row / batch / stream index */
    int64_t *streams; /* blob_param_streams: claimed split indices */
    int64_t stream_count;
};

static int sha256_hex(const void *data, size_t size, char out[65]) {
    unsigned char digest[crypto_hash_sha256_BYTES];
    crypto_hash_sha256(digest, (const unsigned char *)data, (unsigned long long)size);
    sodium_bin2hex(out, 65, digest, sizeof(digest));
    return 0;
}
static int field(struct ArrowSchema *schema, const char *name, const char *format) {
    int rc = ArrowSchemaSetFormat(schema, format);
    if (!rc) rc = ArrowSchemaSetName(schema, name);
    return rc;
}
/* (names[i] formats[i]) output struct schema. */
static int output_schema(struct ArrowSchema *out, int n, const char *const *names,
                         const char *const *formats) {
    ArrowSchemaInit(out);
    int rc = ArrowSchemaSetTypeStruct(out, n);
    for (int i = 0; !rc && i < n; ++i) rc = field(out->children[i], names[i], formats[i]);
    return rc;
}
static int argument_schema(struct ArrowSchema *out, int n, const char *const *names,
                           const char *const *formats, const int *constant,
                           struct ArrowError *error) {
    ArrowSchemaInit(out);
    int rc = ArrowSchemaSetTypeStruct(out, n);
    for (int i = 0; !rc && i < n; ++i) {
        ArrowSchemaRelease(out->children[i]);
        rc = VgiArgumentInit(out->children[i], names[i], formats[i], constant[i], 0, 0, error);
    }
    return rc;
}
static struct ArrowStringView payload_of(struct VgiFunctionCall *call, int64_t position) {
    return VgiArgumentString(call, position, "payload");
}

/* ------------------------------------------------------------------ table */

static int blob_init(void *data, struct VgiFunctionCall *call) {
    struct BlobFunction *f = data;
    struct BlobState *state = calloc(1, sizeof(*state));
    if (!state) return ENOMEM;
    call->state = state;
    if (f->kind != BLOB_PARAM_STREAMS) return 0;
    if (!call->split_payloads.array.release)
        return VgiRpcFail(call->rpc, "RuntimeError", "FAILED_PRECONDITION", "",
                          "blob_param_streams is split-only; vgi_split_scans is probably off");
    const struct ArrowArrayView *payloads = call->split_payloads.view.children[0];
    state->stream_count = call->split_payloads.array.length;
    state->streams =
        calloc((size_t)(state->stream_count ? state->stream_count : 1), sizeof(*state->streams));
    if (!state->streams) return ENOMEM;
    for (int64_t i = 0; i < state->stream_count; ++i) {
        struct ArrowStringView p = VgiValueString(payloads, i);
        if (p.size_bytes != 8)
            return VgiRpcFail(call->rpc, "RuntimeError", "INVALID_ARGUMENT", "",
                              "blob_param_streams: malformed split payload");
        uint64_t value = 0;
        for (int b = 0; b < 8; ++b) value |= (uint64_t)(unsigned char)p.data[b] << (8 * b);
        state->streams[i] = (int64_t)value;
    }
    return 0;
}
static void blob_finalize(void *data, struct VgiFunctionCall *call) {
    (void)data;
    struct BlobState *state = call->state;
    if (state) free(state->streams);
    free(state);
    call->state = NULL;
}
/* Append (prefix, strlen(payload), sha256(payload)) as one row. */
static int digest_row(struct VgiFunctionCall *call, struct VgiBatch *output, int has_prefix,
                      int64_t prefix, struct ArrowStringView payload) {
    int rc = VgiBatchInit(output, &call->output_schema, &call->rpc->error);
    int column = 0;
    if (!rc && has_prefix) rc = ArrowArrayAppendInt(output->array.children[column++], prefix);
    char hex[65];
    if (!rc && payload.data) {
        sha256_hex(payload.data, (size_t)payload.size_bytes, hex);
        rc = ArrowArrayAppendInt(output->array.children[column], payload.size_bytes);
        if (!rc)
            rc = ArrowArrayAppendString(output->array.children[column + 1], ArrowCharView(hex));
    } else if (!rc) {
        rc = ArrowArrayAppendNull(output->array.children[column], 1);
        if (!rc) rc = ArrowArrayAppendNull(output->array.children[column + 1], 1);
    }
    if (!rc) rc = ArrowArrayFinishElement(&output->array);
    if (!rc) rc = VgiBatchFinish(output, &call->rpc->error);
    return rc;
}
static int blob_rows_batch(struct VgiFunctionCall *call, struct BlobState *state,
                           struct VgiBatch *output, int *finished) {
    int64_t rows = VgiArgumentInt(call, 0, "rows", 0), width = VgiArgumentInt(call, 1, "width", 0);
    if (rows < 0) rows = 0;
    if (width < 0) width = 0;
    if (state->next >= rows) {
        *finished = 1;
        return 0;
    }
    int64_t stop =
        state->next + BLOB_ROWS_PER_BATCH < rows ? state->next + BLOB_ROWS_PER_BATCH : rows;
    char *text = malloc((size_t)(width ? width : 1));
    if (!text) return ENOMEM;
    int rc = VgiBatchInit(output, &call->output_schema, &call->rpc->error);
    for (int64_t i = state->next; !rc && i < stop; ++i) {
        memset(text, 'a' + (int)(i % 26), (size_t)width);
        rc = ArrowArrayAppendInt(output->array.children[0], i);
        if (!rc)
            rc = ArrowArrayAppendString(output->array.children[1],
                                        (struct ArrowStringView){text, width});
        if (!rc) rc = ArrowArrayFinishElement(&output->array);
    }
    free(text);
    if (!rc) rc = VgiBatchFinish(output, &call->rpc->error);
    state->next = stop;
    return rc;
}
static int blob_table_process(void *data, struct VgiFunctionCall *call,
                              const struct VgiBatch *input, struct VgiBatch *output,
                              struct ArrowBuffer *metadata, int *finished) {
    (void)input;
    (void)metadata;
    struct BlobFunction *f = data;
    struct BlobState *state = call->state;
    switch (f->kind) {
        case BLOB_ROWS:
            return blob_rows_batch(call, state, output, finished);
        case BLOB_PARAM_ECHO:
        case BLOB_PARAM_TABLE_SCAN:
            if (state->next++) {
                *finished = 1;
                return 0;
            }
            return digest_row(call, output, 0, 0, payload_of(call, 0));
        case BLOB_PARAM_BATCHES: {
            int64_t batches = VgiArgumentInt(call, 1, "batches", 0);
            if (state->next >= batches) {
                *finished = 1;
                return 0;
            }
            int64_t i = state->next++;
            return digest_row(call, output, 1, i, payload_of(call, 0));
        }
        case BLOB_PARAM_STREAMS:
            if (state->next >= state->stream_count) {
                *finished = 1;
                return 0;
            }
            return digest_row(call, output, 1, state->streams[state->next++], payload_of(call, 0));
        default:
            return EINVAL;
    }
}
static int blob_plan(void *data, struct VgiFunctionCall *call, const char *method,
                     struct VgiBatch *result) {
    struct BlobFunction *f = data;
    if (!strcmp(method, "table_function_cardinality")) {
        int64_t count = f->kind == BLOB_ROWS            ? VgiArgumentInt(call, 0, "rows", 0)
                        : f->kind == BLOB_PARAM_BATCHES ? VgiArgumentInt(call, 1, "batches", 0)
                        : f->kind == BLOB_PARAM_STREAMS ? VgiArgumentInt(call, 1, "streams", 1)
                                                        : 1;
        if (count < 0) count = 0;
        int rc = VgiBatchSetInt(result, "estimate", count);
        return rc ? rc : VgiBatchSetInt(result, "max", count);
    }
    if (strcmp(method, "table_function_plan") || f->kind != BLOB_PARAM_STREAMS) return ENODATA;
    int64_t count = VgiArgumentInt(call, 1, "streams", 1);
    if (count < 1) count = 1;
    if (count > 100000) return EOVERFLOW;
    int rc = 0;
    for (int64_t i = 0; !rc && i < count; ++i) {
        unsigned char payload[8];
        for (int b = 0; b < 8; ++b) payload[b] = (unsigned char)((uint64_t)i >> (8 * b));
        struct VgiBatch split = {0};
        rc = VgiBatchInitNamed(&split, "ScanSplit", &call->rpc->error);
        if (!rc) rc = VgiBatchSetBytes(&split, "payload", payload, 8);
        if (!rc) rc = VgiBatchSetInt(&split, "estimated_rows", 1);
        if (!rc) rc = VgiBatchSetInt(&split, "rows_exact", 1);
        if (!rc) rc = VgiBatchFinishRow(&split, &call->rpc->error);
        if (!rc) rc = VgiPlanAppendSplit(result, &split, &call->rpc->error);
        VgiBatchReset(&split);
    }
    if (!rc) rc = ArrowArrayFinishElement(VgiBatchColumn(result, "splits"));
    if (!rc) rc = VgiBatchSetInt(result, "estimated_total_rows", count);
    if (!rc) rc = VgiBatchSetInt(result, "estimated_total_splits", count);
    return rc;
}
static int64_t blob_streams_workers(void *data, struct VgiFunctionCall *call) {
    (void)data;
    struct BlobState *state = call->state;
    return state && state->stream_count > 1 ? state->stream_count : 1;
}

/* ----------------------------------------------------------------- scalar */

static int blob_scalar_process(void *data, struct VgiFunctionCall *call,
                               const struct VgiBatch *input, struct VgiBatch *output,
                               struct ArrowBuffer *metadata, int *finished) {
    (void)metadata;
    (void)finished;
    struct BlobFunction *f = data;
    int rc = VgiBatchInit(output, &call->output_schema, &call->rpc->error);
    struct ArrowArray *out = rc ? NULL : output->array.children[0];
    const struct ArrowArrayView *first =
        input->view.n_children > 0 ? input->view.children[0] : NULL;
    const struct ArrowArrayView *second =
        input->view.n_children > 1 ? input->view.children[1] : NULL;
    char hex[65] = {0};
    struct ArrowStringView payload = {NULL, 0};
    if (!rc && f->kind == BLOB_PARAM_SCALAR) {
        payload = payload_of(call, 1);
        if (payload.data) sha256_hex(payload.data, (size_t)payload.size_bytes, hex);
    }
    if (!rc && !first) rc = EINVAL;
    for (int64_t row = 0; !rc && row < input->array.length; ++row) {
        if (ArrowArrayViewIsNull(first, row)) {
            rc = ArrowArrayAppendNull(out, 1);
        } else if (f->kind == BLOB_LEN) {
            rc = ArrowArrayAppendInt(out, VgiValueString(first, row).size_bytes);
        } else if (f->kind == BLOB_PARAM_SCALAR) {
            rc = payload.data ? ArrowArrayAppendString(out, ArrowCharView(hex))
                              : ArrowArrayAppendNull(out, 1);
        } else {
            if (!second || ArrowArrayViewIsNull(second, row)) {
                rc = ArrowArrayAppendNull(out, 1);
            } else {
                struct ArrowStringView s = VgiValueString(first, row);
                int64_t n = ArrowArrayViewGetIntUnsafe(second, row);
                if (n < 0) n = 0;
                if (s.size_bytes && n > INT64_MAX / s.size_bytes) {
                    rc = EOVERFLOW;
                    break;
                }
                struct ArrowBuffer text;
                ArrowBufferInit(&text);
                rc = ArrowBufferReserve(&text, s.size_bytes * n);
                for (int64_t k = 0; !rc && k < n; ++k)
                    rc = ArrowBufferAppend(&text, s.data, s.size_bytes);
                if (!rc)
                    rc = ArrowArrayAppendString(
                        out, (struct ArrowStringView){(const char *)text.data, text.size_bytes});
                ArrowBufferReset(&text);
            }
        }
        if (!rc) rc = ArrowArrayFinishElement(&output->array);
    }
    if (!rc) rc = VgiBatchFinish(output, &call->rpc->error);
    return rc;
}

/* ----------------------------------------------------------- registration */

struct BlobSpec {
    const char *name;
    enum BlobKind kind;
    enum VgiFunctionKind function_kind;
    int arg_count;
    const char *arg_names[2], *arg_formats[2];
    int arg_constant[2];
    int out_count;
    const char *out_names[3], *out_formats[3];
};
static const struct BlobSpec specs[] = {
    {"blob_repeat",
     BLOB_REPEAT,
     VGI_SCALAR,
     2,
     {"s", "n"},
     {"u", "l"},
     {0, 0},
     1,
     {"result"},
     {"U"}},
    {"blob_len", BLOB_LEN, VGI_SCALAR, 1, {"s"}, {"u"}, {0}, 1, {"result"}, {"l"}},
    {"blob_rows",
     BLOB_ROWS,
     VGI_TABLE,
     2,
     {"rows", "width"},
     {"l", "l"},
     {1, 1},
     2,
     {"i", "payload"},
     {"l", "u"}},
    {"blob_param_echo",
     BLOB_PARAM_ECHO,
     VGI_TABLE,
     1,
     {"payload"},
     {"u"},
     {1},
     2,
     {"payload_len", "payload_sha256"},
     {"l", "u"}},
    {"blob_param_batches",
     BLOB_PARAM_BATCHES,
     VGI_TABLE,
     2,
     {"payload", "batches"},
     {"u", "l"},
     {1, 1},
     3,
     {"i", "payload_len", "payload_sha256"},
     {"l", "l", "u"}},
    {"blob_param_streams",
     BLOB_PARAM_STREAMS,
     VGI_TABLE,
     2,
     {"payload", "streams"},
     {"u", "l"},
     {1, 1},
     3,
     {"stream", "payload_len", "payload_sha256"},
     {"l", "l", "u"}},
    {"blob_param_scalar",
     BLOB_PARAM_SCALAR,
     VGI_SCALAR,
     2,
     {"i", "payload"},
     {"l", "u"},
     {0, 1},
     1,
     {"result"},
     {"u"}},
    {"blob_param_table_scan",
     BLOB_PARAM_TABLE_SCAN,
     VGI_TABLE,
     1,
     {"payload"},
     {"u"},
     {1},
     2,
     {"payload_len", "payload_sha256"},
     {"l", "u"}},
};
struct BlobData {
    struct BlobFunction function;
    struct ArrowSchema output;
};
static void release_blob_data(void *data) {
    struct BlobData *d = data;
    if (d->output.release) ArrowSchemaRelease(&d->output);
    free(d);
}
static int copy_output(void *data, struct VgiFunctionCall *call, struct ArrowSchema *output) {
    (void)call;
    return ArrowSchemaDeepCopy(&((struct BlobData *)data)->output, output);
}
int ExampleBlobRegister(struct VgiWorker *worker, struct ArrowError *error) {
    const char *primary = getenv("VGI_WORKER_CATALOG_NAME");
    if (primary && *primary && strcmp(primary, "example")) return 0;
    int rc = 0;
    for (size_t i = 0; !rc && i < sizeof(specs) / sizeof(*specs); ++i) {
        const struct BlobSpec *spec = &specs[i];
        struct BlobData *data = calloc(1, sizeof(*data));
        if (!data) return ENOMEM;
        data->function.kind = spec->kind;
        struct ArrowSchema arguments = {0};
        struct VgiFunction fn = {0};
        fn.userdata = data;
        fn.release = release_blob_data;
        rc = argument_schema(&arguments, spec->arg_count, spec->arg_names, spec->arg_formats,
                             spec->arg_constant, error);
        if (!rc)
            rc = output_schema(&data->output, spec->out_count, spec->out_names, spec->out_formats);
        if (!rc)
            rc = VgiFunctionInit(&fn, spec->name, spec->function_kind, &arguments, &data->output,
                                 error);
        if (arguments.release) ArrowSchemaRelease(&arguments);
        if (rc) {
            if (fn.info.schema.release)
                VgiFunctionReset(&fn);
            else
                release_blob_data(data);
            return rc;
        }
        fn.catalog = BLOB_CATALOG;
        fn.schema_path = "main";
        fn.bind = copy_output;
        if (spec->function_kind == VGI_SCALAR) {
            fn.process = blob_scalar_process;
        } else {
            fn.init = blob_init;
            fn.process = blob_table_process;
            fn.finalize = blob_finalize;
            fn.plan = blob_plan;
            if (spec->kind == BLOB_PARAM_STREAMS) {
                fn.max_workers = blob_streams_workers;
                rc = VgiBatchSetInt(&fn.info, "supports_splits", 1);
            }
            if (spec->kind == BLOB_PARAM_TABLE_SCAN) {
                /* Every scan binds over RPC and promises equivalent scans
                 * within one query, so a serialized-plan rebind must rebuild
                 * byte-identical bind requests. */
                fn.repeatability = "STABLE_WITHIN_QUERY";
                if (!rc)
                    rc = VgiBatchSetString(&fn.info, "visibility",
                                           VGI_FUNCTION_VISIBILITY_TABLE_ONLY);
            }
        }
        if (!rc) rc = VgiWorkerRegisterFunction(worker, &fn, error);
        VgiFunctionReset(&fn);
    }
    return rc;
}
