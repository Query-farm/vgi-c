/* Protocol 3.4 nested-binary conformance (vgi docs/large-values-3.4.md §3.4)
 * against the shared corpus in conformance/nested-binary/, plus the C SDK's
 * own 3.4 rules: exactly one of arguments / arguments_location, the split
 * fingerprint of a location, and sealed persistence of the held bind call.
 *
 * vgi-c has no typed records: requests are name-keyed batches and a nested
 * bind_call is a binary column whose bytes are kept as received. These tests
 * check that every path that decodes, opens or re-encodes a request keeps
 * those bytes (rules 1, 4 and 11). */
#include <errno.h>
#include <json-c/json.h>
#include <sodium.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "arguments_internal.h"
#include "opaque_internal.h"
#include "storage_internal.h"
#include "vgi/split.h"
#include "vgi/worker.h"
#include "vgi_rpc/ipc.h"

#define CHECK(x)                                                                  \
    do {                                                                          \
        if (!(x)) {                                                               \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #x); \
            exit(1);                                                              \
        }                                                                         \
    } while (0)

static const char *corpus;

static void read_file(const char *name, struct ArrowBuffer *out) {
    char path[4096];
    snprintf(path, sizeof(path), "%s/%s", corpus, name);
    FILE *f = fopen(path, "rb");
    if (!f) fprintf(stderr, "cannot open %s\n", path);
    CHECK(f);
    ArrowBufferInit(out);
    char chunk[65536];
    size_t n;
    while ((n = fread(chunk, 1, sizeof(chunk), f)) > 0)
        CHECK(!ArrowBufferAppend(out, chunk, (int64_t)n));
    fclose(f);
}
static struct ArrowBufferView view(const struct ArrowBuffer *b) {
    return (struct ArrowBufferView){{b->data}, b->size_bytes};
}
static int same(struct ArrowBufferView a, struct ArrowBufferView b) {
    return a.size_bytes == b.size_bytes &&
           (!a.size_bytes || !memcmp(a.data.data, b.data.data, (size_t)a.size_bytes));
}
static struct json_object *read_json(const char *name) {
    struct ArrowBuffer text;
    read_file(name, &text);
    CHECK(!ArrowBufferAppend(&text, "", 1));
    struct json_object *o = json_tokener_parse((const char *)text.data);
    ArrowBufferReset(&text);
    CHECK(o);
    return o;
}

/* Compare one decoded value with its expected.json encoding: binary ->
 * {"hex"}, utf8 / dictionary<utf8> -> string, bool / int64 -> scalar,
 * list -> array, struct -> object, null -> null. */
static void check_value(const struct ArrowSchema *schema, const struct ArrowArrayView *value,
                        int64_t row, struct json_object *expected, const char *path) {
    if (!expected || json_object_is_type(expected, json_type_null)) {
        if (!ArrowArrayViewIsNull(value, row)) fprintf(stderr, "%s: expected null\n", path);
        CHECK(ArrowArrayViewIsNull(value, row));
        return;
    }
    if (ArrowArrayViewIsNull(value, row)) fprintf(stderr, "%s: unexpected null\n", path);
    CHECK(!ArrowArrayViewIsNull(value, row));
    const char *format = schema->format;
    if (!strcmp(format, "z") || !strcmp(format, "Z")) {
        struct json_object *hex = NULL;
        CHECK(json_object_object_get_ex(expected, "hex", &hex));
        struct ArrowStringView bytes = VgiValueString(value, row);
        char *actual = malloc((size_t)bytes.size_bytes * 2 + 1);
        CHECK(actual);
        sodium_bin2hex(actual, (size_t)bytes.size_bytes * 2 + 1, (const unsigned char *)bytes.data,
                       (size_t)bytes.size_bytes);
        if (strcmp(actual, json_object_get_string(hex)))
            fprintf(stderr, "%s: bytes differ\n", path);
        CHECK(!strcmp(actual, json_object_get_string(hex)));
        free(actual);
    } else if (!strcmp(format, "u") || !strcmp(format, "U") || schema->dictionary) {
        struct ArrowStringView text = VgiValueString(value, row);
        const char *want = json_object_get_string(expected);
        if (text.size_bytes != (int64_t)strlen(want) || memcmp(text.data, want, strlen(want)))
            fprintf(stderr, "%s: '%.*s' != '%s'\n", path, (int)text.size_bytes, text.data, want);
        CHECK(text.size_bytes == (int64_t)strlen(want) && !memcmp(text.data, want, strlen(want)));
    } else if (!strcmp(format, "b")) {
        CHECK(ArrowArrayViewGetIntUnsafe(value, row) == json_object_get_boolean(expected));
    } else if (!strcmp(format, "l")) {
        CHECK(ArrowArrayViewGetIntUnsafe(value, row) == json_object_get_int64(expected));
    } else if (!strcmp(format, "+l") || !strcmp(format, "+L")) {
        int64_t start = ArrowArrayViewListChildOffset(value, row),
                end = ArrowArrayViewListChildOffset(value, row + 1);
        CHECK(json_object_is_type(expected, json_type_array));
        CHECK((int64_t)json_object_array_length(expected) == end - start);
        for (int64_t i = start; i < end; ++i)
            check_value(schema->children[0], value->children[0], i,
                        json_object_array_get_idx(expected, (size_t)(i - start)), path);
    } else if (!strcmp(format, "+s")) {
        for (int64_t i = 0; i < schema->n_children; ++i) {
            struct json_object *child = NULL;
            json_object_object_get_ex(expected, schema->children[i]->name, &child);
            check_value(schema->children[i], value->children[i], row + value->offset, child,
                        schema->children[i]->name);
        }
    } else {
        fprintf(stderr, "%s: unhandled format %s\n", path, format);
        CHECK(0);
    }
}
static void check_fields(const struct VgiBatch *batch, struct json_object *fields) {
    int64_t checked = 0;
    json_object_object_foreach(fields, name, expected) {
        int64_t index = VgiFieldIndex(&batch->schema, name);
        if (index < 0) fprintf(stderr, "missing column %s\n", name);
        CHECK(index >= 0);
        check_value(batch->schema.children[index], batch->view.children[index], 0, expected, name);
        ++checked;
    }
    CHECK(checked == batch->schema.n_children);
}

/* Test 1: byte identity through decode, access, and re-encode. */
static void byte_identity(void) {
    static const char *const embedding[] = {
        "init-noncanonical.arrow",       "cardinality-noncanonical.arrow",
        "statistics-noncanonical.arrow", "dynamic-to-string-noncanonical.arrow",
        "plan-noncanonical.arrow",       "projection-noncanonical.arrow"};
    struct ArrowBuffer held;
    read_file("bind_call.bin", &held);
    struct json_object *expected = read_json("expected.json"), *fields = NULL;
    CHECK(json_object_object_get_ex(expected, "fields", &fields));
    struct ArrowError error = {{0}};
    for (size_t i = 0; i < sizeof(embedding) / sizeof(*embedding); ++i) {
        struct ArrowBuffer file, reencoded;
        read_file(embedding[i], &file);
        ArrowBufferInit(&reencoded);
        for (int access = 0; access < 2; ++access) {
            struct VgiBatch request = {0}, bind = {0}, again = {0};
            CHECK(!VgiBatchDecode(&request, view(&file), &error));
            CHECK(same(VgiBatchBytes(&request, "bind_call"), view(&held)));
            if (access) {
                CHECK(!VgiBatchChild(&request, "bind_call", &bind, &error));
                check_fields(&bind, fields);
            }
            ArrowBufferReset(&reencoded);
            ArrowBufferInit(&reencoded);
            CHECK(!VgiIpcWriteBatch(&request.schema, &request.array, NULL, &reencoded, &error));
            CHECK(!VgiBatchDecode(&again, view(&reencoded), &error));
            CHECK(same(VgiBatchBytes(&again, "bind_call"), view(&held)));
            VgiBatchReset(&request);
            VgiBatchReset(&bind);
            VgiBatchReset(&again);
        }
        ArrowBufferReset(&file);
        ArrowBufferReset(&reencoded);
    }
    json_object_put(expected);
    ArrowBufferReset(&held);
}

static void bind_request(struct VgiBatch *bind, const char *name, int with_location,
                         const char *ref) {
    struct ArrowError error = {{0}};
    CHECK(!VgiBatchInitNamed(bind, "BindRequest", &error));
    CHECK(!VgiBatchSetString(bind, "function_name", name));
    CHECK(!VgiBatchSetString(bind, "function_type", "TABLE"));
    CHECK(!VgiBatchSetInt(bind, "resolved_secrets_provided", 1));
    if (!with_location) {
        CHECK(!VgiBatchSetBytes(bind, "arguments", "args-ipc", 8));
        CHECK(!VgiBatchFinishRow(bind, &error));
        return;
    }
    CHECK(!VgiBatchSetNull(bind, "arguments"));
    struct ArrowArray *location = VgiBatchColumn(bind, "arguments_location");
    CHECK(location);
    CHECK(!ArrowArrayAppendBytes(location->children[0],
                                 (struct ArrowBufferView){{ref}, (int64_t)strlen(ref)}));
    CHECK(!ArrowArrayAppendInt(location->children[1], 624));
    CHECK(!ArrowArrayAppendString(
        location->children[2],
        ArrowCharView("44b0e88d72b1bce9172983e9932bf3e71df09d0b502ea971c024a1015dcd8746")));
    CHECK(!ArrowArrayFinishElement(location));
    CHECK(!VgiBatchFinishRow(bind, &error));
}

/* Test 2: typed construction, embedded in an InitRequest. */
static void typed_construction(void) {
    struct ArrowError error = {{0}};
    struct VgiBatch bind = {0}, init = {0}, decoded = {0}, inner = {0};
    bind_request(&bind, "typed_fixture", 1, "sealed-ref");
    struct ArrowBuffer bind_bytes, twice, init_bytes;
    ArrowBufferInit(&bind_bytes);
    ArrowBufferInit(&twice);
    ArrowBufferInit(&init_bytes);
    CHECK(!VgiIpcWriteBatch(&bind.schema, &bind.array, NULL, &bind_bytes, &error));
    CHECK(!VgiIpcWriteBatch(&bind.schema, &bind.array, NULL, &twice, &error));
    CHECK(same(view(&bind_bytes), view(&twice)));
    CHECK(!VgiBatchInitNamed(&init, "InitRequest", &error));
    CHECK(!VgiBatchSetBytes(&init, "bind_call", bind_bytes.data, bind_bytes.size_bytes));
    CHECK(!VgiBatchSetBytes(&init, "output_schema", "", 0));
    CHECK(!VgiBatchFinishRow(&init, &error));
    CHECK(!VgiIpcWriteBatch(&init.schema, &init.array, NULL, &init_bytes, &error));
    CHECK(!VgiBatchDecode(&decoded, view(&init_bytes), &error));
    CHECK(same(VgiBatchBytes(&decoded, "bind_call"), view(&bind_bytes)));
    CHECK(!VgiBatchChild(&decoded, "bind_call", &inner, &error));
    struct ArrowStringView name = VgiBatchString(&inner, "function_name");
    CHECK(name.size_bytes == 13 && !memcmp(name.data, "typed_fixture", 13));
    CHECK(VgiBatchIsNull(&inner, "arguments") && !VgiBatchIsNull(&inner, "arguments_location"));
    VgiBatchReset(&bind);
    VgiBatchReset(&init);
    VgiBatchReset(&decoded);
    VgiBatchReset(&inner);
    ArrowBufferReset(&bind_bytes);
    ArrowBufferReset(&twice);
    ArrowBufferReset(&init_bytes);
}

/* Test 3: null arguments decode as null (not as empty bytes), and the worker
 * refuses both-null and both-set with invalid_bind_request. */
static void null_arguments(void) {
    struct ArrowError error = {{0}};
    struct ArrowBuffer file;
    struct VgiBatch bind = {0};
    read_file("bind-arguments-null.arrow", &file);
    CHECK(!VgiBatchDecode(&bind, view(&file), &error));
    CHECK(VgiBatchIsNull(&bind, "arguments"));
    CHECK(!VgiBatchIsNull(&bind, "arguments_location"));
    struct json_object *manifest = read_json("manifest.json"), *binds = NULL;
    CHECK(json_object_object_get_ex(manifest, "binds", &binds));
    struct json_object *fields = NULL;
    CHECK(json_object_object_get_ex(json_object_array_get_idx(binds, 0), "fields", &fields));
    check_fields(&bind, fields);
    struct VgiRpcCall call = {0};
    CHECK(VgiBindArgumentsCheck(&call, &bind) == 0);
    VgiBatchReset(&bind);
    ArrowBufferReset(&file);
    const char *invalid[] = {"bind-both-null.arrow", "bind-both-set.arrow"};
    struct json_object *message = NULL;
    CHECK(json_object_object_get_ex(manifest, "error_message", &message));
    for (int i = 0; i < 2; ++i) {
        struct VgiRpcCall refused = {0};
        read_file(invalid[i], &file);
        CHECK(!VgiBatchDecode(&bind, view(&file), &error));
        CHECK(VgiBindArgumentsCheck(&refused, &bind) == EINVAL);
        CHECK(!strcmp(refused.error_kind, "invalid_bind_request"));
        CHECK(!strcmp(refused.error_code, "INVALID_ARGUMENT"));
        CHECK(!strcmp(refused.error.message, json_object_get_string(message)));
        free(refused.error_storage);
        VgiBatchReset(&bind);
        ArrowBufferReset(&file);
    }
    json_object_put(manifest);
}

/* Seal one opaque column the way responses are sealed (VgiOpaqueSealResponse). */
static void seal_value(struct VgiRpcCall *call, const char *column, const char *plaintext,
                       struct ArrowBuffer *out) {
    struct ArrowError error = {{0}};
    struct ArrowSchema schema = {0};
    ArrowSchemaInit(&schema);
    CHECK(!ArrowSchemaSetTypeStruct(&schema, 1));
    CHECK(!ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_BINARY));
    CHECK(!ArrowSchemaSetName(schema.children[0], column));
    struct VgiBatch batch = {0};
    CHECK(!VgiBatchInit(&batch, &schema, &error));
    CHECK(!VgiBatchSetBytes(&batch, column, plaintext, (int64_t)strlen(plaintext)));
    CHECK(!VgiBatchFinishRow(&batch, &error));
    CHECK(!VgiOpaqueSealResponse(call, &batch));
    struct ArrowBufferView sealed = VgiBatchBytes(&batch, column);
    ArrowBufferInit(out);
    CHECK(!ArrowBufferAppend(out, sealed.data.data, sealed.size_bytes));
    VgiBatchReset(&batch);
    ArrowSchemaRelease(&schema);
}

/* Test 4 (rule 11): opening the sealed attach and transaction values of a
 * request's bind call keeps the received bytes, and those are what the C SDK
 * re-serializes (the table-buffering persistence). */
static void reencode_after_open(void) {
    struct ArrowError error = {{0}};
    unsigned char key[32];
    randombytes_buf(key, sizeof(key));
    struct VgiRequestState sealing = {0};
    ArrowBufferInit(&sealing.envelope);
    ArrowBufferInit(&sealing.bind_call);
    struct VgiRpcCall call = {0};
    call.sealing_key = key;
    call.application_state = &sealing;
    const char *attach_plain = "{\"catalog\":\"example\"}", *tx_plain = "tx-plaintext-7";
    struct ArrowBuffer attach, tx;
    seal_value(&call, "attach_opaque_data", attach_plain, &attach);
    CHECK(!ArrowBufferAppend(&sealing.envelope, attach.data, attach.size_bytes));
    seal_value(&call, "transaction_opaque_data", tx_plain, &tx);
    CHECK(attach.size_bytes > 41 && tx.size_bytes > 41);

    /* The bind call as the client sends it, sealed values inside. */
    struct VgiBatch bind = {0}, init = {0}, outer = {0};
    CHECK(!VgiBatchInitNamed(&bind, "BindRequest", &error));
    CHECK(!VgiBatchSetString(&bind, "function_name", "rule11"));
    CHECK(!VgiBatchSetString(&bind, "function_type", "TABLE"));
    CHECK(!VgiBatchSetBytes(&bind, "arguments", "args", 4));
    CHECK(!VgiBatchSetBytes(&bind, "attach_opaque_data", attach.data, attach.size_bytes));
    CHECK(!VgiBatchSetBytes(&bind, "transaction_opaque_data", tx.data, tx.size_bytes));
    CHECK(!VgiBatchSetInt(&bind, "resolved_secrets_provided", 1));
    CHECK(!VgiBatchFinishRow(&bind, &error));
    struct ArrowBuffer received, init_bytes;
    ArrowBufferInit(&received);
    ArrowBufferInit(&init_bytes);
    CHECK(!VgiIpcWriteBatch(&bind.schema, &bind.array, NULL, &received, &error));
    CHECK(!VgiBatchInitNamed(&init, "InitRequest", &error));
    CHECK(!VgiBatchSetBytes(&init, "bind_call", received.data, received.size_bytes));
    CHECK(!VgiBatchSetBytes(&init, "output_schema", "", 0));
    CHECK(!VgiBatchFinishRow(&init, &error));
    CHECK(!VgiIpcWriteBatch(&init.schema, &init.array, NULL, &init_bytes, &error));
    struct ArrowSchema schema = {0};
    ArrowSchemaInit(&schema);
    CHECK(!ArrowSchemaSetTypeStruct(&schema, 1));
    CHECK(!ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_BINARY));
    CHECK(!ArrowSchemaSetName(schema.children[0], "request"));
    CHECK(!VgiBatchInit(&outer, &schema, &error));
    CHECK(!VgiBatchSetBytes(&outer, "request", init_bytes.data, init_bytes.size_bytes));
    CHECK(!VgiBatchFinishRow(&outer, &error));

    /* A fresh request: capture, then open (what worker_dispatch does). */
    struct VgiRequestState state = {0};
    ArrowBufferInit(&state.envelope);
    ArrowBufferInit(&state.bind_call);
    struct VgiMethod method = {0};
    method.name = "init";
    struct VgiRpcCall request = {0};
    request.sealing_key = key;
    request.application_state = &state;
    request.method = &method;
    request.request = outer;
    CHECK(!VgiOpaqueCaptureBindCall(&request, &state.bind_call));
    CHECK(!VgiOpaqueOpenRequest(&request));
    CHECK(same(view(&state.bind_call), view(&received)));
    /* The working copy is opened... */
    struct VgiBatch opened_init = {0}, opened = {0};
    CHECK(!VgiBatchChild(&request.request, "request", &opened_init, &error));
    CHECK(!VgiBatchChild(&opened_init, "bind_call", &opened, &error));
    struct ArrowBufferView plain = VgiBatchBytes(&opened, "attach_opaque_data");
    CHECK(plain.size_bytes == (int64_t)strlen(attach_plain) &&
          !memcmp(plain.data.data, attach_plain, strlen(attach_plain)));
    plain = VgiBatchBytes(&opened, "transaction_opaque_data");
    CHECK(plain.size_bytes == (int64_t)strlen(tx_plain));
    /* ...and never written back: the received bytes still carry the sealed
     * envelopes, and they are what persistence re-serializes. */
    struct VgiBatch held = {0};
    CHECK(!VgiBatchDecode(&held, view(&state.bind_call), &error));
    CHECK(same(VgiBatchBytes(&held, "attach_opaque_data"), view(&attach)));
    CHECK(same(VgiBatchBytes(&held, "transaction_opaque_data"), view(&tx)));
    char path[] = "/tmp/vgi-c-nested-XXXXXX";
    int fd = mkstemp(path);
    CHECK(fd >= 0);
    close(fd);
    struct VgiStorage *storage = NULL;
    CHECK(!VgiStorageOpen(path, &storage, &error));
    struct ArrowBuffer sealed, reopened;
    ArrowBufferInit(&sealed);
    ArrowBufferInit(&reopened);
    struct ArrowBufferView scope = {{"exec-1"}, 6};
    CHECK(
        !VgiStorageSeal(storage, "function.bind", scope, view(&state.bind_call), &sealed, &error));
    CHECK(sealed.size_bytes == state.bind_call.size_bytes + 41);
    for (int64_t i = 0; i + 6 <= sealed.size_bytes; ++i)
        CHECK(memcmp(sealed.data + i, "rule11", 6));
    CHECK(!VgiStorageOpenSealed(storage, "function.bind", scope, view(&sealed), &reopened, &error));
    CHECK(same(view(&reopened), view(&received)));
    ArrowBufferReset(&reopened);
    ArrowBufferInit(&reopened);
    CHECK(VgiStorageOpenSealed(storage, "function.bind", (struct ArrowBufferView){{"exec-2"}, 6},
                               view(&sealed), &reopened, &error) == EBADMSG);
    sealed.data[sealed.size_bytes - 1] ^= 1;
    CHECK(VgiStorageOpenSealed(storage, "function.bind", scope, view(&sealed), &reopened, &error) ==
          EBADMSG);
    VgiStorageClose(storage);
    char key_path[sizeof(path) + 16];
    snprintf(key_path, sizeof(key_path), "%s.seal-key", path);
    CHECK(!access(key_path, F_OK));
    unlink(key_path);
    unlink(path);
    char wal[sizeof(path) + 8];
    snprintf(wal, sizeof(wal), "%s-wal", path);
    unlink(wal);
    snprintf(wal, sizeof(wal), "%s-shm", path);
    unlink(wal);

    VgiBatchReset(&held);
    VgiBatchReset(&opened);
    VgiBatchReset(&opened_init);
    VgiBatchReset(&request.request);
    VgiBatchReset(&bind);
    VgiBatchReset(&init);
    ArrowSchemaRelease(&schema);
    ArrowBufferReset(&attach);
    ArrowBufferReset(&tx);
    ArrowBufferReset(&received);
    ArrowBufferReset(&init_bytes);
    ArrowBufferReset(&sealed);
    ArrowBufferReset(&reopened);
    ArrowBufferReset(&state.envelope);
    ArrowBufferReset(&state.bind_call);
    ArrowBufferReset(&sealing.envelope);
    ArrowBufferReset(&sealing.bind_call);
    free(request.error_storage);
    free(call.error_storage);
}

/* Split fingerprints: a location contributes size + sha256, never its ref. */
static void fingerprints(void) {
    struct VgiBatch inline_bind = {0}, a = {0}, b = {0};
    bind_request(&inline_bind, "fp", 0, NULL);
    bind_request(&a, "fp", 1, "ref-with-nonce-1");
    bind_request(&b, "fp", 1, "ref-with-nonce-2");
    unsigned char fi[16], fa[16], fb[16];
    CHECK(!VgiSplitFingerprint(&inline_bind, fi));
    CHECK(!VgiSplitFingerprint(&a, fa));
    CHECK(!VgiSplitFingerprint(&b, fb));
    CHECK(!memcmp(fa, fb, 16));
    CHECK(memcmp(fa, fi, 16));
    VgiBatchReset(&inline_bind);
    VgiBatchReset(&a);
    VgiBatchReset(&b);
}

int main(int argc, char **argv) {
    CHECK(sodium_init() >= 0);
    corpus = argc > 1 ? argv[1] : "conformance/nested-binary";
    byte_identity();
    typed_construction();
    null_arguments();
    reencode_after_open();
    fingerprints();
    puts(
        "nested-binary: byte identity, typed construction, null arguments, rule 11, "
        "fingerprints passed");
    return 0;
}
