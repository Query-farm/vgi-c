#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "fixtures.h"
static int named(const struct ExampleFunction *f, const char *name) {
    return !strcmp(f->name, name);
}
int ExampleAccumulateSupported(const char *name) {
    return !strcmp(name, "accumulate") || !strcmp(name, "accumulate_read") ||
           !strcmp(name, "accumulate_clear");
}
static struct ArrowBufferView bv(const struct ArrowBuffer *b) {
    return (struct ArrowBufferView){{b->data}, b->size_bytes};
}
static struct ArrowBufferView exec_scope(struct VgiFunctionCall *c) { return bv(&c->execution_id); }
static int fail(struct VgiFunctionCall *c, const char *message) {
    return VgiRpcFail(c->rpc, "ValueError", "INVALID_ARGUMENT", "", message);
}
static int collection_scope(struct VgiFunctionCall *c, unsigned char hash[32]) {
    struct ArrowStringView name = VgiArgumentString(c, 0, NULL);
    int nonspace = 0;
    for (int64_t i = 0; i < name.size_bytes; ++i) nonspace |= !isspace((unsigned char)name.data[i]);
    if (!nonspace) return fail(c, "collection name must be a non-empty string");
    if (name.size_bytes > 255) return fail(c, "collection name must be at most 255 bytes");
    struct ArrowBufferView cookie = VgiBatchBytes(&c->bind_request, "attach_opaque_data");
    crypto_hash_sha256_state state;
    crypto_hash_sha256_init(&state);
    crypto_hash_sha256_update(&state, (const unsigned char *)"vgi-c.accumulate\0", 17);
    unsigned char size[8];
    for (int i = 0; i < 8; ++i) size[i] = (unsigned char)((uint64_t)cookie.size_bytes >> (8 * i));
    crypto_hash_sha256_update(&state, size, 8);
    crypto_hash_sha256_update(&state, cookie.data.as_uint8, (unsigned long long)cookie.size_bytes);
    crypto_hash_sha256_update(&state, (const unsigned char *)name.data,
                              (unsigned long long)name.size_bytes);
    crypto_hash_sha256_final(&state, hash);
    return 0;
}
static int equal_type(const struct ArrowSchema *a, const struct ArrowSchema *b) {
    if (strcmp(a->format, b->format) || strcmp(a->name ? a->name : "", b->name ? b->name : "") ||
        a->n_children != b->n_children || !!a->dictionary != !!b->dictionary)
        return 0;
    for (int64_t i = 0; i < a->n_children; ++i)
        if (!equal_type(a->children[i], b->children[i])) return 0;
    return !a->dictionary || equal_type(a->dictionary, b->dictionary);
}
static int load(struct VgiFunctionCall *c, struct ArrowBufferView scope, const char *key,
                struct VgiBatch *out) {
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    int rc = VgiStorageGet(c->storage, scope, key, &bytes, &c->rpc->error);
    if (!rc) rc = VgiBatchDecode(out, bv(&bytes), &c->rpc->error);
    ArrowBufferReset(&bytes);
    return rc;
}
static int save(struct VgiFunctionCall *c, struct ArrowBufferView scope, const char *key,
                const struct VgiBatch *batch) {
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    int rc = VgiIpcWriteBatch(&batch->schema, &batch->array, NULL, &bytes, &c->rpc->error);
    if (!rc) rc = VgiStoragePut(c->storage, scope, key, bv(&bytes), &c->rpc->error);
    ArrowBufferReset(&bytes);
    return rc;
}
int ExampleAccumulateBind(void *data, struct VgiFunctionCall *c, struct ArrowSchema *out) {
    struct ExampleFunction *f = data;
    unsigned char hash[32];
    int rc = collection_scope(c, hash);
    if (rc) return rc;
    if (named(f, "accumulate_clear")) {
        ArrowSchemaInit(out);
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeStruct(out, 2));
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(out->children[0], NANOARROW_TYPE_STRING));
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(out->children[0], "name"));
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(out->children[1], NANOARROW_TYPE_INT64));
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(out->children[1], "rows_cleared"));
        out->children[0]->flags = out->children[1]->flags = 0;
        return 0;
    }
    if (!c->storage) rc = VgiStorageOpen(NULL, &c->storage, &c->rpc->error);
    if (rc) return rc;
    struct ArrowBufferView scope = {{hash}, 32};
    struct VgiBatch pin = {0};
    rc = VgiStorageBegin(c->storage, &c->rpc->error);
    if (rc) return rc;
    rc = load(c, scope, "schema", &pin);
    int absent = rc == ENOENT;
    if (absent) rc = 0;
    if (!rc && named(f, "accumulate_read")) {
        if (absent)
            rc = fail(c, "no accumulation named by this collection in this session");
        else
            rc = ArrowSchemaDeepCopy(&pin.schema, out);
    } else if (!rc) {
        if (!c->input_schema.release)
            rc = fail(c, "accumulate requires a table input");
        else if (VgiFieldIndex(&c->input_schema, "_timestamp") >= 0)
            rc = fail(c, "input may not contain a reserved '_timestamp' column");
        int64_t n = c->input_schema.n_children;
        if (!rc && !absent) {
            if (pin.schema.n_children != n + 1)
                rc = fail(c, "input schema does not match the schema already accumulated");
            for (int64_t i = 0; !rc && i < n; ++i)
                if (!equal_type(pin.schema.children[i], c->input_schema.children[i]))
                    rc = fail(c, "input schema does not match the schema already accumulated");
        }
        if (!rc) {
            ArrowSchemaInit(out);
            rc = ArrowSchemaSetTypeStruct(out, n + 1);
            for (int64_t i = 0; !rc && i < n; ++i) {
                ArrowSchemaRelease(out->children[i]);
                rc = ArrowSchemaDeepCopy(c->input_schema.children[i], out->children[i]);
            }
            if (!rc) rc = ArrowSchemaSetFormat(out->children[n], "tsu:");
            if (!rc) rc = ArrowSchemaSetName(out->children[n], "_timestamp");
            if (!rc) out->children[n]->flags = 0;
        }
        if (!rc && absent) {
            rc = VgiBatchInit(&pin, out, &c->rpc->error);
            if (!rc) rc = VgiBatchFinish(&pin, &c->rpc->error);
            if (!rc) rc = save(c, scope, "schema", &pin);
        }
    }
    if (!rc) rc = VgiStorageCommit(c->storage, &c->rpc->error);
    if (rc) VgiStorageRollback(c->storage);
    VgiBatchReset(&pin);
    return rc;
}
int ExampleAccumulateSink(void *data, struct VgiFunctionCall *c, const struct VgiBatch *input,
                          struct ArrowBuffer *state_id) {
    (void)data;
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    int rc = 0;
    if (input->array.length) {
        rc = VgiIpcWriteBatch(&input->schema, &input->array, NULL, &bytes, &c->rpc->error);
        if (!rc)
            rc = VgiStorageAppend(c->storage, exec_scope(c), "acc.input", bv(&bytes), NULL,
                                  &c->rpc->error);
    }
    if (!rc) rc = ArrowBufferAppend(state_id, c->execution_id.data, c->execution_id.size_bytes);
    ArrowBufferReset(&bytes);
    return rc;
}
static int append_range(struct VgiBatch *out, const struct VgiBatch *in, int64_t start, int64_t end,
                        struct ArrowError *error) {
    for (int64_t row = start; row < end; ++row)
        NANOARROW_RETURN_NOT_OK(
            VgiArrayAppendValue(&out->array, &out->schema, &in->view, &in->schema, row, error));
    return 0;
}
static int ttl(struct VgiFunctionCall *c, int64_t *value, int *present) {
    const struct ArrowArrayView *v = VgiArgument(c, -1, "ttl");
    *present = v && !ArrowArrayViewIsNull(v, 0);
    if (!*present) return 0;
    if (v->storage_type != NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO) return EINVAL;
    struct ArrowInterval i;
    ArrowIntervalInit(&i, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO);
    ArrowArrayViewGetIntervalUnsafe(v, 0, &i);
    int64_t days, us;
    if (__builtin_mul_overflow((int64_t)i.months, INT64_C(30), &days) ||
        __builtin_add_overflow(days, (int64_t)i.days, &days) ||
        __builtin_mul_overflow(days, INT64_C(86400000000), &us) ||
        __builtin_add_overflow(us, i.ns / 1000, value))
        return EOVERFLOW;
    return 0;
}
int ExampleAccumulateCombine(void *data, struct VgiFunctionCall *c, struct VgiBatch *result) {
    (void)data;
    unsigned char hash[32];
    int rc = collection_scope(c, hash);
    struct ArrowBufferView scope = {{hash}, 32};
    struct VgiBatch pin = {0}, old = {0}, all = {0}, retained = {0}, staged = {0}, input = {0};
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    if (!rc) rc = VgiStorageBegin(c->storage, &c->rpc->error);
    if (!rc) rc = load(c, scope, "schema", &pin);
    if (!rc) {
        rc = load(c, scope, "rows", &old);
        if (rc == ENOENT) rc = 0;
    }
    if (!rc) rc = VgiBatchInit(&all, &pin.schema, &c->rpc->error);
    if (!rc && old.array.release)
        rc = append_range(&all, &old, 0, old.array.length, &c->rpc->error);
    int64_t first_new = all.array.length, after = 0, id = 0;
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    int64_t micros = (int64_t)now.tv_sec * 1000000 + now.tv_nsec / 1000;
    while (!rc) {
        bytes.size_bytes = 0;
        rc = VgiStorageNext(c->storage, exec_scope(c), "acc.input", after, &id, &bytes,
                            &c->rpc->error);
        if (rc == ENOENT) {
            rc = 0;
            break;
        }
        if (rc) break;
        after = id;
        rc = VgiBatchDecode(&input, bv(&bytes), &c->rpc->error);
        for (int64_t row = 0; !rc && row < input.array.length; ++row) {
            for (int64_t col = 0; !rc && col < all.schema.n_children - 1; ++col) {
                int64_t source = VgiFieldIndex(&input.schema, all.schema.children[col]->name);
                rc = source < 0
                         ? EINVAL
                         : VgiArrayAppendValue(all.array.children[col], all.schema.children[col],
                                               input.view.children[source],
                                               input.schema.children[source], row, &c->rpc->error);
            }
            if (!rc)
                rc = ArrowArrayAppendInt(all.array.children[all.schema.n_children - 1], micros);
            if (!rc) rc = ArrowArrayFinishElement(&all.array);
        }
        VgiBatchReset(&input);
    }
    if (!rc) rc = VgiBatchFinish(&all, &c->rpc->error);
    int present = 0;
    int64_t lifetime = 0, cutoff = 0, start = 0;
    if (!rc) rc = ttl(c, &lifetime, &present);
    if (!rc && present && __builtin_sub_overflow(micros, lifetime, &cutoff)) rc = EOVERFLOW;
    if (!rc && present && cutoff > 0) {
        const struct ArrowArrayView *timestamps = all.view.children[all.view.n_children - 1];
        while (start < all.array.length && ArrowArrayViewGetIntUnsafe(timestamps, start) < cutoff)
            ++start;
    }
    int64_t cap = VgiArgumentInt(c, -1, "max_row_size", 0);
    if (cap > 0 && all.array.length - start > cap) start = all.array.length - cap;
    if (!rc) rc = VgiBatchInit(&retained, &all.schema, &c->rpc->error);
    if (!rc) rc = append_range(&retained, &all, start, all.array.length, &c->rpc->error);
    if (!rc) rc = VgiBatchFinish(&retained, &c->rpc->error);
    if (!rc) rc = save(c, scope, "rows", &retained);
    struct ArrowStringView mode = VgiArgumentString(c, -1, "result");
    int none = mode.size_bytes == 4 && !memcmp(mode.data, "none", 4),
        new = mode.size_bytes == 3 && !memcmp(mode.data, "new", 3);
    if (!rc) rc = VgiBatchInit(&staged, &all.schema, &c->rpc->error);
    if (!rc && !none)
        rc = append_range(&staged, &all, new ? first_new : start, all.array.length, &c->rpc->error);
    if (!rc) rc = VgiBatchFinish(&staged, &c->rpc->error);
    if (!rc) rc = save(c, exec_scope(c), "acc.output", &staged);
    if (!rc) rc = VgiStorageCommit(c->storage, &c->rpc->error);
    if (rc) VgiStorageRollback(c->storage);
    if (!rc) {
        struct ArrowArray *ids = VgiBatchColumn(result, "finalize_state_ids");
        rc = ArrowArrayAppendBytes(ids->children[0], exec_scope(c));
        if (!rc) rc = ArrowArrayFinishElement(ids);
    }
    VgiBatchReset(&pin);
    VgiBatchReset(&old);
    VgiBatchReset(&all);
    VgiBatchReset(&retained);
    VgiBatchReset(&staged);
    VgiBatchReset(&input);
    ArrowBufferReset(&bytes);
    return rc;
}
struct AccumulateState {
    struct VgiBatch rows;
    int64_t cursor;
};
int ExampleAccumulateInit(void *data, struct VgiFunctionCall *c) {
    struct ExampleFunction *f = data;
    struct AccumulateState *s = calloc(1, sizeof(*s));
    if (!s) return ENOMEM;
    c->state = s;
    unsigned char hash[32];
    int rc = collection_scope(c, hash);
    struct ArrowBufferView scope = {{hash}, 32};
    if (rc) return rc;
    if (named(f, "accumulate")) {
        struct ArrowStringView phase = VgiBatchString(&c->init_request, "phase");
        if (phase.size_bytes != 24 || memcmp(phase.data, "TABLE_BUFFERING_FINALIZE", 24)) return 0;
        return load(c, exec_scope(c), "acc.output", &s->rows);
    }
    if (named(f, "accumulate_read")) {
        rc = load(c, scope, "rows", &s->rows);
        if (rc == ENOENT) {
            rc = VgiBatchInit(&s->rows, &c->output_schema, &c->rpc->error);
            if (!rc) rc = VgiBatchFinish(&s->rows, &c->rpc->error);
        }
        return rc;
    }
    struct VgiBatch old = {0};
    rc = VgiStorageBegin(c->storage, &c->rpc->error);
    if (!rc) {
        rc = load(c, scope, "rows", &old);
        if (rc == ENOENT) rc = 0;
    }
    if (!rc) rc = VgiStorageClear(c->storage, scope, &c->rpc->error);
    if (!rc) rc = VgiStorageCommit(c->storage, &c->rpc->error);
    if (rc) VgiStorageRollback(c->storage);
    if (!rc) rc = VgiBatchInit(&s->rows, &c->output_schema, &c->rpc->error);
    struct ArrowStringView name = VgiArgumentString(c, 0, NULL);
    if (!rc) rc = VgiBatchSetBytes(&s->rows, "name", name.data, name.size_bytes);
    if (!rc) rc = VgiBatchSetInt(&s->rows, "rows_cleared", old.array.length);
    if (!rc) rc = VgiBatchFinishRow(&s->rows, &c->rpc->error);
    VgiBatchReset(&old);
    return rc;
}
void ExampleAccumulateFree(void *data, struct VgiFunctionCall *c) {
    (void)data;
    struct AccumulateState *s = c->state;
    if (s) VgiBatchReset(&s->rows);
    free(s);
    c->state = NULL;
}
int ExampleAccumulateProcess(void *data, struct VgiFunctionCall *c, const struct VgiBatch *input,
                             struct VgiBatch *out, struct ArrowBuffer *metadata, int *finished) {
    (void)data;
    (void)input;
    (void)metadata;
    struct AccumulateState *s = c->state;
    if (s->cursor >= s->rows.array.length) {
        *finished = 1;
        return 0;
    }
    int64_t end =
        s->rows.array.length - s->cursor > 65536 ? s->cursor + 65536 : s->rows.array.length;
    int rc = VgiBatchInit(out, &c->output_schema, &c->rpc->error);
    if (!rc) rc = append_range(out, &s->rows, s->cursor, end, &c->rpc->error);
    if (!rc) rc = VgiBatchFinish(out, &c->rpc->error);
    s->cursor = end;
    return rc;
}
