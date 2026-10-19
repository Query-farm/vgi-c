#include "vgi/cache.h"

#include <errno.h>
#include <sodium.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "fixtures.h"
#include "vgi/context.h"
#include "vgi/partition.h"

static _Atomic int64_t nonce_counter;
static int named(const struct ExampleFunction *f, const char *s) { return !strcmp(f->name, s); }
struct CacheState {
    int64_t index, count, batch, nonce;
    int emitted;
    struct ArrowSchema schema;
};
int ExampleCacheSupported(const char *name) {
    const char *names[] = {"cache_partitioned",
                           "cache_partition_scope",
                           "cache_partition_parallel",
                           "cache_partition_multicol",
                           "cache_partition_proj",
                           "secret_cache_nonce",
                           "secret_cached_lateral",
                           "cacheable_numbers",
                           "cache_no_store",
                           "cache_scoped_txn",
                           "cache_nonce",
                           "cache_multicol",
                           "cache_big",
                           "cache_revalidatable",
                           "cache_parallel",
                           "cache_bench",
                           "cache_poison",
                           "cache_projection",
                           "cache_filtered",
                           "cache_ordered",
                           "cache_external_fail",
                           "cache_whoami",
                           "cache_versioned_scan",
                           "test_same_name_cached",
                           "cache_types",
                           "cached_echo",
                           "cached_reval_echo",
                           "cached_double",
                           "cached_reval_double",
                           "cached_explode"};
    for (size_t i = 0; i < sizeof(names) / sizeof(*names); ++i)
        if (!strcmp(name, names[i])) return 1;
    return 0;
}
int ExampleCacheExchange(const char *name) {
    return !strncmp(name, "cached_", 7) || !strcmp(name, "secret_cached_lateral");
}
static int partitioned(const struct ExampleFunction *f) {
    return !strncmp(f->name, "cache_partition", 15);
}
static int partition_count(const struct ExampleFunction *f) {
    return named(f, "cache_partition_proj")                                               ? 2
           : named(f, "cache_partition_multicol") || named(f, "cache_partition_parallel") ? 4
                                                                                          : 5;
}
int ExampleCacheBind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *out) {
    struct ExampleFunction *f = data;
    if (named(f, "cached_echo") || named(f, "cached_reval_echo"))
        return ArrowSchemaDeepCopy(&call->input_schema, out);
    int count =
        partitioned(f)
            ? (named(f, "cache_partition_multicol") || named(f, "cache_partition_proj") ? 3 : 2)
        : named(f, "secret_cache_nonce") || named(f, "secret_cached_lateral") ? 2
        : named(f, "cache_multicol") || named(f, "cache_projection")          ? 3
        : named(f, "cache_scoped_txn")                                        ? 2
        : named(f, "cache_types")                                             ? 6
                                                                              : 1;
    ArrowSchemaInit(out);
    int rc = ArrowSchemaSetTypeStruct(out, count);
    for (int i = 0; !rc && i < count; ++i) {
        const char *name = "n", *format = "l";
        if (count == 3) {
            const char *names[] = {"a", "b", "c"};
            name = names[i];
        } else if (named(f, "cache_nonce") || named(f, "cache_revalidatable") ||
                   (named(f, "cache_scoped_txn") && i))
            name = "nonce";
        else if (named(f, "cache_parallel") || named(f, "cache_bench") ||
                 named(f, "cache_versioned_scan"))
            name = "v";
        else if (named(f, "cache_whoami")) {
            name = "who";
            format = "u";
        } else if (named(f, "test_same_name_cached")) {
            name = "tag";
            format = "u";
        } else if (named(f, "cached_double") || named(f, "cached_reval_double"))
            name = "doubled";
        else if (named(f, "cached_explode"))
            name = "i";
        if (named(f, "secret_cache_nonce") || named(f, "secret_cached_lateral")) {
            name = i ? "nonce" : "secret_string";
            format = i ? "l" : "u";
        }
        if (partitioned(f)) {
            if (named(f, "cache_partition_multicol")) {
                const char *names[] = {"region", "year", "amount"};
                name = names[i];
                format = i ? "l" : "u";
            } else {
                const char *names[] = {"country", "sales", "extra"};
                name = names[i];
                format = i ? "l" : "u";
            }
            if (i == 0 || (named(f, "cache_partition_multicol") && i == 1))
                rc = VgiPartitionField(out->children[i]);
        }
        if (named(f, "cache_types")) {
            const char *names[] = {"id", "tags", "attrs", "amt", "ts", "label"};
            name = names[i];
            const char *formats[] = {"l", "+l", "+s", "d:18,2", "tsu:", "u"};
            format = formats[i];
            if (i == 1) {
                rc = ArrowSchemaSetType(out->children[i], NANOARROW_TYPE_LIST);
                if (!rc)
                    rc = ArrowSchemaSetType(out->children[i]->children[0], NANOARROW_TYPE_INT64);
            }
            if (i == 2) {
                rc = ArrowSchemaSetTypeStruct(out->children[i], 2);
                if (!rc)
                    rc = ArrowSchemaSetType(out->children[i]->children[0], NANOARROW_TYPE_INT64);
                if (!rc) rc = ArrowSchemaSetName(out->children[i]->children[0], "x");
                if (!rc)
                    rc = ArrowSchemaSetType(out->children[i]->children[1], NANOARROW_TYPE_STRING);
                if (!rc) rc = ArrowSchemaSetName(out->children[i]->children[1], "y");
            }
        }
        if (!rc) rc = ArrowSchemaSetFormat(out->children[i], format);
        if (!rc) rc = ArrowSchemaSetName(out->children[i], name);
    }
    return rc;
}
static struct ArrowBufferView scope(struct VgiFunctionCall *call) {
    return (struct ArrowBufferView){{call->execution_id.data}, call->execution_id.size_bytes};
}
static int64_t count(struct ExampleFunction *f, struct VgiFunctionCall *call) {
    if (named(f, "secret_cache_nonce")) return 1;
    if (partitioned(f)) return partition_count(f);
    if (named(f, "cache_big")) return VgiArgumentInt(call, -1, "rows", 5000);
    if (named(f, "cache_ordered")) return VgiArgumentInt(call, -1, "rows", 200000);
    if (named(f, "cache_filtered")) return VgiArgumentInt(call, -1, "rows", 100);
    if (named(f, "cache_parallel") || named(f, "cache_types") || named(f, "cache_bench"))
        return VgiArgumentInt(call, 0, NULL, 10);
    if (named(f, "cache_projection") || named(f, "cache_external_fail")) return 3;
    if (named(f, "cache_nonce") || named(f, "cache_revalidatable") || named(f, "cache_whoami") ||
        named(f, "test_same_name_cached"))
        return 1;
    if (named(f, "cache_versioned_scan")) {
        int64_t version = VgiArgumentInt(call, 0, NULL, 3);
        return version == 1 ? 3 : version == 2 ? 2 : 4;
    }
    return VgiArgumentInt(call, -1, "n",
                          named(f, "cache_multicol") || named(f, "cache_poison") ? 4 : 10);
}
int ExampleCacheInit(void *data, struct VgiFunctionCall *call) {
    struct ExampleFunction *f = data;
    struct CacheState *s = calloc(1, sizeof(*s));
    if (!s) return ENOMEM;
    call->state = s;
    if (ExampleCacheExchange(f->name)) return 0;
    int rc = ExampleCacheBind(data, call, &s->schema);
    s->count = count(f, call);
    if (s->count < 0) s->count = 0;
    s->batch = 1000;
    if (named(f, "cache_types") || named(f, "cache_filtered")) s->batch = 2048;
    if (named(f, "cache_ordered")) s->batch = VgiArgumentInt(call, -1, "chunk_size", 1000);
    if (named(f, "cache_scoped_txn") || named(f, "cache_nonce") || named(f, "cache_revalidatable"))
        s->nonce = atomic_fetch_add(&nonce_counter, 1);
    if (named(f, "secret_cache_nonce")) {
        uint64_t random;
        randombytes_buf(&random, sizeof(random));
        s->nonce = (int64_t)(random >> 8);
    }
    if (named(f, "cache_poison") && s->count > 0) s->batch = s->count;
    if (named(f, "cache_parallel")) {
        s->batch = VgiArgumentInt(call, -1, "batch_size", 24000);
        if (!VgiBatchBytes(&call->init_request, "execution_id").size_bytes) {
            int64_t chunk = s->count / 24 + (s->count % 24 != 0);
            if (chunk < 1) chunk = 1;
            for (int64_t i = 0; !rc && i < s->count;) {
                int64_t end = s->count - i < chunk ? s->count : i + chunk;
                uint8_t range[16];
                for (int k = 0; k < 8; ++k) {
                    range[k] = (uint8_t)((uint64_t)i >> (8 * k));
                    range[k + 8] = (uint8_t)((uint64_t)end >> (8 * k));
                }
                rc = VgiStorageAppend(call->storage, scope(call), "cache.ranges",
                                      (struct ArrowBufferView){{range}, 16}, NULL,
                                      &call->rpc->error);
                i = end;
            }
        }
        s->count = 0;
    }
    if (named(f, "cache_partition_parallel") &&
        !VgiBatchBytes(&call->init_request, "execution_id").size_bytes)
        for (uint8_t i = 0; !rc && i < 4; ++i)
            rc = VgiStorageAppend(call->storage, scope(call), "cache.partitions",
                                  (struct ArrowBufferView){{&i}, 1}, NULL, &call->rpc->error);
    if (s->batch < 1) s->batch = 1;
    return rc;
}
void ExampleCacheFree(void *data, struct VgiFunctionCall *call) {
    (void)data;
    struct CacheState *s = call->state;
    if (s && s->schema.release) ArrowSchemaRelease(&s->schema);
    free(s);
    call->state = NULL;
}
static int cache_metadata(struct ExampleFunction *f, struct VgiFunctionCall *call,
                          struct ArrowBuffer *metadata, int *confirmed) {
    struct VgiCacheControl c = {0};
    c.has_ttl = 1;
    c.ttl_seconds = VgiArgumentInt(call, -1, "ttl", 300);
    if (named(f, "cache_no_store")) {
        c.has_ttl = 0;
        c.no_store = 1;
    }
    if (named(f, "cache_scoped_txn")) c.scope = "transaction";
    if (named(f, "cached_double") || named(f, "cached_explode") ||
        named(f, "secret_cached_lateral"))
        c.per_value = 1;
    if (partitioned(f) && !named(f, "cache_partitioned")) c.partition_scope = 1;
    if (named(f, "cache_revalidatable") || named(f, "cached_reval_echo") ||
        named(f, "cached_reval_double")) {
        c.ttl_seconds = 0;
        c.revalidatable = 1;
        c.etag = named(f, "cache_revalidatable") ? "\"rev-v1\""
                 : named(f, "cached_reval_echo") ? "\"vgi-cpp-reval-echo\""
                                                 : "\"vgi-cpp-reval-double\"";
        struct ArrowStringView value =
            VgiMetadataGet(call->input_metadata, "vgi.cache.if_none_match");
        if (!value.data && !ExampleCacheExchange(f->name))
            value =
                VgiMetadataGet((const char *)call->rpc->metadata.data, "vgi.cache.if_none_match");
        c.not_modified = value.data && value.size_bytes == (int64_t)strlen(c.etag) &&
                         !memcmp(value.data, c.etag, (size_t)value.size_bytes);
    }
    *confirmed = c.not_modified;
    return VgiCacheControlMetadata(&c, metadata);
}
static int types_value(struct ArrowArray *dest, const struct ArrowSchema *schema, int64_t row) {
    const char *name = schema->name;
    if (strcmp(name, "id") && row % 5 == 0) return ArrowArrayAppendNull(dest, 1);
    if (!strcmp(name, "tags")) {
        for (int i = 0; i < 3; ++i)
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(dest->children[0], row + i));
        return ArrowArrayFinishElement(dest);
    }
    if (!strcmp(name, "attrs")) {
        for (int64_t i = 0; i < schema->n_children; ++i) {
            if (!strcmp(schema->children[i]->name, "x")) {
                NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(dest->children[i], row));
            } else {
                char value[64];
                snprintf(value, sizeof(value), "y%lld", (long long)row);
                NANOARROW_RETURN_NOT_OK(
                    ArrowArrayAppendString(dest->children[i], ArrowCharView(value)));
            }
        }
        return ArrowArrayFinishElement(dest);
    }
    if (!strcmp(name, "label")) {
        char value[64];
        snprintf(value, sizeof(value), "label-%lld", (long long)row);
        return ArrowArrayAppendString(dest, ArrowCharView(value));
    }
    if (!strcmp(name, "amt")) {
        struct ArrowDecimal value;
        ArrowDecimalInit(&value, 128, 18, 2);
        ArrowDecimalSetInt(&value, row * 100 + row % 100);
        return ArrowArrayAppendDecimal(dest, &value);
    }
    return ArrowArrayAppendInt(dest, row);
}
static int exchange(struct ExampleFunction *f, struct VgiFunctionCall *call,
                    const struct VgiBatch *input, struct VgiBatch *out,
                    struct ArrowBuffer *metadata) {
    int confirmed = 0, rc = cache_metadata(f, call, metadata, &confirmed);
    if (!rc) rc = VgiBatchInit(out, &call->output_schema, &call->rpc->error);
    struct ArrowBuffer parents;
    ArrowBufferInit(&parents);
    int64_t widest = 1;
    if (named(f, "cached_explode")) {
        widest = 0;
        for (int64_t row = 0; row < input->array.length; ++row)
            if (!ArrowArrayViewIsNull(input->view.children[0], row)) {
                int64_t n = ArrowArrayViewGetIntUnsafe(input->view.children[0], row);
                if (n > widest) widest = n;
            }
    }
    if (named(f, "secret_cached_lateral")) {
        const struct ArrowSchema *schema = NULL;
        const struct ArrowArrayView *secret = VgiSecretSelect(call, "vgi_example", NULL, &schema);
        struct ArrowStringView value =
            VgiValueString(VgiStructField(secret, schema, "secret_string"), 0);
        uint64_t random;
        randombytes_buf(&random, sizeof(random));
        int64_t nonce = (int64_t)(random >> 8);
        for (int64_t row = 0; !rc && row < input->array.length; ++row) {
            for (int64_t col = 0; !rc && col < out->schema.n_children; ++col) {
                if (!strcmp(out->schema.children[col]->name, "secret_string"))
                    rc = value.data ? ArrowArrayAppendString(out->array.children[col], value)
                                    : ArrowArrayAppendNull(out->array.children[col], 1);
                else
                    rc = ArrowArrayAppendInt(out->array.children[col], nonce);
            }
            if (!rc) rc = ArrowArrayFinishElement(&out->array);
        }
        widest = 0;
    }
    for (int64_t k = 0; !rc && !confirmed && k < widest; ++k)
        for (int64_t row = 0; !rc && row < input->array.length; ++row) {
            if (named(f, "cached_echo") || named(f, "cached_reval_echo")) {
                rc = VgiArrayAppendValue(&out->array, &out->schema, &input->view, &input->schema,
                                         row, &call->rpc->error);
                continue;
            }
            int null = ArrowArrayViewIsNull(input->view.children[0], row);
            int64_t value = null ? 0 : ArrowArrayViewGetIntUnsafe(input->view.children[0], row);
            if (named(f, "cached_explode")) {
                if (null || value <= k) continue;
                if (row > INT32_MAX) {
                    rc = EOVERFLOW;
                    break;
                }
                uint8_t id[4];
                for (int i = 0; i < 4; ++i) id[i] = (uint8_t)((uint64_t)row >> (8 * i));
                rc = ArrowBufferAppend(&parents, id, 4);
                value = k;
            } else
                value = (int64_t)((uint64_t)value * 2);
            if (!rc)
                rc = null ? ArrowArrayAppendNull(out->array.children[0], 1)
                          : ArrowArrayAppendInt(out->array.children[0], value);
            if (!rc) rc = ArrowArrayFinishElement(&out->array);
        }
    if (!rc && named(f, "cached_explode")) {
        size_t size =
            sodium_base64_ENCODED_LEN((size_t)parents.size_bytes, sodium_base64_VARIANT_ORIGINAL);
        char *encoded = malloc(size);
        if (!encoded)
            rc = ENOMEM;
        else {
            sodium_bin2base64(encoded, size, parents.data, (size_t)parents.size_bytes,
                              sodium_base64_VARIANT_ORIGINAL);
            rc = VgiMetadataSet(metadata, "vgi_rpc.parent_row#b64", encoded);
            free(encoded);
        }
    }
    ArrowBufferReset(&parents);
    if (!rc) rc = VgiBatchFinish(out, &call->rpc->error);
    return rc;
}
static int partition_process(struct ExampleFunction *f, struct VgiFunctionCall *call,
                             struct VgiBatch *out, struct ArrowBuffer *metadata, int *finished) {
    struct CacheState *s = call->state;
    int rc = 0;
    if (named(f, "cache_partition_parallel")) {
        struct ArrowBuffer item;
        ArrowBufferInit(&item);
        rc =
            VgiStoragePop(call->storage, scope(call), "cache.partitions", &item, &call->rpc->error);
        if (rc == ENOENT) {
            *finished = 1;
            rc = 0;
        } else if (!rc && (item.size_bytes != 1 || item.data[0] > 3))
            rc = EINVAL;
        else if (!rc)
            s->index = item.data[0];
        ArrowBufferReset(&item);
        if (rc || *finished) return rc;
    }
    if (s->index >= s->count) {
        *finished = 1;
        return 0;
    }
    const char *countries[] = {"AU", "BR", "CA", "FR", "US"},
               *parallel[] = {"AU", "CA", "US", NULL}, *projection[] = {"CA", "US"},
               *regions[] = {"EU", "US"};
    const char *country = named(f, "cache_partition_parallel") ? parallel[s->index]
                          : named(f, "cache_partition_proj")   ? projection[s->index]
                                                               : countries[s->index];
    int64_t rows = VgiArgumentInt(call, 0, NULL, 1);
    if (rows < 0) rows = 0;
    rc = VgiBatchInit(out, &s->schema, &call->rpc->error);
    struct VgiBatch one = {0};
    if (!rc) rc = VgiBatchInit(&one, &s->schema, &call->rpc->error);
    for (int pass = 0; !rc && pass < 2; ++pass) {
        struct VgiBatch *dest = pass ? out : &one;
        int64_t n = pass ? rows : 1;
        for (int64_t row = 0; !rc && row < n; ++row) {
            for (int64_t col = 0; !rc && col < dest->schema.n_children; ++col) {
                const char *name = dest->schema.children[col]->name;
                struct ArrowArray *a = dest->array.children[col];
                if (!strcmp(name, "country"))
                    rc = country ? ArrowArrayAppendString(a, ArrowCharView(country))
                                 : ArrowArrayAppendNull(a, 1);
                else if (!strcmp(name, "region"))
                    rc = ArrowArrayAppendString(a, ArrowCharView(regions[s->index / 2]));
                else if (!strcmp(name, "year"))
                    rc = ArrowArrayAppendInt(a, s->index % 2 ? 2022 : 2020);
                else
                    rc = ArrowArrayAppendInt(
                        a, s->index * (named(f, "cache_partition_multicol") ? 1000 : 1000000) +
                               row + (!strcmp(name, "extra") ? 500 : 0));
            }
            if (!rc) rc = ArrowArrayFinishElement(&dest->array);
        }
        if (!rc) rc = VgiBatchFinish(dest, &call->rpc->error);
    }
    if (!rc) rc = VgiPartitionMetadata(&s->schema, &one, metadata, &call->rpc->error);
    int confirmed = 0;
    if (!rc && (!s->emitted || !named(f, "cache_partitioned")))
        rc = cache_metadata(f, call, metadata, &confirmed);
    VgiBatchReset(&one);
    ++s->emitted;
    ++s->index;
    return rc;
}
int ExampleCacheProcess(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                        struct VgiBatch *out, struct ArrowBuffer *metadata, int *finished) {
    struct ExampleFunction *f = data;
    struct CacheState *s = call->state;
    if (ExampleCacheExchange(f->name)) return exchange(f, call, input, out, metadata);
    if (partitioned(f)) return partition_process(f, call, out, metadata, finished);
    int rc = 0;
    if (named(f, "cache_parallel") && s->index >= s->count) {
        struct ArrowBuffer item;
        ArrowBufferInit(&item);
        rc = VgiStoragePop(call->storage, scope(call), "cache.ranges", &item, &call->rpc->error);
        if (rc == ENOENT) {
            *finished = 1;
            rc = 0;
        } else if (!rc && item.size_bytes != 16)
            rc = EINVAL;
        else if (!rc) {
            uint64_t start = 0, end = 0;
            for (int i = 0; i < 8; ++i) {
                start |= (uint64_t)item.data[i] << (8 * i);
                end |= (uint64_t)item.data[i + 8] << (8 * i);
            }
            s->index = (int64_t)start;
            s->count = (int64_t)end;
        }
        ArrowBufferReset(&item);
        if (rc || *finished) return rc;
    }
    if (s->index >= s->count && (s->emitted || !named(f, "cache_scoped_txn"))) {
        if (named(f, "cache_poison"))
            return VgiRpcFail(call->rpc, "RuntimeError", "INTERNAL", "",
                              "cache_poison: intentional mid-stream failure");
        if (named(f, "cache_external_fail") && s->emitted == 1) {
            s->emitted++;
            rc = VgiBatchInit(out, &call->output_schema, &call->rpc->error);
            if (!rc)
                rc = VgiMetadataSet(metadata, "vgi_rpc.location",
                                    "http://127.0.0.1:9/vgi-cache-poison-nonexistent");
            if (!rc) rc = VgiBatchFinish(out, &call->rpc->error);
            return rc;
        }
        *finished = 1;
        return 0;
    }
    int confirmed = 0;
    if (!s->emitted) rc = cache_metadata(f, call, metadata, &confirmed);
    s->emitted++;
    if (!rc)
        rc = VgiBatchInit(out, named(f, "cache_filtered") ? &s->schema : &call->output_schema,
                          &call->rpc->error);
    int64_t rows = s->count - s->index;
    if (rows > s->batch) rows = s->batch;
    if (confirmed) {
        rows = 0;
        s->index = s->count;
    }
    for (int64_t i = 0; !rc && i < rows; ++i) {
        int64_t row = s->index + i;
        for (int64_t col = 0; !rc && col < out->schema.n_children; ++col) {
            const char *name = out->schema.children[col]->name;
            struct ArrowArray *dest = out->array.children[col];
            if (named(f, "secret_cache_nonce") && !strcmp(name, "secret_string")) {
                const struct ArrowSchema *schema = NULL;
                const struct ArrowArrayView *secret =
                    VgiSecretSelect(call, "vgi_example", NULL, &schema);
                struct ArrowStringView value =
                    VgiValueString(VgiStructField(secret, schema, "secret_string"), 0);
                rc = value.data ? ArrowArrayAppendString(dest, value)
                                : ArrowArrayAppendNull(dest, 1);
            } else if (named(f, "cache_types"))
                rc = types_value(dest, out->schema.children[col], row);
            else if (named(f, "cache_whoami"))
                rc = ArrowArrayAppendString(
                    dest, ArrowCharView(call->rpc->principal ? call->rpc->principal : "anonymous"));
            else if (named(f, "test_same_name_cached"))
                rc = ArrowArrayAppendString(dest, ArrowCharView(f->path));
            else {
                int64_t value = row;
                if (!strcmp(name, "nonce"))
                    value = s->nonce;
                else if (named(f, "cache_multicol") || named(f, "cache_projection")) {
                    value = (int64_t)((uint64_t)(row + named(f, "cache_projection")) *
                                      (!strcmp(name, "b")   ? 10
                                       : !strcmp(name, "c") ? 100
                                                            : 1));
                } else if (named(f, "cache_versioned_scan")) {
                    int64_t version = VgiArgumentInt(call, 0, NULL, 3);
                    value = (version == 1 ? 100 : version == 2 ? 200 : 300) + row + 1;
                }
                rc = ArrowArrayAppendInt(dest, value);
            }
        }
        if (!rc) rc = ArrowArrayFinishElement(&out->array);
    }
    s->index += rows;
    if (!rc) rc = VgiBatchFinish(out, &call->rpc->error);
    return rc;
}
int64_t ExampleCacheWorkers(void *data, struct VgiFunctionCall *call) {
    (void)call;
    return named(data, "cache_parallel") || named(data, "cache_partition_parallel") ? 8 : 1;
}
