#include <ctype.h>
#include <errno.h>
#include <json-c/json.h>
#include <limits.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>

#include "catalog_internal.h"
#include "memory_catalog_internal.h"
#include "vgi/storage.h"
struct Memory {
    struct VgiBatch info, attach, initial;
    int unversioned;
    enum VgiCatalogEtag etag;
};
struct VgiMemoryService {
    struct Memory *items;
    size_t count;
};
struct Objects {
    struct VgiBatch *items;
    size_t count;
};
struct Schema {
    struct VgiBatch info;
    struct Objects tables, views;
};
struct Snapshot {
    struct Schema *schemas;
    size_t count;
    int64_t version;
};
static int is(struct ArrowStringView a, const char *b) {
    return a.size_bytes == (int64_t)strlen(b) && !memcmp(a.data, b, (size_t)a.size_bytes);
}
static int text_compare(struct ArrowStringView a, struct ArrowStringView b, int fold) {
    int64_t n = a.size_bytes < b.size_bytes ? a.size_bytes : b.size_bytes;
    for (int64_t i = 0; i < n; ++i) {
        int x = (unsigned char)a.data[i], y = (unsigned char)b.data[i];
        if (fold) {
            x = tolower(x);
            y = tolower(y);
        }
        if (x != y) return x < y ? -1 : 1;
    }
    return (a.size_bytes > b.size_bytes) - (a.size_bytes < b.size_bytes);
}
static int path_compare(const struct ArrowArrayView *a, int64_t row, const struct ArrowArrayView *b,
                        int64_t other, int prefix) {
    if (!a || !b) return a ? 1 : b ? -1 : 0;
    int64_t as = ArrowArrayViewListChildOffset(a, row),
            ae = ArrowArrayViewListChildOffset(a, row + 1),
            bs = ArrowArrayViewListChildOffset(b, other),
            be = ArrowArrayViewListChildOffset(b, other + 1);
    int64_t n = ae - as < be - bs ? ae - as : be - bs;
    for (int64_t i = 0; i < n; ++i) {
        int c = text_compare(VgiValueString(a->children[0], as + i),
                             VgiValueString(b->children[0], bs + i), 0);
        if (c) return c;
    }
    return prefix && be - bs <= ae - as ? 0 : ((ae - as) > (be - bs)) - ((ae - as) < (be - bs));
}
static int append_batch(struct ArrowArray *list, const struct VgiBatch *batch,
                        struct ArrowError *error) {
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    int rc = VgiIpcWriteBatch(&batch->schema, &batch->array, NULL, &bytes, error);
    if (!rc)
        rc = ArrowArrayAppendBytes(list->children[0],
                                   (struct ArrowBufferView){{bytes.data}, bytes.size_bytes});
    ArrowBufferReset(&bytes);
    return rc;
}
static int copy_field(struct VgiBatch *out, const char *dest, const struct VgiBatch *source,
                      const char *name, struct ArrowError *error) {
    int64_t a = VgiFieldIndex(&out->schema, dest), b = VgiFieldIndex(&source->schema, name);
    if (a < 0 || b < 0) return EINVAL;
    return VgiArrayAppendValue(out->array.children[a], out->schema.children[a],
                               source->view.children[b], source->schema.children[b], 0, error);
}
static void objects_free(struct Objects *o) {
    for (size_t i = 0; i < o->count; ++i) VgiBatchReset(&o->items[i]);
    free(o->items);
    memset(o, 0, sizeof(*o));
}
static void schema_free(struct Schema *s) {
    VgiBatchReset(&s->info);
    objects_free(&s->tables);
    objects_free(&s->views);
}
static void snapshot_free(struct Snapshot *s) {
    for (size_t i = 0; i < s->count; ++i) schema_free(&s->schemas[i]);
    free(s->schemas);
    memset(s, 0, sizeof(*s));
}
static int objects_load(const struct ArrowArrayView *list, int64_t row, struct Objects *o,
                        struct ArrowError *error) {
    int64_t start = ArrowArrayViewListChildOffset(list, row),
            end = ArrowArrayViewListChildOffset(list, row + 1);
    o->items = calloc((size_t)(end - start), sizeof(*o->items));
    if (end > start && !o->items) return ENOMEM;
    o->count = (size_t)(end - start);
    for (int64_t i = start; i < end; ++i) {
        struct ArrowStringView bytes = VgiValueString(list->children[0], i);
        NANOARROW_RETURN_NOT_OK(VgiBatchDecode(
            &o->items[i - start], (struct ArrowBufferView){{bytes.data}, bytes.size_bytes}, error));
    }
    return 0;
}
static int snapshot_load(const struct VgiBatch *batch, struct Snapshot *s,
                         struct ArrowError *error) {
    s->version = VgiBatchInt(batch, "catalog_version", 1);
    const struct ArrowArrayView *list = VgiBatchGet(batch, "schemas"), *rows = list->children[0];
    const struct ArrowSchema *type =
        batch->schema.children[VgiFieldIndex(&batch->schema, "schemas")]->children[0];
    s->count = (size_t)rows->length;
    s->schemas = calloc(s->count, sizeof(*s->schemas));
    if (s->count && !s->schemas) return ENOMEM;
    for (size_t i = 0; i < s->count; ++i) {
        struct ArrowStringView bytes =
            VgiValueString(rows->children[VgiFieldIndex(type, "schema")], (int64_t)i);
        NANOARROW_RETURN_NOT_OK(VgiBatchDecode(
            &s->schemas[i].info, (struct ArrowBufferView){{bytes.data}, bytes.size_bytes}, error));
        NANOARROW_RETURN_NOT_OK(objects_load(rows->children[VgiFieldIndex(type, "tables")],
                                             (int64_t)i, &s->schemas[i].tables, error));
        NANOARROW_RETURN_NOT_OK(objects_load(rows->children[VgiFieldIndex(type, "views")],
                                             (int64_t)i, &s->schemas[i].views, error));
    }
    return 0;
}
static int object_sort(const void *a, const void *b) {
    return text_compare(VgiBatchString(a, "name"), VgiBatchString(b, "name"), 1);
}
static int schema_sort(const void *a, const void *b) {
    const struct ArrowArrayView *x = VgiBatchGet(&((const struct Schema *)a)->info, "path"),
                                *y = VgiBatchGet(&((const struct Schema *)b)->info, "path");
    int64_t n = ArrowArrayViewListChildOffset(x, 1) - ArrowArrayViewListChildOffset(x, 0),
            m = ArrowArrayViewListChildOffset(y, 1) - ArrowArrayViewListChildOffset(y, 0);
    return n == m ? path_compare(x, 0, y, 0, 0) : (n > m) - (n < m);
}
static int snapshot_encode(struct Snapshot *s, struct VgiBatch *out, struct ArrowError *error) {
    int rc = VgiCatalogContentsInit(out, error);
    if (!rc) rc = VgiBatchSetInt(out, "catalog_version", s->version);
    struct ArrowArray *list = VgiBatchColumn(out, "schemas"),
                      *entry = list ? list->children[0] : NULL;
    const struct ArrowSchema *type =
        list ? out->schema.children[VgiFieldIndex(&out->schema, "schemas")]->children[0] : NULL;
    if (s->count > 1) qsort(s->schemas, s->count, sizeof(*s->schemas), schema_sort);
    for (size_t i = 0; !rc && i < s->count; ++i) {
        struct Schema *schema = &s->schemas[i];
        if (schema->tables.count > 1)
            qsort(schema->tables.items, schema->tables.count, sizeof(*schema->tables.items),
                  object_sort);
        if (schema->views.count > 1)
            qsort(schema->views.items, schema->views.count, sizeof(*schema->views.items),
                  object_sort);
        for (int64_t col = 0; !rc && col < type->n_children; ++col) {
            const char *name = type->children[col]->name;
            struct ArrowArray *dest = entry->children[col];
            if (!strcmp(name, "path")) {
                int64_t id = VgiFieldIndex(&schema->info.schema, "path");
                rc = VgiArrayAppendValue(dest, type->children[col], schema->info.view.children[id],
                                         schema->info.schema.children[id], 0, error);
            } else if (!strcmp(name, "schema")) {
                struct ArrowBuffer bytes;
                ArrowBufferInit(&bytes);
                rc = VgiIpcWriteBatch(&schema->info.schema, &schema->info.array, NULL, &bytes,
                                      error);
                if (!rc)
                    rc = ArrowArrayAppendBytes(
                        dest, (struct ArrowBufferView){{bytes.data}, bytes.size_bytes});
                ArrowBufferReset(&bytes);
            } else {
                struct Objects *objects = !strcmp(name, "tables")  ? &schema->tables
                                          : !strcmp(name, "views") ? &schema->views
                                                                   : NULL;
                for (size_t j = 0; !rc && objects && j < objects->count; ++j)
                    rc = append_batch(dest, &objects->items[j], error);
                if (!rc) rc = ArrowArrayFinishElement(dest);
            }
        }
        if (!rc) rc = ArrowArrayFinishElement(entry);
    }
    if (!rc) rc = ArrowArrayFinishElement(list);
    if (!rc) rc = VgiBatchFinishRow(out, error);
    return rc;
}
void VgiMemoryFree(struct VgiMemoryService *service) {
    if (!service) return;
    for (size_t i = 0; i < service->count; ++i) {
        VgiBatchReset(&service->items[i].info);
        VgiBatchReset(&service->items[i].attach);
        VgiBatchReset(&service->items[i].initial);
    }
    free(service->items);
    free(service);
}
int VgiMemoryAdd(struct VgiMemoryService **service, struct VgiCatalogService **catalogs,
                 const struct VgiMemoryCatalogOptions *options, struct ArrowError *error) {
    if (!options || !options->name || !*options->name) return EINVAL;
    if (!*service) {
        *service = calloc(1, sizeof(**service));
        if (!*service) return ENOMEM;
    }
    struct Memory m = {.unversioned = options->unversioned, .etag = options->etag};
    struct Snapshot s = {0};
    int rc = VgiBatchInitNamed(&m.info, "CatalogInfo", error);
    if (!rc) rc = VgiBatchSetString(&m.info, "name", options->name);
    if (!rc) rc = VgiBatchFinishRow(&m.info, error);
    if (!rc) rc = VgiBatchInitNamed(&m.attach, "CatalogAttachResult", error);
    if (!rc) rc = VgiBatchSetInt(&m.attach, "catalog_version", options->unversioned ? 0 : 1);
    if (!rc) rc = VgiBatchSetInt(&m.attach, "attach_opaque_data_required", 1);
    if (!rc) rc = VgiBatchSetString(&m.attach, "default_schema", "main");
    if (!rc) rc = VgiBatchSetInt(&m.attach, "supports_catalog_contents", 1);
    if (!rc && options->comment) rc = VgiBatchSetString(&m.attach, "comment", options->comment);
    if (!rc) rc = VgiBatchFinishRow(&m.attach, error);
    if (!rc) {
        s.schemas = calloc(1, sizeof(*s.schemas));
        if (!s.schemas)
            rc = ENOMEM;
        else {
            s.count = 1;
            s.version = options->unversioned ? 0 : 1;
        }
    }
    if (!rc) rc = VgiBatchInitNamed(&s.schemas[0].info, "SchemaInfo", error);
    if (!rc) {
        struct ArrowArray *path = VgiBatchColumn(&s.schemas[0].info, "path");
        rc = ArrowArrayAppendString(path->children[0], ArrowCharView("main"));
        if (!rc) rc = ArrowArrayFinishElement(path);
    }
    if (!rc) rc = VgiBatchFinishRow(&s.schemas[0].info, error);
    if (!rc) rc = snapshot_encode(&s, &m.initial, error);
    struct Memory *next = NULL;
    if (!rc) {
        next = realloc((*service)->items, ((*service)->count + 1) * sizeof(*next));
        if (!next)
            rc = ENOMEM;
        else
            (*service)->items = next;
    }
    if (!rc) rc = VgiCatalogAdd(catalogs, &m.info, &m.attach, &m.initial, NULL, error);
    if (!rc)
        (*service)->items[(*service)->count++] = m;
    else {
        VgiBatchReset(&m.info);
        VgiBatchReset(&m.attach);
        VgiBatchReset(&m.initial);
    }
    snapshot_free(&s);
    return rc;
}
static int failure(struct VgiRpcCall *call, const char *text) {
    return VgiRpcFail(call, "ValueError", "INVALID_ARGUMENT", "", text);
}
static int conflict(struct VgiRpcCall *call, const struct VgiBatch *params, int *replace) {
    struct ArrowStringView action = VgiBatchString(params, "on_conflict");
    *replace = is(action, "REPLACE") || is(action, "replace");
    if (*replace || is(action, "IGNORE") || is(action, "ignore")) return 0;
    return failure(call, "An object with this name already exists");
}
static int mutate(struct Snapshot *s, const char *method, const struct VgiBatch *p,
                  struct VgiRpcCall *call, int *changed) {
    *changed = 0;
    const struct ArrowArrayView *path = VgiBatchGet(p, "schema_path");
    if (!path) path = VgiBatchGet(p, "path");
    size_t si = 0;
    for (; si < s->count; ++si)
        if (!path_compare(VgiBatchGet(&s->schemas[si].info, "path"), 0, path, 0, 0)) break;
    if (!strcmp(method, "catalog_schema_create")) {
        if (!path ||
            ArrowArrayViewListChildOffset(path, 0) == ArrowArrayViewListChildOffset(path, 1))
            return failure(call, "schema path is empty");
        if (si < s->count) {
            int replace = 0;
            NANOARROW_RETURN_NOT_OK(conflict(call, p, &replace));
            if (!replace) return 0;
        } else {
            int64_t depth =
                ArrowArrayViewListChildOffset(path, 1) - ArrowArrayViewListChildOffset(path, 0);
            if (depth > 1) {
                int parent = 0;
                for (size_t i = 0; i < s->count; ++i) {
                    const struct ArrowArrayView *other = VgiBatchGet(&s->schemas[i].info, "path");
                    int64_t n = ArrowArrayViewListChildOffset(other, 1) -
                                ArrowArrayViewListChildOffset(other, 0);
                    if (n == depth - 1 && !path_compare(path, 0, other, 0, 1)) parent = 1;
                }
                if (!parent) return failure(call, "Parent schema does not exist");
            }
            struct Schema *next = realloc(s->schemas, (s->count + 1) * sizeof(*next));
            if (!next) return ENOMEM;
            s->schemas = next;
            memset(&s->schemas[s->count++], 0, sizeof(*next));
        }
        struct Schema *schema = &s->schemas[si];
        schema_free(schema);
        int rc = VgiBatchInitNamed(&schema->info, "SchemaInfo", &call->error);
        if (!rc) rc = copy_field(&schema->info, "path", p, "path", &call->error);
        if (!rc && VgiBatchGet(p, "comment"))
            rc = copy_field(&schema->info, "comment", p, "comment", &call->error);
        if (!rc) rc = VgiBatchFinishRow(&schema->info, &call->error);
        *changed = !rc;
        return rc;
    }
    int drop_schema = !strcmp(method, "catalog_schema_drop"),
        drop = !strcmp(method, "catalog_table_drop") || !strcmp(method, "catalog_view_drop"),
        create = !strcmp(method, "catalog_table_create") || !strcmp(method, "catalog_view_create"),
        view = strstr(method, "_view_") != NULL;
    if (!drop_schema && !drop && !create)
        return failure(call, "This operation is not supported by a metadata catalog");
    if (si == s->count) {
        if ((drop || drop_schema) && VgiBatchInt(p, "ignore_not_found", 0)) return 0;
        return failure(call, "Schema does not exist");
    }
    if (drop_schema) {
        int children = 0;
        for (size_t i = 0; i < s->count; ++i)
            if (i != si && !path_compare(VgiBatchGet(&s->schemas[i].info, "path"), 0, path, 0, 1))
                children = 1;
        if ((children || s->schemas[si].tables.count || s->schemas[si].views.count) &&
            !VgiBatchInt(p, "cascade", 0))
            return failure(call, "Schema is not empty; use CASCADE");
        for (size_t i = s->count; i-- > 0;)
            if (!path_compare(VgiBatchGet(&s->schemas[i].info, "path"), 0, path, 0, 1)) {
                schema_free(&s->schemas[i]);
                memmove(&s->schemas[i], &s->schemas[i + 1],
                        (s->count - i - 1) * sizeof(*s->schemas));
                --s->count;
            }
        *changed = 1;
        return 0;
    }
    struct Objects *objects = view ? &s->schemas[si].views : &s->schemas[si].tables;
    struct ArrowStringView name = VgiBatchString(p, "name");
    size_t oi = 0;
    for (; oi < objects->count; ++oi)
        if (!text_compare(name, VgiBatchString(&objects->items[oi], "name"), 1)) break;
    if (drop) {
        if (oi == objects->count)
            return VgiBatchInt(p, "ignore_not_found", 0) ? 0
                                                         : failure(call, "Object does not exist");
        VgiBatchReset(&objects->items[oi]);
        memmove(&objects->items[oi], &objects->items[oi + 1],
                (objects->count - oi - 1) * sizeof(*objects->items));
        --objects->count;
        *changed = 1;
        return 0;
    }
    if (oi < objects->count) {
        int replace = 0;
        NANOARROW_RETURN_NOT_OK(conflict(call, p, &replace));
        if (!replace) return 0;
    } else {
        struct VgiBatch *next = realloc(objects->items, (objects->count + 1) * sizeof(*next));
        if (!next) return ENOMEM;
        objects->items = next;
        memset(&objects->items[objects->count++], 0, sizeof(*next));
    }
    if (!view && !VgiBatchBytes(p, "columns").size_bytes)
        return failure(call, "catalog_table_create: missing columns schema");
    struct VgiBatch *object = &objects->items[oi];
    VgiBatchReset(object);
    int rc = VgiBatchInitNamed(object, view ? "ViewInfo" : "TableInfo", &call->error);
    for (int64_t i = 0; !rc && i < object->schema.n_children; ++i) {
        const char *field = object->schema.children[i]->name;
        if (VgiBatchGet(p, field)) rc = copy_field(object, field, p, field, &call->error);
    }
    if (!rc) rc = VgiBatchFinishRow(object, &call->error);
    *changed = !rc;
    return rc;
}
static int store_batch(struct VgiStorage *store, struct ArrowBufferView scope,
                       const struct VgiBatch *batch, struct ArrowError *error) {
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    int rc = VgiIpcWriteBatch(&batch->schema, &batch->array, NULL, &bytes, error);
    if (!rc)
        rc = VgiStoragePut(store, scope, "memory.contents",
                           (struct ArrowBufferView){{bytes.data}, bytes.size_bytes}, error);
    ArrowBufferReset(&bytes);
    return rc;
}
static int contents_reply(struct Memory *memory, const struct VgiBatch *contents,
                          struct VgiRpcCall *call, struct VgiRpcReply *reply) {
    char etag[80] = {0};
    struct ArrowBuffer collections[VGI_CATALOG_COLLECTIONS];
    int rc = VgiCatalogCollections(contents, collections, &call->error);
    if (!rc && memory->etag == VGI_CATALOG_ETAG_GENERATION)
        snprintf(etag, sizeof(etag), "gen-%lld",
                 (long long)VgiBatchInt(contents, "catalog_version", 1));
    else if (!rc && memory->etag == VGI_CATALOG_ETAG_CONTENT_HASH)
        VgiCatalogCollectionsDigest(collections, etag);
    int unchanged = etag[0] && is(VgiBatchString(&call->request, "if_none_match"), etag);
    struct VgiBatch result = {0};
    if (!rc) rc = VgiBatchInitNamed(&result, "CatalogContentsResult", &call->error);
    if (!rc)
        rc =
            VgiBatchSetInt(&result, "catalog_version", VgiBatchInt(contents, "catalog_version", 1));
    if (!rc && etag[0]) rc = VgiBatchSetString(&result, "etag", etag);
    if (!rc) rc = VgiBatchSetInt(&result, "not_modified", unchanged);
    /* A not_modified answer carries six null collections. */
    if (!rc && unchanged) VgiCatalogCollectionsReset(collections);
    if (!rc) rc = VgiCatalogCollectionsSet(&result, collections);
    if (!rc) rc = VgiBatchFinishRow(&result, &call->error);
    if (!rc) rc = VgiRpcReplyPayload(call, reply, &result);
    VgiBatchReset(&result);
    VgiCatalogCollectionsReset(collections);
    return rc;
}
int VgiMemoryDispatch(struct VgiMemoryService *service, struct VgiCatalogService *catalogs,
                      struct VgiRpcCall *call, struct VgiRpcReply *reply, int *handled) {
    *handled = 0;
    if (!service) return 0;
    const char *method = call->method->name;
    struct VgiBatch decoded = {0};
    const struct VgiBatch *params = &call->request;
    int rc = 0;
    if (VgiBatchGet(params, "request") && !VgiBatchGet(params, "attach_opaque_data")) {
        rc = VgiBatchChild(params, "request", &decoded, &call->error);
        if (rc) return rc;
        params = &decoded;
    }
    int attach = !strcmp(method, "catalog_attach");
    struct ArrowStringView name =
        attach ? VgiBatchString(params, "name") : (struct ArrowStringView){0};
    struct ArrowBufferView cookie = VgiBatchBytes(params, "attach_opaque_data");
    struct json_object *object = NULL, *catalog = NULL;
    if (!attach && cookie.size_bytes > 0 && cookie.size_bytes < INT_MAX) {
        struct json_tokener *parser = json_tokener_new();
        if (!parser) {
            VgiBatchReset(&decoded);
            return ENOMEM;
        }
        object = json_tokener_parse_ex(parser, cookie.data.as_char, (int)cookie.size_bytes);
        json_tokener_free(parser);
        if (object && json_object_object_get_ex(object, "catalog", &catalog) &&
            json_object_is_type(catalog, json_type_string))
            name = ArrowCharView(json_object_get_string(catalog));
    }
    struct Memory *memory = NULL;
    for (size_t i = 0; i < service->count; ++i)
        if (!text_compare(name, VgiBatchString(&service->items[i].info, "name"), 0)) {
            memory = &service->items[i];
            break;
        }
    if (object) json_object_put(object);
    if (!memory) {
        VgiBatchReset(&decoded);
        return 0;
    }
    *handled = 1;
    struct VgiStorage *store = NULL;
    struct VgiBatch contents = {0}, result = {0};
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    struct VgiCatalogService *temporary = NULL;
    int transaction = 0;
    rc = VgiStorageOpen(NULL, &store, &call->error);
    if (!rc && attach) {
        int declared = 0;
        rc = VgiCatalogDispatch(catalogs, NULL, call, reply, &declared);
        if (!rc && !declared) rc = EINVAL;
        if (!rc) rc = VgiBatchChild(&reply->value, "result", &result, &call->error);
        if (!rc)
            rc = store_batch(store, VgiBatchBytes(&result, "attach_opaque_data"), &memory->initial,
                             &call->error);
        goto done;
    }
    if (!rc && !strcmp(method, "catalog_detach")) {
        rc = VgiStorageClear(store, cookie, &call->error);
        goto done;
    }
    if (!rc && call->method->kind == VGI_METHOD_VOID && !strstr(method, "transaction_")) {
        rc = VgiStorageBegin(store, &call->error);
        transaction = !rc;
    }
    if (!rc) rc = VgiStorageGet(store, cookie, "memory.contents", &bytes, &call->error);
    if (rc == ENOENT) rc = failure(call, "Catalog is not attached");
    if (!rc)
        rc = VgiBatchDecode(&contents, (struct ArrowBufferView){{bytes.data}, bytes.size_bytes},
                            &call->error);
    if (!rc && transaction) {
        struct Snapshot snapshot = {0};
        int changed = 0;
        rc = snapshot_load(&contents, &snapshot, &call->error);
        if (!rc) rc = mutate(&snapshot, method, params, call, &changed);
        if (!rc && changed) {
            if (!memory->unversioned && snapshot.version == INT64_MAX)
                rc = EOVERFLOW;
            else if (!memory->unversioned)
                ++snapshot.version;
            if (!rc) rc = snapshot_encode(&snapshot, &result, &call->error);
            if (!rc) rc = store_batch(store, cookie, &result, &call->error);
        }
        snapshot_free(&snapshot);
        if (!rc) rc = VgiStorageCommit(store, &call->error);
        if (rc) VgiStorageRollback(store);
        transaction = 0;
        goto done;
    }
    if (!rc && !strcmp(method, "catalog_transaction_begin")) {
        rc = VgiBatchInitNamed(&result, "CatalogTransactionBeginResult", &call->error);
        if (!rc) rc = VgiBatchFinishRow(&result, &call->error);
        if (!rc) rc = VgiRpcReplyPayload(call, reply, &result);
        goto done;
    }
    if (!rc && (!strcmp(method, "catalog_transaction_commit") ||
                !strcmp(method, "catalog_transaction_rollback")))
        goto done;
    if (!rc && !strcmp(method, "catalog_copy_from_formats")) {
        rc = VgiBatchInitNamed(&result, "CatalogCopyFromFormatsResult", &call->error);
        if (!rc) rc = VgiBatchFinishRow(&result, &call->error);
        if (!rc) rc = VgiRpcReplyPayload(call, reply, &result);
        goto done;
    }
    if (!rc && !strcmp(method, "catalog_contents")) {
        rc = contents_reply(memory, &contents, call, reply);
        goto done;
    }
    if (!rc)
        rc = VgiCatalogAdd(&temporary, &memory->info, &memory->attach, &contents, NULL,
                           &call->error);
    if (!rc) {
        int served = 0;
        rc = VgiCatalogDispatch(temporary, NULL, call, reply, &served);
        if (!rc && !served)
            rc = failure(call, "This operation is not supported by a metadata catalog");
    }
done:
    if (transaction) VgiStorageRollback(store);
    VgiStorageClose(store);
    VgiCatalogFree(temporary);
    VgiBatchReset(&decoded);
    VgiBatchReset(&contents);
    VgiBatchReset(&result);
    ArrowBufferReset(&bytes);
    return rc;
}
