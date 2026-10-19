#include <errno.h>
#include <json-c/json.h>
#include <limits.h>
#include <string.h>
#include <strings.h>

#include "catalog_internal.h"
#include "vgi/storage.h"

/* Option values are never embedded in an unsealed attachment handle. Each
 * attachment's random identifier scopes its private, cross-process state. */
int VgiCatalogLoadOptions(struct ArrowBufferView cookie, struct VgiBatch *out,
                          struct ArrowError *error) {
    if (cookie.size_bytes <= 0 || cookie.size_bytes > INT_MAX) return 0;
    struct json_tokener *parser = json_tokener_new();
    if (!parser) return ENOMEM;
    struct json_object *object = json_tokener_parse_ex(parser, cookie.data.as_char,
                                                       (int)cookie.size_bytes),
                       *present = NULL;
    int has = object && json_object_object_get_ex(object, "options", &present) &&
              json_object_get_boolean(present);
    if (object) json_object_put(object);
    json_tokener_free(parser);
    if (!has) return 0;
    struct VgiStorage *store = NULL;
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    int rc = VgiStorageOpen(NULL, &store, error);
    if (!rc) rc = VgiStorageGet(store, cookie, "attachment.options", &bytes, error);
    if (!rc)
        rc = VgiBatchDecode(out, (struct ArrowBufferView){{bytes.data}, bytes.size_bytes}, error);
    if (rc == ENOENT)
        ArrowErrorSet(error, "Attachment options have expired; re-attach the catalog");
    ArrowBufferReset(&bytes);
    VgiStorageClose(store);
    return rc;
}

int VgiCatalogMergeOptions(const struct VgiBatch *info, const struct VgiBatch *request,
                           struct VgiBatch *out, struct ArrowError *error) {
    const struct ArrowArrayView *specs = VgiBatchGet(info, "attach_option_specs");
    if (!specs || ArrowArrayViewIsNull(specs, 0)) return 0;
    int64_t start = ArrowArrayViewListChildOffset(specs, 0),
            end = ArrowArrayViewListChildOffset(specs, 1), n = end - start;
    if (!n) return 0;
    struct VgiBatch supplied = {0}, spec = {0}, def = {0};
    struct ArrowSchema schema = {0}, type = {0};
    struct ArrowBufferView bytes = VgiBatchBytes(request, "options");
    int rc = bytes.size_bytes ? VgiBatchDecode(&supplied, bytes, error) : 0;
    if (!rc && supplied.array.release && supplied.array.length != 1) {
        ArrowErrorSet(error, "ATTACH options must contain exactly one row");
        rc = EINVAL;
    }
    ArrowSchemaInit(&schema);
    if (!rc) rc = ArrowSchemaSetTypeStruct(&schema, n);
    for (int64_t i = 0; !rc && i < n; ++i) {
        struct ArrowStringView encoded = VgiValueString(specs->children[0], start + i);
        rc = VgiBatchDecode(&spec, (struct ArrowBufferView){{encoded.data}, encoded.size_bytes},
                            error);
        if (!rc) rc = VgiIpcReadSchema(VgiBatchBytes(&spec, "type"), &type, error);
        if (!rc && type.n_children != 1) {
            ArrowErrorSet(error, "ATTACH option type must describe one field");
            rc = EINVAL;
        }
        if (!rc) {
            ArrowSchemaRelease(schema.children[i]);
            rc = ArrowSchemaDeepCopy(type.children[0], schema.children[i]);
        }
        struct ArrowStringView name = VgiBatchString(&spec, "name");
        if (!rc) {
            char *text = ArrowMalloc(name.size_bytes + 1);
            if (!text)
                rc = ENOMEM;
            else {
                memcpy(text, name.data, (size_t)name.size_bytes);
                text[name.size_bytes] = 0;
                rc = ArrowSchemaSetName(schema.children[i], text);
                ArrowFree(text);
            }
        }
        if (!rc) schema.children[i]->flags |= ARROW_FLAG_NULLABLE;
        if (type.release) ArrowSchemaRelease(&type);
        VgiBatchReset(&spec);
    }
    /* Reject misspellings even for callers bypassing the engine's binder. */
    for (int64_t i = 0; !rc && i < supplied.schema.n_children; ++i) {
        int found = 0;
        for (int64_t j = 0; j < n; ++j)
            if (!strcasecmp(supplied.schema.children[i]->name, schema.children[j]->name)) found = 1;
        if (!found) {
            ArrowErrorSet(error, "Unknown ATTACH option '%s'", supplied.schema.children[i]->name);
            rc = EINVAL;
        }
        for (int64_t j = 0; !rc && j < i; ++j)
            if (!strcasecmp(supplied.schema.children[i]->name, supplied.schema.children[j]->name)) {
                ArrowErrorSet(error, "Duplicate ATTACH option '%s'",
                              supplied.schema.children[i]->name);
                rc = EINVAL;
            }
    }
    if (!rc) rc = VgiBatchInit(out, &schema, error);
    for (int64_t i = 0; !rc && i < n; ++i) {
        int64_t id = -1;
        for (int64_t j = 0; j < supplied.schema.n_children; ++j)
            if (!strcasecmp(supplied.schema.children[j]->name, schema.children[i]->name)) id = j;
        struct ArrowStringView encoded = VgiValueString(specs->children[0], start + i);
        rc = VgiBatchDecode(&spec, (struct ArrowBufferView){{encoded.data}, encoded.size_bytes},
                            error);
        if (!rc && id >= 0)
            rc = VgiArrayAppendValue(out->array.children[i], schema.children[i],
                                     supplied.view.children[id], supplied.schema.children[id], 0,
                                     error);
        else if (!rc) {
            struct ArrowBufferView default_value = VgiBatchBytes(&spec, "default_value");
            if (default_value.size_bytes) {
                rc = VgiBatchDecode(&def, default_value, error);
                if (!rc && (def.schema.n_children != 1 || def.array.length != 1)) rc = EINVAL;
                if (!rc)
                    rc =
                        VgiArrayAppendValue(out->array.children[i], schema.children[i],
                                            def.view.children[0], def.schema.children[0], 0, error);
            } else if (VgiBatchInt(&spec, "required", 0)) {
                ArrowErrorSet(error, "ATTACH required option '%s' is missing",
                              schema.children[i]->name);
                rc = EINVAL;
            } else
                rc = ArrowArrayAppendNull(out->array.children[i], 1);
        }
        VgiBatchReset(&def);
        VgiBatchReset(&spec);
    }
    if (!rc) rc = ArrowArrayFinishElement(&out->array);
    if (!rc) rc = VgiBatchFinish(out, error);
    if (schema.release) ArrowSchemaRelease(&schema);
    VgiBatchReset(&supplied);
    if (rc) VgiBatchReset(out);
    return rc;
}

int VgiCatalogStoreOptions(struct ArrowStringView cookie, const struct VgiBatch *options,
                           struct ArrowError *error) {
    if (!options->schema.release) return 0;
    struct VgiStorage *store = NULL;
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    int rc = VgiIpcWriteBatch(&options->schema, &options->array, NULL, &bytes, error);
    if (!rc) rc = VgiStorageOpen(NULL, &store, error);
    if (!rc)
        rc = VgiStoragePut(store, (struct ArrowBufferView){{cookie.data}, cookie.size_bytes},
                           "attachment.options",
                           (struct ArrowBufferView){{bytes.data}, bytes.size_bytes}, error);
    ArrowBufferReset(&bytes);
    VgiStorageClose(store);
    return rc;
}
