#include "vgi/catalog.h"

#include <ctype.h>
#include <errno.h>
#include <json-c/json.h>
#include <limits.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>

#include "catalog_internal.h"
#include "tickets_internal.h"
#include "vgi/generated/protocol.h"
#include "vgi/storage.h"
struct Snapshot {
    char *version;
    struct VgiBatch contents, extras;
};
struct Catalog {
    char *name;
    struct VgiBatch info, attach, contents, extras;
    struct json_object *versions;
    struct Snapshot *snapshots;
    size_t snapshot_count;
};
struct VgiCatalogService {
    struct Catalog *catalogs;
    size_t count;
};
static char *copy_text(struct ArrowStringView value) {
    if (value.size_bytes < 0 || (uint64_t)value.size_bytes >= SIZE_MAX) return NULL;
    char *out = malloc((size_t)value.size_bytes + 1);
    if (out) {
        if (value.size_bytes) memcpy(out, value.data, (size_t)value.size_bytes);
        out[value.size_bytes] = 0;
    }
    return out;
}
static int text_is(struct ArrowStringView value, const char *text) {
    return value.size_bytes == (int64_t)strlen(text) &&
           !memcmp(value.data, text, (size_t)value.size_bytes);
}
static int kind_is(struct ArrowStringView value, const char *text) {
    size_t n = strlen(text);
    if (value.size_bytes < (int64_t)n) return 0;
    for (size_t i = 0; i < n; ++i)
        if (tolower((unsigned char)value.data[i]) != tolower((unsigned char)text[i])) return 0;
    return value.size_bytes == (int64_t)n || value.data[n] == '_';
}
static int copy_batch(const struct VgiBatch *input, struct VgiBatch *out,
                      struct ArrowError *error) {
    return VgiBatchCoerce(input, &input->schema, out, error);
}
/* Copy one named value of a one-row batch; a missing field is null. */
static int copy_value(struct VgiBatch *out, const struct VgiBatch *source, const char *name,
                      struct ArrowError *error) {
    int64_t a = VgiFieldIndex(&out->schema, name), b = VgiFieldIndex(&source->schema, name);
    if (a < 0) return EINVAL;
    if (b < 0) return ArrowArrayAppendNull(out->array.children[a], 1);
    return VgiArrayAppendValue(out->array.children[a], out->schema.children[a],
                               source->view.children[b], source->schema.children[b], 0, error);
}
static void catalog_reset(struct Catalog *c) {
    free(c->name);
    VgiBatchReset(&c->info);
    VgiBatchReset(&c->attach);
    VgiBatchReset(&c->contents);
    VgiBatchReset(&c->extras);
    if (c->versions) json_object_put(c->versions);
    for (size_t i = 0; i < c->snapshot_count; ++i) {
        free(c->snapshots[i].version);
        VgiBatchReset(&c->snapshots[i].contents);
        VgiBatchReset(&c->snapshots[i].extras);
    }
    free(c->snapshots);
}
void VgiCatalogFree(struct VgiCatalogService *s) {
    if (s) {
        for (size_t i = 0; i < s->count; ++i) catalog_reset(&s->catalogs[i]);
        free(s->catalogs);
        free(s);
    }
}
int VgiCatalogAdd(struct VgiCatalogService **service, const struct VgiBatch *info,
                  const struct VgiBatch *attach, const struct VgiBatch *contents,
                  const struct VgiBatch *extras, struct ArrowError *error) {
    if (!info || !attach || !contents || info->array.length != 1 || attach->array.length != 1 ||
        contents->array.length != 1)
        return EINVAL;
    int option_rc = VgiTicketCheckOptions(info, error);
    if (option_rc) return option_rc;
    if (!*service) {
        *service = calloc(1, sizeof(**service));
        if (!*service) return ENOMEM;
    }
    struct VgiCatalogService *s = *service;
    struct Catalog c = {0};
    c.name = copy_text(VgiBatchString(info, "name"));
    if (!c.name) return ENOMEM;
    for (size_t i = 0; i < s->count; ++i)
        if (!strcmp(s->catalogs[i].name, c.name)) {
            catalog_reset(&c);
            return EEXIST;
        }
    int rc = copy_batch(info, &c.info, error);
    if (!rc) rc = copy_batch(attach, &c.attach, error);
    if (!rc) rc = copy_batch(contents, &c.contents, error);
    if (!rc && extras) rc = copy_batch(extras, &c.extras, error);
    if (!rc) {
        struct Catalog *next = realloc(s->catalogs, (s->count + 1) * sizeof(*next));
        if (!next)
            rc = ENOMEM;
        else {
            s->catalogs = next;
            s->catalogs[s->count++] = c;
            return 0;
        }
    }
    catalog_reset(&c);
    return rc;
}
static struct Catalog *find(struct VgiCatalogService *s, const char *name) {
    for (size_t i = 0; i < s->count; ++i)
        if (!strcmp(s->catalogs[i].name, name)) return &s->catalogs[i];
    return NULL;
}
int VgiCatalogSetVersions(struct VgiCatalogService *s, const char *name,
                          const struct VgiCatalogVersions *v, struct ArrowError *error) {
    struct Catalog *c = s ? find(s, name) : NULL;
    if (!c || !v) return EINVAL;
    struct json_object *object = json_object_new_object();
    if (!object) return ENOMEM;
    const char *keys[] = {"data", "impl"};
    const char *const *sets[] = {v->data_versions, v->implementation_versions};
    size_t sizes[] = {v->data_version_count, v->implementation_version_count};
    const char *defaults[] = {v->default_data_version, v->default_implementation_version};
    for (int k = 0; k < 2; ++k) {
        struct json_object *list = json_object_new_array();
        int found = !sizes[k];
        for (size_t i = 0; i < sizes[k]; ++i) {
            if (!sets[k] || !sets[k][i]) {
                json_object_put(list);
                json_object_put(object);
                return EINVAL;
            }
            json_object_array_add(list, json_object_new_string(sets[k][i]));
            found |= defaults[k] && !strcmp(defaults[k], sets[k][i]);
        }
        json_object_object_add(object, keys[k], list);
        if (!found) {
            json_object_put(object);
            ArrowErrorSet(error, "Default catalog version must be supported");
            return EINVAL;
        }
    }
    json_object_object_add(object, "default_data",
                           json_object_new_string(defaults[0] ? defaults[0] : ""));
    json_object_object_add(object, "default_impl",
                           json_object_new_string(defaults[1] ? defaults[1] : ""));
    json_object_object_add(object, "npm", json_object_new_boolean(v->npm_resolution));
    if (c->versions) json_object_put(c->versions);
    c->versions = object;
    return 0;
}
int VgiCatalogAddSnapshot(struct VgiCatalogService *s, const char *name, const char *version,
                          const struct VgiBatch *contents, const struct VgiBatch *extras,
                          struct ArrowError *error) {
    struct Catalog *c = s ? find(s, name) : NULL;
    if (!c || !version || !contents || contents->array.length != 1) return EINVAL;
    for (size_t i = 0; i < c->snapshot_count; ++i)
        if (!strcmp(c->snapshots[i].version, version)) return EEXIST;
    struct Snapshot snapshot = {0};
    snapshot.version = copy_text(ArrowCharView(version));
    int rc = snapshot.version ? copy_batch(contents, &snapshot.contents, error) : ENOMEM;
    if (!rc && extras) rc = copy_batch(extras, &snapshot.extras, error);
    if (!rc) {
        struct Snapshot *next = realloc(c->snapshots, (c->snapshot_count + 1) * sizeof(*next));
        if (!next)
            rc = ENOMEM;
        else {
            c->snapshots = next;
            c->snapshots[c->snapshot_count++] = snapshot;
            return 0;
        }
    }
    free(snapshot.version);
    VgiBatchReset(&snapshot.contents);
    VgiBatchReset(&snapshot.extras);
    return rc;
}
static int version_parse(const char *text, uint32_t out[3]) {
    int n = 0;
    memset(out, 0, 3 * sizeof(*out));
    while (*text && n < 3) {
        if (!isdigit((unsigned char)*text)) return 0;
        uint64_t value = 0;
        do {
            value = value * 10 + (unsigned)(*text++ - '0');
            if (value > UINT32_MAX) return 0;
        } while (isdigit((unsigned char)*text));
        out[n++] = (uint32_t)value;
        if (!*text) break;
        if (*text++ != '.' || !*text) return 0;
    }
    return *text ? 0 : n;
}
static int version_cmp(const uint32_t a[3], const uint32_t b[3]) {
    for (int i = 0; i < 3; ++i)
        if (a[i] != b[i]) return a[i] < b[i] ? -1 : 1;
    return 0;
}
static int resolve_version(struct Catalog *catalog, const struct VgiBatch *request,
                           int implementation, const char **out, struct ArrowError *error) {
    *out = NULL;
    if (!catalog->versions) return 0;
    const char *label = implementation ? "implementation_version" : "data_version_spec";
    struct json_object *list = json_object_object_get(catalog->versions,
                                                      implementation ? "impl" : "data"),
                       *fallback = json_object_object_get(
                           catalog->versions, implementation ? "default_impl" : "default_data");
    if (!json_object_array_length(list)) return 0;
    struct ArrowStringView value = VgiBatchString(request, label);
    if (!value.size_bytes) {
        *out = json_object_get_string(fallback);
        return 0;
    }
    char *spec = copy_text(value);
    if (!spec) return ENOMEM;
    int npm = json_object_get_boolean(json_object_object_get(catalog->versions, "npm")),
        ranged = spec[0] == '^' || spec[0] == '~';
    uint32_t base[3], best[3] = {0};
    int parts = version_parse(spec + ranged, base);
    for (size_t i = 0; i < json_object_array_length(list); ++i) {
        const char *candidate = json_object_get_string(json_object_array_get_idx(list, i));
        uint32_t version[3];
        int matches = !strcmp(spec, candidate);
        if (npm && parts && version_parse(candidate, version) == 3) {
            if (ranged && parts == 3)
                matches = version[0] == base[0] && version_cmp(version, base) >= 0 &&
                          (spec[0] == '^' || version[1] == base[1]);
            else if (!ranged && parts < 3)
                matches = parts == 1 ? version[0] == base[0] : !version_cmp(version, base);
            if (matches && (!*out || version_cmp(version, best) > 0)) {
                *out = candidate;
                memcpy(best, version, sizeof(best));
            }
        } else if (matches)
            *out = candidate;
    }
    if (!*out) ArrowErrorSet(error, "Unsupported %s", label);
    free(spec);
    return *out ? 0 : EINVAL;
}
static struct Catalog select_snapshot(struct Catalog *source, const char *version) {
    struct Catalog selected = *source;
    for (size_t i = 0; version && i < source->snapshot_count; ++i)
        if (!strcmp(source->snapshots[i].version, version)) {
            selected.contents = source->snapshots[i].contents;
            selected.extras = source->snapshots[i].extras;
            break;
        }
    return selected;
}
static char *cookie_version(struct ArrowStringView cookie) {
    if (!cookie.data || cookie.size_bytes > INT_MAX) return NULL;
    struct json_tokener *parser = json_tokener_new();
    if (!parser) return NULL;
    struct json_object *object = json_tokener_parse_ex(parser, cookie.data, (int)cookie.size_bytes),
                       *version = NULL;
    char *out = NULL;
    if (object && json_object_object_get_ex(object, "data_version", &version))
        out = copy_text(ArrowCharView(json_object_get_string(version)));
    if (object) json_object_put(object);
    json_tokener_free(parser);
    return out;
}
static char *cookie_catalog(struct ArrowStringView cookie) {
    if (!cookie.data || cookie.size_bytes > INT_MAX) return NULL;
    struct json_tokener *parser = json_tokener_new();
    if (!parser) return NULL;
    struct json_object *object = json_tokener_parse_ex(parser, cookie.data, (int)cookie.size_bytes),
                       *name = NULL;
    char *out = NULL;
    if (object && json_object_object_get_ex(object, "catalog", &name))
        out = copy_text(ArrowCharView(json_object_get_string(name)));
    if (object) json_object_put(object);
    json_tokener_free(parser);
    return out;
}
static int path_equal(const struct ArrowArrayView *left, int64_t row,
                      const struct ArrowArrayView *right) {
    if (!left || !right || ArrowArrayViewIsNull(left, row) || ArrowArrayViewIsNull(right, 0))
        return 0;
    int64_t a = ArrowArrayViewListChildOffset(left, row),
            b = ArrowArrayViewListChildOffset(right, 0);
    int64_t n = ArrowArrayViewListChildOffset(left, row + 1) - a;
    if (n != ArrowArrayViewListChildOffset(right, 1) - b) return 0;
    for (int64_t i = 0; i < n; ++i) {
        struct ArrowStringView x = VgiValueString(left->children[0], a + i),
                               y = VgiValueString(right->children[0], b + i);
        if (x.size_bytes != y.size_bytes || memcmp(x.data, y.data, (size_t)x.size_bytes)) return 0;
    }
    return 1;
}
static int encode(const struct VgiBatch *batch, struct ArrowBuffer *out, struct ArrowError *error) {
    return VgiIpcWriteBatch(&batch->schema, &batch->array, NULL, out, error);
}
static int append_encoded(struct ArrowArray *list, const struct VgiBatch *batch,
                          struct ArrowError *error) {
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    int rc = encode(batch, &bytes, error);
    if (!rc)
        rc = ArrowArrayAppendBytes(list->children[0],
                                   (struct ArrowBufferView){{bytes.data}, bytes.size_bytes});
    ArrowBufferReset(&bytes);
    return rc;
}
/* Captured descriptors may predate the current record layout, so re-encode one as
 * the generated record (wire 2.2 dictionary-encodes FunctionInfo's schemas and
 * drops SchemaInfo's attach cookie). Fields are matched by name. */
static int append_record(struct ArrowArray *list, const char *record, struct ArrowStringView bytes,
                         struct ArrowError *error) {
    struct VgiBatch source = {0}, target = {0};
    int rc =
        VgiBatchDecode(&source, (struct ArrowBufferView){{bytes.data}, bytes.size_bytes}, error);
    if (!rc) rc = VgiBatchInitNamed(&target, record, error);
    if (!rc)
        rc = VgiArrayAppendValue(&target.array, &target.schema, &source.view, &source.schema, 0,
                                 error);
    if (!rc) rc = VgiBatchFinish(&target, error);
    if (!rc) rc = append_encoded(list, &target, error);
    VgiBatchReset(&source);
    VgiBatchReset(&target);
    return rc;
}
static int clone_with_cookie(const struct VgiBatch *source, struct ArrowStringView cookie,
                             const char *data_version, const char *impl_version,
                             struct VgiBatch *out, struct ArrowError *error) {
    int rc = VgiBatchInit(out, &source->schema, error);
    for (int64_t i = 0; !rc && i < source->schema.n_children; ++i) {
        if (!strcmp(source->schema.children[i]->name, "attach_opaque_data"))
            rc = ArrowArrayAppendBytes(out->array.children[i],
                                       (struct ArrowBufferView){{cookie.data}, cookie.size_bytes});
        else if (data_version && !strcmp(source->schema.children[i]->name, "resolved_data_version"))
            rc = ArrowArrayAppendString(out->array.children[i], ArrowCharView(data_version));
        else if (impl_version &&
                 !strcmp(source->schema.children[i]->name, "resolved_implementation_version"))
            rc = ArrowArrayAppendString(out->array.children[i], ArrowCharView(impl_version));
        else
            rc =
                VgiArrayAppendValue(out->array.children[i], out->schema.children[i],
                                    source->view.children[i], source->schema.children[i], 0, error);
    }
    if (!rc) rc = ArrowArrayFinishElement(&out->array);
    if (!rc) rc = VgiBatchFinish(out, error);
    return rc;
}
static int reply_raw(struct VgiRpcCall *call, struct VgiRpcReply *reply,
                     struct ArrowStringView bytes) {
    struct ArrowSchema schema = {0};
    int rc = call->method->result(&schema, &call->error);
    if (!rc) rc = VgiBatchInit(&reply->value, &schema, &call->error);
    if (!rc)
        rc = bytes.data ? VgiBatchSetBytes(&reply->value, "result", bytes.data, bytes.size_bytes)
                        : VgiBatchSetNull(&reply->value, "result");
    if (!rc) rc = VgiBatchFinishRow(&reply->value, &call->error);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}
/* Optional immutable snapshots augment ordinary descriptors. Versions are selected
 * before serving any descriptor, so a scan and its advertised schema agree. */
static int extra_reply(struct Catalog *catalog, struct VgiRpcCall *call, struct VgiRpcReply *reply,
                       int *found) {
    *found = 0;
    const struct VgiBatch *e = &catalog->extras;
    const char *method = call->method->name;
    int table = !strcmp(method, "catalog_table_get"),
        scan = !strcmp(method, "catalog_table_scan_function_get") ||
               !strcmp(method, "catalog_table_scan_branches_get"),
        stats = !strcmp(method, "catalog_table_column_statistics_get");
    if (!table && !scan && !stats) return 0;
    int64_t mi = VgiFieldIndex(&e->schema, "method"), pi = VgiFieldIndex(&e->schema, "path"),
            ni = VgiFieldIndex(&e->schema, "name"), vi = VgiFieldIndex(&e->schema, "value"),
            version_id = VgiFieldIndex(&e->schema, "version"),
            year_id = VgiFieldIndex(&e->schema, "timestamp_year"),
            at_id = VgiFieldIndex(&e->schema, "accepts_at");
    if (mi < 0 || pi < 0 || ni < 0 || vi < 0) return 0;
    struct ArrowStringView wanted = VgiBatchString(&call->request, "name"),
                           unit = VgiBatchString(&call->request, "at_unit"),
                           value = VgiBatchString(&call->request, "at_value");
    const struct ArrowArrayView *path = VgiBatchGet(&call->request, "schema_path");
    int has_at = unit.size_bytes > 0, by_version = kind_is(unit, "VERSION"),
        by_year = kind_is(unit, "TIMESTAMP");
    int64_t requested = 0;
    if (has_at) {
        char text[64];
        if (value.size_bytes <= 0 || value.size_bytes >= 64)
            return VgiRpcFail(call, "ValueError", "INVALID_ARGUMENT", "",
                              "Invalid time travel value");
        memcpy(text, value.data, (size_t)value.size_bytes);
        text[value.size_bytes] = 0;
        char *end = NULL;
        errno = 0;
        requested = strtoll(text, &end, 10);
        if (errno || end == text || (by_version && *end))
            return VgiRpcFail(call, "ValueError", "INVALID_ARGUMENT", "", "Unknown version");
    }
    int64_t selected = -1, selected_version = INT64_MIN, fallback = -1, earliest = INT64_MAX;
    int versions = 0, accepts = 0, known = 0;
    for (int64_t row = 0; row < e->array.length; ++row) {
        struct ArrowStringView name = VgiValueString(e->view.children[ni], row);
        if (name.size_bytes != wanted.size_bytes ||
            memcmp(name.data, wanted.data, (size_t)name.size_bytes) ||
            !path_equal(e->view.children[pi], row, path))
            continue;
        known = 1;
        if (at_id >= 0 && !ArrowArrayViewIsNull(e->view.children[at_id], row))
            accepts |= ArrowArrayViewGetIntUnsafe(e->view.children[at_id], row) != 0;
        int match = text_is(VgiValueString(e->view.children[mi], row), method);
        if (version_id < 0 || ArrowArrayViewIsNull(e->view.children[version_id], row)) {
            if (match) fallback = row;
            continue;
        }
        ++versions;
        int64_t version = ArrowArrayViewGetIntUnsafe(e->view.children[version_id], row),
                year = year_id < 0 || ArrowArrayViewIsNull(e->view.children[year_id], row)
                           ? INT64_MAX
                           : ArrowArrayViewGetIntUnsafe(e->view.children[year_id], row);
        if (year < earliest) earliest = year;
        int eligible =
            !has_at || (by_version && version == requested) || (by_year && year <= requested);
        if (match && eligible && version > selected_version) {
            selected = row;
            selected_version = version;
        }
    }
    if (has_at && known && !stats) {
        if (versions && (!by_version && !by_year))
            return VgiRpcFail(call, "ValueError", "INVALID_ARGUMENT", "", "unsupported AT unit");
        if (versions && selected < 0) {
            char message[160];
            if (by_year)
                snprintf(message, sizeof(message), "table did not exist before %lld",
                         (long long)earliest);
            else
                snprintf(message, sizeof(message), "Unknown version");
            return VgiRpcFail(call, "ValueError", "INVALID_ARGUMENT", "", message);
        }
        if (!versions && !accepts)
            return VgiRpcFail(call, "ValueError", "INVALID_ARGUMENT", "",
                              "this table does not support time travel");
    }
    int64_t row = selected >= 0 ? selected : fallback;
    if (row >= 0) {
        *found = 1;
        return reply_raw(call, reply, VgiValueString(e->view.children[vi], row));
    }
    if (stats) {
        *found = 1;
        return reply_raw(call, reply, (struct ArrowStringView){0});
    }
    return 0;
}
int VgiCatalogDispatch(struct VgiCatalogService *service, const char *primary,
                       struct VgiRpcCall *call, struct VgiRpcReply *reply, int *handled) {
    *handled = 0;
    if (!service || !service->count) return 0;
    const char *method = call->method->name;
    struct VgiBatch request = {0}, result = {0}, item = {0};
    struct Catalog *catalog = NULL, selected = {0};
    int rc = 0;
    if (!strcmp(method, "catalog_catalogs")) {
        *handled = 1;
        rc = VgiBatchInitNamed(&result, "CatalogsResult", &call->error);
        if (rc) {
            struct ArrowSchema schema = {0};
            rc = call->method->payload(&schema, &call->error);
            if (!rc) rc = VgiBatchInit(&result, &schema, &call->error);
            if (schema.release) ArrowSchemaRelease(&schema);
        }
        for (size_t i = 0; !rc && i < service->count; ++i)
            rc = append_encoded(VgiBatchColumn(&result, "items"), &service->catalogs[i].info,
                                &call->error);
        if (!rc) rc = ArrowArrayFinishElement(VgiBatchColumn(&result, "items"));
    } else {
        if (!strcmp(method, "catalog_attach")) {
            rc = VgiBatchChild(&call->request, "request", &request, &call->error);
            char *name = rc ? NULL : copy_text(VgiBatchString(&request, "name"));
            catalog = name ? find(service, name) : NULL;
            if (!catalog && primary) catalog = find(service, primary);
            free(name);
        } else {
            char *name = cookie_catalog(VgiBatchString(&call->request, "attach_opaque_data"));
            catalog = name ? find(service, name) : NULL;
            free(name);
        }
        if (!catalog) {
            VgiBatchReset(&request);
            return rc;
        }
        char *version = cookie_version(VgiBatchString(&call->request, "attach_opaque_data"));
        selected = select_snapshot(catalog, version);
        catalog = &selected;
        free(version);
        *handled = 1;
        int extra_found = 0;
        rc = extra_reply(catalog, call, reply, &extra_found);
        if (rc || extra_found) {
            VgiBatchReset(&request);
            return rc;
        }
        if (!strcmp(method, "catalog_attach")) {
            struct VgiBatch options = {0};
            const char *data_version = NULL, *impl_version = NULL;
            rc = resolve_version(catalog, &request, 0, &data_version, &call->error);
            if (!rc) rc = resolve_version(catalog, &request, 1, &impl_version, &call->error);
            if (!rc) rc = VgiCatalogMergeOptions(&catalog->info, &request, &options, &call->error);
            unsigned char id[16];
            char hex[33];
            randombytes_buf(id, sizeof(id));
            sodium_bin2hex(hex, sizeof(hex), id, sizeof(id));
            struct json_object *cookie = json_object_new_object();
            json_object_object_add(cookie, "catalog", json_object_new_string(catalog->name));
            json_object_object_add(cookie, "attachment_id", json_object_new_string(hex));
            if (options.schema.release)
                json_object_object_add(cookie, "options", json_object_new_boolean(1));
            if (data_version)
                json_object_object_add(cookie, "data_version",
                                       json_object_new_string(data_version));
            struct ArrowStringView value =
                ArrowCharView(json_object_to_json_string_ext(cookie, JSON_C_TO_STRING_PLAIN));
            if (!rc) rc = VgiCatalogStoreOptions(value, &options, &call->error);
            if (!rc)
                rc = clone_with_cookie(&catalog->attach, value, data_version, impl_version, &result,
                                       &call->error);
            VgiBatchReset(&options);
            json_object_put(cookie);
        } else if (!strcmp(method, "catalog_detach")) {
            struct VgiStorage *storage = NULL;
            rc = VgiStorageOpen(NULL, &storage, &call->error);
            if (!rc)
                rc = VgiStorageClear(storage, VgiBatchBytes(&call->request, "attach_opaque_data"),
                                     &call->error);
            VgiStorageClose(storage);
            VgiBatchReset(&request);
            return rc;
        } else if (!strcmp(method, "catalog_transaction_begin")) {
            unsigned char random[16];
            char hex[33];
            randombytes_buf(random, sizeof(random));
            sodium_bin2hex(hex, sizeof(hex), random, sizeof(random));
            rc = VgiBatchInitNamed(&result, "CatalogTransactionBeginResult", &call->error);
            if (!rc) rc = VgiBatchSetBytes(&result, "transaction_opaque_data", hex, 32);
        } else if (!strcmp(method, "catalog_transaction_commit") ||
                   !strcmp(method, "catalog_transaction_rollback")) {
            struct VgiStorage *storage = NULL;
            rc = VgiStorageOpen(NULL, &storage, &call->error);
            if (!rc)
                rc = VgiStorageClear(storage,
                                     VgiBatchBytes(&call->request, "transaction_opaque_data"),
                                     &call->error);
            VgiStorageClose(storage);
            VgiBatchReset(&request);
            return rc;
        } else if (!strcmp(method, "catalog_version")) {
            struct ArrowSchema schema = {0};
            rc = call->method->payload(&schema, &call->error);
            if (!rc) rc = VgiBatchInit(&result, &schema, &call->error);
            if (schema.release) ArrowSchemaRelease(&schema);
            if (!rc)
                rc = VgiBatchSetInt(&result, "version",
                                    VgiBatchInt(&catalog->contents, "catalog_version", 1));
        } else if (!strcmp(method, "catalog_contents")) {
            if (!strcmp(catalog->name, "contents_broken"))
                rc = VgiRpcFail(call, "ValueError", "INTERNAL", "",
                                "contents_broken: catalog_contents deliberately fails");
            else {
                struct ArrowBuffer collections[VGI_CATALOG_COLLECTIONS];
                rc = VgiCatalogCollections(&catalog->contents, collections, &call->error);
                if (!rc) rc = VgiBatchInitNamed(&result, "CatalogContentsResult", &call->error);
                if (!rc)
                    rc = VgiBatchSetInt(&result, "catalog_version",
                                        VgiBatchInt(&catalog->contents, "catalog_version", 1));
                if (!rc) rc = copy_value(&result, &catalog->contents, "etag", &call->error);
                if (!rc) rc = VgiBatchSetInt(&result, "not_modified", 0);
                if (!rc) rc = VgiCatalogCollectionsSet(&result, collections);
                if (!rc) rc = VgiBatchFinishRow(&result, &call->error);
                VgiCatalogCollectionsReset(collections);
            }
        } else if (call->method->payload) {
            struct ArrowSchema schema = {0};
            rc = call->method->payload(&schema, &call->error);
            if (!rc) rc = VgiBatchInit(&result, &schema, &call->error);
            if (schema.release) ArrowSchemaRelease(&schema);
        }
        const struct ArrowArrayView *list = VgiBatchGet(&catalog->contents, "schemas"),
                                    *entries = list ? list->children[0] : NULL;
        const struct ArrowSchema *entry_schema =
            list ? catalog->contents.schema
                       .children[VgiFieldIndex(&catalog->contents.schema, "schemas")]
                       ->children[0]
                 : NULL;
        int64_t path_id = entry_schema ? VgiFieldIndex(entry_schema, "path") : -1,
                info_id = entry_schema ? VgiFieldIndex(entry_schema, "schema") : -1;
        const struct ArrowArrayView *requested_path = VgiBatchGet(&call->request, "schema_path");
        if (!requested_path) requested_path = VgiBatchGet(&call->request, "path");
        int schemas = !strcmp(method, "catalog_schemas") || !strcmp(method, "catalog_schema_get");
        const char *kind = NULL;
        if (strstr(method, "tables") || !strcmp(method, "catalog_table_get"))
            kind = "tables";
        else if (strstr(method, "views") || !strcmp(method, "catalog_view_get"))
            kind = "views";
        else if (strstr(method, "indexes") || !strcmp(method, "catalog_index_get"))
            kind = "indexes";
        else if (strstr(method, "macros") || !strcmp(method, "catalog_macro_get"))
            kind = kind_is(VgiBatchString(&call->request, "type"), "TABLE") ? "table_macros"
                                                                            : "scalar_macros";
        else if (strstr(method, "functions"))
            kind = kind_is(VgiBatchString(&call->request, "type"), "SCALAR") ? "scalar_functions"
                   : kind_is(VgiBatchString(&call->request, "type"), "AGGREGATE")
                       ? "aggregate_functions"
                       : "table_functions";
        if (!strcmp(method, "catalog_contents")) {
            /* Answered above from the whole contents. */
        } else if (schemas || kind) {
            for (int64_t row = 0; !rc && entries && row < entries->length; ++row) {
                if (requested_path && !path_equal(entries->children[path_id], row, requested_path))
                    continue;
                if (schemas)
                    rc = append_record(VgiBatchColumn(&result, "items"), "SchemaInfo",
                                       VgiValueString(entries->children[info_id], row),
                                       &call->error);
                if (!rc && kind) {
                    int64_t id = VgiFieldIndex(entry_schema, kind);
                    const struct ArrowArrayView *items = entries->children[id];
                    int64_t start = ArrowArrayViewListChildOffset(items, row),
                            end = ArrowArrayViewListChildOffset(items, row + 1);
                    struct ArrowStringView wanted = VgiBatchString(&call->request, "name");
                    for (int64_t j = start; !rc && j < end; ++j) {
                        struct ArrowStringView data = VgiValueString(items->children[0], j);
                        if (wanted.data) {
                            rc = VgiBatchDecode(
                                &item, (struct ArrowBufferView){{data.data}, data.size_bytes},
                                &call->error);
                            struct ArrowStringView name = VgiBatchString(&item, "name");
                            int match = wanted.size_bytes == name.size_bytes &&
                                        !memcmp(wanted.data, name.data, (size_t)name.size_bytes);
                            VgiBatchReset(&item);
                            if (!rc && !match) continue;
                        }
                        if (!rc && strstr(kind, "functions"))
                            rc = append_record(VgiBatchColumn(&result, "items"), "FunctionInfo",
                                               data, &call->error);
                        else if (!rc)
                            rc = ArrowArrayAppendBytes(
                                VgiBatchColumn(&result, "items")->children[0],
                                (struct ArrowBufferView){{data.data}, data.size_bytes});
                    }
                }
            }
            if (!rc) rc = ArrowArrayFinishElement(VgiBatchColumn(&result, "items"));
        } else if (!strcmp(method, "catalog_table_scan_function_get") ||
                   !strcmp(method, "catalog_table_scan_branches_get")) {
            int found = 0;
            const struct VgiBatch *extra = &catalog->extras;
            int64_t mi = VgiFieldIndex(&extra->schema, "method"),
                    pi = VgiFieldIndex(&extra->schema, "path"),
                    ni = VgiFieldIndex(&extra->schema, "name"),
                    vi = VgiFieldIndex(&extra->schema, "value");
            struct ArrowStringView wanted = VgiBatchString(&call->request, "name");
            for (int64_t i = 0;
                 !rc && mi >= 0 && pi >= 0 && ni >= 0 && vi >= 0 && i < extra->array.length; ++i) {
                struct ArrowStringView name = VgiValueString(extra->view.children[ni], i);
                if (!text_is(VgiValueString(extra->view.children[mi], i), method) ||
                    name.size_bytes != wanted.size_bytes ||
                    memcmp(name.data, wanted.data, (size_t)name.size_bytes) ||
                    !path_equal(extra->view.children[pi], i, requested_path))
                    continue;
                rc = reply_raw(call, reply, VgiValueString(extra->view.children[vi], i));
                found = 1;
                break;
            }
            if (!rc && !found)
                rc = VgiRpcFail(call, "ValueError", "NOT_FOUND", "",
                                "No scan descriptor for the requested table");
        } else if (strcmp(method, "catalog_attach") && strcmp(method, "catalog_version") &&
                   strcmp(method, "catalog_transaction_begin")) {
            /* Let the worker's remaining services handle transactional/DDL and
             * COPY operations; immutable snapshots do not fabricate success. */
            *handled = 0;
        }
    }
    if (!rc && *handled && result.array.release && !reply->value.array.release) {
        if (!result.array.length) rc = VgiBatchFinishRow(&result, &call->error);
        if (!rc) rc = VgiRpcReplyPayload(call, reply, &result);
    }
    VgiBatchReset(&request);
    VgiBatchReset(&result);
    VgiBatchReset(&item);
    return rc;
}

const struct VgiBatch *VgiCatalogInfo(struct VgiCatalogService *s, const char *name) {
    struct Catalog *c = s ? find(s, name) : NULL;
    return c ? &c->info : NULL;
}

/* Wire 2.2 answers catalog_contents with six catalog-wide collections, each one
 * Arrow IPC stream of its record type, rather than the per-schema layout the
 * declarative contents keep. Order follows vgi-python's encode_inventory:
 * parents before children, then each schema's objects by source list. */
static const char *const collection_names[VGI_CATALOG_COLLECTIONS] = {
    "schemas", "tables", "views", "functions", "macros", "indexes"};
static const char *const collection_records[VGI_CATALOG_COLLECTIONS] = {
    "SchemaInfo", "TableInfo", "ViewInfo", "FunctionInfo", "MacroInfo", "IndexInfo"};
static const struct {
    const char *source;
    int collection;
} collection_sources[] = {{"tables", 1},
                          {"views", 2},
                          {"indexes", 5},
                          {"scalar_functions", 3},
                          {"aggregate_functions", 3},
                          {"table_functions", 3},
                          {"scalar_macros", 4},
                          {"table_macros", 4}};
static int append_row(struct VgiBatch *records, struct ArrowStringView bytes,
                      struct ArrowError *error) {
    struct VgiBatch source = {0};
    int rc =
        VgiBatchDecode(&source, (struct ArrowBufferView){{bytes.data}, bytes.size_bytes}, error);
    for (int64_t i = 0; !rc && i < source.array.length; ++i)
        rc = VgiArrayAppendValue(&records->array, &records->schema, &source.view, &source.schema, i,
                                 error);
    VgiBatchReset(&source);
    return rc;
}
int VgiCatalogContentsInit(struct VgiBatch *out, struct ArrowError *error) {
    struct ArrowSchema schema = {0}, *entry = NULL;
    ArrowSchemaInit(&schema);
    int rc = ArrowSchemaSetTypeStruct(&schema, 2);
    if (!rc) rc = ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_INT64);
    if (!rc) rc = ArrowSchemaSetName(schema.children[0], "catalog_version");
    if (!rc) schema.children[0]->flags = 0;
    if (!rc) rc = ArrowSchemaSetFormat(schema.children[1], "+l");
    if (!rc) rc = ArrowSchemaSetName(schema.children[1], "schemas");
    if (!rc) rc = ArrowSchemaAllocateChildren(schema.children[1], 1);
    if (!rc) {
        schema.children[1]->flags = 0;
        entry = schema.children[1]->children[0];
        rc = VgiSchemaInit("SchemaContents", entry, error);
    }
    if (!rc) rc = ArrowSchemaSetName(entry, "item");
    if (!rc) rc = VgiBatchInit(out, &schema, error);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}
int VgiCatalogCollections(const struct VgiBatch *contents,
                          struct ArrowBuffer out[VGI_CATALOG_COLLECTIONS],
                          struct ArrowError *error) {
    struct VgiBatch records[VGI_CATALOG_COLLECTIONS];
    memset(records, 0, sizeof(records));
    int rc = 0;
    for (int i = 0; i < VGI_CATALOG_COLLECTIONS; ++i) {
        ArrowBufferInit(&out[i]);
        if (!rc) rc = VgiBatchInitNamed(&records[i], collection_records[i], error);
    }
    int64_t field = VgiFieldIndex(&contents->schema, "schemas");
    const struct ArrowArrayView *list = field < 0 ? NULL : contents->view.children[field],
                                *entries = list ? list->children[0] : NULL;
    const struct ArrowSchema *type = list ? contents->schema.children[field]->children[0] : NULL;
    int64_t path_id = type ? VgiFieldIndex(type, "path") : -1,
            info_id = type ? VgiFieldIndex(type, "schema") : -1;
    if (!rc && entries && (path_id < 0 || info_id < 0)) rc = EINVAL;
    int64_t first = entries ? ArrowArrayViewListChildOffset(list, 0) : 0,
            last = entries ? ArrowArrayViewListChildOffset(list, 1) : 0, remaining = last - first;
    for (int64_t depth = 0; !rc && remaining > 0; ++depth) {
        for (int64_t row = first; !rc && row < last; ++row) {
            const struct ArrowArrayView *path = entries->children[path_id];
            if (ArrowArrayViewListChildOffset(path, row + 1) -
                    ArrowArrayViewListChildOffset(path, row) !=
                depth)
                continue;
            --remaining;
            rc = append_row(&records[0], VgiValueString(entries->children[info_id], row), error);
            for (size_t s = 0; !rc && s < sizeof(collection_sources) / sizeof(*collection_sources);
                 ++s) {
                int64_t id = VgiFieldIndex(type, collection_sources[s].source);
                if (id < 0) continue;
                const struct ArrowArrayView *items = entries->children[id];
                for (int64_t j = ArrowArrayViewListChildOffset(items, row);
                     !rc && j < ArrowArrayViewListChildOffset(items, row + 1); ++j)
                    rc = append_row(&records[collection_sources[s].collection],
                                    VgiValueString(items->children[0], j), error);
            }
        }
    }
    for (int i = 0; !rc && i < VGI_CATALOG_COLLECTIONS; ++i) {
        if (!records[i].array.length) continue;
        rc = VgiBatchFinish(&records[i], error);
        if (!rc) rc = VgiIpcWriteBatch(&records[i].schema, &records[i].array, NULL, &out[i], error);
    }
    for (int i = 0; i < VGI_CATALOG_COLLECTIONS; ++i) VgiBatchReset(&records[i]);
    if (rc) VgiCatalogCollectionsReset(out);
    return rc;
}
void VgiCatalogCollectionsReset(struct ArrowBuffer collections[VGI_CATALOG_COLLECTIONS]) {
    for (int i = 0; i < VGI_CATALOG_COLLECTIONS; ++i) ArrowBufferReset(&collections[i]);
}
int VgiCatalogCollectionsSet(struct VgiBatch *result,
                             const struct ArrowBuffer collections[VGI_CATALOG_COLLECTIONS]) {
    int rc = 0;
    for (int i = 0; !rc && i < VGI_CATALOG_COLLECTIONS; ++i)
        rc = collections[i].size_bytes
                 ? VgiBatchSetBytes(result, collection_names[i], collections[i].data,
                                    collections[i].size_bytes)
                 : VgiBatchSetNull(result, collection_names[i]);
    return rc;
}
void VgiCatalogCollectionsDigest(const struct ArrowBuffer collections[VGI_CATALOG_COLLECTIONS],
                                 char hex[65]) {
    static const char prefix[] = "vgi.catalog.contents.2.2";
    crypto_hash_sha256_state state;
    crypto_hash_sha256_init(&state);
    crypto_hash_sha256_update(&state, (const unsigned char *)prefix, sizeof(prefix));
    for (int i = 0; i < VGI_CATALOG_COLLECTIONS; ++i) {
        const struct ArrowBuffer *data = &collections[i];
        unsigned char present = data->size_bytes > 0, length[8];
        crypto_hash_sha256_update(&state, (const unsigned char *)collection_names[i],
                                  strlen(collection_names[i]) + 1);
        crypto_hash_sha256_update(&state, &present, 1);
        if (!present) continue;
        for (int b = 0; b < 8; ++b)
            length[b] = (unsigned char)((uint64_t)data->size_bytes >> (8 * b));
        crypto_hash_sha256_update(&state, length, sizeof(length));
        crypto_hash_sha256_update(&state, data->data, (unsigned long long)data->size_bytes);
    }
    unsigned char hash[32];
    crypto_hash_sha256_final(&state, hash);
    sodium_bin2hex(hex, 65, hash, sizeof(hash));
}
