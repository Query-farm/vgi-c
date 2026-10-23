#include <errno.h>
#include <json-c/json.h>
#include <math.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

#include "vgi/rpc.h"
#include "tickets_internal.h"
struct Tickets {
    unsigned char key[32];
    int64_t ceiling;
    VgiTicketCatalog catalog;
    void *state;
};
static char *copy_view(struct ArrowStringView v) {
    if (v.size_bytes < 0 || memchr(v.data, 0, (size_t)v.size_bytes)) return NULL;
    char *out = malloc((size_t)v.size_bytes + 1);
    if (out) {
        memcpy(out, v.data, (size_t)v.size_bytes);
        out[v.size_bytes] = 0;
    }
    return out;
}
static void violation(struct json_object *list, const char *field, const char *description) {
    struct json_object *v = json_object_new_object();
    json_object_object_add(v, "field", json_object_new_string(field));
    json_object_object_add(v, "description", json_object_new_string(description));
    json_object_array_add(list, v);
}
static int invalid(struct VgiRpcCall *c, const char *kind, struct json_object *violations) {
    int rc =
        VgiRpcFail(c, "ValueError", "INVALID_ARGUMENT", kind, "attach ticket request is invalid");
    struct json_object *details = json_object_new_array(), *detail = json_object_new_object();
    json_object_object_add(detail, "@type", json_object_new_string("vgi_rpc.BadRequest"));
    json_object_object_add(detail, "field_violations", json_object_get(violations));
    json_object_array_add(details, detail);
    VgiRpcSetErrorDetails(c, json_object_to_json_string_ext(details, JSON_C_TO_STRING_PLAIN));
    json_object_put(details);
    return rc;
}
static int ticket_error(struct VgiRpcCall *c, int code) {
    if (code == ENOMEM) return code;
    if (code == ESTALE) {
        int rc = VgiRpcFail(c, "AttachTicketExpiredError", "FAILED_PRECONDITION",
                            "attach_ticket_expired", "attach ticket has expired");
        VgiRpcSetErrorDetails(c,
                              "[{\"@type\":\"vgi_rpc.PreconditionFailure\",\"violations\":[{"
                              "\"type\":\"ATTACH_TICKET\",\"subject\":\"vgi_attach_ticket\","
                              "\"description\":\"attach ticket has expired\"}]}]");
        return rc;
    }
    struct json_object *v = json_object_new_array();
    violation(v, "vgi_attach_ticket", "attach ticket not accepted");
    int rc = invalid(c, "attach_ticket_invalid", v);
    json_object_put(v);
    return rc;
}
int VgiTicketCheckOptions(const struct VgiBatch *info, struct ArrowError *error) {
    const struct ArrowArrayView *list = VgiBatchGet(info, "attach_option_specs");
    if (!list || ArrowArrayViewIsNull(list, 0)) return 0;
    int64_t start = ArrowArrayViewListChildOffset(list, 0),
            end = ArrowArrayViewListChildOffset(list, 1);
    for (int64_t i = start; i < end; ++i) {
        struct ArrowStringView bytes = VgiValueString(list->children[0], i);
        struct VgiBatch spec = {0};
        int rc =
            VgiBatchDecode(&spec, (struct ArrowBufferView){{bytes.data}, bytes.size_bytes}, error);
        char *name = !rc ? copy_view(VgiBatchString(&spec, "name")) : NULL;
        if (!rc && !name) rc = ENOMEM;
        if (!rc && !strcasecmp(name, "vgi_attach_ticket")) {
            ArrowErrorSet(error, "vgi_attach_ticket is a reserved attach option");
            rc = EINVAL;
        }
        free(name);
        VgiBatchReset(&spec);
        if (rc) return rc;
    }
    return 0;
}
int VgiTicketRedeem(struct VgiRpcCall *c) {
    if (strcmp(c->method->name, "catalog_attach")) return 0;
    struct VgiBatch request = {0}, options = {0}, restored = {0}, outer = {0};
    struct VgiAttachTicket claims = {0};
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    int rc = VgiBatchChild(&c->request, "request", &request, &c->error);
    if (!rc && VgiBatchBytes(&request, "options").size_bytes)
        rc = VgiBatchChild(&request, "options", &options, &c->error);
    int64_t ticket = -1;
    for (int64_t i = 0; !rc && i < options.schema.n_children; ++i)
        if (!strcasecmp(options.schema.children[i]->name, "vgi_attach_ticket")) ticket = i;
    if (rc || ticket < 0) goto done;
    if (options.schema.n_children != 1) {
        struct json_object *v = json_object_new_array();
        violation(v, "options", "vgi_attach_ticket must be the only option");
        rc = invalid(c, "invalid_request", v);
        json_object_put(v);
        goto done;
    }
    if (options.array.length != 1 ||
        options.view.children[ticket]->storage_type != NANOARROW_TYPE_STRING ||
        ArrowArrayViewIsNull(options.view.children[ticket], 0) || !c->signing_key) {
        rc = ticket_error(c, EINVAL);
        goto done;
    }
    char *token = copy_view(VgiValueString(options.view.children[ticket], 0));
    if (!token) {
        rc = ticket_error(c, EINVAL);
        goto done;
    }
    rc = VgiAttachTicketOpen(c->signing_key, c->principal, token, (double)time(NULL), &claims);
    sodium_memzero(token, strlen(token));
    free(token);
    if (rc) {
        rc = ticket_error(c, rc);
        goto done;
    }
    rc = VgiBatchInit(&restored, &request.schema, &c->error);
    for (int64_t i = 0; !rc && i < request.schema.n_children; ++i) {
        const char *name = request.schema.children[i]->name;
        struct ArrowArray *out = restored.array.children[i];
        if (!strcmp(name, "name"))
            rc = ArrowArrayAppendString(out, ArrowCharView(claims.catalog_name));
        else if (!strcmp(name, "data_version_spec"))
            rc = ArrowArrayAppendString(out, ArrowCharView(claims.data_version_spec));
        else if (!strcmp(name, "implementation_version"))
            rc = ArrowArrayAppendString(out, ArrowCharView(claims.implementation_version));
        else if (!strcmp(name, "options"))
            rc = claims.options.size_bytes
                     ? ArrowArrayAppendBytes(out,
                                             (struct ArrowBufferView){{claims.options.data},
                                                                      claims.options.size_bytes})
                     : ArrowArrayAppendNull(out, 1);
        else
            rc = VgiArrayAppendValue(out, restored.schema.children[i], request.view.children[i],
                                     request.schema.children[i], 0, &c->error);
    }
    if (!rc) rc = VgiBatchFinishRow(&restored, &c->error);
    if (!rc) rc = VgiIpcWriteBatch(&restored.schema, &restored.array, NULL, &bytes, &c->error);
    if (!rc) rc = VgiBatchInit(&outer, &c->request.schema, &c->error);
    if (!rc) rc = VgiBatchSetBytes(&outer, "request", bytes.data, bytes.size_bytes);
    if (!rc) rc = VgiBatchFinishRow(&outer, &c->error);
    if (!rc) {
        VgiBatchReset(&c->request);
        c->request = outer;
        memset(&outer, 0, sizeof(outer));
    }
done:
    VgiBatchReset(&request);
    VgiBatchReset(&options);
    VgiBatchReset(&restored);
    VgiBatchReset(&outer);
    VgiAttachTicketReset(&claims);
    ArrowBufferReset(&bytes);
    return rc;
}
static int seal_attach(void *state, struct VgiRpcCall *c, struct VgiRpcReply *reply) {
    struct Tickets *s = state;
    if (!c->principal || !*c->principal) {
        int rc = VgiRpcFail(c, "PermissionError", "PERMISSION_DENIED", "action_denied",
                            "an anonymous caller cannot seal an attach ticket");
        VgiRpcSetErrorDetails(
            c, "[{\"@type\":\"vgi_rpc.ErrorInfo\",\"metadata\":{\"action\":\"seal_attach\"}}]");
        return rc;
    }
    struct VgiBatch raw = {0}, request = {0}, options = {0}, spec = {0}, payload = {0};
    struct ArrowSchema schema = {0};
    struct ArrowBuffer token;
    ArrowBufferInit(&token);
    struct json_object *violations = json_object_new_array();
    int rc = VgiBatchChild(&c->request, "request", &raw, &c->error);
    if (!rc) rc = VgiSchemaTicketsSealAttachRequest(&schema, &c->error);
    if (!rc) rc = VgiBatchCoerce(&raw, &schema, &request, &c->error);
    if (schema.release) ArrowSchemaRelease(&schema);
    char *catalog = !rc ? copy_view(VgiBatchString(&request, "catalog_name")) : NULL,
         *data = !rc ? copy_view(VgiBatchString(&request, "data_version_spec")) : NULL,
         *impl = !rc ? copy_view(VgiBatchString(&request, "implementation_version")) : NULL;
    if (!rc && (!catalog || !data || !impl)) rc = ENOMEM;
    int64_t ttl = VgiBatchInt(&request, "ttl_seconds", 0);
    if (ttl < 0) violation(violations, "ttl_seconds", "must be 0 or positive");
    struct ArrowBufferView encoded = VgiBatchBytes(&request, "options");
    if (!rc && encoded.size_bytes) rc = VgiBatchDecode(&options, encoded, &c->error);
    if (options.array.length > 1) violation(violations, "options", "must be a one-row record");
    if (encoded.size_bytes > 16384)
        violation(violations, "options", "a ticket carries at most 16384 bytes");
    const struct VgiBatch *info = catalog ? s->catalog(s->state, catalog) : NULL;
    if (!rc && !info) violation(violations, "catalog_name", "no catalog with this name");
    const struct ArrowArrayView *specs = info ? VgiBatchGet(info, "attach_option_specs") : NULL;
    int64_t first = specs && !ArrowArrayViewIsNull(specs, 0)
                        ? ArrowArrayViewListChildOffset(specs, 0)
                        : 0,
            last = specs && !ArrowArrayViewIsNull(specs, 0)
                       ? ArrowArrayViewListChildOffset(specs, 1)
                       : 0;
    unsigned char *known = calloc((size_t)options.schema.n_children + 1, 1);
    if (!known) rc = ENOMEM;
    for (int64_t i = first; !rc && i < last; ++i) {
        struct ArrowStringView bytes = VgiValueString(specs->children[0], i);
        rc = VgiBatchDecode(&spec, (struct ArrowBufferView){{bytes.data}, bytes.size_bytes},
                            &c->error);
        char *name = !rc ? copy_view(VgiBatchString(&spec, "name")) : NULL;
        if (!rc && !name) rc = ENOMEM;
        int found = 0;
        for (int64_t j = 0; !rc && options.array.length == 1 && j < options.schema.n_children; ++j)
            if (!strcasecmp(name, options.schema.children[j]->name)) {
                known[j] = 1;
                found = 1;
            }
        if (!rc && !found && VgiBatchInt(&spec, "required", 0)) {
            char field[512];
            snprintf(field, sizeof(field), "options.%s", name);
            violation(violations, field, "required");
        }
        free(name);
        VgiBatchReset(&spec);
    }
    for (int64_t i = 0; !rc && options.array.length == 1 && i < options.schema.n_children; ++i)
        if (!known[i] || !strcasecmp(options.schema.children[i]->name, "vgi_attach_ticket")) {
            char field[512];
            snprintf(field, sizeof(field), "options.%s", options.schema.children[i]->name);
            violation(violations, field, "not an attach option this catalog declares");
        }
    free(known);
    if (!rc && json_object_array_length(violations)) rc = invalid(c, "invalid_request", violations);
    if (!rc) {
        int64_t lifetime = ttl ? ttl : s->ceiling;
        if (s->ceiling && lifetime > s->ceiling) lifetime = s->ceiling;
        int64_t now = (int64_t)time(NULL);
        if (lifetime > INT64_MAX - now)
            rc = EINVAL;
        else {
            unsigned char random[16];
            char id[33];
            randombytes_buf(random, 16);
            sodium_bin2hex(id, sizeof(id), random, 16);
            struct VgiAttachTicket claims = {.issued_at = now,
                                             .expires_at = lifetime ? now + lifetime : 0,
                                             .ticket_id = id,
                                             .catalog_name = catalog,
                                             .data_version_spec = data,
                                             .implementation_version = impl};
            ArrowBufferInit(&claims.options);
            if (options.array.length == 1 && options.schema.n_children)
                rc = ArrowBufferAppend(&claims.options, encoded.data.as_uint8, encoded.size_bytes);
            if (!rc) rc = VgiAttachTicketMint(s->key, c->principal, &claims, NULL, &token);
            ArrowBufferReset(&claims.options);
            if (!rc) rc = VgiBatchInitNamed(&payload, "TicketsSealAttachResult", &c->error);
            if (!rc) rc = VgiBatchSetString(&payload, "ticket", (char *)token.data);
            /* Microseconds since the epoch (UTC); null when the ticket never expires. */
            struct ArrowArray *expires = VgiBatchColumn(&payload, "expires_at");
            if (!rc && claims.expires_at > INT64_MAX / 1000000)
                rc = EINVAL;
            else if (!rc)
                rc = claims.expires_at ? ArrowArrayAppendInt(expires, claims.expires_at * 1000000)
                                       : ArrowArrayAppendNull(expires, 1);
            if (!rc) rc = VgiBatchFinishRow(&payload, &c->error);
            if (!rc) rc = VgiRpcReplyPayload(c, reply, &payload);
        }
    }
    if (rc && !c->error_type) {
        violation(violations, "request", "invalid seal_attach request");
        rc = invalid(c, "invalid_request", violations);
    }
    free(catalog);
    free(data);
    free(impl);
    VgiBatchReset(&raw);
    VgiBatchReset(&request);
    VgiBatchReset(&options);
    VgiBatchReset(&spec);
    VgiBatchReset(&payload);
    ArrowBufferReset(&token);
    json_object_put(violations);
    return rc;
}
static void release(void *state) {
    struct Tickets *s = state;
    sodium_memzero(s->key, 32);
    free(s);
}
int VgiTicketRegister(struct VgiRpcServer *server, const unsigned char key[32], int64_t ceiling,
                      VgiTicketCatalog lookup, void *state, struct ArrowError *error) {
    if (ceiling < 0) return EINVAL;
    struct Tickets *s = calloc(1, sizeof(*s));
    if (!s) return ENOMEM;
    memcpy(s->key, key, 32);
    s->ceiling = ceiling;
    s->catalog = lookup;
    s->state = state;
    struct VgiRpcProtocol p = {VGI_TICKETS_NAME,
                               VGI_TICKETS_VERSION,
                               VGI_TICKETS_HASH,
                               VGI_TICKETS_METHODS,
                               VGI_TICKETS_METHOD_COUNT,
                               seal_attach,
                               s};
    int rc = VgiRpcServerAddManagedProtocol(server, &p, release, error);
    if (rc) release(s);
    return rc;
}
