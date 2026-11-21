/* pass_claims: the fixture function of the SDK-neutral hosted-mode
 * conformance harness (the extension repository's test/hosted/), registered
 * only when VGI_ORCHARD_HOSTED is on so ordinary function counts are unchanged.
 *
 * main.pass_claims(x BIGINT) -> VARCHAR returns, for each input row, the
 * caller's identity as the JSON text vgi-python's fixture produces with
 * json.dumps(..., sort_keys=True): {"claims": <the verified claim set>,
 * "domain": "orchard", "principal": "<sub>"}. Claims keep their JSON types. */
#include "hosted_fixture.h"

#include <errno.h>
#include <inttypes.h>
#include <json-c/json.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "fixtures.h"

static int append(struct ArrowBuffer *out, const char *text) {
    return ArrowBufferAppend(out, text, (int64_t)strlen(text));
}
/* A JSON string as Python's json.dumps(ensure_ascii=True) writes it. */
static int append_string(struct ArrowBuffer *out, const char *s, size_t n) {
    int rc = append(out, "\"");
    for (size_t i = 0; !rc && i < n;) {
        unsigned char c = (unsigned char)s[i];
        char escape[16];
        if (c == '"' || c == '\\') {
            snprintf(escape, sizeof(escape), "\\%c", c);
            ++i;
        } else if (c == '\n' || c == '\r' || c == '\t' || c == '\b' || c == '\f') {
            snprintf(escape, sizeof(escape), "\\%c",
                     c == '\n'   ? 'n'
                     : c == '\r' ? 'r'
                     : c == '\t' ? 't'
                     : c == '\b' ? 'b'
                                 : 'f');
            ++i;
        } else if (c < 0x20) {
            snprintf(escape, sizeof(escape), "\\u%04x", c);
            ++i;
        } else if (c < 0x80) {
            escape[0] = (char)c;
            escape[1] = 0;
            ++i;
        } else {
            /* Decode one UTF-8 sequence; invalid bytes become U+FFFD. */
            uint32_t cp = 0xFFFD;
            size_t length = c >= 0xF0 ? 4 : c >= 0xE0 ? 3 : c >= 0xC0 ? 2 : 1;
            if (length > 1 && i + length <= n) {
                uint32_t value = c & (0xFF >> (length + 1));
                int ok = 1;
                for (size_t k = 1; ok && k < length; ++k) {
                    unsigned char next = (unsigned char)s[i + k];
                    ok = (next & 0xC0) == 0x80;
                    value = (value << 6) | (next & 0x3F);
                }
                if (ok)
                    cp = value;
                else
                    length = 1;
            } else
                length = 1;
            i += length;
            if (cp > 0xFFFF) {
                cp -= 0x10000;
                snprintf(escape, sizeof(escape), "\\u%04x\\u%04x", 0xD800 + (cp >> 10),
                         0xDC00 + (cp & 0x3FF));
            } else
                snprintf(escape, sizeof(escape), "\\u%04x", cp);
        }
        rc = append(out, escape);
    }
    return rc ? rc : append(out, "\"");
}
static int append_value(struct ArrowBuffer *out, struct json_object *value);
static int compare_keys(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}
static int append_value(struct ArrowBuffer *out, struct json_object *value) {
    char number[64];
    switch (json_object_get_type(value)) {
        case json_type_null:
            return append(out, "null");
        case json_type_boolean:
            return append(out, json_object_get_boolean(value) ? "true" : "false");
        case json_type_int:
            snprintf(number, sizeof(number), "%" PRId64, json_object_get_int64(value));
            return append(out, number);
        case json_type_double: {
            /* Python's repr: the shortest round-trip form, with ".0" if integral. */
            double d = json_object_get_double(value);
            for (int precision = 1; precision <= 17; ++precision) {
                snprintf(number, sizeof(number), "%.*g", precision, d);
                if (strtod(number, NULL) == d) break;
            }
            if (!strpbrk(number, ".eni")) strcat(number, ".0");
            return append(out, number);
        }
        case json_type_string:
            return append_string(out, json_object_get_string(value),
                                 (size_t)json_object_get_string_len(value));
        case json_type_array: {
            int rc = append(out, "[");
            size_t n = json_object_array_length(value);
            for (size_t i = 0; !rc && i < n; ++i) {
                if (i) rc = append(out, ", ");
                if (!rc) rc = append_value(out, json_object_array_get_idx(value, i));
            }
            return rc ? rc : append(out, "]");
        }
        case json_type_object: {
            size_t n = (size_t)json_object_object_length(value), k = 0;
            const char **keys = calloc(n ? n : 1, sizeof(*keys));
            if (!keys) return ENOMEM;
            json_object_object_foreach(value, key, child) {
                (void)child;
                keys[k++] = key;
            }
            qsort(keys, k, sizeof(*keys), compare_keys);
            int rc = append(out, "{");
            for (size_t i = 0; !rc && i < k; ++i) {
                if (i) rc = append(out, ", ");
                if (!rc) rc = append_string(out, keys[i], strlen(keys[i]));
                if (!rc) rc = append(out, ": ");
                struct json_object *child = NULL;
                json_object_object_get_ex(value, keys[i], &child);
                if (!rc) rc = append_value(out, child);
            }
            free(keys);
            return rc ? rc : append(out, "}");
        }
    }
    return EINVAL;
}
static int pass_json(const struct VgiRpcCall *rpc, struct ArrowBuffer *out) {
    struct json_object *claims =
        rpc->auth_claims_json ? json_tokener_parse(rpc->auth_claims_json) : NULL;
    int rc = append(out, "{\"claims\": ");
    if (!rc)
        rc = claims && json_object_is_type(claims, json_type_object) ? append_value(out, claims)
                                                                     : append(out, "{}");
    if (!rc) rc = append(out, ", \"domain\": ");
    const char *domain = rpc->auth_domain ? rpc->auth_domain : "";
    if (!rc) rc = append_string(out, domain, strlen(domain));
    if (!rc) rc = append(out, ", \"principal\": ");
    const char *principal = rpc->principal ? rpc->principal : "";
    if (!rc) rc = append_string(out, principal, strlen(principal));
    if (!rc) rc = append(out, "}");
    if (claims) json_object_put(claims);
    return rc;
}
static int process(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                   struct VgiBatch *output, struct ArrowBuffer *metadata, int *finished) {
    (void)data;
    (void)metadata;
    (void)finished;
    int rc = VgiBatchInit(output, &call->output_schema, &call->rpc->error);
    if (rc) return rc;
    struct ArrowBuffer text;
    ArrowBufferInit(&text);
    rc = pass_json(call->rpc, &text);
    for (int64_t row = 0; !rc && row < input->array.length; ++row) {
        rc = ArrowArrayAppendString(
            output->array.children[0],
            (struct ArrowStringView){(const char *)text.data, text.size_bytes});
        if (!rc) rc = ArrowArrayFinishElement(&output->array);
    }
    if (!rc) rc = VgiBatchFinish(output, &call->rpc->error);
    ArrowBufferReset(&text);
    return rc;
}
static int bind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *output) {
    (void)call;
    return ArrowSchemaDeepCopy(&((struct ExampleFunction *)data)->output, output);
}
static void release(void *data) {
    struct ExampleFunction *function = data;
    if (function->arguments.release) ArrowSchemaRelease(&function->arguments);
    if (function->output.release) ArrowSchemaRelease(&function->output);
    free(function);
}
int ExampleHostedRegister(struct VgiWorker *worker, struct ArrowError *error) {
    struct ExampleFunction *data = calloc(1, sizeof(*data));
    if (!data) return ENOMEM;
    strcpy(data->name, "pass_claims");
    strcpy(data->path, "main");
    strcpy(data->catalog, "example");
    const char *override = getenv("VGI_WORKER_CATALOG_NAME");
    if (override && *override) snprintf(data->catalog, sizeof(data->catalog), "%s", override);
    struct VgiFunction fn = {0};
    fn.userdata = data;
    fn.release = release;
    ArrowSchemaInit(&data->arguments);
    ArrowSchemaInit(&data->output);
    /* x is the input column. The trailing constant variadic accepts and ignores
     * positional values: the harness calls pass_claims the way vgi-python's
     * client does, with one positional argument whose value is ignored. */
    int rc = ArrowSchemaSetTypeStruct(&data->arguments, 2);
    if (!rc) {
        ArrowSchemaRelease(data->arguments.children[0]);
        rc = VgiArgumentInit(data->arguments.children[0], "x", "l", 0, 0, 0, error);
    }
    if (!rc) {
        ArrowSchemaRelease(data->arguments.children[1]);
        rc = VgiArgumentInit(data->arguments.children[1], "ignored", "n", 1, 0, 1, error);
    }
    if (!rc) rc = ArrowSchemaSetTypeStruct(&data->output, 1);
    if (!rc) rc = ArrowSchemaSetType(data->output.children[0], NANOARROW_TYPE_STRING);
    if (!rc) rc = ArrowSchemaSetName(data->output.children[0], "result");
    if (!rc)
        rc = VgiFunctionInit(&fn, data->name, VGI_SCALAR, &data->arguments, &data->output, error);
    if (rc) {
        VgiFunctionReset(&fn);
        return rc;
    }
    fn.catalog = data->catalog;
    fn.schema_path = data->path;
    fn.bind = bind;
    fn.process = process;
    rc = VgiBatchSetString(&fn.info, "description",
                           "Return the caller's verified Orchard Pass claims as JSON.");
    if (!rc) rc = VgiWorkerRegisterFunction(worker, &fn, error);
    VgiFunctionReset(&fn);
    return rc;
}
