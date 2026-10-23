#include <errno.h>
#include <inttypes.h>
#include <json-c/json.h>
#include <math.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>

#include "fixtures.h"
#include "vgi/context.h"

int ExampleExtraSupported(const char *name) {
    return !strcmp(name, "format_number") || !strcmp(name, "smart_format") ||
           !strcmp(name, "secret_field") || !strcmp(name, "return_secret_value") ||
           !strcmp(name, "secret_cached_scalar");
}
static int append_rendered(struct ArrowBuffer *out, const struct ArrowArrayView *value,
                           int64_t row) {
    struct ArrowStringView text = VgiValueString(value, row);
    if (text.data) return ArrowBufferAppend(out, text.data, text.size_bytes);
    if (!value || !value->length || ArrowArrayViewIsNull(value, row)) return 0;
    char buffer[128];
    if (value->storage_type == NANOARROW_TYPE_BOOL)
        snprintf(buffer, sizeof(buffer), "%s",
                 ArrowArrayViewGetIntUnsafe(value, row) ? "true" : "false");
    else if (value->storage_type == NANOARROW_TYPE_DOUBLE ||
             value->storage_type == NANOARROW_TYPE_FLOAT)
        snprintf(buffer, sizeof(buffer), "%.17g", ArrowArrayViewGetDoubleUnsafe(value, row));
    else if (VgiValueIsNumeric(value))
        snprintf(buffer, sizeof(buffer), "%" PRId64, ArrowArrayViewGetIntUnsafe(value, row));
    else
        return EINVAL;
    return ArrowBufferAppend(out, buffer, (int64_t)strlen(buffer));
}
static const struct ArrowArrayView *secret(struct VgiFunctionCall *call, const char *field) {
    const struct ArrowSchema *schema = NULL;
    const struct ArrowArrayView *value = VgiSecretSelect(call, "vgi_example", NULL, &schema);
    return VgiStructField(value, schema, field);
}
static int key_compare(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}
static int secret_json(struct VgiFunctionCall *call, struct ArrowBuffer *text) {
    struct json_object *object = json_object_new_object();
    if (!object) return ENOMEM;
    int rc = 0;
    for (int64_t i = 0; !rc && i < call->secrets.view.n_children; ++i) {
        const struct ArrowSchema *schema = call->secrets.schema.children[i];
        const struct ArrowArrayView *value = call->secrets.view.children[i];
        if (value->storage_type != NANOARROW_TYPE_STRUCT) continue;
        int64_t type_index = VgiFieldIndex(schema, "type");
        struct ArrowStringView type = type_index < 0
                                          ? (struct ArrowStringView){NULL, 0}
                                          : VgiValueString(value->children[type_index], 0);
        if (!type.data || type.size_bytes != 11 || memcmp(type.data, "vgi_example", 11)) continue;
        const char **keys = calloc((size_t)schema->n_children, sizeof(*keys));
        if (!keys) {
            rc = ENOMEM;
            break;
        }
        for (int64_t j = 0; j < schema->n_children; ++j) keys[j] = schema->children[j]->name;
        qsort(keys, (size_t)schema->n_children, sizeof(*keys), key_compare);
        for (int64_t j = 0; !rc && j < schema->n_children; ++j) {
            struct ArrowBuffer rendered;
            ArrowBufferInit(&rendered);
            int64_t index = VgiFieldIndex(schema, keys[j]);
            rc = append_rendered(&rendered, value->children[index], 0);
            if (!rc)
                json_object_object_add(object, keys[j],
                                       json_object_new_string_len((const char *)rendered.data,
                                                                  (int)rendered.size_bytes));
            ArrowBufferReset(&rendered);
        }
        free(keys);
        break;
    }
    const char *serialized = json_object_to_json_string_ext(object, JSON_C_TO_STRING_PLAIN);
    if (!rc) rc = ArrowBufferAppend(text, serialized, (int64_t)strlen(serialized));
    json_object_put(object);
    return rc;
}
int ExampleExtraProcess(struct ExampleFunction *function, struct VgiFunctionCall *call,
                        const struct VgiBatch *input, struct VgiBatch *output,
                        struct ArrowBuffer *metadata) {
    int rc = VgiBatchInit(output, &call->output_schema, &call->rpc->error);
    int formatting =
        !strcmp(function->name, "format_number") || !strcmp(function->name, "smart_format");
    struct ArrowBuffer text;
    ArrowBufferInit(&text);
    if (!formatting) {
        if (!strcmp(function->name, "return_secret_value"))
            rc = secret_json(call, &text);
        else if (!strcmp(function->name, "secret_field")) {
            rc = ArrowBufferAppend(&text, "port=", 5);
            if (!rc) rc = append_rendered(&text, secret(call, "port"), 0);
            if (!rc) rc = ArrowBufferAppend(&text, ";name=", 6);
            if (!rc) rc = append_rendered(&text, secret(call, "secret_string"), 0);
        } else {
            uint64_t nonce;
            randombytes_buf(&nonce, sizeof(nonce));
            nonce >>= 8;
            char digits[32];
            snprintf(digits, sizeof(digits), "|%" PRIu64, nonce);
            rc = append_rendered(&text, secret(call, "secret_string"), 0);
            if (!rc) rc = ArrowBufferAppend(&text, digits, (int64_t)strlen(digits));
            if (!rc) rc = VgiMetadataSet(metadata, "vgi.cache.ttl", "300");
            if (!rc) rc = VgiMetadataSet(metadata, "vgi.cache.scope", "catalog");
            if (!rc) rc = VgiMetadataSet(metadata, "vgi.cache.per_value", "1");
        }
    }
    for (int64_t row = 0; !rc && row < input->array.length; ++row) {
        if (formatting) {
            text.size_bytes = 0;
            const struct ArrowArrayView *value =
                input->view.n_children ? input->view.children[input->view.n_children - 1] : NULL;
            if (!value || ArrowArrayViewIsNull(value, row)) {
                rc = ArrowArrayAppendNull(output->array.children[0], 1);
                if (!rc) rc = ArrowArrayFinishElement(&output->array);
                continue;
            }
            char digits[512];
            double number;
            rc = VgiValueNumber(value, input->schema.children[input->view.n_children - 1], row,
                                &number, &call->rpc->error);
            if (rc) break;
            if (!strcmp(function->name, "format_number")) {
                int64_t precision = function->arguments.n_children > 1
                                        ? VgiArgumentInt(call, 0, "precision", 0)
                                        : 0;
                if (precision < 0 || precision > 10) {
                    rc = EINVAL;
                    break;
                }
                if (function->arguments.n_children > 2) {
                    struct ArrowStringView prefix = VgiArgumentString(call, 1, "prefix");
                    rc = ArrowBufferAppend(&text, prefix.data, prefix.size_bytes);
                }
                snprintf(digits, sizeof(digits), "%.*f", (int)precision, number);
            } else {
                if (isfinite(number) && trunc(number) == number)
                    snprintf(digits, sizeof(digits), "%.1f", number);
                else {
                    for (int precision = 1; precision <= 17; ++precision) {
                        snprintf(digits, sizeof(digits), "%.*g", precision, number);
                        if (strtod(digits, NULL) == number) break;
                    }
                }
                if (!strcmp(function->arguments.children[0]->format, "u")) {
                    struct ArrowStringView prefix = VgiArgumentString(call, 0, "prefix");
                    rc = ArrowBufferAppend(&text, prefix.data, prefix.size_bytes);
                } else {
                    int64_t width = VgiArgumentInt(call, 0, "width", 0) - (int64_t)strlen(digits);
                    for (int64_t i = 0; !rc && i < width; ++i)
                        rc = ArrowBufferAppend(&text, " ", 1);
                }
            }
            if (!rc) rc = ArrowBufferAppend(&text, digits, (int64_t)strlen(digits));
        }
        if (!rc)
            rc = ArrowArrayAppendString(
                output->array.children[0],
                (struct ArrowStringView){(const char *)text.data, text.size_bytes});
        if (!rc) rc = ArrowArrayFinishElement(&output->array);
    }
    if (!rc) rc = VgiBatchFinish(output, &call->rpc->error);
    ArrowBufferReset(&text);
    return rc;
}
