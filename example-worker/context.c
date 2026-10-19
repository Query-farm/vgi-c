#include "vgi/context.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "fixtures.h"
static int named(const struct ExampleFunction *f, const char *name) {
    return !strcmp(f->name, name);
}
int ExampleContextSupported(const char *name) {
    const char *names[] = {"settings_aware",   "struct_settings", "filter_by_setting",
                           "secret_in_out",    "secret_demo",     "scoped_secret_demo",
                           "multi_secret_demo"};
    for (size_t i = 0; i < sizeof(names) / sizeof(*names); ++i)
        if (!strcmp(name, names[i])) return 1;
    return 0;
}
int ExampleContextBind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *out) {
    struct ExampleFunction *f = data;
    if (named(f, "filter_by_setting")) return ArrowSchemaDeepCopy(&call->input_schema, out);
    int64_t count = named(f, "secret_in_out") ? call->input_schema.n_children + 1
                    : named(f, "settings_aware")
                        ? 3 + (VgiBatchInt(&call->settings, "vgi_verbose_mode", 0) != 0)
                    : named(f, "struct_settings")   ? 2
                    : named(f, "multi_secret_demo") ? 1
                                                    : 3;
    ArrowSchemaInit(out);
    int rc = ArrowSchemaSetTypeStruct(out, count);
    for (int64_t i = 0; !rc && i < count; ++i) {
        const char *name;
        enum ArrowType type = NANOARROW_TYPE_STRING;
        if (named(f, "secret_in_out") && i < count - 1) {
            ArrowSchemaRelease(out->children[i]);
            rc = ArrowSchemaDeepCopy(call->input_schema.children[i], out->children[i]);
            continue;
        }
        if (named(f, "secret_in_out"))
            name = "secret_string";
        else if (named(f, "settings_aware")) {
            const char *names[] = {"id", "greeting", "value", "details"};
            name = names[i];
            type = i == 0   ? NANOARROW_TYPE_INT64
                   : i == 2 ? NANOARROW_TYPE_DOUBLE
                            : NANOARROW_TYPE_STRING;
        } else if (named(f, "struct_settings")) {
            name = i ? "label" : "n";
            type = i ? NANOARROW_TYPE_STRING : NANOARROW_TYPE_INT64;
        } else if (named(f, "multi_secret_demo"))
            name = "api_key";
        else if (named(f, "secret_demo")) {
            const char *names[] = {"key", "value", "arrow_type"};
            name = names[i];
        } else {
            const char *names[] = {"scope", "found", "secret_keys"};
            name = names[i];
            if (i == 1) type = NANOARROW_TYPE_BOOL;
        }
        rc = ArrowSchemaSetType(out->children[i], type);
        if (!rc) rc = ArrowSchemaSetName(out->children[i], name);
    }
    return rc;
}
int ExampleContextLookups(void *data, struct VgiFunctionCall *call, struct VgiBatch *result) {
    (void)data;
    struct ArrowStringView path = VgiArgumentString(call, 0, NULL);
    const char *names[] = {"lookup_secret_types", "lookup_scopes", "lookup_names"};
    struct ArrowStringView values[] = {ArrowCharView("vgi_example"),
                                       path.data ? path : ArrowCharView(""), ArrowCharView("")};
    for (int i = 0; i < 3; ++i) {
        struct ArrowArray *list = VgiBatchColumn(result, names[i]);
        NANOARROW_RETURN_NOT_OK(ArrowArrayAppendString(list->children[0], values[i]));
        NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(list));
    }
    return 0;
}
static int compare_keys(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}
int ExampleContextProcess(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                          struct VgiBatch *out, struct ArrowBuffer *metadata, int *finished) {
    (void)metadata;
    struct ExampleFunction *f = data;
    int exchange = named(f, "filter_by_setting") || named(f, "secret_in_out");
    if (!exchange && *(int *)call->state) {
        *finished = 1;
        return 0;
    }
    if (!exchange) *(int *)call->state = 1;
    int rc = VgiBatchInit(out, &call->output_schema, &call->rpc->error);
    struct ArrowStringView path = VgiArgumentString(call, 0, NULL);
    const struct ArrowSchema *secret_schema = NULL;
    const struct ArrowArrayView *secret = VgiSecretSelect(
        call, "vgi_example",
        named(f, "multi_secret_demo") || named(f, "scoped_secret_demo") ? &path : NULL,
        &secret_schema);
    int64_t fields = secret_schema ? secret_schema->n_children : 0;
    const char **keys = fields ? calloc((size_t)fields, sizeof(*keys)) : NULL;
    if (fields && !keys) rc = ENOMEM;
    if (!rc) {
        for (int64_t i = 0; i < fields; ++i) keys[i] = secret_schema->children[i]->name;
        if (fields) qsort(keys, (size_t)fields, sizeof(*keys), compare_keys);
    }
    int64_t rows = exchange                  ? input->array.length
                   : named(f, "secret_demo") ? fields
                   : (named(f, "settings_aware") || named(f, "struct_settings"))
                       ? VgiArgumentInt(call, 0, NULL, 0)
                       : 1;
    for (int64_t row = 0; !rc && row < rows; ++row) {
        if (named(f, "filter_by_setting")) {
            const struct ArrowArrayView *value = VgiBatchGet(input, "value");
            if (!value) {
                rc = EINVAL;
                break;
            }
            if (ArrowArrayViewIsNull(value, row) ||
                ArrowArrayViewGetIntUnsafe(value, row) <
                    VgiBatchInt(&call->settings, "threshold", 0))
                continue;
            rc = VgiArrayAppendValue(&out->array, &out->schema, &input->view, &input->schema, row,
                                     &call->rpc->error);
            continue;
        }
        for (int64_t col = 0; !rc && col < out->schema.n_children; ++col) {
            const char *name = out->schema.children[col]->name;
            struct ArrowArray *dest = out->array.children[col];
            struct ArrowBuffer text;
            ArrowBufferInit(&text);
            if (named(f, "secret_in_out")) {
                if (!strcmp(name, "secret_string")) {
                    const struct ArrowArrayView *value =
                        VgiStructField(secret, secret_schema, "secret_string");
                    struct ArrowStringView string = VgiValueString(value, 0);
                    rc = string.data ? ArrowArrayAppendString(dest, string)
                                     : ArrowArrayAppendNull(dest, 1);
                } else {
                    int64_t id = VgiFieldIndex(&input->schema, name);
                    rc = id < 0 ? EINVAL
                                : VgiArrayAppendValue(
                                      dest, out->schema.children[col], input->view.children[id],
                                      input->schema.children[id], row, &call->rpc->error);
                }
            } else if (named(f, "settings_aware")) {
                if (!strcmp(name, "id"))
                    rc = ArrowArrayAppendInt(dest, row);
                else if (!strcmp(name, "value"))
                    rc = ArrowArrayAppendDouble(
                        dest,
                        (double)row * 2.5 * (double)VgiBatchInt(&call->settings, "multiplier", 1));
                else if (!strcmp(name, "greeting")) {
                    struct ArrowStringView greeting = VgiBatchString(&call->settings, "greeting");
                    rc = ArrowArrayAppendString(dest,
                                                greeting.data ? greeting : ArrowCharView("Hello"));
                } else {
                    char value[64];
                    snprintf(value, sizeof(value), "row_%lld", (long long)row);
                    rc = ArrowArrayAppendString(dest, ArrowCharView(value));
                }
            } else if (named(f, "struct_settings")) {
                int64_t id = VgiFieldIndex(&call->settings.schema, "config");
                const struct ArrowSchema *schema =
                    id < 0 ? NULL : call->settings.schema.children[id];
                const struct ArrowArrayView *value = VgiBatchGet(&call->settings, "config");
                const struct ArrowArrayView *start = VgiStructField(value, schema, "start"),
                                            *step = VgiStructField(value, schema, "step"),
                                            *label = VgiStructField(value, schema, "label");
                int64_t first = start && !ArrowArrayViewIsNull(start, 0)
                                    ? ArrowArrayViewGetIntUnsafe(start, 0)
                                    : 0,
                        increment = step && !ArrowArrayViewIsNull(step, 0)
                                        ? ArrowArrayViewGetIntUnsafe(step, 0)
                                        : 1;
                if (!strcmp(name, "n"))
                    rc = ArrowArrayAppendInt(
                        dest, (int64_t)((uint64_t)first + (uint64_t)row * (uint64_t)increment));
                else {
                    struct ArrowStringView prefix = VgiValueString(label, 0);
                    if (!prefix.data) prefix = ArrowCharView("item");
                    char suffix[64];
                    snprintf(suffix, sizeof(suffix), "_%lld", (long long)row);
                    rc = ArrowBufferAppend(&text, prefix.data, prefix.size_bytes);
                    if (!rc) rc = ArrowBufferAppend(&text, suffix, strlen(suffix));
                    if (!rc)
                        rc = ArrowArrayAppendString(
                            dest,
                            (struct ArrowStringView){(const char *)text.data, text.size_bytes});
                }
            } else if (named(f, "secret_demo")) {
                int64_t id = VgiFieldIndex(secret_schema, keys[row]);
                if (!strcmp(name, "key"))
                    rc = ArrowArrayAppendString(dest, ArrowCharView(keys[row]));
                else if (!strcmp(name, "arrow_type")) {
                    struct ArrowSchemaView type;
                    rc = ArrowSchemaViewInit(&type, secret_schema->children[id], &call->rpc->error);
                    if (!rc)
                        rc = ArrowArrayAppendString(
                            dest, ArrowCharView(type.type == NANOARROW_TYPE_STRING
                                                    ? "string"
                                                    : ArrowTypeString(type.type)));
                } else {
                    rc = VgiValueRender(secret->children[id], secret_schema->children[id], 0, &text,
                                        &call->rpc->error);
                    if (!rc)
                        rc = ArrowArrayAppendString(
                            dest,
                            (struct ArrowStringView){(const char *)text.data, text.size_bytes});
                }
            } else if (named(f, "multi_secret_demo")) {
                struct ArrowStringView value =
                    VgiValueString(VgiStructField(secret, secret_schema, "api_key"), 0);
                rc = ArrowArrayAppendString(dest, value.data ? value : ArrowCharView(""));
            } else if (!strcmp(name, "scope"))
                rc = ArrowArrayAppendString(dest, path.data ? path : ArrowCharView(""));
            else if (!strcmp(name, "found"))
                rc = ArrowArrayAppendInt(dest, secret != NULL);
            else {
                for (int64_t i = 0; !rc && i < fields; ++i) {
                    if (i) rc = ArrowBufferAppend(&text, ",", 1);
                    if (!rc) rc = ArrowBufferAppend(&text, keys[i], strlen(keys[i]));
                }
                if (!rc)
                    rc = ArrowArrayAppendString(
                        dest, (struct ArrowStringView){(const char *)text.data, text.size_bytes});
            }
            ArrowBufferReset(&text);
        }
        if (!rc) rc = ArrowArrayFinishElement(&out->array);
    }
    free(keys);
    if (!rc) rc = VgiBatchFinish(out, &call->rpc->error);
    return rc;
}
