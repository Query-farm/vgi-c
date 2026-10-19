#include <math.h>
#include <stdio.h>
#include <string.h>

#include "vgi/worker.h"

const struct ArrowArrayView *VgiArgument(const struct VgiFunctionCall *call, int64_t position,
                                         const char *name) {
    const struct ArrowArrayView *values = &call->arguments.view;
    const struct ArrowSchema *schema = &call->arguments.schema;
    if (!schema->release || !values->length) return NULL;
    int nested = schema->n_children == 1 && schema->children[0]->name &&
                 !strcmp(schema->children[0]->name, "args");
    if (nested) {
        schema = schema->children[0];
        values = values->children[0];
        if (values->storage_type != NANOARROW_TYPE_STRUCT) return NULL;
    }
    if (name) {
        int64_t index = VgiFieldIndex(schema, name);
        if (index < 0 && nested) {
            char key[512];
            snprintf(key, sizeof(key), "named_%s", name);
            index = VgiFieldIndex(schema, key);
        }
        if (index >= 0) return values->children[index];
    }
    if (position < 0) return NULL;
    /* The engine sends only positional constants, densely numbered. Hand-built
     * callers may instead send the entire declared signature. */
    int64_t wire_position = position;
    if (call->argument_schema && schema->n_children < call->argument_schema->n_children) {
        wire_position = 0;
        for (int64_t i = 0; i < position && i < call->argument_schema->n_children; ++i) {
            const struct ArrowSchema *argument = call->argument_schema->children[i];
            struct ArrowStringView constant = VgiMetadataGet(argument->metadata, "vgi_const");
            struct ArrowStringView named = VgiMetadataGet(argument->metadata, "vgi_arg");
            if (constant.data && !named.data) ++wire_position;
        }
    }
    if (!nested) return wire_position < values->n_children ? values->children[wire_position] : NULL;
    char key[64];
    snprintf(key, sizeof(key), "positional_%lld", (long long)wire_position);
    int64_t index = VgiFieldIndex(schema, key);
    return index < 0 ? NULL : values->children[index];
}
int64_t VgiArgumentInt(const struct VgiFunctionCall *call, int64_t position, const char *name,
                       int64_t fallback) {
    const struct ArrowArrayView *value = VgiArgument(call, position, name);
    return !value || !value->length || ArrowArrayViewIsNull(value, 0) || !VgiValueIsNumeric(value)
               ? fallback
               : ArrowArrayViewGetIntUnsafe(value, 0);
}
static const struct ArrowSchema *argument_value_schema(const struct VgiFunctionCall *call,
                                                       const struct ArrowArrayView *value) {
    const struct ArrowArrayView *root = &call->arguments.view;
    const struct ArrowSchema *schema = &call->arguments.schema;
    if (root->n_children == 1 && schema->children[0]->name &&
        !strcmp(schema->children[0]->name, "args")) {
        root = root->children[0];
        schema = schema->children[0];
    }
    for (int64_t i = 0; i < root->n_children; ++i)
        if (root->children[i] == value) return schema->children[i];
    return NULL;
}
const struct ArrowSchema *VgiArgumentSchema(const struct VgiFunctionCall *call, int64_t position,
                                            const char *name) {
    return argument_value_schema(call, VgiArgument(call, position, name));
}
double VgiArgumentDouble(const struct VgiFunctionCall *call, int64_t position, const char *name,
                         double fallback) {
    const struct ArrowArrayView *value = VgiArgument(call, position, name);
    const struct ArrowSchema *schema = argument_value_schema(call, value);
    double result;
    return !schema || VgiValueDouble(value, schema, 0, &result, NULL) ? fallback : result;
}
struct ArrowStringView VgiArgumentString(const struct VgiFunctionCall *call, int64_t position,
                                         const char *name) {
    return VgiValueString(VgiArgument(call, position, name), 0);
}
