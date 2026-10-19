#include <stdio.h>
#include <string.h>

#include "vgi/generated/protocol.h"

#define CHECK(expr)                                                    \
    do {                                                               \
        if (!(expr)) {                                                 \
            fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #expr); \
            return 1;                                                  \
        }                                                              \
    } while (0)

int main(void) {
    struct ArrowError error = {{0}};
    for (size_t i = 0; i < VgiSchemaCount(); ++i) {
        struct ArrowSchema schema = {0};
        struct ArrowArrayView view;
        CHECK(VgiSchemaInit(VgiSchemaName(i), &schema, &error) == 0);
        CHECK(ArrowArrayViewInitFromSchema(&view, &schema, &error) == 0);
        ArrowArrayViewReset(&view);
        ArrowSchemaRelease(&schema);
    }
    CHECK(VgiSchemaName(VgiSchemaCount()) == NULL);
    CHECK(VgiMethodFind("missing") == NULL);
    CHECK(VgiMethodFind(NULL) == NULL);
    for (size_t i = 0; i < VGI_METHOD_COUNT; ++i) {
        const struct VgiMethod *method = &VGI_METHODS[i];
        CHECK(VgiMethodFind(method->name) == method);
        CHECK(method->params != NULL);
        CHECK(method->kind != VGI_METHOD_UNARY || method->result != NULL);
    }
    struct ArrowSchema schema = {0};
    CHECK(VgiSchemaInit("missing", &schema, &error) != 0);
    CHECK(schema.release == NULL);
    CHECK(VgiSchemaFunctionInfo(&schema, &error) == 0);
    int dictionaries = 0;
    for (int64_t i = 0; i < schema.n_children; ++i)
        dictionaries += schema.children[i]->dictionary != NULL;
    CHECK(dictionaries > 0);
    CHECK(VgiSchemaFunctionInfo(&schema, &error) != 0); /* Do not leak a live output. */
    ArrowSchemaRelease(&schema);
    printf("Validated %zu schemas and %zu methods (%s %s)\n", VgiSchemaCount(), VGI_METHOD_COUNT,
           VGI_PROTOCOL_NAME, VGI_PROTOCOL_VERSION);
    return 0;
}
