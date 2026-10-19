#include <sodium.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "vgi/cache.h"
#include "vgi/partition.h"
#define CHECK(x)                                                    \
    do {                                                            \
        if (!(x)) {                                                 \
            fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x); \
            return 1;                                               \
        }                                                           \
    } while (0)
static int equals(const struct ArrowBuffer *metadata, const char *key, const char *text) {
    struct ArrowStringView value = VgiMetadataGet((const char *)metadata->data, key);
    return value.size_bytes == (int64_t)strlen(text) &&
           !memcmp(value.data, text, (size_t)value.size_bytes);
}
int main(void) {
    struct ArrowError error = {{0}};
    struct ArrowBuffer metadata;
    ArrowBufferInit(&metadata);
    CHECK(!VgiMetadataSet(&metadata, "unrelated", "preserve"));
    struct VgiCacheControl cache = {.has_ttl = 1,
                                    .ttl_seconds = -1,
                                    .etag = "\"v1\"",
                                    .revalidatable = 1,
                                    .not_modified = 1,
                                    .partition_scope = 1};
    CHECK(!VgiCacheControlMetadata(&cache, &metadata));
    CHECK(equals(&metadata, "vgi.cache.ttl", "0"));
    CHECK(equals(&metadata, "vgi.cache.scope", "catalog"));
    CHECK(equals(&metadata, "vgi.cache.not_modified", "1"));
    CHECK(equals(&metadata, "unrelated", "preserve"));
    struct ArrowSchema schema = {0};
    ArrowSchemaInit(&schema);
    CHECK(!ArrowSchemaSetTypeStruct(&schema, 4));
    const char *names[] = {"unsigned", "signed", "decimal", "all_null"};
    const char *formats[] = {"L", "l", "d:38,0", "u"};
    for (int i = 0; i < 4; ++i) {
        CHECK(!ArrowSchemaSetName(schema.children[i], names[i]));
        CHECK(!ArrowSchemaSetFormat(schema.children[i], formats[i]));
        CHECK(!VgiPartitionField(schema.children[i]));
    }
    struct VgiBatch input = {0}, bounds = {0};
    CHECK(!VgiBatchInit(&input, &schema, &error));
    const uint64_t unsigned_values[] = {UINT64_MAX, UINT64_MAX - 1, UINT64_C(9007199254740992),
                                        UINT64_C(9007199254740993)};
    const int64_t signed_values[] = {INT64_MAX, INT64_MAX - 1, INT64_MIN, INT64_MIN + 1};
    for (int row = 0; row < 4; ++row) {
        CHECK(!ArrowArrayAppendUInt(input.array.children[0], unsigned_values[row]));
        CHECK(!ArrowArrayAppendInt(input.array.children[1], signed_values[row]));
        struct ArrowDecimal value;
        ArrowDecimalInit(&value, 128, 38, 0);
        ArrowDecimalSetInt(&value, signed_values[row]);
        CHECK(!ArrowArrayAppendDecimal(input.array.children[2], &value));
        CHECK(!ArrowArrayAppendNull(input.array.children[3], 1));
        CHECK(!ArrowArrayFinishElement(&input.array));
    }
    CHECK(!VgiBatchFinish(&input, &error));
    CHECK(!VgiPartitionMetadata(&schema, &input, &metadata, &error));
    struct ArrowStringView encoded =
        VgiMetadataGet((const char *)metadata.data, "vgi_partition_values#b64");
    CHECK(encoded.data);
    unsigned char *decoded = malloc((size_t)encoded.size_bytes);
    CHECK(decoded);
    size_t size;
    CHECK(!sodium_base642bin(decoded, (size_t)encoded.size_bytes, encoded.data,
                             (size_t)encoded.size_bytes, NULL, &size, NULL,
                             sodium_base64_VARIANT_ORIGINAL));
    CHECK(!VgiBatchDecode(&bounds, (struct ArrowBufferView){{decoded}, (int64_t)size}, &error));
    CHECK(bounds.array.length == 2 && bounds.schema.n_children == 4);
    CHECK(ArrowArrayViewGetUIntUnsafe(bounds.view.children[0], 0) == UINT64_C(9007199254740992));
    CHECK(ArrowArrayViewGetUIntUnsafe(bounds.view.children[0], 1) == UINT64_MAX);
    CHECK(ArrowArrayViewGetIntUnsafe(bounds.view.children[1], 0) == INT64_MIN);
    CHECK(ArrowArrayViewGetIntUnsafe(bounds.view.children[1], 1) == INT64_MAX);
    struct ArrowDecimal value;
    ArrowDecimalInit(&value, 128, 38, 0);
    ArrowArrayViewGetDecimalUnsafe(bounds.view.children[2], 0, &value);
    CHECK(ArrowDecimalGetIntUnsafe(&value) == INT64_MIN);
    ArrowArrayViewGetDecimalUnsafe(bounds.view.children[2], 1, &value);
    CHECK(ArrowDecimalGetIntUnsafe(&value) == INT64_MAX);
    CHECK(ArrowArrayViewIsNull(bounds.view.children[3], 0) &&
          ArrowArrayViewIsNull(bounds.view.children[3], 1));
    free(decoded);
    VgiBatchReset(&bounds);
    VgiBatchReset(&input);
    ArrowSchemaRelease(&schema);
    ArrowBufferReset(&metadata);
    puts("Cache metadata and exact partition bounds passed");
    return 0;
}
