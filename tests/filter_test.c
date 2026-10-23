#include "vgi/filter.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "vgi/rpc.h"
#define CHECK(x)                                                    \
    do {                                                            \
        if (!(x)) {                                                 \
            fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x); \
            exit(1);                                                \
        }                                                           \
    } while (0)
static struct ArrowError error;
static int parse_over(const char *json, int64_t value, const struct ArrowSchema *column,
                      struct VgiFilters **out) {
    struct VgiBatch batch = {0}, init = {0};
    struct ArrowSchema schema = {0}, input = {0};
    struct ArrowBuffer metadata, ipc;
    ArrowBufferInit(&metadata);
    ArrowBufferInit(&ipc);
    ArrowSchemaInit(&schema);
    CHECK(!ArrowSchemaSetTypeStruct(&schema, 3));
    const char *names[] = {"filter_spec", "value_0", "type_0"};
    enum ArrowType types[] = {NANOARROW_TYPE_STRING, NANOARROW_TYPE_INT64, NANOARROW_TYPE_INT8};
    for (int i = 0; i < 3; ++i) {
        CHECK(!ArrowSchemaSetType(schema.children[i], types[i]));
        CHECK(!ArrowSchemaSetName(schema.children[i], names[i]));
    }
    schema.children[0]->flags = 0;
    CHECK(!VgiMetadataSet(&metadata, "vgi_filter_encoding", "vgi.filters.v2"));
    CHECK(!VgiMetadataSet(&metadata, "vgi_filter_version", "2"));
    CHECK(!VgiMetadataSet(&metadata, "vgi_evaluation_context", "vgi.none.v1"));
    CHECK(!ArrowSchemaSetMetadata(&schema, (char *)metadata.data));
    CHECK(!VgiBatchInit(&batch, &schema, &error));
    CHECK(!VgiBatchSetString(&batch, "filter_spec", json));
    CHECK(!VgiBatchSetInt(&batch, "value_0", value));
    CHECK(!VgiBatchSetNull(&batch, "type_0"));
    CHECK(!VgiBatchFinishRow(&batch, &error));
    CHECK(!VgiIpcWriteBatch(&batch.schema, &batch.array, NULL, &ipc, &error));
    ArrowSchemaInit(&input);
    CHECK(!ArrowSchemaSetTypeStruct(&input, 1));
    ArrowSchemaRelease(input.children[0]);
    CHECK(!ArrowSchemaDeepCopy(column, input.children[0]));
    int rc = VgiFiltersParse((struct ArrowBufferView){{ipc.data}, ipc.size_bytes}, &input, &init,
                             out, &error);
    ArrowSchemaRelease(&input);
    ArrowSchemaRelease(&schema);
    ArrowBufferReset(&metadata);
    ArrowBufferReset(&ipc);
    VgiBatchReset(&batch);
    return rc;
}
static int parse(const char *json, int64_t value, struct VgiFilters **out) {
    struct ArrowSchema column = {0};
    ArrowSchemaInit(&column);
    CHECK(!ArrowSchemaSetType(&column, NANOARROW_TYPE_INT64));
    CHECK(!ArrowSchemaSetName(&column, "x"));
    int rc = parse_over(json, value, &column, out);
    ArrowSchemaRelease(&column);
    return rc;
}
static int apply(struct VgiFilters *filters, int64_t a, int64_t b, int64_t *length) {
    struct ArrowSchema schema = {0};
    struct VgiBatch input = {0}, output = {0};
    ArrowSchemaInit(&schema);
    CHECK(!ArrowSchemaSetTypeStruct(&schema, 1));
    CHECK(!ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_INT64));
    CHECK(!ArrowSchemaSetName(schema.children[0], "x"));
    CHECK(!VgiBatchInit(&input, &schema, &error));
    CHECK(!VgiBatchSetInt(&input, "x", a));
    CHECK(!VgiBatchFinishRow(
        &input,
        &error)); /* FinishRow finalizes the view; construct the second row with a fresh append. */
    VgiBatchReset(&input);
    CHECK(!VgiBatchInit(&input, &schema, &error));
    CHECK(!ArrowArrayAppendInt(input.array.children[0], a));
    CHECK(!ArrowArrayFinishElement(&input.array));
    CHECK(!ArrowArrayAppendInt(input.array.children[0], b));
    CHECK(!ArrowArrayFinishElement(&input.array));
    CHECK(!VgiBatchFinish(&input, &error));
    int rc = VgiFiltersApply(filters, &input, &output, &error);
    *length = output.array.length;
    VgiBatchReset(&output);
    VgiBatchReset(&input);
    ArrowSchemaRelease(&schema);
    return rc;
}
#define START                                                                                     \
    "{\"encoding\":\"vgi.filters.v2\",\"semantics\":\"vgi.duckdb.standard.v1\",\"kind\":"         \
    "\"snapshot\",\"predicates\":[{\"id\":\"p\",\"revision\":0,\"mode\":\"required\",\"source\":" \
    "\"query\",\"expression\":"
#define END "}]}"
#define COL "{\"node\":\"column_ref\",\"column_index\":0,\"column_name\":\"x\"}"
#define LIT "{\"node\":\"literal\",\"value_ref\":0}"
/* Column x of a type that is not a number: DuckDB MAP, ARRAY (fixed-size list)
 * and INTERVAL. VgiValueDouble must never see these (it reads decimal type
 * information nanoarrow leaves uninitialized for them). */
enum Kind { MAP, ARRAY, INTERVAL, INTERVAL_MONTHS };
static void column_schema(enum Kind kind, struct ArrowSchema *out) {
    ArrowSchemaInit(out);
    switch (kind) {
        case MAP:
            CHECK(!ArrowSchemaSetType(out, NANOARROW_TYPE_MAP));
            CHECK(!ArrowSchemaSetType(out->children[0]->children[0], NANOARROW_TYPE_STRING));
            CHECK(!ArrowSchemaSetType(out->children[0]->children[1], NANOARROW_TYPE_INT64));
            break;
        case ARRAY:
            CHECK(!ArrowSchemaSetTypeFixedSize(out, NANOARROW_TYPE_FIXED_SIZE_LIST, 2));
            CHECK(!ArrowSchemaSetType(out->children[0], NANOARROW_TYPE_INT32));
            break;
        case INTERVAL:
            CHECK(!ArrowSchemaSetType(out, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO));
            break;
        case INTERVAL_MONTHS:
            CHECK(!ArrowSchemaSetType(out, NANOARROW_TYPE_INTERVAL_MONTHS));
            break;
    }
    CHECK(!ArrowSchemaSetName(out, "x"));
}
static void append_value(enum Kind kind, struct ArrowArray *x) {
    switch (kind) {
        case MAP: {
            struct ArrowArray *entries = x->children[0];
            CHECK(!ArrowArrayAppendString(entries->children[0], ArrowCharView("k")));
            CHECK(!ArrowArrayAppendInt(entries->children[1], 7));
            CHECK(!ArrowArrayFinishElement(entries));
            CHECK(!ArrowArrayFinishElement(x));
            break;
        }
        case ARRAY:
            CHECK(!ArrowArrayAppendInt(x->children[0], 1));
            CHECK(!ArrowArrayAppendInt(x->children[0], 2));
            CHECK(!ArrowArrayFinishElement(x));
            break;
        case INTERVAL:
        case INTERVAL_MONTHS: {
            struct ArrowInterval interval;
            ArrowIntervalInit(&interval, kind == INTERVAL ? NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO
                                                          : NANOARROW_TYPE_INTERVAL_MONTHS);
            interval.months = 1;
            interval.days = 2;
            interval.ns = 3;
            CHECK(!ArrowArrayAppendInterval(x, &interval));
            break;
        }
    }
}
static void check_not_a_number(enum Kind kind) {
    struct ArrowSchema column = {0}, schema = {0};
    column_schema(kind, &column);
    for (int negated = 0; negated < 2; ++negated) {
        char json[512];
        snprintf(json, sizeof(json),
                 "{\"encoding\":\"vgi.filters.v2\",\"semantics\":\"vgi.duckdb.standard.v1\","
                 "\"kind\":\"snapshot\",\"predicates\":[{\"id\":\"p\",\"revision\":0,"
                 "\"mode\":\"required\",\"source\":\"query\",\"expression\":{\"node\":"
                 "\"is_null\",\"expression\":{\"node\":\"column_ref\",\"column_index\":0,"
                 "\"column_name\":\"x\"},\"negated\":%s}}]}",
                 negated ? "true" : "false");
        struct VgiFilters *f = NULL;
        CHECK(!parse_over(json, 0, &column, &f));
        ArrowSchemaInit(&schema);
        CHECK(!ArrowSchemaSetTypeStruct(&schema, 1));
        ArrowSchemaRelease(schema.children[0]);
        CHECK(!ArrowSchemaDeepCopy(&column, schema.children[0]));
        struct VgiBatch input = {0}, output = {0};
        CHECK(!VgiBatchInit(&input, &schema, &error));
        /* Rows: value, null, value. */
        append_value(kind, input.array.children[0]);
        CHECK(!ArrowArrayFinishElement(&input.array));
        CHECK(!ArrowArrayAppendNull(input.array.children[0], 1));
        CHECK(!ArrowArrayFinishElement(&input.array));
        append_value(kind, input.array.children[0]);
        CHECK(!ArrowArrayFinishElement(&input.array));
        CHECK(!VgiBatchFinish(&input, &error));
        int rc = VgiFiltersApply(f, &input, &output, &error);
        if (rc) fprintf(stderr, "kind %d: %s\n", (int)kind, error.message);
        CHECK(rc == 0);
        CHECK(output.array.length == (negated ? 2 : 1));
        VgiBatchReset(&output);
        VgiBatchReset(&input);
        ArrowSchemaRelease(&schema);
        VgiFiltersFree(f);
    }
    ArrowSchemaRelease(&column);
}
int main(void) {
    check_not_a_number(MAP);
    check_not_a_number(ARRAY);
    check_not_a_number(INTERVAL);
    check_not_a_number(INTERVAL_MONTHS);
    struct VgiFilters *f = NULL;
    int64_t n = 0;
    CHECK(!parse(START "{\"node\":\"comparison\",\"op\":\"eq\",\"left\":" COL ",\"right\":" LIT
                       "}" END,
                 INT64_C(9007199254740993), &f));
    CHECK(!apply(f, INT64_C(9007199254740992), INT64_C(9007199254740993), &n));
    CHECK(n == 1);
    VgiFiltersFree(f);
    f = NULL;
    const char *invalid[] = {START "{\"node\":\"literal\",\"value_ref\":0}" END,
                             START
                             "{\"node\":\"comparison\",\"op\":\"eq\",\"left\":{\"node\":\"column_"
                             "ref\",\"column_index\":0,\"column_name\":\"wrong\"},\"right\":" LIT
                             "}" END,
                             START "{\"node\":\"is_null\",\"expression\":" COL
                                   ",\"negated\":false,\"negated\":true}" END,
                             START "{\"node\":\"is_null\",\"expression\":" COL
                                   ",\"negated\":false,\"negat\\u0065d\":true}" END};
    for (size_t i = 0; i < sizeof(invalid) / sizeof(*invalid); ++i) {
        CHECK(parse(invalid[i], 0, &f) == EINVAL);
        CHECK(f == NULL);
    }
    CHECK(!parse(
        START
        "{\"node\":\"comparison\",\"op\":\"eq\",\"left\":{\"node\":\"cast\",\"expression\":" COL
        ",\"type_ref\":0},\"right\":{\"node\":\"cast\",\"expression\":" LIT ",\"type_ref\":0}}" END,
        127, &f));
    CHECK(!apply(f, 126, 127, &n));
    CHECK(n == 1);
    CHECK(apply(f, 127, 128, &n) != 0);
    VgiFiltersFree(f);
    f = NULL;
    CHECK(!parse(START "{\"node\":\"comparison\",\"op\":\"eq\",\"left\":{\"node\":\"arithmetic\","
                       "\"op\":\"add\",\"left\":" COL ",\"right\":" LIT "},\"right\":" LIT "}" END,
                 1, &f));
    CHECK(apply(f, INT64_MAX, 0, &n) != 0);
    VgiFiltersFree(f);
    puts("Filter validation, exact integers, cast bounds, overflow, and non-number columns passed");
    return 0;
}
