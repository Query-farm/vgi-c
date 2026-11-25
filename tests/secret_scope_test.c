/* Secret scope matching against DuckDB's own rule (BaseSecret::MatchScore,
 * Haybarn 2.0.1 engine, duckdb#28684): an authority-only URI scope has an
 * implicit trailing '/'. data/scope_match_vectors.json holds 3025
 * {scope, path, match} cases generated from the engine's C++, copied verbatim
 * from the cross-SDK vector file shipped with the vgi-python fix. */
#include <json-c/json.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "vgi/context.h"
#define CHECK(x)                                                    \
    do {                                                            \
        if (!(x)) {                                                 \
            fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x); \
            exit(1);                                                \
        }                                                           \
    } while (0)
static struct ArrowStringView sv(const char *text) { return ArrowCharView(text); }

/* The first 25 cases of the vector file, inline so they read as documentation. */
static const struct {
    const char *scope, *path;
    int match;
} curated[] = {
    {"snmp://10.0.0.1", "snmp://10.0.0.1", 1},
    {"snmp://10.0.0.1", "snmp://10.0.0.1/x", 1},
    {"snmp://10.0.0.1", "snmp://10.0.0.15", 0},
    {"snmp://10.0.0.1", "snmp://10.0.0.1:161", 0},
    {"s3://bucket", "s3://bucket/key", 1},
    {"s3://bucket", "s3://bucket-evil/key", 0},
    {"s3://bucket", "s3://bucket?x=1", 0},
    {"dlt://vgi_dlt_fixture", "dlt://vgi_dlt_fixture:shop", 0},
    {"dlt://vgi_dlt_fixture", "dlt://vgi_dlt_fixture/shop", 1},
    {"s3://bucket/", "s3://bucket/key", 1},
    {"s3://bucket/pre", "s3://bucket/prefix/x", 1},
    {"s3://bucket/pre", "s3://bucket/other", 0},
    {"https://h?a", "https://h?ab", 1},
    {"https://h#f", "https://h#fx", 1},
    {"a://b\\c", "a://b\\cd", 1},
    {"snmp://", "snmp://anything", 1},
    {"snmp://10.0.0.", "snmp://10.0.0.15:161", 0},
    {"foo", "foobar", 1},
    {"c:\\data", "c:\\data2\\x", 1},
    {"x:/y://z", "x:/y://zz", 1},
    {"s3://bucket", "s3://buck", 0},
    {"s3://bucket", "s3://bucket", 1},
    {"vgi-conformance://host1", "vgi-conformance://host1/data", 1},
    {"vgi-conformance://host1", "vgi-conformance://host10/data", 0},
    {"vgi-conformance://host1", "vgi-conformance://host1:9000", 0},
};

static void curated_cases(void) {
    for (size_t i = 0; i < sizeof(curated) / sizeof(curated[0]); ++i) {
        if (!VgiScopeMatches(sv(curated[i].scope), sv(curated[i].path)) != !curated[i].match) {
            fprintf(stderr, "curated mismatch: scope=%s path=%s expected=%d\n", curated[i].scope,
                    curated[i].path, curated[i].match);
            exit(1);
        }
    }
    CHECK(!VgiScopeMatches(sv(""), sv("s3://bucket/x")));
    /* Views need not be NUL-terminated: only the first 15 bytes are the scope. */
    struct ArrowStringView cut = {"snmp://10.0.0.15", 15};
    CHECK(VgiScopeMatches(cut, sv("snmp://10.0.0.1/x")));
    CHECK(!VgiScopeMatches(cut, sv("snmp://10.0.0.15")));
}

static void shared_vectors(const char *file) {
    struct json_object *root = json_object_from_file(file);
    CHECK(root);
    struct json_object *cases = json_object_object_get(root, "cases");
    CHECK(cases && json_object_array_length(cases) > 3000);
    size_t mismatches = 0;
    for (size_t i = 0; i < json_object_array_length(cases); ++i) {
        struct json_object *c = json_object_array_get_idx(cases, i);
        struct json_object *scope = json_object_object_get(c, "scope");
        struct json_object *path = json_object_object_get(c, "path");
        /* Lengths, not strlen: the views are byte ranges. */
        struct ArrowStringView s = {json_object_get_string(scope),
                                    json_object_get_string_len(scope)};
        struct ArrowStringView p = {json_object_get_string(path), json_object_get_string_len(path)};
        int expected = json_object_get_boolean(json_object_object_get(c, "match"));
        if (!VgiScopeMatches(s, p) != !expected) {
            if (++mismatches <= 20)
                fprintf(stderr, "mismatch: scope=%s path=%s expected=%d\n", s.data, p.data,
                        expected);
        }
    }
    json_object_put(root);
    CHECK(mismatches == 0);
}

/* Three secrets of type snmp, one column each, the way the extension ships
 * them: struct<type, scope, api_key> in a one-row batch. */
static void selector(void) {
    const char *names[] = {"host1", "host15", "fallback"};
    const char *values[3][3] = {{"snmp", "snmp://10.0.0.1", "host1"},
                                {"snmp", "snmp://10.0.0.15", "host15"},
                                {"snmp", "", "default"}};
    const char *fields[] = {"type", "scope", "api_key"};
    struct ArrowError error;
    struct ArrowSchema schema;
    ArrowSchemaInit(&schema);
    CHECK(!ArrowSchemaSetTypeStruct(&schema, 3));
    for (int i = 0; i < 3; ++i) {
        CHECK(!ArrowSchemaSetTypeStruct(schema.children[i], 3));
        CHECK(!ArrowSchemaSetName(schema.children[i], names[i]));
        for (int j = 0; j < 3; ++j) {
            CHECK(!ArrowSchemaSetType(schema.children[i]->children[j], NANOARROW_TYPE_STRING));
            CHECK(!ArrowSchemaSetName(schema.children[i]->children[j], fields[j]));
        }
    }
    struct VgiFunctionCall call = {0};
    CHECK(!VgiBatchInit(&call.secrets, &schema, &error));
    schema.release(&schema);
    for (int i = 0; i < 3; ++i) {
        struct ArrowArray *column = call.secrets.array.children[i];
        for (int j = 0; j < 3; ++j)
            CHECK(!ArrowArrayAppendString(column->children[j], sv(values[i][j])));
        CHECK(!ArrowArrayFinishElement(column));
    }
    CHECK(!ArrowArrayFinishElement(&call.secrets.array));
    CHECK(!VgiBatchFinish(&call.secrets, &error));

    const char *paths[] = {"snmp://10.0.0.1/sys", "snmp://10.0.0.15", "snmp://10.0.0.1:161"};
    const char *expected[] = {"host1", "host15", "default"};
    for (int i = 0; i < 3; ++i) {
        const struct ArrowSchema *found_schema = NULL;
        struct ArrowStringView path = sv(paths[i]);
        const struct ArrowArrayView *found = VgiSecretSelect(&call, "snmp", &path, &found_schema);
        CHECK(found && found_schema);
        struct ArrowStringView key =
            VgiValueString(VgiStructField(found, found_schema, "api_key"), 0);
        if (key.size_bytes != (int64_t)strlen(expected[i]) ||
            memcmp(key.data, expected[i], (size_t)key.size_bytes)) {
            fprintf(stderr, "selector: path=%s expected=%s got=%.*s\n", paths[i], expected[i],
                    (int)key.size_bytes, key.data ? key.data : "");
            exit(1);
        }
    }
    VgiBatchReset(&call.secrets);
}

int main(int argc, char **argv) {
    if (argc != 2) {
        fprintf(stderr, "usage: %s scope_match_vectors.json\n", argv[0]);
        return 2;
    }
    curated_cases();
    shared_vectors(argv[1]);
    selector();
    puts("scope matching ok");
    return 0;
}
