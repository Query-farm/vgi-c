/* vgi.v3 canonical constraint records: the builder and decoder, the served
 * (relaxed) nullability, the ATTACH-equivalent checks including the CHECK
 * grammar, capability selection, and a declarative catalog that negotiates the
 * read profiles and serves its tables relaxed. */
#include "vgi/constraints.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "vgi/catalog.h"
#include "vgi/worker.h"
#include "vgi_rpc/stream.h"

#define CHECK(x)                                                    \
    do {                                                            \
        if (!(x)) {                                                 \
            fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x); \
            exit(1);                                                \
        }                                                           \
    } while (0)

static struct ArrowError error;
static const char *const main_schema[] = {"main"};

/* t(id int64 not null, v int64 not null, s utf8 not null,
 *   bbox struct<x double not null> not null, rid int64 not null [is_row_id]) */
static void table_columns(struct ArrowSchema *out) {
    ArrowSchemaInit(out);
    CHECK(!ArrowSchemaSetTypeStruct(out, 5));
    const char *names[] = {"id", "v", "s", "bbox", "rid"};
    enum ArrowType types[] = {NANOARROW_TYPE_INT64, NANOARROW_TYPE_INT64, NANOARROW_TYPE_STRING,
                              NANOARROW_TYPE_STRUCT, NANOARROW_TYPE_INT64};
    for (int i = 0; i < 5; ++i) {
        struct ArrowSchema *f = out->children[i];
        if (types[i] == NANOARROW_TYPE_STRUCT) {
            CHECK(!ArrowSchemaSetTypeStruct(f, 1));
            CHECK(!ArrowSchemaSetType(f->children[0], NANOARROW_TYPE_DOUBLE));
            CHECK(!ArrowSchemaSetName(f->children[0], "x"));
            f->children[0]->flags = 0;
        } else {
            CHECK(!ArrowSchemaSetType(f, types[i]));
        }
        CHECK(!ArrowSchemaSetName(f, names[i]));
        f->flags = 0;
    }
    struct ArrowBuffer metadata;
    CHECK(!ArrowMetadataBuilderInit(&metadata, NULL));
    CHECK(!ArrowMetadataBuilderAppend(&metadata, ArrowCharView("is_row_id"), ArrowCharView("")));
    CHECK(!ArrowSchemaSetMetadata(out->children[4], (const char *)metadata.data));
    ArrowBufferReset(&metadata);
}
static struct VgiConstraintSpec spec(const char *id, enum VgiConstraintKind kind,
                                     const struct VgiColumnPath *columns, size_t count) {
    struct VgiConstraintSpec s;
    memset(&s, 0, sizeof(s));
    s.schema_path = main_schema;
    s.schema_path_count = 1;
    s.table = "t";
    s.id = id;
    s.kind = kind;
    s.columns = columns;
    s.column_count = count;
    s.enforcement = VGI_ENFORCEMENT_ENFORCED;
    s.validation = VGI_VALIDATION_ALL_ROWS;
    return s;
}
static void encode(const struct VgiConstraintSpec *s, struct ArrowBuffer *out) {
    ArrowBufferInit(out);
    int rc = VgiConstraintRecordEncode(s, out, &error);
    if (rc) fprintf(stderr, "encode: %s\n", error.message);
    CHECK(!rc);
}
static int encode_fails(const struct VgiConstraintSpec *s, const char *needle) {
    struct ArrowBuffer out;
    ArrowBufferInit(&out);
    int rc = VgiConstraintRecordEncode(s, &out, &error);
    ArrowBufferReset(&out);
    return rc == EINVAL && strstr(error.message, needle);
}
static struct ArrowBufferView view(const struct ArrowBuffer *b) {
    return (struct ArrowBufferView){{b->data}, b->size_bytes};
}

static const char *const id_path[] = {"id"}, *const v_path[] = {"v"}, *const s_path[] = {"s"},
                         *const x_path[] = {"bbox", "x"}, *const rid_path[] = {"rid"};
static const struct VgiColumnPath id_col = {id_path, 1}, v_col = {v_path, 1}, s_col = {s_path, 1},
                                  x_col = {x_path, 2}, rid_col = {rid_path, 1};

static void check_builder(void) {
    struct VgiConstraintSpec s = spec("pk", VGI_CONSTRAINT_PRIMARY_KEY, &id_col, 1);
    s.declared_name = "t_pk";
    struct ArrowBuffer bytes;
    encode(&s, &bytes);
    struct VgiConstraintRecord r;
    CHECK(!VgiConstraintRecordDecode(view(&bytes), &r, &error));
    CHECK(r.kind == VGI_CONSTRAINT_PRIMARY_KEY && !strcmp(r.id, "pk") && !strcmp(r.table, "t"));
    CHECK(r.schema_path.count == 1 && !strcmp(r.schema_path.components[0], "main"));
    CHECK(r.column_count == 1 && !strcmp(r.columns[0].components[0], "id"));
    CHECK(r.enforcement == VGI_ENFORCEMENT_ENFORCED && r.validation == VGI_VALIDATION_ALL_ROWS);
    CHECK(!strcmp(r.declared_name, "t_pk") && !r.check_sql && !r.has_references);
    VgiConstraintRecordReset(&r);
    ArrowBufferReset(&bytes);

    s = spec("chk", VGI_CONSTRAINT_CHECK, NULL, 0);
    s.check_sql = "v >= 0";
    encode(&s, &bytes);
    CHECK(!VgiConstraintRecordDecode(view(&bytes), &r, &error));
    CHECK(r.kind == VGI_CONSTRAINT_CHECK && !strcmp(r.check_sql, "v >= 0"));
    CHECK(!strcmp(r.check_profile, VGI_SCALAR_EXPRESSION_READ) && r.check_default_context);
    VgiConstraintRecordReset(&r);
    ArrowBufferReset(&bytes);

    struct VgiConstraintReference ref = {main_schema, 1, "parent", &id_col, 1, "parent_pk"};
    s = spec("fk", VGI_CONSTRAINT_FOREIGN_KEY, &v_col, 1);
    s.references = &ref;
    encode(&s, &bytes);
    CHECK(!VgiConstraintRecordDecode(view(&bytes), &r, &error));
    CHECK(r.has_references && !strcmp(r.ref_table, "parent") &&
          !strcmp(r.ref_constraint_id, "parent_pk") && !strcmp(r.ref_key_owner, "parent"));
    CHECK(!strcmp(r.match, "SIMPLE") && !strcmp(r.on_delete, "NO_ACTION"));
    VgiConstraintRecordReset(&r);
    ArrowBufferReset(&bytes);

    s = spec("nn", VGI_CONSTRAINT_NOT_NULL, &id_col, 1);
    s.enforcement = VGI_ENFORCEMENT_UNSTATED;
    CHECK(encode_fails(&s, "stated explicitly"));
    s = spec("nn", VGI_CONSTRAINT_NOT_NULL, &id_col, 1);
    s.check_sql = "id > 0";
    CHECK(encode_fails(&s, "only CHECK"));
    struct VgiColumnPath two[] = {id_col, v_col};
    s = spec("nn", VGI_CONSTRAINT_NOT_NULL, two, 2);
    CHECK(encode_fails(&s, "exactly one column path"));
    struct VgiColumnPath dup[] = {id_col, id_col};
    s = spec("u", VGI_CONSTRAINT_UNIQUE, dup, 2);
    CHECK(encode_fails(&s, "duplicate column path"));
    s = spec("fk", VGI_CONSTRAINT_FOREIGN_KEY, &v_col, 1);
    CHECK(encode_fails(&s, "references is required"));
}

static int field_nullable(const struct ArrowSchema *s, int64_t i) {
    return (s->children[i]->flags & ARROW_FLAG_NULLABLE) != 0;
}
static void check_served_schema(void) {
    struct ArrowSchema columns = {0}, served = {0};
    table_columns(&columns);
    /* No records: every field (nested child included) is served nullable,
     * the row-id column keeps its declaration. */
    CHECK(!VgiConstraintServedSchema(&columns, NULL, 0, &served, &error));
    for (int64_t i = 0; i < 4; ++i) CHECK(field_nullable(&served, i));
    CHECK(served.children[3]->children[0]->flags & ARROW_FLAG_NULLABLE);
    CHECK(!field_nullable(&served, 4));
    ArrowSchemaRelease(&served);

    /* Records name id (PK) and bbox.x (NOT_NULL); a nullable declaration of a
     * record-named field is served non-nullable to match the attestation. */
    columns.children[0]->flags |= ARROW_FLAG_NULLABLE;
    struct ArrowBuffer pk, nn;
    struct VgiConstraintSpec s = spec("pk", VGI_CONSTRAINT_PRIMARY_KEY, &id_col, 1);
    encode(&s, &pk);
    s = spec("x_nn", VGI_CONSTRAINT_NOT_NULL, &x_col, 1);
    encode(&s, &nn);
    struct ArrowBufferView records[] = {view(&pk), view(&nn)};
    CHECK(!VgiConstraintServedSchema(&columns, records, 2, &served, &error));
    CHECK(!field_nullable(&served, 0) && field_nullable(&served, 1) && field_nullable(&served, 3));
    CHECK(!(served.children[3]->children[0]->flags & ARROW_FLAG_NULLABLE));
    ArrowSchemaRelease(&served);
    CHECK(!VgiConstraintValidate(main_schema, 1, "t", &columns, records, 2, &error));
    ArrowBufferReset(&pk);
    ArrowBufferReset(&nn);
    ArrowSchemaRelease(&columns);
}

static int validate_one(const struct VgiConstraintSpec *s, const char *needle) {
    struct ArrowSchema columns = {0};
    table_columns(&columns);
    struct ArrowBuffer bytes;
    encode(s, &bytes);
    struct ArrowBufferView record = view(&bytes);
    int rc = VgiConstraintValidate(main_schema, 1, "t", &columns, &record, 1, &error);
    ArrowBufferReset(&bytes);
    ArrowSchemaRelease(&columns);
    if (!needle) {
        if (rc) fprintf(stderr, "validate: %s\n", error.message);
        return rc == 0;
    }
    if (rc != EINVAL || !strstr(error.message, needle))
        fprintf(stderr, "expected '%s', got rc=%d '%s'\n", needle, rc, rc ? error.message : "");
    return rc == EINVAL && strstr(error.message, needle) && strstr(error.message, "main.t");
}
static int check_sql(const char *sql, const char *needle) {
    struct VgiConstraintSpec s = spec("chk", VGI_CONSTRAINT_CHECK, NULL, 0);
    s.check_sql = sql;
    return validate_one(&s, needle);
}
static void check_validation(void) {
    struct VgiConstraintSpec s = spec("nn", VGI_CONSTRAINT_NOT_NULL, &x_col, 1);
    CHECK(validate_one(&s, NULL));
    s.validation = VGI_VALIDATION_FUTURE_WRITES;
    CHECK(validate_one(&s, "ENFORCED + ALL_ROWS"));
    s = spec("nn", VGI_CONSTRAINT_NOT_NULL, &x_col, 1);
    s.enforcement = VGI_ENFORCEMENT_METADATA_ONLY;
    CHECK(validate_one(&s, "ENFORCED + ALL_ROWS"));
    s = spec("nn", VGI_CONSTRAINT_NOT_NULL, &x_col, 1);
    s.table = "other";
    CHECK(validate_one(&s, "not this table"));
    const char *const elsewhere[] = {"elsewhere"};
    s = spec("nn", VGI_CONSTRAINT_NOT_NULL, &x_col, 1);
    s.schema_path = elsewhere;
    CHECK(validate_one(&s, "not this table"));
    static const char *const missing[] = {"bbox", "y"};
    struct VgiColumnPath missing_col = {missing, 2};
    s = spec("nn", VGI_CONSTRAINT_NOT_NULL, &missing_col, 1);
    CHECK(validate_one(&s, "NOT_NULL column bbox.y"));
    s = spec("nn", VGI_CONSTRAINT_NOT_NULL, &rid_col, 1);
    CHECK(validate_one(&s, "NOT_NULL column rid"));
    s = spec("pk", VGI_CONSTRAINT_PRIMARY_KEY, &x_col, 1);
    CHECK(validate_one(&s, "nested"));
    const char *const two_schema[] = {"main", "inner"};
    s = spec("nn", VGI_CONSTRAINT_NOT_NULL, &id_col, 1);
    s.schema_path = two_schema;
    s.schema_path_count = 2;
    {
        struct ArrowSchema columns = {0};
        table_columns(&columns);
        struct ArrowBuffer bytes;
        encode(&s, &bytes);
        struct ArrowBufferView record = view(&bytes);
        CHECK(VgiConstraintValidate(two_schema, 2, "t", &columns, &record, 1, &error) == EINVAL);
        CHECK(strstr(error.message, "one-component schema path"));
        ArrowBufferReset(&bytes);
        ArrowSchemaRelease(&columns);
    }
    const char *const other_schema[] = {"other"};
    struct VgiConstraintReference ref = {other_schema, 1, "parent", &id_col, 1, "parent_pk"};
    s = spec("fk", VGI_CONSTRAINT_FOREIGN_KEY, &v_col, 1);
    s.references = &ref;
    CHECK(validate_one(&s, "same schema"));

    /* Two records together: duplicate ids, declared names, primary keys. */
    struct ArrowSchema columns = {0};
    table_columns(&columns);
    struct ArrowBuffer a, b;
    struct VgiConstraintSpec first = spec("k", VGI_CONSTRAINT_PRIMARY_KEY, &id_col, 1),
                             second = spec("k2", VGI_CONSTRAINT_PRIMARY_KEY, &v_col, 1);
    encode(&first, &a);
    encode(&second, &b);
    struct ArrowBufferView pair[] = {view(&a), view(&b)};
    CHECK(VgiConstraintValidate(main_schema, 1, "t", &columns, pair, 2, &error) == EINVAL);
    CHECK(strstr(error.message, "second PRIMARY_KEY") && strstr(error.message, "'k2'"));
    ArrowBufferReset(&b);
    second = spec("k", VGI_CONSTRAINT_UNIQUE, &v_col, 1);
    encode(&second, &b);
    pair[1] = view(&b);
    CHECK(VgiConstraintValidate(main_schema, 1, "t", &columns, pair, 2, &error) == EINVAL);
    CHECK(strstr(error.message, "duplicates another constraint id"));
    ArrowBufferReset(&a);
    ArrowBufferReset(&b);
    first = spec("u1", VGI_CONSTRAINT_UNIQUE, &id_col, 1);
    first.declared_name = "Label";
    second = spec("u2", VGI_CONSTRAINT_UNIQUE, &v_col, 1);
    second.declared_name = "label";
    encode(&first, &a);
    encode(&second, &b);
    pair[0] = view(&a);
    pair[1] = view(&b);
    CHECK(VgiConstraintValidate(main_schema, 1, "t", &columns, pair, 2, &error) == EINVAL);
    CHECK(strstr(error.message, "case-insensitively"));
    ArrowBufferReset(&a);
    ArrowBufferReset(&b);
    ArrowSchemaRelease(&columns);

    /* The CHECK grammar: one numeric comparison. */
    CHECK(check_sql("v >= 0", NULL));
    CHECK(check_sql("(V) < -1.5e3", NULL));
    CHECK(check_sql("(0 <> \"id\")", NULL));
    CHECK(check_sql("v != - 2 -- trailing comment", NULL));
    CHECK(check_sql("v > 0 AND id > 0", "Boolean comparison"));
    CHECK(check_sql("abs(v) > 0", "Boolean comparison"));
    CHECK(check_sql("v + 1 > 0", "Boolean comparison"));
    CHECK(check_sql("-v > 0", "Boolean comparison"));
    CHECK(check_sql("v::DOUBLE > 0", "Boolean comparison"));
    CHECK(check_sql("t.v > 0", "unsupported character"));
    CHECK(check_sql("v > '0'", "numeric operands"));
    CHECK(check_sql("s > 0", "primitive signed numeric"));
    CHECK(check_sql("rid > 0", "synthetic column"));
    CHECK(check_sql("w > 0", "missing"));
    CHECK(check_sql("v > 0 > 1", "Boolean comparison"));
}

static void check_capabilities(void) {
    struct VgiCatalogCapabilities available = {0}, offered = {0}, selected = {0}, decoded = {0};
    CHECK(!VgiCatalogCapabilitiesAdd(&available, VGI_CAPABILITY_READ, VGI_CONSTRAINT_RECORDS_READ,
                                     &error));
    CHECK(!VgiCatalogCapabilitiesAdd(&available, VGI_CAPABILITY_READ, VGI_SCALAR_EXPRESSION_READ,
                                     &error));
    CHECK(
        !VgiCatalogCapabilitiesAdd(&available, VGI_CAPABILITY_DDL, VGI_CONSTRAINT_CREATE, &error));
    CHECK(VgiCatalogCapabilitiesAdd(&available, VGI_CAPABILITY_READ, VGI_CONSTRAINT_RECORDS_READ,
                                    &error) == EINVAL);
    CHECK(VgiCatalogCapabilitiesAdd(&available, VGI_CAPABILITY_READ, "Bad.Profile", &error) ==
          EINVAL);
    /* Without the records profile none of its dependents survive. */
    CHECK(!VgiCatalogCapabilitiesAdd(&offered, VGI_CAPABILITY_READ, VGI_SCALAR_EXPRESSION_READ,
                                     &error));
    CHECK(!VgiCatalogCapabilitiesSelect(&available, &offered, &selected, &error));
    CHECK(!selected.counts[VGI_CAPABILITY_READ]);
    VgiCatalogCapabilitiesReset(&selected);
    CHECK(!VgiCatalogCapabilitiesAdd(&offered, VGI_CAPABILITY_READ, VGI_CONSTRAINT_RECORDS_READ,
                                     &error));
    CHECK(!VgiCatalogCapabilitiesAdd(&offered, VGI_CAPABILITY_DDL, VGI_CONSTRAINT_CREATE, &error));
    CHECK(!VgiCatalogCapabilitiesSelect(&available, &offered, &selected, &error));
    CHECK(selected.counts[VGI_CAPABILITY_READ] == 2);
    /* CREATE needs an enforcing authority. */
    CHECK(!VgiCatalogCapabilitiesHas(&selected, VGI_CAPABILITY_DDL, VGI_CONSTRAINT_CREATE));
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    CHECK(!VgiCatalogCapabilitiesEncode(&selected, &bytes, &error));
    CHECK(!VgiCatalogCapabilitiesDecode(view(&bytes), &decoded, &error));
    CHECK(decoded.counts[VGI_CAPABILITY_READ] == 2 &&
          !strcmp(decoded.profiles[VGI_CAPABILITY_READ][0], VGI_CONSTRAINT_RECORDS_READ));
    ArrowBufferReset(&bytes);
    VgiCatalogCapabilitiesReset(&available);
    VgiCatalogCapabilitiesReset(&offered);
    VgiCatalogCapabilitiesReset(&selected);
    VgiCatalogCapabilitiesReset(&decoded);
}

/* ---- A declarative catalog serving table t ------------------------------- */

static void table_info(const struct ArrowBufferView *records, size_t count,
                       struct ArrowBuffer *out) {
    struct ArrowSchema columns = {0};
    table_columns(&columns);
    struct ArrowBuffer schema_bytes;
    ArrowBufferInit(&schema_bytes);
    CHECK(!VgiIpcWriteSchema(&columns, &schema_bytes, &error));
    struct VgiBatch info = {0};
    CHECK(!VgiBatchInitNamed(&info, "TableInfo", &error));
    CHECK(!VgiBatchSetString(&info, "name", "t"));
    struct ArrowArray *path = VgiBatchColumn(&info, "schema_path");
    CHECK(!ArrowArrayAppendString(path->children[0], ArrowCharView("main")));
    CHECK(!ArrowArrayFinishElement(path));
    CHECK(!VgiBatchSetBytes(&info, "columns", schema_bytes.data, schema_bytes.size_bytes));
    struct ArrowArray *list = VgiBatchColumn(&info, "constraints");
    for (size_t i = 0; i < count; ++i) CHECK(!ArrowArrayAppendBytes(list->children[0], records[i]));
    CHECK(!ArrowArrayFinishElement(list));
    CHECK(!VgiBatchFinishRow(&info, &error));
    ArrowBufferInit(out);
    CHECK(!VgiIpcWriteBatch(&info.schema, &info.array, NULL, out, &error));
    VgiBatchReset(&info);
    ArrowBufferReset(&schema_bytes);
    ArrowSchemaRelease(&columns);
}
static void contents_init(struct VgiBatch *out, const struct ArrowBuffer *table) {
    struct ArrowSchema schema = {0};
    ArrowSchemaInit(&schema);
    CHECK(!ArrowSchemaSetTypeStruct(&schema, 2));
    CHECK(!ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_INT64));
    CHECK(!ArrowSchemaSetName(schema.children[0], "catalog_version"));
    CHECK(!ArrowSchemaSetFormat(schema.children[1], "+l"));
    CHECK(!ArrowSchemaSetName(schema.children[1], "schemas"));
    CHECK(!ArrowSchemaAllocateChildren(schema.children[1], 1));
    CHECK(!VgiSchemaInit("SchemaContents", schema.children[1]->children[0], &error));
    CHECK(!ArrowSchemaSetName(schema.children[1]->children[0], "item"));
    CHECK(!VgiBatchInit(out, &schema, &error));
    const struct ArrowSchema *type = schema.children[1]->children[0];
    struct ArrowArray *entry = out->array.children[1]->children[0];
    CHECK(!ArrowArrayAppendInt(out->array.children[0], 1));
    struct VgiBatch info = {0};
    struct ArrowBuffer info_bytes;
    ArrowBufferInit(&info_bytes);
    CHECK(!VgiBatchInitNamed(&info, "SchemaInfo", &error));
    struct ArrowArray *info_path = VgiBatchColumn(&info, "path");
    CHECK(!ArrowArrayAppendString(info_path->children[0], ArrowCharView("main")));
    CHECK(!ArrowArrayFinishElement(info_path));
    CHECK(!VgiBatchFinishRow(&info, &error));
    CHECK(!VgiIpcWriteBatch(&info.schema, &info.array, NULL, &info_bytes, &error));
    for (int64_t i = 0; i < type->n_children; ++i) {
        const char *name = type->children[i]->name;
        struct ArrowArray *child = entry->children[i];
        if (!strcmp(name, "path")) {
            CHECK(!ArrowArrayAppendString(child->children[0], ArrowCharView("main")));
        } else if (!strcmp(name, "schema")) {
            CHECK(!ArrowArrayAppendBytes(child, view(&info_bytes)));
            continue;
        } else if (!strcmp(name, "tables")) {
            CHECK(!ArrowArrayAppendBytes(child->children[0], view(table)));
        }
        CHECK(!ArrowArrayFinishElement(child));
    }
    CHECK(!ArrowArrayFinishElement(entry));
    CHECK(!ArrowArrayFinishElement(out->array.children[1]));
    CHECK(!ArrowArrayFinishElement(&out->array));
    CHECK(!VgiBatchFinish(out, &error));
    VgiBatchReset(&info);
    ArrowBufferReset(&info_bytes);
    ArrowSchemaRelease(&schema);
}
static int register_table(struct VgiWorker *worker, const struct ArrowBufferView *records,
                          size_t count) {
    struct VgiBatch info = {0}, attach = {0}, contents = {0};
    struct ArrowBuffer table;
    table_info(records, count, &table);
    CHECK(!VgiBatchInitNamed(&info, "CatalogInfo", &error));
    CHECK(!VgiBatchSetString(&info, "name", "cat"));
    CHECK(!VgiBatchFinishRow(&info, &error));
    CHECK(!VgiBatchInitNamed(&attach, "CatalogAttachResult", &error));
    CHECK(!VgiBatchFinishRow(&attach, &error));
    contents_init(&contents, &table);
    int rc = VgiWorkerRegisterCatalog(worker, &info, &attach, &contents, NULL, &error);
    VgiBatchReset(&info);
    VgiBatchReset(&attach);
    VgiBatchReset(&contents);
    ArrowBufferReset(&table);
    return rc;
}
static const struct VgiMethod *method(const char *name) {
    for (size_t i = 0; i < VGI_METHOD_COUNT; ++i)
        if (!strcmp(VGI_METHODS[i].name, name)) return &VGI_METHODS[i];
    CHECK(0);
    return NULL;
}
static void call_init(struct VgiRpcCall *call, const char *name) {
    memset(call, 0, sizeof(*call));
    call->method = method(name);
    struct ArrowSchema params = {0};
    CHECK(!call->method->params(&params, &error));
    CHECK(!VgiBatchInit(&call->request, &params, &error));
    ArrowSchemaRelease(&params);
}
static void dispatch(struct VgiWorker *worker, struct VgiRpcCall *call, struct VgiBatch *result) {
    struct VgiRpcReply reply;
    memset(&reply, 0, sizeof(reply));
    CHECK(!VgiBatchFinishRow(&call->request, &error));
    int handled = 0;
    int rc = VgiWorkerCatalogDefault(worker, call, &reply, &handled);
    if (rc) fprintf(stderr, "%s: %s\n", call->method->name, call->error.message);
    CHECK(!rc && handled);
    CHECK(!VgiBatchDecode(result, VgiBatchBytes(&reply.value, "result"), &error));
    VgiBatchReset(&call->request);
    VgiBatchReset(&reply.value);
    if (reply.metadata.data) ArrowBufferReset(&reply.metadata);
}
/* catalog_attach, offering the constraint read profiles when offer is set. */
static void attach(struct VgiWorker *worker, int offer, struct VgiBatch *result) {
    struct VgiBatch request = {0};
    CHECK(!VgiBatchInitNamed(&request, "CatalogAttachRequest", &error));
    CHECK(!VgiBatchSetString(&request, "name", "cat"));
    struct ArrowBuffer client, catalog, request_bytes;
    ArrowBufferInit(&client);
    ArrowBufferInit(&catalog);
    ArrowBufferInit(&request_bytes);
    if (offer) {
        struct VgiCatalogCapabilities caps = {0};
        CHECK(!VgiCatalogCapabilitiesAdd(&caps, VGI_CAPABILITY_READ, VGI_CONSTRAINT_RECORDS_READ,
                                         &error));
        CHECK(!VgiCatalogCapabilitiesAdd(&caps, VGI_CAPABILITY_READ, VGI_SCALAR_EXPRESSION_READ,
                                         &error));
        CHECK(!VgiCatalogCapabilitiesEncode(&caps, &catalog, &error));
        VgiCatalogCapabilitiesReset(&caps);
        struct VgiBatch capabilities = {0};
        CHECK(!VgiBatchInitNamed(&capabilities, "ClientCapabilities", &error));
        CHECK(!VgiBatchSetString(&capabilities, "engine", "duckdb"));
        CHECK(!VgiBatchSetBytes(&capabilities, "catalog", catalog.data, catalog.size_bytes));
        CHECK(!VgiBatchFinishRow(&capabilities, &error));
        CHECK(!VgiIpcWriteBatch(&capabilities.schema, &capabilities.array, NULL, &client, &error));
        VgiBatchReset(&capabilities);
        CHECK(!VgiBatchSetBytes(&request, "client_capabilities", client.data, client.size_bytes));
    }
    CHECK(!VgiBatchFinishRow(&request, &error));
    CHECK(!VgiIpcWriteBatch(&request.schema, &request.array, NULL, &request_bytes, &error));
    struct VgiRpcCall call;
    call_init(&call, "catalog_attach");
    CHECK(
        !VgiBatchSetBytes(&call.request, "request", request_bytes.data, request_bytes.size_bytes));
    dispatch(worker, &call, result);
    VgiBatchReset(&request);
    ArrowBufferReset(&client);
    ArrowBufferReset(&catalog);
    ArrowBufferReset(&request_bytes);
}
static void selected_profiles(const struct VgiBatch *result, const char *field,
                              struct VgiCatalogCapabilities *out) {
    memset(out, 0, sizeof(*out));
    if (VgiBatchIsNull(result, field)) return;
    CHECK(!VgiCatalogCapabilitiesDecode(VgiBatchBytes(result, field), out, &error));
}
static void check_catalog(void) {
    struct ArrowBuffer pk, nn, chk;
    struct VgiConstraintSpec s = spec("pk", VGI_CONSTRAINT_PRIMARY_KEY, &id_col, 1);
    encode(&s, &pk);
    s = spec("x_nn", VGI_CONSTRAINT_NOT_NULL, &x_col, 1);
    encode(&s, &nn);
    s = spec("chk", VGI_CONSTRAINT_CHECK, NULL, 0);
    s.check_sql = "v >= 0";
    encode(&s, &chk);
    struct ArrowBufferView records[] = {view(&pk), view(&nn), view(&chk)};

    struct VgiWorker *worker = VgiWorkerNew("cat");
    CHECK(worker);
    CHECK(!register_table(worker, records, 3));
    /* The catalog offers both read profiles; selection follows the client. */
    struct VgiBatch result = {0};
    struct VgiCatalogCapabilities caps;
    attach(worker, 0, &result);
    selected_profiles(&result, "catalog_capabilities", &caps);
    CHECK(VgiCatalogCapabilitiesHas(&caps, VGI_CAPABILITY_READ, VGI_CONSTRAINT_RECORDS_READ) &&
          VgiCatalogCapabilitiesHas(&caps, VGI_CAPABILITY_READ, VGI_SCALAR_EXPRESSION_READ));
    VgiCatalogCapabilitiesReset(&caps);
    selected_profiles(&result, "selected_capabilities", &caps);
    CHECK(!caps.counts[VGI_CAPABILITY_READ]);
    VgiCatalogCapabilitiesReset(&caps);
    VgiBatchReset(&result);
    attach(worker, 1, &result);
    selected_profiles(&result, "selected_capabilities", &caps);
    CHECK(caps.counts[VGI_CAPABILITY_READ] == 2);
    VgiCatalogCapabilitiesReset(&caps);
    VgiBatchReset(&result);

    /* The served table: only record-named fields are non-nullable. */
    struct VgiRpcCall call;
    call_init(&call, "catalog_schema_contents_tables");
    const char *cookie = "{\"catalog\":\"cat\"}";
    CHECK(!VgiBatchSetBytes(&call.request, "attach_opaque_data", cookie, (int64_t)strlen(cookie)));
    struct ArrowArray *path = VgiBatchColumn(&call.request, "path");
    CHECK(!ArrowArrayAppendString(path->children[0], ArrowCharView("main")));
    CHECK(!ArrowArrayFinishElement(path));
    dispatch(worker, &call, &result);
    const struct ArrowArrayView *items = VgiBatchGet(&result, "items");
    CHECK(ArrowArrayViewListChildOffset(items, 1) == 1);
    struct ArrowStringView bytes = VgiValueString(items->children[0], 0);
    struct VgiBatch table = {0};
    CHECK(
        !VgiBatchDecode(&table, (struct ArrowBufferView){{bytes.data}, bytes.size_bytes}, &error));
    struct ArrowSchema columns = {0};
    CHECK(!VgiIpcReadSchema(VgiBatchBytes(&table, "columns"), &columns, &error));
    CHECK(!field_nullable(&columns, 0) && field_nullable(&columns, 1) &&
          field_nullable(&columns, 2));
    CHECK(field_nullable(&columns, 3) &&
          !(columns.children[3]->children[0]->flags & ARROW_FLAG_NULLABLE));
    CHECK(!field_nullable(&columns, 4));
    ArrowSchemaRelease(&columns);
    VgiBatchReset(&table);
    VgiBatchReset(&result);
    VgiWorkerFree(worker);

    /* A catalog the extension would refuse fails at registration. */
    struct ArrowBuffer metadata_only, fk;
    s = spec("m", VGI_CONSTRAINT_UNIQUE, &v_col, 1);
    s.enforcement = VGI_ENFORCEMENT_METADATA_ONLY;
    encode(&s, &metadata_only);
    worker = VgiWorkerNew("cat");
    struct ArrowBufferView bad = view(&metadata_only);
    CHECK(register_table(worker, &bad, 1) == EINVAL);
    CHECK(strstr(error.message, "catalog 'cat'") && strstr(error.message, "'m'") &&
          strstr(error.message, "ENFORCED + ALL_ROWS"));
    VgiWorkerFree(worker);
    struct VgiConstraintReference ref = {main_schema, 1, "missing", &id_col, 1, "missing_pk"};
    s = spec("fk", VGI_CONSTRAINT_FOREIGN_KEY, &v_col, 1);
    s.references = &ref;
    encode(&s, &fk);
    worker = VgiWorkerNew("cat");
    bad = view(&fk);
    CHECK(register_table(worker, &bad, 1) == EINVAL);
    CHECK(strstr(error.message, "does not declare"));
    VgiWorkerFree(worker);
    ArrowBufferReset(&metadata_only);
    ArrowBufferReset(&fk);
    ArrowBufferReset(&pk);
    ArrowBufferReset(&nn);
    ArrowBufferReset(&chk);
}

int main(void) {
    check_builder();
    check_served_schema();
    check_validation();
    check_capabilities();
    check_catalog();
    puts("constraint records, served nullability, ATTACH checks and negotiation passed");
    return 0;
}
