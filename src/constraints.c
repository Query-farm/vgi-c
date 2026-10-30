/* Canonical constraint records: the public builder, its decoder, the served
 * schema, the ATTACH-equivalent checks, and catalog capability negotiation.
 * Rules mirror the DuckDB extension's vgi_constraint_contract.cpp. */
#include "vgi/constraints.h"

#include <ctype.h>
#include <errno.h>
#include <sodium.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "constraints_internal.h"
#include "vgi/batch.h"
#include "vgi_rpc/stream.h"

#define MAX_RECORD_BYTES (8 * 1024 * 1024)
#define MAX_RECORDS 1024
#define MAX_PATH_DEPTH 64
#define MAX_PROFILES 256

static int failf(struct ArrowError *error, const char *format, ...) {
    char message[sizeof(error->message)];
    va_list args;
    va_start(args, format);
    vsnprintf(message, sizeof(message), format, args);
    va_end(args);
    if (error) ArrowErrorSet(error, "%s", message);
    return EINVAL;
}
static char *dup_view(struct ArrowStringView value) {
    if (!value.data || value.size_bytes < 0) return NULL;
    char *out = malloc((size_t)value.size_bytes + 1);
    if (out) {
        if (value.size_bytes) memcpy(out, value.data, (size_t)value.size_bytes);
        out[value.size_bytes] = 0;
    }
    return out;
}
static int view_is(struct ArrowStringView value, const char *text) {
    return value.data && value.size_bytes == (int64_t)strlen(text) &&
           !memcmp(value.data, text, (size_t)value.size_bytes);
}
/* Nonempty, at most 4096 bytes, no NUL: the extension's identifier rule. */
static int identifier_ok(const char *value) {
    if (!value || !*value) return 0;
    size_t n = strlen(value);
    return n <= 4096;
}
static int ci_equal(const char *a, const char *b) {
    for (; *a && *b; ++a, ++b)
        if (tolower((unsigned char)*a) != tolower((unsigned char)*b)) return 0;
    return *a == *b;
}

static const char *const kind_names[] = {"NOT_NULL", "PRIMARY_KEY", "UNIQUE", "CHECK",
                                         "FOREIGN_KEY"};
static const char *const enforcement_names[] = {NULL, "ENFORCED", "METADATA_ONLY"};
static const char *const validation_names[] = {NULL, "ALL_ROWS", "FUTURE_WRITES", "UNVALIDATED"};
const char *VgiConstraintKindName(enum VgiConstraintKind kind) {
    return (unsigned)kind < 5 ? kind_names[kind] : NULL;
}
const char *VgiConstraintEnforcementName(enum VgiConstraintEnforcement enforcement) {
    return (unsigned)enforcement < 3 ? enforcement_names[enforcement] : NULL;
}
const char *VgiConstraintValidationName(enum VgiConstraintValidation validation) {
    return (unsigned)validation < 4 ? validation_names[validation] : NULL;
}
static int parse_name(struct ArrowStringView value, const char *const *names, int count) {
    for (int i = 0; i < count; ++i)
        if (names[i] && view_is(value, names[i])) return i;
    return -1;
}

/* ---- Owned paths --------------------------------------------------------- */

static void path_free(struct VgiOwnedPath *path) {
    for (size_t i = 0; path && i < path->count; ++i) free(path->components[i]);
    if (path) {
        free(path->components);
        path->components = NULL;
        path->count = 0;
    }
}
static void paths_free(struct VgiOwnedPath **paths, size_t *count) {
    for (size_t i = 0; *paths && i < *count; ++i) path_free(&(*paths)[i]);
    free(*paths);
    *paths = NULL;
    *count = 0;
}
static void path_join(const char *const *components, size_t count, char *out, size_t size) {
    size_t used = 0;
    out[0] = 0;
    for (size_t i = 0; i < count && used + 1 < size; ++i) {
        int n = snprintf(out + used, size - used, "%s%s", i ? "." : "", components[i]);
        if (n < 0) break;
        used += (size_t)n;
    }
}
static int path_equal(const struct VgiOwnedPath *a, const struct VgiOwnedPath *b) {
    if (a->count != b->count) return 0;
    for (size_t i = 0; i < a->count; ++i)
        if (strcmp(a->components[i], b->components[i])) return 0;
    return 1;
}
/* Read row's list<string>; a null list or element fails. */
static int read_path(const struct ArrowArrayView *list, int64_t row, struct VgiOwnedPath *out,
                     const char *what, struct ArrowError *error) {
    if (!list || ArrowArrayViewIsNull(list, row)) return failf(error, "null %s", what);
    int64_t start = ArrowArrayViewListChildOffset(list, row),
            end = ArrowArrayViewListChildOffset(list, row + 1);
    if (end - start > 4096) return failf(error, "oversized %s", what);
    out->components = calloc((size_t)(end - start) + 1, sizeof(char *));
    if (!out->components) return ENOMEM;
    for (int64_t i = start; i < end; ++i) {
        struct ArrowStringView part = VgiValueString(list->children[0], i);
        if (!part.data) return failf(error, "null element in %s", what);
        if (memchr(part.data, 0, (size_t)part.size_bytes))
            return failf(error, "NUL byte in %s", what);
        if (!(out->components[out->count] = dup_view(part))) return ENOMEM;
        ++out->count;
    }
    return 0;
}
/* Read row's list<list<string>>. */
static int read_paths(const struct ArrowArrayView *outer, int64_t row, struct VgiOwnedPath **out,
                      size_t *count, const char *what, struct ArrowError *error) {
    if (!outer || ArrowArrayViewIsNull(outer, row)) return failf(error, "null %s", what);
    int64_t start = ArrowArrayViewListChildOffset(outer, row),
            end = ArrowArrayViewListChildOffset(outer, row + 1);
    if (end - start > MAX_RECORDS) return failf(error, "too many %s", what);
    *out = calloc((size_t)(end - start) + 1, sizeof(**out));
    if (!*out) return ENOMEM;
    for (int64_t i = start; i < end; ++i) {
        int rc = read_path(outer->children[0], i, &(*out)[*count], what, error);
        ++*count;
        if (rc) return rc;
    }
    return 0;
}

/* ---- Records ------------------------------------------------------------- */

static int same_layout(const struct ArrowSchema *a, const struct ArrowSchema *b) {
    if (!a || !b || a->n_children != b->n_children || strcmp(a->format, b->format)) return 0;
    if (!a->dictionary != !b->dictionary) return 0;
    if (a->dictionary && !same_layout(a->dictionary, b->dictionary)) return 0;
    for (int64_t i = 0; i < a->n_children; ++i) {
        const char *x = a->children[i]->name, *y = b->children[i]->name;
        if (strcmp(x ? x : "", y ? y : "") || !same_layout(a->children[i], b->children[i]))
            return 0;
    }
    return 1;
}
/* Decode exactly one row of the generated record `name`. */
static int decode_record(struct ArrowBufferView bytes, const char *name, struct VgiBatch *out,
                         struct ArrowError *error) {
    if (!bytes.data.data || bytes.size_bytes <= 0 || bytes.size_bytes > MAX_RECORD_BYTES)
        return failf(error, "invalid %s record size", name);
    struct ArrowSchema expected = {0};
    int rc = VgiSchemaInit(name, &expected, error);
    if (!rc) {
        rc = VgiBatchDecode(out, bytes, error);
        if (rc) rc = failf(error, "invalid %s record", name);
    }
    if (!rc && (out->array.length != 1 || !same_layout(&out->schema, &expected)))
        rc = failf(error, "invalid %s record", name);
    if (expected.release) ArrowSchemaRelease(&expected);
    if (rc) VgiBatchReset(out);
    return rc;
}
static int encode_batch(struct VgiBatch *batch, struct ArrowBuffer *out, struct ArrowError *error) {
    int rc = VgiBatchFinishRow(batch, error);
    if (!rc) rc = VgiIpcWriteBatch(&batch->schema, &batch->array, NULL, out, error);
    return rc;
}
static int append_strings(struct ArrowArray *list, const char *const *items, size_t count) {
    for (size_t i = 0; i < count; ++i)
        NANOARROW_RETURN_NOT_OK(ArrowArrayAppendString(list->children[0], ArrowCharView(items[i])));
    return ArrowArrayFinishElement(list);
}
static int append_paths(struct ArrowArray *list, const struct VgiColumnPath *paths, size_t count) {
    for (size_t i = 0; i < count; ++i)
        NANOARROW_RETURN_NOT_OK(
            append_strings(list->children[0], paths[i].components, paths[i].count));
    return ArrowArrayFinishElement(list);
}
static int set_buffer(struct VgiBatch *batch, const char *name, const struct ArrowBuffer *value) {
    return VgiBatchSetBytes(batch, name, value->data, value->size_bytes);
}
static int object_ref(const char *kind, const char *const *schema_path, size_t schema_count,
                      const char *name, const char *owner, struct ArrowBuffer *out,
                      struct ArrowError *error) {
    struct VgiBatch batch = {0};
    int rc = VgiBatchInitNamed(&batch, "CatalogObjectRef", error);
    if (!rc) rc = VgiBatchSetString(&batch, "kind", kind);
    if (!rc) rc = append_strings(VgiBatchColumn(&batch, "schema_path"), schema_path, schema_count);
    if (!rc) rc = VgiBatchSetString(&batch, "name", name);
    if (!rc) rc = VgiBatchSetString(&batch, "owner_table", owner);
    if (!rc) rc = encode_batch(&batch, out, error);
    VgiBatchReset(&batch);
    return rc;
}
static int name_ok(const char *value, const char *what, struct ArrowError *error) {
    return identifier_ok(value) ? 0 : failf(error, "%s must be nonempty", what);
}
static int spec_paths_ok(const struct VgiColumnPath *paths, size_t count, const char *id,
                         struct ArrowError *error) {
    if (count && !paths) return failf(error, "constraint '%s': missing column paths", id);
    for (size_t i = 0; i < count; ++i) {
        if (!paths[i].count || paths[i].count > MAX_PATH_DEPTH || !paths[i].components)
            return failf(error, "constraint '%s': empty or oversized column path", id);
        for (size_t j = 0; j < paths[i].count; ++j)
            if (!identifier_ok(paths[i].components[j]))
                return failf(error, "constraint '%s': empty column path component", id);
        for (size_t k = 0; k < i; ++k) {
            int same = paths[k].count == paths[i].count;
            for (size_t j = 0; same && j < paths[i].count; ++j)
                same = !strcmp(paths[k].components[j], paths[i].components[j]);
            if (same) {
                char text[512];
                path_join(paths[i].components, paths[i].count, text, sizeof(text));
                return failf(error, "constraint '%s': duplicate column path %s", id, text);
            }
        }
    }
    return 0;
}
int VgiConstraintRecordEncode(const struct VgiConstraintSpec *spec, struct ArrowBuffer *out,
                              struct ArrowError *error) {
    if (!spec || !out || out->size_bytes) return EINVAL;
    const char *id = spec->id ? spec->id : "";
    int rc = 0;
    if (!spec->schema_path_count || !spec->schema_path)
        return failf(error, "constraint '%s': schema_path is empty", id);
    for (size_t i = 0; i < spec->schema_path_count; ++i)
        NANOARROW_RETURN_NOT_OK(name_ok(spec->schema_path[i], "schema path component", error));
    NANOARROW_RETURN_NOT_OK(name_ok(spec->table, "constraint table", error));
    NANOARROW_RETURN_NOT_OK(name_ok(spec->id, "constraint id", error));
    if (!VgiConstraintKindName(spec->kind))
        return failf(error, "constraint '%s': unknown kind", id);
    if (spec->enforcement == VGI_ENFORCEMENT_UNSTATED ||
        spec->validation == VGI_VALIDATION_UNSTATED ||
        !VgiConstraintEnforcementName(spec->enforcement) ||
        !VgiConstraintValidationName(spec->validation))
        return failf(error, "constraint '%s': enforcement and validation must be stated explicitly",
                     id);
    int check = spec->kind == VGI_CONSTRAINT_CHECK,
        foreign = spec->kind == VGI_CONSTRAINT_FOREIGN_KEY;
    if (check != (spec->check_sql != NULL))
        return failf(error, "constraint '%s': check_sql is required for CHECK and only CHECK", id);
    if (check && !*spec->check_sql) return failf(error, "constraint '%s': check_sql is empty", id);
    if (check ? spec->column_count != 0 : spec->column_count == 0)
        return failf(error,
                     check ? "constraint '%s': CHECK takes no column paths"
                           : "constraint '%s': %s requires column paths",
                     id, VgiConstraintKindName(spec->kind));
    if (spec->kind == VGI_CONSTRAINT_NOT_NULL && spec->column_count != 1)
        return failf(error, "constraint '%s': NOT_NULL requires exactly one column path", id);
    NANOARROW_RETURN_NOT_OK(spec_paths_ok(spec->columns, spec->column_count, id, error));
    if (foreign != (spec->references != NULL))
        return failf(error,
                     "constraint '%s': references is required for FOREIGN_KEY and only FOREIGN_KEY",
                     id);
    if (spec->declared_name)
        NANOARROW_RETURN_NOT_OK(name_ok(spec->declared_name, "declared_name", error));
    const struct VgiConstraintReference *ref = spec->references;
    if (foreign) {
        if (!ref->schema_path_count || !ref->schema_path)
            return failf(error, "constraint '%s': foreign key schema_path is empty", id);
        for (size_t i = 0; i < ref->schema_path_count; ++i)
            NANOARROW_RETURN_NOT_OK(name_ok(ref->schema_path[i], "foreign key schema path", error));
        NANOARROW_RETURN_NOT_OK(name_ok(ref->table, "foreign key referenced table", error));
        NANOARROW_RETURN_NOT_OK(
            name_ok(ref->constraint_id, "foreign key referenced constraint_id", error));
        NANOARROW_RETURN_NOT_OK(spec_paths_ok(ref->columns, ref->column_count, id, error));
        if (ref->column_count != spec->column_count)
            return failf(error,
                         "constraint '%s': foreign key local and referenced arity must match", id);
    }
    struct ArrowBuffer ref_bytes, check_bytes, fk_bytes, target_bytes, table_ref, definition_bytes;
    struct ArrowBuffer *buffers[] = {&ref_bytes,    &check_bytes, &fk_bytes,
                                     &target_bytes, &table_ref,   &definition_bytes};
    for (size_t i = 0; i < sizeof(buffers) / sizeof(*buffers); ++i) ArrowBufferInit(buffers[i]);
    struct VgiBatch batch = {0};
    if (check) {
        rc = VgiBatchInitNamed(&batch, "SqlExpression", error);
        if (!rc) rc = VgiBatchSetString(&batch, "sql", spec->check_sql);
        if (!rc) rc = VgiBatchSetString(&batch, "profile", VGI_SCALAR_EXPRESSION_READ);
        if (!rc)
            rc = append_strings(VgiBatchColumn(&batch, "schema_path"), spec->schema_path,
                                spec->schema_path_count);
        struct ArrowArray *context = rc ? NULL : VgiBatchColumn(&batch, "evaluation_context");
        const char *values[] = {"UTC", "binary", "asc", "nulls_last"};
        for (int i = 0; !rc && i < 4; ++i)
            rc = ArrowArrayAppendString(context->children[i], ArrowCharView(values[i]));
        if (!rc) rc = ArrowArrayAppendInt(context->children[4], 1);
        if (!rc) rc = ArrowArrayFinishElement(context);
        if (!rc) rc = encode_batch(&batch, &check_bytes, error);
        VgiBatchReset(&batch);
    }
    if (!rc && foreign) {
        rc = object_ref("table", ref->schema_path, ref->schema_path_count, ref->table, NULL,
                        &table_ref, error);
        if (!rc) rc = VgiBatchInitNamed(&batch, "ForeignKeyDefinition", error);
        if (!rc) rc = set_buffer(&batch, "referenced_table", &table_ref);
        if (!rc)
            rc = append_paths(VgiBatchColumn(&batch, "referenced_column_paths"), ref->columns,
                              ref->column_count);
        if (!rc) rc = VgiBatchSetString(&batch, "match", "SIMPLE");
        if (!rc) rc = VgiBatchSetString(&batch, "on_update", "NO_ACTION");
        if (!rc) rc = VgiBatchSetString(&batch, "on_delete", "NO_ACTION");
        if (!rc) rc = encode_batch(&batch, &fk_bytes, error);
        VgiBatchReset(&batch);
        if (!rc)
            rc = object_ref("constraint", ref->schema_path, ref->schema_path_count,
                            ref->constraint_id, ref->table, &target_bytes, error);
    }
    if (!rc) rc = VgiBatchInitNamed(&batch, "ConstraintDefinition", error);
    if (!rc) rc = VgiBatchSetString(&batch, "kind", VgiConstraintKindName(spec->kind));
    if (!rc)
        rc =
            append_paths(VgiBatchColumn(&batch, "column_paths"), spec->columns, spec->column_count);
    if (!rc) rc = VgiBatchSetString(&batch, "declared_name", spec->declared_name);
    if (!rc)
        rc = check ? set_buffer(&batch, "check_expression", &check_bytes)
                   : VgiBatchSetNull(&batch, "check_expression");
    if (!rc)
        rc = foreign ? set_buffer(&batch, "foreign_key", &fk_bytes)
                     : VgiBatchSetNull(&batch, "foreign_key");
    if (!rc) rc = encode_batch(&batch, &definition_bytes, error);
    VgiBatchReset(&batch);
    if (!rc)
        rc = object_ref("constraint", spec->schema_path, spec->schema_path_count, spec->id,
                        spec->table, &ref_bytes, error);
    if (!rc) rc = VgiBatchInitNamed(&batch, "ConstraintInfo", error);
    if (!rc) rc = set_buffer(&batch, "ref", &ref_bytes);
    if (!rc) rc = set_buffer(&batch, "definition", &definition_bytes);
    if (!rc)
        rc = VgiBatchSetString(&batch, "enforcement",
                               VgiConstraintEnforcementName(spec->enforcement));
    if (!rc)
        rc = VgiBatchSetString(&batch, "validation", VgiConstraintValidationName(spec->validation));
    if (!rc)
        rc = foreign ? set_buffer(&batch, "referenced_constraint", &target_bytes)
                     : VgiBatchSetNull(&batch, "referenced_constraint");
    if (!rc) rc = encode_batch(&batch, out, error);
    VgiBatchReset(&batch);
    for (size_t i = 0; i < sizeof(buffers) / sizeof(*buffers); ++i) ArrowBufferReset(buffers[i]);
    if (rc) ArrowBufferReset(out);
    return rc;
}

void VgiConstraintRecordReset(struct VgiConstraintRecord *r) {
    if (!r) return;
    path_free(&r->schema_path);
    free(r->table);
    free(r->id);
    paths_free(&r->columns, &r->column_count);
    free(r->declared_name);
    free(r->check_sql);
    path_free(&r->ref_schema_path);
    free(r->ref_table);
    paths_free(&r->ref_columns, &r->ref_column_count);
    free(r->ref_constraint_id);
    path_free(&r->ref_key_schema_path);
    free(r->ref_key_owner);
    free(r->match);
    free(r->on_update);
    free(r->on_delete);
    free(r->check_profile);
    path_free(&r->check_schema_path);
    memset(r, 0, sizeof(*r));
}
/* A CatalogObjectRef of `kind`: the name, schema path and owner table. */
static int read_ref(struct ArrowBufferView bytes, const char *kind, struct VgiOwnedPath *schema,
                    char **name, char **owner, struct ArrowError *error) {
    struct VgiBatch ref = {0};
    int rc = decode_record(bytes, "CatalogObjectRef", &ref, error);
    if (!rc && !view_is(VgiBatchString(&ref, "kind"), kind))
        rc = failf(error, "CatalogObjectRef is not a %s reference", kind);
    if (!rc) rc = read_path(VgiBatchGet(&ref, "schema_path"), 0, schema, "schema_path", error);
    struct ArrowStringView text = VgiBatchString(&ref, "name");
    if (!rc && !text.data) rc = failf(error, "CatalogObjectRef has no name");
    if (!rc && !(*name = dup_view(text))) rc = ENOMEM;
    text = VgiBatchString(&ref, "owner_table");
    if (!rc && !strcmp(kind, "table") && text.data)
        rc = failf(error, "foreign key referenced_table is not a table reference");
    if (!rc && owner && text.data && !(*owner = dup_view(text))) rc = ENOMEM;
    VgiBatchReset(&ref);
    return rc;
}
static int read_enum(const struct VgiBatch *batch, const char *field, char **out,
                     struct ArrowError *error) {
    struct ArrowStringView value = VgiBatchString(batch, field);
    if (!value.data) return failf(error, "null %s", field);
    return (*out = dup_view(value)) ? 0 : ENOMEM;
}
static int context_is_default(const struct VgiBatch *expression) {
    int64_t field = VgiFieldIndex(&expression->schema, "evaluation_context");
    if (field < 0) return 0;
    const struct ArrowArrayView *context = expression->view.children[field];
    const struct ArrowSchema *type = expression->schema.children[field];
    if (ArrowArrayViewIsNull(context, 0)) return 0;
    const char *names[] = {"timezone", "collation", "default_order", "default_null_order"};
    const char *wanted[] = {"UTC", "binary", "asc", "nulls_last"};
    for (int i = 0; i < 4; ++i) {
        int64_t child = VgiFieldIndex(type, names[i]);
        if (child < 0 || !view_is(VgiValueString(context->children[child], 0), wanted[i])) return 0;
    }
    int64_t order = VgiFieldIndex(type, "preserve_insertion_order");
    return order >= 0 && !ArrowArrayViewIsNull(context->children[order], 0) &&
           ArrowArrayViewGetIntUnsafe(context->children[order], 0) != 0;
}
int VgiConstraintRecordDecode(struct ArrowBufferView bytes, struct VgiConstraintRecord *out,
                              struct ArrowError *error) {
    if (!out) return EINVAL;
    memset(out, 0, sizeof(*out));
    struct VgiBatch info = {0}, definition = {0}, nested = {0};
    int rc = decode_record(bytes, "ConstraintInfo", &info, error);
    if (!rc)
        rc = read_ref(VgiBatchBytes(&info, "ref"), "constraint", &out->schema_path, &out->id,
                      &out->table, error);
    if (!rc && !out->table) rc = failf(error, "constraint '%s' has no owner table", out->id);
    if (!rc)
        rc = decode_record(VgiBatchBytes(&info, "definition"), "ConstraintDefinition", &definition,
                           error);
    int kind = rc ? -1 : parse_name(VgiBatchString(&definition, "kind"), kind_names, 5);
    if (!rc && kind < 0) rc = failf(error, "constraint '%s': unknown constraint kind", out->id);
    if (!rc) out->kind = (enum VgiConstraintKind)kind;
    int enforcement =
        rc ? -1 : parse_name(VgiBatchString(&info, "enforcement"), enforcement_names, 3);
    int validation = rc ? -1 : parse_name(VgiBatchString(&info, "validation"), validation_names, 4);
    if (!rc && (enforcement < 1 || validation < 1))
        rc = failf(error, "constraint '%s': unknown enforcement or validation", out->id);
    if (!rc) {
        out->enforcement = (enum VgiConstraintEnforcement)enforcement;
        out->validation = (enum VgiConstraintValidation)validation;
        rc = read_paths(VgiBatchGet(&definition, "column_paths"), 0, &out->columns,
                        &out->column_count, "column_paths", error);
    }
    struct ArrowStringView label =
        rc ? (struct ArrowStringView){0} : VgiBatchString(&definition, "declared_name");
    if (!rc && label.data && !(out->declared_name = dup_view(label))) rc = ENOMEM;
    int has_check = !rc && !VgiBatchIsNull(&definition, "check_expression");
    int has_fk = !rc && !VgiBatchIsNull(&definition, "foreign_key");
    int has_target = !rc && !VgiBatchIsNull(&info, "referenced_constraint");
    if (!rc && has_check != (out->kind == VGI_CONSTRAINT_CHECK))
        rc = failf(error, "constraint '%s': inconsistent CHECK payload", out->id);
    if (!rc && (has_fk != (out->kind == VGI_CONSTRAINT_FOREIGN_KEY) || has_target != has_fk))
        rc = failf(error, "constraint '%s': inconsistent FOREIGN_KEY payload", out->id);
    if (!rc && has_check) {
        rc = decode_record(VgiBatchBytes(&definition, "check_expression"), "SqlExpression", &nested,
                           error);
        if (!rc) rc = read_enum(&nested, "sql", &out->check_sql, error);
        if (!rc) rc = read_enum(&nested, "profile", &out->check_profile, error);
        if (!rc)
            rc = read_path(VgiBatchGet(&nested, "schema_path"), 0, &out->check_schema_path,
                           "schema_path", error);
        if (!rc) out->check_default_context = context_is_default(&nested);
        VgiBatchReset(&nested);
    }
    if (!rc && has_fk) {
        out->has_references = 1;
        rc = decode_record(VgiBatchBytes(&definition, "foreign_key"), "ForeignKeyDefinition",
                           &nested, error);
        if (!rc)
            rc = read_ref(VgiBatchBytes(&nested, "referenced_table"), "table",
                          &out->ref_schema_path, &out->ref_table, NULL, error);
        if (!rc)
            rc = read_paths(VgiBatchGet(&nested, "referenced_column_paths"), 0, &out->ref_columns,
                            &out->ref_column_count, "referenced_column_paths", error);
        if (!rc) rc = read_enum(&nested, "match", &out->match, error);
        if (!rc) rc = read_enum(&nested, "on_update", &out->on_update, error);
        if (!rc) rc = read_enum(&nested, "on_delete", &out->on_delete, error);
        VgiBatchReset(&nested);
        if (!rc)
            rc = read_ref(VgiBatchBytes(&info, "referenced_constraint"), "constraint",
                          &out->ref_key_schema_path, &out->ref_constraint_id, &out->ref_key_owner,
                          error);
    }
    VgiBatchReset(&info);
    VgiBatchReset(&definition);
    if (rc) VgiConstraintRecordReset(out);
    return rc;
}

/* ---- Served schema ------------------------------------------------------- */

static int metadata_key(const char *metadata, const char *key, int prefix) {
    struct ArrowMetadataReader reader;
    struct ArrowStringView k, v;
    if (!metadata || ArrowMetadataReaderInit(&reader, metadata)) return 0;
    size_t n = strlen(key);
    while (reader.remaining_keys > 0 && !ArrowMetadataReaderRead(&reader, &k, &v))
        if (prefix ? k.size_bytes >= (int64_t)n && !memcmp(k.data, key, n) : view_is(k, key))
            return 1;
    return 0;
}
static int is_row_id(const struct ArrowSchema *field) {
    return metadata_key(field->metadata, "is_row_id", 0);
}
/* A STRUCT the extension descends into: not an extension or VGI logical type. */
static int ordinary_struct(const struct ArrowSchema *field) {
    return !strcmp(field->format, "+s") &&
           !metadata_key(field->metadata, "ARROW:extension:name", 0) &&
           !metadata_key(field->metadata, "vgi.logical_type.", 1);
}
struct Required {
    const struct VgiOwnedPath **paths;
    size_t count;
};
static int required_has(const struct Required *required, const char *const *path, size_t depth) {
    for (size_t i = 0; i < required->count; ++i) {
        const struct VgiOwnedPath *p = required->paths[i];
        if (p->count != depth) continue;
        size_t j = 0;
        while (j < depth && !strcmp(p->components[j], path[j])) ++j;
        if (j == depth) return 1;
    }
    return 0;
}
static int serve_field(struct ArrowSchema *field, const char **path, size_t depth,
                       const struct Required *required) {
    if (depth > MAX_PATH_DEPTH) return EINVAL;
    path[depth - 1] = field->name ? field->name : "";
    if (required_has(required, path, depth))
        field->flags &= ~ARROW_FLAG_NULLABLE;
    else
        field->flags |= ARROW_FLAG_NULLABLE;
    if (ordinary_struct(field))
        for (int64_t i = 0; i < field->n_children; ++i)
            NANOARROW_RETURN_NOT_OK(serve_field(field->children[i], path, depth + 1, required));
    return 0;
}
static int decode_all(const struct ArrowBufferView *records, size_t count,
                      struct VgiConstraintRecord **out, struct ArrowError *error) {
    if (count > MAX_RECORDS) return failf(error, "more than %d constraint records", MAX_RECORDS);
    *out = calloc(count + 1, sizeof(**out));
    if (!*out) return ENOMEM;
    int64_t total = 0;
    for (size_t i = 0; i < count; ++i) {
        total += records[i].size_bytes;
        if (records[i].size_bytes <= 0 || total > MAX_RECORD_BYTES)
            return failf(error, "constraint records must be nonempty and total at most 8 MiB");
        NANOARROW_RETURN_NOT_OK(VgiConstraintRecordDecode(records[i], &(*out)[i], error));
    }
    return 0;
}
static void free_all(struct VgiConstraintRecord *records, size_t count) {
    for (size_t i = 0; records && i < count; ++i) VgiConstraintRecordReset(&records[i]);
    free(records);
}
static int required_init(const struct VgiConstraintRecord *records, size_t count,
                         struct Required *out) {
    size_t n = 0;
    for (size_t i = 0; i < count; ++i)
        if (records[i].kind == VGI_CONSTRAINT_NOT_NULL ||
            records[i].kind == VGI_CONSTRAINT_PRIMARY_KEY)
            n += records[i].column_count;
    out->count = 0;
    out->paths = calloc(n + 1, sizeof(*out->paths));
    if (!out->paths) return ENOMEM;
    for (size_t i = 0; i < count; ++i)
        if (records[i].kind == VGI_CONSTRAINT_NOT_NULL ||
            records[i].kind == VGI_CONSTRAINT_PRIMARY_KEY)
            for (size_t j = 0; j < records[i].column_count; ++j)
                out->paths[out->count++] = &records[i].columns[j];
    return 0;
}
static int serve_schema(const struct ArrowSchema *columns,
                        const struct VgiConstraintRecord *records, size_t count,
                        struct ArrowSchema *out, struct ArrowError *error) {
    struct Required required = {0};
    int rc = required_init(records, count, &required);
    if (!rc) rc = ArrowSchemaDeepCopy(columns, out);
    const char *path[MAX_PATH_DEPTH + 1];
    for (int64_t i = 0; !rc && i < out->n_children; ++i)
        if (!is_row_id(out->children[i])) rc = serve_field(out->children[i], path, 1, &required);
    if (rc == EINVAL) failf(error, "column nesting exceeds %d levels", MAX_PATH_DEPTH);
    if (rc && out->release) ArrowSchemaRelease(out);
    free(required.paths);
    return rc;
}
int VgiConstraintServedSchema(const struct ArrowSchema *columns,
                              const struct ArrowBufferView *records, size_t count,
                              struct ArrowSchema *out, struct ArrowError *error) {
    if (!columns || !out || out->release || (count && !records)) return EINVAL;
    struct VgiConstraintRecord *decoded = NULL;
    int rc = decode_all(records, count, &decoded, error);
    if (!rc) rc = serve_schema(columns, decoded, count, out, error);
    free_all(decoded, count);
    return rc;
}

/* ---- CHECK grammar --------------------------------------------------------
 * The extension parses "SELECT <sql>" with DuckDB and accepts exactly one
 * comparison (=, <>, <, >, <=, >=) of two operands, each a numeric literal or
 * an unqualified column of type TINYINT..BIGINT, FLOAT or DOUBLE. This is a
 * recognizer of that subset; it never accepts what the extension refuses. */

enum TokKind { TOK_IDENT, TOK_QUOTED, TOK_STRING, TOK_NUMBER, TOK_OP, TOK_END };
struct Tok {
    enum TokKind kind;
    char *text;
};
struct Tokens {
    struct Tok *items;
    size_t count;
};
static void tokens_free(struct Tokens *t) {
    for (size_t i = 0; i < t->count; ++i) free(t->items[i].text);
    free(t->items);
}
static int tok_push(struct Tokens *t, enum TokKind kind, const char *text, size_t n) {
    struct Tok *next = realloc(t->items, (t->count + 1) * sizeof(*next));
    if (!next) return ENOMEM;
    t->items = next;
    char *copy = malloc(n + 1);
    if (!copy) return ENOMEM;
    memcpy(copy, text, n);
    copy[n] = 0;
    t->items[t->count++] = (struct Tok){kind, copy};
    return 0;
}
static int tokenize(const char *sql, struct Tokens *out, struct ArrowError *error) {
    size_t i = 0, n = strlen(sql);
    static const char *const ops[] = {"::", "||", "<=", ">=", "<>", "!=", "==", "=", "<",
                                      ">",  "+",  "-",  "*",  "/",  "%",  "(",  ")", ","};
    while (i < n) {
        char c = sql[i];
        if (isspace((unsigned char)c)) {
            ++i;
        } else if (c == '-' && sql[i + 1] == '-') {
            while (i < n && sql[i] != '\n') ++i;
        } else if (c == '/' && sql[i + 1] == '*') {
            const char *end = strstr(sql + i + 2, "*/");
            if (!end) return failf(error, "unterminated comment");
            i = (size_t)(end - sql) + 2;
        } else if (isalpha((unsigned char)c) || c == '_') {
            size_t j = i;
            while (j < n && (isalnum((unsigned char)sql[j]) || sql[j] == '_')) ++j;
            if (sql[j] == '\'' || sql[j] == '$')
                return failf(error, "prefixed string literals and parameters are unsupported");
            NANOARROW_RETURN_NOT_OK(tok_push(out, TOK_IDENT, sql + i, j - i));
            i = j;
        } else if (c == '"' || c == '\'') {
            char *text = malloc(n - i + 1);
            if (!text) return ENOMEM;
            size_t used = 0, j = i + 1;
            for (;; ++j) {
                if (j >= n) {
                    free(text);
                    return failf(error, "unterminated quoted text");
                }
                if (sql[j] == c) {
                    if (sql[j + 1] == c) {
                        text[used++] = c;
                        ++j;
                        continue;
                    }
                    break;
                }
                text[used++] = sql[j];
            }
            int rc = c == '"' && !used
                         ? failf(error, "empty quoted identifier")
                         : tok_push(out, c == '"' ? TOK_QUOTED : TOK_STRING, text, used);
            free(text);
            if (rc) return rc;
            i = j + 1;
        } else if (isdigit((unsigned char)c) || (c == '.' && isdigit((unsigned char)sql[i + 1]))) {
            size_t j = i;
            while (isdigit((unsigned char)sql[j])) ++j;
            if (sql[j] == '.') {
                ++j;
                while (isdigit((unsigned char)sql[j])) ++j;
            }
            if (sql[j] == 'e' || sql[j] == 'E') {
                size_t k = j + 1;
                if (sql[k] == '+' || sql[k] == '-') ++k;
                if (!isdigit((unsigned char)sql[k]))
                    return failf(error, "malformed numeric literal");
                j = k;
                while (isdigit((unsigned char)sql[j])) ++j;
            }
            if (isalpha((unsigned char)sql[j]) || sql[j] == '_')
                return failf(error, "malformed numeric literal");
            NANOARROW_RETURN_NOT_OK(tok_push(out, TOK_NUMBER, sql + i, j - i));
            i = j;
        } else {
            size_t k = 0;
            for (; k < sizeof(ops) / sizeof(*ops); ++k)
                if (!strncmp(sql + i, ops[k], strlen(ops[k]))) break;
            if (k == sizeof(ops) / sizeof(*ops))
                return failf(error, "unsupported character '%c'", c);
            NANOARROW_RETURN_NOT_OK(tok_push(out, TOK_OP, ops[k], strlen(ops[k])));
            i += strlen(ops[k]);
        }
    }
    return tok_push(out, TOK_END, "", 0);
}
struct Operand {
    int column;  /* else a constant */
    int numeric; /* a numeric constant */
    const char *name;
};
static int tok_op(const struct Tokens *t, size_t i, const char *op) {
    return i < t->count && t->items[i].kind == TOK_OP && !strcmp(t->items[i].text, op);
}
static int reserved(const char *word) {
    static const char *const words[] = {
        "and",    "or",       "not",   "is",     "isnull",   "notnull", "as",   "case",
        "when",   "then",     "else",  "end",    "in",       "between", "like", "ilike",
        "select", "from",     "where", "exists", "distinct", "all",     "any",  "some",
        "cast",   "try_cast", "true",  "false",  "null",     "collate"};
    for (size_t i = 0; i < sizeof(words) / sizeof(*words); ++i)
        if (ci_equal(word, words[i])) return 1;
    return 0;
}
static int operand_at(const struct Tokens *t, size_t i, size_t *end, struct Operand *o, int depth) {
    if (depth > 256 || i >= t->count) return 0;
    const struct Tok *tok = &t->items[i];
    if (tok_op(t, i, "(")) {
        size_t e = 0;
        if (operand_at(t, i + 1, &e, o, depth + 1) && tok_op(t, e, ")")) {
            *end = e + 1;
            return 1;
        }
        return 0;
    }
    if (tok_op(t, i, "-")) {
        /* DuckDB folds a negated numeric literal into the constant. */
        size_t e = 0;
        if (operand_at(t, i + 1, &e, o, depth + 1) && !o->column && o->numeric) {
            *end = e;
            return 1;
        }
        return 0;
    }
    *o = (struct Operand){0};
    switch (tok->kind) {
        case TOK_NUMBER:
            o->numeric = 1;
            break;
        case TOK_STRING:
            break;
        case TOK_IDENT:
            if (ci_equal(tok->text, "true") || ci_equal(tok->text, "false") ||
                ci_equal(tok->text, "null"))
                break;
            if (reserved(tok->text) || tok_op(t, i + 1, "(")) return 0;
            o->column = 1;
            o->name = tok->text;
            break;
        case TOK_QUOTED:
            if (tok_op(t, i + 1, "(")) return 0;
            o->column = 1;
            o->name = tok->text;
            break;
        default:
            return 0;
    }
    *end = i + 1;
    return 1;
}
static int comparison_op(const struct Tokens *t, size_t i) {
    static const char *const ops[] = {"=", "==", "<>", "!=", "<", ">", "<=", ">="};
    for (size_t k = 0; k < sizeof(ops) / sizeof(*ops); ++k)
        if (tok_op(t, i, ops[k])) return 1;
    return 0;
}
static int check_at(const struct Tokens *t, size_t i, size_t *end, struct Operand operands[2],
                    int depth) {
    if (depth > 256) return 0;
    size_t e = 0;
    if (tok_op(t, i, "(") && check_at(t, i + 1, &e, operands, depth + 1) && tok_op(t, e, ")")) {
        *end = e + 1;
        return 1;
    }
    size_t middle = 0;
    if (operand_at(t, i, &middle, &operands[0], 0) && comparison_op(t, middle) &&
        operand_at(t, middle + 1, &e, &operands[1], 0)) {
        *end = e;
        return 1;
    }
    return 0;
}
static int numeric_column(const char *format) {
    static const char *const formats[] = {"c", "s", "i", "l", "f", "g"};
    for (size_t i = 0; i < sizeof(formats) / sizeof(*formats); ++i)
        if (!strcmp(format, formats[i])) return 1;
    return 0;
}
static int validate_check(const char *sql, const struct ArrowSchema *columns,
                          struct ArrowError *error) {
    if (!*sql || strlen(sql) > 1024 * 1024) return failf(error, "invalid CHECK SQL size or text");
    struct Tokens tokens = {0};
    int rc = tokenize(sql, &tokens, error);
    struct Operand operands[2];
    size_t end = 0;
    if (!rc && (!check_at(&tokens, 0, &end, operands, 0) || tokens.items[end].kind != TOK_END))
        rc = failf(error,
                   "CHECK supports only a Boolean comparison of numeric columns and constants");
    for (int k = 0; !rc && k < 2; ++k) {
        if (!operands[k].column) {
            if (!operands[k].numeric) rc = failf(error, "CHECK requires numeric operands");
            continue;
        }
        int64_t found = -1;
        for (int64_t i = 0; !rc && i < columns->n_children; ++i) {
            if (!ci_equal(columns->children[i]->name ? columns->children[i]->name : "",
                          operands[k].name))
                continue;
            if (found >= 0)
                rc = failf(error, "CHECK column reference '%s' is ambiguous", operands[k].name);
            found = i;
        }
        if (!rc && (found < 0 || is_row_id(columns->children[found])))
            rc = failf(error, "CHECK references a missing or synthetic column '%s'",
                       operands[k].name);
        const struct ArrowSchema *field = rc ? NULL : columns->children[found];
        if (!rc && metadata_key(field->metadata, "ARROW:extension:name", 0))
            rc = failf(error, "CHECK extension-typed operands are unsupported");
        if (!rc && (field->dictionary || !numeric_column(field->format)))
            rc = failf(error, "CHECK requires primitive signed numeric columns; '%s' is not one",
                       field->name);
    }
    tokens_free(&tokens);
    return rc;
}

/* ---- ATTACH checks ------------------------------------------------------- */

static const struct ArrowSchema *top_level(const struct ArrowSchema *columns, const char *name) {
    const struct ArrowSchema *found = NULL;
    for (int64_t i = 0; i < columns->n_children; ++i) {
        if (strcmp(columns->children[i]->name ? columns->children[i]->name : "", name)) continue;
        if (found) return NULL;
        found = columns->children[i];
    }
    return found && !is_row_id(found) ? found : NULL;
}
static const struct ArrowSchema *nested(const struct ArrowSchema *parent, const char *name) {
    const struct ArrowSchema *found = NULL;
    for (int64_t i = 0; i < parent->n_children; ++i) {
        if (strcmp(parent->children[i]->name ? parent->children[i]->name : "", name)) continue;
        if (found) return NULL;
        found = parent->children[i];
    }
    return found;
}
static int same_path(const struct VgiOwnedPath *a, const char *const *b, size_t count) {
    if (a->count != count) return 0;
    for (size_t i = 0; i < count; ++i)
        if (strcmp(a->components[i], b[i])) return 0;
    return 1;
}
static int validate_records(const char *const *owner_schema, size_t owner_count, const char *owner,
                            const char *display, const struct ArrowSchema *raw,
                            const struct VgiConstraintRecord *records, size_t count,
                            struct ArrowError *error) {
    struct ArrowSchema columns = {0};
    int rc = serve_schema(raw, records, count, &columns, error);
    char message[sizeof(error->message)], text[512];
    int primary = 0;
#define BAD(...)                                                                      \
    do {                                                                              \
        snprintf(message, sizeof(message), __VA_ARGS__);                              \
        rc = failf(error, "Table '%s': constraint '%s' %s", display, r->id, message); \
        goto done;                                                                    \
    } while (0)
    for (size_t i = 0; !rc && i < count; ++i) {
        const struct VgiConstraintRecord *r = &records[i];
        if (!identifier_ok(r->id)) BAD("has an invalid id");
        if (r->schema_path.count != 1)
            BAD("must name a one-component schema path; DuckDB reads no other");
        if (!identifier_ok(r->schema_path.components[0])) BAD("has an invalid schema name");
        if (!identifier_ok(r->table)) BAD("has an invalid owner table");
        if (owner_schema &&
            (!same_path(&r->schema_path, owner_schema, owner_count) || strcmp(r->table, owner))) {
            path_join((const char *const *)r->schema_path.components, r->schema_path.count, text,
                      sizeof(text));
            BAD("belongs to %s.%s, not this table", text, r->table);
        }
        for (size_t j = 0; j < i; ++j) {
            if (!strcmp(records[j].id, r->id)) BAD("duplicates another constraint id");
            if (r->declared_name && records[j].declared_name &&
                ci_equal(r->declared_name, records[j].declared_name))
                BAD("reuses declared name '%s' (names are compared case-insensitively)",
                    r->declared_name);
        }
        if (r->declared_name && !identifier_ok(r->declared_name))
            BAD("has an invalid declared name");
        if (r->enforcement != VGI_ENFORCEMENT_ENFORCED || r->validation != VGI_VALIDATION_ALL_ROWS)
            BAD("is %s/%s; DuckDB accepts only ENFORCED + ALL_ROWS records",
                VgiConstraintEnforcementName(r->enforcement),
                VgiConstraintValidationName(r->validation));
        for (size_t j = 0; j < r->column_count; ++j) {
            if (!r->columns[j].count || r->columns[j].count > MAX_PATH_DEPTH)
                BAD("has an empty or oversized column path");
            for (size_t k = 0; k < r->columns[j].count; ++k)
                if (!identifier_ok(r->columns[j].components[k]))
                    BAD("has an invalid column path component");
            for (size_t k = 0; k < j; ++k)
                if (path_equal(&r->columns[k], &r->columns[j])) BAD("repeats a column path");
        }
        if (r->kind != VGI_CONSTRAINT_CHECK && !r->column_count) BAD("names no column paths");
        switch (r->kind) {
            case VGI_CONSTRAINT_NOT_NULL: {
                if (r->column_count != 1) BAD("NOT_NULL requires exactly one column path");
                const struct VgiOwnedPath *p = &r->columns[0];
                const struct ArrowSchema *field = top_level(&columns, p->components[0]);
                for (size_t depth = 1; field && depth < p->count; ++depth)
                    field = ordinary_struct(field) ? nested(field, p->components[depth]) : NULL;
                if (!field) {
                    path_join((const char *const *)p->components, p->count, text, sizeof(text));
                    BAD("NOT_NULL column %s is not a column (or an ordinary STRUCT member) of the "
                        "table",
                        text);
                }
                break;
            }
            case VGI_CONSTRAINT_PRIMARY_KEY:
            case VGI_CONSTRAINT_UNIQUE:
            case VGI_CONSTRAINT_FOREIGN_KEY: {
                const char *kind = VgiConstraintKindName(r->kind);
                for (size_t j = 0; j < r->column_count; ++j) {
                    const struct VgiOwnedPath *p = &r->columns[j];
                    path_join((const char *const *)p->components, p->count, text, sizeof(text));
                    if (p->count != 1)
                        BAD("%s column %s is nested; DuckDB reads nested paths only for NOT_NULL",
                            kind, text);
                    if (!top_level(&columns, p->components[0]))
                        BAD("%s column %s is not a column of the table", kind, text);
                }
                if (r->kind == VGI_CONSTRAINT_PRIMARY_KEY) {
                    if (primary) BAD("is a second PRIMARY_KEY; a table has at most one");
                    primary = 1;
                }
                if (r->kind != VGI_CONSTRAINT_FOREIGN_KEY) break;
                if (!r->ref_constraint_id || !path_equal(&r->ref_schema_path, &r->schema_path) ||
                    !path_equal(&r->ref_key_schema_path, &r->ref_schema_path) ||
                    !r->ref_key_owner || strcmp(r->ref_key_owner, r->ref_table))
                    BAD("must reference an identified key of a table in the same schema");
                if (!identifier_ok(r->ref_table)) BAD("has an invalid referenced table");
                if (!identifier_ok(r->ref_constraint_id))
                    BAD("has an invalid referenced constraint id");
                if (r->ref_column_count != r->column_count)
                    BAD("local and referenced column counts differ");
                for (size_t j = 0; j < r->ref_column_count; ++j)
                    if (r->ref_columns[j].count != 1)
                        BAD("referenced column paths must be top-level");
                if (strcmp(r->match, "SIMPLE")) BAD("must be MATCH SIMPLE");
                const char *actions[] = {r->on_update, r->on_delete};
                for (int k = 0; k < 2; ++k)
                    if (strcmp(actions[k], "NO_ACTION") && strcmp(actions[k], "RESTRICT"))
                        BAD("uses referential action %s; DuckDB reads only NO_ACTION and RESTRICT",
                            actions[k]);
                break;
            }
            case VGI_CONSTRAINT_CHECK: {
                if (strcmp(r->check_profile, VGI_SCALAR_EXPRESSION_READ))
                    BAD("CHECK expression profile must be " VGI_SCALAR_EXPRESSION_READ);
                if (!path_equal(&r->check_schema_path, &r->schema_path))
                    BAD("CHECK expression belongs to another schema");
                if (!r->check_default_context)
                    BAD("CHECK requires the default evaluation context (UTC, binary, asc, "
                        "nulls_last, preserve_insertion_order)");
                struct ArrowError detail;
                ArrowErrorInit(&detail);
                if (validate_check(r->check_sql, &columns, &detail))
                    BAD("CHECK (%s): %s", r->check_sql, detail.message);
                break;
            }
        }
    }
#undef BAD
done:
    if (columns.release) ArrowSchemaRelease(&columns);
    return rc;
}
int VgiConstraintValidate(const char *const *schema_path, size_t schema_path_count,
                          const char *table, const struct ArrowSchema *columns,
                          const struct ArrowBufferView *records, size_t count,
                          struct ArrowError *error) {
    if (!table || !columns || (count && !records)) return EINVAL;
    char display[512];
    if (schema_path) {
        path_join(schema_path, schema_path_count, display, sizeof(display));
        size_t used = strlen(display);
        snprintf(display + used, sizeof(display) - used, "%s%s", used ? "." : "", table);
    } else {
        snprintf(display, sizeof(display), "%s", table);
    }
    struct VgiConstraintRecord *decoded = NULL;
    int rc = decode_all(records, count, &decoded, error);
    if (rc && rc != ENOMEM) {
        char detail[sizeof(error->message)];
        snprintf(detail, sizeof(detail), "%s", error ? error->message : "");
        rc = failf(error, "Table '%s': %s", display, detail);
    }
    if (!rc)
        rc = validate_records(schema_path, schema_path_count, table, display, columns, decoded,
                              count, error);
    free_all(decoded, count);
    return rc;
}

/* ---- Capabilities -------------------------------------------------------- */

static const char *const category_names[VGI_CAPABILITY_CATEGORIES] = {"read", "ddl", "write",
                                                                      "transaction"};
/* ^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*\.v[1-9][0-9]*$, at most 256 bytes. */
static int profile_ok(const char *p) {
    size_t n = p ? strlen(p) : 0;
    if (!n || n > 256) return 0;
    const char *last = strrchr(p, '.');
    if (!last || last[1] != 'v' || last[2] < '1' || last[2] > '9') return 0;
    for (const char *c = last + 3; *c; ++c)
        if (!isdigit((unsigned char)*c)) return 0;
    for (const char *segment = p; segment < last;) {
        if (*segment < 'a' || *segment > 'z') return 0;
        const char *c = segment + 1;
        while (c < last && *c != '.') {
            if (!(islower((unsigned char)*c) || isdigit((unsigned char)*c) || *c == '_')) return 0;
            ++c;
        }
        segment = c + 1;
    }
    return 1;
}
int VgiCatalogCapabilitiesHas(const struct VgiCatalogCapabilities *c,
                              enum VgiCapabilityCategory category, const char *profile) {
    if (!c || (unsigned)category >= VGI_CAPABILITY_CATEGORIES || !profile) return 0;
    for (size_t i = 0; i < c->counts[category]; ++i)
        if (!strcmp(c->profiles[category][i], profile)) return 1;
    return 0;
}
int VgiCatalogCapabilitiesAdd(struct VgiCatalogCapabilities *c, enum VgiCapabilityCategory category,
                              const char *profile, struct ArrowError *error) {
    if (!c || (unsigned)category >= VGI_CAPABILITY_CATEGORIES) return EINVAL;
    if (!profile_ok(profile) || VgiCatalogCapabilitiesHas(c, category, profile) ||
        c->counts[category] >= MAX_PROFILES)
        return failf(error, "invalid or duplicate catalog capability profile: %s",
                     profile ? profile : "(null)");
    char **next = realloc(c->profiles[category], (c->counts[category] + 1) * sizeof(*next));
    if (!next) return ENOMEM;
    c->profiles[category] = next;
    if (!(next[c->counts[category]] = dup_view(ArrowCharView(profile)))) return ENOMEM;
    ++c->counts[category];
    return 0;
}
void VgiCatalogCapabilitiesReset(struct VgiCatalogCapabilities *c) {
    for (int k = 0; c && k < VGI_CAPABILITY_CATEGORIES; ++k) {
        for (size_t i = 0; i < c->counts[k]; ++i) free(c->profiles[k][i]);
        free(c->profiles[k]);
        c->profiles[k] = NULL;
        c->counts[k] = 0;
    }
}
static int capabilities_empty(const struct VgiCatalogCapabilities *c) {
    for (int k = 0; k < VGI_CAPABILITY_CATEGORIES; ++k)
        if (c->counts[k]) return 0;
    return 1;
}
int VgiCatalogCapabilitiesEncode(const struct VgiCatalogCapabilities *c, struct ArrowBuffer *out,
                                 struct ArrowError *error) {
    if (!c || !out || out->size_bytes) return EINVAL;
    struct VgiBatch batch = {0};
    int rc = VgiBatchInitNamed(&batch, "CatalogCapabilities", error);
    for (int k = 0; !rc && k < VGI_CAPABILITY_CATEGORIES; ++k) {
        for (size_t i = 0; !rc && i < c->counts[k]; ++i)
            if (!profile_ok(c->profiles[k][i]))
                rc = failf(error, "invalid catalog capability profile: %s", c->profiles[k][i]);
        if (!rc)
            rc = append_strings(VgiBatchColumn(&batch, category_names[k]),
                                (const char *const *)c->profiles[k], c->counts[k]);
    }
    if (!rc) rc = encode_batch(&batch, out, error);
    VgiBatchReset(&batch);
    if (rc) ArrowBufferReset(out);
    return rc;
}
int VgiCatalogCapabilitiesDecode(struct ArrowBufferView bytes, struct VgiCatalogCapabilities *out,
                                 struct ArrowError *error) {
    if (!out) return EINVAL;
    memset(out, 0, sizeof(*out));
    struct VgiBatch batch = {0};
    int rc = decode_record(bytes, "CatalogCapabilities", &batch, error);
    for (int k = 0; !rc && k < VGI_CAPABILITY_CATEGORIES; ++k) {
        struct VgiOwnedPath list = {0};
        rc = read_path(VgiBatchGet(&batch, category_names[k]), 0, &list,
                       "catalog capability profile", error);
        for (size_t i = 0; !rc && i < list.count; ++i)
            rc = VgiCatalogCapabilitiesAdd(out, (enum VgiCapabilityCategory)k, list.components[i],
                                           error);
        path_free(&list);
    }
    VgiBatchReset(&batch);
    if (rc) VgiCatalogCapabilitiesReset(out);
    return rc;
}
int VgiClientCatalogOffer(struct ArrowBufferView client, struct VgiCatalogCapabilities *out,
                          struct ArrowError *error) {
    if (!out) return EINVAL;
    memset(out, 0, sizeof(*out));
    if (!client.data.data || client.size_bytes <= 0) return 0;
    if (client.size_bytes > MAX_RECORD_BYTES)
        return failf(error, "invalid client capability record size");
    /* Read the catalog offer by name: the rest of ClientCapabilities grows. */
    struct VgiBatch batch = {0};
    int rc = VgiBatchDecode(&batch, client, error);
    if (!rc && batch.array.length != 1) rc = failf(error, "invalid ClientCapabilities record");
    if (!rc && !VgiBatchIsNull(&batch, "catalog"))
        rc = VgiCatalogCapabilitiesDecode(VgiBatchBytes(&batch, "catalog"), out, error);
    VgiBatchReset(&batch);
    return rc;
}
int VgiCatalogCapabilitiesSelect(const struct VgiCatalogCapabilities *available,
                                 const struct VgiCatalogCapabilities *offered,
                                 struct VgiCatalogCapabilities *out, struct ArrowError *error) {
    if (!available || !offered || !out) return EINVAL;
    memset(out, 0, sizeof(*out));
    int rc = 0;
    int records =
        VgiCatalogCapabilitiesHas(available, VGI_CAPABILITY_READ, VGI_CONSTRAINT_RECORDS_READ) &&
        VgiCatalogCapabilitiesHas(offered, VGI_CAPABILITY_READ, VGI_CONSTRAINT_RECORDS_READ);
    int enforce =
        records &&
        VgiCatalogCapabilitiesHas(available, VGI_CAPABILITY_WRITE, VGI_CONSTRAINT_ENFORCE) &&
        VgiCatalogCapabilitiesHas(offered, VGI_CAPABILITY_WRITE, VGI_CONSTRAINT_ENFORCE);
    for (int k = 0; !rc && k < VGI_CAPABILITY_CATEGORIES; ++k)
        for (size_t i = 0; !rc && i < available->counts[k]; ++i) {
            const char *profile = available->profiles[k][i];
            if (!VgiCatalogCapabilitiesHas(offered, (enum VgiCapabilityCategory)k, profile))
                continue;
            /* These describe or act on canonical constraints, so none outlives
             * the records profile, and CREATE needs an authority that enforces. */
            if (!records && (!strcmp(profile, VGI_SCALAR_EXPRESSION_READ) ||
                             !strcmp(profile, VGI_CONSTRAINT_CREATE) ||
                             !strcmp(profile, VGI_CONSTRAINT_ENFORCE)))
                continue;
            if (!enforce && !strcmp(profile, VGI_CONSTRAINT_CREATE)) continue;
            rc = VgiCatalogCapabilitiesAdd(out, (enum VgiCapabilityCategory)k, profile, error);
        }
    if (rc) VgiCatalogCapabilitiesReset(out);
    return rc;
}

/* ---- Generated columns ---------------------------------------------------
 * The extension reads a VIRTUAL generated column from two field metadata keys:
 * vgi.column.generated.v1 (canonical base64 of one SqlExpression record) and
 * vgi.column.generation.v1 = "virtual". The legacy generated_expression key is
 * translated to that form. The SQL grammar itself is left to ATTACH. */

#define GENERATED_KEY "vgi.column.generated.v1"
#define GENERATION_KEY "vgi.column.generation.v1"
static int metadata_get(const char *metadata, const char *key, struct ArrowStringView *out) {
    struct ArrowMetadataReader reader;
    struct ArrowStringView k, v;
    if (!metadata || ArrowMetadataReaderInit(&reader, metadata)) return 0;
    while (reader.remaining_keys > 0 && !ArrowMetadataReaderRead(&reader, &k, &v))
        if (view_is(k, key)) {
            *out = v;
            return 1;
        }
    return 0;
}
/* Encode a default-context SqlExpression for sql owned by schema_path. */
static int expression_record(struct ArrowStringView sql, const struct VgiOwnedPath *schema_path,
                             struct ArrowBuffer *out, struct ArrowError *error) {
    struct VgiBatch batch = {0};
    int rc = VgiBatchInitNamed(&batch, "SqlExpression", error);
    if (!rc) rc = VgiArrayAppendString(VgiBatchColumn(&batch, "sql"), sql);
    if (!rc) rc = VgiBatchSetString(&batch, "profile", VGI_SCALAR_EXPRESSION_READ);
    if (!rc)
        rc = append_strings(VgiBatchColumn(&batch, "schema_path"),
                            (const char *const *)schema_path->components, schema_path->count);
    struct ArrowArray *context = rc ? NULL : VgiBatchColumn(&batch, "evaluation_context");
    const char *values[] = {"UTC", "binary", "asc", "nulls_last"};
    for (int i = 0; !rc && i < 4; ++i)
        rc = ArrowArrayAppendString(context->children[i], ArrowCharView(values[i]));
    if (!rc) rc = ArrowArrayAppendInt(context->children[4], 1);
    if (!rc) rc = ArrowArrayFinishElement(context);
    if (!rc) rc = encode_batch(&batch, out, error);
    VgiBatchReset(&batch);
    return rc;
}
/* Replace legacy generated_expression metadata with the canonical keys. */
static int translate_generated(struct ArrowSchema *field, struct ArrowStringView sql,
                               const struct VgiOwnedPath *schema_path, struct ArrowError *error) {
    struct ArrowBuffer record, metadata;
    ArrowBufferInit(&record);
    ArrowBufferInit(&metadata);
    char *encoded = NULL;
    int rc = expression_record(sql, schema_path, &record, error);
    size_t size =
        rc ? 0
           : sodium_base64_ENCODED_LEN((size_t)record.size_bytes, sodium_base64_VARIANT_ORIGINAL);
    if (!rc && !(encoded = malloc(size))) rc = ENOMEM;
    if (!rc)
        sodium_bin2base64(encoded, size, record.data, (size_t)record.size_bytes,
                          sodium_base64_VARIANT_ORIGINAL);
    if (!rc) rc = ArrowMetadataBuilderInit(&metadata, NULL);
    struct ArrowMetadataReader reader;
    struct ArrowStringView k, v;
    if (!rc) rc = ArrowMetadataReaderInit(&reader, field->metadata);
    while (!rc && reader.remaining_keys > 0) {
        rc = ArrowMetadataReaderRead(&reader, &k, &v);
        if (!rc && !view_is(k, "generated_expression"))
            rc = ArrowMetadataBuilderAppend(&metadata, k, v);
    }
    if (!rc)
        rc = ArrowMetadataBuilderAppend(&metadata, ArrowCharView(GENERATED_KEY),
                                        ArrowCharView(encoded));
    if (!rc)
        rc = ArrowMetadataBuilderAppend(&metadata, ArrowCharView(GENERATION_KEY),
                                        ArrowCharView("virtual"));
    if (!rc) rc = ArrowSchemaSetMetadata(field, (const char *)metadata.data);
    free(encoded);
    ArrowBufferReset(&record);
    ArrowBufferReset(&metadata);
    return rc;
}
static int generated_type(const struct ArrowSchema *field) {
    static const char *const formats[] = {"b", "c", "s", "i", "l", "u"};
    if (field->dictionary || metadata_key(field->metadata, "ARROW:extension:name", 0)) return 0;
    for (size_t i = 0; i < sizeof(formats) / sizeof(*formats); ++i)
        if (!strcmp(field->format, formats[i])) return 1;
    return 0;
}
static int check_generated(struct ArrowSchema *columns, const struct VgiOwnedPath *schema_path,
                           const char *table, struct ArrowError *error) {
    int generated = 0, physical = 0, rc = 0;
    for (int64_t i = 0; !rc && i < columns->n_children; ++i) {
        struct ArrowSchema *field = columns->children[i];
        const char *name = field->name ? field->name : "";
        struct ArrowStringView legacy = {0}, expression = {0}, category = {0};
        int has_legacy = metadata_get(field->metadata, "generated_expression", &legacy),
            has_expression = metadata_get(field->metadata, GENERATED_KEY, &expression),
            has_category = metadata_get(field->metadata, GENERATION_KEY, &category);
        if (has_legacy && (has_expression || has_category))
            return failf(error,
                         "Table '%s': generated column '%s' carries both legacy "
                         "generated_expression and canonical metadata",
                         table, name);
        if (has_legacy) {
            if (!legacy.size_bytes)
                return failf(error, "Table '%s': generated column '%s' has empty SQL", table, name);
            NANOARROW_RETURN_NOT_OK(translate_generated(field, legacy, schema_path, error));
            has_expression = metadata_get(field->metadata, GENERATED_KEY, &expression);
            has_category = metadata_get(field->metadata, GENERATION_KEY, &category);
        }
        struct ArrowMetadataReader reader;
        struct ArrowStringView k, v;
        if (field->metadata && !ArrowMetadataReaderInit(&reader, field->metadata))
            while (reader.remaining_keys > 0 && !ArrowMetadataReaderRead(&reader, &k, &v))
                if ((k.size_bytes >= 21 && !memcmp(k.data, "vgi.column.generated.", 21) &&
                     !view_is(k, GENERATED_KEY)) ||
                    (k.size_bytes >= 22 && !memcmp(k.data, "vgi.column.generation.", 22) &&
                     !view_is(k, GENERATION_KEY)))
                    return failf(error, "Table '%s': column '%s' has unknown generated metadata",
                                 table, name);
        if (!has_expression && !has_category) {
            physical |= !is_row_id(field);
            continue;
        }
        generated = 1;
#define REFUSE(message) \
    return failf(error, "Table '%s': generated column '%s': %s", table, name, message)
        if (has_expression != has_category)
            REFUSE("generated expression and category must occur together");
        if (!view_is(category, "virtual")) REFUSE("only VIRTUAL generated columns are supported");
        if (metadata_key(field->metadata, "vgi.column.default.v1", 0) ||
            metadata_key(field->metadata, "default", 0))
            REFUSE("a generated column cannot also carry a default");
        if (!(field->flags & ARROW_FLAG_NULLABLE))
            REFUSE(
                "generated columns must be nullable (a NOT_NULL or PRIMARY_KEY record names it)");
        if (!generated_type(field))
            REFUSE("generated columns require a signed integer, Boolean or utf8 type");
        size_t length = (size_t)expression.size_bytes, size = 0;
        unsigned char *bytes = length ? malloc(length) : NULL;
        if (!bytes) REFUSE("invalid generated metadata size");
        int bad = sodium_base642bin(bytes, length, expression.data, length, NULL, &size, NULL,
                                    sodium_base64_VARIANT_ORIGINAL) ||
                  !size ||
                  sodium_base64_ENCODED_LEN(size, sodium_base64_VARIANT_ORIGINAL) - 1 != length;
        struct VgiBatch record = {0};
        struct VgiOwnedPath owner = {0};
        if (!bad)
            bad = decode_record((struct ArrowBufferView){{bytes}, (int64_t)size}, "SqlExpression",
                                &record, error) != 0;
        free(bytes);
        if (!bad)
            bad = !view_is(VgiBatchString(&record, "profile"), VGI_SCALAR_EXPRESSION_READ) ||
                  !VgiBatchString(&record, "sql").size_bytes || !context_is_default(&record) ||
                  read_path(VgiBatchGet(&record, "schema_path"), 0, &owner, "schema_path", error) ||
                  owner.count != 1 || schema_path->count != 1 ||
                  !ci_equal(owner.components[0], schema_path->components[0]);
        path_free(&owner);
        VgiBatchReset(&record);
        if (bad)
            REFUSE(
                "vgi.column.generated.v1 must be canonical base64 of one default-context "
                "vgi.expression.duckdb.scalar.v1 SqlExpression owned by the table's schema");
#undef REFUSE
    }
    if (!rc && generated && !physical)
        rc = failf(error, "Table '%s': a table with generated columns requires a physical column",
                   table);
    return rc;
}

/* ---- Serving TableInfo --------------------------------------------------- */

static const char *const legacy_fields[] = {"not_null_constraints", "unique_constraints",
                                            "check_constraints", "primary_key_constraints",
                                            "foreign_key_constraints"};
struct Table {
    struct VgiBatch info;
    struct ArrowSchema columns;
    struct VgiOwnedPath schema_path;
    char *name;
    struct ArrowBufferView *records;
    size_t record_count;
};
static void table_reset(struct Table *t) {
    VgiBatchReset(&t->info);
    if (t->columns.release) ArrowSchemaRelease(&t->columns);
    path_free(&t->schema_path);
    free(t->name);
    free(t->records);
    memset(t, 0, sizeof(*t));
}
static int table_read(struct ArrowBufferView bytes, struct Table *t, struct ArrowError *error) {
    int rc = VgiBatchDecode(&t->info, bytes, error);
    if (!rc && t->info.array.length != 1) rc = failf(error, "invalid TableInfo record");
    struct ArrowStringView name =
        rc ? (struct ArrowStringView){0} : VgiBatchString(&t->info, "name");
    if (!rc && !(t->name = dup_view(name)))
        rc = name.data ? ENOMEM : failf(error, "TableInfo has no name");
    for (size_t i = 0; !rc && i < sizeof(legacy_fields) / sizeof(*legacy_fields); ++i) {
        const struct ArrowArrayView *legacy = VgiBatchGet(&t->info, legacy_fields[i]);
        if (legacy && !ArrowArrayViewIsNull(legacy, 0) &&
            ArrowArrayViewListChildOffset(legacy, 1) > ArrowArrayViewListChildOffset(legacy, 0))
            rc = failf(error,
                       "Table '%s' declares %s, which vgi.v3 removed; serve canonical "
                       "ConstraintInfo records in TableInfo.constraints, built with "
                       "VgiConstraintRecordEncode",
                       t->name, legacy_fields[i]);
    }
    if (!rc)
        rc = read_path(VgiBatchGet(&t->info, "schema_path"), 0, &t->schema_path, "schema_path",
                       error);
    if (!rc) rc = VgiIpcReadSchema(VgiBatchBytes(&t->info, "columns"), &t->columns, error);
    const struct ArrowArrayView *list = rc ? NULL : VgiBatchGet(&t->info, "constraints");
    if (list && !ArrowArrayViewIsNull(list, 0)) {
        int64_t start = ArrowArrayViewListChildOffset(list, 0),
                end = ArrowArrayViewListChildOffset(list, 1);
        if (end - start > MAX_RECORDS)
            rc = failf(error, "Table '%s': more than %d constraint records", t->name, MAX_RECORDS);
        t->records = rc ? NULL : calloc((size_t)(end - start) + 1, sizeof(*t->records));
        if (!rc && !t->records) rc = ENOMEM;
        for (int64_t i = start; !rc && i < end; ++i) {
            struct ArrowStringView value = VgiValueString(list->children[0], i);
            if (!value.data) rc = failf(error, "Table '%s': null constraint record", t->name);
            t->records[t->record_count++] =
                (struct ArrowBufferView){{value.data}, value.size_bytes};
        }
    }
    return rc;
}
int VgiTableInfoServe(struct ArrowBufferView bytes, struct ArrowBuffer *out, int *has_records,
                      int *has_check, struct ArrowError *error) {
    if (!out || out->size_bytes) return EINVAL;
    struct Table t = {0};
    struct VgiConstraintRecord *records = NULL;
    struct ArrowSchema served = {0};
    struct ArrowBuffer columns;
    ArrowBufferInit(&columns);
    struct VgiBatch target = {0};
    int rc = table_read(bytes, &t, error);
    if (!rc)
        rc = VgiConstraintValidate((const char *const *)t.schema_path.components,
                                   t.schema_path.count, t.name, &t.columns, t.records,
                                   t.record_count, error);
    if (!rc) rc = decode_all(t.records, t.record_count, &records, error);
    if (!rc) rc = serve_schema(&t.columns, records, t.record_count, &served, error);
    if (!rc) rc = check_generated(&served, &t.schema_path, t.name, error);
    if (!rc) rc = VgiIpcWriteSchema(&served, &columns, error);
    if (!rc) rc = VgiBatchInitNamed(&target, "TableInfo", error);
    for (int64_t i = 0; !rc && i < target.schema.n_children; ++i) {
        const char *field = target.schema.children[i]->name;
        int64_t source = VgiFieldIndex(&t.info.schema, field);
        if (!strcmp(field, "columns"))
            rc = set_buffer(&target, field, &columns);
        else if (source >= 0)
            rc = VgiArrayAppendValue(target.array.children[i], target.schema.children[i],
                                     t.info.view.children[source], t.info.schema.children[source],
                                     0, error);
    }
    if (!rc) rc = encode_batch(&target, out, error);
    if (!rc && has_records) *has_records = t.record_count > 0;
    if (!rc && has_check) {
        *has_check = 0;
        for (size_t i = 0; i < t.record_count; ++i)
            *has_check |= records[i].kind == VGI_CONSTRAINT_CHECK;
    }
    free_all(records, t.record_count);
    table_reset(&t);
    if (served.release) ArrowSchemaRelease(&served);
    ArrowBufferReset(&columns);
    VgiBatchReset(&target);
    if (rc) ArrowBufferReset(out);
    return rc;
}
int VgiTableInfoCheckForeignKeys(const struct ArrowBufferView *tables, size_t count,
                                 struct ArrowError *error) {
    struct Table *t = calloc(count + 1, sizeof(*t));
    struct VgiConstraintRecord **records = calloc(count + 1, sizeof(*records));
    int rc = t && records ? 0 : ENOMEM;
    for (size_t i = 0; !rc && i < count; ++i) {
        rc = table_read(tables[i], &t[i], error);
        if (!rc) rc = decode_all(t[i].records, t[i].record_count, &records[i], error);
    }
    char where[600];
    for (size_t i = 0; !rc && i < count; ++i)
        for (size_t j = 0; !rc && j < t[i].record_count; ++j) {
            const struct VgiConstraintRecord *fk = &records[i][j];
            if (fk->kind != VGI_CONSTRAINT_FOREIGN_KEY) continue;
            path_join((const char *const *)t[i].schema_path.components, t[i].schema_path.count,
                      where, sizeof(where));
            size_t target = count;
            for (size_t k = 0; k < count && target == count; ++k)
                if (path_equal(&t[k].schema_path, &fk->ref_schema_path) &&
                    !strcmp(t[k].name, fk->ref_table))
                    target = k;
            if (target == count) {
                rc = failf(error,
                           "Table '%s.%s': constraint '%s' references table %s, which this "
                           "catalog does not declare in the same schema",
                           where, t[i].name, fk->id, fk->ref_table);
                break;
            }
            const struct VgiConstraintRecord *key = NULL;
            for (size_t k = 0; k < t[target].record_count && !key; ++k)
                if (!strcmp(records[target][k].id, fk->ref_constraint_id))
                    key = &records[target][k];
            if (!key ||
                (key->kind != VGI_CONSTRAINT_PRIMARY_KEY && key->kind != VGI_CONSTRAINT_UNIQUE)) {
                rc = failf(error,
                           "Table '%s.%s': constraint '%s' references key '%s', which is not a "
                           "PRIMARY_KEY or UNIQUE record of %s",
                           where, t[i].name, fk->id, fk->ref_constraint_id, fk->ref_table);
                break;
            }
            int same = key->column_count == fk->ref_column_count;
            for (size_t k = 0; same && k < key->column_count; ++k)
                same = path_equal(&key->columns[k], &fk->ref_columns[k]);
            if (!same)
                rc = failf(error,
                           "Table '%s.%s': constraint '%s' references columns that differ from "
                           "key '%s'",
                           where, t[i].name, fk->id, fk->ref_constraint_id);
        }
    for (size_t i = 0; t && records && i < count; ++i) {
        free_all(records[i], t[i].record_count);
        table_reset(&t[i]);
    }
    free(t);
    free(records);
    return rc;
}
int VgiCatalogNegotiate(int records, struct ArrowBufferView catalog_capabilities,
                        struct ArrowBufferView client_capabilities, struct ArrowBuffer *available,
                        struct ArrowBuffer *selected, struct ArrowError *error) {
    struct VgiCatalogCapabilities offer = {0}, offered = {0}, chosen = {0};
    int rc = 0;
    if (catalog_capabilities.data.data && catalog_capabilities.size_bytes > 0)
        rc = VgiCatalogCapabilitiesDecode(catalog_capabilities, &offer, error);
    const char *automatic[] = {VGI_CONSTRAINT_RECORDS_READ, VGI_SCALAR_EXPRESSION_READ};
    for (int i = 0; !rc && records && i < 2; ++i)
        if (!VgiCatalogCapabilitiesHas(&offer, VGI_CAPABILITY_READ, automatic[i]))
            rc = VgiCatalogCapabilitiesAdd(&offer, VGI_CAPABILITY_READ, automatic[i], error);
    if (!rc && !capabilities_empty(&offer)) {
        rc = VgiClientCatalogOffer(client_capabilities, &offered, error);
        if (!rc) rc = VgiCatalogCapabilitiesSelect(&offer, &offered, &chosen, error);
        if (!rc) rc = VgiCatalogCapabilitiesEncode(&offer, available, error);
        if (!rc) rc = VgiCatalogCapabilitiesEncode(&chosen, selected, error);
    }
    VgiCatalogCapabilitiesReset(&offer);
    VgiCatalogCapabilitiesReset(&offered);
    VgiCatalogCapabilitiesReset(&chosen);
    if (rc) {
        ArrowBufferReset(available);
        ArrowBufferReset(selected);
    }
    return rc;
}
