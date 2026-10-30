#ifndef VGI_CONSTRAINTS_H
#define VGI_CONSTRAINTS_H
/* Canonical constraint records and catalog capability negotiation (vgi.v3).
 *
 * A table's constraints travel as TableInfo.constraints: a list of IPC-encoded
 * ConstraintInfo records, each the provider's attestation of one fact about
 * every row the table can return. The SDK never derives them from Arrow
 * nullability; build each one with VgiConstraintRecordEncode().
 *
 * The DuckDB extension holds every served table to its ATTACH contract, and
 * the SDK applies the same rules when a catalog is registered and again when
 * a table is served:
 *
 *  (a) records are read only when ATTACH negotiated VGI_CONSTRAINT_RECORDS_READ
 *      (and CHECK records also VGI_SCALAR_EXPRESSION_READ). A declarative
 *      catalog whose tables carry records offers both automatically and
 *      selects what the client's ClientCapabilities.catalog offer accepts.
 *  (b) only ENFORCED + ALL_ROWS records are accepted, owned by the serving
 *      table and its one-component schema, with unique ids and declared names
 *      (case-insensitive), at most one PRIMARY_KEY, NOT_NULL paths that resolve
 *      through ordinary STRUCTs, top-level PRIMARY_KEY / UNIQUE / FOREIGN_KEY
 *      columns, same-schema MATCH SIMPLE NO_ACTION / RESTRICT foreign keys,
 *      and CHECKs that compare numeric literals and primitive signed numeric
 *      columns under the default evaluation context.
 *  (c) a field (descending through ordinary STRUCTs) is served non-nullable
 *      exactly when a NOT_NULL or PRIMARY_KEY record names its path; every
 *      other field is served nullable. The top-level is_row_id column is left
 *      as declared.
 *
 * Inputs are borrowed. Outputs are owned by the caller and must be empty on
 * entry; on failure they are left empty and error names the table, the
 * constraint id and the column path where one applies. */
#include <stddef.h>

#include "nanoarrow/nanoarrow.h"
#ifdef __cplusplus
extern "C" {
#endif

/* Read profile: the catalog serves canonical ConstraintInfo records. */
#define VGI_CONSTRAINT_RECORDS_READ "vgi.constraint.records.read.v1"
/* Read profile: CHECK records carry DuckDB scalar SQL; only with the above. */
#define VGI_SCALAR_EXPRESSION_READ "vgi.expression.duckdb.scalar.v1"
/* DDL profile: CREATE TABLE may carry constraint definitions. */
#define VGI_CONSTRAINT_CREATE "vgi.constraint.create.v1"
/* Write profile: the catalog enforces its constraints on every write path. */
#define VGI_CONSTRAINT_ENFORCE "vgi.constraint.enforce.v1"

enum VgiConstraintKind {
    VGI_CONSTRAINT_NOT_NULL,
    VGI_CONSTRAINT_PRIMARY_KEY,
    VGI_CONSTRAINT_UNIQUE,
    VGI_CONSTRAINT_CHECK,
    VGI_CONSTRAINT_FOREIGN_KEY
};
/* Both attestations must be stated: the zero value is refused. */
enum VgiConstraintEnforcement {
    VGI_ENFORCEMENT_UNSTATED,
    VGI_ENFORCEMENT_ENFORCED,
    VGI_ENFORCEMENT_METADATA_ONLY
};
enum VgiConstraintValidation {
    VGI_VALIDATION_UNSTATED,
    VGI_VALIDATION_ALL_ROWS,
    VGI_VALIDATION_FUTURE_WRITES,
    VGI_VALIDATION_UNVALIDATED
};
/* Wire spellings ("NOT_NULL", "ENFORCED", "ALL_ROWS", ...); NULL when invalid. */
const char *VgiConstraintKindName(enum VgiConstraintKind kind);
const char *VgiConstraintEnforcementName(enum VgiConstraintEnforcement enforcement);
const char *VgiConstraintValidationName(enum VgiConstraintValidation validation);

/* Raw components from a top-level column down through STRUCT children, e.g.
 * {"id"} or {"bbox", "xmin"}. Dots inside a component are literal. */
struct VgiColumnPath {
    const char *const *components;
    size_t count;
};
/* The key a FOREIGN_KEY record points at. */
struct VgiConstraintReference {
    const char *const *schema_path;
    size_t schema_path_count;
    const char *table;
    const struct VgiColumnPath *columns;
    size_t column_count;
    /* id of the referenced table's PRIMARY_KEY or UNIQUE record. */
    const char *constraint_id;
};
struct VgiConstraintSpec {
    /* Schema path and name of the owning table. */
    const char *const *schema_path;
    size_t schema_path_count;
    const char *table;
    /* Stable provider-owned identity, unique within the table. */
    const char *id;
    enum VgiConstraintKind kind;
    /* Exactly one for NOT_NULL, none for CHECK, at least one otherwise. */
    const struct VgiColumnPath *columns;
    size_t column_count;
    /* State ENFORCED + ALL_ROWS only when every row the table can return
     * satisfies the constraint; DuckDB reads no other combination. */
    enum VgiConstraintEnforcement enforcement;
    enum VgiConstraintValidation validation;
    const char *declared_name;                       /* optional SQL label */
    const char *check_sql;                           /* CHECK only: DuckDB scalar SQL */
    const struct VgiConstraintReference *references; /* FOREIGN_KEY only */
};
/* Encode one canonical ConstraintInfo record for TableInfo.constraints.
 * Initialize out with ArrowBufferInit; release it with ArrowBufferReset. */
int VgiConstraintRecordEncode(const struct VgiConstraintSpec *spec, struct ArrowBuffer *out,
                              struct ArrowError *error);

struct VgiOwnedPath {
    char **components;
    size_t count;
};
/* A decoded ConstraintInfo record; release with VgiConstraintRecordReset. */
struct VgiConstraintRecord {
    struct VgiOwnedPath schema_path;
    char *table;
    char *id;
    enum VgiConstraintKind kind;
    enum VgiConstraintEnforcement enforcement;
    enum VgiConstraintValidation validation;
    struct VgiOwnedPath *columns;
    size_t column_count;
    char *declared_name; /* NULL when absent */
    char *check_sql;     /* CHECK only */
    /* FOREIGN_KEY only (has_references). ref_constraint_id is NULL when the
     * record names no referenced key. */
    int has_references;
    struct VgiOwnedPath ref_schema_path;
    char *ref_table;
    struct VgiOwnedPath *ref_columns;
    size_t ref_column_count;
    char *ref_constraint_id;
    /* What the extension also checks. */
    struct VgiOwnedPath ref_key_schema_path;
    char *ref_key_owner;
    char *match, *on_update, *on_delete;
    char *check_profile;
    struct VgiOwnedPath check_schema_path;
    int check_default_context;
};
/* Decode one TableInfo.constraints entry; out must be zeroed. */
int VgiConstraintRecordDecode(struct ArrowBufferView bytes, struct VgiConstraintRecord *out,
                              struct ArrowError *error);
void VgiConstraintRecordReset(struct VgiConstraintRecord *record);

/* The schema a table is served with (rule (c) above). Field metadata is
 * preserved. out must be released with ArrowSchemaRelease. */
int VgiConstraintServedSchema(const struct ArrowSchema *columns,
                              const struct ArrowBufferView *records, size_t count,
                              struct ArrowSchema *out, struct ArrowError *error);
/* Check a table's records as the extension does at ATTACH (rule (b)), against
 * the table as served. schema_path may be NULL to skip the ownership check, in
 * which case table is only the name used in messages. A FOREIGN_KEY's target
 * table is checked by the catalog that serves it. */
int VgiConstraintValidate(const char *const *schema_path, size_t schema_path_count,
                          const char *table, const struct ArrowSchema *columns,
                          const struct ArrowBufferView *records, size_t count,
                          struct ArrowError *error);

/* Catalog capability negotiation: CatalogAttachResult.catalog_capabilities
 * and selected_capabilities, and ClientCapabilities.catalog. */
enum VgiCapabilityCategory {
    VGI_CAPABILITY_READ,
    VGI_CAPABILITY_DDL,
    VGI_CAPABILITY_WRITE,
    VGI_CAPABILITY_TRANSACTION,
    VGI_CAPABILITY_CATEGORIES
};
struct VgiCatalogCapabilities {
    char **profiles[VGI_CAPABILITY_CATEGORIES];
    size_t counts[VGI_CAPABILITY_CATEGORIES];
};
/* Append a profile (a dotted lowercase name ending .vN); duplicates fail. */
int VgiCatalogCapabilitiesAdd(struct VgiCatalogCapabilities *capabilities,
                              enum VgiCapabilityCategory category, const char *profile,
                              struct ArrowError *error);
int VgiCatalogCapabilitiesHas(const struct VgiCatalogCapabilities *capabilities,
                              enum VgiCapabilityCategory category, const char *profile);
int VgiCatalogCapabilitiesEncode(const struct VgiCatalogCapabilities *capabilities,
                                 struct ArrowBuffer *out, struct ArrowError *error);
int VgiCatalogCapabilitiesDecode(struct ArrowBufferView bytes, struct VgiCatalogCapabilities *out,
                                 struct ArrowError *error);
/* The catalog offer inside CatalogAttachRequest.client_capabilities (an
 * IPC-encoded ClientCapabilities record); empty when the client sent none. */
int VgiClientCatalogOffer(struct ArrowBufferView client_capabilities,
                          struct VgiCatalogCapabilities *out, struct ArrowError *error);
/* available intersected with offered per category, in available's order, then
 * pruned to what the extension accepts together: SCALAR_EXPRESSION_READ,
 * CONSTRAINT_CREATE and CONSTRAINT_ENFORCE only alongside
 * CONSTRAINT_RECORDS_READ, and CONSTRAINT_CREATE only alongside
 * CONSTRAINT_ENFORCE. */
int VgiCatalogCapabilitiesSelect(const struct VgiCatalogCapabilities *available,
                                 const struct VgiCatalogCapabilities *offered,
                                 struct VgiCatalogCapabilities *out, struct ArrowError *error);
void VgiCatalogCapabilitiesReset(struct VgiCatalogCapabilities *capabilities);
#ifdef __cplusplus
}
#endif
#endif
