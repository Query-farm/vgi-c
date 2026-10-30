#ifndef VGI_CONSTRAINTS_INTERNAL_H
#define VGI_CONSTRAINTS_INTERNAL_H
#include "vgi/constraints.h"

/* Check one TableInfo record against the ATTACH contract and re-encode it as
 * served: columns rewritten by VgiConstraintServedSchema. Records produced by
 * vgi.v2 (not_null_constraints, unique_constraints, ...) are refused with the
 * builder to use instead. has_records and has_check may be NULL. out must be
 * initialized and empty; it stays empty on failure. */
int VgiTableInfoServe(struct ArrowBufferView table_info, struct ArrowBuffer *out, int *has_records,
                      int *has_check, struct ArrowError *error);
/* Check every FOREIGN_KEY record among tables (TableInfo records served by one
 * catalog snapshot) against its target: a declared table of the same schema
 * whose PRIMARY_KEY or UNIQUE record has the referenced id and columns. */
int VgiTableInfoCheckForeignKeys(const struct ArrowBufferView *tables, size_t count,
                                 struct ArrowError *error);
/* VGI_CONSTRAINT_RECORDS_READ plus VGI_SCALAR_EXPRESSION_READ when records is
 * set, and the read profiles of catalog_capabilities (an encoded
 * CatalogCapabilities record, may be empty), then the selection against the
 * attach request's client_capabilities (may be empty). Both outputs are
 * encoded CatalogCapabilities records and stay empty when nothing is offered. */
int VgiCatalogNegotiate(int records, struct ArrowBufferView catalog_capabilities,
                        struct ArrowBufferView client_capabilities, struct ArrowBuffer *available,
                        struct ArrowBuffer *selected, struct ArrowError *error);
#endif
