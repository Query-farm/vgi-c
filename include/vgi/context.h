#ifndef VGI_CONTEXT_H
#define VGI_CONTEXT_H
#include "vgi/worker.h"
#ifdef __cplusplus
extern "C" {
#endif
/* Select by type and, when supplied, the longest matching newline-separated
 * scope prefix (matched by VgiScopeMatches, so an authority-only scope stops at
 * the authority boundary); an unscoped secret of the type is the fallback.
 * Returned values are borrowed from the call. */
const struct ArrowArrayView *VgiSecretSelect(const struct VgiFunctionCall *call, const char *type,
                                             const struct ArrowStringView *path,
                                             const struct ArrowSchema **schema);
/* Nonzero when the secret scope `prefix` covers `path`, exactly as DuckDB
 * decides (BaseSecret::MatchScore as of the Haybarn 2.0.1 engine, upstream
 * duckdb#28684). A scope matches when it is a byte prefix of `path`, except
 * that an authority-only URI scope (`scheme://authority` with nothing after the
 * authority) has an implicit trailing `/`: `snmp://10.0.0.1` covers
 * `snmp://10.0.0.1` and `snmp://10.0.0.1/x` but not `snmp://10.0.0.15` or
 * `snmp://10.0.0.1:161`, and `s3://bucket` does not cover `s3://bucket-evil/`.
 * An empty `prefix` is not a scope and never matches. Neither view needs to be
 * NUL-terminated. */
int VgiScopeMatches(struct ArrowStringView prefix, struct ArrowStringView path);
const struct ArrowArrayView *VgiStructField(const struct ArrowArrayView *value,
                                            const struct ArrowSchema *schema, const char *name);
int VgiValueRender(const struct ArrowArrayView *value, const struct ArrowSchema *schema,
                   int64_t row, struct ArrowBuffer *text, struct ArrowError *error);
#ifdef __cplusplus
}
#endif
#endif
