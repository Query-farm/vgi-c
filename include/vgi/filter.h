#ifndef VGI_FILTER_H
#define VGI_FILTER_H
#include "vgi/batch.h"
#include "vgi/statistics.h"
#ifdef __cplusplus
extern "C" {
#endif
struct VgiFilters;
struct VgiColumnBounds {
    int has_min, has_max;
    int64_t min, max;
};
int VgiFiltersColumnBounds(const struct VgiFilters *, const char *, struct VgiColumnBounds *,
                           struct ArrowError *);
int VgiFiltersParse(struct ArrowBufferView bytes, const struct ArrowSchema *schema,
                    const struct VgiBatch *init_request, struct VgiFilters **out,
                    struct ArrowError *error);
/* Apply a revisioned delta atomically; stale revisions are ignored. */
int VgiFiltersApplyDelta(struct VgiFilters *, struct ArrowBufferView, struct ArrowError *);
void VgiFiltersFree(struct VgiFilters *filters);
/* Protocol 3.2 filtered scan planning. Parse the pushdown_filters of a
 * catalog_table_scan_branches_get request (one Filter Encoding v2 snapshot)
 * against the table's declared columns (TableInfo.columns, in declaration
 * order). *out is NULL when the request carries no predicates: bind-time
 * planning, or a query without usable predicates. Every predicate is advisory;
 * the client re-applies all of them, so returning extra branches is correct and
 * omitting one that holds a matching row is not. Free with VgiFiltersFree. */
int VgiScanPlanningFiltersParse(const struct VgiBatch *request, const struct ArrowSchema *columns,
                                struct VgiFilters **out, struct ArrowError *error);
/* Zero only when the predicates provably exclude every row described by stats
 * (count entries, matched to columns by name): a candidate file, partition or
 * branch whose non-null values lie within [min, max]. Columns without
 * statistics, predicates it cannot reason about and incomparable bounds may
 * match, so the answer errs towards keeping data. NULL filters match. A DATE
 * column's bound may be VGI_STAT_INT64 days since the epoch or its ISO
 * "YYYY-MM-DD" text (VGI_STAT_STRING), the form the client casts. */
int VgiFiltersMayMatch(const struct VgiFilters *filters, const struct VgiColumnStatistics *stats,
                       size_t count);
int VgiFiltersApply(const struct VgiFilters *filters, const struct VgiBatch *input,
                    struct VgiBatch *output, struct ArrowError *error);
/* Append SQL-style diagnostic text, or the structural representation when repr is nonzero. */
int VgiFiltersFormat(const struct VgiFilters *, int repr, struct ArrowBuffer *,
                     struct ArrowError *);
int VgiFiltersHasColumn(const struct VgiFilters *, const char *name);
int VgiFiltersColumns(const struct VgiFilters *, struct VgiBatch *, struct ArrowError *);
/* ENODATA means no enumerable constraint. A successful zero-row result is an empty set. */
int VgiFiltersColumnValues(const struct VgiFilters *, const char *name, struct VgiBatch *,
                           struct ArrowError *);
#ifdef __cplusplus
}
#endif
#endif
