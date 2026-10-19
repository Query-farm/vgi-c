#ifndef VGI_FILTER_H
#define VGI_FILTER_H
#include "vgi/batch.h"
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
