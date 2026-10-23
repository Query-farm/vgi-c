#ifndef VGI_FILTER_INTERNAL_H
#define VGI_FILTER_INTERNAL_H
#include "vgi/filter.h"
/* VgiFiltersApply that also appends each kept input row index (int64) to kept. */
int VgiFiltersSelect(const struct VgiFilters *, const struct VgiBatch *input,
                     struct VgiBatch *output, struct ArrowBuffer *kept, struct ArrowError *);
#endif
