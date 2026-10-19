#ifndef VGI_SPLIT_INTERNAL_H
#define VGI_SPLIT_INTERNAL_H
#include "vgi/worker.h"
int VgiSplitSealPlan(struct VgiFunctionCall *, struct VgiBatch *, int64_t, const unsigned char *);
int VgiSplitRedeem(struct VgiFunctionCall *, int64_t, const unsigned char *);
#endif
