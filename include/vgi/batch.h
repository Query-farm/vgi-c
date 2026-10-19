#ifndef VGI_COMPAT_BATCH_H
#define VGI_COMPAT_BATCH_H
#include "vgi_rpc/batch.h"
#include "vgi/generated/protocol.h"
#ifdef __cplusplus
extern "C" {
#endif
int VgiBatchInitNamed(struct VgiBatch *, const char *, struct ArrowError *);
#ifdef __cplusplus
}
#endif
#endif
