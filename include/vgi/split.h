#ifndef VGI_SPLIT_H
#define VGI_SPLIT_H
#include "vgi/batch.h"
#ifdef __cplusplus
extern "C" {
#endif
struct VgiSplitIdentity {
    int authenticated;
    const char *domain;
    const char *principal;
};
/* Fingerprint the function/schema/arguments/settings identity used by a bind. */
int VgiSplitFingerprint(const struct VgiBatch *bind_call, unsigned char out[16]);
/* Tokens use the canonical v1 envelope. A non-NULL 32-byte key requires
 * authenticated encryption on both build and open; token flags cannot opt out.
 * Open returns ESTALE for a changed snapshot and EINVAL for an invalid token. */
int VgiSplitTokenBuild(struct ArrowBufferView payload, const unsigned char fingerprint[16],
                       struct ArrowBufferView anchor, const unsigned char *key,
                       const struct VgiSplitIdentity *identity, struct ArrowBuffer *out);
int VgiSplitTokenOpen(struct ArrowBufferView token, const unsigned char fingerprint[16],
                      struct ArrowBufferView anchor, const unsigned char *key,
                      const struct VgiSplitIdentity *identity, struct ArrowBuffer *out);
/* Append a serialized ScanSplit to an unfinished TableFunctionPlanResult.
 * The worker adds and validates token envelopes independently of callbacks. */
int VgiPlanAppendSplit(struct VgiBatch *plan, const struct VgiBatch *split,
                       struct ArrowError *error);
#ifdef __cplusplus
}
#endif
#endif
