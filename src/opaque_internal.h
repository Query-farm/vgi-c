#ifndef VGI_OPAQUE_INTERNAL_H
#define VGI_OPAQUE_INTERNAL_H
#include "vgi/rpc.h"
/* Per-call application state: the opened attach envelope first (opaque.c
 * reads it through a struct ArrowBuffer pointer), then the received bind
 * call bytes. */
struct VgiRequestState {
    struct ArrowBuffer envelope;
    struct ArrowBuffer bind_call;
};
/* Copy the bind call bytes of a bind, init or table_function_* request into
 * out, exactly as received, before VgiOpaqueOpenRequest rewrites the decoded
 * request (rule 11: opened values live beside the held bytes, never in them).
 * Other methods leave out empty. */
int VgiOpaqueCaptureBindCall(const struct VgiRpcCall *, struct ArrowBuffer *out);
int VgiOpaqueOpenRequest(struct VgiRpcCall *);
/* Open the sealed opaque values of a decoded batch (a stored bind call), as
 * VgiOpaqueOpenRequest does for the request. No-op without a sealing key. */
int VgiOpaqueOpenBatch(struct VgiRpcCall *, struct VgiBatch *);
int VgiOpaqueSealResponse(struct VgiRpcCall *, struct VgiBatch *);
#endif
