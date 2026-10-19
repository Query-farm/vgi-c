#ifndef VGI_OPAQUE_INTERNAL_H
#define VGI_OPAQUE_INTERNAL_H
#include "vgi/rpc.h"
int VgiOpaqueOpenRequest(struct VgiRpcCall *);
int VgiOpaqueSealResponse(struct VgiRpcCall *, struct VgiBatch *);
#endif
