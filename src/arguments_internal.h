#ifndef VGI_ARGUMENTS_INTERNAL_H
#define VGI_ARGUMENTS_INTERNAL_H
#include "vgi_rpc/rpc.h"
/* Protocol 3.4 refusals (vgi docs/large-values-3.4.md §2, §5a). */
#define VGI_KIND_INVALID_BIND_REQUEST "invalid_bind_request"
#define VGI_INVALID_BIND_REQUEST_MESSAGE \
    "BindRequest must set exactly one of 'arguments' and 'arguments_location'"
#define VGI_ARGUMENTS_REF_MISSING_MESSAGE \
    "this prepared statement's large argument has expired; prepare it again"
/* A BindRequest (a batch with the arguments_location column) must set exactly
 * one of `arguments` and `arguments_location`; anything else fails the call
 * with invalid_bind_request (INVALID_ARGUMENT). Other requests pass. */
int VgiBindArgumentsCheck(struct VgiRpcCall *, const struct VgiBatch *bind);
#endif
