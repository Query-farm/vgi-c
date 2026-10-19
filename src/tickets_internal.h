#ifndef VGI_TICKETS_INTERNAL_H
#define VGI_TICKETS_INTERNAL_H
#include "vgi/credentials.h"
#include "vgi/rpc.h"
typedef const struct VgiBatch *(*VgiTicketCatalog)(void *, const char *);
int VgiTicketRegister(struct VgiRpcServer *, const unsigned char key[32], int64_t ceiling,
                      VgiTicketCatalog, void *, struct ArrowError *);
int VgiTicketRedeem(struct VgiRpcCall *);
int VgiTicketCheckOptions(const struct VgiBatch *, struct ArrowError *);
#endif
