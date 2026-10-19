#ifndef VGI_MEMORY_CATALOG_INTERNAL_H
#define VGI_MEMORY_CATALOG_INTERNAL_H
#include "vgi/catalog.h"
struct VgiMemoryService;
struct VgiCatalogService;
int VgiMemoryAdd(struct VgiMemoryService **, struct VgiCatalogService **,
                 const struct VgiMemoryCatalogOptions *, struct ArrowError *);
void VgiMemoryFree(struct VgiMemoryService *);
int VgiMemoryDispatch(struct VgiMemoryService *, struct VgiCatalogService *, struct VgiRpcCall *,
                      struct VgiRpcReply *, int *);
#endif
