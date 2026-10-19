#ifndef VGI_CATALOG_INTERNAL_H
#define VGI_CATALOG_INTERNAL_H
#include "vgi/rpc.h"
struct VgiCatalogService;
struct VgiCatalogVersions;
int VgiCatalogSetVersions(struct VgiCatalogService *, const char *,
                          const struct VgiCatalogVersions *, struct ArrowError *);
int VgiCatalogAddSnapshot(struct VgiCatalogService *, const char *, const char *,
                          const struct VgiBatch *, const struct VgiBatch *, struct ArrowError *);
int VgiCatalogAdd(struct VgiCatalogService **service, const struct VgiBatch *info,
                  const struct VgiBatch *attach, const struct VgiBatch *contents,
                  const struct VgiBatch *extras, struct ArrowError *error);
void VgiCatalogFree(struct VgiCatalogService *service);
int VgiCatalogLoadOptions(struct ArrowBufferView cookie, struct VgiBatch *out,
                          struct ArrowError *error);
int VgiCatalogMergeOptions(const struct VgiBatch *, const struct VgiBatch *, struct VgiBatch *,
                           struct ArrowError *);
int VgiCatalogStoreOptions(struct ArrowStringView, const struct VgiBatch *, struct ArrowError *);
int VgiCatalogDispatch(struct VgiCatalogService *service, const char *primary,
                       struct VgiRpcCall *call, struct VgiRpcReply *reply, int *handled);
const struct VgiBatch *VgiCatalogInfo(struct VgiCatalogService *, const char *);
#endif
