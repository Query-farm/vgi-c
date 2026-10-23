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
/* Declarative contents keep the per-schema layout: catalog_version (int64) and
 * schemas, a list of generated SchemaContents. */
int VgiCatalogContentsInit(struct VgiBatch *out, struct ArrowError *error);
/* catalog_contents (wire 2.2) carries six collections: schemas, tables, views,
 * functions, macros and indexes, each an IPC stream of its record, empty if none. */
#define VGI_CATALOG_COLLECTIONS 6
int VgiCatalogCollections(const struct VgiBatch *contents,
                          struct ArrowBuffer out[VGI_CATALOG_COLLECTIONS], struct ArrowError *);
void VgiCatalogCollectionsReset(struct ArrowBuffer collections[VGI_CATALOG_COLLECTIONS]);
/* Set the six collection columns of a CatalogContentsResult row; empty ones are null. */
int VgiCatalogCollectionsSet(struct VgiBatch *result,
                             const struct ArrowBuffer collections[VGI_CATALOG_COLLECTIONS]);
/* vgi-python's content-hash ETag over the six collections, as lowercase hex. */
void VgiCatalogCollectionsDigest(const struct ArrowBuffer collections[VGI_CATALOG_COLLECTIONS],
                                 char hex[65]);
#endif
