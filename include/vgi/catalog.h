#ifndef VGI_CATALOG_H
#define VGI_CATALOG_H
#include "vgi/rpc.h"
#ifdef __cplusplus
extern "C" {
#endif
struct VgiWorker;
/* An application may handle dynamic catalog operations before the declarative
 * catalog. Set handled=0 to delegate; the callback and state must outlive worker. */
typedef int (*VgiCatalogHandler)(void *state, struct VgiRpcCall *call, struct VgiRpcReply *reply,
                                 int *handled);
void VgiWorkerSetCatalogHandler(struct VgiWorker *worker, VgiCatalogHandler handler, void *state);
/* Dispatch against registered immutable catalogs, bypassing the application callback. */
int VgiWorkerCatalogDefault(struct VgiWorker *worker, struct VgiRpcCall *call,
                            struct VgiRpcReply *reply, int *handled);
enum VgiCatalogEtag {
    VGI_CATALOG_ETAG_NONE,
    VGI_CATALOG_ETAG_GENERATION,
    VGI_CATALOG_ETAG_CONTENT_HASH
};
struct VgiMemoryCatalogOptions {
    const char *name;
    const char *comment;
    int unversioned;
    enum VgiCatalogEtag etag;
};
/* Private per-attachment metadata catalog with schema/table/view CREATE and
 * DROP. State uses the worker's SQLite store; DDL applies immediately. */
int VgiWorkerRegisterMemoryCatalog(struct VgiWorker *, const struct VgiMemoryCatalogOptions *,
                                   struct ArrowError *);
/* Register an immutable declarative catalog. Inputs are copied. The schemas
 * are generated CatalogInfo, CatalogAttachResult and CatalogContentsResult.
 * extras is optional: rows of method, path:list<string>, name, value:binary
 * for table, scan/branch, and column-statistics descriptors. Optional version
 * (int64) and timestamp_year (int32) select historical snapshots. Optional
 * accepts_at (bool) allows application scans to resolve AT themselves. */
int VgiWorkerRegisterCatalog(struct VgiWorker *worker, const struct VgiBatch *info,
                             const struct VgiBatch *attach, const struct VgiBatch *contents,
                             const struct VgiBatch *extras, struct ArrowError *error);
struct VgiCatalogVersions {
    const char *const *data_versions;
    size_t data_version_count;
    const char *default_data_version;
    const char *const *implementation_versions;
    size_t implementation_version_count;
    const char *default_implementation_version;
    /* Exact X.Y.Z, ^X.Y.Z, ~X.Y.Z, X, or X.Y pinned to X.Y.0. */
    int npm_resolution;
};
int VgiWorkerSetCatalogVersions(struct VgiWorker *, const char *catalog,
                                const struct VgiCatalogVersions *, struct ArrowError *);
/* Add an immutable schema/table snapshot for one supported data version. */
int VgiWorkerAddCatalogSnapshot(struct VgiWorker *, const char *catalog, const char *data_version,
                                const struct VgiBatch *contents, const struct VgiBatch *extras,
                                struct ArrowError *);
#ifdef __cplusplus
}
#endif
#endif
