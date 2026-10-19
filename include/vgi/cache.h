#ifndef VGI_CACHE_H
#define VGI_CACHE_H
#include "vgi/rpc.h"
#ifdef __cplusplus
extern "C" {
#endif
/* Zero initialization leaves all optional fields absent. Strings are borrowed
 * for the duration of VgiCacheControlMetadata(). */
struct VgiCacheControl {
    int has_ttl;
    int64_t ttl_seconds;
    const char *expires;
    const char *scope; /* NULL means catalog. */
    int no_store;
    const char *etag;
    const char *last_modified;
    int revalidatable;
    int has_stale_while_revalidate;
    int64_t stale_while_revalidate;
    int has_stale_if_error;
    int64_t stale_if_error;
    int not_modified;
    int partition_scope;
    int per_value;
};
/* Merge the advertisement into Arrow message metadata. */
int VgiCacheControlMetadata(const struct VgiCacheControl *control, struct ArrowBuffer *metadata);
#ifdef __cplusplus
}
#endif
#endif
