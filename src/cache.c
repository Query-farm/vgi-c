#include "vgi/cache.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
static int number(struct ArrowBuffer *metadata, const char *key, int64_t value) {
    char text[32];
    snprintf(text, sizeof(text), "%" PRId64, value);
    return VgiMetadataSet(metadata, key, text);
}
int VgiCacheControlMetadata(const struct VgiCacheControl *c, struct ArrowBuffer *metadata) {
    if (!c || !metadata) return EINVAL;
    int rc = VgiMetadataSet(metadata, "vgi.cache.scope", c->scope ? c->scope : "catalog");
    if (!rc && c->has_ttl)
        rc = number(metadata, "vgi.cache.ttl", c->ttl_seconds < 0 ? 0 : c->ttl_seconds);
    const char *keys[] = {"vgi.cache.expires", "vgi.cache.etag", "vgi.cache.last_modified"};
    const char *values[] = {c->expires, c->etag, c->last_modified};
    for (int i = 0; !rc && i < 3; ++i)
        if (values[i]) rc = VgiMetadataSet(metadata, keys[i], values[i]);
    const char *flags[] = {"vgi.cache.no_store", "vgi.cache.revalidatable",
                           "vgi.cache.not_modified", "vgi.cache.partition_scope",
                           "vgi.cache.per_value"};
    int enabled[] = {c->no_store, c->revalidatable, c->not_modified, c->partition_scope,
                     c->per_value};
    for (int i = 0; !rc && i < 5; ++i)
        if (enabled[i]) rc = VgiMetadataSet(metadata, flags[i], "1");
    if (!rc && c->has_stale_while_revalidate)
        rc = number(metadata, "vgi.cache.stale_while_revalidate", c->stale_while_revalidate);
    if (!rc && c->has_stale_if_error)
        rc = number(metadata, "vgi.cache.stale_if_error", c->stale_if_error);
    return rc;
}
