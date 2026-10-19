#ifndef VGI_STORAGE_H
#define VGI_STORAGE_H
#include "nanoarrow/nanoarrow.h"
#ifdef __cplusplus
extern "C" {
#endif
struct VgiStorage;
/* Each handle owns a connection. Use separate handles for concurrent calls.
 * NULL path selects a private per-user database under TMPDIR. */
int VgiStorageOpen(const char *path, struct VgiStorage **out, struct ArrowError *error);
void VgiStorageClose(struct VgiStorage *store);
int VgiStorageBegin(struct VgiStorage *store, struct ArrowError *error);
int VgiStorageCommit(struct VgiStorage *store, struct ArrowError *error);
void VgiStorageRollback(struct VgiStorage *store);
/* Get returns ENOENT for an absent key; a present empty value returns zero. */
int VgiStorageGet(struct VgiStorage *store, struct ArrowBufferView scope, const char *key,
                  struct ArrowBuffer *out, struct ArrowError *error);
int VgiStoragePut(struct VgiStorage *store, struct ArrowBufferView scope, const char *key,
                  struct ArrowBufferView value, struct ArrowError *error);
int VgiStorageDelete(struct VgiStorage *store, struct ArrowBufferView scope, const char *key,
                     struct ArrowError *error);
int VgiStorageAppend(struct VgiStorage *store, struct ArrowBufferView scope, const char *name,
                     struct ArrowBufferView value, int64_t *id, struct ArrowError *error);
int VgiStorageNext(struct VgiStorage *store, struct ArrowBufferView scope, const char *name,
                   int64_t after, int64_t *id, struct ArrowBuffer *out, struct ArrowError *error);
int VgiStoragePop(struct VgiStorage *store, struct ArrowBufferView scope, const char *name,
                  struct ArrowBuffer *out, struct ArrowError *error);
int VgiStorageClear(struct VgiStorage *store, struct ArrowBufferView scope,
                    struct ArrowError *error);
#ifdef __cplusplus
}
#endif
#endif
