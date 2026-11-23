#ifndef VGI_STORAGE_INTERNAL_H
#define VGI_STORAGE_INTERNAL_H
#include "vgi/storage.h"
/* Framework values that carry secrets (the table-buffering bind call) are
 * sealed before they reach the database: XChaCha20-Poly1305 under the storage
 * key, bound to the value's name and scope. The key is derived from
 * VGI_SIGNING_KEY when set (so every replica sharing it can open the value);
 * otherwise it is a random key kept next to the database (mode 0600, created
 * atomically), shared by the worker processes that share the database. A
 * copy of the database alone never reveals the plaintext. Format:
 * 0x01 || nonce (24) || ciphertext. */
int VgiStorageSeal(struct VgiStorage *, const char *name, struct ArrowBufferView scope,
                   struct ArrowBufferView plaintext, struct ArrowBuffer *out, struct ArrowError *);
/* EBADMSG for a value this key did not seal for this name and scope. */
int VgiStorageOpenSealed(struct VgiStorage *, const char *name, struct ArrowBufferView scope,
                         struct ArrowBufferView sealed, struct ArrowBuffer *out,
                         struct ArrowError *);
#endif
