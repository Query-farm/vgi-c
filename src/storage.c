#define _POSIX_C_SOURCE 200809L
#include "vgi/storage.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <sodium.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <unistd.h>

#include "storage_internal.h"
#include "vgi_rpc/vendor.h"

struct VgiStorage {
    sqlite3 *db;
    char *path;
    unsigned char key[32];
    int has_key;
    /* Value externalization (VGI_STORAGE_EXTERNALIZE with a blob vendor):
     * every value is enveloped, and values of at least `threshold` bytes
     * live in the vendor. */
    struct VgiVendor *vendor;
    int64_t threshold;
    int enveloped;
    char call_id[33];
    int64_t call_index;
};

/* ------------------------------------------------------------------------ */
/* Value envelope v1:                                                       */
/*   inline:  0x00 || raw                                                   */
/*   pointer: 0x01 || u64be(size) || sha256(raw) (32) || object_id (ASCII)  */
/* ------------------------------------------------------------------------ */

#define ENVELOPE_INLINE 0
#define ENVELOPE_POINTER 1
#define ENVELOPE_FIXED 41
#define DEFAULT_EXTERNALIZE_BYTES (1024 * 1024)

static pthread_once_t vendor_once = PTHREAD_ONCE_INIT;
static struct VgiVendor *storage_vendor;
static int storage_enveloped;
static int64_t storage_threshold = DEFAULT_EXTERNALIZE_BYTES;
static void storage_vendor_init(void) {
    const char *flag = getenv("VGI_STORAGE_EXTERNALIZE");
    if (!flag || !*flag || !strcmp(flag, "0") || !strcasecmp(flag, "false")) return;
    struct ArrowError error = {{0}};
    if (VgiVendorFromEnv(&storage_vendor, &error))
        fprintf(stderr, "vgi-c: storage externalization disabled: %s\n", error.message);
    if (!storage_vendor) return;
    storage_enveloped = 1;
    const char *bytes = getenv("VGI_STORAGE_EXTERNALIZE_BYTES");
    if (bytes && *bytes) {
        char *end = NULL;
        long long value = strtoll(bytes, &end, 10);
        if (end && !*end && value > 0) storage_threshold = value;
    }
}
static int object_id_valid(const char *id, size_t n) {
    if (n < 1 || n > 128) return 0;
    for (size_t i = 0; i < n; ++i)
        if (!isalnum((unsigned char)id[i]) && !strchr("._:-", id[i])) return 0;
    return 1;
}
/* The vendor scope of a storage scope (conventions in the blob-test catalog):
 * shard loc-anon (this store has no attach-keyed shards) and execution
 * s.<hex scope>, hashed past 128 characters. */
static void vendor_scope(struct ArrowBufferView scope, char execution[160]) {
    if (scope.size_bytes * 2 + 2 <= 128) {
        execution[0] = 's';
        execution[1] = '.';
        sodium_bin2hex(execution + 2, 158,
                       scope.size_bytes ? scope.data.as_uint8 : (const uint8_t *)"",
                       (size_t)scope.size_bytes);
        return;
    }
    unsigned char digest[32];
    crypto_hash_sha256(digest, scope.data.as_uint8, (unsigned long long)scope.size_bytes);
    memcpy(execution, "s.h", 3);
    sodium_bin2hex(execution + 3, 157, digest, sizeof(digest));
}
static int envelope_encode(struct VgiStorage *store, struct ArrowBufferView scope,
                           struct ArrowBufferView value, struct ArrowBuffer *out,
                           struct ArrowError *error) {
    if (!store->vendor || value.size_bytes < store->threshold) {
        const unsigned char tag = ENVELOPE_INLINE;
        int rc = ArrowBufferAppend(out, &tag, 1);
        if (!rc && value.size_bytes) rc = ArrowBufferAppend(out, value.data.data, value.size_bytes);
        return rc;
    }
    char execution[160], key[64];
    vendor_scope(scope, execution);
    if (!store->call_id[0]) {
        unsigned char id[16];
        randombytes_buf(id, sizeof(id));
        sodium_bin2hex(store->call_id, sizeof(store->call_id), id, sizeof(id));
    }
    snprintf(key, sizeof(key), "%s.%lld", store->call_id, (long long)store->call_index++);
    struct VgiVendorScope vendor = {"loc-anon", execution};
    char *object_id = NULL;
    struct ArrowError detail = {{0}};
    int rc = VgiVendorPutBlob(store->vendor, value, "fs", &vendor, key, &object_id, &detail);
    if (rc) {
        ArrowErrorSet(error, "storage blob could not be written: %s", detail.message);
        return rc;
    }
    unsigned char header[ENVELOPE_FIXED];
    header[0] = ENVELOPE_POINTER;
    for (int i = 0; i < 8; ++i)
        header[1 + i] = (unsigned char)((uint64_t)value.size_bytes >> (56 - 8 * i));
    crypto_hash_sha256(header + 9, value.data.as_uint8, (unsigned long long)value.size_bytes);
    rc = object_id_valid(object_id, strlen(object_id)) ? 0 : EINVAL;
    if (rc) ArrowErrorSet(error, "storage blob: the vendor returned a malformed object id");
    if (!rc) rc = ArrowBufferAppend(out, header, sizeof(header));
    if (!rc) rc = ArrowBufferAppend(out, object_id, (int64_t)strlen(object_id));
    free(object_id);
    return rc;
}
/* Decode an enveloped value into out (appending). */
static int envelope_decode(struct VgiStorage *store, struct ArrowBufferView stored,
                           struct ArrowBuffer *out, struct ArrowError *error) {
    if (stored.size_bytes < 1) {
        ArrowErrorSet(error, "storage value is empty (no envelope)");
        return EIO;
    }
    const uint8_t *p = stored.data.as_uint8;
    if (p[0] == ENVELOPE_INLINE)
        return stored.size_bytes > 1 ? ArrowBufferAppend(out, p + 1, stored.size_bytes - 1) : 0;
    if (p[0] != ENVELOPE_POINTER || stored.size_bytes <= ENVELOPE_FIXED ||
        !object_id_valid((const char *)p + ENVELOPE_FIXED,
                         (size_t)(stored.size_bytes - ENVELOPE_FIXED))) {
        ArrowErrorSet(error, "storage value has a malformed envelope");
        return EIO;
    }
    char id[129];
    size_t id_length = (size_t)(stored.size_bytes - ENVELOPE_FIXED);
    memcpy(id, p + ENVELOPE_FIXED, id_length);
    id[id_length] = 0;
    uint64_t size = 0;
    for (int i = 0; i < 8; ++i) size = (size << 8) | p[1 + i];
    if (!store->vendor) {
        ArrowErrorSet(error, "storage blob %s could not be read: no blob vendor is configured", id);
        return EIO;
    }
    struct ArrowBuffer blob;
    ArrowBufferInit(&blob);
    struct ArrowError detail = {{0}};
    /* Read one byte past the pointer's size, so a longer object is a size
     * mismatch rather than a fetch-cap error. */
    int rc = VgiVendorGetBlob(store->vendor, id, (int64_t)size + 1, &blob, &detail);
    if (rc == ENOENT)
        ArrowErrorSet(error,
                      "storage blob %s is missing: not found in the blob vendor (expired or "
                      "deleted?)",
                      id);
    else if (rc == EFBIG)
        ArrowErrorSet(error, "storage blob %s failed its integrity check: size mismatch", id);
    else if (rc)
        ArrowErrorSet(error, "storage blob %s could not be read: %s", id, detail.message);
    if (!rc && (uint64_t)blob.size_bytes != size) {
        ArrowErrorSet(error, "storage blob %s failed its integrity check: size mismatch", id);
        rc = EIO;
    }
    if (!rc) {
        unsigned char digest[32];
        crypto_hash_sha256(digest, blob.data, (unsigned long long)blob.size_bytes);
        if (sodium_memcmp(digest, p + 9, 32)) {
            ArrowErrorSet(error, "storage blob %s failed its integrity check: sha256 mismatch", id);
            rc = EIO;
        }
    }
    if (!rc && blob.size_bytes) rc = ArrowBufferAppend(out, blob.data, blob.size_bytes);
    ArrowBufferReset(&blob);
    return rc ? (rc == ENOENT || rc == EFBIG ? EIO : rc) : 0;
}
/* Wrap a value for the backend; *wire views either value or encoded. */
static int store_value(struct VgiStorage *store, struct ArrowBufferView scope,
                       struct ArrowBufferView value, struct ArrowBuffer *encoded,
                       struct ArrowBufferView *wire, struct ArrowError *error) {
    *wire = value;
    if (!store->enveloped) return 0;
    int rc = envelope_encode(store, scope, value, encoded, error);
    if (!rc) *wire = (struct ArrowBufferView){{encoded->data}, encoded->size_bytes};
    return rc;
}
/* Unwrap a value the backend appended to out at `base`. */
static int load_value(struct VgiStorage *store, struct ArrowBuffer *out, int64_t base,
                      struct ArrowError *error) {
    if (!store->enveloped) return 0;
    struct ArrowBuffer stored;
    ArrowBufferInit(&stored);
    int rc = ArrowBufferAppend(&stored, out->data + base, out->size_bytes - base);
    out->size_bytes = base;
    if (!rc)
        rc = envelope_decode(store, (struct ArrowBufferView){{stored.data}, stored.size_bytes}, out,
                             error);
    ArrowBufferReset(&stored);
    return rc;
}
/* Best-effort clean-up of a pointer's blob (queue pops). */
static void forget_value(struct VgiStorage *store, struct ArrowBufferView stored) {
    if (!store->vendor || stored.size_bytes <= ENVELOPE_FIXED ||
        stored.data.as_uint8[0] != ENVELOPE_POINTER)
        return;
    char id[129];
    size_t n = (size_t)(stored.size_bytes - ENVELOPE_FIXED);
    if (n > 128) return;
    memcpy(id, stored.data.as_uint8 + ENVELOPE_FIXED, n);
    id[n] = 0;
    const char *ids[] = {id};
    struct ArrowError ignored = {{0}};
    VgiVendorDeleteBlobs(store->vendor, ids, 1, NULL, &ignored);
}
static int sql_error(struct VgiStorage *store, int code, struct ArrowError *error) {
    if (code == SQLITE_OK || code == SQLITE_DONE) return 0;
    ArrowErrorSet(error, "Storage: %s", sqlite3_errmsg(store->db));
    return code == SQLITE_NOMEM                           ? ENOMEM
           : code == SQLITE_BUSY || code == SQLITE_LOCKED ? EBUSY
                                                          : EIO;
}
static int execute(struct VgiStorage *store, const char *sql, struct ArrowError *error) {
    return sql_error(store, sqlite3_exec(store->db, sql, NULL, NULL, NULL), error);
}
int VgiStorageOpen(const char *path, struct VgiStorage **out, struct ArrowError *error) {
    if (!out || *out) return EINVAL;
    char filename[4096];
    if (!path) {
        const char *temporary = getenv("TMPDIR");
        if (!temporary || !*temporary) temporary = "/tmp";
        int length = snprintf(filename, sizeof(filename), "%s/vgi-c-worker-%lu", temporary,
                              (unsigned long)getuid());
        if (length < 0 || (size_t)length + 16 >= sizeof(filename)) return ENAMETOOLONG;
        if (mkdir(filename, 0700) && errno != EEXIST) return errno;
        struct stat stat;
        if (lstat(filename, &stat) || !S_ISDIR(stat.st_mode) || stat.st_uid != getuid() ||
            (stat.st_mode & 0077)) {
            ArrowErrorSet(error, "Storage directory must be owned by this user with mode 0700");
            return EACCES;
        }
        strcat(filename, "/state.sqlite3");
        path = filename;
    }
    struct VgiStorage *store = calloc(1, sizeof(*store));
    if (!store) return ENOMEM;
    store->path = strdup(path);
    if (!store->path) {
        free(store);
        return ENOMEM;
    }
    pthread_once(&vendor_once, storage_vendor_init);
    store->enveloped = storage_enveloped;
    store->vendor = storage_vendor;
    store->threshold = storage_threshold;
    int rc = sql_error(
        store,
        sqlite3_open_v2(path, &store->db,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL),
        error);
    if (!rc) rc = sql_error(store, sqlite3_busy_timeout(store->db, 30000), error);
    if (!rc)
        rc = execute(
            store,
            "PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS kv(scope BLOB NOT NULL, key TEXT "
            "NOT NULL, value BLOB NOT NULL, PRIMARY KEY(scope,key)); CREATE TABLE IF NOT EXISTS "
            "log(id INTEGER PRIMARY KEY AUTOINCREMENT, scope BLOB NOT NULL, name TEXT NOT NULL, "
            "value BLOB NOT NULL); CREATE INDEX IF NOT EXISTS log_scope ON log(scope,name,id);",
            error);
    if (rc) {
        VgiStorageClose(store);
        return rc;
    }
    *out = store;
    return 0;
}
void VgiStorageClose(struct VgiStorage *store) {
    if (store) {
        if (store->db) sqlite3_close(store->db);
        sodium_memzero(store->key, sizeof(store->key));
        free(store->path);
        free(store);
    }
}
/* Read exactly 32 bytes of key material from path. */
static int read_key(const char *path, unsigned char key[32]) {
    int fd = open(path, O_RDONLY | O_NOFOLLOW);
    if (fd < 0) return errno;
    ssize_t n = read(fd, key, 32);
    close(fd);
    return n == 32 ? 0 : EIO;
}
static int load_key(struct VgiStorage *store, struct ArrowError *error) {
    if (store->has_key) return 0;
    const char *signing = getenv("VGI_SIGNING_KEY");
    if (signing && *signing) {
        static const char label[] = "vgi-c.storage.seal.v1";
        crypto_generichash(store->key, 32, (const unsigned char *)label, sizeof(label) - 1,
                           (const unsigned char *)signing,
                           strlen(signing) > 64 ? 64 : strlen(signing));
        store->has_key = 1;
        return 0;
    }
    size_t size = strlen(store->path) + 48;
    char *key_path = malloc(size), *temporary = malloc(size);
    if (!key_path || !temporary) {
        free(key_path);
        free(temporary);
        return ENOMEM;
    }
    snprintf(key_path, size, "%s.seal-key", store->path);
    int rc = read_key(key_path, store->key);
    if (rc == ENOENT) {
        /* Create it atomically: write a private temporary file, then link it
         * into place; a process that loses the race reads the winner's key. */
        unsigned char key[32];
        randombytes_buf(key, sizeof(key));
        snprintf(temporary, size, "%s.seal-key.%08x", store->path, randombytes_random());
        int fd = open(temporary, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0600);
        rc = fd < 0 ? errno : 0;
        if (!rc && (write(fd, key, sizeof(key)) != (ssize_t)sizeof(key) || fsync(fd))) rc = EIO;
        if (fd >= 0) close(fd);
        if (!rc && link(temporary, key_path) && errno != EEXIST) rc = errno;
        if (fd >= 0) unlink(temporary);
        sodium_memzero(key, sizeof(key));
        if (!rc) rc = read_key(key_path, store->key);
    }
    free(key_path);
    free(temporary);
    if (rc) {
        ArrowErrorSet(error, "Storage: cannot load the sealing key (%s)", strerror(rc));
        return rc;
    }
    store->has_key = 1;
    return 0;
}
static int seal_aad(const char *name, struct ArrowBufferView scope, struct ArrowBuffer *aad) {
    static const char label[] = "vgi-c.storage.sealed.v1";
    int rc = ArrowBufferAppend(aad, label, sizeof(label));
    if (!rc) rc = ArrowBufferAppend(aad, name, (int64_t)strlen(name) + 1);
    if (!rc && scope.size_bytes) rc = ArrowBufferAppend(aad, scope.data.data, scope.size_bytes);
    return rc;
}
int VgiStorageSeal(struct VgiStorage *store, const char *name, struct ArrowBufferView scope,
                   struct ArrowBufferView plaintext, struct ArrowBuffer *out,
                   struct ArrowError *error) {
    if (!store || !name || !out || plaintext.size_bytes < 0) return EINVAL;
    int rc = load_key(store, error);
    struct ArrowBuffer aad;
    ArrowBufferInit(&aad);
    if (!rc) rc = seal_aad(name, scope, &aad);
    int64_t base = out->size_bytes;
    if (!rc) rc = ArrowBufferResize(out, base + 25 + plaintext.size_bytes + 16, 0);
    if (!rc) {
        uint8_t *p = out->data + base;
        p[0] = 1;
        randombytes_buf(p + 1, 24);
        unsigned long long length = 0;
        crypto_aead_xchacha20poly1305_ietf_encrypt(
            p + 25, &length, plaintext.size_bytes ? plaintext.data.as_uint8 : (const uint8_t *)"",
            (unsigned long long)plaintext.size_bytes, aad.data, (unsigned long long)aad.size_bytes,
            NULL, p + 1, store->key);
    }
    ArrowBufferReset(&aad);
    return rc;
}
int VgiStorageOpenSealed(struct VgiStorage *store, const char *name, struct ArrowBufferView scope,
                         struct ArrowBufferView sealed, struct ArrowBuffer *out,
                         struct ArrowError *error) {
    if (!store || !name || !out) return EINVAL;
    if (sealed.size_bytes < 41 || sealed.data.as_uint8[0] != 1) {
        ArrowErrorSet(error, "Storage: %s is not a sealed value", name);
        return EBADMSG;
    }
    int rc = load_key(store, error);
    struct ArrowBuffer aad;
    ArrowBufferInit(&aad);
    if (!rc) rc = seal_aad(name, scope, &aad);
    int64_t base = out->size_bytes;
    if (!rc) rc = ArrowBufferResize(out, base + sealed.size_bytes - 41, 0);
    if (!rc) {
        unsigned long long length = 0;
        if (crypto_aead_xchacha20poly1305_ietf_decrypt(
                out->data + base, &length, NULL, sealed.data.as_uint8 + 25,
                (unsigned long long)sealed.size_bytes - 25, aad.data,
                (unsigned long long)aad.size_bytes, sealed.data.as_uint8 + 1, store->key)) {
            ArrowErrorSet(error, "Storage: %s failed its integrity check", name);
            rc = EBADMSG;
        }
    }
    if (rc) out->size_bytes = base;
    ArrowBufferReset(&aad);
    return rc;
}
int VgiStorageBegin(struct VgiStorage *store, struct ArrowError *error) {
    return execute(store, "BEGIN IMMEDIATE", error);
}
int VgiStorageCommit(struct VgiStorage *store, struct ArrowError *error) {
    return execute(store, "COMMIT", error);
}
void VgiStorageRollback(struct VgiStorage *store) {
    if (store) sqlite3_exec(store->db, "ROLLBACK", NULL, NULL, NULL);
}
static int prepare(struct VgiStorage *store, const char *sql, struct ArrowBufferView scope,
                   const char *key, sqlite3_stmt **statement, struct ArrowError *error) {
    int rc = sqlite3_prepare_v2(store->db, sql, -1, statement, NULL);
    if (rc == SQLITE_OK)
        rc = sqlite3_bind_blob64(*statement, 1, scope.size_bytes ? scope.data.data : "",
                                 (sqlite3_uint64)scope.size_bytes, SQLITE_TRANSIENT);
    if (rc == SQLITE_OK && key) rc = sqlite3_bind_text(*statement, 2, key, -1, SQLITE_TRANSIENT);
    return sql_error(store, rc, error);
}
static int bind_value(struct VgiStorage *store, sqlite3_stmt *statement, int column,
                      struct ArrowBufferView value, struct ArrowError *error) {
    return sql_error(store,
                     sqlite3_bind_blob64(statement, column, value.size_bytes ? value.data.data : "",
                                         (sqlite3_uint64)value.size_bytes, SQLITE_TRANSIENT),
                     error);
}
int VgiStorageGet(struct VgiStorage *store, struct ArrowBufferView scope, const char *key,
                  struct ArrowBuffer *out, struct ArrowError *error) {
    sqlite3_stmt *statement = NULL;
    int rc = prepare(store, "SELECT value FROM kv WHERE scope=? AND key=?", scope, key, &statement,
                     error);
    if (!rc) {
        int code = sqlite3_step(statement);
        int64_t base = out->size_bytes;
        if (code == SQLITE_ROW) {
            rc = ArrowBufferAppend(out, sqlite3_column_blob(statement, 0),
                                   sqlite3_column_bytes(statement, 0));
            if (!rc) rc = load_value(store, out, base, error);
        } else
            rc = code == SQLITE_DONE ? ENOENT : sql_error(store, code, error);
    }
    sqlite3_finalize(statement);
    return rc;
}
int VgiStoragePut(struct VgiStorage *store, struct ArrowBufferView scope, const char *key,
                  struct ArrowBufferView value, struct ArrowError *error) {
    sqlite3_stmt *statement = NULL;
    int rc = prepare(store,
                     "INSERT INTO kv(scope,key,value) VALUES(?,?,?) ON CONFLICT(scope,key) DO "
                     "UPDATE SET value=excluded.value",
                     scope, key, &statement, error);
    struct ArrowBuffer encoded;
    ArrowBufferInit(&encoded);
    struct ArrowBufferView wire = value;
    if (!rc) rc = store_value(store, scope, value, &encoded, &wire, error);
    if (!rc) rc = bind_value(store, statement, 3, wire, error);
    if (!rc) rc = sql_error(store, sqlite3_step(statement), error);
    sqlite3_finalize(statement);
    ArrowBufferReset(&encoded);
    return rc;
}
int VgiStorageDelete(struct VgiStorage *store, struct ArrowBufferView scope, const char *key,
                     struct ArrowError *error) {
    sqlite3_stmt *statement = NULL;
    int rc =
        prepare(store, "DELETE FROM kv WHERE scope=? AND key=?", scope, key, &statement, error);
    if (!rc) rc = sql_error(store, sqlite3_step(statement), error);
    sqlite3_finalize(statement);
    return rc;
}
int VgiStorageAppend(struct VgiStorage *store, struct ArrowBufferView scope, const char *name,
                     struct ArrowBufferView value, int64_t *id, struct ArrowError *error) {
    sqlite3_stmt *statement = NULL;
    int rc = prepare(store, "INSERT INTO log(scope,name,value) VALUES(?,?,?)", scope, name,
                     &statement, error);
    struct ArrowBuffer encoded;
    ArrowBufferInit(&encoded);
    struct ArrowBufferView wire = value;
    if (!rc) rc = store_value(store, scope, value, &encoded, &wire, error);
    if (!rc) rc = bind_value(store, statement, 3, wire, error);
    ArrowBufferReset(&encoded);
    if (!rc) rc = sql_error(store, sqlite3_step(statement), error);
    if (!rc && id) *id = sqlite3_last_insert_rowid(store->db);
    sqlite3_finalize(statement);
    return rc;
}
int VgiStorageNext(struct VgiStorage *store, struct ArrowBufferView scope, const char *name,
                   int64_t after, int64_t *id, struct ArrowBuffer *out, struct ArrowError *error) {
    sqlite3_stmt *statement = NULL;
    int rc = prepare(
        store, "SELECT id,value FROM log WHERE scope=? AND name=? AND id>? ORDER BY id LIMIT 1",
        scope, name, &statement, error);
    if (!rc) rc = sql_error(store, sqlite3_bind_int64(statement, 3, after), error);
    if (!rc) {
        int code = sqlite3_step(statement);
        if (code == SQLITE_ROW) {
            int64_t base = out->size_bytes;
            *id = sqlite3_column_int64(statement, 0);
            rc = ArrowBufferAppend(out, sqlite3_column_blob(statement, 1),
                                   sqlite3_column_bytes(statement, 1));
            if (!rc) rc = load_value(store, out, base, error);
        } else
            rc = code == SQLITE_DONE ? ENOENT : sql_error(store, code, error);
    }
    sqlite3_finalize(statement);
    return rc;
}
int VgiStoragePop(struct VgiStorage *store, struct ArrowBufferView scope, const char *name,
                  struct ArrowBuffer *out, struct ArrowError *error) {
    int own_transaction = sqlite3_get_autocommit(store->db),
        rc = own_transaction ? VgiStorageBegin(store, error) : 0;
    int64_t id = 0;
    if (!rc) rc = VgiStorageNext(store, scope, name, 0, &id, out, error);
    if (!rc && store->vendor) {
        sqlite3_stmt *raw = NULL;
        if (sqlite3_prepare_v2(store->db, "SELECT value FROM log WHERE id=?", -1, &raw, NULL) ==
                SQLITE_OK &&
            sqlite3_bind_int64(raw, 1, id) == SQLITE_OK && sqlite3_step(raw) == SQLITE_ROW)
            forget_value(store, (struct ArrowBufferView){{sqlite3_column_blob(raw, 0)},
                                                         sqlite3_column_bytes(raw, 0)});
        sqlite3_finalize(raw);
    }
    if (!rc) {
        sqlite3_stmt *statement = NULL;
        rc = sql_error(
            store,
            sqlite3_prepare_v2(store->db, "DELETE FROM log WHERE id=?", -1, &statement, NULL),
            error);
        if (!rc) rc = sql_error(store, sqlite3_bind_int64(statement, 1, id), error);
        if (!rc) rc = sql_error(store, sqlite3_step(statement), error);
        sqlite3_finalize(statement);
    }
    if (own_transaction) {
        if (rc)
            VgiStorageRollback(store);
        else
            rc = VgiStorageCommit(store, error);
    }
    return rc;
}
int VgiStorageClear(struct VgiStorage *store, struct ArrowBufferView scope,
                    struct ArrowError *error) {
    const char *sql[] = {"DELETE FROM kv WHERE scope=?", "DELETE FROM log WHERE scope=?"};
    int own_transaction = sqlite3_get_autocommit(store->db),
        rc = own_transaction ? VgiStorageBegin(store, error) : 0;
    for (int i = 0; !rc && i < 2; ++i) {
        sqlite3_stmt *statement = NULL;
        rc = prepare(store, sql[i], scope, NULL, &statement, error);
        if (!rc) rc = sql_error(store, sqlite3_step(statement), error);
        sqlite3_finalize(statement);
    }
    if (own_transaction) {
        if (rc)
            VgiStorageRollback(store);
        else
            rc = VgiStorageCommit(store, error);
    }
    if (!rc && store->vendor) {
        char execution[160];
        vendor_scope(scope, execution);
        struct VgiVendorScope vendor = {"loc-anon", execution};
        struct ArrowError ignored = {{0}};
        VgiVendorDeleteScope(store->vendor, &vendor, NULL, &ignored);
    }
    return rc;
}
