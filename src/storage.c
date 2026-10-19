#define _POSIX_C_SOURCE 200809L
#include "vgi/storage.h"

#include <errno.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

struct VgiStorage {
    sqlite3 *db;
};
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
        free(store);
    }
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
        if (code == SQLITE_ROW)
            rc = ArrowBufferAppend(out, sqlite3_column_blob(statement, 0),
                                   sqlite3_column_bytes(statement, 0));
        else
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
    if (!rc) rc = bind_value(store, statement, 3, value, error);
    if (!rc) rc = sql_error(store, sqlite3_step(statement), error);
    sqlite3_finalize(statement);
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
    if (!rc) rc = bind_value(store, statement, 3, value, error);
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
            *id = sqlite3_column_int64(statement, 0);
            rc = ArrowBufferAppend(out, sqlite3_column_blob(statement, 1),
                                   sqlite3_column_bytes(statement, 1));
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
    return rc;
}
