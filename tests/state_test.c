#define _DARWIN_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "vgi/storage.h"
#define CHECK(x)                                                    \
    do {                                                            \
        if (!(x)) {                                                 \
            fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x); \
            return 1;                                               \
        }                                                           \
    } while (0)
static struct ArrowBufferView view(const void *data, int64_t size) {
    return (struct ArrowBufferView){{data}, size};
}
int main(void) {
    char directory[] = "/tmp/vgi-c-state-XXXXXX";
    CHECK(mkdtemp(directory));
    char path[512];
    snprintf(path, sizeof(path), "%s/state.sqlite3", directory);
    struct VgiStorage *storage = NULL;
    struct ArrowError error = {{0}};
    struct ArrowBuffer value;
    ArrowBufferInit(&value);
    const unsigned char key[] = {0, 's', 0, 'c', 0};
    struct ArrowBufferView scope = view(key, sizeof(key));
    CHECK(!VgiStorageOpen(path, &storage, &error));
    CHECK(VgiStorageGet(storage, scope, "empty", &value, &error) == ENOENT);
    CHECK(!VgiStoragePut(storage, scope, "empty", view(NULL, 0), &error));
    CHECK(!VgiStorageGet(storage, scope, "empty", &value, &error) && value.size_bytes == 0);
    CHECK(!VgiStorageBegin(storage, &error));
    CHECK(!VgiStoragePut(storage, scope, "rolled-back", view("no", 2), &error));
    VgiStorageRollback(storage);
    CHECK(VgiStorageGet(storage, scope, "rolled-back", &value, &error) == ENOENT);
    CHECK(!VgiStorageBegin(storage, &error));
    CHECK(!VgiStoragePut(storage, scope, "committed", view("yes", 3), &error));
    CHECK(!VgiStorageCommit(storage, &error));
    VgiStorageClose(storage);
    storage = NULL;
    /* Concurrent producers, each with its own connection and process. */
    pid_t children[4];
    for (int child = 0; child < 4; ++child) {
        children[child] = fork();
        CHECK(children[child] >= 0);
        if (!children[child]) {
            struct VgiStorage *s = NULL;
            if (VgiStorageOpen(path, &s, &error)) _exit(2);
            for (int item = 0; item < 50; ++item) {
                unsigned char bytes[2] = {(unsigned char)child, (unsigned char)item};
                if (VgiStorageAppend(s, scope, "queue", view(bytes, 2), NULL, &error)) _exit(3);
            }
            VgiStorageClose(s);
            _exit(0);
        }
    }
    for (int child = 0; child < 4; ++child) {
        int status;
        CHECK(waitpid(children[child], &status, 0) == children[child]);
        CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    }
    CHECK(!VgiStorageOpen(path, &storage, &error));
    unsigned char seen[4][50] = {{0}};
    for (int i = 0; i < 200; ++i) {
        value.size_bytes = 0;
        CHECK(!VgiStoragePop(storage, scope, "queue", &value, &error));
        CHECK(value.size_bytes == 2 && value.data[0] < 4 && value.data[1] < 50);
        CHECK(!seen[value.data[0]][value.data[1]]++);
    }
    CHECK(VgiStoragePop(storage, scope, "queue", &value, &error) == ENOENT);
    value.size_bytes = 0;
    CHECK(!VgiStorageGet(storage, scope, "committed", &value, &error));
    CHECK(value.size_bytes == 3 && !memcmp(value.data, "yes", 3));
    CHECK(!VgiStorageClear(storage, scope, &error));
    CHECK(VgiStorageGet(storage, scope, "empty", &value, &error) == ENOENT);
    CHECK(VgiStorageGet(storage, scope, "committed", &value, &error) == ENOENT);
    VgiStorageClose(storage);
    ArrowBufferReset(&value);
    CHECK(!unlink(path));
    /* SQLite on macOS may retain empty WAL/SHM files after the last close. */
    char sidecar[520];
    snprintf(sidecar, sizeof(sidecar), "%s-wal", path);
    CHECK(!unlink(sidecar) || errno == ENOENT);
    snprintf(sidecar, sizeof(sidecar), "%s-shm", path);
    CHECK(!unlink(sidecar) || errno == ENOENT);
    CHECK(!rmdir(directory));
    puts("Storage transactions and cross-process queues passed");
    return 0;
}
