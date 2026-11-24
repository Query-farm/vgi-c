/* PROTOCOL_VERSIONS_V1 and the storage-backend refusal against the example
 * worker over stdio, driven by the vgi-rpc-c client.
 *
 *   vgi-protocol-versions-test WORKER_BINARY */
#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "vgi/worker.h"
#include "vgi_rpc/client.h"

#define CHECK(x)                                                                  \
    do {                                                                          \
        if (!(x)) {                                                               \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #x); \
            exit(1);                                                              \
        }                                                                         \
    } while (0)

static const char *worker;

static struct VgiClient *connect_worker(const char *version) {
    const char *argv[] = {worker, NULL};
    struct VgiClientOptions options;
    VgiClientOptionsInit(&options);
    options.transport = VGI_CLIENT_STDIO;
    options.argv = argv;
    options.protocol = VGI_PROTOCOL_NAME;
    options.protocol_version = version;
    struct VgiClient *client = NULL;
    struct ArrowError error = {{0}};
    CHECK(VgiClientConnect(&options, &client, &error) == 0);
    return client;
}
/* Any vgi.v3 method: the version gate runs before the method is resolved. */
static int call(struct VgiClient *client) {
    struct VgiAnnotatedBatch in = {0}, out = {0};
    struct ArrowError error = {{0}};
    struct ArrowSchema schema = {0};
    ArrowBufferInit(&in.metadata);
    CHECK(ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_STRUCT) == 0);
    CHECK(ArrowSchemaAllocateChildren(&schema, 0) == 0);
    CHECK(VgiBatchInit(&in.batch, &schema, &error) == 0);
    CHECK(VgiBatchFinishRow(&in.batch, &error) == 0);
    int rc = VgiClientUnary(client, "catalogs", &in, &out);
    VgiAnnotatedBatchReset(&in);
    VgiAnnotatedBatchReset(&out);
    ArrowSchemaRelease(&schema);
    return rc;
}
static const char *advertised(struct VgiClient *client) {
    struct VgiAnnotatedBatch listing = {0}, description = {0};
    CHECK(VgiClientDescribe(client, &listing, &description) == 0);
    VgiAnnotatedBatchReset(&listing);
    VgiAnnotatedBatchReset(&description);
    return VgiClientProtocolVersionsAdvertised(client);
}
/* Start the worker with one environment change; return its exit status and stderr. */
static int start(const char *name, const char *value, char *err, size_t size) {
    int pipes[2];
    CHECK(pipe(pipes) == 0);
    pid_t pid = fork();
    CHECK(pid >= 0);
    if (!pid) {
        setenv(name, value, 1);
        dup2(pipes[1], 2);
        close(pipes[0]);
        int null = open("/dev/null", O_RDONLY);
        dup2(null, 0);
        execl(worker, worker, "--http", "0", (char *)NULL);
        _exit(127);
    }
    close(pipes[1]);
    size_t used = 0;
    ssize_t n;
    while (used + 1 < size && (n = read(pipes[0], err + used, size - 1 - used)) > 0)
        used += (size_t)n;
    err[used] = 0;
    close(pipes[0]);
    int status = 0;
    waitpid(pid, &status, 0);
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

int main(int argc, char **argv) {
    CHECK(argc == 2);
    worker = argv[1];
    unsetenv(VGI_TEST_PROTOCOL_RANGE_ENV);

    /* The worker advertises the protocols it gates; reflection carries it. */
    struct VgiClient *client = connect_worker(VGI_PROTOCOL_VERSION);
    const char *text = advertised(client);
    CHECK(text && !strncmp(text, "vgi.v3=3.4-3.4", 14));
    /* 3.4 is inside the default window: whatever else happens, no refusal. */
    CHECK(call(client) == 0 ||
          strcmp(VgiClientLastError(client)->kind, VGI_RPC_PROTOCOL_VERSION_UNSUPPORTED));
    VgiClientFree(client);

    /* VGI_TEST_PROTOCOL_RANGE: a fake older worker. */
    setenv(VGI_TEST_PROTOCOL_RANGE_ENV, "3.3-3.3", 1);
    client = connect_worker(VGI_PROTOCOL_VERSION);
    CHECK(call(client) != 0);
    const struct VgiClientError *e = VgiClientLastError(client);
    CHECK(!strcmp(e->kind, VGI_RPC_PROTOCOL_VERSION_UNSUPPORTED));
    CHECK(!strcmp(e->code, "FAILED_PRECONDITION"));
    CHECK(strstr(e->message, "client speaks vgi.v3 3.4"));
    CHECK(strstr(e->message, "this server supports vgi.v3 3.3-3.3"));
    CHECK(strstr(e->message, "The server is too old"));
    /* Learned from the refusal: no common version, so the next call fails unsent. */
    CHECK(call(client) != 0);
    e = VgiClientLastError(client);
    CHECK(!strcmp(e->kind, VGI_RPC_PROTOCOL_VERSION_UNSUPPORTED));
    CHECK(strstr(e->message, "no common vgi.v3 version"));
    CHECK(strstr(e->message, "3.3-3.3") && strstr(e->message, "3.4-3.4"));
    VgiClientFree(client);
    /* name=MIN-MAX names the protocol explicitly. */
    setenv(VGI_TEST_PROTOCOL_RANGE_ENV, "vgi.v3=3.3-3.4", 1);
    client = connect_worker(VGI_PROTOCOL_VERSION);
    text = advertised(client);
    CHECK(text && !strncmp(text, "vgi.v3=3.3-3.4", 14));
    VgiClientFree(client);
    unsetenv(VGI_TEST_PROTOCOL_RANGE_ENV);

    char err[4096];
    /* A malformed switch refuses to start. */
    CHECK(start(VGI_TEST_PROTOCOL_RANGE_ENV, "three", err, sizeof(err)) != 0);
    CHECK(strstr(err, VGI_TEST_PROTOCOL_RANGE_ENV));

    /* Direct object storage contract: C is vendor-only in protocol 3.4. */
    CHECK(start("VGI_STORAGE_BACKEND", "s3", err, sizeof(err)) != 0);
    CHECK(strstr(err, "not supported by the C SDK") && strstr(err, "'s3'"));
    CHECK(start("VGI_STORAGE_BACKEND", "gcs", err, sizeof(err)) != 0);
    CHECK(strstr(err, "not supported by the C SDK") && strstr(err, "'gcs'"));
    CHECK(start("VGI_STORAGE_S3_BUCKET", "bucket", err, sizeof(err)) != 0);
    CHECK(strstr(err, "VGI_STORAGE_S3_BUCKET"));
    CHECK(start("VGI_STORAGE_BACKEND", "tape", err, sizeof(err)) != 0);
    CHECK(strstr(err, "must be vendor, s3, gcs or none"));
    puts("protocol versions and storage backend checks passed");
    return 0;
}
