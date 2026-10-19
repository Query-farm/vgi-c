#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "fixtures.h"
#include "identity_fixture.h"

int main(int argc, char **argv) {
    struct ArrowError error = {{0}};
    struct VgiWorker *worker = VgiWorkerNew(getenv("VGI_WORKER_CATALOG_NAME"));
    int rc = worker ? ExampleRegister(worker, &error) : ENOMEM;
    if (!rc) rc = ExampleSecondaryRegister(worker, &error);
    struct ExampleIdentity identity = {0};
    if (!rc) rc = ExampleIdentityConfigure(worker, &identity, &argc, argv, &error);
    if (!rc) rc = VgiWorkerRun(worker, argc, argv, &error);
    if (rc) fprintf(stderr, "vgi-c: %s (error %d)\n", error.message, rc);
    VgiWorkerFree(worker);
    ExampleIdentityReset(&identity);
    return rc == EINVAL ? 2 : rc ? 1 : 0;
}
