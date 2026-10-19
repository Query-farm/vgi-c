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
    struct VgiTelemetry *telemetry = NULL;
    if (!rc && (getenv("VGI_RPC_OTEL_ENDPOINT") || getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))) {
        struct VgiTelemetryOptions options;
        VgiTelemetryOptionsInit(&options);
        if (getenv("VGI_RPC_OTEL_ENDPOINT")) options.endpoint = getenv("VGI_RPC_OTEL_ENDPOINT");
        rc = VgiTelemetryNew(&options, &telemetry, &error);
        if (!rc) rc = VgiWorkerSetTelemetry(worker, telemetry);
    }
    if (!rc) rc = VgiWorkerRun(worker, argc, argv, &error);
    if (rc) fprintf(stderr, "vgi-c: %s (error %d)\n", error.message, rc);
    VgiWorkerFree(worker);
    VgiTelemetryFree(telemetry);
    ExampleIdentityReset(&identity);
    return rc == EINVAL ? 2 : rc ? 1 : 0;
}
