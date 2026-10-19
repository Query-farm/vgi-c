#ifndef VGI_EXAMPLE_IDENTITY_H
#define VGI_EXAMPLE_IDENTITY_H
#include "vgi/http.h"
struct VgiWorker;
struct ExampleIdentity {
    int principal_header, test_bearers;
    struct VgiIdentityOptions identity;
    struct VgiHttpOptions http;
    char *principals;
    const char *allowlist[64];
};
int ExampleIdentityConfigure(struct VgiWorker *, struct ExampleIdentity *, int *, char **,
                             struct ArrowError *);
void ExampleIdentityReset(struct ExampleIdentity *);
#endif
