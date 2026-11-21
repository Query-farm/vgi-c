#ifndef VGI_EXAMPLE_HOSTED_FIXTURE_H
#define VGI_EXAMPLE_HOSTED_FIXTURE_H
#include "vgi/worker.h"
/* Register pass_claims (Orchard hosted mode only). */
int ExampleHostedRegister(struct VgiWorker *, struct ArrowError *);
#endif
