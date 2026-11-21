#ifndef VGI_HOSTED_INTERNAL_H
#define VGI_HOSTED_INTERNAL_H
#include "vgi/hosted.h"
#include "vgi_rpc/http.h"
#include "vgi_rpc/jwt.h"

/* Validate the hosted-mode environment and the caller's HTTP options. On
 * success *effective is requested with the Pass verifier installed, every
 * request authenticated and CORS open; free *verifier after serving. EINVAL
 * with a message describes the first conflict found. */
int VgiHostedPrepare(const struct VgiHttpOptions *requested, struct VgiHttpOptions *effective,
                     struct VgiJwtVerifier **verifier, struct ArrowError *error);
/* EINVAL with the hosted-mode prefix and reason. */
int VgiHostedRefuse(struct ArrowError *error, const char *reason);

#endif
