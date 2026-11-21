#ifndef VGI_HOSTED_H
#define VGI_HOSTED_H
/* Orchard hosted mode (VGI_ORCHARD_HOSTED=1).
 *
 * A hosted worker serves VGI over HTTP only, to callers presenting a Pass: a
 * short-lived RFC 9068 access token (typ at+jwt, ES256) issued by the Orchard
 * Front Desk for one canonical Desk Address. VgiWorkerRun and
 * VgiWorkerServeHttp switch it on from the environment and fail closed:
 *
 * - VGI_JWT_ISSUER, VGI_JWT_AUDIENCE and VGI_JWT_JWKS_URI must each hold one
 *   non-empty value (no commas), and VGI_SIGNING_KEY at least 32 bytes;
 * - any other credential (VGI_BEARER_TOKENS, VGI_OPTIONAL_BEARER_TOKENS,
 *   sealed-grant keys, VGI_OAUTH_RESOURCE, VGI_PROXY_PROOF_MODE other than off,
 *   identity hooks, a custom authenticate callback) or a transport other than
 *   --http refuses to start;
 * - every request except HEAD /health and CORS preflights needs a valid Pass.
 *
 * Function code reads the Pass through VgiRpcCall: principal is "sub",
 * auth_domain is "orchard" and auth_claims_json is the verified claim set. */
#include "nanoarrow/nanoarrow.h"
#ifdef __cplusplus
extern "C" {
#endif

#define VGI_HOSTED_ENV "VGI_ORCHARD_HOSTED"

/* *enabled is 1 for 1/true/yes/on and 0 when unset or 0/false/no/off (any
 * case, surrounding spaces ignored). Any other value is a configuration error:
 * EINVAL with a message, so a typo cannot start a worker without
 * authentication. */
int VgiHostedModeEnabled(int *enabled, struct ArrowError *error);

#ifdef __cplusplus
}
#endif
#endif
