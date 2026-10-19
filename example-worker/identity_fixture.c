#include "identity_fixture.h"

#include <errno.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "fixtures.h"
/* These hooks are fixture-only, opt-in, and intentionally trust test headers. */
static int authenticate(void *state, const struct VgiHttpRequest *request,
                        struct VgiHttpIdentity *out, struct ArrowError *error) {
    (void)error;
    struct ExampleIdentity *s = state;
    if (s->test_bearers && request->authorization && *request->authorization) {
        if (!strcmp(request->authorization, "Bearer vgi-test-alice"))
            out->principal = "alice";
        else if (!strcmp(request->authorization, "Bearer vgi-test-bob"))
            out->principal = "bob";
        else
            return EACCES;
        out->domain = "bearer";
        out->auth_time = (double)time(NULL);
        out->has_auth_time = 1;
        return 0;
    }
    if (s->principal_header) {
        const char *p = request->header(request->state, "X-Conformance-Principal");
        out->principal = p && *p ? p : NULL;
        out->domain = "conformance";
        const char *t = request->header(request->state, "X-Conformance-Auth-Time");
        if (t) {
            char *end = NULL;
            double value = strtod(t, &end);
            if (end != t && !*end && isfinite(value)) {
                out->auth_time = value;
                out->has_auth_time = 1;
            }
        }
        return 0;
    }
    return request->authorization && *request->authorization ? EACCES : 0;
}
static int unavailable(struct VgiRpcCall *call, int retry) {
    int rc = VgiRpcFail(call, "IdentityUnavailableError", "UNAVAILABLE", "identity_unavailable",
                        "conformance: authority unavailable");
    char details[128];
    snprintf(details, sizeof(details),
             "[{\"@type\":\"vgi_rpc.RetryInfo\",\"retry_delay_seconds\":%d}]", retry);
    VgiRpcSetErrorDetails(call, details);
    return rc;
}
static int resolve(void *state, struct VgiRpcCall *call, const char *token,
                   struct VgiTokenIdentity *out) {
    (void)state;
    if (!strcmp(token, "conformance-unavailable-token")) return unavailable(call, 5);
    if (!strcmp(token, "conformance-auth-unavailable-token")) return unavailable(call, 7);
    if (!strcmp(token, "conformance-unknown-token")) return ENOENT;
    out->principal = "subject@conformance.example";
    out->token_name = "conformance-subject";
    out->ttl_seconds = 300;
    if (!strcmp(token, "conformance-zero-ttl-token")) out->ttl_seconds = 0;
    if (!strcmp(token, "conformance-minimal-token")) out->token_name = "";
    if (!strcmp(token, "  conformance-padded-probe  ")) out->token_name = "conformance-padded";
    return 0;
}
static int mint(void *state, struct VgiRpcCall *call, const char *purpose,
                const char *const *scopes, size_t count, int64_t ttl, struct VgiIssuedGrant *out) {
    (void)state;
    (void)ttl;
    if (!strcmp(purpose, "conformance-auth-unavailable")) return unavailable(call, 7);
    if (!strcmp(purpose, "conformance-refused")) return ENOENT;
    size_t size = strlen(call->principal) + 32;
    for (size_t i = 0; i < count; ++i) size += strlen(scopes[i]) + 1;
    char *token = malloc(size);
    if (!token) return ENOMEM;
    snprintf(token, size, "conformance-grant-for:%s|", call->principal);
    for (size_t i = 0; i < count; ++i) {
        if (i) strcat(token, ",");
        strcat(token, scopes[i]);
    }
    out->token = token;
    out->grant_id = !strcmp(purpose, "conformance-minimal") ? "" : "conformance-grant-id";
    out->expires_at = 1893456000.0;
    return 0;
}
static void release(void *state, void *result, int is_grant) {
    (void)state;
    if (is_grant) free((void *)((struct VgiIssuedGrant *)result)->token);
}

int ExampleIdentityConfigure(struct VgiWorker *worker, struct ExampleIdentity *s, int *argc,
                             char **argv, struct ArrowError *error) {
    int identity = getenv("VGI_FIXTURE_IDENTITY") && !strcmp(getenv("VGI_FIXTURE_IDENTITY"), "1");
    s->test_bearers =
        getenv("VGI_FIXTURE_TEST_BEARERS") && !strcmp(getenv("VGI_FIXTURE_TEST_BEARERS"), "1");
    const char *allow = getenv("VGI_INTROSPECT_PRINCIPALS");
    int write = 1;
    for (int i = 1; i < *argc; ++i) {
        if (!strcmp(argv[i], "--conformance-identity"))
            identity = 1;
        else if (!strcmp(argv[i], "--conformance-principal-header"))
            s->principal_header = 1;
        else if (!strcmp(argv[i], "--introspect-principals")) {
            if (++i == *argc) return EINVAL;
            allow = argv[i];
        } else
            argv[write++] = argv[i];
    }
    *argc = write;
    argv[write] = NULL;
    if (identity) s->principal_header = 1;
    VgiHttpOptionsInit(&s->http);
    s->http.auth_state = s;
    if (s->principal_header || s->test_bearers) s->http.authenticate_request = authenticate;
    if (identity) {
        if (!allow || !*allow) {
            ArrowErrorSet(
                error,
                "conformance identity needs --introspect-principals or VGI_INTROSPECT_PRINCIPALS");
            return EINVAL;
        }
        s->principals = malloc(strlen(allow) + 1);
        if (!s->principals) return ENOMEM;
        strcpy(s->principals, allow);
        char *p = s->principals;
        while (p && *p) {
            if (s->identity.introspect_principal_count == 64) return EINVAL;
            s->allowlist[s->identity.introspect_principal_count++] = p;
            char *comma = strchr(p, ',');
            if (comma) *comma++ = 0;
            p = comma;
        }
        s->identity.introspect_principals = s->allowlist;
        s->identity.resolve_token = resolve;
        s->identity.mint_grant = mint;
        s->identity.release_result = release;
        s->http.identity = &s->identity;
    }
    VgiWorkerSetHttpOptions(worker, &s->http);
    return 0;
}
void ExampleIdentityReset(struct ExampleIdentity *s) { free(s->principals); }
