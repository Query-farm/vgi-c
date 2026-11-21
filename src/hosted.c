/* Orchard hosted mode: see <vgi/hosted.h>. The design is the extension
 * repository's docs/location-delegation.md ("The Field Worker: SDK hosted
 * mode"); vgi-python's vgi/hosted.py is the reference implementation. */
#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "hosted_internal.h"

#define HOSTED_CHALLENGE "Bearer error=\"invalid_token\""
#define REQUIRED_TYP "at+jwt"
#define LEEWAY_SECONDS 60
#define JWKS_MAX_AGE_SECONDS 600.0
#define JWKS_REFRESH_MIN_INTERVAL_SECONDS 60.0
#define JWKS_FETCH_TIMEOUT_SECONDS 10.0
#define MIN_SIGNING_KEY_BYTES 32

int VgiHostedRefuse(struct ArrowError *error, const char *reason) {
    ArrowErrorSet(error, VGI_HOSTED_ENV "=1 (Orchard hosted mode): %s", reason);
    return EINVAL;
}
/* Copy value without surrounding whitespace into out. */
static void trimmed(const char *value, char *out, size_t size) {
    while (value && isspace((unsigned char)*value)) ++value;
    size_t n = value ? strlen(value) : 0;
    while (n && isspace((unsigned char)value[n - 1])) --n;
    if (n >= size) n = size - 1;
    if (n) memcpy(out, value, n);
    out[n] = 0;
}
int VgiHostedModeEnabled(int *enabled, struct ArrowError *error) {
    *enabled = 0;
    const char *raw = getenv(VGI_HOSTED_ENV);
    if (!raw) return 0;
    char value[16];
    trimmed(raw, value, sizeof(value));
    static const char *on[] = {"1", "true", "yes", "on"}, *off[] = {"", "0", "false", "no", "off"};
    if (strlen(raw) < sizeof(value)) {
        for (size_t i = 0; i < sizeof(on) / sizeof(*on); ++i)
            if (!strcasecmp(value, on[i])) {
                *enabled = 1;
                return 0;
            }
        for (size_t i = 0; i < sizeof(off) / sizeof(*off); ++i)
            if (!strcasecmp(value, off[i])) return 0;
    }
    ArrowErrorSet(error, VGI_HOSTED_ENV " must be 1 or 0 (or true/false), got '%.64s'", raw);
    return EINVAL;
}
static int set(const char *name) {
    const char *value = getenv(name);
    while (value && isspace((unsigned char)*value)) ++value;
    return value && *value;
}
/* One required, single-valued variable. Unset counts as empty. */
static int single_value(const char *name, char *out, size_t size, struct ArrowError *error) {
    const char *raw = getenv(name);
    char reason[256];
    if (raw && strlen(raw) >= size) {
        snprintf(reason, sizeof(reason), "%s is too long.", name);
        return VgiHostedRefuse(error, reason);
    }
    trimmed(raw, out, size);
    if (!*out) {
        snprintf(reason, sizeof(reason), "%s must be set (unset and empty are both errors).", name);
        return VgiHostedRefuse(error, reason);
    }
    if (strchr(out, ',')) {
        snprintf(reason, sizeof(reason),
                 "%s must be exactly one value; got a comma-separated list. A Pass is issued for "
                 "one canonical Desk Address and one issuer.",
                 name);
        return VgiHostedRefuse(error, reason);
    }
    return 0;
}
static int check_environment(struct ArrowError *error) {
    static const struct {
        const char *name, *what;
    } conflicts[] = {
        {"VGI_BEARER_TOKENS", "static bearer tokens"},
        {"VGI_OPTIONAL_BEARER_TOKENS", "optional static bearer tokens"},
        {"VGI_RPC_GRANT_KEYS", "sealed-grant keys"},
        {"VGI_OAUTH_RESOURCE",
         "OAuth resource metadata (a Field Worker never advertises resource_metadata)"},
    };
    char reason[512];
    for (size_t i = 0; i < sizeof(conflicts) / sizeof(*conflicts); ++i)
        if (set(conflicts[i].name)) {
            snprintf(reason, sizeof(reason),
                     "%s is set (%s); the only credential a hosted worker accepts is a Pass.",
                     conflicts[i].name, conflicts[i].what);
            return VgiHostedRefuse(error, reason);
        }
    char mode[16] = "off";
    if (set("VGI_PROXY_PROOF_MODE")) trimmed(getenv("VGI_PROXY_PROOF_MODE"), mode, sizeof(mode));
    if (strcasecmp(mode, "off"))
        return VgiHostedRefuse(
            error, "VGI_PROXY_PROOF_MODE must be off; hosted workers authenticate by Pass only.");
    return 0;
}
static int check_options(const struct VgiHttpOptions *o, struct ArrowError *error) {
    const struct VgiIdentityOptions *identity = o->identity;
    if (o->grant_keys || (identity && identity->grant_keys))
        return VgiHostedRefuse(
            error, "sealed-grant keys (--grant-key / VGI_RPC_GRANT_KEYS) are configured.");
    if (identity && identity->resolve_token)
        return VgiHostedRefuse(error, "the worker overrides resolve_token (VgiIdentityOptions).");
    if (identity && identity->mint_grant)
        return VgiHostedRefuse(error, "the worker overrides mint_grant (VgiIdentityOptions).");
    if (o->authenticate || o->authenticate_request || o->authenticate_early || o->release_identity)
        return VgiHostedRefuse(
            error,
            "a custom authenticate callback is configured; hosted workers verify Passes only.");
    if ((o->bearer_tokens && *o->bearer_tokens) ||
        (o->optional_bearer_tokens && *o->optional_bearer_tokens))
        return VgiHostedRefuse(error,
                               "static bearer tokens are configured; the only credential a hosted "
                               "worker accepts is a Pass.");
    if (o->proof.mode != VGI_PROOF_OFF)
        return VgiHostedRefuse(
            error, "a proxy proof is configured; hosted workers authenticate by Pass only.");
    return 0;
}
/* The key seals catalog opaque data and HTTP state tokens. A Field Worker's
 * machines stop and start, and a deployment can run on several of them: a key
 * minted per process makes every restart reject the sessions DuckDB holds. */
static int check_signing_key(const struct VgiHttpOptions *o, struct ArrowError *error) {
    if (!o->signing_key || !*o->signing_key || set("VGI_SIGNING_KEY_MINTED"))
        return VgiHostedRefuse(
            error,
            "VGI_SIGNING_KEY is not set; hosted workers need a stable key shared by every machine "
            "of the deployment, or every restart breaks attached sessions.");
    if (strlen(o->signing_key) < MIN_SIGNING_KEY_BYTES)
        return VgiHostedRefuse(error, "VGI_SIGNING_KEY must be at least 32 bytes.");
    return 0;
}
int VgiHostedPrepare(const struct VgiHttpOptions *requested, struct VgiHttpOptions *effective,
                     struct VgiJwtVerifier **verifier, struct ArrowError *error) {
    *verifier = NULL;
    char issuer[2048], audience[2048], jwks_uri[2048], algorithms[512] = "";
    int rc = single_value("VGI_JWT_ISSUER", issuer, sizeof(issuer), error);
    if (!rc) rc = single_value("VGI_JWT_AUDIENCE", audience, sizeof(audience), error);
    if (!rc) rc = single_value("VGI_JWT_JWKS_URI", jwks_uri, sizeof(jwks_uri), error);
    if (rc) return rc;
    const char *raw_algorithms = getenv("VGI_JWT_ALGORITHMS");
    if (raw_algorithms && strlen(raw_algorithms) >= sizeof(algorithms))
        return VgiHostedRefuse(error, "VGI_JWT_ALGORITHMS is too long.");
    trimmed(raw_algorithms, algorithms, sizeof(algorithms));
    for (const char *p = algorithms; *p;) {
        while (*p == ',' || isspace((unsigned char)*p)) ++p;
        if ((!strncasecmp(p, "none", 4) &&
             (!p[4] || p[4] == ',' || isspace((unsigned char)p[4]))) ||
            !strncasecmp(p, "hs", 2)) {
            char reason[768];
            snprintf(
                reason, sizeof(reason),
                "VGI_JWT_ALGORITHMS='%s' is not allowed; Passes are asymmetric (default ES256), "
                "never 'none' or HS*.",
                algorithms);
            return VgiHostedRefuse(error, reason);
        }
        while (*p && *p != ',') ++p;
    }
    rc = check_environment(error);
    if (!rc) rc = check_options(requested, error);
    if (!rc) rc = check_signing_key(requested, error);
    if (rc) return rc;
    struct VgiJwtOptions options;
    VgiJwtOptionsInit(&options);
    options.issuer = issuer;
    options.audience = audience;
    options.jwks_uri = jwks_uri;
    options.algorithms = *algorithms ? algorithms : "ES256";
    options.required_typ = REQUIRED_TYP;
    options.require_exp = 1;
    options.single_audience = 1;
    options.leeway_seconds = LEEWAY_SECONDS;
    options.jwks_max_age_seconds = JWKS_MAX_AGE_SECONDS;
    options.jwks_refresh_min_interval_seconds = JWKS_REFRESH_MIN_INTERVAL_SECONDS;
    options.jwks_fetch_timeout_seconds = JWKS_FETCH_TIMEOUT_SECONDS;
    options.domain = "orchard";
    struct ArrowError detail = {{0}};
    rc = VgiJwtVerifierNew(&options, verifier, &detail);
    if (rc) {
        char reason[1024];
        snprintf(reason, sizeof(reason), "cannot verify Passes: %.900s", detail.message);
        return rc == EINVAL ? VgiHostedRefuse(error, reason) : rc;
    }
    *effective = *requested;
    VgiJwtInstall(*verifier, effective);
    effective->require_authentication = 1;
    effective->auth_challenge = HOSTED_CHALLENGE;
    /* The only credential is a bearer Pass, so CORS can be open. */
    effective->cors_origin = "*";
    return 0;
}
