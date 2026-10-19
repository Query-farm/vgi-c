#include "vgi/credentials.h"

#include <errno.h>
#include <json-c/json.h>
#include <sodium.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x)                                                                  \
    do {                                                                          \
        if (!(x)) {                                                               \
            fprintf(stderr, "%s:%d: %s (%s)\n", __FILE__, __LINE__, #x, current); \
            exit(1);                                                              \
        }                                                                         \
    } while (0)
static const char *current = "setup";
static struct json_object *get(struct json_object *a, const char *key) {
    return json_object_object_get(a, key);
}
static struct json_object *fallback(struct json_object *a, struct json_object *b, const char *key) {
    struct json_object *v = get(a, key);
    return v ? v : get(b, key);
}
static const char *str(struct json_object *a, const char *key) {
    return json_object_get_string(get(a, key));
}
static int64_t num(struct json_object *a, const char *key) {
    return json_object_get_int64(get(a, key));
}
static size_t b64(const char *s, unsigned char *out, size_t cap) {
    size_t n = 0;
    CHECK(
        !sodium_base642bin(out, cap, s, strlen(s), NULL, &n, NULL, sodium_base64_VARIANT_ORIGINAL));
    return n;
}
static void ticket_vectors(struct json_object *root) {
    struct json_object *defaults = get(root, "defaults");
    const char *sets[] = {"mint", "accept", "reject"};
    for (int s = 0; s < 3; ++s) {
        struct json_object *list = get(root, sets[s]);
        for (size_t i = 0; i < json_object_array_length(list); ++i) {
            struct json_object *v = json_object_array_get_idx(list, i);
            current = str(v, "name");
            unsigned char key[32], raw[512], nonce[24];
            size_t n = b64(json_object_get_string(fallback(v, defaults, "signing_key_b64")), raw,
                           sizeof(raw));
            VgiSigningKey(raw, n, key);
            struct VgiAttachTicket c = {0};
            ArrowBufferInit(&c.options);
            if (!s) {
                c.issued_at = num(v, "issued_at");
                c.expires_at = num(v, "expires_at");
                c.ticket_id = (char *)str(v, "ticket_id");
                c.catalog_name = (char *)str(v, "catalog_name");
                c.data_version_spec = (char *)str(v, "data_version_spec");
                c.implementation_version = (char *)str(v, "implementation_version");
                const char *encoded = str(v, "options_ipc_b64");
                CHECK(!ArrowBufferResize(&c.options, (int64_t)strlen(encoded), 0));
                c.options.size_bytes =
                    (int64_t)b64(encoded, c.options.data, (size_t)c.options.size_bytes);
                CHECK(!sodium_hex2bin(nonce, 24, str(v, "nonce_hex"), 48, NULL, NULL, NULL));
                struct ArrowBuffer token;
                ArrowBufferInit(&token);
                CHECK(!VgiAttachTicketMint(key, str(v, "principal"), &c, nonce, &token));
                CHECK(!strcmp((char *)token.data, str(v, "token")));
                ArrowBufferReset(&token);
                ArrowBufferReset(&c.options);
            } else {
                double now = json_object_get_double(fallback(v, defaults, "now"));
                int rc = VgiAttachTicketOpen(key, str(v, "principal"), str(v, "token"), now, &c);
                if (s == 2)
                    CHECK(rc == (!strcmp(str(v, "error_kind"), "attach_ticket_expired") ? ESTALE
                                                                                        : EINVAL));
                else {
                    CHECK(!rc);
                    struct json_object *expected = get(v, "claims");
                    CHECK(c.issued_at == num(expected, "issued_at"));
                    CHECK(c.expires_at == num(expected, "expires_at"));
                    CHECK(!strcmp(c.catalog_name, str(expected, "catalog_name")));
                    CHECK(!strcmp(c.ticket_id, str(expected, "ticket_id")));
                }
                VgiAttachTicketReset(&c);
            }
        }
    }
}
static void grant_vectors(struct json_object *root) {
    struct json_object *defaults = get(root, "defaults");
    const char *sets[] = {"mint", "accept", "reject"};
    for (int s = 0; s < 3; ++s) {
        struct json_object *list = get(root, sets[s]);
        for (size_t i = 0; i < json_object_array_length(list); ++i) {
            struct json_object *v = json_object_array_get_idx(list, i);
            current = str(v, "name");
            unsigned char rawkeys[8][32], nonce[24];
            struct VgiGrantKeys k = {
                .keys = (const unsigned char(*)[32])rawkeys,
                .audience = json_object_get_string(fallback(v, defaults, "audience")),
                .max_ttl_seconds = json_object_get_int64(fallback(v, defaults, "max_ttl_seconds")),
                .clock_skew_seconds =
                    json_object_get_int64(fallback(v, defaults, "clock_skew_seconds"))};
            if (!s) {
                k.key_count = 1;
                CHECK(b64(str(v, "minting_key_b64"), rawkeys[0], 32) == 32);
            } else {
                struct json_object *keys = fallback(v, defaults, "verify_keys_b64");
                k.key_count = json_object_array_length(keys);
                CHECK(k.key_count <= 8);
                for (size_t j = 0; j < k.key_count; ++j)
                    CHECK(b64(json_object_get_string(json_object_array_get_idx(keys, j)),
                              rawkeys[j], 32) == 32);
            }
            struct VgiGrant c = {0};
            if (!s) {
                struct json_object *expected = get(v, "claims");
                c.issued_at = num(expected, "issued_at");
                c.expires_at = num(expected, "expires_at");
                c.grant_id = (char *)str(expected, "grant_id");
                c.principal = (char *)str(expected, "principal");
                c.purpose = (char *)str(expected, "purpose");
                struct json_object *scopes = get(expected, "scopes");
                c.scope_count = json_object_array_length(scopes);
                c.scopes = calloc(c.scope_count + 1, sizeof(char *));
                for (size_t j = 0; j < c.scope_count; ++j)
                    c.scopes[j] =
                        (char *)json_object_get_string(json_object_array_get_idx(scopes, j));
                CHECK(!sodium_hex2bin(nonce, 24, str(v, "nonce_hex"), 48, NULL, NULL, NULL));
                struct ArrowBuffer token;
                ArrowBufferInit(&token);
                CHECK(!VgiGrantMint(&k, &c, nonce, &token));
                CHECK(!strcmp((char *)token.data, str(v, "token")));
                ArrowBufferReset(&token);
                free(c.scopes);
            } else {
                int rc = VgiGrantOpen(&k, str(v, "token"),
                                      json_object_get_double(fallback(v, defaults, "now")), &c);
                if (s == 2)
                    CHECK(rc == (json_object_get_boolean(get(v, "expired")) ? ESTALE : EINVAL));
                else
                    CHECK(!rc);
                VgiGrantReset(&c);
            }
        }
    }
}
int main(int argc, char **argv) {
    CHECK(argc == 3);
    CHECK(sodium_init() >= 0);
    struct json_object *tickets = json_object_from_file(argv[1]),
                       *grants = json_object_from_file(argv[2]);
    CHECK(tickets && grants);
    ticket_vectors(tickets);
    grant_vectors(grants);
    json_object_put(tickets);
    json_object_put(grants);
    puts("54 canonical grant/ticket vectors passed");
    return 0;
}
