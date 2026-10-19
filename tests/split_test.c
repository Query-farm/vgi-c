#include "vgi/split.h"

#include <errno.h>
#include <sodium.h>
#include <stdio.h>
#include <string.h>

#define CHECK(x)                                                    \
    do {                                                            \
        if (!(x)) {                                                 \
            fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x); \
            return 1;                                               \
        }                                                           \
    } while (0)
static struct ArrowBufferView view(const struct ArrowBuffer *b) {
    return (struct ArrowBufferView){{b->data}, b->size_bytes};
}
int main(void) {
    CHECK(sodium_init() >= 0);
    const unsigned char payload[] = {0, 1, 2, 0, 255}, anchor[] = {1, 0, 0, 0};
    unsigned char key[32], other_key[32], fingerprint[16] = {0}, other_fingerprint[16] = {1};
    memset(key, 3, sizeof(key));
    memset(other_key, 4, sizeof(other_key));
    struct ArrowBufferView input = {{payload}, sizeof(payload)},
                           snapshot = {{anchor}, sizeof(anchor)};
    struct VgiSplitIdentity alice = {1, "bearer", "alice"}, bob = {1, "bearer", "bob"},
                            other_domain = {1, "grant", "alice"}, anonymous = {0, NULL, NULL};
    struct ArrowBuffer token, opened;
    ArrowBufferInit(&token);
    ArrowBufferInit(&opened);
    CHECK(!VgiSplitTokenBuild(input, fingerprint, snapshot, key, &alice, &token));
    CHECK(!VgiSplitTokenOpen(view(&token), fingerprint, snapshot, key, &alice, &opened));
    CHECK(opened.size_bytes == sizeof(payload) && !memcmp(opened.data, payload, sizeof(payload)));
    ArrowBufferReset(&opened);
    const struct VgiSplitIdentity *wrong[] = {&bob, &other_domain, &anonymous};
    for (size_t i = 0; i < 3; ++i) {
        CHECK(VgiSplitTokenOpen(view(&token), fingerprint, snapshot, key, wrong[i], &opened) ==
              EINVAL);
        CHECK(!opened.size_bytes);
    }
    CHECK(VgiSplitTokenOpen(view(&token), other_fingerprint, snapshot, key, &alice, &opened) ==
          EINVAL);
    CHECK(VgiSplitTokenOpen(view(&token), fingerprint, snapshot, other_key, &alice, &opened) ==
          EINVAL);
    CHECK(VgiSplitTokenOpen(view(&token), fingerprint, snapshot, NULL, &alice, &opened) == EINVAL);
    /* Every byte is authenticated: snapshot changes are separately classified. */
    for (int64_t i = 0; i < token.size_bytes; ++i) {
        token.data[i] ^= 1;
        int rc = VgiSplitTokenOpen(view(&token), fingerprint, snapshot, key, &alice, &opened);
        CHECK(rc == EINVAL || rc == ESTALE);
        CHECK(!opened.size_bytes);
        token.data[i] ^= 1;
    }
    for (int64_t size = 0; size < token.size_bytes; ++size) {
        CHECK(VgiSplitTokenOpen((struct ArrowBufferView){{token.data}, size}, fingerprint, snapshot,
                                key, &alice, &opened) != 0);
        CHECK(!opened.size_bytes);
    }
    ArrowBufferReset(&token);
    CHECK(!VgiSplitTokenBuild(input, fingerprint, snapshot, NULL, &alice, &token));
    CHECK(VgiSplitTokenOpen(view(&token), fingerprint, snapshot, key, &alice, &opened) ==
          EINVAL); /* no downgrade */
    CHECK(!VgiSplitTokenOpen(view(&token), fingerprint, snapshot, NULL, &alice, &opened));
    CHECK(opened.size_bytes == sizeof(payload));
    ArrowBufferReset(&opened);
    ArrowBufferReset(&token);
    puts(
        "Split tokens: round trip, tampering, truncation, identity, key and downgrade checks "
        "passed");
    return 0;
}
