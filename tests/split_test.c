#include "vgi/split.h"

#include <errno.h>
#include <sodium.h>
#include <stdio.h>
#include <string.h>

#include "vgi_rpc/ipc.h"

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
static int append_list(struct VgiBatch *batch, const char *name, const char *const *values,
                       size_t n) {
    struct ArrowArray *list = VgiBatchColumn(batch, name);
    if (!list) return EINVAL;
    for (size_t i = 0; i < n; ++i)
        if (ArrowArrayAppendString(list->children[0], ArrowCharView(values[i]))) return EINVAL;
    return ArrowArrayFinishElement(list);
}
static int list_length(const struct ArrowArrayView *list) {
    return (int)(ArrowArrayViewListChildOffset(list, 1) - ArrowArrayViewListChildOffset(list, 0));
}
static int bytes_equal(struct ArrowBufferView v, const char *expected) {
    return v.size_bytes == (int64_t)strlen(expected) &&
           !memcmp(v.data.data, expected, strlen(expected));
}
/* Protocol 3.1 distribution fields survive the generated ScanSplit and
 * TableFunctionPlanResult records and VgiPlanAppendSplit. */
static int check_partition_distribution(void) {
    struct ArrowError error = {0};
    struct VgiBatch split = {0}, plan = {0}, decoded = {0}, decoded_split = {0};
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    CHECK(!VgiBatchInitNamed(&split, "ScanSplit", &error));
    CHECK(!VgiBatchSetBytes(&split, "payload", "p", 1));
    CHECK(!VgiBatchSetInt(&split, "rows_exact", 1));
    CHECK(!VgiBatchSetBytes(&split, "key_value", "key", 3));
    CHECK(!VgiBatchSetInt(&split, "range_index", 2));
    struct ArrowArray *ids = VgiBatchColumn(&split, "location_ids");
    CHECK(ids && !ArrowArrayAppendInt(ids->children[0], 0) &&
          !ArrowArrayAppendInt(ids->children[0], 1) && !ArrowArrayFinishElement(ids));
    CHECK(!VgiBatchFinishRow(&split, &error));
    CHECK(!VgiBatchInitNamed(&plan, "TableFunctionPlanResult", &error));
    CHECK(!VgiPlanAppendSplit(&plan, &split, &error));
    CHECK(!ArrowArrayFinishElement(VgiBatchColumn(&plan, "splits")));
    const char *const locations[] = {"worker-a", "worker-b"};
    CHECK(!append_list(&plan, "locations", locations, 2));
    CHECK(!VgiBatchSetBytes(&plan, "key_grouping", "grouping", 8));
    CHECK(!VgiBatchSetBytes(&plan, "range_partitioning", "ranges", 6));
    CHECK(!VgiBatchSetInt(&plan, "max_workers", 3));
    CHECK(!VgiBatchSetString(&plan, "scope", ""));
    CHECK(!VgiBatchFinishRow(&plan, &error));
    CHECK(!VgiIpcWriteBatch(&plan.schema, &plan.array, NULL, &bytes, &error));
    CHECK(!VgiBatchDecode(&decoded, (struct ArrowBufferView){{bytes.data}, bytes.size_bytes},
                          &error));
    CHECK(VgiBatchInt(&decoded, "max_workers", -1) == 3);
    CHECK(bytes_equal(VgiBatchBytes(&decoded, "key_grouping"), "grouping"));
    CHECK(bytes_equal(VgiBatchBytes(&decoded, "range_partitioning"), "ranges"));
    const struct ArrowArrayView *listed = VgiBatchGet(&decoded, "locations");
    CHECK(listed && list_length(listed) == 2);
    struct ArrowStringView second = VgiValueString(listed->children[0], 1);
    CHECK(second.size_bytes == 8 && !memcmp(second.data, "worker-b", 8));
    const struct ArrowArrayView *splits = VgiBatchGet(&decoded, "splits");
    CHECK(splits && list_length(splits) == 1);
    struct ArrowStringView encoded = VgiValueString(splits->children[0], 0);
    CHECK(!VgiBatchDecode(&decoded_split,
                          (struct ArrowBufferView){{encoded.data}, encoded.size_bytes}, &error));
    CHECK(bytes_equal(VgiBatchBytes(&decoded_split, "key_value"), "key"));
    CHECK(VgiBatchInt(&decoded_split, "range_index", -1) == 2);
    CHECK(VgiBatchIsNull(&decoded_split, "column_bounds"));
    const struct ArrowArrayView *location_ids = VgiBatchGet(&decoded_split, "location_ids");
    CHECK(location_ids && list_length(location_ids) == 2);
    CHECK(ArrowArrayViewGetIntUnsafe(location_ids->children[0], 1) == 1);
    VgiBatchReset(&split);
    VgiBatchReset(&plan);
    VgiBatchReset(&decoded);
    VgiBatchReset(&decoded_split);
    ArrowBufferReset(&bytes);
    return 0;
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
    CHECK(!check_partition_distribution());
    puts(
        "Split tokens: round trip, tampering, truncation, identity, key and downgrade checks "
        "passed; protocol 3.1 distribution fields round trip");
    return 0;
}
