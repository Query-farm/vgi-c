#include "vgi/split.h"

#include <errno.h>
#include <limits.h>
#include <sodium.h>
#include <string.h>

#include "split_internal.h"
static int valid(struct ArrowBufferView value) {
    return value.size_bytes >= 0 && (!value.size_bytes || value.data.data);
}
static void feed(crypto_hash_sha256_state *hash, const char *key, const void *data, int64_t size) {
    crypto_hash_sha256_update(hash, (const unsigned char *)key, strlen(key) + 1);
    if (size) crypto_hash_sha256_update(hash, data, (unsigned long long)size);
    const unsigned char zero = 0;
    crypto_hash_sha256_update(hash, &zero, 1);
}
int VgiSplitFingerprint(const struct VgiBatch *bind, unsigned char out[16]) {
    if (!bind || !out) return EINVAL;
    crypto_hash_sha256_state hash;
    crypto_hash_sha256_init(&hash);
    const char prefix[] = "vgi.split_token.v1";
    crypto_hash_sha256_update(&hash, (const unsigned char *)prefix, sizeof(prefix));
    const struct ArrowArrayView *path = VgiBatchGet(bind, "schema_path");
    if (path && !ArrowArrayViewIsNull(path, 0)) {
        int64_t start = ArrowArrayViewListChildOffset(path, 0),
                end = ArrowArrayViewListChildOffset(path, 1);
        for (int64_t i = start; i < end; ++i) {
            struct ArrowStringView part = VgiValueString(path->children[0], i);
            feed(&hash, "schema_path", part.data, part.size_bytes);
        }
    }
    struct ArrowStringView name = VgiBatchString(bind, "function_name");
    feed(&hash, "function_name", name.data, name.size_bytes);
    struct ArrowBufferView args = VgiBatchBytes(bind, "arguments"),
                           settings = VgiBatchBytes(bind, "settings");
    feed(&hash, "arguments", args.data.data, args.size_bytes);
    feed(&hash, "settings", settings.data.data, settings.size_bytes);
    unsigned char digest[crypto_hash_sha256_BYTES];
    crypto_hash_sha256_final(&hash, digest);
    memcpy(out, digest, 16);
    sodium_memzero(digest, sizeof(digest));
    return 0;
}
static int aad(struct ArrowBufferView header, const struct VgiSplitIdentity *identity,
               struct ArrowBuffer *out) {
    NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(out, header.data.data, header.size_bytes));
    unsigned char authenticated = identity && identity->authenticated ? 1 : 0;
    NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(out, &authenticated, 1));
    if (!authenticated) return ArrowBufferAppend(out, "anonymous", 9);
    const char *domain = identity->domain ? identity->domain : "",
               *principal = identity->principal ? identity->principal : "";
    NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(out, domain, (int64_t)strlen(domain) + 1));
    return ArrowBufferAppend(out, principal, (int64_t)strlen(principal));
}
int VgiSplitTokenBuild(struct ArrowBufferView payload, const unsigned char fingerprint[16],
                       struct ArrowBufferView anchor, const unsigned char *key,
                       const struct VgiSplitIdentity *identity, struct ArrowBuffer *out) {
    if (!out || out->size_bytes || !fingerprint || !valid(payload) || !valid(anchor) ||
        anchor.size_bytes > UINT16_MAX)
        return EINVAL;
    if (payload.size_bytes > INT64_MAX - anchor.size_bytes - 61) return EOVERFLOW;
    unsigned char header[20] = {1, key ? 1 : 0, (unsigned char)anchor.size_bytes,
                                (unsigned char)(anchor.size_bytes >> 8)};
    memcpy(header + 4, fingerprint, 16);
    int rc = ArrowBufferAppend(out, header, 20);
    if (!rc) rc = ArrowBufferAppend(out, anchor.data.data, anchor.size_bytes);
    if (!key) {
        if (!rc) rc = ArrowBufferAppend(out, payload.data.data, payload.size_bytes);
        if (rc) out->size_bytes = 0;
        return rc;
    }
    struct ArrowBuffer associated;
    ArrowBufferInit(&associated);
    if (!rc)
        rc = aad((struct ArrowBufferView){{out->data}, out->size_bytes}, identity, &associated);
    int64_t at = out->size_bytes;
    if (!rc) rc = ArrowBufferResize(out, at + 1 + 24 + payload.size_bytes + 16, 0);
    if (!rc) {
        out->data[at] = 1;
        randombytes_buf(out->data + at + 1, 24);
        unsigned long long size;
        if (crypto_aead_xchacha20poly1305_ietf_encrypt(
                out->data + at + 25, &size, payload.data.data,
                (unsigned long long)payload.size_bytes, associated.data,
                (unsigned long long)associated.size_bytes, NULL, out->data + at + 1, key))
            rc = EIO;
    }
    ArrowBufferReset(&associated);
    if (rc) out->size_bytes = 0;
    return rc;
}
int VgiSplitTokenOpen(struct ArrowBufferView token, const unsigned char fingerprint[16],
                      struct ArrowBufferView anchor, const unsigned char *key,
                      const struct VgiSplitIdentity *identity, struct ArrowBuffer *out) {
    if (!out || out->size_bytes || !fingerprint || !valid(token) || !valid(anchor) ||
        token.size_bytes < 20)
        return EINVAL;
    const unsigned char *data = token.data.as_uint8;
    if (data[0] != 1 || data[1] != (key ? 1 : 0) || sodium_memcmp(data + 4, fingerprint, 16))
        return EINVAL;
    int64_t size = (int64_t)data[2] + ((int64_t)data[3] << 8), at = 20 + size;
    if (at > token.size_bytes) return EINVAL;
    if (anchor.size_bytes != size || (size && memcmp(data + 20, anchor.data.data, (size_t)size)))
        return ESTALE;
    if (!key) return ArrowBufferAppend(out, data + at, token.size_bytes - at);
    if (token.size_bytes - at < 41 || data[at] != 1) return EINVAL;
    struct ArrowBuffer associated;
    ArrowBufferInit(&associated);
    int rc = aad((struct ArrowBufferView){{data}, at}, identity, &associated);
    if (!rc) rc = ArrowBufferResize(out, token.size_bytes - at - 41, 0);
    if (!rc) {
        unsigned long long plaintext = 0;
        if (crypto_aead_xchacha20poly1305_ietf_decrypt(
                out->data, &plaintext, NULL, data + at + 25,
                (unsigned long long)(token.size_bytes - at - 25), associated.data,
                (unsigned long long)associated.size_bytes, data + at + 1, key))
            rc = EINVAL;
    }
    ArrowBufferReset(&associated);
    if (rc) out->size_bytes = 0;
    return rc;
}
int VgiPlanAppendSplit(struct VgiBatch *plan, const struct VgiBatch *split,
                       struct ArrowError *error) {
    struct ArrowArray *list = VgiBatchColumn(plan, "splits");
    if (!list || !split) return EINVAL;
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    int rc = VgiIpcWriteBatch(&split->schema, &split->array, NULL, &bytes, error);
    if (!rc)
        rc = ArrowArrayAppendBytes(list->children[0],
                                   (struct ArrowBufferView){{bytes.data}, bytes.size_bytes});
    ArrowBufferReset(&bytes);
    return rc;
}
static void anchor_bytes(int64_t version, unsigned char bytes[8]) {
    for (int i = 0; i < 8; ++i) bytes[i] = (unsigned char)((uint64_t)version >> (8 * i));
}
int VgiSplitSealPlan(struct VgiFunctionCall *call, struct VgiBatch *plan, int64_t current_version,
                     const unsigned char *key) {
    unsigned char fingerprint[16], anchor[8];
    int rc = VgiSplitFingerprint(&call->bind_request, fingerprint);
    anchor_bytes(VgiBatchInt(plan, "catalog_version", current_version), anchor);
    struct VgiBatch sealed = {0}, split = {0}, replacement = {0};
    struct ArrowBuffer token;
    ArrowBufferInit(&token);
    struct VgiSplitIdentity identity = {call->rpc->principal != NULL, call->rpc->auth_domain,
                                        call->rpc->principal};
    if (!rc) rc = VgiBatchInit(&sealed, &plan->schema, &call->rpc->error);
    for (int64_t col = 0; !rc && col < plan->schema.n_children; ++col) {
        if (strcmp(plan->schema.children[col]->name, "splits")) {
            rc = VgiArrayAppendValue(sealed.array.children[col], sealed.schema.children[col],
                                     plan->view.children[col], plan->schema.children[col], 0,
                                     &call->rpc->error);
            continue;
        }
        const struct ArrowArrayView *list = plan->view.children[col];
        int64_t start = ArrowArrayViewListChildOffset(list, 0),
                end = ArrowArrayViewListChildOffset(list, 1);
        for (int64_t i = start; !rc && i < end; ++i) {
            struct ArrowStringView bytes = VgiValueString(list->children[0], i);
            rc = VgiBatchDecode(&split, (struct ArrowBufferView){{bytes.data}, bytes.size_bytes},
                                &call->rpc->error);
            token.size_bytes = 0;
            if (!rc)
                rc = VgiSplitTokenBuild(VgiBatchBytes(&split, "payload"), fingerprint,
                                        (struct ArrowBufferView){{anchor}, 8}, key, &identity,
                                        &token);
            if (!rc) rc = VgiBatchInit(&replacement, &split.schema, &call->rpc->error);
            for (int64_t j = 0; !rc && j < split.schema.n_children; ++j) {
                if (!strcmp(split.schema.children[j]->name, "token"))
                    rc = ArrowArrayAppendBytes(
                        replacement.array.children[j],
                        (struct ArrowBufferView){{token.data}, token.size_bytes});
                else
                    rc = VgiArrayAppendValue(replacement.array.children[j],
                                             replacement.schema.children[j], split.view.children[j],
                                             split.schema.children[j], 0, &call->rpc->error);
            }
            if (!rc) rc = VgiBatchFinishRow(&replacement, &call->rpc->error);
            if (!rc) rc = VgiPlanAppendSplit(&sealed, &replacement, &call->rpc->error);
            VgiBatchReset(&split);
            VgiBatchReset(&replacement);
        }
        if (!rc) rc = ArrowArrayFinishElement(sealed.array.children[col]);
    }
    if (!rc) rc = VgiBatchFinishRow(&sealed, &call->rpc->error);
    if (!rc) {
        VgiBatchReset(plan);
        *plan = sealed;
        memset(&sealed, 0, sizeof(sealed));
    }
    VgiBatchReset(&sealed);
    VgiBatchReset(&split);
    VgiBatchReset(&replacement);
    ArrowBufferReset(&token);
    return rc;
}
int VgiSplitRedeem(struct VgiFunctionCall *call, int64_t current_version,
                   const unsigned char *key) {
    const struct ArrowArrayView *tokens = VgiBatchGet(&call->init_request, "split_tokens");
    if (!tokens || ArrowArrayViewIsNull(tokens, 0)) return 0;
    int64_t start = ArrowArrayViewListChildOffset(tokens, 0),
            end = ArrowArrayViewListChildOffset(tokens, 1);
    if (start == end) return 0;
    unsigned char fingerprint[16], anchor[8];
    int rc = VgiSplitFingerprint(&call->bind_request, fingerprint);
    anchor_bytes(current_version, anchor);
    struct ArrowSchema schema = {0};
    ArrowSchemaInit(&schema);
    if (!rc) rc = ArrowSchemaSetTypeStruct(&schema, 1);
    if (!rc) rc = ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_BINARY);
    if (!rc) rc = ArrowSchemaSetName(schema.children[0], "payload");
    if (!rc) rc = VgiBatchInit(&call->split_payloads, &schema, &call->rpc->error);
    struct ArrowBuffer payload;
    ArrowBufferInit(&payload);
    struct VgiSplitIdentity identity = {call->rpc->principal != NULL, call->rpc->auth_domain,
                                        call->rpc->principal};
    for (int64_t i = start; !rc && i < end; ++i) {
        struct ArrowStringView value = VgiValueString(tokens->children[0], i);
        payload.size_bytes = 0;
        rc =
            VgiSplitTokenOpen((struct ArrowBufferView){{value.data}, value.size_bytes}, fingerprint,
                              (struct ArrowBufferView){{anchor}, 8}, key, &identity, &payload);
        if (rc) {
            rc = VgiRpcFail(
                call->rpc, "RuntimeError",
                rc == ESTALE ? "SPLIT_SNAPSHOT_EXPIRED" : "SPLIT_TOKEN_INVALID", "",
                rc == ESTALE
                    ? "SPLIT_SNAPSHOT_EXPIRED: re-run the query against the current snapshot"
                    : "SPLIT_TOKEN_INVALID: token is malformed or bound elsewhere");
            break;
        }
        rc = ArrowArrayAppendBytes(call->split_payloads.array.children[0],
                                   (struct ArrowBufferView){{payload.data}, payload.size_bytes});
        if (!rc) rc = ArrowArrayFinishElement(&call->split_payloads.array);
    }
    if (!rc) rc = VgiBatchFinish(&call->split_payloads, &call->rpc->error);
    ArrowBufferReset(&payload);
    ArrowSchemaRelease(&schema);
    return rc;
}
