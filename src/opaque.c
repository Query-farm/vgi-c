#include <errno.h>
#include <sodium.h>
#include <string.h>

#include "opaque_internal.h"

static int reject(struct VgiRpcCall *call, const char *field) {
    char message[96];
    snprintf(message, sizeof(message), "%s not recognized", field);
    return VgiRpcFail(call, "OpaqueDataNotRecognizedError", "INVALID_ARGUMENT",
                      "opaque_data_not_recognized", message);
}
static int transform(struct VgiRpcCall *call, const char *field, struct ArrowBufferView input,
                     int seal, struct ArrowBuffer *out) {
    struct ArrowBuffer aad;
    ArrowBufferInit(&aad);
    const char *label = !strcmp(field, "attach_opaque_data") ? "vgi.attach_opaque_data.v1"
                                                             : "vgi.transaction_opaque_data.v1";
    int rc = ArrowBufferAppend(&aad, label, (int64_t)strlen(label) + 1);
    const uint8_t authenticated = call->principal ? 1 : 0;
    if (!rc) rc = ArrowBufferAppend(&aad, &authenticated, 1);
    if (!rc && authenticated) {
        const char *domain = call->auth_domain ? call->auth_domain : "";
        rc = ArrowBufferAppend(&aad, domain, (int64_t)strlen(domain) + 1);
        if (!rc) rc = ArrowBufferAppend(&aad, call->principal, (int64_t)strlen(call->principal));
    }
    if (!rc && !authenticated) rc = ArrowBufferAppend(&aad, "anonymous", 9);
    if (!rc && strcmp(field, "attach_opaque_data")) {
        if (!(*(struct ArrowBuffer *)call->application_state).size_bytes)
            rc = reject(call, field);
        else {
            rc = ArrowBufferAppend(&aad, "", 1);
            if (!rc)
                rc = ArrowBufferAppend(&aad, (*(struct ArrowBuffer *)call->application_state).data,
                                       (*(struct ArrowBuffer *)call->application_state).size_bytes);
        }
    }
    if (!rc && seal) {
        if (input.size_bytes > INT64_MAX - 41) rc = EOVERFLOW;
        if (!rc) rc = ArrowBufferResize(out, input.size_bytes + 41, 0);
        if (!rc) {
            out->data[0] = 2;
            randombytes_buf(out->data + 1, 24);
            unsigned long long length = 0;
            rc = crypto_aead_xchacha20poly1305_ietf_encrypt(
                out->data + 25, &length, input.data.as_uint8, (unsigned long long)input.size_bytes,
                aad.data, (unsigned long long)aad.size_bytes, NULL, out->data + 1,
                call->sealing_key);
        }
    } else if (!rc) {
        if (input.size_bytes < 41 || input.data.as_uint8[0] != 2) rc = reject(call, field);
        if (!rc) rc = ArrowBufferResize(out, input.size_bytes - 41, 0);
        if (!rc) {
            unsigned long long length = 0;
            if (crypto_aead_xchacha20poly1305_ietf_decrypt(
                    out->data, &length, NULL, input.data.as_uint8 + 25,
                    (unsigned long long)input.size_bytes - 25, aad.data,
                    (unsigned long long)aad.size_bytes, input.data.as_uint8 + 1, call->sealing_key))
                rc = reject(call, field);
        }
    }
    ArrowBufferReset(&aad);
    if (rc) ArrowBufferReset(out);
    return rc;
}
static int rewrite(struct VgiRpcCall *, const struct VgiBatch *, struct VgiBatch *, int, int);
static int rewrite_value(struct VgiRpcCall *call, const struct ArrowSchema *schema,
                         const struct ArrowArrayView *value, int64_t row, struct ArrowArray *out,
                         int seal, int depth) {
    const char *name = schema->name ? schema->name : "";
    if (depth > 20) return EINVAL;
    if (ArrowArrayViewIsNull(value, row)) return ArrowArrayAppendNull(out, 1);
    int opaque = !strcmp(name, "attach_opaque_data") || !strcmp(name, "transaction_opaque_data");
    if (opaque && value->storage_type != NANOARROW_TYPE_BINARY &&
        value->storage_type != NANOARROW_TYPE_LARGE_BINARY)
        return reject(call, name);
    if (opaque) {
        if (seal && !strcmp(name, "attach_opaque_data") && (*(struct ArrowBuffer *)call->application_state).size_bytes)
            return ArrowArrayAppendBytes(
                out, (struct ArrowBufferView){{(*(struct ArrowBuffer *)call->application_state).data},
                                              (*(struct ArrowBuffer *)call->application_state).size_bytes});
        struct ArrowStringView text = VgiValueString(value, row);
        struct ArrowBuffer bytes;
        ArrowBufferInit(&bytes);
        int rc = transform(call, name, (struct ArrowBufferView){{text.data}, text.size_bytes}, seal,
                           &bytes);
        if (!rc)
            rc = ArrowArrayAppendBytes(out,
                                       (struct ArrowBufferView){{bytes.data}, bytes.size_bytes});
        ArrowBufferReset(&bytes);
        return rc;
    }
    int nested =
        !strcmp(name, "request") || !strcmp(name, "bind_call") ||
        (seal && (!strcmp(name, "result") || !strcmp(name, "item") || !strcmp(name, "schema")));
    if (nested && value->storage_type == NANOARROW_TYPE_BINARY) {
        struct ArrowStringView text = VgiValueString(value, row);
        if (text.size_bytes >= 4 && !memcmp(text.data, "\xff\xff\xff\xff", 4)) {
            struct VgiBatch source = {0}, target = {0};
            struct ArrowBuffer bytes;
            ArrowBufferInit(&bytes);
            int rc = VgiBatchDecode(&source, (struct ArrowBufferView){{text.data}, text.size_bytes},
                                    &call->error);
            if (!rc) rc = rewrite(call, &source, &target, seal, depth + 1);
            if (!rc)
                rc = VgiIpcWriteBatch(&target.schema, &target.array, NULL, &bytes, &call->error);
            if (!rc)
                rc = ArrowArrayAppendBytes(
                    out, (struct ArrowBufferView){{bytes.data}, bytes.size_bytes});
            ArrowBufferReset(&bytes);
            VgiBatchReset(&source);
            VgiBatchReset(&target);
            return rc;
        }
    }
    if (seal && value->storage_type == NANOARROW_TYPE_STRUCT) {
        for (int64_t i = 0; i < schema->n_children; ++i)
            NANOARROW_RETURN_NOT_OK(rewrite_value(call, schema->children[i], value->children[i],
                                                  row + value->offset, out->children[i], seal,
                                                  depth + 1));
        return ArrowArrayFinishElement(out);
    }
    if (seal && value->storage_type == NANOARROW_TYPE_LIST) {
        int64_t start = ArrowArrayViewListChildOffset(value, row),
                end = ArrowArrayViewListChildOffset(value, row + 1);
        for (int64_t i = start; i < end; ++i)
            NANOARROW_RETURN_NOT_OK(rewrite_value(call, schema->children[0], value->children[0], i,
                                                  out->children[0], seal, depth + 1));
        return ArrowArrayFinishElement(out);
    }
    return VgiArrayAppendValue(out, schema, value, schema, row, &call->error);
}
static int rewrite(struct VgiRpcCall *call, const struct VgiBatch *source, struct VgiBatch *out,
                   int seal, int depth) {
    if (depth > 20) return EINVAL;
    int64_t attach = VgiFieldIndex(&source->schema, "attach_opaque_data"),
            tx = VgiFieldIndex(&source->schema, "transaction_opaque_data");
    if (!seal && (attach >= 0 || tx >= 0) && source->array.length != 1)
        return reject(call, attach >= 0 ? "attach_opaque_data" : "transaction_opaque_data");
    if (!seal && attach >= 0 && !VgiBatchIsNull(source, "attach_opaque_data")) {
        struct ArrowBufferView value = VgiBatchBytes(source, "attach_opaque_data");
        if ((*(struct ArrowBuffer *)call->application_state).size_bytes &&
            (value.size_bytes != (*(struct ArrowBuffer *)call->application_state).size_bytes ||
             memcmp(value.data.data, (*(struct ArrowBuffer *)call->application_state).data, (size_t)value.size_bytes)))
            return reject(call, "attach_opaque_data");
        if (!(*(struct ArrowBuffer *)call->application_state).size_bytes)
            NANOARROW_RETURN_NOT_OK(
                ArrowBufferAppend(&(*(struct ArrowBuffer *)call->application_state), value.data.data, value.size_bytes));
    }
    int rc = VgiBatchInit(out, &source->schema, &call->error);
    for (int64_t row = 0; !rc && row < source->array.length; ++row) {
        for (int64_t i = 0; !rc && i < source->schema.n_children; ++i)
            rc = rewrite_value(call, source->schema.children[i], source->view.children[i], row,
                               out->array.children[i], seal, depth);
        if (!rc) rc = ArrowArrayFinishElement(&out->array);
    }
    if (!rc) rc = VgiBatchFinish(out, &call->error);
    return rc;
}
int VgiOpaqueOpenRequest(struct VgiRpcCall *call) {
    if (!call->sealing_key) return 0;
    struct VgiBatch opened = {0};
    int rc = rewrite(call, &call->request, &opened, 0, 0);
    if (!rc) {
        VgiBatchReset(&call->request);
        call->request = opened;
    } else
        VgiBatchReset(&opened);
    return rc;
}
int VgiOpaqueSealResponse(struct VgiRpcCall *call, struct VgiBatch *batch) {
    if (!call->sealing_key || !batch->array.release) return 0;
    struct VgiBatch sealed = {0};
    int rc = rewrite(call, batch, &sealed, 1, 0);
    if (!rc) {
        VgiBatchReset(batch);
        *batch = sealed;
    } else
        VgiBatchReset(&sealed);
    return rc;
}
