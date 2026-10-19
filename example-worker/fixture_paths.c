#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "fixtures.h"
/* The checked-in descriptors use the reference fixture's /tmp default. Resolve
 * that prefix at startup, including IPC descriptors nested in binary columns. */
static int has_path(struct ArrowStringView bytes) {
    for (int64_t i = 0; i + 5 <= bytes.size_bytes; ++i)
        if (bytes.data[i] == '/' && !memcmp(bytes.data + i, "/tmp/", 5)) return 1;
    return 0;
}
static int rewrite_value(struct ArrowArray *out, const struct ArrowSchema *schema,
                         const struct ArrowArrayView *view, int64_t row, const char *root,
                         int depth, struct ArrowError *error);
static int rewrite_batch(struct VgiBatch *batch, const char *root, int depth,
                         struct ArrowError *error) {
    struct VgiBatch out = {0};
    int rc = VgiBatchInit(&out, &batch->schema, error);
    for (int64_t row = 0; !rc && row < batch->array.length; ++row)
        rc = rewrite_value(&out.array, &out.schema, &batch->view, row, root, depth, error);
    if (!rc) rc = VgiBatchFinish(&out, error);
    if (!rc) {
        VgiBatchReset(batch);
        *batch = out;
        memset(&out, 0, sizeof(out));
    }
    VgiBatchReset(&out);
    return rc;
}
static int rewrite_value(struct ArrowArray *out, const struct ArrowSchema *schema,
                         const struct ArrowArrayView *view, int64_t row, const char *root,
                         int depth, struct ArrowError *error) {
    if (depth > 32) return EOVERFLOW;
    if (ArrowArrayViewIsNull(view, row)) return ArrowArrayAppendNull(out, 1);
    if (view->storage_type == NANOARROW_TYPE_STRUCT) {
        for (int64_t i = 0; i < schema->n_children; ++i)
            NANOARROW_RETURN_NOT_OK(rewrite_value(out->children[i], schema->children[i],
                                                  view->children[i], row + view->offset, root,
                                                  depth + 1, error));
        return ArrowArrayFinishElement(out);
    }
    if (view->storage_type == NANOARROW_TYPE_LIST) {
        int64_t start = ArrowArrayViewListChildOffset(view, row),
                end = ArrowArrayViewListChildOffset(view, row + 1);
        for (int64_t i = start; i < end; ++i)
            NANOARROW_RETURN_NOT_OK(rewrite_value(out->children[0], schema->children[0],
                                                  view->children[0], i, root, depth + 1, error));
        return ArrowArrayFinishElement(out);
    }
    if (view->storage_type == NANOARROW_TYPE_STRING) {
        struct ArrowStringView text = VgiValueString(view, row);
        if (text.size_bytes >= 5 && !memcmp(text.data, "/tmp/", 5)) {
            struct ArrowBuffer path;
            ArrowBufferInit(&path);
            int rc = ArrowBufferAppend(&path, root, strlen(root));
            if (!rc) rc = ArrowBufferAppend(&path, text.data + 4, text.size_bytes - 4);
            if (!rc)
                rc = ArrowArrayAppendString(
                    out, (struct ArrowStringView){(const char *)path.data, path.size_bytes});
            ArrowBufferReset(&path);
            return rc;
        }
    }
    if (view->storage_type == NANOARROW_TYPE_BINARY) {
        struct ArrowStringView bytes = VgiValueString(view, row);
        if (bytes.size_bytes > 8 && !memcmp(bytes.data, "\xff\xff\xff\xff", 4) && has_path(bytes)) {
            struct VgiBatch nested = {0};
            struct ArrowError ignored = {{0}};
            int rc = VgiBatchDecode(
                &nested, (struct ArrowBufferView){{bytes.data}, bytes.size_bytes}, &ignored);
            if (!rc) {
                struct ArrowBuffer encoded;
                ArrowBufferInit(&encoded);
                rc = rewrite_batch(&nested, root, depth + 1, error);
                if (!rc)
                    rc = VgiIpcWriteBatch(&nested.schema, &nested.array, NULL, &encoded, error);
                if (!rc)
                    rc = ArrowArrayAppendBytes(
                        out, (struct ArrowBufferView){{encoded.data}, encoded.size_bytes});
                ArrowBufferReset(&encoded);
                VgiBatchReset(&nested);
                return rc;
            }
            VgiBatchReset(&nested);
        }
    }
    return VgiArrayAppendValue(out, schema, view, schema, row, error);
}
int ExampleFixturePaths(struct VgiBatch *batch, struct ArrowError *error) {
    const char *root = getenv("VGI_TEST_BRANCH_DIR");
    if (!root || !*root || !strcmp(root, "/tmp")) return 0;
    return rewrite_batch(batch, root, 0, error);
}
