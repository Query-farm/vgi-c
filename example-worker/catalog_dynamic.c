#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "fixtures.h"
#include "vgi/catalog.h"
static int text_is(struct ArrowStringView value, const char *text) {
    return value.size_bytes == (int64_t)strlen(text) &&
           !memcmp(value.data, text, (size_t)value.size_bytes);
}
int ExampleCatalogHandler(void *state, struct VgiRpcCall *call, struct VgiRpcReply *reply,
                          int *handled) {
    *handled = 0;
    const char *method = call->method->name;
    if (strcmp(method, "catalog_table_get") && strcmp(method, "catalog_table_scan_function_get") &&
        strcmp(method, "catalog_table_scan_branches_get"))
        return 0;
    if (!text_is(VgiBatchString(&call->request, "name"), "cache_versioned")) return 0;
    *handled = 1;
    int64_t version = 3;
    struct ArrowStringView unit = VgiBatchString(&call->request, "at_unit"),
                           value = VgiBatchString(&call->request, "at_value");
    if (unit.data && unit.size_bytes) {
        if (!text_is(unit, "VERSION") && !text_is(unit, "version"))
            return VgiRpcFail(call, "ValueError", "INVALID_ARGUMENT", "",
                              "Unsupported time travel unit");
        if (value.size_bytes != 1 || value.data[0] < '1' || value.data[0] > '3')
            return VgiRpcFail(call, "ValueError", "INVALID_ARGUMENT", "", "Unknown version");
        version = value.data[0] - '0';
    }
    struct ArrowSchema schema = {0};
    ArrowSchemaInit(&schema);
    struct VgiBatch args = {0}, result = {0}, branch = {0};
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    int rc = ArrowSchemaSetTypeStruct(&schema, 1);
    if (!rc) rc = ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_INT64);
    if (!rc) rc = ArrowSchemaSetName(schema.children[0], "arg_0");
    if (!rc) rc = VgiBatchInit(&args, &schema, &call->error);
    if (!rc) rc = VgiBatchSetInt(&args, "arg_0", version);
    if (!rc) rc = VgiBatchFinishRow(&args, &call->error);
    if (!rc) rc = VgiIpcWriteBatch(&args.schema, &args.array, NULL, &bytes, &call->error);
    int branches = !strcmp(method, "catalog_table_scan_branches_get");
    if (!rc)
        rc = VgiBatchInitNamed(&branch, branches ? "ScanBranch" : "ScanFunctionResult",
                               &call->error);
    if (!rc) rc = VgiBatchSetString(&branch, "function_name", "cache_versioned_scan");
    if (!rc) rc = VgiBatchSetBytes(&branch, "arguments", bytes.data, bytes.size_bytes);
    struct ArrowArray *path = VgiBatchColumn(&branch, "schema_path");
    if (!rc) rc = ArrowArrayAppendString(path->children[0], ArrowCharView("data"));
    if (!rc) rc = ArrowArrayFinishElement(path);
    if (!rc) rc = VgiBatchFinishRow(&branch, &call->error);
    if (!rc && branches) {
        rc = VgiBatchInitNamed(&result, "ScanBranchesResult", &call->error);
        bytes.size_bytes = 0;
        if (!rc) rc = VgiIpcWriteBatch(&branch.schema, &branch.array, NULL, &bytes, &call->error);
        struct ArrowArray *list = VgiBatchColumn(&result, "branches");
        if (!rc)
            rc = ArrowArrayAppendBytes(list->children[0],
                                       (struct ArrowBufferView){{bytes.data}, bytes.size_bytes});
        if (!rc) rc = ArrowArrayFinishElement(list);
        if (!rc) rc = VgiBatchFinishRow(&result, &call->error);
    }
    if (!rc && !strcmp(method, "catalog_table_get")) {
        struct VgiBatch original = {0}, table = {0}, changed = {0};
        int default_handled = 0;
        rc = VgiWorkerCatalogDefault(state, call, reply, &default_handled);
        if (!rc && !default_handled) rc = EINVAL;
        if (!rc) rc = VgiBatchChild(&reply->value, "result", &original, &call->error);
        if (!rc) rc = VgiBatchInit(&result, &original.schema, &call->error);
        struct ArrowBuffer scan;
        ArrowBufferInit(&scan);
        if (!rc) rc = VgiIpcWriteBatch(&branch.schema, &branch.array, NULL, &scan, &call->error);
        const struct ArrowArrayView *list = VgiBatchGet(&original, "items");
        for (int64_t i = 0; !rc && list && i < list->children[0]->length; ++i) {
            struct ArrowStringView data = VgiValueString(list->children[0], i);
            rc = VgiBatchDecode(&table, (struct ArrowBufferView){{data.data}, data.size_bytes},
                                &call->error);
            if (!rc) rc = VgiBatchInit(&changed, &table.schema, &call->error);
            for (int64_t col = 0; !rc && col < table.schema.n_children; ++col) {
                if (!strcmp(table.schema.children[col]->name, "scan_function"))
                    rc = ArrowArrayAppendBytes(
                        changed.array.children[col],
                        (struct ArrowBufferView){{scan.data}, scan.size_bytes});
                else
                    rc = VgiArrayAppendValue(changed.array.children[col],
                                             changed.schema.children[col], table.view.children[col],
                                             table.schema.children[col], 0, &call->error);
            }
            if (!rc) rc = VgiBatchFinishRow(&changed, &call->error);
            bytes.size_bytes = 0;
            if (!rc)
                rc = VgiIpcWriteBatch(&changed.schema, &changed.array, NULL, &bytes, &call->error);
            if (!rc)
                rc =
                    ArrowArrayAppendBytes(VgiBatchColumn(&result, "items")->children[0],
                                          (struct ArrowBufferView){{bytes.data}, bytes.size_bytes});
            VgiBatchReset(&table);
            VgiBatchReset(&changed);
        }
        if (!rc) rc = ArrowArrayFinishElement(VgiBatchColumn(&result, "items"));
        if (!rc) rc = VgiBatchFinishRow(&result, &call->error);
        VgiBatchReset(&original);
        VgiBatchReset(&reply->value);
        ArrowBufferReset(&scan);
        branches = 1;
    }
    if (!rc) rc = VgiRpcReplyPayload(call, reply, branches ? &result : &branch);
    ArrowSchemaRelease(&schema);
    VgiBatchReset(&args);
    VgiBatchReset(&result);
    VgiBatchReset(&branch);
    ArrowBufferReset(&bytes);
    return rc;
}
