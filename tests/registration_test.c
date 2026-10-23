/* Registration and declarative-catalog checks: binary FunctionInfo fields set
 * through their wire-2.2 dictionary encoding, the max_workers = 1 refusal for
 * resumable functions from either side (registered FunctionInfo or a catalog's
 * served entry), and catalog_contents answering a captured etag. */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "vgi/catalog.h"
#include "vgi/worker.h"

#define CHECK(x)                                                    \
    do {                                                            \
        if (!(x)) {                                                 \
            fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x); \
            exit(1);                                                \
        }                                                           \
    } while (0)

static struct ArrowError error;

static int stub_init(void *data, struct VgiFunctionCall *call) {
    (void)data;
    (void)call;
    return 0;
}
static int stub_process(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                        struct VgiBatch *output, struct ArrowBuffer *metadata, int *finished) {
    (void)data, (void)call, (void)input, (void)output, (void)metadata, (void)finished;
    return 0;
}
static int stub_resume(void *data, struct VgiFunctionCall *call, struct VgiBatch *output,
                       struct ArrowBuffer *metadata) {
    (void)data, (void)call, (void)output, (void)metadata;
    return 0;
}
static void int64_struct(struct ArrowSchema *out, const char *name) {
    ArrowSchemaInit(out);
    CHECK(!ArrowSchemaSetTypeStruct(out, 1));
    CHECK(!ArrowSchemaSetType(out->children[0], NANOARROW_TYPE_INT64));
    CHECK(!ArrowSchemaSetName(out->children[0], name));
}
/* A function of one constant named int64 argument n, returning n. */
static void function_init(struct VgiFunction *fn, const char *name, enum VgiFunctionKind kind,
                          int resumable) {
    struct ArrowSchema arguments = {0}, output = {0};
    ArrowSchemaInit(&arguments);
    CHECK(!ArrowSchemaSetTypeStruct(&arguments, 1));
    ArrowSchemaRelease(arguments.children[0]);
    CHECK(!VgiArgumentInit(arguments.children[0], "n", "l", 1, 1, 0, &error));
    int64_struct(&output, "n");
    memset(fn, 0, sizeof(*fn));
    CHECK(!VgiFunctionInit(fn, name, kind, &arguments, &output, &error));
    ArrowSchemaRelease(&arguments);
    ArrowSchemaRelease(&output);
    fn->catalog = "cat";
    fn->schema_path = "main";
    fn->init = stub_init;
    fn->process = stub_process;
    if (resumable) fn->resume = stub_resume;
}
static int register_resumable(struct VgiWorker *worker) {
    struct VgiFunction fn;
    function_init(&fn, "paged", VGI_TABLE_IN_OUT, 1);
    int rc = VgiWorkerRegisterFunction(worker, &fn, &error);
    VgiFunctionReset(&fn);
    return rc;
}
/* An IPC FunctionInfo for "paged"; max_workers < 0 leaves it null. */
static void entry_bytes(int64_t max_workers, struct ArrowBuffer *out) {
    struct VgiFunction fn;
    function_init(&fn, "paged", VGI_TABLE_IN_OUT, 0);
    CHECK(!VgiBatchSetString(&fn.info, "stability", "CONSISTENT"));
    if (max_workers >= 0) CHECK(!VgiBatchSetInt(&fn.info, "max_workers", max_workers));
    CHECK(!VgiBatchFinishRow(&fn.info, &error));
    ArrowBufferInit(out);
    CHECK(!VgiIpcWriteBatch(&fn.info.schema, &fn.info.array, NULL, out, &error));
    VgiFunctionReset(&fn);
}
/* Declarative contents: catalog_version, an optional captured etag, and one
 * schema "main" serving the function entry (if any) as a table function. */
static void contents_init(struct VgiBatch *out, const struct ArrowBuffer *function,
                          const char *etag) {
    struct ArrowSchema schema = {0};
    ArrowSchemaInit(&schema);
    CHECK(!ArrowSchemaSetTypeStruct(&schema, 3));
    CHECK(!ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_INT64));
    CHECK(!ArrowSchemaSetName(schema.children[0], "catalog_version"));
    CHECK(!ArrowSchemaSetType(schema.children[1], NANOARROW_TYPE_STRING));
    CHECK(!ArrowSchemaSetName(schema.children[1], "etag"));
    CHECK(!ArrowSchemaSetFormat(schema.children[2], "+l"));
    CHECK(!ArrowSchemaSetName(schema.children[2], "schemas"));
    CHECK(!ArrowSchemaAllocateChildren(schema.children[2], 1));
    CHECK(!VgiSchemaInit("SchemaContents", schema.children[2]->children[0], &error));
    CHECK(!ArrowSchemaSetName(schema.children[2]->children[0], "item"));
    CHECK(!VgiBatchInit(out, &schema, &error));
    const struct ArrowSchema *type = schema.children[2]->children[0];
    struct ArrowArray *entry = out->array.children[2]->children[0];
    CHECK(!ArrowArrayAppendInt(out->array.children[0], 1));
    CHECK(etag ? !ArrowArrayAppendString(out->array.children[1], ArrowCharView(etag))
               : !ArrowArrayAppendNull(out->array.children[1], 1));
    /* SchemaInfo for main. */
    struct VgiBatch info = {0};
    struct ArrowBuffer info_bytes;
    ArrowBufferInit(&info_bytes);
    CHECK(!VgiBatchInitNamed(&info, "SchemaInfo", &error));
    struct ArrowArray *info_path = VgiBatchColumn(&info, "path");
    CHECK(info_path);
    CHECK(!ArrowArrayAppendString(info_path->children[0], ArrowCharView("main")));
    CHECK(!ArrowArrayFinishElement(info_path));
    CHECK(!VgiBatchFinishRow(&info, &error));
    CHECK(!VgiIpcWriteBatch(&info.schema, &info.array, NULL, &info_bytes, &error));
    for (int64_t i = 0; i < type->n_children; ++i) {
        const char *name = type->children[i]->name;
        struct ArrowArray *child = entry->children[i];
        if (!strcmp(name, "path")) {
            CHECK(!ArrowArrayAppendString(child->children[0], ArrowCharView("main")));
        } else if (!strcmp(name, "schema")) {
            CHECK(!ArrowArrayAppendBytes(
                child, (struct ArrowBufferView){{info_bytes.data}, info_bytes.size_bytes}));
            continue;
        } else if (!strcmp(name, "table_functions") && function) {
            CHECK(!ArrowArrayAppendBytes(
                child->children[0],
                (struct ArrowBufferView){{function->data}, function->size_bytes}));
        }
        CHECK(!ArrowArrayFinishElement(child));
    }
    CHECK(!ArrowArrayFinishElement(entry));
    CHECK(!ArrowArrayFinishElement(out->array.children[2]));
    CHECK(!ArrowArrayFinishElement(&out->array));
    CHECK(!VgiBatchFinish(out, &error));
    VgiBatchReset(&info);
    ArrowBufferReset(&info_bytes);
    ArrowSchemaRelease(&schema);
}
static int register_catalog(struct VgiWorker *worker, const char *name, int64_t max_workers,
                            int with_function, const char *etag) {
    struct VgiBatch info = {0}, attach = {0}, contents = {0};
    struct ArrowBuffer function;
    ArrowBufferInit(&function);
    if (with_function) entry_bytes(max_workers, &function);
    CHECK(!VgiBatchInitNamed(&info, "CatalogInfo", &error));
    CHECK(!VgiBatchSetString(&info, "name", name));
    CHECK(!VgiBatchFinishRow(&info, &error));
    CHECK(!VgiBatchInitNamed(&attach, "CatalogAttachResult", &error));
    CHECK(!VgiBatchFinishRow(&attach, &error));
    contents_init(&contents, with_function ? &function : NULL, etag);
    int rc = VgiWorkerRegisterCatalog(worker, &info, &attach, &contents, NULL, &error);
    VgiBatchReset(&info);
    VgiBatchReset(&attach);
    VgiBatchReset(&contents);
    ArrowBufferReset(&function);
    return rc;
}
static int add_snapshot(struct VgiWorker *worker, const char *name, int64_t max_workers) {
    struct VgiBatch contents = {0};
    struct ArrowBuffer function;
    entry_bytes(max_workers, &function);
    contents_init(&contents, &function, NULL);
    int rc = VgiWorkerAddCatalogSnapshot(worker, name, "2.0.0", &contents, NULL, &error);
    VgiBatchReset(&contents);
    ArrowBufferReset(&function);
    return rc;
}
static void refused(int rc) {
    CHECK(rc == EINVAL);
    CHECK(strstr(error.message, "max_workers = 1"));
}

static void check_parameter_default_values(void) {
    struct VgiWorker *worker = VgiWorkerNew("cat");
    CHECK(worker);
    struct VgiFunction fn;
    function_init(&fn, "defaults", VGI_TABLE, 0);
    /* The defaults are one IPC row of the arguments that have them. */
    struct ArrowSchema schema = {0};
    struct VgiBatch defaults = {0};
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    int64_struct(&schema, "n");
    CHECK(!VgiBatchInit(&defaults, &schema, &error));
    CHECK(!VgiBatchSetInt(&defaults, "n", 42));
    CHECK(!VgiBatchFinishRow(&defaults, &error));
    CHECK(!VgiIpcWriteBatch(&defaults.schema, &defaults.array, NULL, &bytes, &error));
    CHECK(VgiFunctionSetBytes(&fn, "no_such_field", bytes.data, bytes.size_bytes) == ENOENT);
    CHECK(!VgiFunctionSetBytes(&fn, "parameter_default_values", bytes.data, bytes.size_bytes));
    CHECK(!VgiBatchFinishRow(&fn.info, &error));
    struct ArrowBufferView stored = VgiBatchBytes(&fn.info, "parameter_default_values");
    CHECK(stored.size_bytes == bytes.size_bytes);
    CHECK(!memcmp(stored.data.data, bytes.data, (size_t)bytes.size_bytes));
    CHECK(!VgiWorkerRegisterFunction(worker, &fn, &error));
    VgiFunctionReset(&fn);
    ArrowBufferReset(&bytes);
    VgiBatchReset(&defaults);
    ArrowSchemaRelease(&schema);
    VgiWorkerFree(worker);
}

static void check_shared_connection_refusal(void) {
    /* Function first, then a catalog serving it with max_workers = 1. */
    struct VgiWorker *worker = VgiWorkerNew("cat");
    CHECK(worker);
    CHECK(!register_resumable(worker));
    refused(register_catalog(worker, "cat", 1, 1, NULL));
    CHECK(!register_catalog(worker, "other", 1, 1, NULL)); /* a different catalog's paged */
    CHECK(!register_catalog(worker, "cat", 4, 1, NULL));
    VgiWorkerFree(worker);

    /* Catalog first, then the resumable function; a plain one is still fine. */
    worker = VgiWorkerNew("cat");
    CHECK(worker);
    CHECK(!register_catalog(worker, "cat", 1, 1, NULL));
    refused(register_resumable(worker));
    struct VgiFunction fn;
    function_init(&fn, "paged", VGI_TABLE_IN_OUT, 0);
    CHECK(!VgiWorkerRegisterFunction(worker, &fn, &error));
    VgiFunctionReset(&fn);
    VgiWorkerFree(worker);

    /* A null max_workers is fine; a snapshot that says 1 is not. */
    worker = VgiWorkerNew("cat");
    CHECK(worker);
    CHECK(!register_resumable(worker));
    CHECK(!register_catalog(worker, "cat", -1, 1, NULL));
    refused(add_snapshot(worker, "cat", 1));
    CHECK(!add_snapshot(worker, "cat", 2));
    VgiWorkerFree(worker);

    /* The registered FunctionInfo itself. */
    worker = VgiWorkerNew("cat");
    CHECK(worker);
    function_init(&fn, "paged", VGI_TABLE_IN_OUT, 1);
    CHECK(!VgiBatchSetInt(&fn.info, "max_workers", 1));
    refused(VgiWorkerRegisterFunction(worker, &fn, &error));
    VgiFunctionReset(&fn);
    VgiWorkerFree(worker);
}

static const struct VgiMethod *method(const char *name) {
    for (size_t i = 0; i < VGI_METHOD_COUNT; ++i)
        if (!strcmp(VGI_METHODS[i].name, name)) return &VGI_METHODS[i];
    CHECK(0);
    return NULL;
}
/* Ask for catalog_contents with an optional if_none_match; decode the result. */
static void contents_call(struct VgiWorker *worker, const char *if_none_match,
                          struct VgiBatch *result) {
    struct VgiRpcCall call;
    struct VgiRpcReply reply;
    memset(&call, 0, sizeof(call));
    memset(&reply, 0, sizeof(reply));
    call.method = method("catalog_contents");
    struct ArrowSchema params = {0};
    CHECK(!call.method->params(&params, &error));
    CHECK(!VgiBatchInit(&call.request, &params, &error));
    const char *cookie = "{\"catalog\":\"etagged\"}";
    CHECK(!VgiBatchSetBytes(&call.request, "attach_opaque_data", cookie, (int64_t)strlen(cookie)));
    CHECK(!VgiBatchSetString(&call.request, "if_none_match", if_none_match));
    CHECK(!VgiBatchFinishRow(&call.request, &error));
    int handled = 0;
    int rc = VgiWorkerCatalogDefault(worker, &call, &reply, &handled);
    if (rc) fprintf(stderr, "catalog_contents: %s\n", call.error.message);
    CHECK(!rc && handled);
    CHECK(!VgiBatchDecode(result, VgiBatchBytes(&reply.value, "result"), &error));
    VgiBatchReset(&call.request);
    VgiBatchReset(&reply.value);
    if (reply.metadata.data) ArrowBufferReset(&reply.metadata);
    ArrowSchemaRelease(&params);
}
static void check_contents_etag(void) {
    struct VgiWorker *worker = VgiWorkerNew("etagged");
    CHECK(worker);
    CHECK(!register_catalog(worker, "etagged", -1, 0, "\"e1\""));
    const char *requests[] = {"\"e1\"", "\"e0\"", NULL};
    for (int i = 0; i < 3; ++i) {
        struct VgiBatch result = {0};
        contents_call(worker, requests[i], &result);
        struct ArrowStringView etag = VgiBatchString(&result, "etag");
        CHECK(etag.size_bytes == 4 && !memcmp(etag.data, "\"e1\"", 4));
        CHECK(VgiBatchInt(&result, "not_modified", -1) == (i == 0));
        CHECK(VgiBatchIsNull(&result, "schemas") == (i == 0));
        VgiBatchReset(&result);
    }
    VgiWorkerFree(worker);
}

int main(void) {
    check_parameter_default_values();
    check_shared_connection_refusal();
    check_contents_etag();
    puts("FunctionInfo binary fields, max_workers = 1 refusals, and contents etag passed");
    return 0;
}
