#include "fixtures.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "catalog_data.h"
#include "fixture_data.h"
#include "vgi/catalog.h"

static void release_function(void *data) {
    struct ExampleFunction *function = data;
    if (function->output.release) ArrowSchemaRelease(&function->output);
    if (function->arguments.release) ArrowSchemaRelease(&function->arguments);
    free(function);
}
static int register_catalogs(struct VgiWorker *worker, struct ArrowError *error) {
    struct VgiBatch catalogs = {0};
    int rc = VgiBatchDecode(&catalogs,
                            (struct ArrowBufferView){{catalog_data}, sizeof(catalog_data)}, error);
    if (!rc) rc = ExampleFixturePaths(&catalogs, error);
    const char *primary = getenv("VGI_WORKER_CATALOG_NAME");
    if (!primary || !*primary) primary = "example";
    for (int64_t row = 0; !rc && row < catalogs.array.length; ++row) {
        struct ArrowStringView owner =
            VgiValueString(VgiBatchGet(&catalogs, "fixture_primary"), row);
        if (owner.size_bytes != (int64_t)strlen(primary) ||
            memcmp(owner.data, primary, (size_t)owner.size_bytes))
            continue;
        struct ArrowStringView version =
            VgiValueString(VgiBatchGet(&catalogs, "data_version"), row);

        struct VgiBatch info = {0}, attach = {0}, contents = {0}, extras = {0};
        struct VgiBatch *destinations[] = {&info, &attach, &contents};
        const char *names[] = {"info", "attach", "contents"};
        for (int i = 0; !rc && i < 3; ++i) {
            const struct ArrowArrayView *col = VgiBatchGet(&catalogs, names[i]);
            struct ArrowStringView bytes = VgiValueString(col, row);
            rc = VgiBatchDecode(destinations[i],
                                (struct ArrowBufferView){{bytes.data}, bytes.size_bytes}, error);
        }
        int64_t id = VgiFieldIndex(&catalogs.schema, "extras");
        if (!rc && id >= 0) {
            const struct ArrowSchema *schema = catalogs.schema.children[id]->children[0];
            const struct ArrowArrayView *list = catalogs.view.children[id];
            rc = VgiBatchInit(&extras, schema, error);
            int64_t start = ArrowArrayViewListChildOffset(list, row),
                    end = ArrowArrayViewListChildOffset(list, row + 1);
            for (int64_t i = start; !rc && i < end; ++i)
                rc = VgiArrayAppendValue(&extras.array, &extras.schema, list->children[0], schema,
                                         i, error);
            if (!rc) rc = VgiBatchFinish(&extras, error);
        }
        if (!rc && version.size_bytes) {
            char text[64];
            if (version.size_bytes >= 64)
                rc = EINVAL;
            else {
                memcpy(text, version.data, (size_t)version.size_bytes);
                text[version.size_bytes] = 0;
                rc = VgiWorkerAddCatalogSnapshot(worker, primary, text, &contents, &extras, error);
            }
        } else if (!rc)
            rc = VgiWorkerRegisterCatalog(worker, &info, &attach, &contents, &extras, error);
        VgiBatchReset(&info);
        VgiBatchReset(&attach);
        VgiBatchReset(&contents);
        VgiBatchReset(&extras);
    }
    if (!rc && (!strcmp(primary, "versioned") || !strcmp(primary, "versioned_tables"))) {
        int tables = !strcmp(primary, "versioned_tables");
        const char *data_simple[] = {"1.0.0", "1.1.0", "1.2.0"},
                   *data_tables[] = {"1.0.0", "1.1.0", "2.0.0", "3.0.0"};
        const char *impl_simple[] = {"1.0.0"}, *impl_tables[] = {"10.0.0", "10.1.0", "11.0.0"};
        struct VgiCatalogVersions versions = {tables ? data_tables : data_simple,
                                              tables ? 4 : 3,
                                              tables ? "3.0.0" : "1.2.0",
                                              tables ? impl_tables : impl_simple,
                                              tables ? 3 : 1,
                                              tables ? "11.0.0" : "1.0.0",
                                              tables};
        rc = VgiWorkerSetCatalogVersions(worker, primary, &versions, error);
    }
    VgiBatchReset(&catalogs);
    return rc;
}
static int register_hidden(struct VgiWorker *worker, struct ArrowError *error) {
    struct ExampleFunction *data = calloc(1, sizeof(*data));
    if (!data) return ENOMEM;
    strcpy(data->name, "cache_multicol");
    strcpy(data->path, "main");
    strcpy(data->catalog, "example");
    const char *override = getenv("VGI_WORKER_CATALOG_NAME");
    if (override && *override) snprintf(data->catalog, sizeof(data->catalog), "%s", override);
    struct VgiFunction fn = {0};
    fn.userdata = data;
    fn.release = release_function;
    ArrowSchemaInit(&data->arguments);
    int rc = ArrowSchemaSetTypeStruct(&data->arguments, 2);
    for (int i = 0; !rc && i < 2; ++i) {
        ArrowSchemaRelease(data->arguments.children[i]);
        rc = VgiArgumentInit(data->arguments.children[i], i ? "ttl" : "n", "l", 1, 1, 0, error);
    }
    struct VgiFunctionCall context = {0};
    if (!rc) rc = ExampleCacheBind(data, &context, &data->output);
    if (!rc)
        rc = VgiFunctionInit(&fn, data->name, VGI_TABLE, &data->arguments, &data->output, error);
    if (rc) {
        VgiFunctionReset(&fn);
        return rc;
    }
    fn.catalog = data->catalog;
    fn.schema_path = data->path;
    fn.bind = ExampleCacheBind;
    fn.init = ExampleCacheInit;
    fn.process = ExampleCacheProcess;
    fn.finalize = ExampleCacheFree;
    if (!rc) rc = VgiWorkerRegisterFunction(worker, &fn, error);
    VgiFunctionReset(&fn);
    return rc;
}
int ExampleRegister(struct VgiWorker *worker, struct ArrowError *error) {
    struct VgiBatch catalog = {0};
    int rc = VgiBatchDecode(&catalog,
                            (struct ArrowBufferView){{fixture_data}, sizeof(fixture_data)}, error);
    int64_t name_index = VgiFieldIndex(&catalog.schema, "name"),
            kind_index = VgiFieldIndex(&catalog.schema, "function_type");
    for (int64_t i = 0; !rc && i < catalog.array.length; ++i) {
        struct ArrowStringView name = VgiValueString(catalog.view.children[name_index], i);
        struct ArrowStringView kind = VgiValueString(catalog.view.children[kind_index], i);
        struct ExampleFunction *data = calloc(1, sizeof(*data));
        if (!data) {
            rc = ENOMEM;
            break;
        }
        snprintf(data->name, sizeof(data->name), "%.*s", (int)name.size_bytes, name.data);
        int aggregate = kind.size_bytes == 9 &&
                        (!memcmp(kind.data, "AGGREGATE", 9) || !memcmp(kind.data, "aggregate", 9));
        int table = kind.size_bytes == 5 &&
                    (!memcmp(kind.data, "TABLE", 5) || !memcmp(kind.data, "table", 5));
        int buffering = ExampleBufferSupported(data->name);
        if (buffering   ? 0
            : table     ? !ExampleTableSupported(data->name)
            : aggregate ? !ExampleAggregateSupported(data->name)
                        : !ExampleScalarSupported(data->name) || kind.size_bytes != 6 ||
                              (memcmp(kind.data, "SCALAR", 6) && memcmp(kind.data, "scalar", 6))) {
            ArrowErrorSet(error, "No C implementation for captured fixture %s", data->name);
            free(data);
            rc = ENOTSUP;
            break;
        }
        struct VgiFunction function = {0};
        function.kind = table ? (ExampleTableIsExchange(data->name) ? VGI_TABLE_IN_OUT : VGI_TABLE)
                        : aggregate ? VGI_AGGREGATE
                                    : VGI_SCALAR;
        function.userdata = data;
        function.release = release_function;
        rc = VgiBatchInitNamed(&function.info, "FunctionInfo", error);
        if (!rc)
            rc = VgiArrayAppendValue(&function.info.array, &function.info.schema, &catalog.view,
                                     &catalog.schema, i, error);
        if (!rc) rc = VgiBatchFinish(&function.info, error);
        if (!rc)
            rc = VgiIpcReadSchema(VgiBatchBytes(&function.info, "arguments"), &function.arguments,
                                  error);
        if (!rc)
            rc = VgiIpcReadSchema(VgiBatchBytes(&function.info, "output_schema"), &function.output,
                                  error);
        if (!rc) rc = ArrowSchemaDeepCopy(&function.output, &data->output);
        if (!rc) rc = ArrowSchemaDeepCopy(&function.arguments, &data->arguments);
        const struct ArrowArrayView *path = VgiBatchGet(&function.info, "schema_path");
        if (!rc && path && !ArrowArrayViewIsNull(path, 0)) {
            int64_t start = ArrowArrayViewListChildOffset(path, 0),
                    end = ArrowArrayViewListChildOffset(path, 1);
            size_t length = 0;
            for (int64_t j = start; j < end; ++j) {
                struct ArrowStringView part = VgiValueString(path->children[0], j);
                int written = snprintf(data->path + length, sizeof(data->path) - length, "%s%.*s",
                                       j > start ? "." : "", (int)part.size_bytes, part.data);
                if (written < 0 || (size_t)written >= sizeof(data->path) - length) {
                    rc = EINVAL;
                    break;
                }
                length += (size_t)written;
            }
        }
        function.schema_path = data->path[0] ? data->path : "main";
        int64_t catalog_index = VgiFieldIndex(&catalog.schema, "fixture_catalog");
        if (catalog_index >= 0) {
            struct ArrowStringView value = VgiValueString(catalog.view.children[catalog_index], i);
            snprintf(data->catalog, sizeof(data->catalog), "%.*s", (int)value.size_bytes,
                     value.data);
            const char *override = getenv("VGI_WORKER_CATALOG_NAME");
            if (override && *override && !strcmp(data->catalog, "example"))
                snprintf(data->catalog, sizeof(data->catalog), "%s", override);
            function.catalog = data->catalog;
        }
        function.bind = ExampleScalarBind;
        function.process = ExampleScalarProcess;
        if (table) {
            function.plan = ExampleTablePlan;
            if (ExampleCacheSupported(data->name)) function.max_workers = ExampleCacheWorkers;
            if (ExamplePartitionSupported(data->name))
                function.max_workers = ExamplePartitionWorkers;
            if (ExampleStaticSupported(data->name)) function.max_workers = ExampleStaticWorkers;
            if (!strcmp(data->name, "scoped_secret_demo"))
                function.secret_lookups = ExampleContextLookups;
            function.bind = ExampleTableBind;
            function.init = ExampleTableInit;
            function.process = ExampleTableProcess;
            function.finalize = ExampleTableFinalize;
            function.auto_apply_filters =
                !strcmp(data->name, "rff_rowid_scan") ||
                !strcmp(data->name, "filter_echo_table_scan") ||
                !strncmp(data->name, "tt_pushdown", 11) ||
                !strcmp(data->name, "late_materialization") ||
                !strcmp(data->name, "filter_echo_partitioned") ||
                !strcmp(data->name, "split_dynamic_filter") || ExampleFilterSupported(data->name) ||
                !strcmp(data->name, "order_echo") ||
                (!strncmp(data->name, "cache_partition", 15) &&
                 strcmp(data->name, "cache_partitioned")) ||
                !strcmp(data->name, "cache_filtered") || !strcmp(data->name, "sequence") ||
                !strcmp(data->name, "double_sequence");
        }
        if (buffering) {
            function.kind = kind.size_bytes == 5 ? VGI_TABLE_IN_OUT : VGI_BUFFERING;
            function.bind = ExampleBufferBind;
            function.init = ExampleBufferInit;
            function.process = ExampleBufferProcess;
            function.finalize = ExampleBufferRelease;
            function.buffer_process = ExampleBufferSink;
            function.buffer_combine = ExampleBufferCombine;
        }
        if (aggregate) {
            function.bind = ExampleAggregateBind;
            function.process = NULL;
            function.aggregate_update = ExampleAggregateUpdate;
            function.aggregate_combine = ExampleAggregateCombine;
            function.aggregate_finalize = ExampleAggregateFinalize;
        }
        if (ExampleAccumulateSupported(data->name)) {
            function.bind = ExampleAccumulateBind;
            function.init = ExampleAccumulateInit;
            function.process = ExampleAccumulateProcess;
            function.finalize = ExampleAccumulateFree;
            function.buffer_process = ExampleAccumulateSink;
            function.buffer_combine = ExampleAccumulateCombine;
        }
        if (ExampleCopyReader(data->name) || ExampleCopyWriter(data->name)) {
            function.bind = ExampleCopyBind;
            function.secret_lookups = ExampleCopyLookups;
            if (ExampleCopyWriter(data->name)) {
                function.init = NULL;
                function.finalize = NULL;
                function.process = NULL;
                function.buffer_process = ExampleCopyWrite;
                function.buffer_combine = ExampleCopyClose;
            }
            if (!rc) rc = ExampleCopyRegister(worker, &function, error);
        }
        if (!rc) rc = VgiWorkerRegisterFunction(worker, &function, error);
        if (rc) fprintf(stderr, "vgi-c: registering %s: %s\n", data->name, error->message);
        VgiFunctionReset(&function);
    }
    VgiBatchReset(&catalog);
    if (!rc) rc = register_hidden(worker, error);
    const char *primary = getenv("VGI_WORKER_CATALOG_NAME");
    int composite = !primary || !*primary || !strcmp(primary, "example");
    const char *memory_names[] = {"contents_memory", "contents_reval", "contents_hash"};
    for (int i = 0; !rc && composite && i < 3; ++i) {
        struct VgiMemoryCatalogOptions options = {
            memory_names[i], "Mutable catalog_contents fixture", i == 0, (enum VgiCatalogEtag)i};
        rc = VgiWorkerRegisterMemoryCatalog(worker, &options, error);
    }
    if (!rc) rc = register_catalogs(worker, error);
    if (!rc) VgiWorkerSetCatalogHandler(worker, ExampleCatalogHandler, worker);
    return rc;
}
