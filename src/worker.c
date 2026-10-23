#include "vgi/worker.h"

#include <ctype.h>
#include <errno.h>
#include <json-c/json.h>
#include <limits.h>
#include <math.h>
#include <signal.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>

#include "catalog_internal.h"
#include "filter_internal.h"
#include "memory_catalog_internal.h"
#include "opaque_internal.h"
#include "split_internal.h"
#include "tickets_internal.h"
#include "vgi/catalog.h"
#include "vgi/copy.h"
#include "vgi/http.h"
#include "vgi/split.h"

struct RegisteredFunction {
    struct VgiFunction function;
    char *catalog;
    char *schema;
    char *name;
};
struct VgiWorker {
    struct VgiHttpOptions http_options;
    int has_http_options;
    VgiCatalogHandler catalog_handler;
    void *catalog_state;
    char *catalog;
    struct VgiCatalogService *catalog_service;
    struct VgiMemoryService *memory_service;
    struct RegisteredFunction *functions;
    size_t function_count;
    struct VgiRpcServer *rpc;
    struct VgiBatch *copy_formats;
    size_t copy_format_count;
};
static char *text_copy(const char *text) {
    size_t size = strlen(text) + 1;
    char *out = malloc(size);
    if (out) memcpy(out, text, size);
    return out;
}
static char *text_view(struct ArrowStringView text) {
    char *out = malloc((size_t)text.size_bytes + 1);
    if (out) {
        memcpy(out, text.data, (size_t)text.size_bytes);
        out[text.size_bytes] = 0;
    }
    return out;
}
static int text_is(struct ArrowStringView text, const char *expected) {
    return text.size_bytes == (int64_t)strlen(expected) &&
           !memcmp(text.data, expected, (size_t)text.size_bytes);
}
static int enum_is(struct ArrowStringView text, const char *expected) {
    if (text.size_bytes != (int64_t)strlen(expected)) return 0;
    for (int64_t i = 0; i < text.size_bytes; ++i)
        if (tolower((unsigned char)text.data[i]) != tolower((unsigned char)expected[i])) return 0;
    return 1;
}
static int worker_dispatch(void *state, struct VgiRpcCall *call, struct VgiRpcReply *reply);

int VgiArgumentInit(struct ArrowSchema *out, const char *name, const char *format, int constant,
                    int named, int variadic, struct ArrowError *error) {
    if (!out || out->release || !name || !format) return EINVAL;
    ArrowSchemaInit(out);
    struct ArrowBuffer metadata;
    ArrowBufferInit(&metadata);
    int rc = ArrowSchemaSetName(out, name);
    if (!rc)
        rc = ArrowSchemaSetFormat(
            out, !strcmp(format, "any") || !strcmp(format, "table") ? "n" : format);
    if (!rc && constant) rc = VgiMetadataSet(&metadata, "vgi_const", "true");
    if (!rc && named) rc = VgiMetadataSet(&metadata, "vgi_arg", "named");
    if (!rc && variadic) rc = VgiMetadataSet(&metadata, "vgi_varargs", "true");
    if (!rc && (!strcmp(format, "any") || !strcmp(format, "table")))
        rc = VgiMetadataSet(&metadata, "vgi_type", format);
    if (!rc) rc = ArrowSchemaSetMetadata(out, (const char *)metadata.data);
    out->flags |= ARROW_FLAG_NULLABLE;
    ArrowBufferReset(&metadata);
    if (rc) {
        ArrowSchemaRelease(out);
        ArrowErrorSet(error, "Failed to build argument %s", name);
    }
    return rc;
}
int VgiFunctionInit(struct VgiFunction *function, const char *name, enum VgiFunctionKind kind,
                    const struct ArrowSchema *arguments, const struct ArrowSchema *output,
                    struct ArrowError *error) {
    if (!function || function->info.schema.release || !name || !arguments || !output) return EINVAL;
    function->kind = kind;
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    int rc = ArrowSchemaDeepCopy(arguments, &function->arguments);
    if (!rc) rc = ArrowSchemaDeepCopy(output, &function->output);
    if (!rc) rc = VgiBatchInitNamed(&function->info, "FunctionInfo", error);
    if (!rc) rc = VgiBatchSetString(&function->info, "name", name);
    /* The wire carries FunctionType member names. */
    const char *type = kind == VGI_SCALAR      ? "SCALAR"
                       : kind == VGI_AGGREGATE ? "AGGREGATE"
                       : kind == VGI_BUFFERING ? "TABLE_BUFFERING"
                                               : "TABLE";
    if (!rc) rc = VgiBatchSetString(&function->info, "function_type", type);
    if (!rc) rc = VgiIpcWriteSchema(arguments, &bytes, error);
    if (!rc) rc = VgiFunctionSetBytes(function, "arguments", bytes.data, bytes.size_bytes);
    ArrowBufferReset(&bytes);
    if (!rc) rc = VgiIpcWriteSchema(output, &bytes, error);
    if (!rc) rc = VgiFunctionSetBytes(function, "output_schema", bytes.data, bytes.size_bytes);
    ArrowBufferReset(&bytes);
    if (rc) VgiFunctionReset(function);
    return rc;
}
/* Wire 2.2 dictionary-encodes FunctionInfo's binary fields (arguments,
 * output_schema, parameter_default_values); append either form. */
int VgiFunctionSetBytes(struct VgiFunction *function, const char *name, const void *data,
                        int64_t size) {
    if (!function || !function->info.array.release || !name || size < 0 || (!data && size))
        return EINVAL;
    struct ArrowArray *col = VgiBatchColumn(&function->info, name);
    if (!col) return ENOENT;
    if (!data) return ArrowArrayAppendNull(col, 1);
    struct ArrowBufferView value = {{data}, size};
    if (col->dictionary) {
        int64_t index = col->dictionary->length;
        NANOARROW_RETURN_NOT_OK(ArrowArrayAppendBytes(col->dictionary, value));
        return ArrowArrayAppendInt(col, index);
    }
    return ArrowArrayAppendBytes(col, value);
}
void VgiFunctionReset(struct VgiFunction *function) {
    VgiBatchReset(&function->info);
    if (function->arguments.release) ArrowSchemaRelease(&function->arguments);
    if (function->output.release) ArrowSchemaRelease(&function->output);
    if (function->release) function->release(function->userdata);
    memset(function, 0, sizeof(*function));
}
struct VgiWorker *VgiWorkerNew(const char *catalog) {
    struct VgiWorker *worker = calloc(1, sizeof(*worker));
    if (!worker) return NULL;
    worker->catalog = text_copy(catalog ? catalog : "example");
    worker->rpc = VgiRpcServerNew();
    if (!worker->catalog || !worker->rpc) {
        VgiWorkerFree(worker);
        return NULL;
    }
    const char *version = getenv("VGI_PROTOCOL_VERSION_OVERRIDE");
    struct VgiRpcProtocol protocol = {VGI_PROTOCOL_NAME,
                                      version ? version : VGI_PROTOCOL_VERSION,
                                      VGI_PROTOCOL_HASH,
                                      VGI_METHODS,
                                      VGI_METHOD_COUNT,
                                      worker_dispatch,
                                      worker,
                                      NULL};
    if (VgiRpcServerAddProtocol(worker->rpc, &protocol, NULL)) {
        VgiWorkerFree(worker);
        return NULL;
    }
    return worker;
}
void VgiWorkerFree(struct VgiWorker *worker) {
    if (!worker) return;
    for (size_t i = 0; i < worker->function_count; ++i) {
        VgiFunctionReset(&worker->functions[i].function);
        free(worker->functions[i].catalog);
        free(worker->functions[i].schema);
        free(worker->functions[i].name);
    }
    free(worker->functions);
    for (size_t i = 0; i < worker->copy_format_count; ++i) VgiBatchReset(&worker->copy_formats[i]);
    free(worker->copy_formats);
    VgiCatalogFree(worker->catalog_service);
    VgiMemoryFree(worker->memory_service);
    VgiRpcServerFree(worker->rpc);
    free(worker->catalog);
    free(worker);
}
static int set_default_string(struct VgiBatch *batch, const char *name, const char *value) {
    struct ArrowArray *col = VgiBatchColumn(batch, name);
    return col && !col->length ? VgiBatchSetString(batch, name, value) : 0;
}
static int append_path(struct ArrowArray *list, const char *path) {
    const char *start = path;
    for (const char *p = path;; ++p) {
        if (*p == '.' || !*p) {
            NANOARROW_RETURN_NOT_OK(VgiArrayAppendString(
                list->children[0], (struct ArrowStringView){start, p - start}));
            start = p + 1;
        }
        if (!*p) break;
    }
    return ArrowArrayFinishElement(list);
}
/* Refuse what the client cannot drive. resume receives no input, so it continues
 * from the state init creates; a catalog max_workers of 1 runs every pipeline on
 * one shared connection, which cannot leave output pending. input_rows_per_call
 * splits the per-row input of a blended function only. */
static int check_registration(const struct VgiFunction *function, struct ArrowError *error) {
    struct ArrowStringView name = VgiBatchString(&function->info, "name");
    if (function->resume && function->kind != VGI_TABLE_IN_OUT) {
        ArrowErrorSet(error,
                      "%.*s: resume (continued output) applies only to a table-in-out function",
                      (int)name.size_bytes, name.data);
        return EINVAL;
    }
    if (function->resume && !function->init) {
        ArrowErrorSet(error,
                      "%.*s: resume continues from the state init creates; register an init "
                      "callback that sets call->state",
                      (int)name.size_bytes, name.data);
        return EINVAL;
    }
    if (function->resume && !VgiBatchIsNull(&function->info, "max_workers") &&
        VgiBatchInt(&function->info, "max_workers", 0) == 1) {
        ArrowErrorSet(error,
                      "%.*s: resume (continued output) is not supported with max_workers = 1; the "
                      "client runs such a function on one shared connection",
                      (int)name.size_bytes, name.data);
        return EINVAL;
    }
    if (!VgiBatchIsNull(&function->info, "input_rows_per_call")) {
        int64_t rows = VgiBatchInt(&function->info, "input_rows_per_call", 0);
        if (!VgiBatchInt(&function->info, "input_from_args", 0)) {
            ArrowErrorSet(error,
                          "%.*s: input_rows_per_call applies only to a blended (input_from_args) "
                          "function, whose per-row input the client can split into calls",
                          (int)name.size_bytes, name.data);
            return EINVAL;
        }
        if (rows < 1) {
            ArrowErrorSet(error, "%.*s: input_rows_per_call must be a positive integer, got %lld",
                          (int)name.size_bytes, name.data, (long long)rows);
            return EINVAL;
        }
    }
    return 0;
}
/* The extension decides whether a function runs on one shared connection from
 * the FunctionInfo the catalog serves, so a declarative catalog entry that says
 * max_workers = 1 is refused for a resumable function as the registered
 * FunctionInfo is, whichever of the two is added first. */
static int refuse_shared_entry(const struct VgiFunction *function, const char *catalog,
                               const char *schema, const struct VgiBatch *entry,
                               struct ArrowError *error) {
    struct ArrowStringView name = VgiBatchString(&function->info, "name"),
                           served = VgiBatchString(entry, "name");
    if (!function->resume || !name.data || !served.data || name.size_bytes != served.size_bytes ||
        memcmp(name.data, served.data, (size_t)name.size_bytes) ||
        strcmp(catalog, function->catalog) || strcmp(schema, function->schema_path) ||
        VgiBatchIsNull(entry, "max_workers") || VgiBatchInt(entry, "max_workers", 0) != 1)
        return 0;
    ArrowErrorSet(error,
                  "%.*s: resume (continued output) is not supported with max_workers = 1, which "
                  "catalog %s advertises for %s.%.*s; the client runs such a function on one "
                  "shared connection",
                  (int)name.size_bytes, name.data, catalog, schema, (int)name.size_bytes,
                  name.data);
    return EINVAL;
}
struct EntryCheck {
    const struct VgiFunction *function;
    struct VgiWorker *worker;
    const char *catalog;
};
static int check_entry_against_function(void *state, const char *catalog, const char *schema,
                                        const struct VgiBatch *entry, struct ArrowError *error) {
    struct EntryCheck *check = state;
    return refuse_shared_entry(check->function, catalog, schema, entry, error);
}
static int check_entry_against_worker(void *state, const char *schema, const struct VgiBatch *entry,
                                      struct ArrowError *error) {
    struct EntryCheck *check = state;
    int rc = 0;
    for (size_t i = 0; !rc && i < check->worker->function_count; ++i)
        rc = refuse_shared_entry(&check->worker->functions[i].function, check->catalog, schema,
                                 entry, error);
    return rc;
}
static int check_catalog_contents(struct VgiWorker *worker, const char *catalog,
                                  const struct VgiBatch *contents, struct ArrowError *error) {
    if (!worker || !catalog || !contents) return 0;
    struct EntryCheck check = {NULL, worker, catalog};
    return VgiCatalogContentsFunctions(contents, check_entry_against_worker, &check, error);
}
int VgiWorkerRegisterFunction(struct VgiWorker *worker, struct VgiFunction *function,
                              struct ArrowError *error) {
    if (!worker || !function || !function->info.array.release) return EINVAL;
    const char *path = function->schema_path ? function->schema_path : "main";
    const char *catalog = function->catalog ? function->catalog : worker->catalog;
    int rc = set_default_string(&function->info, "stability", "CONSISTENT");
    if (!rc) rc = set_default_string(&function->info, "null_handling", "DEFAULT");
    if (!rc) rc = set_default_string(&function->info, "partition_kind", "NOT_PARTITIONED");
    if (!rc) rc = set_default_string(&function->info, "order_dependent", "NOT_ORDER_DEPENDENT");
    if (!rc)
        rc = set_default_string(&function->info, "distinct_dependent", "NOT_DISTINCT_DEPENDENT");
    struct ArrowArray *schema_path = VgiBatchColumn(&function->info, "schema_path");
    if (!rc && !schema_path->length) rc = append_path(schema_path, path);
    if (!rc && !function->info.array.length) rc = VgiBatchFinishRow(&function->info, error);
    if (!rc) rc = check_registration(function, error);
    if (!rc && function->resume) {
        struct VgiFunction placed = *function;
        placed.catalog = catalog;
        placed.schema_path = path;
        struct EntryCheck check = {&placed, worker, NULL};
        rc = VgiCatalogFunctions(worker->catalog_service, check_entry_against_function, &check,
                                 error);
    }
    if (rc) return rc;
    struct RegisteredFunction owned = {0};
    owned.catalog = text_copy(catalog);
    owned.schema = text_copy(path);
    owned.name = text_view(VgiBatchString(&function->info, "name"));
    struct RegisteredFunction *next =
        realloc(worker->functions, (worker->function_count + 1) * sizeof(*next));
    if (!next || !owned.catalog || !owned.schema || !owned.name) {
        if (next) worker->functions = next;
        free(owned.catalog);
        free(owned.schema);
        free(owned.name);
        return ENOMEM;
    }
    owned.function = *function;
    owned.function.catalog = owned.catalog;
    owned.function.schema_path = owned.schema;
    worker->functions = next;
    worker->functions[worker->function_count++] = owned;
    memset(function, 0, sizeof(*function));
    return 0;
}
static char *schema_path(const struct VgiBatch *batch) {
    const char *field = VgiFieldIndex(&batch->schema, "schema_path") >= 0 ? "schema_path" : "path";
    const struct ArrowArrayView *list = VgiBatchGet(batch, field);
    if (!list || VgiBatchIsNull(batch, field)) return text_copy("main");
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    int64_t start = ArrowArrayViewListChildOffset(list, 0),
            end = ArrowArrayViewListChildOffset(list, 1);
    int rc = 0;
    for (int64_t i = start; !rc && i < end; ++i) {
        struct ArrowStringView value = ArrowArrayViewGetStringUnsafe(list->children[0], i);
        if (i > start) rc = ArrowBufferAppend(&bytes, ".", 1);
        if (!rc) rc = ArrowBufferAppend(&bytes, value.data, value.size_bytes);
    }
    if (!rc) rc = ArrowBufferAppend(&bytes, "", 1);
    char *result = rc ? NULL : text_copy(bytes.size_bytes > 1 ? (const char *)bytes.data : "main");
    ArrowBufferReset(&bytes);
    return result;
}
static char *catalog_of(struct VgiWorker *worker, const struct VgiBatch *batch) {
    struct ArrowStringView value = VgiBatchString(batch, "attach_opaque_data");
    struct json_object *object = NULL;
    /* A length-bounded parser is needed because binary cookies are not terminated. */
    if (value.data && value.size_bytes <= INT_MAX) {
        struct json_tokener *parser = json_tokener_new();
        if (!parser) return NULL;
        object = json_tokener_parse_ex(parser, value.data, (int)value.size_bytes);
        json_tokener_free(parser);
    }
    struct json_object *catalog = NULL;
    char *result = text_copy(object && json_object_object_get_ex(object, "catalog", &catalog)
                                 ? json_object_get_string(catalog)
                                 : worker->catalog);
    if (object) json_object_put(object);
    return result;
}
static struct RegisteredFunction *find_function(struct VgiWorker *worker,
                                                const struct VgiBatch *bind) {
    char *path = schema_path(bind), *catalog = catalog_of(worker, bind);
    struct RegisteredFunction *result = NULL;
    struct ArrowStringView name = VgiBatchString(bind, "function_name");
    struct ArrowStringView kind = VgiBatchString(bind, "function_type");
    struct ArrowSchema input = {0};
    struct ArrowBufferView input_bytes = VgiBatchBytes(bind, "input_schema");
    if (input_bytes.size_bytes && VgiIpcReadSchema(input_bytes, &input, NULL)) {
        free(path);
        free(catalog);
        return NULL;
    }
    struct VgiFunctionCall arguments = {0};
    struct ArrowBufferView argument_bytes = VgiBatchBytes(bind, "arguments");
    if (argument_bytes.size_bytes && VgiBatchDecode(&arguments.arguments, argument_bytes, NULL)) {
        if (input.release) ArrowSchemaRelease(&input);
        free(path);
        free(catalog);
        return NULL;
    }
    int64_t positional_count = 0;
    const struct ArrowSchema *wire_args = &arguments.arguments.schema;
    if (wire_args->n_children == 1 && !strcmp(wire_args->children[0]->name, "args"))
        wire_args = wire_args->children[0];
    for (int64_t i = 0; i < wire_args->n_children; ++i)
        if (!strncmp(wire_args->children[i]->name, "positional_", 11)) ++positional_count;
    int best_score = -1;
    if (getenv("VGI_TRACE"))
        fprintf(stderr, "vgi-c: resolve %s.%s.%.*s (%.*s) among %zu\n", catalog, path,
                (int)name.size_bytes, name.data, (int)kind.size_bytes, kind.data,
                worker->function_count);
    for (size_t i = 0; path && catalog && i < worker->function_count; ++i) {
        struct RegisteredFunction *fn = &worker->functions[i];
        if (!strcmp(path, fn->schema) && !strcmp(catalog, fn->catalog) && text_is(name, fn->name) &&
            (!kind.data || (enum_is(kind, "table") && fn->function.kind == VGI_BUFFERING) ||
             enum_is(VgiBatchString(&fn->function.info, "function_type"),
                     enum_is(kind, "scalar")            ? "scalar"
                     : enum_is(kind, "aggregate")       ? "aggregate"
                     : enum_is(kind, "table_buffering") ? "table_buffering"
                                                        : "table"))) {
            int score = 0, valid = 1;
            arguments.argument_schema = &fn->function.arguments;
            int64_t expected_constants = 0;
            int const_varargs = 0;
            for (int64_t j = 0; valid && j < fn->function.arguments.n_children; ++j) {
                const struct ArrowSchema *argument = fn->function.arguments.children[j];
                if (!VgiMetadataGet(argument->metadata, "vgi_const").data ||
                    VgiMetadataGet(argument->metadata, "vgi_arg").data)
                    continue;
                if (VgiMetadataGet(argument->metadata, "vgi_varargs").data)
                    const_varargs = 1;
                else
                    ++expected_constants;
                const struct ArrowArrayView *value = VgiArgument(&arguments, j, NULL);
                if (!value || !value->length || ArrowArrayViewIsNull(value, 0) ||
                    !strcmp(argument->format, "n"))
                    continue;
                struct ArrowSchemaView type;
                if (ArrowSchemaViewInit(&type, argument, NULL)) {
                    valid = 0;
                    break;
                }
                if (value->storage_type == type.storage_type)
                    score += 100;
                else {
                    struct ArrowArrayView expected = {.storage_type = type.storage_type};
                    int decimal = value->storage_type == NANOARROW_TYPE_DECIMAL32 ||
                                  value->storage_type == NANOARROW_TYPE_DECIMAL64 ||
                                  value->storage_type == NANOARROW_TYPE_DECIMAL128 ||
                                  value->storage_type == NANOARROW_TYPE_DECIMAL256;
                    if (VgiValueIsNumeric(&expected) && (VgiValueIsNumeric(value) || decimal))
                        score += 10;
                    else
                        valid = 0;
                }
            }
            if (argument_bytes.size_bytes &&
                (const_varargs ? positional_count < expected_constants
                               : expected_constants != positional_count))
                valid = 0;
            int64_t column = 0;
            for (int64_t j = 0; valid && input.release && j < fn->function.arguments.n_children;
                 ++j) {
                const struct ArrowSchema *argument = fn->function.arguments.children[j];
                if (VgiMetadataGet(argument->metadata, "vgi_const").data ||
                    VgiMetadataGet(argument->metadata, "vgi_arg").data)
                    continue;
                int variadic = VgiMetadataGet(argument->metadata, "vgi_varargs").data != NULL;
                if (variadic && column == input.n_children) break;
                do {
                    if (column >= input.n_children) {
                        valid = 0;
                        break;
                    }
                    const struct ArrowSchema *actual = input.children[column++];
                    if (actual->dictionary) actual = actual->dictionary;
                    if (!strcmp(argument->format, "n"))
                        ++score;
                    else if (!strcmp(argument->format, actual->format))
                        score += 100;
                    else {
                        struct ArrowSchemaView expected_type, actual_type;
                        if (ArrowSchemaViewInit(&expected_type, argument, NULL) ||
                            ArrowSchemaViewInit(&actual_type, actual, NULL)) {
                            valid = 0;
                            break;
                        }
                        struct ArrowArrayView a = {.storage_type = expected_type.storage_type},
                                              b = {.storage_type = actual_type.storage_type};
                        if (VgiValueIsNumeric(&a) && VgiValueIsNumeric(&b))
                            score += 10;
                        else if (expected_type.type == NANOARROW_TYPE_STRING &&
                                 (actual_type.type == NANOARROW_TYPE_LARGE_STRING ||
                                  actual_type.type == NANOARROW_TYPE_STRING_VIEW))
                            score += 90;
                        else {
                            valid = 0;
                            break;
                        }
                    }
                } while (variadic && column < input.n_children);
            }
            if (valid && score > best_score) {
                result = fn;
                best_score = score;
            }
        }
    }
    if (input.release) ArrowSchemaRelease(&input);
    VgiBatchReset(&arguments.arguments);
    free(path);
    free(catalog);
    return result;
}
static int payload_init(struct VgiRpcCall *call, struct VgiBatch *out) {
    if (!call->method->payload) return EINVAL;
    struct ArrowSchema schema = {0};
    int rc = call->method->payload(&schema, &call->error);
    if (!rc) rc = VgiBatchInit(out, &schema, &call->error);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}
static int append_item(struct ArrowArray *list, const struct VgiBatch *item,
                       struct ArrowError *error) {
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    int rc = VgiIpcWriteBatch(&item->schema, &item->array, NULL, &bytes, error);
    if (!rc)
        rc = ArrowArrayAppendBytes(list->children[0],
                                   (struct ArrowBufferView){{bytes.data}, bytes.size_bytes});
    ArrowBufferReset(&bytes);
    return rc;
}
int VgiWorkerRegisterMemoryCatalog(struct VgiWorker *worker,
                                   const struct VgiMemoryCatalogOptions *options,
                                   struct ArrowError *error) {
    return VgiMemoryAdd(&worker->memory_service, &worker->catalog_service, options, error);
}
int VgiWorkerSetCatalogVersions(struct VgiWorker *worker, const char *name,
                                const struct VgiCatalogVersions *versions,
                                struct ArrowError *error) {
    return VgiCatalogSetVersions(worker->catalog_service, name, versions, error);
}
int VgiWorkerAddCatalogSnapshot(struct VgiWorker *worker, const char *name, const char *version,
                                const struct VgiBatch *contents, const struct VgiBatch *extras,
                                struct ArrowError *error) {
    int rc = check_catalog_contents(worker, name, contents, error);
    return rc ? rc
              : VgiCatalogAddSnapshot(worker->catalog_service, name, version, contents, extras,
                                      error);
}
int VgiWorkerRegisterCatalog(struct VgiWorker *worker, const struct VgiBatch *info,
                             const struct VgiBatch *attach, const struct VgiBatch *contents,
                             const struct VgiBatch *extras, struct ArrowError *error) {
    if (!worker || !info) return EINVAL;
    struct ArrowStringView catalog = VgiBatchString(info, "name");
    char *name = catalog.data ? text_view(catalog) : NULL;
    int rc = name ? check_catalog_contents(worker, name, contents, error) : 0;
    free(name);
    return rc ? rc : VgiCatalogAdd(&worker->catalog_service, info, attach, contents, extras, error);
}
int VgiWorkerRegisterCopyFormat(struct VgiWorker *worker, const struct VgiBatch *info,
                                struct ArrowError *error) {
    if (!worker || !info || info->array.length != 1) return EINVAL;
    struct ArrowStringView name = VgiBatchString(info, "format_name"),
                           handler = VgiBatchString(info, "handler"),
                           direction = VgiBatchString(info, "direction");
    if (!name.size_bytes || !handler.size_bytes ||
        (!text_is(direction, "from") && !text_is(direction, "to") && !text_is(direction, "both")))
        return EINVAL;
    for (size_t i = 0; i < worker->copy_format_count; ++i) {
        struct ArrowStringView other = VgiBatchString(&worker->copy_formats[i], "format_name");
        if (other.size_bytes == name.size_bytes &&
            !memcmp(other.data, name.data, (size_t)name.size_bytes)) {
            ArrowErrorSet(error, "Duplicate COPY format");
            return EEXIST;
        }
    }
    struct VgiBatch owned = {0};
    struct ArrowSchema schema = {0};
    int rc = VgiSchemaCopyFromFormatInfo(&schema, error);
    if (!rc) rc = VgiBatchCoerce(info, &schema, &owned, error);
    if (schema.release) ArrowSchemaRelease(&schema);
    if (!rc) {
        struct VgiBatch *next =
            realloc(worker->copy_formats, (worker->copy_format_count + 1) * sizeof(*next));
        if (!next)
            rc = ENOMEM;
        else {
            worker->copy_formats = next;
            worker->copy_formats[worker->copy_format_count++] = owned;
            return 0;
        }
    }
    VgiBatchReset(&owned);
    return rc;
}
void VgiWorkerSetCatalogHandler(struct VgiWorker *worker, VgiCatalogHandler handler, void *state) {
    worker->catalog_handler = handler;
    worker->catalog_state = state;
}
int VgiWorkerCatalogDefault(struct VgiWorker *worker, struct VgiRpcCall *call,
                            struct VgiRpcReply *reply, int *handled) {
    return VgiCatalogDispatch(worker->catalog_service, worker->catalog, call, reply, handled);
}
static int catalog_dispatch(struct VgiWorker *worker, struct VgiRpcCall *call,
                            struct VgiRpcReply *reply) {
    int ticket_rc = VgiTicketRedeem(call);
    if (ticket_rc) return ticket_rc;
    if (!strcmp(call->method->name, "catalog_copy_from_formats")) {
        struct VgiBatch result = {0};
        int rc = payload_init(call, &result);
        for (size_t i = 0; !rc && i < worker->copy_format_count; ++i)
            rc = append_item(VgiBatchColumn(&result, "items"), &worker->copy_formats[i],
                             &call->error);
        if (!rc) rc = ArrowArrayFinishElement(VgiBatchColumn(&result, "items"));
        if (!rc) rc = VgiBatchFinishRow(&result, &call->error);
        if (!rc) rc = VgiRpcReplyPayload(call, reply, &result);
        VgiBatchReset(&result);
        return rc;
    }
    if (worker->catalog_handler) {
        int custom = 0;
        int rc = worker->catalog_handler(worker->catalog_state, call, reply, &custom);
        if (rc || custom) return rc;
    }
    int handled = 0;
    int catalog_rc =
        VgiCatalogDispatch(worker->catalog_service, worker->catalog, call, reply, &handled);
    if (handled || catalog_rc) return catalog_rc;
    const char *method = call->method->name;
    if (call->method->kind == VGI_METHOD_VOID) {
        if (!strcmp(method, "catalog_detach")) return 0;
        return VgiRpcFail(call, "NotImplementedError", "UNIMPLEMENTED", "method_not_implemented",
                          "catalog is read-only");
    }
    struct VgiBatch payload = {0}, item = {0}, request = {0};
    int rc = payload_init(call, &payload);
    if (rc) return rc;
    struct ArrowArray *items = VgiBatchColumn(&payload, "items");
    if (!strcmp(method, "catalog_catalogs")) {
        for (size_t i = 0; !rc && i <= worker->function_count; ++i) {
            const char *name =
                i == worker->function_count ? worker->catalog : worker->functions[i].catalog;
            int duplicate = 0;
            for (size_t j = 0; j < i; ++j)
                if (!strcmp(worker->functions[j].catalog, name)) duplicate = 1;
            if (duplicate) continue;
            rc = VgiBatchInitNamed(&item, "CatalogInfo", &call->error);
            if (!rc) rc = VgiBatchSetString(&item, "name", name);
            if (!rc) rc = VgiBatchFinishRow(&item, &call->error);
            if (!rc) rc = append_item(items, &item, &call->error);
            VgiBatchReset(&item);
        }
    } else if (!strcmp(method, "catalog_attach")) {
        rc = VgiBatchChild(&call->request, "request", &request, &call->error);
        struct ArrowStringView requested = VgiBatchString(&request, "name");
        char *catalog = requested.data ? text_view(requested) : text_copy(worker->catalog);
        int exists = catalog && !strcmp(catalog, worker->catalog);
        for (size_t i = 0; catalog && i < worker->function_count; ++i)
            if (!strcmp(catalog, worker->functions[i].catalog)) exists = 1;
        if (!exists) {
            free(catalog);
            catalog = text_copy(worker->catalog);
            if (!catalog) rc = ENOMEM;
        }
        struct json_object *cookie = json_object_new_object();
        json_object_object_add(cookie, "catalog",
                               json_object_new_string(catalog ? catalog : worker->catalog));
        free(catalog);
        unsigned char id[16];
        char hex[33];
        randombytes_buf(id, sizeof(id));
        sodium_bin2hex(hex, sizeof(hex), id, sizeof(id));
        json_object_object_add(cookie, "attachment_id", json_object_new_string(hex));
        const char *encoded = json_object_to_json_string_ext(cookie, JSON_C_TO_STRING_PLAIN);
        if (!rc)
            rc =
                VgiBatchSetBytes(&payload, "attach_opaque_data", encoded, (int64_t)strlen(encoded));
        json_object_put(cookie);
        if (!rc) rc = VgiBatchSetString(&payload, "default_schema", "main");
        if (!rc) rc = VgiBatchSetInt(&payload, "catalog_version", 1);
        if (!rc) rc = VgiBatchSetInt(&payload, "catalog_version_frozen", 1);
        if (!rc) rc = VgiBatchSetInt(&payload, "attach_opaque_data_required", 1);
    } else if (!strcmp(method, "catalog_version")) {
        rc = VgiBatchSetInt(&payload, "version", 1);
    } else if (!strcmp(method, "catalog_schemas") || !strcmp(method, "catalog_schema_get")) {
        char *catalog = catalog_of(worker, &call->request);
        char *requested =
            !strcmp(method, "catalog_schema_get") ? schema_path(&call->request) : NULL;
        if (!catalog || (!strcmp(method, "catalog_schema_get") && !requested)) rc = ENOMEM;
        for (size_t i = 0; !rc && i <= worker->function_count; ++i) {
            const char *path = i == worker->function_count ? "main" : worker->functions[i].schema;
            if (i < worker->function_count && strcmp(worker->functions[i].catalog, catalog))
                continue;
            if (requested && strcmp(requested, path)) continue;
            int duplicate = 0;
            for (size_t j = 0; j < i; ++j)
                if (!strcmp(worker->functions[j].catalog, catalog) &&
                    !strcmp(worker->functions[j].schema, path))
                    duplicate = 1;
            if (duplicate) continue;
            rc = VgiBatchInitNamed(&item, "SchemaInfo", &call->error);
            if (!rc) rc = append_path(VgiBatchColumn(&item, "path"), path);
            if (!rc) rc = VgiBatchFinishRow(&item, &call->error);
            if (!rc) rc = append_item(items, &item, &call->error);
            VgiBatchReset(&item);
        }
        free(catalog);
        free(requested);
    } else if (!strcmp(method, "catalog_schema_contents_functions")) {
        char *path = schema_path(&call->request), *catalog = catalog_of(worker, &call->request);
        if (!path || !catalog) rc = ENOMEM;
        for (size_t i = 0; !rc && i < worker->function_count; ++i) {
            const struct RegisteredFunction *fn = &worker->functions[i];
            if (strcmp(fn->catalog, catalog) || strcmp(fn->schema, path)) continue;
            struct ArrowStringView requested_type = VgiBatchString(&call->request, "type");
            struct ArrowStringView declared_type =
                VgiBatchString(&fn->function.info, "function_type");
            if (requested_type.data) {
                char declared[32];
                snprintf(declared, sizeof(declared), "%.*s", (int)declared_type.size_bytes,
                         declared_type.data);
                const char *entry_kind = fn->function.kind == VGI_SCALAR      ? "SCALAR_FUNCTION"
                                         : fn->function.kind == VGI_AGGREGATE ? "AGGREGATE_FUNCTION"
                                                                              : "TABLE_FUNCTION";
                if (!enum_is(requested_type, declared) && !enum_is(requested_type, entry_kind))
                    continue;
            }
            const struct ArrowArrayView *filters = VgiBatchGet(&call->request, "function_types");
            int accepted = 1;
            if (filters && filters->length && !ArrowArrayViewIsNull(filters, 0)) {
                int64_t first = ArrowArrayViewListChildOffset(filters, 0),
                        last = ArrowArrayViewListChildOffset(filters, 1);
                accepted = first == last;
                for (int64_t j = first; j < last; ++j) {
                    const struct ArrowArrayView *value = filters->children[0];
                    int64_t index = value->dictionary ? ArrowArrayViewGetIntUnsafe(value, j) : j;
                    struct ArrowStringView filter = ArrowArrayViewGetStringUnsafe(
                        value->dictionary ? value->dictionary : value, index);
                    const char *kind = fn->function.kind == VGI_SCALAR      ? "SCALAR_FUNCTION"
                                       : fn->function.kind == VGI_AGGREGATE ? "AGGREGATE_FUNCTION"
                                                                            : "TABLE_FUNCTION";
                    if (text_is(filter, kind)) accepted = 1;
                }
            }
            if (accepted) rc = append_item(items, &fn->function.info, &call->error);
        }
        free(path);
        free(catalog);
    } else if (!items) {
        rc = VgiRpcFail(call, "MethodNotImplementedError", "UNIMPLEMENTED",
                        "method_not_implemented", "Catalog method is not implemented");
    }
    if (!rc && items) rc = ArrowArrayFinishElement(items);
    if (!rc) rc = VgiBatchFinishRow(&payload, &call->error);
    if (!rc) rc = VgiRpcReplyPayload(call, reply, &payload);
    VgiBatchReset(&request);
    VgiBatchReset(&item);
    VgiBatchReset(&payload);
    return rc;
}

static void function_call_reset(struct VgiFunctionCall *context) {
    VgiBatchReset(&context->bind_request);
    VgiBatchReset(&context->init_request);
    VgiBatchReset(&context->arguments);
    VgiBatchReset(&context->settings);
    VgiBatchReset(&context->secrets);
    VgiBatchReset(&context->attach_options);
    if (context->input_schema.release) ArrowSchemaRelease(&context->input_schema);
    if (context->output_schema.release) ArrowSchemaRelease(&context->output_schema);
    ArrowBufferReset(&context->execution_id);
    VgiStorageClose(context->storage);
    VgiFiltersFree(context->filters);
    VgiBatchReset(&context->split_payloads);
}
static int read_context(struct VgiFunctionCall *context) {
    struct ArrowError *error = &context->rpc->error;
    const char *names[] = {"arguments", "settings", "secrets"};
    struct VgiBatch *outputs[] = {&context->arguments, &context->settings, &context->secrets};
    int rc = 0;
    for (int i = 0; !rc && i < 3; ++i) {
        struct ArrowBufferView value = VgiBatchBytes(&context->bind_request, names[i]);
        if (value.size_bytes) rc = VgiBatchDecode(outputs[i], value, error);
    }
    struct ArrowBufferView schema = VgiBatchBytes(&context->bind_request, "input_schema");
    if (!rc && schema.size_bytes) rc = VgiIpcReadSchema(schema, &context->input_schema, error);
    struct ArrowBufferView cookie = VgiBatchBytes(&context->bind_request, "attach_opaque_data");
    if (!rc && cookie.size_bytes)
        rc = VgiCatalogLoadOptions(cookie, &context->attach_options, error);
    return rc;
}
static int bind_dispatch(struct VgiWorker *worker, struct VgiRpcCall *call,
                         struct VgiRpcReply *reply) {
    struct VgiFunctionCall context = {0};
    context.rpc = call;
    ArrowBufferInit(&context.execution_id);
    int rc = VgiBatchChild(&call->request, "request", &context.bind_request, &call->error);
    if (!rc) rc = read_context(&context);
    struct RegisteredFunction *registered =
        rc ? NULL : find_function(worker, &context.bind_request);
    if (!rc && !registered)
        rc = VgiRpcFail(call, "ValueError", "INVALID_ARGUMENT", "",
                        "No matching function in requested catalog/schema");
    if (!rc) {
        struct VgiFunction *fn = &registered->function;
        context.argument_schema = &fn->arguments;
        rc = fn->bind ? fn->bind(fn->userdata, &context, &context.output_schema)
                      : ArrowSchemaDeepCopy(&fn->output, &context.output_schema);
    }
    struct VgiBatch payload = {0};
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    if (!rc) rc = payload_init(call, &payload);
    if (!rc) rc = VgiIpcWriteSchema(&context.output_schema, &bytes, &call->error);
    if (!rc) rc = VgiBatchSetBytes(&payload, "output_schema", bytes.data, bytes.size_bytes);
    if (!rc) rc = VgiBatchSetBytes(&payload, "opaque_data", "", 0);
    if (!rc && !VgiBatchInt(&context.bind_request, "resolved_secrets_provided", 0)) {
        struct VgiFunction *fn = &registered->function;
        if (fn->secret_lookups)
            rc = fn->secret_lookups(fn->userdata, &context, &payload);
        else {
            const struct ArrowArrayView *lookups = VgiBatchGet(&fn->info, "required_secrets");
            int64_t id = VgiFieldIndex(&fn->info.schema, "required_secrets");
            if (lookups && !ArrowArrayViewIsNull(lookups, 0)) {
                const struct ArrowSchema *schema = fn->info.schema.children[id]->children[0];
                const char *source_names[] = {"secret_type", "scope", "secret_name"},
                           *target_names[] = {"lookup_secret_types", "lookup_scopes",
                                              "lookup_names"};
                int64_t start = ArrowArrayViewListChildOffset(lookups, 0),
                        end = ArrowArrayViewListChildOffset(lookups, 1);
                for (int j = 0; !rc && j < 3; ++j) {
                    struct ArrowArray *list = VgiBatchColumn(&payload, target_names[j]);
                    int64_t field = VgiFieldIndex(schema, source_names[j]);
                    for (int64_t i = start; !rc && i < end; ++i) {
                        struct ArrowStringView value =
                            VgiValueString(lookups->children[0]->children[field], i);
                        rc = ArrowArrayAppendString(list->children[0],
                                                    value.data ? value : ArrowCharView(""));
                    }
                    if (!rc) rc = ArrowArrayFinishElement(list);
                }
            }
        }
    }
    if (!rc) rc = VgiBatchFinishRow(&payload, &call->error);
    if (!rc) rc = VgiRpcReplyPayload(call, reply, &payload);
    ArrowBufferReset(&bytes);
    VgiBatchReset(&payload);
    function_call_reset(&context);
    return rc;
}
struct FunctionStream {
    struct VgiFunctionCall context;
    struct VgiFunction *function;
    /* The last reply said HAVE_MORE_OUTPUT, so the next input must be a tick.
     * Kept with the stream, which serves every turn on each transport. */
    int more_pending;
};
#define VGI_STATUS_KEY "vgi.status"
static int refuse(struct VgiRpcCall *rpc, struct ArrowStringView name, const char *reason) {
    char message[256];
    snprintf(message, sizeof(message), "%.*s: %s", (int)name.size_bytes, name.data, reason);
    return VgiRpcFail(rpc, "RuntimeError", "FAILED_PRECONDITION", "", message);
}
static int has_cache_control(const struct ArrowBuffer *metadata) {
    struct ArrowMetadataReader reader;
    struct ArrowStringView key, value;
    if (!metadata->data || ArrowMetadataReaderInit(&reader, (const char *)metadata->data)) return 0;
    while (reader.remaining_keys > 0 && !ArrowMetadataReaderRead(&reader, &key, &value))
        if (key.size_bytes >= 10 && !memcmp(key.data, "vgi.cache.", 10)) return 1;
    return 0;
}
/* Keep the parent_row entries of the rows an auto-applied filter kept. */
static int filter_parent_rows(struct ArrowBuffer *metadata, const struct ArrowBuffer *kept,
                              int64_t rows) {
    static const char key[] = "vgi_rpc.parent_row#b64";
    struct ArrowStringView encoded =
        VgiMetadataGet(metadata->data ? (const char *)metadata->data : NULL, key);
    if (!encoded.data) return 0;
    size_t length = (size_t)encoded.size_bytes, size = 0;
    unsigned char *parents = malloc(length + 1);
    if (!parents) return ENOMEM;
    int rc = sodium_base642bin(parents, length + 1, encoded.data, length, NULL, &size, NULL,
                               sodium_base64_VARIANT_ORIGINAL)
                 ? EINVAL
                 : 0;
    /* Malformed provenance is left for the client to reject. */
    if (rc || (int64_t)size != rows * 4) {
        free(parents);
        return 0;
    }
    const int64_t *rows_kept = (const int64_t *)kept->data;
    int64_t count = kept->size_bytes / (int64_t)sizeof(int64_t);
    for (int64_t i = 0; i < count; ++i) memmove(parents + 4 * i, parents + 4 * rows_kept[i], 4);
    size_t capacity =
        sodium_base64_ENCODED_LEN((size_t)(4 * count), sodium_base64_VARIANT_ORIGINAL);
    char *text = malloc(capacity);
    if (!text)
        rc = ENOMEM;
    else {
        sodium_bin2base64(text, capacity, parents, (size_t)(4 * count),
                          sodium_base64_VARIANT_ORIGINAL);
        rc = VgiMetadataSet(metadata, key, text);
    }
    free(text);
    free(parents);
    return rc;
}
static int function_next(void *state, struct VgiRpcCall *rpc, const struct VgiBatch *input,
                         const char *metadata, struct VgiBatch *output,
                         struct ArrowBuffer *output_metadata, int *finished) {
    struct FunctionStream *stream = state;
    struct VgiFunction *fn = stream->function;
    stream->context.rpc = rpc;
    stream->context.input_metadata = metadata;
    if (fn->kind == VGI_BUFFERING &&
        !enum_is(VgiBatchString(&stream->context.init_request, "phase"),
                 "TABLE_BUFFERING_FINALIZE")) {
        *finished = 1;
        return 0;
    }
    struct ArrowStringView delta = VgiMetadataGet(metadata, "vgi_pushdown_filters");
    if (delta.data) {
        if (delta.size_bytes > 24 * 1024 * 1024)
            return VgiRpcFail(rpc, "ValueError", "INVALID_ARGUMENT", "",
                              "Dynamic filter exceeds size limit");
        struct ArrowBuffer decoded;
        ArrowBufferInit(&decoded);
        int delta_rc = ArrowBufferResize(&decoded, delta.size_bytes, 0);
        size_t size = 0;
        if (!delta_rc && sodium_base642bin(decoded.data, (size_t)decoded.size_bytes, delta.data,
                                           (size_t)delta.size_bytes, NULL, &size, NULL,
                                           sodium_base64_VARIANT_ORIGINAL))
            delta_rc = EINVAL;
        if (!delta_rc)
            delta_rc = VgiFiltersApplyDelta(stream->context.filters,
                                            (struct ArrowBufferView){{decoded.data}, (int64_t)size},
                                            &rpc->error);
        ArrowBufferReset(&decoded);
        if (delta_rc) return delta_rc;
    }
    /* Continued output: a reply that says more (HAVE_MORE_OUTPUT) is followed by a
     * tick, a zero-row input carrying vgi.status: CONTINUE, which resume answers.
     * While output is pending a tick is the only valid input, and a tick is valid
     * only then. Every reply of an input indexes that input's rows. */
    int tick = text_is(VgiMetadataGet(metadata, VGI_STATUS_KEY), "CONTINUE");
    struct ArrowStringView name = VgiBatchString(&fn->info, "name");
    if (tick && !stream->more_pending)
        return refuse(rpc, name, "continuation tick received, but no output is pending");
    if (tick && input->array.length)
        return refuse(rpc, name, "a continuation tick must have no rows");
    if (!tick && stream->more_pending)
        return refuse(rpc, name,
                      "input received while output is pending; expected a continuation tick");
    stream->context.more_output = 0;
    int rc;
    if (tick)
        rc = fn->resume(fn->userdata, &stream->context, output, output_metadata);
    else if (fn->process)
        rc = fn->process(fn->userdata, &stream->context, input, output, output_metadata, finished);
    else
        return VgiRpcFail(rpc, "MethodNotImplementedError", "UNIMPLEMENTED",
                          "method_not_implemented", "Function has no process callback");
    int more = !rc && stream->context.more_output;
    if (more && fn->kind != VGI_TABLE_IN_OUT)
        rc = refuse(rpc, name,
                    "more output is only valid in a table-in-out function's process or resume");
    else if (more && !fn->resume)
        rc = refuse(rpc, name, "set more output but has no resume callback");
    else if (more && *finished)
        rc = refuse(rpc, name, "set more output on a finished stream");
    else if (!rc && fn->kind == VGI_TABLE_IN_OUT && !*finished && !output->array.release)
        /* One reply per turn, as vgi-rpc's exchange requires ("No data batch was
         * emitted"): every input, and every tick, is answered with a batch. */
        rc = refuse(rpc, name, "every table-in-out reply must carry a batch");
    else if (!rc && tick && has_cache_control(output_metadata))
        rc = refuse(rpc, name,
                    "cache control may only ride an input's first reply, not a continuation");
    if (!rc && !*finished && fn->auto_apply_filters && stream->context.filters) {
        struct VgiBatch filtered = {0};
        struct ArrowBuffer kept;
        ArrowBufferInit(&kept);
        rc = VgiFiltersSelect(stream->context.filters, output, &filtered, &kept, &rpc->error);
        /* Provenance is per output row, so it must lose the rows the filter does. */
        if (!rc) rc = filter_parent_rows(output_metadata, &kept, output->array.length);
        if (!rc) {
            VgiBatchReset(output);
            *output = filtered;
        } else
            VgiBatchReset(&filtered);
        ArrowBufferReset(&kept);
    }
    if (!rc && !*finished && (fn->kind == VGI_TABLE || fn->kind == VGI_TABLE_IN_OUT)) {
        struct VgiBatch projected = {0};
        rc = VgiBatchCoerce(output, &stream->context.output_schema, &projected, &rpc->error);
        if (!rc) {
            VgiBatchReset(output);
            *output = projected;
        }
    }
    if (!rc && more) rc = VgiMetadataSet(output_metadata, VGI_STATUS_KEY, "HAVE_MORE_OUTPUT");
    /* An error ends the stream; the client discards the connection. */
    stream->more_pending = more && !rc;
    if (rc && !rpc->error.message[0])
        ArrowErrorSet(&rpc->error, "Function process failed (error %d)", rc);
    if (!rc && !*finished && fn->kind == VGI_SCALAR && output->array.length != input->array.length)
        rc = VgiRpcFail(rpc, "RuntimeError", "INTERNAL", "",
                        "Scalar callback returned a different row count");
    return rc;
}
static void function_release(void *state) {
    struct FunctionStream *stream = state;
    if (stream->function && stream->function->finalize)
        stream->function->finalize(stream->function->userdata, &stream->context);
    function_call_reset(&stream->context);
    free(stream);
}
static int init_dispatch(struct VgiWorker *worker, struct VgiRpcCall *call,
                         struct VgiRpcReply *reply) {
    struct FunctionStream *stream = calloc(1, sizeof(*stream));
    if (!stream) return ENOMEM;
    stream->context.rpc = call;
    ArrowBufferInit(&stream->context.execution_id);
    reply->stream.release = function_release;
    reply->stream.state = stream;
    struct VgiFunctionCall *context = &stream->context;
    int rc = VgiBatchChild(&call->request, "request", &context->init_request, &call->error);
    if (!rc)
        rc = VgiBatchChild(&context->init_request, "bind_call", &context->bind_request,
                           &call->error);
    if (!rc) rc = read_context(context);
    struct RegisteredFunction *registered =
        rc ? NULL : find_function(worker, &context->bind_request);
    if (!rc && !registered)
        rc = VgiRpcFail(call, "ValueError", "INVALID_ARGUMENT", "",
                        "No matching function in requested catalog/schema");
    if (!rc) {
        stream->function = &registered->function;
        context->argument_schema = &registered->function.arguments;
    }
    if (!rc)
        rc = VgiIpcReadSchema(VgiBatchBytes(&context->init_request, "output_schema"),
                              &context->output_schema, &call->error);
    if (!rc)
        rc = VgiFiltersParse(VgiBatchBytes(&context->init_request, "pushdown_filters"),
                             &context->output_schema, &context->init_request, &context->filters,
                             &call->error);
    const struct ArrowArrayView *projection = VgiBatchGet(&context->init_request, "projection_ids");
    if (!rc && projection && !ArrowArrayViewIsNull(projection, 0)) {
        int64_t start = ArrowArrayViewListChildOffset(projection, 0),
                end = ArrowArrayViewListChildOffset(projection, 1);
        if (end > start) {
            struct ArrowSchema narrowed = {0};
            ArrowSchemaInit(&narrowed);
            rc = ArrowSchemaSetTypeStruct(&narrowed, end - start);
            for (int64_t i = start; !rc && i < end; ++i) {
                int64_t id = ArrowArrayViewGetIntUnsafe(projection->children[0], i);
                if (id < 0 || id >= context->output_schema.n_children) {
                    rc = EINVAL;
                    break;
                }
                ArrowSchemaRelease(narrowed.children[i - start]);
                rc = ArrowSchemaDeepCopy(context->output_schema.children[id],
                                         narrowed.children[i - start]);
            }
            if (!rc) {
                ArrowSchemaRelease(&context->output_schema);
                ArrowSchemaMove(&narrowed, &context->output_schema);
            }
            if (narrowed.release) ArrowSchemaRelease(&narrowed);
        }
    }
    struct ArrowBufferView execution = VgiBatchBytes(&context->init_request, "execution_id");
    if (!rc && execution.size_bytes)
        rc = ArrowBufferAppend(&context->execution_id, execution.data.data, execution.size_bytes);
    else if (!rc) {
        unsigned char id[16];
        char hex[33];
        randombytes_buf(id, sizeof(id));
        sodium_bin2hex(hex, sizeof(hex), id, sizeof(id));
        rc = ArrowBufferAppend(&context->execution_id, hex, 32);
    }
    if (!rc) rc = VgiStorageOpen(NULL, &context->storage, &call->error);
    if (!rc && stream->function->kind == VGI_BUFFERING && !execution.size_bytes) {
        struct ArrowBuffer bind_bytes;
        ArrowBufferInit(&bind_bytes);
        rc = VgiIpcWriteBatch(&context->bind_request.schema, &context->bind_request.array, NULL,
                              &bind_bytes, &call->error);
        if (!rc)
            rc = VgiStoragePut(context->storage,
                               (struct ArrowBufferView){{context->execution_id.data},
                                                        context->execution_id.size_bytes},
                               "function.bind",
                               (struct ArrowBufferView){{bind_bytes.data}, bind_bytes.size_bytes},
                               &call->error);
        ArrowBufferReset(&bind_bytes);
    }
    if (!rc) rc = VgiSplitRedeem(context, 1, call->sealing_key);
    if (!rc && stream->function->init)
        rc = stream->function->init(stream->function->userdata, context);
    if (!rc) rc = VgiBatchInitNamed(&reply->stream.header, "GlobalInitResponse", &call->error);
    if (!rc)
        rc = VgiBatchSetBytes(&reply->stream.header, "execution_id", context->execution_id.data,
                              context->execution_id.size_bytes);
    if (!rc)
        rc = VgiBatchSetInt(&reply->stream.header, "max_workers",
                            stream->function->max_workers
                                ? stream->function->max_workers(stream->function->userdata, context)
                                : 1);
    if (!rc) rc = VgiBatchFinishRow(&reply->stream.header, &call->error);
    if (!rc) rc = ArrowSchemaDeepCopy(&context->output_schema, &reply->stream.output_schema);
    reply->stream.next = function_next;
    reply->stream.is_producer = stream->function && (stream->function->kind == VGI_TABLE ||
                                                     stream->function->kind == VGI_BUFFERING);
    return rc;
}
static struct ArrowBufferView buffer_view(const struct ArrowBuffer *buffer) {
    return (struct ArrowBufferView){{buffer->data}, buffer->size_bytes};
}
static int aggregate_dispatch(struct VgiWorker *worker, struct VgiRpcCall *call,
                              struct VgiRpcReply *reply) {
    const char *method = call->method->name;
    struct VgiFunctionCall context = {0};
    context.rpc = call;
    struct VgiBatch request = {0}, input = {0}, payload = {0}, result = {0};
    struct ArrowBuffer bytes, state, source;
    ArrowBufferInit(&bytes);
    ArrowBufferInit(&state);
    ArrowBufferInit(&source);
    ArrowBufferInit(&context.execution_id);
    int rc = VgiBatchChild(&call->request, "request", &request, &call->error);
    if (!rc && !strcmp(method, "aggregate_destructor")) {
        /* Cleanup is idempotent and must never replace the query's error. */
        if (!VgiStorageOpen(NULL, &context.storage, NULL))
            VgiStorageClear(context.storage, VgiBatchBytes(&request, "execution_id"), NULL);
        rc = payload_init(call, &payload);
        if (!rc) rc = VgiBatchFinishRow(&payload, &call->error);
        if (!rc) rc = VgiRpcReplyPayload(call, reply, &payload);
        VgiBatchReset(&request);
        VgiBatchReset(&payload);
        function_call_reset(&context);
        return rc;
    }
    struct RegisteredFunction *registered = rc ? NULL : find_function(worker, &request);
    if (!rc && (!registered || registered->function.kind != VGI_AGGREGATE))
        rc = VgiRpcFail(call, "ValueError", "INVALID_ARGUMENT", "", "Aggregate function not found");
    struct VgiFunction *function = registered ? &registered->function : NULL;
    if (!rc) context.argument_schema = &function->arguments;
    if (!rc) rc = VgiStorageOpen(NULL, &context.storage, &call->error);
    if (!rc) rc = payload_init(call, &payload);
    if (!rc && !strcmp(method, "aggregate_bind")) {
        rc = VgiIpcWriteBatch(&request.schema, &request.array, NULL, &bytes, &call->error);
        if (!rc) rc = VgiBatchDecode(&context.bind_request, buffer_view(&bytes), &call->error);
        if (!rc) rc = read_context(&context);
        if (!rc)
            rc = function->bind
                     ? function->bind(function->userdata, &context, &context.output_schema)
                     : ArrowSchemaDeepCopy(&function->output, &context.output_schema);
        unsigned char random[16];
        char execution[33];
        randombytes_buf(random, sizeof(random));
        sodium_bin2hex(execution, sizeof(execution), random, sizeof(random));
        if (!rc) rc = ArrowBufferAppend(&context.execution_id, execution, 32);
        if (!rc)
            rc = VgiStoragePut(context.storage, buffer_view(&context.execution_id),
                               "aggregate.bind", buffer_view(&bytes), &call->error);
        ArrowBufferReset(&bytes);
        if (!rc) rc = VgiIpcWriteSchema(&context.output_schema, &bytes, &call->error);
        if (!rc) rc = VgiBatchSetBytes(&payload, "output_schema", bytes.data, bytes.size_bytes);
        if (!rc)
            rc = VgiBatchSetBytes(&payload, "execution_id", context.execution_id.data,
                                  context.execution_id.size_bytes);
    } else if (!rc) {
        struct ArrowBufferView execution = VgiBatchBytes(&request, "execution_id");
        rc = ArrowBufferAppend(&context.execution_id, execution.data.data, execution.size_bytes);
        if (!rc)
            rc = VgiStorageGet(context.storage, execution, "aggregate.bind", &bytes, &call->error);
        if (rc == ENOENT)
            rc = VgiRpcFail(call, "ValueError", "NOT_FOUND", "", "Unknown aggregate execution_id");
        if (!rc) rc = VgiBatchDecode(&context.bind_request, buffer_view(&bytes), &call->error);
        if (!rc) rc = read_context(&context);
        if (!rc) rc = VgiStorageBegin(context.storage, &call->error);
        const char *field = !strcmp(method, "aggregate_update")    ? "input_batch"
                            : !strcmp(method, "aggregate_combine") ? "merge_batch"
                                                                   : "group_ids_batch";
        if (!rc) rc = VgiBatchChild(&request, field, &input, &call->error);
        if (!rc && !strcmp(method, "aggregate_finalize")) {
            rc = VgiIpcReadSchema(VgiBatchBytes(&request, "output_schema"), &context.output_schema,
                                  &call->error);
            if (!rc) rc = VgiBatchInit(&result, &context.output_schema, &call->error);
        }
        const struct ArrowArrayView *ids = !strcmp(method, "aggregate_update")
                                               ? VgiBatchGet(&input, "__vgi_group_id")
                                           : input.view.n_children ? input.view.children[0]
                                                                   : NULL;
        if (!rc && (!ids || ids->storage_type != NANOARROW_TYPE_INT64 || ids->null_count > 0))
            rc = VgiRpcFail(call, "ValueError", "INVALID_ARGUMENT", "",
                            "Aggregate requires non-null int64 group ids");
        for (int64_t row = 0; !rc && row < input.array.length; ++row) {
            char key[80];
            snprintf(key, sizeof(key), "aggregate.state.%lld",
                     (long long)ArrowArrayViewGetIntUnsafe(ids, row));
            ArrowBufferReset(&state);
            ArrowBufferReset(&source);
            int present = 0;
            if (strcmp(method, "aggregate_destructor")) {
                rc = VgiStorageGet(context.storage, execution, key, &state, &call->error);
                present = !rc;
                if (rc == ENOENT) rc = 0;
            }
            if (!rc && !strcmp(method, "aggregate_update")) {
                rc = function->aggregate_update
                         ? function->aggregate_update(function->userdata, &context, &input, row,
                                                      &state, &present)
                         : ENOTSUP;
                if (!rc && present)
                    rc = VgiStoragePut(context.storage, execution, key, buffer_view(&state),
                                       &call->error);
            } else if (!rc && !strcmp(method, "aggregate_combine")) {
                if (!present) continue;
                const struct ArrowArrayView *targets = VgiBatchGet(&input, "target_group_id");
                if (!targets && input.view.n_children > 1) targets = input.view.children[1];
                if (!targets || targets->storage_type != NANOARROW_TYPE_INT64 ||
                    ArrowArrayViewIsNull(targets, row)) {
                    rc = EINVAL;
                    break;
                }
                snprintf(key, sizeof(key), "aggregate.state.%lld",
                         (long long)ArrowArrayViewGetIntUnsafe(targets, row));
                ArrowBufferMove(&state, &source);
                rc = VgiStorageGet(context.storage, execution, key, &state, &call->error);
                if (rc == ENOENT) {
                    rc = 0;
                    ArrowBufferMove(&source, &state);
                } else if (!rc)
                    rc = function->aggregate_combine
                             ? function->aggregate_combine(function->userdata, &context, &state,
                                                           buffer_view(&source))
                             : ENOTSUP;
                if (!rc)
                    rc = VgiStoragePut(context.storage, execution, key, buffer_view(&state),
                                       &call->error);
            } else if (!rc && !strcmp(method, "aggregate_finalize")) {
                struct ArrowBufferView value = buffer_view(&state);
                rc = function->aggregate_finalize
                         ? function->aggregate_finalize(function->userdata, &context,
                                                        present ? &value : NULL, &result)
                         : ENOTSUP;
                if (!rc) rc = ArrowArrayFinishElement(&result.array);
            } else if (!rc && !strcmp(method, "aggregate_destructor")) {
                rc = VgiStorageDelete(context.storage, execution, key, &call->error);
            } else if (!rc)
                rc = VgiRpcFail(call, "MethodNotImplementedError", "UNIMPLEMENTED",
                                "method_not_implemented", "Aggregate operation is not implemented");
        }
        if (!rc && !strcmp(method, "aggregate_finalize")) {
            rc = VgiBatchFinish(&result, &call->error);
            ArrowBufferReset(&bytes);
            if (!rc)
                rc = VgiIpcWriteBatch(&result.schema, &result.array, NULL, &bytes, &call->error);
            if (!rc) rc = VgiBatchSetBytes(&payload, "result_batch", bytes.data, bytes.size_bytes);
        }
        if (!rc)
            rc = VgiStorageCommit(context.storage, &call->error);
        else
            VgiStorageRollback(context.storage);
    }
    if (!rc) rc = VgiBatchFinishRow(&payload, &call->error);
    if (!rc) rc = VgiRpcReplyPayload(call, reply, &payload);
    ArrowBufferReset(&bytes);
    ArrowBufferReset(&state);
    ArrowBufferReset(&source);
    VgiBatchReset(&request);
    VgiBatchReset(&input);
    VgiBatchReset(&payload);
    VgiBatchReset(&result);
    function_call_reset(&context);
    return rc;
}

/* Persist the partition independently of the connection handling each frame. */
static int aggregate_window_dispatch(struct VgiWorker *worker, struct VgiRpcCall *call,
                                     struct VgiRpcReply *reply) {
    const char *method = call->method->name;
    struct VgiFunctionCall context = {.rpc = call};
    struct VgiBatch request = {0}, partition = {0}, init = {0}, payload = {0}, result = {0};
    struct ArrowBuffer bytes, state;
    ArrowBufferInit(&bytes);
    ArrowBufferInit(&state);
    ArrowBufferInit(&context.execution_id);
    int rc = VgiBatchChild(&call->request, "request", &request, &call->error);
    struct ArrowBufferView execution = VgiBatchBytes(&request, "execution_id");
    char key[80];
    snprintf(key, sizeof(key), "window.partition.%lld",
             (long long)VgiBatchInt(&request, "partition_id", 0));
    if (!rc) rc = VgiStorageOpen(NULL, &context.storage, &call->error);
    if (!strcmp(method, "aggregate_window_destructor")) {
        if (!rc) VgiStorageDelete(context.storage, execution, key, NULL);
        rc = 0;
    } else if (!rc && !strcmp(method, "aggregate_window_init")) {
        rc = VgiIpcWriteBatch(&request.schema, &request.array, NULL, &bytes, &call->error);
        if (!rc)
            rc = VgiStoragePut(context.storage, execution, key, buffer_view(&bytes), &call->error);
    } else if (!rc) {
        struct RegisteredFunction *registered = find_function(worker, &request);
        struct VgiFunction *fn = registered ? &registered->function : NULL;
        if (!fn || !fn->aggregate_update || !fn->aggregate_finalize)
            rc = VgiRpcFail(call, "ValueError", "INVALID_ARGUMENT", "",
                            "Window aggregate not found");
        if (!rc) context.argument_schema = &fn->arguments;
        if (!rc) rc = VgiStorageGet(context.storage, execution, key, &bytes, &call->error);
        if (!rc) rc = VgiBatchDecode(&init, buffer_view(&bytes), &call->error);
        ArrowBufferReset(&bytes);
        if (!rc) rc = VgiBatchChild(&init, "partition_batch", &partition, &call->error);
        if (!rc)
            rc = VgiIpcReadSchema(VgiBatchBytes(&init, "output_schema"), &context.output_schema,
                                  &call->error);
        if (!rc) rc = VgiBatchInit(&result, &context.output_schema, &call->error);
        if (!rc) {
            int found =
                VgiStorageGet(context.storage, execution, "aggregate.bind", &bytes, &call->error);
            if (!found) {
                rc = VgiBatchDecode(&context.bind_request, buffer_view(&bytes), &call->error);
                if (!rc) rc = read_context(&context);
            } else if (found != ENOENT)
                rc = found;
        }
        const struct ArrowArrayView *starts = VgiBatchGet(&request, "frame_starts"),
                                    *ends = VgiBatchGet(&request, "frame_ends"),
                                    *counts = VgiBatchGet(&request, "frames_per_row");
        int batch = !strcmp(method, "aggregate_window_batch");
        int64_t rows = batch ? VgiBatchInt(&request, "count", 0) : 1, frame = 0, offset = 0,
                frame_count = 0, count_offset = 0;
        if (!rc && (!starts || !ends || rows < 0)) rc = EINVAL;
        if (!rc) {
            offset = ArrowArrayViewListChildOffset(starts, 0);
            frame_count = ArrowArrayViewListChildOffset(starts, 1) - offset;
            if (ArrowArrayViewListChildOffset(ends, 1) - ArrowArrayViewListChildOffset(ends, 0) !=
                frame_count)
                rc = EINVAL;
            if (batch && (!counts || ArrowArrayViewListChildOffset(counts, 1) -
                                             ArrowArrayViewListChildOffset(counts, 0) !=
                                         rows))
                rc = EINVAL;
            if (batch && counts) count_offset = ArrowArrayViewListChildOffset(counts, 0);
        }
        struct ArrowBufferView mask = VgiBatchBytes(&init, "filter_mask");
        for (int64_t row = 0; !rc && row < rows; ++row) {
            ArrowBufferReset(&state);
            int present = 0;
            int64_t n = batch ? ArrowArrayViewGetIntUnsafe(counts->children[0], count_offset + row)
                              : frame_count;
            if (n < 0 || n > frame_count - frame) {
                rc = EINVAL;
                break;
            }
            for (int64_t j = 0; !rc && j < n; ++j, ++frame) {
                int64_t start = ArrowArrayViewGetIntUnsafe(starts->children[0], offset + frame);
                int64_t end = ArrowArrayViewGetIntUnsafe(
                    ends->children[0], ArrowArrayViewListChildOffset(ends, 0) + frame);
                if (start < 0 || end < start || end > partition.array.length) {
                    rc = EINVAL;
                    break;
                }
                for (int64_t k = start; !rc && k < end; ++k) {
                    if (mask.size_bytes && (k / 8 >= mask.size_bytes ||
                                            !(mask.data.as_uint8[k / 8] & (1u << (k % 8)))))
                        continue;
                    rc = fn->aggregate_update(fn->userdata, &context, &partition, k, &state,
                                              &present);
                }
            }
            struct ArrowBufferView value = buffer_view(&state);
            if (!rc)
                rc = fn->aggregate_finalize(fn->userdata, &context, present ? &value : NULL,
                                            &result);
            if (!rc) rc = ArrowArrayFinishElement(&result.array);
        }
        if (!rc && frame != frame_count) rc = EINVAL;
        if (!rc) rc = VgiBatchFinish(&result, &call->error);
    }
    if (!rc) rc = payload_init(call, &payload);
    if (!rc && result.array.release) {
        ArrowBufferReset(&bytes);
        rc = VgiIpcWriteBatch(&result.schema, &result.array, NULL, &bytes, &call->error);
        if (!rc) rc = VgiBatchSetBytes(&payload, "result_batch", bytes.data, bytes.size_bytes);
    }
    if (!rc) rc = VgiBatchFinishRow(&payload, &call->error);
    if (!rc) rc = VgiRpcReplyPayload(call, reply, &payload);
    ArrowBufferReset(&bytes);
    ArrowBufferReset(&state);
    VgiBatchReset(&request);
    VgiBatchReset(&init);
    VgiBatchReset(&partition);
    VgiBatchReset(&result);
    VgiBatchReset(&payload);
    function_call_reset(&context);
    return rc;
}
static int aggregate_streaming_dispatch(struct VgiWorker *worker, struct VgiRpcCall *call,
                                        struct VgiRpcReply *reply) {
    const char *method = call->method->name;
    struct VgiFunctionCall context = {.rpc = call};
    struct VgiBatch request = {0}, input = {0}, values = {0}, keys = {0}, payload = {0},
                    result = {0};
    struct ArrowSchema value_schema = {0}, key_schema = {0};
    struct ArrowBuffer bytes, state;
    ArrowBufferInit(&bytes);
    ArrowBufferInit(&state);
    ArrowBufferInit(&context.execution_id);
    int rc = VgiBatchChild(&call->request, "request", &request, &call->error);
    if (!rc) rc = VgiStorageOpen(NULL, &context.storage, &call->error);
    struct ArrowBufferView execution = VgiBatchBytes(&request, "execution_id");
    if (!rc) rc = payload_init(call, &payload);
    if (!strcmp(method, "aggregate_streaming_close")) {
        if (!rc) VgiStorageClear(context.storage, execution, NULL);
        rc = payload.array.release ? 0 : rc;
    } else if (!rc && !strcmp(method, "aggregate_streaming_open")) {
        unsigned char id[16];
        char hex[33];
        randombytes_buf(id, sizeof(id));
        sodium_bin2hex(hex, sizeof(hex), id, sizeof(id));
        rc = VgiIpcWriteBatch(&request.schema, &request.array, NULL, &bytes, &call->error);
        if (!rc)
            rc = VgiStoragePut(context.storage, (struct ArrowBufferView){{hex}, 32},
                               "aggregate.streaming", buffer_view(&bytes), &call->error);
        if (!rc) rc = VgiBatchSetBytes(&payload, "execution_id", hex, 32);
    } else if (!rc) {
        struct RegisteredFunction *registered = find_function(worker, &request);
        struct VgiFunction *fn = registered ? &registered->function : NULL;
        if (!fn || !fn->aggregate_update || !fn->aggregate_finalize)
            rc = VgiRpcFail(call, "ValueError", "INVALID_ARGUMENT", "",
                            "Streaming aggregate not found");
        if (!rc) context.argument_schema = &fn->arguments;
        if (!rc)
            rc = VgiStorageGet(context.storage, execution, "aggregate.streaming", &bytes,
                               &call->error);
        if (!rc) rc = VgiBatchDecode(&context.bind_request, buffer_view(&bytes), &call->error);
        if (!rc) rc = read_context(&context);
        if (!rc)
            rc = VgiIpcReadSchema(VgiBatchBytes(&context.bind_request, "output_schema"),
                                  &context.output_schema, &call->error);
        if (!rc) rc = VgiBatchInit(&result, &context.output_schema, &call->error);
        if (!rc) rc = VgiBatchChild(&request, "input_batch", &input, &call->error);
        int64_t partitions = VgiBatchInt(&context.bind_request, "partition_key_count", 0);
        int64_t skip = partitions + VgiBatchInt(&context.bind_request, "order_key_count", 0);
        if (!rc && (partitions < 0 || skip < partitions || skip > input.schema.n_children))
            rc = EINVAL;
        if (!rc) {
            ArrowSchemaInit(&value_schema);
            ArrowSchemaInit(&key_schema);
            rc = ArrowSchemaSetTypeStruct(&value_schema, input.schema.n_children - skip);
            if (!rc) rc = ArrowSchemaSetTypeStruct(&key_schema, partitions);
            for (int64_t i = 0; !rc && i < value_schema.n_children; ++i) {
                ArrowSchemaRelease(value_schema.children[i]);
                rc = ArrowSchemaDeepCopy(input.schema.children[i + skip], value_schema.children[i]);
            }
            for (int64_t i = 0; !rc && i < partitions; ++i) {
                const struct ArrowSchema *source = input.schema.children[i];
                ArrowSchemaRelease(key_schema.children[i]);
                rc = ArrowSchemaDeepCopy(source->dictionary ? source->dictionary : source,
                                         key_schema.children[i]);
                if (!rc) rc = ArrowSchemaSetName(key_schema.children[i], source->name);
            }
        }
        if (!rc) rc = VgiBatchInit(&values, &value_schema, &call->error);
        for (int64_t row = 0; !rc && row < input.array.length; ++row) {
            for (int64_t i = 0; !rc && i < value_schema.n_children; ++i)
                rc = VgiArrayAppendValue(values.array.children[i], value_schema.children[i],
                                         input.view.children[i + skip],
                                         input.schema.children[i + skip], row, &call->error);
            if (!rc) rc = ArrowArrayFinishElement(&values.array);
        }
        if (!rc) rc = VgiBatchFinish(&values, &call->error);
        if (!rc) rc = VgiStorageBegin(context.storage, &call->error);
        for (int64_t row = 0; !rc && row < input.array.length; ++row) {
            char key[96] = "streaming.state.global";
            if (partitions) {
                VgiBatchReset(&keys);
                ArrowBufferReset(&bytes);
                rc = VgiBatchInit(&keys, &key_schema, &call->error);
                for (int64_t i = 0; !rc && i < partitions; ++i)
                    rc = VgiArrayAppendValue(keys.array.children[i], key_schema.children[i],
                                             input.view.children[i], input.schema.children[i], row,
                                             &call->error);
                if (!rc) rc = ArrowArrayFinishElement(&keys.array);
                if (!rc) rc = VgiBatchFinish(&keys, &call->error);
                if (!rc)
                    rc = VgiIpcWriteBatch(&keys.schema, &keys.array, NULL, &bytes, &call->error);
                if (!rc) {
                    unsigned char hash[32];
                    crypto_hash_sha256(hash, bytes.data, (unsigned long long)bytes.size_bytes);
                    memcpy(key, "streaming.state.", 16);
                    sodium_bin2hex(key + 16, sizeof(key) - 16, hash, sizeof(hash));
                }
            }
            ArrowBufferReset(&state);
            int present = 0;
            if (!rc) {
                rc = VgiStorageGet(context.storage, execution, key, &state, &call->error);
                present = !rc;
                if (rc == ENOENT) rc = 0;
            }
            if (!rc)
                rc = fn->aggregate_update(fn->userdata, &context, &values, row, &state, &present);
            if (!rc && present)
                rc = VgiStoragePut(context.storage, execution, key, buffer_view(&state),
                                   &call->error);
            struct ArrowBufferView value = buffer_view(&state);
            if (!rc)
                rc = fn->aggregate_finalize(fn->userdata, &context, present ? &value : NULL,
                                            &result);
            if (!rc) rc = ArrowArrayFinishElement(&result.array);
        }
        if (!rc)
            rc = VgiStorageCommit(context.storage, &call->error);
        else
            VgiStorageRollback(context.storage);
        if (!rc) rc = VgiBatchFinish(&result, &call->error);
        ArrowBufferReset(&bytes);
        if (!rc) rc = VgiIpcWriteBatch(&result.schema, &result.array, NULL, &bytes, &call->error);
        if (!rc) rc = VgiBatchSetBytes(&payload, "result_batch", bytes.data, bytes.size_bytes);
    }
    if (!rc) rc = VgiBatchFinishRow(&payload, &call->error);
    if (!rc) rc = VgiRpcReplyPayload(call, reply, &payload);
    ArrowBufferReset(&bytes);
    ArrowBufferReset(&state);
    VgiBatchReset(&request);
    VgiBatchReset(&input);
    VgiBatchReset(&values);
    VgiBatchReset(&keys);
    VgiBatchReset(&payload);
    VgiBatchReset(&result);
    if (key_schema.release) ArrowSchemaRelease(&key_schema);
    if (value_schema.release) ArrowSchemaRelease(&value_schema);
    function_call_reset(&context);
    return rc;
}

static int buffering_dispatch(struct VgiWorker *worker, struct VgiRpcCall *call,
                              struct VgiRpcReply *reply) {
    struct VgiFunctionCall context = {.rpc = call};
    struct VgiBatch input = {0}, payload = {0};
    struct ArrowBuffer bytes, state_id;
    ArrowBufferInit(&bytes);
    ArrowBufferInit(&state_id);
    ArrowBufferInit(&context.execution_id);
    int rc = VgiBatchChild(&call->request, "request", &context.init_request, &call->error);
    struct ArrowBufferView execution = VgiBatchBytes(&context.init_request, "execution_id");
    if (!rc) rc = VgiStorageOpen(NULL, &context.storage, &call->error);
    const char *method = call->method->name;
    if (!strcmp(method, "table_buffering_destructor")) {
        if (!rc) VgiStorageClear(context.storage, execution, NULL);
        rc = 0;
    } else {
        struct RegisteredFunction *registered =
            rc ? NULL : find_function(worker, &context.init_request);
        struct VgiFunction *fn = registered ? &registered->function : NULL;
        if (!rc && (!fn || fn->kind != VGI_BUFFERING))
            rc = VgiRpcFail(call, "ValueError", "NOT_FOUND", "", "Buffered function not found");
        if (!rc) context.argument_schema = &fn->arguments;
        if (!rc)
            rc =
                ArrowBufferAppend(&context.execution_id, execution.data.data, execution.size_bytes);
        if (!rc)
            rc = VgiStorageGet(context.storage, execution, "function.bind", &bytes, &call->error);
        if (!rc) rc = VgiBatchDecode(&context.bind_request, buffer_view(&bytes), &call->error);
        if (!rc) rc = read_context(&context);
        if (!rc)
            rc = fn->bind ? fn->bind(fn->userdata, &context, &context.output_schema)
                          : ArrowSchemaDeepCopy(&fn->output, &context.output_schema);
        if (!rc) rc = payload_init(call, &payload);
        if (!rc && !strcmp(method, "table_buffering_process")) {
            rc = VgiBatchChild(&context.init_request, "input_batch", &input, &call->error);
            if (!rc && fn->buffer_process)
                rc = fn->buffer_process(fn->userdata, &context, &input, &state_id);
            else if (!rc) {
                ArrowBufferReset(&bytes);
                rc = VgiIpcWriteBatch(&input.schema, &input.array, NULL, &bytes, &call->error);
                if (!rc)
                    rc = VgiStorageAppend(context.storage, execution, "buffer.chunks",
                                          buffer_view(&bytes), NULL, &call->error);
                if (!rc)
                    rc = ArrowBufferAppend(&state_id, execution.data.data, execution.size_bytes);
            }
            if (!rc)
                rc = VgiBatchSetBytes(&payload, "state_id", state_id.data, state_id.size_bytes);
        } else if (!rc && !strcmp(method, "table_buffering_combine")) {
            if (fn->buffer_combine)
                rc = fn->buffer_combine(fn->userdata, &context, &payload);
            else {
                struct ArrowArray *ids = VgiBatchColumn(&payload, "finalize_state_ids");
                rc = ArrowArrayAppendBytes(ids->children[0], execution);
                if (!rc) rc = ArrowArrayFinishElement(ids);
            }
        }
    }
    if (!rc && !payload.array.release) rc = payload_init(call, &payload);
    if (!rc) rc = VgiBatchFinishRow(&payload, &call->error);
    if (!rc) rc = VgiRpcReplyPayload(call, reply, &payload);
    VgiBatchReset(&input);
    VgiBatchReset(&payload);
    ArrowBufferReset(&bytes);
    ArrowBufferReset(&state_id);
    function_call_reset(&context);
    return rc;
}

static int planning_dispatch(struct VgiWorker *worker, struct VgiRpcCall *call,
                             struct VgiRpcReply *reply) {
    struct VgiFunctionCall context = {.rpc = call};
    struct VgiBatch result = {0};
    ArrowBufferInit(&context.execution_id);
    int rc = VgiBatchChild(&call->request, "request", &context.init_request, &call->error);
    if (!rc)
        rc = VgiBatchChild(&context.init_request, "bind_call", &context.bind_request, &call->error);
    if (!rc) rc = read_context(&context);
    struct RegisteredFunction *registered =
        rc ? NULL : find_function(worker, &context.bind_request);
    if (!rc && !registered)
        rc = VgiRpcFail(call, "ValueError", "INVALID_ARGUMENT", "",
                        "No matching function in requested catalog/schema");
    if (!rc) context.argument_schema = &registered->function.arguments;
    if (!rc) rc = VgiStorageOpen(NULL, &context.storage, &call->error);
    if (!rc && !strcmp(call->method->name, "table_function_plan")) {
        struct VgiFunction *fn = &registered->function;
        rc = fn->bind ? fn->bind(fn->userdata, &context, &context.output_schema)
                      : ArrowSchemaDeepCopy(&fn->output, &context.output_schema);
        if (!rc)
            rc = VgiFiltersParse(VgiBatchBytes(&context.init_request, "pushdown_filters"),
                                 &context.output_schema, &context.init_request, &context.filters,
                                 &call->error);
    }
    if (!rc && call->method->payload) rc = payload_init(call, &result);
    if (!rc)
        rc = registered->function.plan
                 ? registered->function.plan(registered->function.userdata, &context,
                                             call->method->name, &result)
                 : ENODATA;
    if (rc == ENODATA && !strcmp(call->method->name, "table_function_plan")) {
        struct VgiBatch split = {0};
        rc = VgiBatchInitNamed(&split, "ScanSplit", &call->error);
        if (!rc) rc = VgiBatchFinishRow(&split, &call->error);
        if (!rc) rc = VgiPlanAppendSplit(&result, &split, &call->error);
        if (!rc) rc = ArrowArrayFinishElement(VgiBatchColumn(&result, "splits"));
        VgiBatchReset(&split);
    }
    if (rc == ENODATA) rc = 0;
    if (!rc && !strcmp(call->method->name, "table_function_plan")) {
        if (!result.array.length) rc = VgiBatchFinishRow(&result, &call->error);
        if (!rc) rc = VgiSplitSealPlan(&context, &result, 1, call->sealing_key);
    }
    if (!rc && result.array.release) {
        if (!result.array.length && call->method->payload)
            rc = VgiBatchFinishRow(&result, &call->error);
        if (!rc) rc = VgiRpcReplyPayload(call, reply, &result);
    } else if (!rc) {
        struct ArrowSchema schema = {0};
        rc = call->method->result(&schema, &call->error);
        if (!rc) rc = VgiBatchInit(&reply->value, &schema, &call->error);
        if (!rc) rc = VgiBatchSetNull(&reply->value, "result");
        if (!rc) rc = VgiBatchFinishRow(&reply->value, &call->error);
        if (schema.release) ArrowSchemaRelease(&schema);
    }
    VgiBatchReset(&result);
    function_call_reset(&context);
    return rc;
}

static int worker_dispatch_impl(void *state, struct VgiRpcCall *call, struct VgiRpcReply *reply) {
    struct VgiWorker *worker = state;
    if (getenv("VGI_TRACE")) fprintf(stderr, "vgi-c: %s\n", call->method->name);
    if (!strncmp(call->method->name, "catalog_", 8)) {
        int handled = 0;
        int rc = VgiMemoryDispatch(worker->memory_service, worker->catalog_service, call, reply,
                                   &handled);
        if (rc || handled) return rc;
        return catalog_dispatch(worker, call, reply);
    }
    if (!strcmp(call->method->name, "bind")) return bind_dispatch(worker, call, reply);
    if (!strcmp(call->method->name, "init")) return init_dispatch(worker, call, reply);
    if (!strncmp(call->method->name, "table_buffering_", 16))
        return buffering_dispatch(worker, call, reply);
    if (!strncmp(call->method->name, "aggregate_window", 16))
        return aggregate_window_dispatch(worker, call, reply);
    if (!strncmp(call->method->name, "aggregate_streaming_", 20))
        return aggregate_streaming_dispatch(worker, call, reply);
    if (!strncmp(call->method->name, "aggregate_", 10))
        return aggregate_dispatch(worker, call, reply);
    if (!strncmp(call->method->name, "table_function_", 15))
        return planning_dispatch(worker, call, reply);
    return VgiRpcFail(call, "MethodNotImplementedError", "UNIMPLEMENTED", "method_not_implemented",
                      "Method is not implemented by this worker");
}
static void release_envelope(void *state) {
    ArrowBufferReset(state);
    free(state);
}
static int worker_dispatch(void *state, struct VgiRpcCall *call, struct VgiRpcReply *reply) {
    struct ArrowBuffer *envelope = calloc(1, sizeof(*envelope));
    if (!envelope) return ENOMEM;
    ArrowBufferInit(envelope);
    call->application_state = envelope;
    call->release_application_state = release_envelope;
    int rc = VgiOpaqueOpenRequest(call);
    if (!rc) rc = worker_dispatch_impl(state, call, reply);
    if (!rc) rc = VgiOpaqueSealResponse(call, &reply->value);
    return rc;
}
int VgiWorkerAddProtocol(struct VgiWorker *worker, const struct VgiRpcProtocol *protocol,
                         struct ArrowError *error) {
    return VgiRpcServerAddProtocol(worker->rpc, protocol, error);
}
int VgiWorkerServe(struct VgiWorker *worker, FILE *input, FILE *output, struct ArrowError *error) {
    return VgiRpcServerServe(worker->rpc, input, output, error);
}

void VgiWorkerSetHttpOptions(struct VgiWorker *w, const struct VgiHttpOptions *o) {
    w->http_options = *o;
    w->has_http_options = 1;
}
int VgiWorkerSetTelemetry(struct VgiWorker *worker, struct VgiTelemetry *telemetry) {
    return worker ? VgiRpcServerSetTelemetry(worker->rpc, telemetry) : EINVAL;
}
static const struct VgiBatch *ticket_catalog(void *state, const char *name) {
    return VgiCatalogInfo(((struct VgiWorker *)state)->catalog_service, name);
}
int VgiWorkerServeHttp(struct VgiWorker *worker, const struct VgiHttpOptions *options,
                       struct ArrowError *error) {
    const struct VgiGrantKeys *keys =
        options->grant_keys ? options->grant_keys
                            : (options->identity ? options->identity->grant_keys : NULL);
    if (options->signing_key && *options->signing_key &&
        (keys || (options->identity && options->identity->mint_grant))) {
        unsigned char key[32];
        VgiSigningKey(options->signing_key, strlen(options->signing_key), key);
        int64_t ceiling = keys ? keys->max_ttl_seconds : 0;
        const char *env = getenv("VGI_RPC_GRANT_MAX_TTL_SECONDS");
        if (!keys && env && *env) {
            char *end = NULL;
            errno = 0;
            ceiling = strtoll(env, &end, 10);
            if (errno || *end || ceiling <= 0) {
                ArrowErrorSet(error, "Invalid grant maximum TTL");
                return EINVAL;
            }
        }
        int rc = VgiTicketRegister(worker->rpc, key, ceiling, ticket_catalog, worker, error);
        sodium_memzero(key, 32);
        if (rc) return rc;
    }
    return VgiRpcServerServeHttp(worker->rpc, options, error);
}
int VgiWorkerRun(struct VgiWorker *worker, int argc, char **argv, struct ArrowError *error) {
    const char *unix_path = NULL, *tcp_address = NULL, *http_port = NULL;
    double idle = 0;
    const char *grant_args[64];
    size_t grant_count = 0;
    const char *grant_audience = NULL, *grant_ttl = NULL, *access_log = getenv("VGI_ACCESS_LOG");
    for (int i = 1; i < argc; ++i) {
        if (!strcmp(argv[i], "--access-log")) {
            if (++i >= argc) return EINVAL;
            access_log = argv[i];
            continue;
        }
        if (!strcmp(argv[i], "--grant-key") || !strcmp(argv[i], "--grant-audience") ||
            !strcmp(argv[i], "--grant-max-ttl")) {
            const char *option = argv[i];
            if (++i >= argc) {
                ArrowErrorSet(error, "%s needs a value", option);
                return EINVAL;
            }
            if (!strcmp(option, "--grant-key")) {
                if (grant_count == 64) return EINVAL;
                grant_args[grant_count++] = argv[i];
            } else if (!strcmp(option, "--grant-audience"))
                grant_audience = argv[i];
            else
                grant_ttl = argv[i];
            continue;
        }
        if (!strcmp(argv[i], "--version")) {
            puts("vgi-c 0.1.0");
            return 0;
        }
        if (!strcmp(argv[i], "--unix") || !strcmp(argv[i], "--tcp") || !strcmp(argv[i], "--http") ||
            !strcmp(argv[i], "--idle-timeout")) {
            const char *option = argv[i];
            if (++i >= argc) {
                ArrowErrorSet(error, "%s needs a value", option);
                return EINVAL;
            }
            if (!strcmp(option, "--unix"))
                unix_path = argv[i];
            else if (!strcmp(option, "--tcp"))
                tcp_address = argv[i];
            else if (!strcmp(option, "--http"))
                http_port = argv[i];
            else {
                char *end = NULL;
                idle = strtod(argv[i], &end);
                if (!*argv[i] || *end || !isfinite(idle) || idle < 0 || idle > 1e9) {
                    ArrowErrorSet(error, "--idle-timeout needs a non-negative number of seconds");
                    return EINVAL;
                }
            }
            continue;
        }
        ArrowErrorSet(error, "Unknown worker option: %s", argv[i]);
        return EINVAL;
    }
    if ((unix_path != NULL) + (tcp_address != NULL) + (http_port != NULL) > 1) {
        ArrowErrorSet(error, "Select one transport");
        return EINVAL;
    }
    signal(SIGPIPE, SIG_IGN);
    if (http_port) {
        char *end = NULL;
        errno = 0;
        long port = strtol(http_port, &end, 10);
        if (errno || !*http_port || *end || port < 0 || port > 65535) {
            ArrowErrorSet(error, "--http needs a port between 0 and 65535");
            return EINVAL;
        }
        struct VgiHttpOptions options;
        if (worker->has_http_options)
            options = worker->http_options;
        else
            VgiHttpOptionsInit(&options);
        options.port = (unsigned)port;
        options.access_log = access_log;
        options.bearer_tokens = getenv("VGI_BEARER_TOKENS");
        options.optional_bearer_tokens = getenv("VGI_OPTIONAL_BEARER_TOKENS");
        options.signing_key = getenv("VGI_SIGNING_KEY");
        options.cors_origin = getenv("VGI_CORS_ORIGIN");
        const char *host = getenv("VGI_HTTP_HOST");
        if (host && *host) options.host = host;
        if (getenv("VGI_HTTP_NO_COMPRESSION")) options.compression = 0;
        unsigned char key_bytes[64][32];
        struct VgiGrantKeys keys = {.keys = (const unsigned char(*)[32])key_bytes,
                                    .max_ttl_seconds = 604800,
                                    .clock_skew_seconds = 60};
        const char *env = getenv("VGI_RPC_GRANT_KEYS");
        char *owned = NULL;
        if (!grant_count && env && *env) {
            owned = text_copy(env);
            if (!owned) return ENOMEM;
            char *p = owned;
            while (p && *p) {
                if (grant_count == 64) {
                    free(owned);
                    return EINVAL;
                }
                grant_args[grant_count++] = p;
                char *comma = strchr(p, ',');
                if (comma) *comma++ = 0;
                p = comma;
            }
        }
        int rc = 0;
        for (size_t i = 0; !rc && i < grant_count; ++i) {
            const char *text = grant_args[i];
            while (isspace((unsigned char)*text)) ++text;
            size_t n = strlen(text);
            while (n && isspace((unsigned char)text[n - 1])) --n;
            size_t decoded = 0;
            const char *end = NULL;
            int variant = n && text[n - 1] == '=' ? sodium_base64_VARIANT_ORIGINAL
                                                  : sodium_base64_VARIANT_ORIGINAL_NO_PADDING;
            if (sodium_base642bin(key_bytes[i], 32, text, n, NULL, &decoded, &end, variant) ||
                decoded != 32 || end != text + n)
                rc = EINVAL;
        }
        keys.key_count = grant_count;
        keys.audience = grant_audience ? grant_audience : getenv("VGI_RPC_GRANT_AUDIENCE");
        const char *ttl = grant_ttl ? grant_ttl : getenv("VGI_RPC_GRANT_MAX_TTL_SECONDS");
        if (grant_count && ttl && *ttl) {
            char *end = NULL;
            errno = 0;
            keys.max_ttl_seconds = strtoll(ttl, &end, 10);
            if (errno || end == ttl || *end) rc = EINVAL;
        }
        if (grant_count) {
            if (VgiGrantKeysValidate(&keys)) rc = EINVAL;
            options.grant_keys = &keys;
        }
        if (rc)
            ArrowErrorSet(error, "Invalid grant key configuration");
        else
            rc = VgiWorkerServeHttp(worker, &options, error);
        sodium_memzero(key_bytes, sizeof(key_bytes));
        free(owned);
        return rc;
    }
    if (unix_path || tcp_address)
        return VgiRpcServerServeSocket(worker->rpc, unix_path, tcp_address, idle, error);
    return VgiWorkerServe(worker, stdin, stdout, error);
}
