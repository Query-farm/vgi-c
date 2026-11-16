/* Protocol 3.2 filtered scan planning fixtures (the shared suite's
 * catalog/scan_planning_*.test), mirroring the vgi-python reference worker.
 *
 * Five days of data, one parquet file per day at
 * ${VGI_TEST_BRANCH_DIR}/planned_days_<day>.parquet. The tests write the first
 * four days only: the 2026-10-05 file never exists, so a scan that fails to
 * prune that day errors with "No files found", and a passing query proves the
 * pruning happened.
 *
 * - data.planned_days (filtered): the worker prunes the day files with the
 *   query's predicates (VgiFiltersMayMatch) and records what it saw in
 *   planned_days_last_plan.json.
 * - data.planned_days_stats (filtered): one branch per day carrying event_date
 *   statistics; the worker ignores the predicates and the client prunes.
 * - data.planned_days_bf (bind): one branch per day whose branch_filter the
 *   client contradicts with the query's predicates.
 * - data.multi_branch_comma_filter (bind): a two-expression branch_filter,
 *   which a 3.2 client rejects. */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include "fixtures.h"
#include "vgi/catalog.h"
#include "vgi/filter.h"
#include "vgi/statistics.h"

#define DAY_COUNT 5
static const char *const DAYS[DAY_COUNT] = {"2026-10-01", "2026-10-02", "2026-10-03", "2026-10-04",
                                            "2026-10-05"};
enum Planned { PLANNED_DAYS, PLANNED_DAYS_STATS, PLANNED_DAYS_BF, COMMA_FILTER, NOT_PLANNED };
static const struct {
    const char *name, *comment;
    int filtered, dated;
} TABLES[] = {
    {"planned_days",
     "Protocol 3.2: worker-side pruning of per-day files — scan_planning_filtered.test", 1, 1},
    {"planned_days_stats",
     "Protocol 3.2: client-side pruning by ScanBranch statistics — scan_planning_stats.test", 1, 1},
    {"planned_days_bf",
     "Protocol 3.2: pruning by branch_filter contradiction — scan_planning_branch_filter.test", 0,
     1},
    {"multi_branch_comma_filter", "Protocol 3.2: multi-expression branch_filter is rejected", 0, 0},
};

static int fail(struct ArrowError *error, const char *message) {
    ArrowErrorSet(error, "%s", message);
    return EINVAL;
}
static int branch_path(char *out, size_t size, const char *name) {
    const char *configured = getenv("VGI_TEST_BRANCH_DIR");
    const char *dir = configured && *configured ? configured : "/tmp";
    size_t length = strlen(dir);
    while (length > 1 && dir[length - 1] == '/') --length;
    int written = snprintf(out, size, "%.*s/%s", (int)length, dir, name);
    return written < 0 || (size_t)written >= size ? ENAMETOOLONG : 0;
}
static int day_file(char *out, size_t size, const char *day) {
    char name[64];
    snprintf(name, sizeof(name), "planned_days_%s.parquet", day);
    return branch_path(out, size, name);
}
/* The declared columns: event_date DATE, n BIGINT (n only for the comma table). */
static int columns(struct ArrowSchema *out, int dated) {
    ArrowSchemaInit(out);
    int rc = ArrowSchemaSetTypeStruct(out, dated ? 2 : 1);
    int n = dated ? 1 : 0;
    if (!rc && dated) rc = ArrowSchemaSetType(out->children[0], NANOARROW_TYPE_DATE32);
    if (!rc && dated) rc = ArrowSchemaSetName(out->children[0], "event_date");
    if (!rc) rc = ArrowSchemaSetType(out->children[n], NANOARROW_TYPE_INT64);
    if (!rc) rc = ArrowSchemaSetName(out->children[n], "n");
    return rc;
}
static enum Planned planned_table(const struct VgiBatch *request) {
    const struct ArrowArrayView *path = VgiBatchGet(request, "schema_path");
    if (!path || ArrowArrayViewIsNull(path, 0)) return NOT_PLANNED;
    int64_t start = ArrowArrayViewListChildOffset(path, 0),
            end = ArrowArrayViewListChildOffset(path, 1);
    struct ArrowStringView schema =
        end - start == 1 ? VgiValueString(path->children[0], start) : (struct ArrowStringView){0};
    if (schema.size_bytes != 4 || strncasecmp(schema.data, "data", 4)) return NOT_PLANNED;
    struct ArrowStringView name = VgiBatchString(request, "name");
    for (int i = 0; i < NOT_PLANNED && name.data; ++i)
        if (name.size_bytes == (int64_t)strlen(TABLES[i].name) &&
            !strncasecmp(name.data, TABLES[i].name, (size_t)name.size_bytes))
            return (enum Planned)i;
    return NOT_PLANNED;
}

/* ---- TableInfo descriptors -------------------------------------------------- */

static int table_info(int index, struct ArrowBuffer *out, struct ArrowError *error) {
    struct ArrowSchema schema = {0};
    struct VgiBatch info = {0};
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    int rc = columns(&schema, TABLES[index].dated);
    if (!rc) rc = VgiIpcWriteSchema(&schema, &bytes, error);
    if (!rc) rc = VgiBatchInitNamed(&info, "TableInfo", error);
    if (!rc) rc = VgiBatchSetString(&info, "name", TABLES[index].name);
    if (!rc) rc = VgiBatchSetString(&info, "comment", TABLES[index].comment);
    struct ArrowArray *path = rc ? NULL : VgiBatchColumn(&info, "schema_path");
    if (!rc) rc = ArrowArrayAppendString(path->children[0], ArrowCharView("data"));
    if (!rc) rc = ArrowArrayFinishElement(path);
    if (!rc) rc = VgiBatchSetBytes(&info, "columns", bytes.data, bytes.size_bytes);
    if (!rc)
        rc = VgiBatchSetString(
            &info, "scan_planning",
            TABLES[index].filtered ? VGI_SCAN_PLANNING_FILTERED : VGI_SCAN_PLANNING_BIND);
    if (!rc) rc = VgiBatchFinishRow(&info, error);
    if (!rc) rc = VgiIpcWriteBatch(&info.schema, &info.array, NULL, out, error);
    if (schema.release) ArrowSchemaRelease(&schema);
    VgiBatchReset(&info);
    ArrowBufferReset(&bytes);
    return rc;
}
/* Copy declarative contents, adding the fixture tables to schema "data". */
int ExampleScanPlanningContents(struct VgiBatch *contents, struct ArrowError *error) {
    int64_t schemas_id = VgiFieldIndex(&contents->schema, "schemas");
    if (schemas_id < 0 || contents->array.length != 1) return fail(error, "unexpected contents");
    const struct ArrowSchema *entry_schema = contents->schema.children[schemas_id]->children[0];
    int64_t path_id = VgiFieldIndex(entry_schema, "path"),
            tables_id = VgiFieldIndex(entry_schema, "tables");
    if (path_id < 0 || tables_id < 0) return fail(error, "unexpected contents entry");
    struct ArrowBuffer infos[NOT_PLANNED];
    for (int i = 0; i < NOT_PLANNED; ++i) ArrowBufferInit(&infos[i]);
    struct VgiBatch out = {0};
    int rc = 0, added = 0;
    for (int i = 0; !rc && i < NOT_PLANNED; ++i) rc = table_info(i, &infos[i], error);
    if (!rc) rc = VgiBatchInit(&out, &contents->schema, error);
    for (int64_t field = 0; !rc && field < contents->schema.n_children; ++field) {
        if (field != schemas_id) {
            rc = VgiArrayAppendValue(out.array.children[field], out.schema.children[field],
                                     contents->view.children[field],
                                     contents->schema.children[field], 0, error);
            continue;
        }
        const struct ArrowArrayView *list = contents->view.children[field],
                                    *entries = list->children[0];
        struct ArrowArray *out_list = out.array.children[field],
                          *out_entries = out_list->children[0];
        const struct ArrowSchema *out_entry = out.schema.children[field]->children[0];
        for (int64_t row = ArrowArrayViewListChildOffset(list, 0);
             !rc && row < ArrowArrayViewListChildOffset(list, 1); ++row) {
            const struct ArrowArrayView *path = entries->children[path_id];
            int64_t first = ArrowArrayViewListChildOffset(path, row),
                    last = ArrowArrayViewListChildOffset(path, row + 1);
            struct ArrowStringView name = last - first == 1
                                              ? VgiValueString(path->children[0], first)
                                              : (struct ArrowStringView){0};
            int data = name.size_bytes == 4 && !memcmp(name.data, "data", 4);
            for (int64_t k = 0; !rc && k < entry_schema->n_children; ++k) {
                if (k != tables_id || !data) {
                    rc = VgiArrayAppendValue(out_entries->children[k], out_entry->children[k],
                                             entries->children[k], entry_schema->children[k], row,
                                             error);
                    continue;
                }
                const struct ArrowArrayView *tables = entries->children[k];
                struct ArrowArray *out_tables = out_entries->children[k];
                for (int64_t t = ArrowArrayViewListChildOffset(tables, row);
                     !rc && t < ArrowArrayViewListChildOffset(tables, row + 1); ++t)
                    rc = VgiArrayAppendValue(
                        out_tables->children[0], out_entry->children[k]->children[0],
                        tables->children[0], entry_schema->children[k]->children[0], t, error);
                for (int i = 0; !rc && i < NOT_PLANNED; ++i)
                    rc = ArrowArrayAppendBytes(
                        out_tables->children[0],
                        (struct ArrowBufferView){{infos[i].data}, infos[i].size_bytes});
                if (!rc) rc = ArrowArrayFinishElement(out_tables);
                added = 1;
            }
            if (!rc) rc = ArrowArrayFinishElement(out_entries);
        }
        if (!rc) rc = ArrowArrayFinishElement(out_list);
    }
    if (!rc && !added) rc = fail(error, "the example catalog has no schema \"data\"");
    if (!rc) rc = ArrowArrayFinishElement(&out.array);
    if (!rc) rc = VgiBatchFinish(&out, error);
    if (!rc) {
        VgiBatchReset(contents);
        *contents = out;
        memset(&out, 0, sizeof(out));
    }
    VgiBatchReset(&out);
    for (int i = 0; i < NOT_PLANNED; ++i) ArrowBufferReset(&infos[i]);
    return rc;
}

/* ---- catalog_table_scan_branches_get ----------------------------------------- */

/* Serialized positional scan arguments: one row, arg_0 either a file list
 * (list<utf8>), one file (utf8) or an integer. */
static int scan_arguments(const char *const *files, size_t count, int as_list, int64_t integer,
                          struct ArrowBuffer *out, struct ArrowError *error) {
    struct ArrowSchema schema = {0};
    struct VgiBatch args = {0};
    ArrowSchemaInit(&schema);
    int rc = ArrowSchemaSetTypeStruct(&schema, 1);
    if (!rc && as_list) {
        rc = ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_LIST);
        if (!rc) rc = ArrowSchemaSetType(schema.children[0]->children[0], NANOARROW_TYPE_STRING);
    } else if (!rc)
        rc = ArrowSchemaSetType(schema.children[0],
                                files ? NANOARROW_TYPE_STRING : NANOARROW_TYPE_INT64);
    if (!rc) rc = ArrowSchemaSetName(schema.children[0], "arg_0");
    if (!rc) rc = VgiBatchInit(&args, &schema, error);
    struct ArrowArray *column = rc ? NULL : args.array.children[0];
    if (!rc && as_list) {
        for (size_t i = 0; !rc && i < count; ++i)
            rc = ArrowArrayAppendString(column->children[0], ArrowCharView(files[i]));
        if (!rc) rc = ArrowArrayFinishElement(column);
    } else if (!rc)
        rc = files ? ArrowArrayAppendString(column, ArrowCharView(files[0]))
                   : ArrowArrayAppendInt(column, integer);
    if (!rc) rc = VgiBatchFinishRow(&args, error);
    if (!rc) rc = VgiIpcWriteBatch(&args.schema, &args.array, NULL, out, error);
    ArrowSchemaRelease(&schema);
    VgiBatchReset(&args);
    return rc;
}
/* What each day's file holds of event_date: exactly that day, never NULL. The
 * bound is the ISO text, which the client casts to the column's DATE. */
static struct VgiColumnStatistics day_statistics(const char *day) {
    struct VgiColumnStatistics stat = {0};
    stat.name = "event_date";
    stat.min = (struct VgiStatValue){VGI_STAT_STRING, 1, 0, 0, ArrowCharView(day)};
    stat.max = stat.min;
    stat.has_null = 0;
    stat.has_not_null = 1;
    stat.distinct_count = -1;
    stat.max_string_length = -1;
    stat.contains_unicode = -1;
    return stat;
}
struct Branch {
    const char *function_name;
    struct ArrowBuffer arguments;
    const char *branch_filter;
    const char *schema;
    int64_t row_count; /* < 0: unknown */
    const struct VgiColumnStatistics *statistics;
};
static int append_branch(struct VgiBatch *result, const struct Branch *b,
                         struct ArrowError *error) {
    struct VgiBatch branch = {0}, stats = {0};
    struct ArrowBuffer bytes, stat_bytes;
    ArrowBufferInit(&bytes);
    ArrowBufferInit(&stat_bytes);
    int rc = VgiBatchInitNamed(&branch, "ScanBranch", error);
    if (!rc) rc = VgiBatchSetString(&branch, "function_name", b->function_name);
    if (!rc)
        rc = VgiBatchSetBytes(&branch, "arguments", b->arguments.data, b->arguments.size_bytes);
    if (!rc && b->branch_filter) rc = VgiBatchSetString(&branch, "branch_filter", b->branch_filter);
    if (!rc) rc = VgiBatchSetInt(&branch, "writable", 0);
    if (!rc && b->schema) {
        struct ArrowArray *path = VgiBatchColumn(&branch, "schema_path");
        rc = ArrowArrayAppendString(path->children[0], ArrowCharView(b->schema));
        if (!rc) rc = ArrowArrayFinishElement(path);
    }
    if (!rc && b->row_count >= 0) rc = VgiBatchSetInt(&branch, "row_count", b->row_count);
    if (!rc && b->statistics) {
        rc = VgiStatisticsInit(&stats, error);
        if (!rc) rc = VgiStatisticsAppend(&stats, b->statistics, error);
        if (!rc) rc = VgiIpcWriteBatch(&stats.schema, &stats.array, NULL, &stat_bytes, error);
        if (!rc)
            rc = VgiBatchSetBytes(&branch, "column_statistics", stat_bytes.data,
                                  stat_bytes.size_bytes);
    }
    if (!rc) rc = VgiBatchFinishRow(&branch, error);
    if (!rc) rc = VgiIpcWriteBatch(&branch.schema, &branch.array, NULL, &bytes, error);
    if (!rc)
        rc = ArrowArrayAppendBytes(VgiBatchColumn(result, "branches")->children[0],
                                   (struct ArrowBufferView){{bytes.data}, bytes.size_bytes});
    VgiBatchReset(&branch);
    VgiBatchReset(&stats);
    ArrowBufferReset(&bytes);
    ArrowBufferReset(&stat_bytes);
    return rc;
}
/* Record what a filtered planning call saw, so a test can assert that the
 * predicates arrived. Only calls carrying predicates write it: filterless
 * fetches (vgi_table_branches() in concurrently running tests sharing the
 * directory) would otherwise overwrite it. */
static int record_plan(const char *const *days, size_t count, struct ArrowError *error) {
    char path[4096], temporary[4200];
    int rc = branch_path(path, sizeof(path), "planned_days_last_plan.json");
    if (rc) return fail(error, "VGI_TEST_BRANCH_DIR is too long");
    snprintf(temporary, sizeof(temporary), "%s.%ld.tmp", path, (long)getpid());
    FILE *file = fopen(temporary, "w");
    if (!file) return fail(error, "cannot write planned_days_last_plan.json");
    fputs("{\"received_filters\": true, \"days\": [", file);
    for (size_t i = 0; i < count; ++i) fprintf(file, "%s\"%s\"", i ? ", " : "", days[i]);
    fputs("]}", file);
    if (fclose(file) || rename(temporary, path)) {
        remove(temporary);
        return fail(error, "cannot write planned_days_last_plan.json");
    }
    return 0;
}
int ExampleScanPlanningHandler(struct VgiRpcCall *call, struct VgiRpcReply *reply, int *handled) {
    if (strcmp(call->method->name, "catalog_table_scan_branches_get")) return 0;
    enum Planned table = planned_table(&call->request);
    if (table == NOT_PLANNED) return 0;
    *handled = 1;
    struct VgiBatch result = {0};
    struct VgiFilters *filters = NULL;
    struct ArrowSchema declared = {0};
    char files[DAY_COUNT][4096];
    const char *kept[DAY_COUNT], *kept_files[DAY_COUNT];
    size_t count = 0;
    int rc = VgiBatchInitNamed(&result, "ScanBranchesResult", &call->error);
    for (int i = 0; !rc && i < DAY_COUNT; ++i) rc = day_file(files[i], sizeof(files[i]), DAYS[i]);
    if (!rc && table == PLANNED_DAYS) {
        rc = columns(&declared, 1);
        if (!rc)
            rc = VgiScanPlanningFiltersParse(&call->request, &declared, &filters, &call->error);
        for (int i = 0; !rc && i < DAY_COUNT; ++i) {
            struct VgiColumnStatistics stat = day_statistics(DAYS[i]);
            if (!VgiFiltersMayMatch(filters, &stat, 1)) continue;
            kept[count] = DAYS[i];
            kept_files[count++] = files[i];
        }
        if (!rc && filters) rc = record_plan(kept, count, &call->error);
        if (!rc && count) {
            struct Branch branch = {"read_parquet", {0}, NULL, NULL, 10 * (int64_t)count, NULL};
            ArrowBufferInit(&branch.arguments);
            rc = scan_arguments(kept_files, count, 1, 0, &branch.arguments, &call->error);
            if (!rc) rc = append_branch(&result, &branch, &call->error);
            ArrowBufferReset(&branch.arguments);
        }
    } else if (!rc && (table == PLANNED_DAYS_STATS || table == PLANNED_DAYS_BF)) {
        for (int i = 0; !rc && i < DAY_COUNT; ++i) {
            char condition[64];
            snprintf(condition, sizeof(condition), "event_date = DATE '%s'", DAYS[i]);
            struct VgiColumnStatistics stat = day_statistics(DAYS[i]);
            const char *file = files[i];
            int stats = table == PLANNED_DAYS_STATS;
            struct Branch branch = {"read_parquet",           {0},
                                    stats ? NULL : condition, NULL,
                                    stats ? 10 : -1,          stats ? &stat : NULL};
            ArrowBufferInit(&branch.arguments);
            rc = scan_arguments(&file, 1, 0, 0, &branch.arguments, &call->error);
            if (!rc) rc = append_branch(&result, &branch, &call->error);
            ArrowBufferReset(&branch.arguments);
        }
    } else if (!rc) {
        /* "n < 5, n > 2" parses as two expressions; 3.1 clients kept the first. */
        struct Branch branch = {"sequence", {0}, "n < 5, n > 2", "main", -1, NULL};
        ArrowBufferInit(&branch.arguments);
        rc = scan_arguments(NULL, 0, 0, 10, &branch.arguments, &call->error);
        if (!rc) rc = append_branch(&result, &branch, &call->error);
        ArrowBufferReset(&branch.arguments);
    }
    if (!rc) rc = ArrowArrayFinishElement(VgiBatchColumn(&result, "branches"));
    struct ArrowArray *extensions = rc ? NULL : VgiBatchColumn(&result, "required_extensions");
    if (!rc && table != COMMA_FILTER)
        rc = ArrowArrayAppendString(extensions->children[0], ArrowCharView("parquet"));
    if (!rc) rc = ArrowArrayFinishElement(extensions);
    if (!rc) rc = VgiBatchFinishRow(&result, &call->error);
    if (!rc) rc = VgiRpcReplyPayload(call, reply, &result);
    VgiFiltersFree(filters);
    if (declared.release) ArrowSchemaRelease(&declared);
    VgiBatchReset(&result);
    return rc;
}
