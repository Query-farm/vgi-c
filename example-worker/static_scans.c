#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "fixtures.h"
static int named(const struct ExampleFunction *f, const char *s) { return !strcmp(f->name, s); }
static int rff(const struct ExampleFunction *f) { return !strncmp(f->name, "rff_", 4); }
static int repro(const struct ExampleFunction *f) { return !strncmp(f->name, "proj_repro_", 11); }
static int tt(const struct ExampleFunction *f) { return !strncmp(f->name, "tt_pushdown", 11); }
int ExampleStaticSupported(const char *s) {
    const char *names[] = {"departments_scan",
                           "products_scan",
                           "employees_scan",
                           "projects_scan",
                           "colors_scan",
                           "rowid_sequence",
                           "filter_echo_table_scan",
                           "versioned_constraints_scan",
                           "versioned_data_scan",
                           "tt_pushdown_scan",
                           "tt_pushdown_cols_scan",
                           "late_materialization",
                           "rff_simple_scan",
                           "rff_none_scan",
                           "rff_struct_scan",
                           "rff_multi_scan",
                           "rff_nested_scan",
                           "rff_rowid_scan",
                           "narrow_scan",
                           "wide_scan",
                           "proj_repro_strict",
                           "proj_repro_full_schema",
                           "proj_repro_chunked",
                           "proj_repro_multi_worker",
                           "versioned_tables_animals_scan",
                           "versioned_tables_animals_color_scan",
                           "versioned_tables_plants_scan"};
    for (size_t i = 0; i < sizeof(names) / sizeof(*names); ++i)
        if (!strcmp(s, names[i])) return 1;
    return 0;
}
static int field(struct ArrowSchema *s, const char *name, const char *format) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetFormat(s, format));
    return ArrowSchemaSetName(s, name);
}
static int marker(struct ArrowSchema *s) {
    struct ArrowBuffer m;
    ArrowBufferInit(&m);
    int rc = VgiMetadataSet(&m, "is_row_id", "");
    if (!rc) rc = ArrowSchemaSetMetadata(s, (const char *)m.data);
    ArrowBufferReset(&m);
    return rc;
}
static int text_is(struct ArrowStringView s, const char *t) {
    return s.size_bytes == (int64_t)strlen(t) && !memcmp(s.data, t, (size_t)s.size_bytes);
}
static int64_t version(struct ExampleFunction *f, struct VgiFunctionCall *call) {
    int64_t v = VgiArgumentInt(call, 0, NULL, tt(f) ? 2 : 3);
    if (named(f, "tt_pushdown_scan")) {
        struct ArrowStringView unit = VgiBatchString(&call->bind_request, "at_unit"),
                               value = VgiBatchString(&call->bind_request, "at_value");
        char text[64];
        snprintf(text, sizeof(text), "%.*s", (int)(value.size_bytes < 63 ? value.size_bytes : 63),
                 value.data ? value.data : "");
        if (text_is(unit, "VERSION") || text_is(unit, "version"))
            v = strtoll(text, NULL, 10);
        else if (text_is(unit, "TIMESTAMP") || text_is(unit, "timestamp"))
            v = atoi(text) <= 2020 ? 1 : 2;
    }
    return v;
}
int ExampleStaticBind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *out) {
    struct ExampleFunction *f = data;
    const char *names[12] = {"id"}, *formats[12] = {"l"};
    int n = 1;
    int64_t v = version(f, call);
    int rowid = -1;
    if (named(f, "departments_scan")) {
        n = 3;
        names[1] = "name";
        names[2] = "budget";
        formats[1] = "u";
        formats[2] = "g";
    } else if (named(f, "products_scan")) {
        n = 4;
        names[1] = "name";
        names[2] = "quantity";
        names[3] = "price";
        formats[1] = "u";
        formats[2] = "l";
        formats[3] = "g";
    } else if (named(f, "employees_scan") || named(f, "versioned_constraints_scan")) {
        n = named(f, "employees_scan") ? 4 : v == 1 ? 2 : v == 2 ? 3 : 4;
        names[1] = "name";
        names[2] = "email";
        names[3] = "department_id";
        formats[1] = formats[2] = "u";
        formats[3] = "l";
    } else if (named(f, "projects_scan")) {
        n = 3;
        names[0] = "department_id";
        names[1] = "project_code";
        names[2] = "title";
        formats[1] = formats[2] = "u";
    } else if (named(f, "colors_scan")) {
        n = 3;
        names[1] = "color";
        names[2] = "hex_code";
        formats[1] = formats[2] = "u";
    } else if (named(f, "versioned_data_scan")) {
        n = v == 1 ? 1 : v == 2 ? 4 : 2;
        names[1] = v == 2 ? "name" : "score";
        names[2] = "score";
        names[3] = "active";
        formats[1] = v == 2 ? "u" : "g";
        formats[2] = "g";
        formats[3] = "b";
    } else if (tt(f)) {
        n = 4;
        names[1] = "val";
        names[2] = "seen_version";
        names[3] = "pushed_filters";
        formats[1] = formats[2] = "l";
        formats[3] = "u";
    } else if (named(f, "filter_echo_table_scan")) {
        n = 3;
        names[0] = "n";
        names[1] = "s";
        names[2] = "pushed_filters";
        formats[1] = formats[2] = "u";
    } else if (named(f, "late_materialization")) {
        n = 4;
        names[0] = "row_id";
        names[1] = "ord";
        names[2] = "payload";
        names[3] = "pushed";
        formats[1] = "l";
        formats[2] = formats[3] = "u";
        rowid = 0;
    } else if (named(f, "rowid_sequence")) {
        n = 3;
        struct ArrowStringView layout = VgiArgumentString(call, -1, "layout"),
                               type = VgiArgumentString(call, -1, "row_id_type");
        if (layout.data && !text_is(layout, "first") && !text_is(layout, "middle") &&
            !text_is(layout, "last"))
            return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "",
                              "layout must be one of the allowed choices: first, middle, last");
        if (type.data && !text_is(type, "int64") && !text_is(type, "string") &&
            !text_is(type, "struct"))
            return VgiRpcFail(
                call->rpc, "ValueError", "INVALID_ARGUMENT", "",
                "row_id_type must be one of the allowed choices: int64, string, struct");
        rowid = text_is(layout, "middle") ? 1 : text_is(layout, "last") ? 2 : 0;
        names[0] = rowid == 0 ? "row_id" : "name";
        names[1] = rowid == 0 ? "name" : rowid == 1 ? "row_id" : "value";
        names[2] = rowid == 2 ? "row_id" : "value";
        formats[0] = formats[1] = formats[2] = "u";
        formats[rowid] = text_is(type, "struct") ? "+s" : text_is(type, "string") ? "u" : "l";
    } else if (rff(f)) {
        n = named(f, "rff_nested_scan") ? 1 : named(f, "rff_rowid_scan") ? 3 : 2;
        names[0] = named(f, "rff_nested_scan")                                 ? "wrapper"
                   : named(f, "rff_rowid_scan")                                ? "row_id"
                   : named(f, "rff_struct_scan") || named(f, "rff_multi_scan") ? "s"
                                                                               : "a";
        names[1] = named(f, "rff_rowid_scan")    ? "bbox"
                   : named(f, "rff_struct_scan") ? "other"
                   : named(f, "rff_multi_scan")  ? "top"
                                                 : "b";
        names[2] = "other";
        formats[1] = formats[2] = "l";
        if (named(f, "rff_rowid_scan")) rowid = 0;
    } else if (repro(f)) {
        n = 12;
        const char *rn[] = {"topic",          "partition",    "offset",          "timestamp",
                            "timestamp_type", "key",          "key_string",      "key_schema_id",
                            "value",          "value_string", "value_schema_id", "headers"};
        const char *rf[] = {"u", "i", "l", "tsm:UTC", "u", "z", "u", "i", "z", "u", "i", "+l"};
        memcpy(names, rn, sizeof(rn));
        memcpy(formats, rf, sizeof(rf));
    } else if (named(f, "narrow_scan") || named(f, "wide_scan")) {
        n = named(f, "narrow_scan") ? 1 : 2;
        names[0] = "id";
        names[1] = "val";
        formats[1] = "l";
    } else if (!strncmp(f->name, "versioned_tables_", 17)) {
        n = named(f, "versioned_tables_animals_color_scan") ? 4 : 3;
        names[0] = "name";
        names[1] = named(f, "versioned_tables_plants_scan") ? "kind" : "legs";
        names[2] = named(f, "versioned_tables_plants_scan") ? "height_m" : "sound";
        names[3] = "color";
        formats[0] = "u";
        formats[1] = named(f, "versioned_tables_plants_scan") ? "u" : "l";
        formats[2] = named(f, "versioned_tables_plants_scan") ? "g" : "u";
        formats[3] = "u";
    }
    ArrowSchemaInit(out);
    int rc = ArrowSchemaSetTypeStruct(out, n);
    for (int i = 0; !rc && i < n; ++i) {
        struct ArrowSchema *s = out->children[i];
        rc = field(s, names[i], formats[i]);
        if (!rc &&
            ((!strcmp(names[i], "s") && rff(f)) || (i == rowid && !strcmp(formats[i], "+s")))) {
            rc = ArrowSchemaSetTypeStruct(s, 2);
            if (!rc) rc = field(s->children[0], "a", "l");
            if (!rc) rc = field(s->children[1], "b", rff(f) ? "l" : "u");
        }
        if (!rc && !strcmp(names[i], "bbox")) {
            rc = ArrowSchemaSetTypeStruct(s, 4);
            const char *corners[] = {"xmin", "ymin", "xmax", "ymax"};
            for (int j = 0; !rc && j < 4; ++j) rc = field(s->children[j], corners[j], "f");
        }
        if (!rc && !strcmp(names[i], "wrapper")) {
            rc = ArrowSchemaSetTypeStruct(s, 1);
            if (!rc) rc = ArrowSchemaSetTypeStruct(s->children[0], 1);
            if (!rc) rc = ArrowSchemaSetName(s->children[0], "mid");
            if (!rc) rc = field(s->children[0]->children[0], "leaf", "l");
        }
        if (!rc && !strcmp(names[i], "headers")) {
            rc = ArrowSchemaSetType(s, NANOARROW_TYPE_LIST);
            if (!rc) rc = ArrowSchemaSetTypeStruct(s->children[0], 2);
            if (!rc) rc = field(s->children[0]->children[0], "k", "u");
            if (!rc) rc = field(s->children[0]->children[1], "v", "z");
        }
        if (!rc && i == rowid) rc = marker(s);
        if (repro(f) && (i < 3 || i == 11)) s->flags &= ~ARROW_FLAG_NULLABLE;
    }
    return rc;
}
struct StaticState {
    struct ArrowSchema schema;
    int64_t count, cursor, batch, version;
};
static int64_t rows(struct ExampleFunction *f, struct VgiFunctionCall *call) {
    int64_t v = version(f, call);
    if (named(f, "rowid_sequence") || named(f, "late_materialization") || repro(f)) {
        int64_t n = VgiArgumentInt(call, 0, NULL, 0);
        return n > 0 ? n : 0;
    }
    if (named(f, "filter_echo_table_scan")) return 100;
    if (tt(f)) return v == 1 ? 5 : 10;
    if (named(f, "rff_rowid_scan")) return 10;
    if (named(f, "rff_multi_scan")) return 2;
    if (named(f, "versioned_data_scan")) return v == 1 ? 3 : v == 2 ? 5 : 4;
    if (named(f, "versioned_constraints_scan")) return v == 1 ? 2 : 3;
    if (named(f, "employees_scan") || !strncmp(f->name, "versioned_tables_", 17)) return 5;
    if (named(f, "narrow_scan") || named(f, "wide_scan")) return 3;
    return 3;
}
int64_t ExampleStaticWorkers(void *data, struct VgiFunctionCall *call) {
    (void)call;
    return named(data, "proj_repro_multi_worker") ? 4 : 1;
}
int ExampleStaticInit(void *data, struct VgiFunctionCall *call) {
    struct ExampleFunction *f = data;
    struct StaticState *s = calloc(1, sizeof(*s));
    if (!s) return ENOMEM;
    call->state = s;
    s->count = rows(f, call);
    s->version = version(f, call);
    s->batch = named(f, "late_materialization") ? VgiArgumentInt(call, -1, "batch_size", 2048)
               : named(f, "proj_repro_chunked") || named(f, "proj_repro_multi_worker") ? 2
                                                                                       : 2048;
    if (s->batch < 1) s->batch = 1;
    return ExampleStaticBind(data, call, &s->schema);
}
void ExampleStaticFree(void *data, struct VgiFunctionCall *call) {
    (void)data;
    struct StaticState *s = call->state;
    if (s && s->schema.release) ArrowSchemaRelease(&s->schema);
    free(s);
    call->state = NULL;
}
static int append(struct ExampleFunction *f, struct VgiFunctionCall *call, struct StaticState *s,
                  const struct ArrowSchema *schema, struct ArrowArray *array, int64_t row,
                  const char *path, struct ArrowStringView witness) {
    const char *name = schema->name;
    char text[128];
    int64_t value = row + 1;
    if (!strcmp(schema->format, "+s")) {
        for (int64_t i = 0; i < schema->n_children; ++i) {
            char nested[256];
            snprintf(nested, sizeof(nested), "%s.%s", path, schema->children[i]->name);
            NANOARROW_RETURN_NOT_OK(
                append(f, call, s, schema->children[i], array->children[i], row, nested, witness));
        }
        return ArrowArrayFinishElement(array);
    }
    if (repro(f)) {
        if (!strcmp(name, "headers")) return ArrowArrayFinishElement(array);
        if (!strcmp(name, "partition")) return ArrowArrayAppendInt(array, row % 4);
        if (!strcmp(name, "offset")) return ArrowArrayAppendInt(array, row);
        if (!strcmp(name, "topic"))
            return ArrowArrayAppendString(array, ArrowCharView("demo_topic"));
        if (!strcmp(name, "key") || !strcmp(name, "key_string") || !strcmp(name, "value") ||
            !strcmp(name, "value_string")) {
            snprintf(text, sizeof(text), "%c%lld", name[0] == 'k' ? 'k' : 'v', (long long)row);
            return ArrowArrayAppendString(array, ArrowCharView(text));
        }
        return ArrowArrayAppendNull(array, 1);
    }
    if (named(f, "rowid_sequence")) {
        if (!strcmp(path, "row_id")) {
            if (!strcmp(schema->format, "l")) return ArrowArrayAppendInt(array, row);
            snprintf(text, sizeof(text), "rid_%lld", (long long)row);
        } else if (!strcmp(path, "row_id.a"))
            return ArrowArrayAppendInt(array, row);
        else
            snprintf(text, sizeof(text), "%s%lld",
                     !strcmp(path, "row_id.b") ? "s_"
                     : !strcmp(name, "name")   ? "item_"
                                               : "val_",
                     (long long)row);
        return ArrowArrayAppendString(array, ArrowCharView(text));
    }
    if (rff(f)) {
        if (!strcmp(path, "row_id"))
            value = row;
        else if (!strcmp(path, "bbox.xmin"))
            return ArrowArrayAppendDouble(array, (double)row);
        else if (!strncmp(path, "bbox.", 5))
            return ArrowArrayAppendDouble(array, !strcmp(name, "ymin")   ? 2
                                                 : !strcmp(name, "xmax") ? 3
                                                                         : 4);
        else
            value *= !strcmp(path, "a") || !strcmp(path, "s.a") || !strcmp(path, "wrapper.mid.leaf")
                         ? 1
                     : !strcmp(path, "other") || !strcmp(path, "top") ? 100
                                                                      : 10;
        return ArrowArrayAppendInt(array, value);
    }
    if (named(f, "late_materialization")) {
        if (!strcmp(name, "row_id"))
            return ArrowArrayAppendInt(array,
                                       VgiArgumentInt(call, -1, "dup_row_id", 0) ? row / 2 : row);
        if (!strcmp(name, "ord")) {
            int64_t stride = VgiArgumentInt(call, -1, "null_ord_stride", 0);
            return stride > 0 && row % stride == 0
                       ? ArrowArrayAppendNull(array, 1)
                       : ArrowArrayAppendInt(
                             array, (int64_t)(((uint64_t)row * UINT64_C(2654435761)) % 1000000007));
        }
        if (!strcmp(name, "pushed")) return ArrowArrayAppendString(array, witness);
        snprintf(text, sizeof(text), "payload_%lld", (long long)row);
        return ArrowArrayAppendString(array, ArrowCharView(text));
    }
    if (named(f, "filter_echo_table_scan")) {
        if (!strcmp(name, "n")) return ArrowArrayAppendInt(array, row);
        if (!strcmp(name, "pushed_filters")) return ArrowArrayAppendString(array, witness);
        snprintf(text, sizeof(text), "row_%lld", (long long)row);
        return ArrowArrayAppendString(array, ArrowCharView(text));
    }
    if (tt(f)) {
        if (!strcmp(name, "pushed_filters")) return ArrowArrayAppendString(array, witness);
        return ArrowArrayAppendInt(array, !strcmp(name, "id")    ? value
                                          : !strcmp(name, "val") ? value * 10
                                                                 : s->version);
    }
    const char *people[] = {"Alice", "Bob", "Carol", "Dave", "Eve"},
               *lower[] = {"alice", "bob", "carol", "dave", "eve"};
    const char *str = NULL;
    double real = 0;
    if (named(f, "departments_scan")) {
        const char *v[] = {"Engineering", "Sales", "HR"};
        double d[] = {500000, 300000, 200000};
        str = v[row];
        real = d[row];
    } else if (named(f, "products_scan")) {
        const char *v[] = {"Widget", "Gadget", "Doohickey"};
        double d[] = {9.99, 24.99, 4.99};
        int64_t q[] = {100, 50, 200};
        str = v[row];
        real = d[row];
        if (!strcmp(name, "quantity")) value = q[row];
    } else if (named(f, "employees_scan") || named(f, "versioned_constraints_scan")) {
        str = people[row];
        if (!strcmp(name, "email")) {
            snprintf(text, sizeof(text), "%s@co.com", lower[row]);
            if (named(f, "versioned_constraints_scan"))
                snprintf(text, sizeof(text), "%c@co", lower[row][0]);
            str = text;
        }
        if (!strcmp(name, "department_id"))
            value = named(f, "employees_scan") ? row / 2 + 1 : row % 2 + 1;
    } else if (named(f, "projects_scan")) {
        const char *codes[] = {"P001", "P002", "P003"},
                   *titles[] = {"Backend API", "Frontend UI", "Sales Portal"};
        str = !strcmp(name, "project_code") ? codes[row] : titles[row];
        value = row / 2 + 1;
    } else if (named(f, "colors_scan")) {
        const char *colors[] = {"blue", "green", "red"}, *hex[] = {"#0000FF", "#00FF00", "#FF0000"};
        str = !strcmp(name, "color") ? colors[row] : hex[row];
    } else if (named(f, "versioned_data_scan")) {
        str = lower[row];
        real = (double)value * 10 + (s->version == 2 ? 0 : 5);
        if (!strcmp(name, "active")) value = row % 2 == 0;
    } else if (named(f, "narrow_scan") || named(f, "wide_scan"))
        value = !strcmp(name, "val") ? (row + 1) * 10 : row;
    else if (!strncmp(f->name, "versioned_tables_", 17)) {
        const char *animals[] = {"chicken", "cow", "horse", "pig", "sheep"},
                   *sounds[] = {"cluck", "moo", "neigh", "oink", "baa"},
                   *colors[] = {"red", "brown", "black", "pink", "white"},
                   *plants[] = {"oak", "pine", "rose", "tomato", "wheat"},
                   *kinds[] = {"tree", "tree", "flower", "vegetable", "grass"};
        double h[] = {20, 25, .6, 1.5, 1};
        str = named(f, "versioned_tables_plants_scan")
                  ? (!strcmp(name, "name") ? plants[row] : kinds[row])
              : !strcmp(name, "name")  ? animals[row]
              : !strcmp(name, "sound") ? sounds[row]
                                       : colors[row];
        value = row ? 4 : 2;
        real = h[row];
    }
    if (!strcmp(schema->format, "u"))
        return ArrowArrayAppendString(array, ArrowCharView(str ? str : ""));
    if (!strcmp(schema->format, "g")) return ArrowArrayAppendDouble(array, real);
    return ArrowArrayAppendInt(array, value);
}
int ExampleStaticProcess(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                         struct VgiBatch *out, struct ArrowBuffer *metadata, int *finished) {
    (void)input;
    (void)metadata;
    struct ExampleFunction *f = data;
    struct StaticState *s = call->state;
    if (s->cursor >= s->count) {
        *finished = 1;
        return 0;
    }
    struct ArrowBuffer witness;
    ArrowBufferInit(&witness);
    int rc = 0;
    if (named(f, "late_materialization")) {
        struct VgiBatch values = {0};
        int vr = VgiFiltersColumnValues(call->filters, "row_id", &values, &call->rpc->error);
        if (vr && vr != ENODATA) rc = vr;
        struct VgiColumnBounds bounds = {0};
        if (!rc) rc = VgiFiltersColumnBounds(call->filters, "row_id", &bounds, &call->rpc->error);
        char range[96] = "none", lo[32] = "none", hi[32] = "none", text[160];
        if (bounds.has_min) snprintf(lo, sizeof(lo), "%lld", (long long)bounds.min);
        if (bounds.has_max) snprintf(hi, sizeof(hi), "%lld", (long long)bounds.max);
        if (bounds.has_min || bounds.has_max) snprintf(range, sizeof(range), "%s..%s", lo, hi);
        snprintf(text, sizeof(text), "rid:in=%lld;rng=%s", (long long)values.array.length, range);
        if (!rc) rc = ArrowBufferAppend(&witness, text, strlen(text));
        VgiBatchReset(&values);
    } else if (tt(f) || named(f, "filter_echo_table_scan"))
        rc = VgiFiltersFormat(call->filters, 0, &witness, &call->rpc->error);
    if (!rc) rc = VgiBatchInit(out, &s->schema, &call->rpc->error);
    int64_t end = s->count - s->cursor > s->batch ? s->cursor + s->batch : s->count;
    for (int64_t row = s->cursor; !rc && row < end; ++row) {
        for (int64_t col = 0; !rc && col < out->schema.n_children; ++col)
            rc = append(f, call, s, out->schema.children[col], out->array.children[col], row,
                        out->schema.children[col]->name,
                        (struct ArrowStringView){(const char *)witness.data, witness.size_bytes});
        if (!rc) rc = ArrowArrayFinishElement(&out->array);
    }
    if (!rc) rc = VgiBatchFinish(out, &call->rpc->error);
    s->cursor = end;
    ArrowBufferReset(&witness);
    return rc;
}
int ExampleStaticPlan(void *data, struct VgiFunctionCall *call, const char *method,
                      struct VgiBatch *result) {
    if (strcmp(method, "table_function_cardinality")) return ENODATA;
    int64_t n = rows(data, call);
    NANOARROW_RETURN_NOT_OK(VgiBatchSetInt(result, "estimate", n));
    return VgiBatchSetInt(result, "max", n);
}
