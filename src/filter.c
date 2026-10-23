#include "vgi/filter.h"

#include <errno.h>
#include <json-c/json.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

#include "filter_internal.h"
#include "vgi/context.h"
#include "vgi/rpc.h"

struct Predicate {
    char id[129];
    uint64_t revision;
    int required;
    struct json_object *node;
    struct VgiFilters *source;
};
struct VgiFilters {
    struct VgiBatch payload;
    struct ArrowSchema schema;
    struct json_object *document;
    struct VgiBatch *joins;
    int64_t n_joins;
    struct Predicate *items;
    size_t n_items;
    struct VgiFilters *sources;
    struct VgiFilters *next;
};
struct Value {
    int null, string, integer_kind;
    int64_t integer;
    uint64_t unsigned_integer;
    long double number;
    struct ArrowStringView text;
    const struct ArrowArrayView *view;
    const struct ArrowSchema *schema;
    int64_t index;
};
static struct json_object *get(struct json_object *node, const char *key) {
    struct json_object *out = NULL;
    if (node) json_object_object_get_ex(node, key, &out);
    return out;
}
static const char *str(struct json_object *node, const char *key) {
    const char *out = json_object_get_string(get(node, key));
    return out ? out : "";
}
static int error_set(struct ArrowError *error, const char *message) {
    ArrowErrorSet(error, "Filter Encoding v2: %s", message);
    return EINVAL;
}
static int from_array(const struct ArrowArrayView *view, const struct ArrowSchema *schema,
                      int64_t row, struct Value *out) {
    if (!view || !schema || row < 0 || row >= view->length) return EINVAL;
    out->view = view;
    out->schema = schema;
    out->index = row;
    out->null = ArrowArrayViewIsNull(view, row);
    if (out->null) return 0;
    if (view->dictionary)
        return from_array(view->dictionary, schema->dictionary,
                          ArrowArrayViewGetIntUnsafe(view, row), out);
    out->text = VgiValueString(view, row);
    if (out->text.data) {
        out->string = 1;
        return 0;
    }
    if (view->storage_type == NANOARROW_TYPE_INT64 || view->storage_type == NANOARROW_TYPE_UINT64 ||
        view->storage_type == NANOARROW_TYPE_INT32 || view->storage_type == NANOARROW_TYPE_UINT32 ||
        view->storage_type == NANOARROW_TYPE_INT16 || view->storage_type == NANOARROW_TYPE_UINT16 ||
        view->storage_type == NANOARROW_TYPE_INT8 || view->storage_type == NANOARROW_TYPE_UINT8) {
        if (view->storage_type == NANOARROW_TYPE_UINT64) {
            out->integer_kind = 2;
            out->unsigned_integer = ArrowArrayViewGetUIntUnsafe(view, row);
            out->number = out->unsigned_integer;
        } else {
            out->integer_kind = 1;
            out->integer = ArrowArrayViewGetIntUnsafe(view, row);
            out->number = out->integer;
        }
    } else if (view->storage_type != NANOARROW_TYPE_STRUCT &&
               view->storage_type != NANOARROW_TYPE_LIST &&
               view->storage_type != NANOARROW_TYPE_LARGE_LIST) {
        /* Nested values are read through their children, never as numbers. */
        double value;
        if (VgiValueDouble(view, schema, row, &value, NULL)) return EINVAL;
        out->number = value;
    }
    return 0;
}
static int compare(struct Value a, struct Value b) {
    if (a.integer_kind && b.integer_kind) {
        if (a.integer_kind == 1 && b.integer_kind == 1)
            return (a.integer > b.integer) - (a.integer < b.integer);
        if (a.integer_kind == 1 && a.integer < 0) return -1;
        if (b.integer_kind == 1 && b.integer < 0) return 1;
        uint64_t x = a.integer_kind == 2 ? a.unsigned_integer : (uint64_t)a.integer,
                 y = b.integer_kind == 2 ? b.unsigned_integer : (uint64_t)b.integer;
        return (x > y) - (x < y);
    }
    if (a.view && b.view && a.schema && b.schema && !strncmp(a.schema->format, "d:", 2) &&
        !strcmp(a.schema->format, b.schema->format)) {
        struct ArrowSchemaView type;
        struct ArrowDecimal left, right;
        if (!ArrowSchemaViewInit(&type, a.schema, NULL)) {
            ArrowDecimalInit(&left, type.decimal_bitwidth, type.decimal_precision,
                             type.decimal_scale);
            ArrowDecimalInit(&right, type.decimal_bitwidth, type.decimal_precision,
                             type.decimal_scale);
            ArrowArrayViewGetDecimalUnsafe(a.view, a.index, &left);
            ArrowArrayViewGetDecimalUnsafe(b.view, b.index, &right);
            if (ArrowDecimalSign(&left) != ArrowDecimalSign(&right))
                return ArrowDecimalSign(&left) < ArrowDecimalSign(&right) ? -1 : 1;
            if (!left.n_words) {
                int64_t x = ArrowDecimalGetIntUnsafe(&left), y = ArrowDecimalGetIntUnsafe(&right);
                return (x > y) - (x < y);
            }
            for (int n = 0; n < left.n_words; ++n) {
                int i = left.high_word_index + (left.high_word_index ? -n : n);
                if (left.words[i] != right.words[i]) return left.words[i] < right.words[i] ? -1 : 1;
            }
            return 0;
        }
    }
    if (a.string && b.string) {
        int64_t n = a.text.size_bytes < b.text.size_bytes ? a.text.size_bytes : b.text.size_bytes;
        int cmp = memcmp(a.text.data, b.text.data, (size_t)n);
        return cmp ? cmp
                   : (a.text.size_bytes > b.text.size_bytes) -
                         (a.text.size_bytes < b.text.size_bytes);
    }
    if (isnan(a.number)) return isnan(b.number) ? 0 : 1;
    if (isnan(b.number)) return -1;
    return (a.number > b.number) - (a.number < b.number);
}
static int integer_fit(struct Value *v, const struct ArrowSchema *type) {
    char format = type->format[0];
    int bits = format == 'c' || format == 'C'   ? 8
               : format == 's' || format == 'S' ? 16
               : format == 'i' || format == 'I' ? 32
                                                : 64;
    int unsign = format == 'C' || format == 'S' || format == 'I' || format == 'L';
    if (unsign) {
        if (v->integer_kind == 1 && v->integer < 0) return ERANGE;
        uint64_t x = v->integer_kind == 2 ? v->unsigned_integer : (uint64_t)v->integer;
        if (bits < 64 && x > (((uint64_t)1 << bits) - 1)) return ERANGE;
        v->integer_kind = 2;
        v->unsigned_integer = x;
        v->number = x;
    } else {
        uint64_t max = bits == 64 ? INT64_MAX : ((uint64_t)1 << (bits - 1)) - 1;
        int64_t min = bits == 64 ? INT64_MIN : -(int64_t)(max + 1);
        if (v->integer_kind == 2 && v->unsigned_integer > max) return ERANGE;
        int64_t x = v->integer_kind == 1 ? v->integer : (int64_t)v->unsigned_integer;
        if (x < min || (x >= 0 && (uint64_t)x > max)) return ERANGE;
        v->integer_kind = 1;
        v->integer = x;
        v->number = x;
    }
    v->schema = type;
    v->view = NULL;
    return 0;
}
static int arithmetic(struct Value a, struct Value b, const char *op, struct Value *out) {
    out->schema = a.schema;
    if (a.integer_kind && b.integer_kind) {
        int overflow = 0;
        out->integer_kind = a.integer_kind;
        if (a.integer_kind == 2) {
            if (!strcmp(op, "add"))
                overflow = __builtin_add_overflow(a.unsigned_integer, b.unsigned_integer,
                                                  &out->unsigned_integer);
            else if (!strcmp(op, "subtract"))
                overflow = __builtin_sub_overflow(a.unsigned_integer, b.unsigned_integer,
                                                  &out->unsigned_integer);
            else if (!strcmp(op, "multiply"))
                overflow = __builtin_mul_overflow(a.unsigned_integer, b.unsigned_integer,
                                                  &out->unsigned_integer);
            else
                return ENOTSUP;
        } else {
            if (!strcmp(op, "add"))
                overflow = __builtin_add_overflow(a.integer, b.integer, &out->integer);
            else if (!strcmp(op, "subtract"))
                overflow = __builtin_sub_overflow(a.integer, b.integer, &out->integer);
            else if (!strcmp(op, "multiply"))
                overflow = __builtin_mul_overflow(a.integer, b.integer, &out->integer);
            else
                return ENOTSUP;
        }
        return overflow ? ERANGE : integer_fit(out, a.schema);
    }
    if (a.schema && !strncmp(a.schema->format, "d:", 2)) return ENOTSUP;
    if (!strcmp(op, "add"))
        out->number = a.number + b.number;
    else if (!strcmp(op, "subtract"))
        out->number = a.number - b.number;
    else if (!strcmp(op, "multiply"))
        out->number = a.number * b.number;
    else
        return ENOTSUP;
    if (a.schema && !strcmp(a.schema->format, "f")) out->number = (float)out->number;
    return 0;
}
static int evaluate(const struct VgiFilters *f, struct json_object *node,
                    const struct VgiBatch *input, int64_t row, int depth, struct Value *out) {
    memset(out, 0, sizeof(*out));
    if (!node || depth > 64) return EINVAL;
    const char *kind = str(node, "node");
    if (!strcmp(kind, "column_ref")) {
        int64_t id = json_object_get_int64(get(node, "column_index"));
        const char *name = str(node, "column_name");
        if (id < 0 || id >= f->schema.n_children || strcmp(f->schema.children[id]->name, name))
            return EINVAL;
        id = VgiFieldIndex(&input->schema, name);
        if (id < 0) return EINVAL;
        return from_array(input->view.children[id], input->schema.children[id], row, out);
    }
    if (!strcmp(kind, "literal")) {
        char name[64];
        snprintf(name, sizeof(name), "value_%lld",
                 (long long)json_object_get_int64(get(node, "value_ref")));
        int64_t id = VgiFieldIndex(&f->payload.schema, name);
        return id < 0 ? EINVAL
                      : from_array(f->payload.view.children[id], f->payload.schema.children[id], 0,
                                   out);
    }
    struct Value a = {0}, b = {0};
    if (!strcmp(kind, "and") || !strcmp(kind, "or")) {
        int is_and = !strcmp(kind, "and"), has_null = 0;
        struct json_object *children = get(node, "children");
        if (!json_object_is_type(children, json_type_array)) return EINVAL;
        for (size_t i = 0; i < json_object_array_length(children); ++i) {
            NANOARROW_RETURN_NOT_OK(
                evaluate(f, json_object_array_get_idx(children, i), input, row, depth + 1, &a));
            has_null |= a.null;
            if (!a.null && (!!a.number != is_and)) {
                out->number = !is_and;
                return 0;
            }
        }
        out->number = is_and;
        out->null = has_null;
        return 0;
    }
    if (!strcmp(kind, "comparison") || !strcmp(kind, "arithmetic")) {
        NANOARROW_RETURN_NOT_OK(evaluate(f, get(node, "left"), input, row, depth + 1, &a));
        NANOARROW_RETURN_NOT_OK(evaluate(f, get(node, "right"), input, row, depth + 1, &b));
        const char *op = str(node, "op");
        if (!strcmp(kind, "comparison") &&
            (!strcmp(op, "distinct_from") || !strcmp(op, "not_distinct_from"))) {
            int equal = a.null || b.null ? a.null && b.null : compare(a, b) == 0;
            out->number = !strcmp(op, "distinct_from") ? !equal : equal;
            return 0;
        }
        if (a.null || b.null) {
            out->null = 1;
            return 0;
        }
        if (!strcmp(kind, "comparison")) {
            int cmp = compare(a, b);
            if (!strcmp(op, "eq"))
                out->number = cmp == 0;
            else if (!strcmp(op, "ne"))
                out->number = cmp != 0;
            else if (!strcmp(op, "lt"))
                out->number = cmp < 0;
            else if (!strcmp(op, "le"))
                out->number = cmp <= 0;
            else if (!strcmp(op, "gt"))
                out->number = cmp > 0;
            else if (!strcmp(op, "ge"))
                out->number = cmp >= 0;
            else
                return ENOTSUP;
        } else
            return arithmetic(a, b, op, out);
        return 0;
    }
    if (!strcmp(kind, "call")) {
        const char *name = json_object_is_type(get(node, "function"), json_type_string)
                               ? json_object_get_string(get(node, "function"))
                               : str(get(node, "function"), "name");
        struct json_object *args = get(node, "arguments");
        if (!json_object_is_type(args, json_type_array) || json_object_array_length(args) < 1)
            return EINVAL;
        NANOARROW_RETURN_NOT_OK(
            evaluate(f, json_object_array_get_idx(args, 0), input, row, depth + 1, &a));
        if (json_object_array_length(args) > 1)
            NANOARROW_RETURN_NOT_OK(
                evaluate(f, json_object_array_get_idx(args, 1), input, row, depth + 1, &b));
        if (a.null || b.null) {
            out->null = 1;
            return 0;
        }
        if (!strcmp(name, "list_contains") && a.view &&
            (a.view->storage_type == NANOARROW_TYPE_LIST ||
             a.view->storage_type == NANOARROW_TYPE_LARGE_LIST)) {
            int64_t start = ArrowArrayViewListChildOffset(a.view, a.index),
                    end = ArrowArrayViewListChildOffset(a.view, a.index + 1);
            for (int64_t i = start; i < end; ++i) {
                struct Value item = {0};
                NANOARROW_RETURN_NOT_OK(
                    from_array(a.view->children[0], a.schema->children[0], i, &item));
                if (!item.null && !compare(item, b)) {
                    out->number = 1;
                    break;
                }
            }
            return 0;
        }
        if (!strcmp(name, "abs")) {
            out->number = fabsl(a.number);
            return 0;
        }
        if (a.string && b.string) {
            int64_t x = a.text.size_bytes, y = b.text.size_bytes;
            if (!strcmp(name, "starts_with"))
                out->number = x >= y && !memcmp(a.text.data, b.text.data, (size_t)y);
            else if (!strcmp(name, "ends_with"))
                out->number = x >= y && !memcmp(a.text.data + x - y, b.text.data, (size_t)y);
            else if (!strcmp(name, "contains")) {
                for (int64_t i = 0; i <= x - y; ++i)
                    if (!memcmp(a.text.data + i, b.text.data, (size_t)y)) {
                        out->number = 1;
                        break;
                    }
            } else
                return ENOTSUP;
            return 0;
        }
        return ENOTSUP;
    }
    NANOARROW_RETURN_NOT_OK(evaluate(f, get(node, "expression"), input, row, depth + 1, &a));
    if (!strcmp(kind, "field_ref")) {
        if (a.null) {
            out->null = 1;
            return 0;
        }
        int64_t id = json_object_get_int64(get(node, "field_index"));
        if (!a.view || a.view->storage_type != NANOARROW_TYPE_STRUCT || id < 0 ||
            id >= a.view->n_children ||
            strcmp(a.schema->children[id]->name, str(node, "field_name")))
            return EINVAL;
        return from_array(a.view->children[id], a.schema->children[id], a.index + a.view->offset,
                          out);
    }
    if (!strcmp(kind, "is_null")) {
        out->number = a.null != json_object_get_boolean(get(node, "negated"));
        return 0;
    }
    if (!strcmp(kind, "not")) {
        out->null = a.null;
        out->number = !a.number;
        return 0;
    }
    if (!strcmp(kind, "cast")) {
        char name[64];
        snprintf(name, sizeof(name), "type_%llu",
                 (unsigned long long)json_object_get_uint64(get(node, "type_ref")));
        int64_t id = VgiFieldIndex(&f->payload.schema, name);
        if (id < 0) return EINVAL;
        const struct ArrowSchema *target = f->payload.schema.children[id];
        while (target->dictionary) target = target->dictionary;
        *out = a;
        if (a.null) return 0;
        if (a.integer_kind) return integer_fit(out, target);
        if (a.schema && (!strcmp(a.schema->format, target->format) ||
                         (!strcmp(a.schema->format, "f") && !strcmp(target->format, "g")))) {
            out->schema = target;
            return 0;
        }
        return ENOTSUP;
    }
    if (!strcmp(kind, "negate")) {
        *out = a;
        if (a.null) return 0;
        if (a.integer_kind == 2) {
            if (a.unsigned_integer) return ERANGE;
            return 0;
        }
        if (a.integer_kind == 1) {
            if (a.integer == INT64_MIN) return ERANGE;
            out->integer = -a.integer;
            return integer_fit(out, a.schema);
        }
        if (a.schema && !strncmp(a.schema->format, "d:", 2)) return ENOTSUP;
        out->number = -a.number;
        return 0;
    }
    if (!strcmp(kind, "in")) {
        if (a.null) {
            out->null = 1;
            return 0;
        }
        struct json_object *set = get(node, "set");
        const struct ArrowArrayView *view = NULL;
        const struct ArrowSchema *schema = NULL;
        int64_t start = 0, end = 0;
        if (!strcmp(str(set, "kind"), "literal")) {
            char name[64];
            snprintf(name, sizeof(name), "value_%lld",
                     (long long)json_object_get_int64(get(set, "value_ref")));
            int64_t id = VgiFieldIndex(&f->payload.schema, name);
            if (id < 0) return EINVAL;
            view = f->payload.view.children[id];
            schema = f->payload.schema.children[id];
            if (view->storage_type != NANOARROW_TYPE_LIST &&
                view->storage_type != NANOARROW_TYPE_LARGE_LIST)
                return EINVAL;
            start = ArrowArrayViewListChildOffset(view, 0);
            end = ArrowArrayViewListChildOffset(view, 1);
            view = view->children[0];
            schema = schema->children[0];
        } else if (!strcmp(str(set, "kind"), "external")) {
            int64_t bi = json_object_get_int64(get(set, "batch_index")),
                    ci = json_object_get_int64(get(set, "column_index"));
            if (bi < 0 || bi >= f->n_joins || ci < 0 || ci >= f->joins[bi].schema.n_children)
                return EINVAL;
            view = f->joins[bi].view.children[ci];
            schema = f->joins[bi].schema.children[ci];
            end = view->length;
            if (strcmp(schema->name, str(set, "column_name"))) return EINVAL;
        } else
            return ENOTSUP;
        int has_null = 0;
        for (int64_t i = start; i < end; ++i) {
            memset(&b, 0, sizeof(b));
            NANOARROW_RETURN_NOT_OK(from_array(view, schema, i, &b));
            if (b.null)
                has_null = 1;
            else if (!compare(a, b)) {
                out->number = !json_object_get_boolean(get(node, "negated"));
                return 0;
            }
        }
        out->null = has_null;
        out->number = json_object_get_boolean(get(node, "negated"));
        return 0;
    }
    return ENOTSUP;
}
void VgiFiltersFree(struct VgiFilters *f) {
    if (!f) return;
    VgiBatchReset(&f->payload);
    if (f->schema.release) ArrowSchemaRelease(&f->schema);
    if (f->document) json_object_put(f->document);
    for (int64_t i = 0; i < f->n_joins; ++i) VgiBatchReset(&f->joins[i]);
    while (f->sources) {
        struct VgiFilters *source = f->sources;
        f->sources = source->next;
        source->next = NULL;
        VgiFiltersFree(source);
    }
    free(f->items);
    free(f->joins);
    free(f);
}
static int metadata_is(const struct ArrowSchema *schema, const char *key, const char *text) {
    struct ArrowStringView value = VgiMetadataGet(schema->metadata, key);
    return value.size_bytes == (int64_t)strlen(text) &&
           !memcmp(value.data, text, (size_t)value.size_bytes);
}
/* JSON-C retains only the last spelling of an object key. Walk the original
 * token stream before accepting it so duplicate (including escaped) keys fail. */
struct JsonScan {
    const char *p, *end;
    size_t nodes;
};
static void json_space(struct JsonScan *s) {
    while (s->p < s->end && strchr(" \t\r\n", *s->p)) ++s->p;
}
static int json_string(struct JsonScan *s, const char **start, size_t *length) {
    *start = s->p;
    if (s->p == s->end || *s->p++ != '"') return EINVAL;
    while (s->p < s->end) {
        char c = *s->p++;
        if (c == '"') {
            *length = (size_t)(s->p - *start);
            return 0;
        }
        if (c == '\\') {
            if (s->p == s->end) return EINVAL;
            ++s->p;
        }
    }
    return EINVAL;
}
static int json_unique(struct JsonScan *s, int depth) {
    json_space(s);
    if (depth > 70 || ++s->nodes > 50000 || s->p == s->end) return EINVAL;
    char c = *s->p;
    if (c == '"') {
        const char *start;
        size_t length;
        return json_string(s, &start, &length);
    }
    if (c != '{' && c != '[') {
        const char *start = s->p;
        while (s->p < s->end && !strchr(" \t\r\n,]}", *s->p)) ++s->p;
        return s->p == start ? EINVAL : 0;
    }
    ++s->p;
    int object = c == '{';
    char close = object ? '}' : ']';
    struct json_object *keys = object ? json_object_new_object() : NULL;
    int rc = 0;
    json_space(s);
    while (s->p < s->end && *s->p != close) {
        if (object) {
            const char *start;
            size_t length;
            rc = json_string(s, &start, &length);
            if (rc) break;
            struct json_tokener *parser = json_tokener_new();
            if (!parser) {
                rc = ENOMEM;
                break;
            }
            struct json_object *key = json_tokener_parse_ex(parser, start, (int)length);
            json_tokener_free(parser);
            if (!key) {
                rc = EINVAL;
                break;
            }
            const char *name = json_object_get_string(key);
            struct json_object *existing = NULL;
            if (strlen(name) != (size_t)json_object_get_string_len(key) ||
                json_object_object_get_ex(keys, name, &existing))
                rc = EINVAL;
            else
                json_object_object_add(keys, name, json_object_new_boolean(1));
            json_object_put(key);
            if (rc) break;
            json_space(s);
            if (s->p == s->end || *s->p++ != ':') {
                rc = EINVAL;
                break;
            }
        }
        rc = json_unique(s, depth + 1);
        if (rc) break;
        json_space(s);
        if (s->p < s->end && *s->p == ',') {
            ++s->p;
            json_space(s);
        } else
            break;
    }
    if (!rc && (s->p == s->end || *s->p++ != close)) rc = EINVAL;
    if (keys) json_object_put(keys);
    return rc;
}
static const struct ArrowSchema filter_boolean = {.format = "b"};
static const struct ArrowSchema *logical(const struct ArrowSchema *s) {
    while (s && s->dictionary) s = s->dictionary;
    return s;
}
static int same_type(const struct ArrowSchema *a, const struct ArrowSchema *b) {
    a = logical(a);
    b = logical(b);
    if (!a || !b || strcmp(a->format, b->format) || a->n_children != b->n_children) return 0;
    for (int64_t i = 0; i < a->n_children; ++i)
        if (!same_type(a->children[i], b->children[i])) return 0;
    return 1;
}
static int integer_type(const struct ArrowSchema *s) {
    s = logical(s);
    return s && s->format && s->format[0] && !s->format[1] && strchr("cCsSiIlL", *s->format);
}
static int numeric_type(const struct ArrowSchema *s) {
    s = logical(s);
    return integer_type(s) || (s && (!strcmp(s->format, "f") || !strcmp(s->format, "g") ||
                                     !strcmp(s->format, "e") || !strncmp(s->format, "d:", 2)));
}
static int json_uint(struct json_object *v) {
    return json_object_is_type(v, json_type_int) && json_object_get_int64(v) >= 0;
}
static int keys_are(struct json_object *n, const char *names) {
    if (!json_object_is_type(n, json_type_object)) return 0;
    size_t count = 0;
    const char *p = names;
    while (*p) {
        const char *end = strchr(p, ',');
        if (!end) end = p + strlen(p);
        char key[64];
        size_t size = (size_t)(end - p);
        if (size >= sizeof(key)) return 0;
        memcpy(key, p, size);
        key[size] = 0;
        struct json_object *v = NULL;
        if (!json_object_object_get_ex(n, key, &v)) return 0;
        ++count;
        p = *end ? end + 1 : end;
    }
    return json_object_object_length(n) == (int)count;
}
static const struct ArrowSchema *payload_type(const struct VgiFilters *f, struct json_object *ref,
                                              const char *prefix) {
    if (!json_uint(ref)) return NULL;
    char field[64];
    snprintf(field, sizeof(field), "%s_%llu", prefix,
             (unsigned long long)json_object_get_uint64(ref));
    int64_t id = VgiFieldIndex(&f->payload.schema, field);
    return id < 0 ? NULL : logical(f->payload.schema.children[id]);
}
static int valid_type(const struct ArrowSchema *s) {
    if (!s) return 0;
    if (VgiMetadataGet(s->metadata, "ARROW:extension:name").size_bytes) return 0;
    for (int64_t i = 0; i < s->n_children; ++i)
        if (!valid_type(s->children[i])) return 0;
    return !s->dictionary || valid_type(s->dictionary);
}
static int validate_expression(const struct VgiFilters *f, struct json_object *node, int depth,
                               size_t *nodes, int root, int required,
                               const struct ArrowSchema **out) {
    if (depth > 64 || ++*nodes > 10000 || !json_object_is_type(node, json_type_object) ||
        !json_object_is_type(get(node, "node"), json_type_string))
        return EINVAL;
    const char *kind = str(node, "node");
    const struct ArrowSchema *a = NULL, *b = NULL;
    *out = NULL;
    if (!strcmp(kind, "column_ref")) {
        if (!keys_are(node, "node,column_index,column_name") ||
            !json_uint(get(node, "column_index")) ||
            !json_object_is_type(get(node, "column_name"), json_type_string))
            return EINVAL;
        uint64_t i = json_object_get_uint64(get(node, "column_index"));
        if (i >= (uint64_t)f->schema.n_children ||
            strcmp(f->schema.children[i]->name, str(node, "column_name")))
            return EINVAL;
        *out = logical(f->schema.children[i]);
    } else if (!strcmp(kind, "literal")) {
        if (!keys_are(node, "node,value_ref")) return EINVAL;
        *out = payload_type(f, get(node, "value_ref"), "value");
    } else if (!strcmp(kind, "and") || !strcmp(kind, "or")) {
        struct json_object *children = get(node, "children");
        if (!keys_are(node, "node,children") || !json_object_is_type(children, json_type_array) ||
            json_object_array_length(children) < 2)
            return EINVAL;
        for (size_t i = 0; i < json_object_array_length(children); ++i) {
            NANOARROW_RETURN_NOT_OK(validate_expression(f, json_object_array_get_idx(children, i),
                                                        depth + 1, nodes, 0, required, &a));
            if (!same_type(a, &filter_boolean)) return EINVAL;
        }
        *out = &filter_boolean;
    } else if (!strcmp(kind, "comparison") || !strcmp(kind, "arithmetic")) {
        if (!keys_are(node, "node,op,left,right")) return EINVAL;
        NANOARROW_RETURN_NOT_OK(
            validate_expression(f, get(node, "left"), depth + 1, nodes, 0, required, &a));
        NANOARROW_RETURN_NOT_OK(
            validate_expression(f, get(node, "right"), depth + 1, nodes, 0, required, &b));
        if (!same_type(a, b)) return EINVAL;
        const char *op = str(node, "op");
        if (!strcmp(kind, "comparison")) {
            const char *ops[] = {
                "eq", "ne", "lt", "le", "gt", "ge", "distinct_from", "not_distinct_from"};
            int found = 0;
            for (int i = 0; i < 8; ++i) found |= !strcmp(op, ops[i]);
            if (!found || a->format[0] == '+') return EINVAL;
            *out = &filter_boolean;
        } else {
            if (!numeric_type(a) ||
                (strcmp(op, "add") && strcmp(op, "subtract") && strcmp(op, "multiply")))
                return EINVAL;
            *out = a;
        }
    } else if (!strcmp(kind, "call")) {
        if (!(keys_are(node, "node,function,arguments") ||
              keys_are(node, "node,function,arguments,options")))
            return EINVAL;
        struct json_object *args = get(node, "arguments"), *options = get(node, "options");
        if (!json_object_is_type(get(node, "function"), json_type_string) ||
            !json_object_is_type(args, json_type_array) || json_object_array_length(args) != 2 ||
            (options && (!json_object_is_type(options, json_type_object) ||
                         json_object_object_length(options))))
            return EINVAL;
        NANOARROW_RETURN_NOT_OK(validate_expression(f, json_object_array_get_idx(args, 0),
                                                    depth + 1, nodes, 0, required, &a));
        NANOARROW_RETURN_NOT_OK(validate_expression(f, json_object_array_get_idx(args, 1),
                                                    depth + 1, nodes, 0, required, &b));
        const char *name = str(node, "function");
        if (!strcmp(name, "list_contains")) {
            if (strcmp(a->format, "+l") || !same_type(a->children[0], b)) return EINVAL;
        } else if ((strcmp(name, "starts_with") && strcmp(name, "ends_with") &&
                    strcmp(name, "contains")) ||
                   strcmp(a->format, "u") || strcmp(b->format, "u"))
            return EINVAL;
        *out = &filter_boolean;
    } else if (!strcmp(kind, "runtime_filter")) {
        if (!root || required || !keys_are(node, "node,algorithm,input,artifact_ref,null_handling"))
            return EINVAL;
        struct json_object *alg = get(node, "algorithm");
        if (!keys_are(alg, "namespace,name,version") ||
            strcmp(str(alg, "namespace"), "duckdb.runtime_filter") ||
            (strcmp(str(alg, "name"), "bloom") && strcmp(str(alg, "name"), "prefix_range")) ||
            !json_uint(get(alg, "version")) || json_object_get_uint64(get(alg, "version")) != 1 ||
            (strcmp(str(node, "null_handling"), "pass") &&
             strcmp(str(node, "null_handling"), "reject")) ||
            !payload_type(f, get(node, "artifact_ref"), "artifact"))
            return EINVAL;
        NANOARROW_RETURN_NOT_OK(
            validate_expression(f, get(node, "input"), depth + 1, nodes, 0, required, &a));
        *out = &filter_boolean;
    } else {
        NANOARROW_RETURN_NOT_OK(
            validate_expression(f, get(node, "expression"), depth + 1, nodes, 0, required, &a));
        if (!strcmp(kind, "not")) {
            if (!keys_are(node, "node,expression") || !same_type(a, &filter_boolean)) return EINVAL;
            *out = &filter_boolean;
        } else if (!strcmp(kind, "is_null")) {
            if (!keys_are(node, "node,expression,negated") ||
                !json_object_is_type(get(node, "negated"), json_type_boolean))
                return EINVAL;
            *out = &filter_boolean;
        } else if (!strcmp(kind, "negate")) {
            if (!keys_are(node, "node,expression") || !numeric_type(a)) return EINVAL;
            *out = a;
        } else if (!strcmp(kind, "field_ref")) {
            if (!keys_are(node, "node,expression,field_index,field_name") ||
                strcmp(a->format, "+s") || !json_uint(get(node, "field_index")))
                return EINVAL;
            uint64_t i = json_object_get_uint64(get(node, "field_index"));
            if (i >= (uint64_t)a->n_children ||
                strcmp(a->children[i]->name, str(node, "field_name")))
                return EINVAL;
            *out = logical(a->children[i]);
        } else if (!strcmp(kind, "cast")) {
            if (!keys_are(node, "node,expression,type_ref")) return EINVAL;
            b = payload_type(f, get(node, "type_ref"), "type");
            if (!b || (!same_type(a, b) && !(integer_type(a) && integer_type(b)) &&
                       (strcmp(a->format, "f") || strcmp(b->format, "g"))))
                return EINVAL;
            *out = b;
        } else if (!strcmp(kind, "in")) {
            if (!keys_are(node, "node,expression,set,negated") ||
                !json_object_is_type(get(node, "negated"), json_type_boolean))
                return EINVAL;
            struct json_object *set = get(node, "set");
            if (!strcmp(str(set, "kind"), "literal")) {
                if (!keys_are(set, "kind,value_ref")) return EINVAL;
                b = payload_type(f, get(set, "value_ref"), "value");
                if (!b || (strcmp(b->format, "+l") && strcmp(b->format, "+L"))) return EINVAL;
                b = logical(b->children[0]);
            } else if (!strcmp(str(set, "kind"), "external")) {
                if (!keys_are(set, "kind,batch_index,column_index,column_name") ||
                    !json_uint(get(set, "batch_index")) || !json_uint(get(set, "column_index")))
                    return EINVAL;
                uint64_t bi = json_object_get_uint64(get(set, "batch_index")),
                         ci = json_object_get_uint64(get(set, "column_index"));
                if (bi >= (uint64_t)f->n_joins || ci >= (uint64_t)f->joins[bi].schema.n_children)
                    return EINVAL;
                b = logical(f->joins[bi].schema.children[ci]);
                if (strcmp(b->name, str(set, "column_name"))) return EINVAL;
            } else
                return EINVAL;
            if (!same_type(a, b)) return EINVAL;
            *out = &filter_boolean;
        } else
            return EINVAL;
    }
    return valid_type(*out) ? 0 : EINVAL;
}
static int validate_predicate(const struct VgiFilters *f, const struct Predicate *p,
                              struct ArrowError *error) {
    size_t nodes = 0;
    const struct ArrowSchema *type = NULL;
    int rc = validate_expression(f, get(p->node, "expression"), 0, &nodes, 1, p->required, &type);
    if (rc || !same_type(type, &filter_boolean))
        return error_set(error, "invalid predicate expression or incompatible types");
    return 0;
}
static int document_load(struct VgiFilters *f, struct ArrowBufferView bytes, const char *kind,
                         struct ArrowError *error) {
    if (bytes.size_bytes <= 0 || bytes.size_bytes > 16 * 1024 * 1024)
        return error_set(error, "invalid payload size");
    int rc = VgiBatchDecode(&f->payload, bytes, error);
    if (rc) return rc;
    const struct ArrowSchema *schema = &f->payload.schema;
    if (f->payload.array.length != 1 || !schema->n_children ||
        strcmp(schema->children[0]->name, "filter_spec") ||
        strcmp(schema->children[0]->format, "u") ||
        (schema->children[0]->flags & ARROW_FLAG_NULLABLE))
        return error_set(error, "first field must be filter_spec: utf8 not null");
    struct ArrowStringView spec = VgiBatchString(&f->payload, "filter_spec");
    if (!spec.data || spec.size_bytes > 1024 * 1024)
        return error_set(error, "invalid filter JSON size");
    for (int64_t i = 1; i < schema->n_children; ++i) {
        const char *name = schema->children[i]->name,
                   *suffix = !strncmp(name, "value_", 6)      ? name + 6
                             : !strncmp(name, "type_", 5)     ? name + 5
                             : !strncmp(name, "artifact_", 9) ? name + 9
                                                              : NULL;
        if (!suffix || !*suffix || (suffix[0] == '0' && suffix[1]))
            return error_set(error, "noncanonical payload field");
        for (const char *c = suffix; *c; ++c)
            if (*c < '0' || *c > '9') return error_set(error, "noncanonical payload field");
        for (int64_t j = 0; j < i; ++j)
            if (!strcmp(name, schema->children[j]->name))
                return error_set(error, "duplicate payload field");
        if (!strncmp(name, "type_", 5) && !ArrowArrayViewIsNull(f->payload.view.children[i], 0))
            return error_set(error, "type payload must contain NULL");
    }
    if (!metadata_is(schema, "vgi_filter_encoding", "vgi.filters.v2") ||
        !metadata_is(schema, "vgi_filter_version", "2") ||
        !metadata_is(schema, "vgi_evaluation_context", "vgi.none.v1"))
        return error_set(error, "unsupported filter encoding or evaluation context");
    const char *forbidden[] = {"vgi_time_zone",         "vgi_calendar",
                               "vgi_default_collation", "vgi_ieee_floating_point_ops",
                               "vgi_integer_division",  "vgi_context_provider_fingerprint"};
    for (size_t i = 0; i < sizeof(forbidden) / sizeof(*forbidden); ++i)
        if (VgiMetadataGet(schema->metadata, forbidden[i]).data)
            return error_set(error, "vgi.none.v1 forbids session-context metadata");
    struct JsonScan scan = {spec.data, spec.data + spec.size_bytes, 0};
    if (json_unique(&scan, 0)) return error_set(error, "duplicate JSON key or invalid JSON");
    json_space(&scan);
    if (scan.p != scan.end) return error_set(error, "trailing JSON data");
    struct json_tokener *parser = json_tokener_new_ex(70);
    if (!parser) return ENOMEM;
    json_tokener_set_flags(parser, JSON_TOKENER_STRICT);
    f->document = json_tokener_parse_ex(parser, spec.data, (int)spec.size_bytes);
    if (json_tokener_get_error(parser) != json_tokener_success) rc = EINVAL;
    json_tokener_free(parser);
    const char *member = !strcmp(kind, "snapshot") ? "predicates" : "updates";
    if (rc || !json_object_is_type(f->document, json_type_object) ||
        json_object_object_length(f->document) != 4 ||
        strcmp(str(f->document, "encoding"), "vgi.filters.v2") ||
        strcmp(str(f->document, "semantics"), "vgi.duckdb.standard.v1") ||
        strcmp(str(f->document, "kind"), kind) ||
        !json_object_is_type(get(f->document, member), json_type_array))
        return error_set(error, "invalid filter document header");
    if (json_object_array_length(get(f->document, member)) > 1024)
        return error_set(error, "predicate count exceeds 1024");
    return 0;
}
static int predicate_header(struct json_object *node, int delta, struct Predicate *out,
                            struct ArrowError *error) {
    const char *id = str(node, "id"), *mode = str(node, "mode"), *source = str(node, "source");
    struct json_object *revision = get(node, "revision");
    if (!*id || strlen(id) > 128 || !json_object_is_type(revision, json_type_int) ||
        json_object_get_int64(revision) < 0)
        return error_set(error, "invalid predicate ID or revision");
    if (strcmp(mode, "required") && strcmp(mode, "advisory"))
        return error_set(error, "invalid predicate mode");
    if (delta && strcmp(mode, "advisory"))
        return error_set(error, "delta upserts must be advisory");
    if (strcmp(source, "query") && strcmp(source, "join") && strcmp(source, "top_n") &&
        strcmp(source, "split_refinement") && strcmp(source, "other"))
        return error_set(error, "invalid predicate source");
    if (!get(node, "expression") || json_object_object_length(node) != (delta ? 6 : 5))
        return error_set(error, "invalid predicate properties");
    strcpy(out->id, id);
    out->revision = json_object_get_uint64(revision);
    out->required = !strcmp(mode, "required");
    out->node = node;
    return 0;
}
static struct VgiFilters predicate_context(const struct VgiFilters *f, const struct Predicate *p) {
    struct VgiFilters context = *f;
    if (p->source) context.payload = p->source->payload;
    return context;
}
int VgiFiltersParse(struct ArrowBufferView bytes, const struct ArrowSchema *schema,
                    const struct VgiBatch *init, struct VgiFilters **out,
                    struct ArrowError *error) {
    struct VgiFilters *f = calloc(1, sizeof(*f));
    if (!f) return ENOMEM;
    int rc = ArrowSchemaDeepCopy(schema, &f->schema);
    if (!rc && bytes.size_bytes) rc = document_load(f, bytes, "snapshot", error);
    const struct ArrowArrayView *joins = VgiBatchGet(init, "join_keys");
    if (!rc && joins && !ArrowArrayViewIsNull(joins, 0)) {
        int64_t start = ArrowArrayViewListChildOffset(joins, 0),
                end = ArrowArrayViewListChildOffset(joins, 1);
        f->n_joins = end - start;
        f->joins = calloc((size_t)f->n_joins, sizeof(*f->joins));
        if (f->n_joins && !f->joins) rc = ENOMEM;
        for (int64_t i = start; !rc && i < end; ++i) {
            struct ArrowStringView value = VgiValueString(joins->children[0], i);
            rc = VgiBatchDecode(&f->joins[i - start],
                                (struct ArrowBufferView){{value.data}, value.size_bytes}, error);
        }
    }
    struct json_object *predicates = get(f->document, "predicates");
    size_t count = predicates ? json_object_array_length(predicates) : 0;
    if (!rc && count) {
        f->items = calloc(count, sizeof(*f->items));
        if (!f->items) rc = ENOMEM;
    }
    for (size_t i = 0; !rc && i < count; ++i) {
        rc = predicate_header(json_object_array_get_idx(predicates, i), 0, &f->items[i], error);
        if (!rc) rc = validate_predicate(f, &f->items[i], error);
        if (!rc && f->items[i].revision) rc = error_set(error, "snapshot revisions must be zero");
        for (size_t j = 0; !rc && j < i; ++j)
            if (!strcmp(f->items[i].id, f->items[j].id))
                rc = error_set(error, "duplicate predicate ID");
    }
    if (rc) {
        VgiFiltersFree(f);
        return rc;
    }
    f->n_items = count;
    *out = f;
    return 0;
}
int VgiFiltersApplyDelta(struct VgiFilters *f, struct ArrowBufferView bytes,
                         struct ArrowError *error) {
    if (!f) return EINVAL;
    struct VgiFilters *source = calloc(1, sizeof(*source));
    if (!source) return ENOMEM;
    int rc = document_load(source, bytes, "delta", error);
    struct json_object *updates = get(source->document, "updates");
    size_t count = updates ? json_object_array_length(updates) : 0;
    struct Predicate *items = rc ? NULL : calloc(f->n_items + count, sizeof(*items));
    if (!rc && f->n_items + count && !items) rc = ENOMEM;
    size_t n = f->n_items;
    if (!rc && n) memcpy(items, f->items, n * sizeof(*items));
    for (size_t i = 0; !rc && i < count; ++i) {
        struct json_object *update = json_object_array_get_idx(updates, i),
                           *revision = get(update, "revision");
        const char *id = str(update, "id"), *operation = str(update, "operation");
        if (!*id || strlen(id) > 128 || !json_object_is_type(revision, json_type_int) ||
            json_object_get_int64(revision) < 0) {
            rc = error_set(error, "invalid delta ID or revision");
            break;
        }
        for (size_t j = 0; j < i; ++j)
            if (!strcmp(id, str(json_object_array_get_idx(updates, j), "id"))) {
                rc = error_set(error, "duplicate delta predicate ID");
                break;
            }
        if (rc) break;
        size_t index = 0;
        while (index < n && strcmp(items[index].id, id)) ++index;
        if (index < n && items[index].required) {
            rc = error_set(error, "delta targets required predicate");
            break;
        }
        struct Predicate parsed = {0};
        strcpy(parsed.id, id);
        parsed.revision = json_object_get_uint64(revision);
        if (!strcmp(operation, "upsert")) {
            rc = predicate_header(update, 1, &parsed, error);
            parsed.source = source;
        } else if (strcmp(operation, "remove") || json_object_object_length(update) != 3)
            rc = error_set(error, "invalid delta operation");
        if (rc) break;
        if (index < n && parsed.revision <= items[index].revision) continue;
        if (parsed.node) {
            struct VgiFilters context = *f;
            context.payload = source->payload;
            rc = validate_predicate(&context, &parsed, error);
            if (rc) break;
        }
        if (index == n) {
            if (n >= 4096) {
                rc = error_set(error, "predicate-ID limit exceeded");
                break;
            }
            ++n;
        }
        items[index] = parsed;
    }
    if (rc) {
        free(items);
        VgiFiltersFree(source);
        return rc;
    }
    free(f->items);
    f->items = items;
    f->n_items = n;
    source->next = f->sources;
    f->sources = source;
    struct VgiFilters **link = &f->sources;
    while (*link) {
        struct VgiFilters *candidate = *link;
        int used = 0;
        for (size_t i = 0; i < n; ++i)
            if (items[i].node && items[i].source == candidate) {
                used = 1;
                break;
            }
        if (used)
            link = &candidate->next;
        else {
            *link = candidate->next;
            candidate->next = NULL;
            VgiFiltersFree(candidate);
        }
    }
    return 0;
}
int VgiFiltersApply(const struct VgiFilters *f, const struct VgiBatch *input,
                    struct VgiBatch *output, struct ArrowError *error) {
    return VgiFiltersSelect(f, input, output, NULL, error);
}
int VgiFiltersSelect(const struct VgiFilters *f, const struct VgiBatch *input,
                     struct VgiBatch *output, struct ArrowBuffer *kept, struct ArrowError *error) {
    int rc = VgiBatchInit(output, &input->schema, error);
    size_t count = f ? f->n_items : 0;
    for (int64_t row = 0; !rc && row < input->array.length; ++row) {
        int keep = 1;
        for (size_t i = 0; !rc && keep && i < count; ++i) {
            struct json_object *predicate = f->items[i].node;
            if (!predicate) continue;
            struct VgiFilters context = predicate_context(f, &f->items[i]);
            struct Value value = {0};
            rc = evaluate(&context, get(predicate, "expression"), input, row, 0, &value);
            if (rc && !strcmp(str(predicate, "mode"), "advisory")) {
                rc = 0;
                continue;
            }
            if (rc) {
                error_set(error, "required predicate cannot be evaluated");
                break;
            }
            keep = !value.null && value.number != 0;
        }
        if (!rc && keep)
            rc = VgiArrayAppendValue(&output->array, &output->schema, &input->view, &input->schema,
                                     row, error);
        if (!rc && keep && kept) rc = ArrowBufferAppendInt64(kept, row);
    }
    if (!rc) rc = VgiBatchFinish(output, error);
    if (rc) VgiBatchReset(output);
    return rc;
}

static int append_text(struct ArrowBuffer *out, const char *text) {
    return ArrowBufferAppend(out, text, (int64_t)strlen(text));
}
static int set_values(const struct VgiFilters *f, struct json_object *set,
                      const struct ArrowArrayView **view, const struct ArrowSchema **schema,
                      int64_t *start, int64_t *end) {
    *start = *end = 0;
    if (!strcmp(str(set, "kind"), "literal")) {
        char key[64];
        snprintf(key, sizeof(key), "value_%lld",
                 (long long)json_object_get_int64(get(set, "value_ref")));
        int64_t id = VgiFieldIndex(&f->payload.schema, key);
        if (id < 0) return EINVAL;
        const struct ArrowArrayView *list = f->payload.view.children[id];
        if (list->storage_type != NANOARROW_TYPE_LIST &&
            list->storage_type != NANOARROW_TYPE_LARGE_LIST)
            return EINVAL;
        *start = ArrowArrayViewListChildOffset(list, 0);
        *end = ArrowArrayViewListChildOffset(list, 1);
        *view = list->children[0];
        *schema = f->payload.schema.children[id]->children[0];
        return 0;
    }
    if (!strcmp(str(set, "kind"), "external")) {
        int64_t bi = json_object_get_int64(get(set, "batch_index")),
                ci = json_object_get_int64(get(set, "column_index"));
        if (bi < 0 || bi >= f->n_joins || ci < 0 || ci >= f->joins[bi].schema.n_children)
            return EINVAL;
        *view = f->joins[bi].view.children[ci];
        *schema = f->joins[bi].schema.children[ci];
        *end = (*view)->length;
        return strcmp((*schema)->name, str(set, "column_name")) ? EINVAL : 0;
    }
    return ENOTSUP;
}
static int render_scalar(const struct ArrowArrayView *array, const struct ArrowSchema *schema,
                         int64_t row, struct ArrowBuffer *out) {
    if (ArrowArrayViewIsNull(array, row)) return append_text(out, "NULL");
    if (array->dictionary)
        return render_scalar(array->dictionary, schema->dictionary,
                             ArrowArrayViewGetIntUnsafe(array, row), out);
    if (array->storage_type == NANOARROW_TYPE_BOOL)
        return append_text(out, ArrowArrayViewGetIntUnsafe(array, row) ? "True" : "False");
    struct ArrowStringView text = VgiValueString(array, row);
    if (text.data) {
        NANOARROW_RETURN_NOT_OK(append_text(out, "'"));
        NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(out, text.data, text.size_bytes));
        return append_text(out, "'");
    }
    return VgiValueRender(array, schema, row, out, NULL);
}
static int boolean_leaf(const struct VgiFilters *f, struct json_object *node, const char **name,
                        int *truth) {
    *truth = 1;
    if (!strcmp(str(node, "node"), "not")) {
        node = get(node, "expression");
        *truth = 0;
    }
    if (strcmp(str(node, "node"), "column_ref")) return 0;
    int64_t id = json_object_get_int64(get(node, "column_index"));
    if (id < 0 || id >= f->schema.n_children) return 0;
    const struct ArrowSchema *schema = f->schema.children[id];
    while (schema->dictionary) schema = schema->dictionary;
    if (strcmp(schema->format, "b")) return 0;
    *name = str(node, "column_name");
    return 1;
}
static int render_node(const struct VgiFilters *f, struct json_object *node,
                       struct ArrowBuffer *out, int predicate, int repr, int depth) {
    if (!node || depth > 64) return EINVAL;
    const char *kind = str(node, "node"), *name = NULL;
    int truth;
    if (predicate && !repr && boolean_leaf(f, node, &name, &truth)) {
        NANOARROW_RETURN_NOT_OK(append_text(out, name));
        return append_text(out, truth ? " = true" : " = false");
    }
    if (!strcmp(kind, "column_ref")) return append_text(out, str(node, "column_name"));
    if (!strcmp(kind, "literal")) {
        char key[64];
        snprintf(key, sizeof(key), "value_%lld",
                 (long long)json_object_get_int64(get(node, "value_ref")));
        int64_t id = VgiFieldIndex(&f->payload.schema, key);
        return id < 0 ? EINVAL
                      : render_scalar(f->payload.view.children[id], f->payload.schema.children[id],
                                      0, out);
    }
    if (!strcmp(kind, "comparison")) {
        if (repr) NANOARROW_RETURN_NOT_OK(append_text(out, "ConstantFilter("));
        NANOARROW_RETURN_NOT_OK(render_node(f, get(node, "left"), out, 0, 0, depth + 1));
        const char *op = str(node, "op"),
                   *symbol = !strcmp(op, "eq")                  ? " = "
                             : !strcmp(op, "ne")                ? " != "
                             : !strcmp(op, "lt")                ? " < "
                             : !strcmp(op, "le")                ? " <= "
                             : !strcmp(op, "gt")                ? " > "
                             : !strcmp(op, "ge")                ? " >= "
                             : !strcmp(op, "distinct_from")     ? " IS DISTINCT FROM "
                             : !strcmp(op, "not_distinct_from") ? " IS NOT DISTINCT FROM "
                                                                : " ? ";
        NANOARROW_RETURN_NOT_OK(append_text(out, symbol));
        NANOARROW_RETURN_NOT_OK(render_node(f, get(node, "right"), out, 0, 0, depth + 1));
        return repr ? append_text(out, ")") : 0;
    }
    if (!strcmp(kind, "and") || !strcmp(kind, "or")) {
        int and = !strcmp(kind, "and");
        NANOARROW_RETURN_NOT_OK(append_text(out, repr ? (and? "AndFilter([" : "OrFilter([") : "("));
        struct json_object *children = get(node, "children");
        if (!json_object_is_type(children, json_type_array)) return EINVAL;
        for (size_t i = 0; i < json_object_array_length(children); ++i) {
            if (i) NANOARROW_RETURN_NOT_OK(append_text(out, repr ? ", " : and? " AND " : " OR "));
            NANOARROW_RETURN_NOT_OK(
                render_node(f, json_object_array_get_idx(children, i), out, 1, repr, depth + 1));
        }
        return append_text(out, repr ? "])" : ")");
    }
    if (!strcmp(kind, "field_ref")) {
        NANOARROW_RETURN_NOT_OK(render_node(f, get(node, "expression"), out, 0, 0, depth + 1));
        NANOARROW_RETURN_NOT_OK(append_text(out, "."));
        return append_text(out, str(node, "field_name"));
    }
    if (!strcmp(kind, "not")) {
        NANOARROW_RETURN_NOT_OK(append_text(out, "NOT ("));
        NANOARROW_RETURN_NOT_OK(render_node(f, get(node, "expression"), out, 0, 0, depth + 1));
        return append_text(out, ")");
    }
    if (!strcmp(kind, "is_null")) {
        NANOARROW_RETURN_NOT_OK(render_node(f, get(node, "expression"), out, 0, 0, depth + 1));
        return append_text(
            out, json_object_get_boolean(get(node, "negated")) ? " IS NOT NULL" : " IS NULL");
    }
    if (!strcmp(kind, "in")) {
        NANOARROW_RETURN_NOT_OK(render_node(f, get(node, "expression"), out, 0, 0, depth + 1));
        NANOARROW_RETURN_NOT_OK(append_text(
            out, json_object_get_boolean(get(node, "negated")) ? " NOT IN (" : " IN ("));
        const struct ArrowArrayView *array;
        const struct ArrowSchema *schema;
        int64_t start, end;
        NANOARROW_RETURN_NOT_OK(set_values(f, get(node, "set"), &array, &schema, &start, &end));
        if (end - start > 20) {
            char text[64];
            snprintf(text, sizeof(text), "%lld values", (long long)(end - start));
            NANOARROW_RETURN_NOT_OK(append_text(out, text));
        } else
            for (int64_t i = start; i < end; ++i) {
                if (i > start) NANOARROW_RETURN_NOT_OK(append_text(out, ", "));
                NANOARROW_RETURN_NOT_OK(render_scalar(array, schema, i, out));
            }
        return append_text(out, ")");
    }
    return append_text(out, "(expression)");
}
int VgiFiltersFormat(const struct VgiFilters *f, int repr, struct ArrowBuffer *out,
                     struct ArrowError *error) {
    size_t count = f ? f->n_items : 0, rendered = 0;
    int rc = 0;
    for (size_t i = 0; !rc && i < count; ++i) {
        if (!f->items[i].node) continue;
        if (!rendered && repr) rc = append_text(out, "PushdownFilters([");
        if (rendered) rc = append_text(out, repr ? ", " : " AND ");
        struct VgiFilters context = predicate_context(f, &f->items[i]);
        if (!rc) rc = render_node(&context, get(f->items[i].node, "expression"), out, 1, repr, 0);
        ++rendered;
    }
    if (!rc && !rendered)
        rc = append_text(out, "(none)");
    else if (!rc && repr)
        rc = append_text(out, "])");
    if (rc) ArrowErrorSet(error, "Cannot format filter expression");
    return rc;
}
static int mentions(struct json_object *node, const char *name, int depth) {
    if (!node || depth > 64) return 0;
    if (json_object_is_type(node, json_type_object)) {
        if (!strcmp(str(node, "node"), "column_ref") && !strcmp(str(node, "column_name"), name))
            return 1;
        json_object_object_foreach(node, key, value) {
            (void)key;
            if (mentions(value, name, depth + 1)) return 1;
        }
    } else if (json_object_is_type(node, json_type_array)) {
        for (size_t i = 0; i < json_object_array_length(node); ++i)
            if (mentions(json_object_array_get_idx(node, i), name, depth + 1)) return 1;
    }
    return 0;
}
int VgiFiltersHasColumn(const struct VgiFilters *f, const char *name) {
    if (!f || !name) return 0;
    for (size_t i = 0; i < f->n_items; ++i)
        if (f->items[i].node && mentions(get(f->items[i].node, "expression"), name, 0)) return 1;
    return 0;
}
int VgiFiltersColumns(const struct VgiFilters *f, struct VgiBatch *out, struct ArrowError *error) {
    struct ArrowSchema schema = {0};
    ArrowSchemaInit(&schema);
    int rc = ArrowSchemaSetTypeStruct(&schema, 1);
    if (!rc) rc = ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_STRING);
    if (!rc) rc = ArrowSchemaSetName(schema.children[0], "column_name");
    if (!rc) rc = VgiBatchInit(out, &schema, error);
    const char **names = f ? calloc((size_t)f->schema.n_children, sizeof(*names)) : NULL;
    int64_t count = 0;
    if (f && f->schema.n_children && !names) rc = ENOMEM;
    for (int64_t i = 0; !rc && f && i < f->schema.n_children; ++i)
        if (VgiFiltersHasColumn(f, f->schema.children[i]->name))
            names[count++] = f->schema.children[i]->name;
    for (int64_t i = 1; i < count; ++i) {
        const char *name = names[i];
        int64_t j = i;
        while (j > 0 && strcmp(names[j - 1], name) > 0) {
            names[j] = names[j - 1];
            --j;
        }
        names[j] = name;
    }
    for (int64_t i = 0; !rc && i < count; ++i) {
        rc = ArrowArrayAppendString(out->array.children[0], ArrowCharView(names[i]));
        if (!rc) rc = ArrowArrayFinishElement(&out->array);
    }
    if (!rc) rc = VgiBatchFinish(out, error);
    free(names);
    ArrowSchemaRelease(&schema);
    return rc;
}
struct Values {
    struct Value *items;
    size_t count;
};
static int add_value(struct Values *values, struct Value value) {
    for (size_t i = 0; i < values->count; ++i)
        if (value.null == values->items[i].null &&
            (value.null || !compare(value, values->items[i])))
            return 0;
    if (values->count >= 1000000) return EOVERFLOW;
    struct Value *next = realloc(values->items, (values->count + 1) * sizeof(*next));
    if (!next) return ENOMEM;
    values->items = next;
    values->items[values->count++] = value;
    return 0;
}
static int direct_column(struct json_object *node, const char *name) {
    return !strcmp(str(node, "node"), "column_ref") && !strcmp(str(node, "column_name"), name);
}
static int discrete(const struct VgiFilters *f, struct json_object *node, const char *name,
                    struct Values *out, int depth) {
    if (!node || depth > 64) return EINVAL;
    const char *kind = str(node, "node"), *boolean_name = NULL;
    int truth;
    if (boolean_leaf(f, node, &boolean_name, &truth) && !strcmp(boolean_name, name))
        return add_value(out, (struct Value){.number = truth});
    if (!strcmp(kind, "comparison") && !strcmp(str(node, "op"), "eq") &&
        direct_column(get(node, "left"), name) &&
        !strcmp(str(get(node, "right"), "node"), "literal")) {
        struct Value value;
        NANOARROW_RETURN_NOT_OK(evaluate(f, get(node, "right"), NULL, 0, 0, &value));
        return add_value(out, value);
    }
    if (!strcmp(kind, "in") && !json_object_get_boolean(get(node, "negated")) &&
        direct_column(get(node, "expression"), name)) {
        const struct ArrowArrayView *array;
        const struct ArrowSchema *schema;
        int64_t start, end;
        NANOARROW_RETURN_NOT_OK(set_values(f, get(node, "set"), &array, &schema, &start, &end));
        for (int64_t i = start; i < end; ++i) {
            struct Value value = {0};
            NANOARROW_RETURN_NOT_OK(from_array(array, schema, i, &value));
            NANOARROW_RETURN_NOT_OK(add_value(out, value));
        }
        return 0;
    }
    if (!strcmp(kind, "and") || !strcmp(kind, "or")) {
        int and = !strcmp(kind, "and");
        struct json_object *children = get(node, "children");
        if (!json_object_is_type(children, json_type_array)) return EINVAL;
        for (size_t i = 0; i < json_object_array_length(children); ++i) {
            int rc = discrete(f, json_object_array_get_idx(children, i), name, out, depth + 1);
            if (and&&rc == ENODATA) continue;
            if (rc) return rc;
            if (and) return 0;
        }
        return and? ENODATA : 0;
    }
    return ENODATA;
}
int VgiFiltersColumnValues(const struct VgiFilters *f, const char *name, struct VgiBatch *out,
                           struct ArrowError *error) {
    if (!f) return ENODATA;
    int64_t id = VgiFieldIndex(&f->schema, name);
    if (id < 0) return ENODATA;
    struct Values values = {0};
    int rc = ENODATA;
    for (size_t i = 0; i < f->n_items; ++i) {
        if (!f->items[i].node) continue;
        struct VgiFilters context = predicate_context(f, &f->items[i]);
        rc = discrete(&context, get(f->items[i].node, "expression"), name, &values, 0);
        if (rc != ENODATA) break;
        free(values.items);
        values = (struct Values){0};
    }
    struct ArrowSchema schema = {0};
    if (!rc) {
        ArrowSchemaInit(&schema);
        rc = ArrowSchemaSetTypeStruct(&schema, 1);
        const struct ArrowSchema *type = f->schema.children[id];
        while (type->dictionary) type = type->dictionary;
        if (!rc) {
            ArrowSchemaRelease(schema.children[0]);
            rc = ArrowSchemaDeepCopy(type, schema.children[0]);
        }
        if (!rc) rc = ArrowSchemaSetName(schema.children[0], name);
        if (!rc) rc = VgiBatchInit(out, &schema, error);
        for (size_t i = 0; !rc && i < values.count; ++i) {
            struct Value *value = &values.items[i];
            rc = value->view ? VgiArrayAppendValue(out->array.children[0], out->schema.children[0],
                                                   value->view, value->schema, value->index, error)
                             : ArrowArrayAppendInt(out->array.children[0], (int64_t)value->number);
            if (!rc) rc = ArrowArrayFinishElement(&out->array);
        }
        if (!rc) rc = VgiBatchFinish(out, error);
    }
    if (schema.release) ArrowSchemaRelease(&schema);
    free(values.items);
    return rc;
}

static int bounds_node(const struct VgiFilters *f, struct json_object *node, const char *name,
                       struct VgiColumnBounds *out, int depth) {
    if (!node || depth > 64) return EINVAL;
    const char *kind = str(node, "node");
    if (!strcmp(kind, "comparison")) {
        struct json_object *left = get(node, "left"), *right = get(node, "right"), *literal = NULL;
        int swapped = 0;
        if (direct_column(left, name) && !strcmp(str(right, "node"), "literal"))
            literal = right;
        else if (direct_column(right, name) && !strcmp(str(left, "node"), "literal")) {
            literal = left;
            swapped = 1;
        }
        if (!literal) return ENODATA;
        struct Value value;
        NANOARROW_RETURN_NOT_OK(evaluate(f, literal, NULL, 0, 0, &value));
        if (value.null || !value.integer_kind ||
            (value.integer_kind == 2 && value.unsigned_integer > INT64_MAX))
            return ENODATA;
        int64_t n = value.integer_kind == 2 ? (int64_t)value.unsigned_integer : value.integer;
        const char *op = str(node, "op");
        if (swapped) {
            if (!strcmp(op, "lt"))
                op = "gt";
            else if (!strcmp(op, "le"))
                op = "ge";
            else if (!strcmp(op, "gt"))
                op = "lt";
            else if (!strcmp(op, "ge"))
                op = "le";
        }
        if (!strcmp(op, "eq")) {
            out->has_min = out->has_max = 1;
            out->min = out->max = n;
        } else if (!strcmp(op, "gt") || !strcmp(op, "ge")) {
            out->has_min = 1;
            out->min = n + (!strcmp(op, "gt") && n < INT64_MAX);
        } else if (!strcmp(op, "lt") || !strcmp(op, "le")) {
            out->has_max = 1;
            out->max = n - (!strcmp(op, "lt") && n > INT64_MIN);
        } else
            return ENODATA;
        return 0;
    }
    if (!strcmp(kind, "in") && !json_object_get_boolean(get(node, "negated")) &&
        direct_column(get(node, "expression"), name)) {
        const struct ArrowArrayView *view;
        const struct ArrowSchema *schema;
        int64_t start, end;
        NANOARROW_RETURN_NOT_OK(set_values(f, get(node, "set"), &view, &schema, &start, &end));
        for (int64_t i = start; i < end; ++i) {
            struct Value value = {0};
            NANOARROW_RETURN_NOT_OK(from_array(view, schema, i, &value));
            if (value.null) continue;
            if (!value.integer_kind ||
                (value.integer_kind == 2 && value.unsigned_integer > INT64_MAX))
                return ENODATA;
            int64_t n = value.integer_kind == 2 ? (int64_t)value.unsigned_integer : value.integer;
            if (!out->has_min || n < out->min) out->min = n;
            if (!out->has_max || n > out->max) out->max = n;
            out->has_min = out->has_max = 1;
        }
        return out->has_min ? 0 : ENODATA;
    }
    if (!strcmp(kind, "and") || !strcmp(kind, "or")) {
        int and = !strcmp(kind, "and"), found = 0;
        struct json_object *children = get(node, "children");
        if (!json_object_is_type(children, json_type_array)) return EINVAL;
        for (size_t i = 0; i < json_object_array_length(children); ++i) {
            struct VgiColumnBounds current = {0};
            int rc =
                bounds_node(f, json_object_array_get_idx(children, i), name, &current, depth + 1);
            if (rc == ENODATA && and) continue;
            if (rc) return rc;
            if (!found)
                *out = current;
            else if (and) {
                if (current.has_min && (!out->has_min || current.min > out->min)) {
                    out->has_min = 1;
                    out->min = current.min;
                }
                if (current.has_max && (!out->has_max || current.max < out->max)) {
                    out->has_max = 1;
                    out->max = current.max;
                }
            } else {
                out->has_min &= current.has_min;
                out->has_max &= current.has_max;
                if (current.min < out->min) out->min = current.min;
                if (current.max > out->max) out->max = current.max;
            }
            found = 1;
        }
        return found ? 0 : ENODATA;
    }
    return ENODATA;
}
int VgiFiltersColumnBounds(const struct VgiFilters *f, const char *name,
                           struct VgiColumnBounds *out, struct ArrowError *error) {
    if (!name || !out) return EINVAL;
    memset(out, 0, sizeof(*out));
    for (size_t i = 0; f && i < f->n_items; ++i) {
        if (!f->items[i].node) continue;
        struct VgiFilters context = predicate_context(f, &f->items[i]);
        struct VgiColumnBounds b = {0};
        int rc = bounds_node(&context, get(f->items[i].node, "expression"), name, &b, 0);
        if (rc == ENODATA) continue;
        if (rc) {
            ArrowErrorSet(error, "Cannot resolve filter bounds");
            return rc;
        }
        if (b.has_min && (!out->has_min || b.min > out->min)) {
            out->has_min = 1;
            out->min = b.min;
        }
        if (b.has_max && (!out->has_max || b.max < out->max)) {
            out->has_max = 1;
            out->max = b.max;
        }
    }
    return 0;
}
