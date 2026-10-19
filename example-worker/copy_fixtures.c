#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "fixtures.h"
#include "vgi/context.h"
#include "vgi/copy.h"
static int named(const struct ExampleFunction *f, const char *s) { return !strcmp(f->name, s); }
int ExampleCopyReader(const char *s) {
    return !strcmp(s, "example_lines_copy_reader") || !strcmp(s, "secret_lines_reader");
}
int ExampleCopyWriter(const char *s) {
    return !strcmp(s, "example_lines_writer") || !strcmp(s, "example_lines_ordered_writer") ||
           !strcmp(s, "secret_lines_writer");
}
static int secret(const struct ExampleFunction *f) { return !strncmp(f->name, "secret_", 7); }
static int same(struct ArrowStringView v, const char *s) {
    return v.size_bytes == (int64_t)strlen(s) && !memcmp(v.data, s, (size_t)v.size_bytes);
}
static char *copy_text(struct ArrowStringView v) {
    if (v.size_bytes < 0 || (uint64_t)v.size_bytes >= SIZE_MAX) return NULL;
    char *s = malloc((size_t)v.size_bytes + 1);
    if (s) {
        if (v.size_bytes) memcpy(s, v.data, (size_t)v.size_bytes);
        s[v.size_bytes] = 0;
    }
    return s;
}
static int error(struct VgiFunctionCall *call, const char *message) {
    return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "", message);
}
static struct ArrowBufferView scope(struct VgiFunctionCall *c) {
    return (struct ArrowBufferView){{c->execution_id.data}, c->execution_id.size_bytes};
}
struct Options {
    struct ArrowStringView null_string, delimiter, fail;
    int64_t skip, repeat;
    int skip_errors, header, exclusive;
};
static int options(struct ExampleFunction *f, struct VgiFunctionCall *c, struct Options *o) {
    memset(o, 0, sizeof(*o));
    o->delimiter = ArrowCharView(",");
    o->repeat = 1;
    if (secret(f)) return 0;
    o->null_string = VgiArgumentString(c, -1, "null_string");
    if (!o->null_string.data) return error(c, "required option 'null_string' is missing");
    struct ArrowStringView delimiter = VgiArgumentString(c, -1, "delimiter");
    if (delimiter.data) o->delimiter = delimiter;
    if (!o->delimiter.size_bytes) return error(c, "'delimiter' must not be empty");
    o->skip = VgiArgumentInt(c, -1, "skip_rows", 0);
    if (o->skip < 0) return error(c, "'skip_rows' must be >= 0");
    struct ArrowStringView on_error = VgiArgumentString(c, -1, "on_error");
    if (on_error.data && !same(on_error, "fail") && !same(on_error, "skip"))
        return error(c, "'on_error' must be one of ['fail', 'skip']");
    o->skip_errors = same(on_error, "skip");
    o->header = VgiArgumentInt(c, -1, "header", 0) != 0;
    o->repeat = VgiArgumentInt(c, -1, "header_repeat", 1);
    if (o->repeat < 0 || o->repeat > 3) return error(c, "'header_repeat' must be between 0 and 3");
    struct ArrowStringView exists = VgiArgumentString(c, -1, "on_exists");
    if (exists.data && !same(exists, "overwrite") && !same(exists, "error"))
        return error(c, "'on_exists' must be one of ['overwrite', 'error']");
    o->exclusive = same(exists, "error");
    o->fail = VgiArgumentString(c, -1, "fail_on_value");
    return 0;
}
int ExampleCopyBind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *out) {
    struct ExampleFunction *f = data;
    if (ExampleCopyReader(f->name)) return VgiCopyFromSchema(call, out, &call->rpc->error);
    if (!call->input_schema.release)
        return error(call, "This handler is a COPY TO format writer; invoke it via COPY TO");
    return ArrowSchemaDeepCopy(&call->input_schema, out);
}
int ExampleCopyLookups(void *data, struct VgiFunctionCall *call, struct VgiBatch *result) {
    struct ExampleFunction *f = data;
    if (!secret(f)) return 0;
    struct ArrowStringView path = VgiCopyPath(call, ExampleCopyReader(f->name) ? "from" : "to");
    if (!path.data) return 0;
    struct ArrowStringView type = VgiArgumentString(call, -1, "secret_type");
    if (!type.data) type = ArrowCharView("vgi_example");
    const char *names[] = {"lookup_secret_types", "lookup_scopes", "lookup_names"};
    struct ArrowStringView values[] = {type, path, ArrowCharView("")};
    for (int i = 0; i < 3; ++i) {
        struct ArrowArray *a = VgiBatchColumn(result, names[i]);
        NANOARROW_RETURN_NOT_OK(ArrowArrayAppendString(a->children[0], values[i]));
        NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(a));
    }
    return 0;
}
static struct ArrowStringView api_key(struct ExampleFunction *f, struct VgiFunctionCall *call) {
    struct ArrowStringView path = VgiCopyPath(call, ExampleCopyReader(f->name) ? "from" : "to"),
                           type = VgiArgumentString(call, -1, "secret_type");
    char *name = type.data ? copy_text(type) : NULL;
    const struct ArrowSchema *schema = NULL;
    const struct ArrowArrayView *value =
        VgiSecretSelect(call, name ? name : "vgi_example", &path, &schema);
    free(name);
    struct ArrowStringView key = VgiValueString(VgiStructField(value, schema, "api_key"), 0);
    return key.data ? key : ArrowCharView("NONE");
}
struct Reader {
    FILE *file;
    char *line, *delimiter;
    size_t capacity;
    int64_t line_number;
    struct Options options;
    int done;
};
int ExampleCopyInit(void *data, struct VgiFunctionCall *call) {
    struct ExampleFunction *f = data;
    if (ExampleCopyWriter(f->name)) return 0;
    struct Reader *r = calloc(1, sizeof(*r));
    if (!r) return ENOMEM;
    call->state = r;
    int rc = options(f, call, &r->options);
    if (rc || secret(f)) return rc;
    struct ArrowStringView path = VgiCopyPath(call, "from");
    if (!path.data)
        return error(call, "This handler is a COPY FROM format reader; invoke it via COPY FROM");
    char *name = copy_text(path);
    r->delimiter = copy_text(r->options.delimiter);
    if (!name || !r->delimiter) {
        free(name);
        return ENOMEM;
    }
    r->file = fopen(name, "rb");
    free(name);
    if (!r->file) return error(call, "example_lines: cannot read source file");
    return 0;
}
void ExampleCopyFree(void *data, struct VgiFunctionCall *call) {
    (void)data;
    struct Reader *r = call->state;
    if (r) {
        if (r->file) fclose(r->file);
        free(r->line);
        free(r->delimiter);
    }
    free(r);
    call->state = NULL;
}
static int parse_cell(struct VgiFunctionCall *call, struct ArrowArray *out,
                      const struct ArrowSchema *schema, const char *text) {
    struct ArrowSchemaView type;
    NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&type, schema, &call->rpc->error));
    char *end = NULL;
    errno = 0;
    int rc = 0;
    switch (type.type) {
        case NANOARROW_TYPE_STRING:
        case NANOARROW_TYPE_LARGE_STRING:
        case NANOARROW_TYPE_BINARY:
        case NANOARROW_TYPE_LARGE_BINARY:
            return ArrowArrayAppendString(out, ArrowCharView(text));
        case NANOARROW_TYPE_BOOL:
            if (!strcmp(text, "true") || !strcmp(text, "1")) return ArrowArrayAppendInt(out, 1);
            if (!strcmp(text, "false") || !strcmp(text, "0")) return ArrowArrayAppendInt(out, 0);
            return error(call, "Cannot parse BOOLEAN value");
        case NANOARROW_TYPE_INT8:
        case NANOARROW_TYPE_INT16:
        case NANOARROW_TYPE_INT32:
        case NANOARROW_TYPE_INT64: {
            int64_t value = strtoll(text, &end, 10);
            if (errno || end == text || *end) return error(call, "Cannot parse integer value");
            rc = ArrowArrayAppendInt(out, value);
            break;
        }
        case NANOARROW_TYPE_UINT8:
        case NANOARROW_TYPE_UINT16:
        case NANOARROW_TYPE_UINT32:
        case NANOARROW_TYPE_UINT64: {
            uint64_t value = strtoull(text, &end, 10);
            if (errno || end == text || *end || *text == '-')
                return error(call, "Cannot parse unsigned integer value");
            rc = ArrowArrayAppendUInt(out, value);
            break;
        }
        case NANOARROW_TYPE_FLOAT:
        case NANOARROW_TYPE_DOUBLE: {
            double value = strtod(text, &end);
            if (errno || end == text || *end)
                return error(call, "Cannot parse floating-point value");
            rc = ArrowArrayAppendDouble(out, value);
            break;
        }
        default:
            return error(call, "example_lines: unsupported target column type");
    }
    return rc;
}
int ExampleCopyRead(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                    struct VgiBatch *out, struct ArrowBuffer *metadata, int *finished) {
    (void)input;
    (void)metadata;
    struct ExampleFunction *f = data;
    struct Reader *r = call->state;
    if (!r || r->done) {
        *finished = 1;
        return 0;
    }
    int rc = VgiBatchInit(out, &call->output_schema, &call->rpc->error);
    if (secret(f)) {
        if (out->schema.n_children != 1)
            return error(call, "secret_lines_in: expected a single-column target");
        struct ArrowStringView value = api_key(f, call);
        char *text = copy_text(value);
        if (!text) return ENOMEM;
        if (!rc) rc = parse_cell(call, out->array.children[0], out->schema.children[0], text);
        free(text);
        if (!rc) rc = ArrowArrayFinishElement(&out->array);
        r->done = 1;
    } else {
        int64_t n = out->schema.n_children;
        char **cells = calloc((size_t)n + 1, sizeof(*cells));
        if (!cells) return ENOMEM;
        while (!rc && out->array.length < 2048) {
            ssize_t length = getline(&r->line, &r->capacity, r->file);
            if (length < 0) {
                if (ferror(r->file)) rc = EIO;
                r->done = 1;
                break;
            }
            if (r->line_number++ < r->options.skip) continue;
            while (length > 0 && (r->line[length - 1] == '\n' || r->line[length - 1] == '\r'))
                r->line[--length] = 0;
            if (!length) continue;
            int64_t count = 0;
            char *start = r->line;
            for (;;) {
                if (count <= n) cells[count] = start;
                ++count;
                char *next = strstr(start, r->delimiter);
                if (!next) break;
                *next = 0;
                start = next + r->options.delimiter.size_bytes;
            }
            if (count != n) {
                if (r->options.skip_errors) continue;
                char message[128];
                snprintf(message, sizeof(message),
                         "example_lines: row has %lld fields, expected %lld", (long long)count,
                         (long long)n);
                rc = error(call, message);
                break;
            }
            for (int64_t col = 0; !rc && col < n; ++col)
                rc = same(r->options.null_string, cells[col])
                         ? ArrowArrayAppendNull(out->array.children[col], 1)
                         : parse_cell(call, out->array.children[col], out->schema.children[col],
                                      cells[col]);
            if (!rc) rc = ArrowArrayFinishElement(&out->array);
        }
        free(cells);
    }
    if (!rc) rc = VgiBatchFinish(out, &call->rpc->error);
    return rc;
}
int ExampleCopyWrite(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                     struct ArrowBuffer *state_id) {
    struct ExampleFunction *f = data;
    struct Options o;
    int rc = options(f, call, &o);
    struct ArrowBuffer text, cell;
    ArrowBufferInit(&text);
    ArrowBufferInit(&cell);
    if (secret(f)) {
        char count[32];
        snprintf(count, sizeof(count), "%lld", (long long)input->array.length);
        if (!rc) rc = ArrowBufferAppend(&text, count, strlen(count));
    } else
        for (int64_t row = 0; !rc && row < input->array.length; ++row) {
            for (int64_t col = 0; !rc && col < input->schema.n_children; ++col) {
                if (col) rc = ArrowBufferAppend(&text, o.delimiter.data, o.delimiter.size_bytes);
                if (rc) break;
                cell.size_bytes = 0;
                if (ArrowArrayViewIsNull(input->view.children[col], row))
                    rc = ArrowBufferAppend(&cell, o.null_string.data, o.null_string.size_bytes);
                else {
                    rc = VgiValueRender(input->view.children[col], input->schema.children[col], row,
                                        &cell, &call->rpc->error);
                    if (!rc && o.fail.size_bytes && cell.size_bytes == o.fail.size_bytes &&
                        !memcmp(cell.data, o.fail.data, (size_t)cell.size_bytes))
                        rc = error(call, "fail_on_value matched a source cell");
                }
                if (!rc) rc = ArrowBufferAppend(&text, cell.data, cell.size_bytes);
            }
            if (!rc) rc = ArrowBufferAppend(&text, "\n", 1);
        }
    if (!rc && input->array.length)
        rc = VgiStorageAppend(call->storage, scope(call), "copy.shards",
                              (struct ArrowBufferView){{text.data}, text.size_bytes}, NULL,
                              &call->rpc->error);
    if (!rc)
        rc = ArrowBufferAppend(state_id, call->execution_id.data, call->execution_id.size_bytes);
    ArrowBufferReset(&text);
    ArrowBufferReset(&cell);
    return rc;
}
static int write_bytes(FILE *file, struct ArrowStringView value) {
    return value.size_bytes && !fwrite(value.data, (size_t)value.size_bytes, 1, file) ? EIO : 0;
}
int ExampleCopyClose(void *data, struct VgiFunctionCall *call, struct VgiBatch *result) {
    struct ExampleFunction *f = data;
    struct Options o;
    int rc = options(f, call, &o);
    struct ArrowStringView path = VgiCopyPath(call, "to");
    if (!path.size_bytes) return error(call, "COPY TO has no destination path");
    char *name = copy_text(path);
    if (!name) return ENOMEM;
    FILE *file = NULL;
    if (!rc) {
        int fd = open(name, O_WRONLY | O_CREAT | (o.exclusive ? O_EXCL : O_TRUNC), 0666);
        if (fd < 0)
            rc = error(call, errno == EEXIST ? "destination already exists"
                                             : "cannot open destination file");
        else {
            file = fdopen(fd, "wb");
            if (!file) {
                close(fd);
                rc = errno;
            }
        }
    }
    free(name);
    if (!rc && o.header)
        for (int64_t repeat = 0; !rc && repeat < o.repeat; ++repeat) {
            for (int64_t col = 0; !rc && col < call->output_schema.n_children; ++col) {
                if (col) rc = write_bytes(file, o.delimiter);
                if (!rc)
                    rc = write_bytes(file, ArrowCharView(call->output_schema.children[col]->name));
            }
            if (!rc) rc = write_bytes(file, ArrowCharView("\n"));
        }
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    int64_t after = 0, id = 0, rows = 0;
    while (!rc) {
        bytes.size_bytes = 0;
        rc = VgiStorageNext(call->storage, scope(call), "copy.shards", after, &id, &bytes,
                            &call->rpc->error);
        if (rc == ENOENT) {
            rc = 0;
            break;
        }
        if (rc) break;
        after = id;
        if (secret(f)) {
            rc = ArrowBufferAppend(&bytes, "", 1);
            if (!rc) {
                int64_t count = strtoll((const char *)bytes.data, NULL, 10);
                if (__builtin_add_overflow(rows, count, &rows)) rc = EOVERFLOW;
            }
        } else
            rc = write_bytes(file,
                             (struct ArrowStringView){(const char *)bytes.data, bytes.size_bytes});
    }
    if (!rc && secret(f)) {
        struct ArrowStringView key = api_key(f, call);
        rc = write_bytes(file, ArrowCharView("api_key="));
        if (!rc) rc = write_bytes(file, key);
        char text[64];
        snprintf(text, sizeof(text), "\nrows=%lld\n", (long long)rows);
        if (!rc) rc = write_bytes(file, ArrowCharView(text));
    }
    if (file && fclose(file) && !rc) rc = EIO;
    ArrowBufferReset(&bytes);
    if (!rc) rc = ArrowArrayFinishElement(VgiBatchColumn(result, "finalize_state_ids"));
    return rc;
}
int ExampleCopyRegister(struct VgiWorker *worker, const struct VgiFunction *fn,
                        struct ArrowError *error) {
    struct ExampleFunction *f = fn->userdata;
    if (!ExampleCopyReader(f->name) && !ExampleCopyWriter(f->name)) return 0;
    struct VgiBatch info = {0};
    int rc = VgiBatchInitNamed(&info, "CopyFromFormatInfo", error);
    const char *format = named(f, "example_lines_copy_reader")      ? "example_lines"
                         : named(f, "secret_lines_reader")          ? "secret_lines_in"
                         : named(f, "secret_lines_writer")          ? "secret_lines_out"
                         : named(f, "example_lines_ordered_writer") ? "example_lines_ordered_out"
                                                                    : "example_lines_out";
    if (!rc) rc = VgiBatchSetString(&info, "format_name", format);
    if (!rc) rc = VgiBatchSetString(&info, "handler", f->name);
    if (!rc) rc = VgiBatchSetString(&info, "direction", ExampleCopyReader(f->name) ? "from" : "to");
    if (!rc) rc = VgiBatchSetInt(&info, "ordered", named(f, "example_lines_ordered_writer"));
    struct ArrowBufferView options = VgiBatchBytes(&fn->info, "arguments");
    if (!rc) rc = VgiBatchSetBytes(&info, "options", options.data.data, options.size_bytes);
    struct ArrowStringView description = VgiBatchString(&fn->info, "description");
    if (!rc) rc = VgiBatchSetBytes(&info, "description", description.data, description.size_bytes);
    if (!rc)
        rc = VgiBatchSetString(&info, "comment",
                               ExampleCopyReader(f->name) ? "Toy delimited-text reader for tests"
                                                          : "Toy delimited-text writer for tests");
    struct ArrowArray *tags = VgiBatchColumn(&info, "tags");
    const char *keys[] = {"category", "stability"},
               *values[] = {ExampleCopyReader(f->name) ? "copy_from" : "copy_to", "test"};
    for (int i = 0; !rc && i < 2; ++i) {
        rc = ArrowArrayAppendString(tags->children[0]->children[0], ArrowCharView(keys[i]));
        if (!rc)
            rc = ArrowArrayAppendString(tags->children[0]->children[1], ArrowCharView(values[i]));
        if (!rc) rc = ArrowArrayFinishElement(tags->children[0]);
    }
    if (!rc) rc = ArrowArrayFinishElement(tags);
    if (!rc) rc = VgiBatchFinishRow(&info, error);
    if (!rc) rc = VgiWorkerRegisterCopyFormat(worker, &info, error);
    VgiBatchReset(&info);
    return rc;
}
