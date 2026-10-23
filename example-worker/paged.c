/* Continued-output fixtures. Their contracts are in the extension's
 * table_in_out tests: blended_paged (lateral_continuation.test), blended_sliced
 * (input_rows_per_call.test) and paged_explode (continuation_classic.test), with
 * fail_on_page, revalidate and not_modified_more in
 * continuation_resume_error_revalidate.test. Each input's output is rows
 * i = 0..n-1 per input row, cut into page_size-row replies; process sends the
 * first, resume the rest. The state keeps only the counts and a position. */
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "fixtures.h"
#include "vgi/cache.h"

struct Paged {
    int64_t *counts;
    int64_t count;
    int64_t row, offset; /* where the next reply resumes */
    int64_t call_rows;   /* blended_sliced: the current call's input rows */
    int64_t page;        /* replies sent for the current input */
    int empty_first;     /* not_modified_more: the first reply is empty and says more */
    int64_t bump;        /* added to every value; -1 sends the whole output in one reply */
};
static int named(const struct ExampleFunction *f, const char *name) {
    return !strcmp(f->name, name);
}
int ExamplePagedSupported(const char *name) {
    return !strcmp(name, "blended_paged") || !strcmp(name, "blended_sliced") ||
           !strcmp(name, "paged_explode");
}
int ExamplePagedBind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *out) {
    struct ExampleFunction *f = data;
    int sliced = named(f, "blended_sliced");
    int64_t page_size = VgiArgumentInt(call, -1, "page_size", sliced ? 1000000 : 2),
            ttl = VgiArgumentInt(call, -1, "cache_ttl", 0);
    char message[96];
    if (page_size < 1 || ttl < 0) {
        if (page_size < 1)
            snprintf(message, sizeof(message), "page_size must be at least 1, got %lld",
                     (long long)page_size);
        else
            snprintf(message, sizeof(message), "cache_ttl must not be negative, got %lld",
                     (long long)ttl);
        return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "", message);
    }
    ArrowSchemaInit(out);
    int rc = ArrowSchemaSetTypeStruct(out, sliced ? 2 : 1);
    for (int64_t i = 0; !rc && i < out->n_children; ++i) {
        rc = ArrowSchemaSetType(out->children[i], NANOARROW_TYPE_INT64);
        if (!rc) rc = ArrowSchemaSetName(out->children[i], i ? "call_rows" : "i");
    }
    return rc;
}
int ExamplePagedInit(void *data, struct VgiFunctionCall *call) {
    (void)data;
    call->state = calloc(1, sizeof(struct Paged));
    return call->state ? 0 : ENOMEM;
}
void ExamplePagedFree(void *data, struct VgiFunctionCall *call) {
    (void)data;
    struct Paged *state = call->state;
    if (state) free(state->counts);
    free(state);
    call->state = NULL;
}
/* The next page_size rows, which may span input rows, and whether more follow.
 * Input rows with no output after the last non-empty page add no reply. */
static int next_page(struct Paged *s, int64_t page_size, int64_t bump, struct VgiBatch *out,
                     struct ArrowBuffer *parents) {
    int64_t i_id = VgiFieldIndex(&out->schema, "i"),
            rows_id = VgiFieldIndex(&out->schema, "call_rows");
    int rc = 0;
    for (int64_t taken = 0; !rc && s->row < s->count && taken < page_size;) {
        if (s->offset == s->counts[s->row]) {
            ++s->row;
            s->offset = 0;
            continue;
        }
        if (i_id >= 0) rc = ArrowArrayAppendInt(out->array.children[i_id], s->offset + bump);
        if (!rc && rows_id >= 0)
            rc = ArrowArrayAppendInt(out->array.children[rows_id], s->call_rows);
        if (!rc) rc = ArrowArrayFinishElement(&out->array);
        if (!rc) rc = ExampleParentRow(parents, s->row);
        ++s->offset;
        ++taken;
    }
    while (s->row < s->count && s->offset == s->counts[s->row]) {
        ++s->row;
        s->offset = 0;
    }
    return rc;
}
static int emit_page(struct ExampleFunction *f, struct VgiFunctionCall *call, struct VgiBatch *out,
                     struct ArrowBuffer *metadata, const struct VgiCacheControl *control) {
    struct Paged *s = call->state;
    int sliced = named(f, "blended_sliced");
    int64_t page_size = VgiArgumentInt(call, -1, "page_size", sliced ? 1000000 : 2);
    if (!sliced && ++s->page == VgiArgumentInt(call, -1, "fail_on_page", 0)) {
        char message[64];
        snprintf(message, sizeof(message), "failing on page %lld as asked", (long long)s->page);
        return VgiRpcFail(call->rpc, "RuntimeError", "INTERNAL", "", message);
    }
    struct ArrowBuffer parents;
    ArrowBufferInit(&parents);
    int rc = VgiBatchInit(out, &call->output_schema, &call->rpc->error);
    if (!rc && !(s->page == 1 && s->empty_first))
        rc = next_page(s, s->bump < 0 ? INT64_MAX : page_size, s->bump < 0 ? 0 : s->bump, out,
                       &parents);
    if (!rc) rc = VgiBatchFinish(out, &call->rpc->error);
    /* paged_explode is a classic table function: no per-row provenance. */
    if (!rc && !named(f, "paged_explode")) rc = ExampleParentsMetadata(metadata, &parents);
    if (!rc && control) rc = VgiCacheControlMetadata(control, metadata);
    call->more_output = s->page == 1 && s->empty_first ? 1 : s->row < s->count;
    ArrowBufferReset(&parents);
    return rc;
}
int ExamplePagedProcess(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                        struct VgiBatch *out, struct ArrowBuffer *metadata, int *finished) {
    (void)finished;
    struct ExampleFunction *f = data;
    struct Paged *s = call->state;
    int sliced = named(f, "blended_sliced");
    if (sliced && input->array.length > 3) {
        char message[96];
        snprintf(message, sizeof(message),
                 "blended_sliced declares input_rows_per_call = 3 but got %lld rows",
                 (long long)input->array.length);
        return VgiRpcFail(call->rpc, "RuntimeError", "INTERNAL", "", message);
    }
    int64_t *counts = calloc((size_t)input->array.length + 1, sizeof(*counts));
    if (!counts) return ENOMEM;
    const struct ArrowArrayView *n = input->view.n_children ? input->view.children[0] : NULL;
    for (int64_t row = 0; n && row < input->array.length; ++row) {
        int64_t value = ArrowArrayViewIsNull(n, row) || !VgiValueIsNumeric(n)
                            ? 0
                            : ArrowArrayViewGetIntUnsafe(n, row);
        counts[row] = value < 0 ? 0 : value;
    }
    free(s->counts);
    *s = (struct Paged){
        .counts = counts, .count = input->array.length, .call_rows = input->array.length};
    int64_t ttl = VgiArgumentInt(call, -1, "cache_ttl", 0);
    struct VgiCacheControl control = {.has_ttl = ttl > 0, .ttl_seconds = ttl, .per_value = sliced};
    int cached = ttl > 0;
    /* revalidate: the output depends on the request alone. A plain request gets
     * the whole output in one reply, cacheable but immediately stale (etag v1);
     * a conditional one is told the data changed (every value 1000 larger, etag
     * v2), paged as usual. not_modified_more breaks the protocol on purpose: an
     * empty not_modified reply (etag v1) that also says more. */
    if (!sliced && VgiArgumentInt(call, -1, "revalidate", 0)) {
        struct ArrowStringView validator =
            VgiMetadataGet(call->input_metadata, "vgi.cache.if_none_match");
        control = (struct VgiCacheControl){
            .has_ttl = 1, .ttl_seconds = 0, .revalidatable = 1, .etag = "v1"};
        cached = 1;
        if (!validator.data)
            s->bump = -1;
        else {
            s->bump = 1000;
            if (VgiArgumentInt(call, -1, "not_modified_more", 0)) {
                s->empty_first = 1;
                control.not_modified = 1;
            } else
                control.etag = "v2";
        }
    }
    return emit_page(f, call, out, metadata, cached ? &control : NULL);
}
int ExamplePagedResume(void *data, struct VgiFunctionCall *call, struct VgiBatch *out,
                       struct ArrowBuffer *metadata) {
    return emit_page(data, call, out, metadata, NULL);
}
