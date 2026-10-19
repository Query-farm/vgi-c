#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "fixtures.h"
struct CancellableState {
    int64_t next, count, sleep_ms;
};
int ExampleCancellableSupported(const char *name) {
    return !strcmp(name, "slow_cancellable") || !strcmp(name, "slow_cancellable_inout");
}
int ExampleCancellableBind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *out) {
    struct ExampleFunction *f = data;
    if (!strcmp(f->name, "slow_cancellable_inout")) {
        if (!call->input_schema.release) return EINVAL;
        return ArrowSchemaDeepCopy(&call->input_schema, out);
    }
    ArrowSchemaInit(out);
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeStruct(out, 1));
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(out->children[0], NANOARROW_TYPE_INT64));
    return ArrowSchemaSetName(out->children[0], "n");
}
int ExampleCancellableInit(void *data, struct VgiFunctionCall *call) {
    (void)data;
    struct CancellableState *s = calloc(1, sizeof(*s));
    if (!s) return ENOMEM;
    s->count = VgiArgumentInt(call, -1, "count", 1000000);
    s->sleep_ms = VgiArgumentInt(call, -1, "sleep_ms", 50);
    if (s->count < 0) s->count = 0;
    if (s->sleep_ms < 0) s->sleep_ms = 0;
    call->state = s;
    return 0;
}
int ExampleCancellableProcess(void *data, struct VgiFunctionCall *call,
                              const struct VgiBatch *input, struct VgiBatch *out,
                              struct ArrowBuffer *metadata, int *finished) {
    (void)metadata;
    struct ExampleFunction *f = data;
    struct CancellableState *s = call->state;
    int exchange = !strcmp(f->name, "slow_cancellable_inout");
    if (!exchange && s->next >= s->count) {
        *finished = 1;
        return 0;
    }
    struct timespec delay = {.tv_sec = (time_t)(s->sleep_ms / 1000),
                             .tv_nsec = (long)(s->sleep_ms % 1000) * 1000000};
    while (nanosleep(&delay, &delay) && errno == EINTR) {
    }
    if (exchange) return VgiBatchCoerce(input, &call->output_schema, out, &call->rpc->error);
    int rc = VgiBatchInit(out, &call->output_schema, &call->rpc->error);
    for (int64_t i = 0; !rc && i < out->schema.n_children; ++i)
        rc = ArrowArrayAppendInt(out->array.children[i], s->next);
    if (!rc) rc = VgiBatchFinishRow(out, &call->rpc->error);
    ++s->next;
    return rc;
}
void ExampleCancellableFinalize(void *data, struct VgiFunctionCall *call) {
    (void)data;
    free(call->state);
    call->state = NULL;
}
