#ifndef VGI_WORKER_H
#define VGI_WORKER_H
#include "vgi/filter.h"
#include "vgi/rpc.h"
#include "vgi/storage.h"
#ifdef __cplusplus
extern "C" {
#endif

struct VgiWorker;
struct VgiFunctionCall {
    struct VgiRpcCall *rpc;
    struct VgiBatch bind_request;
    struct VgiBatch init_request;
    struct VgiBatch arguments;
    struct VgiBatch settings;
    struct VgiBatch secrets;
    /* Merged per-attachment options. Secret values remain in private storage. */
    struct VgiBatch attach_options;
    /* Verified split payloads, one binary column; absent for unplanned scans. */
    struct VgiBatch split_payloads;
    struct ArrowSchema input_schema;
    struct ArrowSchema output_schema;
    struct ArrowBuffer execution_id;
    const struct ArrowSchema *argument_schema;
    struct VgiStorage *storage;
    struct VgiFilters *filters;
    /* Borrowed Arrow metadata for the current input batch. */
    const char *input_metadata;
    void *state;
};
enum VgiFunctionKind { VGI_SCALAR, VGI_TABLE, VGI_TABLE_IN_OUT, VGI_AGGREGATE, VGI_BUFFERING };
struct VgiFunction {
    enum VgiFunctionKind kind;
    int auto_apply_filters;
    struct VgiBatch info;
    struct ArrowSchema arguments;
    struct ArrowSchema output;
    const char *catalog;
    const char *schema_path;
    /* Bind writes an owned output schema. If absent, output is copied. */
    int (*bind)(void *userdata, struct VgiFunctionCall *call, struct ArrowSchema *output);
    int (*init)(void *userdata, struct VgiFunctionCall *call);
    int64_t (*max_workers)(void *userdata, struct VgiFunctionCall *call);
    int (*process)(void *userdata, struct VgiFunctionCall *call, const struct VgiBatch *input,
                   struct VgiBatch *output, struct ArrowBuffer *metadata, int *finished);
    void (*finalize)(void *userdata, struct VgiFunctionCall *call);
    void (*release)(void *userdata);
    void *userdata;
    /* Buffered functions receive unary sink/combine calls and stream their
     * final results through process with phase TABLE_BUFFERING_FINALIZE. */
    int (*buffer_process)(void *userdata, struct VgiFunctionCall *call,
                          const struct VgiBatch *input, struct ArrowBuffer *state_id);
    int (*buffer_combine)(void *userdata, struct VgiFunctionCall *call, struct VgiBatch *result);
    int (*secret_lookups)(void *userdata, struct VgiFunctionCall *call,
                          struct VgiBatch *bind_result);
    int (*plan)(void *userdata, struct VgiFunctionCall *call, const char *method,
                struct VgiBatch *result);
    /* Aggregate state is persisted between calls and shared by worker processes.
     * update sets present when a row creates state; finalize appends one value
     * to each output column, without finishing the top-level struct row. */
    int (*aggregate_update)(void *userdata, struct VgiFunctionCall *call,
                            const struct VgiBatch *input, int64_t row, struct ArrowBuffer *state,
                            int *present);
    int (*aggregate_combine)(void *userdata, struct VgiFunctionCall *call,
                             struct ArrowBuffer *target, struct ArrowBufferView source);
    int (*aggregate_finalize)(void *userdata, struct VgiFunctionCall *call,
                              const struct ArrowBufferView *state, struct VgiBatch *output);
};

/* Function info remains a builder until registration: set any generated
 * FunctionInfo property with VgiBatchSet* before handing it to the worker. */
int VgiFunctionInit(struct VgiFunction *function, const char *name, enum VgiFunctionKind kind,
                    const struct ArrowSchema *arguments, const struct ArrowSchema *output,
                    struct ArrowError *error);
void VgiFunctionReset(struct VgiFunction *function);
struct VgiWorker *VgiWorkerNew(const char *catalog);
void VgiWorkerFree(struct VgiWorker *worker);
/* Successful registration moves function and clears it. */
int VgiWorkerRegisterFunction(struct VgiWorker *worker, struct VgiFunction *function,
                              struct ArrowError *error);
int VgiWorkerAddProtocol(struct VgiWorker *, const struct VgiRpcProtocol *, struct ArrowError *);
struct VgiHttpOptions;
/* Configure default HTTP callbacks before Run; borrowed objects must outlive it. */
void VgiWorkerSetHttpOptions(struct VgiWorker *, const struct VgiHttpOptions *);
/* Configure before serving. The telemetry instance must outlive the worker. */
int VgiWorkerSetTelemetry(struct VgiWorker *, struct VgiTelemetry *);
int VgiWorkerServeHttp(struct VgiWorker *, const struct VgiHttpOptions *, struct ArrowError *);
int VgiWorkerRun(struct VgiWorker *worker, int argc, char **argv, struct ArrowError *error);
int VgiWorkerServe(struct VgiWorker *worker, FILE *input, FILE *output, struct ArrowError *error);

/* Build argument schemas using Arrow C format strings (u=utf8, l=int64, etc.).
 * Constant/named/variadic parameters use the same metadata as every other SDK. */
int VgiArgumentInit(struct ArrowSchema *out, const char *name, const char *format, int constant,
                    int named, int variadic, struct ArrowError *error);
const struct ArrowSchema *VgiArgumentSchema(const struct VgiFunctionCall *call, int64_t position,
                                            const char *name);
const struct ArrowArrayView *VgiArgument(const struct VgiFunctionCall *call, int64_t position,
                                         const char *name);
int64_t VgiArgumentInt(const struct VgiFunctionCall *call, int64_t position, const char *name,
                       int64_t fallback);
double VgiArgumentDouble(const struct VgiFunctionCall *call, int64_t position, const char *name,
                         double fallback);
struct ArrowStringView VgiArgumentString(const struct VgiFunctionCall *call, int64_t position,
                                         const char *name);
#ifdef __cplusplus
}
#endif
#endif
