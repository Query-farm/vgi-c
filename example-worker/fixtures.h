#ifndef VGI_EXAMPLE_FIXTURES_H
#define VGI_EXAMPLE_FIXTURES_H
#include "vgi/worker.h"
struct ExampleFunction {
    char name[128];
    char path[256];
    char catalog[128];
    struct ArrowSchema output;
    struct ArrowSchema arguments;
};
int ExampleRegister(struct VgiWorker *worker, struct ArrowError *error);
int ExampleScalarSupported(const char *name);
int ExampleScalarBind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *output);
int ExampleScalarProcess(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                         struct VgiBatch *output, struct ArrowBuffer *metadata, int *finished);
int ExampleTensorBind(struct VgiFunctionCall *call, struct ArrowSchema *output);
int ExampleTensorRow(struct VgiFunctionCall *call, const struct VgiBatch *input,
                     struct VgiBatch *output, int64_t row);
int ExampleExtraSupported(const char *name);
int ExampleExtraProcess(struct ExampleFunction *function, struct VgiFunctionCall *call,
                        const struct VgiBatch *input, struct VgiBatch *output,
                        struct ArrowBuffer *metadata);
int ExampleAggregateSupported(const char *name);
int ExampleAggregateBind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *out);
int ExampleAggregateUpdate(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                           int64_t row, struct ArrowBuffer *state, int *present);
int ExampleAggregateCombine(void *data, struct VgiFunctionCall *call, struct ArrowBuffer *target,
                            struct ArrowBufferView source);
int ExampleAggregateFinalize(void *data, struct VgiFunctionCall *call,
                             const struct ArrowBufferView *state, struct VgiBatch *output);
int ExampleNestBind(struct VgiFunctionCall *call, struct ArrowSchema *out);
int ExampleNestUpdate(struct VgiFunctionCall *call, const struct VgiBatch *input, int64_t row,
                      struct ArrowBuffer *state, int *present);
int ExampleNestFinalize(struct VgiFunctionCall *call, const struct ArrowBufferView *state,
                        struct VgiBatch *output);
int ExampleTableSupported(const char *name);
int ExampleTableIsExchange(const char *name);
int ExampleTableBind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *out);
int ExampleTableInit(void *data, struct VgiFunctionCall *call);
void ExampleTableFinalize(void *data, struct VgiFunctionCall *call);
int ExampleTableProcess(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                        struct VgiBatch *out, struct ArrowBuffer *metadata, int *finished);
int ExampleBufferSupported(const char *name);
int ExampleBufferBind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *out);
int ExampleBufferInit(void *data, struct VgiFunctionCall *call);
void ExampleBufferRelease(void *data, struct VgiFunctionCall *call);
int ExampleBufferProcess(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                         struct VgiBatch *out, struct ArrowBuffer *metadata, int *finished);
int ExampleBufferSink(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                      struct ArrowBuffer *state_id);
int ExampleBufferCombine(void *data, struct VgiFunctionCall *call, struct VgiBatch *result);
int ExampleBlendedSupported(const char *name);
int ExampleBlendedBind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *out);
int ExampleBlendedProcess(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                          struct VgiBatch *out, struct ArrowBuffer *metadata, int *finished);
int ExampleTablePlan(void *data, struct VgiFunctionCall *call, const char *method,
                     struct VgiBatch *result);
int ExampleContextSupported(const char *name);
int ExampleContextBind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *out);
int ExampleContextLookups(void *data, struct VgiFunctionCall *call, struct VgiBatch *result);
int ExampleContextProcess(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                          struct VgiBatch *out, struct ArrowBuffer *metadata, int *finished);
int ExampleCacheSupported(const char *name);
int ExampleCacheExchange(const char *name);
int ExampleCacheBind(void *, struct VgiFunctionCall *, struct ArrowSchema *);
int ExampleCacheInit(void *, struct VgiFunctionCall *);
void ExampleCacheFree(void *, struct VgiFunctionCall *);
int ExampleCacheProcess(void *, struct VgiFunctionCall *, const struct VgiBatch *,
                        struct VgiBatch *, struct ArrowBuffer *, int *);
int64_t ExampleCacheWorkers(void *, struct VgiFunctionCall *);
int ExampleCatalogHandler(void *, struct VgiRpcCall *, struct VgiRpcReply *, int *);
int ExampleTableExtraSupported(const char *name);
int ExampleTableExtraBind(void *, struct VgiFunctionCall *, struct ArrowSchema *);
int ExampleTableExtraInit(void *, struct VgiFunctionCall *);
void ExampleTableExtraFree(void *, struct VgiFunctionCall *);
int ExampleTableExtraProcess(void *, struct VgiFunctionCall *, const struct VgiBatch *,
                             struct VgiBatch *, struct ArrowBuffer *, int *);
int ExampleTableExtraPlan(void *, struct VgiFunctionCall *, const char *, struct VgiBatch *);
int ExampleFilterSupported(const char *);
int ExampleFilterBind(void *, struct VgiFunctionCall *, struct ArrowSchema *);
int ExampleFilterInit(void *, struct VgiFunctionCall *);
void ExampleFilterFree(void *, struct VgiFunctionCall *);
int ExampleFilterProcess(void *, struct VgiFunctionCall *, const struct VgiBatch *,
                         struct VgiBatch *, struct ArrowBuffer *, int *);
int ExampleSplitSupported(const char *);
int ExampleSplitBind(void *, struct VgiFunctionCall *, struct ArrowSchema *);
int ExampleSplitInit(void *, struct VgiFunctionCall *);
void ExampleSplitFree(void *, struct VgiFunctionCall *);
int ExampleSplitProcess(void *, struct VgiFunctionCall *, const struct VgiBatch *,
                        struct VgiBatch *, struct ArrowBuffer *, int *);
int ExampleSplitPlan(void *, struct VgiFunctionCall *, const char *, struct VgiBatch *);
int ExamplePartitionSupported(const char *);
int ExamplePartitionBind(void *, struct VgiFunctionCall *, struct ArrowSchema *);
int ExamplePartitionInit(void *, struct VgiFunctionCall *);
void ExamplePartitionFree(void *, struct VgiFunctionCall *);
int64_t ExamplePartitionWorkers(void *, struct VgiFunctionCall *);
int ExamplePartitionProcess(void *, struct VgiFunctionCall *, const struct VgiBatch *,
                            struct VgiBatch *, struct ArrowBuffer *, int *);
int ExamplePartitionPlan(void *, struct VgiFunctionCall *, const char *, struct VgiBatch *);
int ExampleStaticSupported(const char *);
int ExampleStaticBind(void *, struct VgiFunctionCall *, struct ArrowSchema *);
int ExampleStaticInit(void *, struct VgiFunctionCall *);
void ExampleStaticFree(void *, struct VgiFunctionCall *);
int64_t ExampleStaticWorkers(void *, struct VgiFunctionCall *);
int ExampleStaticProcess(void *, struct VgiFunctionCall *, const struct VgiBatch *,
                         struct VgiBatch *, struct ArrowBuffer *, int *);
int ExampleStaticPlan(void *, struct VgiFunctionCall *, const char *, struct VgiBatch *);
int ExampleFixturePaths(struct VgiBatch *, struct ArrowError *);
int ExampleCopyReader(const char *);
int ExampleCopyWriter(const char *);
int ExampleCopyBind(void *, struct VgiFunctionCall *, struct ArrowSchema *);
int ExampleCopyInit(void *, struct VgiFunctionCall *);
void ExampleCopyFree(void *, struct VgiFunctionCall *);
int ExampleCopyRead(void *, struct VgiFunctionCall *, const struct VgiBatch *, struct VgiBatch *,
                    struct ArrowBuffer *, int *);
int ExampleCopyWrite(void *, struct VgiFunctionCall *, const struct VgiBatch *,
                     struct ArrowBuffer *);
int ExampleCopyClose(void *, struct VgiFunctionCall *, struct VgiBatch *);
int ExampleCopyLookups(void *, struct VgiFunctionCall *, struct VgiBatch *);
int ExampleCopyRegister(struct VgiWorker *, const struct VgiFunction *, struct ArrowError *);
int ExampleAccumulateSupported(const char *);
int ExampleAccumulateBind(void *, struct VgiFunctionCall *, struct ArrowSchema *);
int ExampleAccumulateInit(void *, struct VgiFunctionCall *);
void ExampleAccumulateFree(void *, struct VgiFunctionCall *);
int ExampleAccumulateProcess(void *, struct VgiFunctionCall *, const struct VgiBatch *,
                             struct VgiBatch *, struct ArrowBuffer *, int *);
int ExampleAccumulateSink(void *, struct VgiFunctionCall *, const struct VgiBatch *,
                          struct ArrowBuffer *);
int ExampleAccumulateCombine(void *, struct VgiFunctionCall *, struct VgiBatch *);
int ExampleSecondaryRegister(struct VgiWorker *, struct ArrowError *);
int ExampleCancellableSupported(const char *);
int ExampleCancellableBind(void *, struct VgiFunctionCall *, struct ArrowSchema *);
int ExampleCancellableInit(void *, struct VgiFunctionCall *);
int ExampleCancellableProcess(void *, struct VgiFunctionCall *, const struct VgiBatch *,
                              struct VgiBatch *, struct ArrowBuffer *, int *);
void ExampleCancellableFinalize(void *, struct VgiFunctionCall *);
#endif
