#ifndef VGI_CONTEXT_H
#define VGI_CONTEXT_H
#include "vgi/worker.h"
#ifdef __cplusplus
extern "C" {
#endif
/* Select by type and, when supplied, the longest matching newline-separated
 * scope prefix. Returned values are borrowed from the call. */
const struct ArrowArrayView *VgiSecretSelect(const struct VgiFunctionCall *call, const char *type,
                                             const struct ArrowStringView *path,
                                             const struct ArrowSchema **schema);
const struct ArrowArrayView *VgiStructField(const struct ArrowArrayView *value,
                                            const struct ArrowSchema *schema, const char *name);
int VgiValueRender(const struct ArrowArrayView *value, const struct ArrowSchema *schema,
                   int64_t row, struct ArrowBuffer *text, struct ArrowError *error);
#ifdef __cplusplus
}
#endif
#endif
