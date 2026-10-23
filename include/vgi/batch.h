#ifndef VGI_COMPAT_BATCH_H
#define VGI_COMPAT_BATCH_H
#include "vgi/generated/protocol.h"
#include "vgi_rpc/batch.h"
#ifdef __cplusplus
extern "C" {
#endif
int VgiBatchInitNamed(struct VgiBatch *, const char *, struct ArrowError *);
/* Nonzero when a value's Arrow type is a number: an integer, floating point,
 * boolean or decimal (temporal types are read through their integer storage). */
int VgiValueIsNumber(const struct ArrowArrayView *value);
/* Read one value as a double. Numbers and decimals convert, text is parsed;
 * any other type (nested, interval, union, ...) fails with EINVAL. Prefer this
 * to VgiValueDouble, which must only be handed numbers and decimals. */
int VgiValueNumber(const struct ArrowArrayView *value, const struct ArrowSchema *schema,
                   int64_t index, double *out, struct ArrowError *error);
#ifdef __cplusplus
}
#endif
#endif
