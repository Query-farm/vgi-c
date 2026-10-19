#ifndef VGI_COPY_H
#define VGI_COPY_H
#include "vgi/worker.h"
#ifdef __cplusplus
extern "C" {
#endif
/* Register a generated CopyFromFormatInfo. Both COPY directions use this schema.
 * The referenced handler is an ordinary TABLE (from) or BUFFERING (to) function.
 * Inputs are copied; format names must be unique. A shared handler may advertise
 * direction="both". Buffered writers close in buffer_combine and return an empty
 * finalize_state_ids list. */
int VgiWorkerRegisterCopyFormat(struct VgiWorker *, const struct VgiBatch *, struct ArrowError *);
/* Borrowed path from BindRequest's COPY context, or an absent view for ordinary
 * SQL calls. Direction is "from" or "to". */
struct ArrowStringView VgiCopyPath(const struct VgiFunctionCall *, const char *direction);
/* COPY FROM binds exactly the target schema supplied by the engine. */
int VgiCopyFromSchema(const struct VgiFunctionCall *, struct ArrowSchema *, struct ArrowError *);
#ifdef __cplusplus
}
#endif
#endif
