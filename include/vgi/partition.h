#ifndef VGI_PARTITION_H
#define VGI_PARTITION_H
#include "vgi/rpc.h"
#ifdef __cplusplus
extern "C" {
#endif
int VgiPartitionField(struct ArrowSchema *field);
/* Write the min/max of marked columns as a two-row IPC batch in message
 * metadata. Empty inputs leave metadata untouched; NULLs are ignored. */
int VgiPartitionMetadata(const struct ArrowSchema *full_schema, const struct VgiBatch *batch,
                         struct ArrowBuffer *metadata, struct ArrowError *error);
#ifdef __cplusplus
}
#endif
#endif
