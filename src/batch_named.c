#include "vgi/batch.h"

int VgiBatchInitNamed(struct VgiBatch *batch, const char *name, struct ArrowError *error) {
    struct ArrowSchema schema = {0};
    int rc = VgiSchemaInit(name, &schema, error);
    if (!rc) rc = VgiBatchInit(batch, &schema, error);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}
