#include "vgi/copy.h"

#include <errno.h>
#include <string.h>

#include "vgi/context.h"
static const struct ArrowArrayView *context_field(const struct VgiFunctionCall *call,
                                                  const char *direction, const char *name) {
    const char *field = !strcmp(direction, "from") ? "copy_from"
                        : !strcmp(direction, "to") ? "copy_to"
                                                   : NULL;
    if (!field) return NULL;
    int64_t i = VgiFieldIndex(&call->bind_request.schema, field);
    if (i < 0) return NULL;
    return VgiStructField(call->bind_request.view.children[i],
                          call->bind_request.schema.children[i], name);
}
struct ArrowStringView VgiCopyPath(const struct VgiFunctionCall *call, const char *direction) {
    return VgiValueString(context_field(call, direction, "file_path"), 0);
}
int VgiCopyFromSchema(const struct VgiFunctionCall *call, struct ArrowSchema *out,
                      struct ArrowError *error) {
    struct ArrowStringView bytes =
        VgiValueString(context_field(call, "from", "expected_schema"), 0);
    if (!bytes.data) {
        ArrowErrorSet(error, "This handler is a COPY FROM format reader; invoke it via COPY FROM");
        return EINVAL;
    }
    return VgiIpcReadSchema((struct ArrowBufferView){{bytes.data}, bytes.size_bytes}, out, error);
}
