#include <stdio.h>

#include "vgi/generated/protocol.h"

int main(void) {
    printf("%s %s\nProtocol hash: %s\n%zu schemas, %zu methods\n", VGI_PROTOCOL_NAME,
           VGI_PROTOCOL_VERSION, VGI_PROTOCOL_HASH, VgiSchemaCount(), VGI_METHOD_COUNT);
    for (size_t i = 0; i < VGI_METHOD_COUNT; ++i) {
        const struct VgiMethod *method = &VGI_METHODS[i];
        const char *kind = method->kind == VGI_METHOD_STREAM ? "stream"
                           : method->kind == VGI_METHOD_VOID ? "void"
                                                             : "unary";
        printf("%-7s %s\n", kind, method->name);
    }
    return 0;
}
