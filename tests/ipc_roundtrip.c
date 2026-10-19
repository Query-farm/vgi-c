#include <stdio.h>

#include "vgi/ipc.h"

int main(int argc, char **argv) {
    if (argc != 3) return 2;
    FILE *file = fopen(argv[1], "rb");
    if (!file) return 2;
    struct ArrowBuffer input, output, metadata;
    ArrowBufferInit(&input);
    ArrowBufferInit(&output);
    ArrowBufferInit(&metadata);
    unsigned char chunk[8192];
    size_t n;
    int result = 0;
    while ((n = fread(chunk, 1, sizeof(chunk), file)) > 0) {
        result = ArrowBufferAppend(&input, chunk, (int64_t)n);
        if (result) break;
    }
    if (ferror(file)) result = 1;
    fclose(file);
    struct ArrowSchema schema = {0};
    struct ArrowArray array = {0};
    struct ArrowError error = {{0}};
    struct ArrowBufferView view = {{input.data}, input.size_bytes};
    if (!result) result = VgiIpcReadBatch(view, &schema, &array, &metadata, &error);
    if (!result)
        result = VgiIpcWriteBatch(&schema, &array, (const char *)metadata.data, &output, &error);
    if (!result) {
        file = fopen(argv[2], "wb");
        if (!file)
            result = 1;
        else {
            if (fwrite(output.data, 1, (size_t)output.size_bytes, file) !=
                (size_t)output.size_bytes)
                result = 1;
            if (fclose(file)) result = 1;
        }
    }
    if (result) fprintf(stderr, "IPC error %d: %s\n", result, error.message);
    if (array.release) ArrowArrayRelease(&array);
    if (schema.release) ArrowSchemaRelease(&schema);
    ArrowBufferReset(&input);
    ArrowBufferReset(&output);
    ArrowBufferReset(&metadata);
    return result ? 1 : 0;
}
