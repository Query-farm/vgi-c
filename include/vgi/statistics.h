#ifndef VGI_STATISTICS_H
#define VGI_STATISTICS_H
#include "vgi/batch.h"
#ifdef __cplusplus
extern "C" {
#endif
enum VgiStatKind { VGI_STAT_INT64, VGI_STAT_DOUBLE, VGI_STAT_STRING, VGI_STAT_BINARY };
struct VgiStatValue {
    enum VgiStatKind kind;
    int present;
    int64_t integer;
    double real;
    struct ArrowStringView bytes;
};
struct VgiColumnStatistics {
    const char *name;
    struct VgiStatValue min, max;
    int has_null, has_not_null;
    /* -1 denotes an unknown count/length; contains_unicode is -1, 0, or 1. */
    int64_t distinct_count, max_string_length;
    int contains_unicode;
};
int VgiStatisticsInit(struct VgiBatch *batch, struct ArrowError *error);
int VgiStatisticsAppend(struct VgiBatch *batch, const struct VgiColumnStatistics *column,
                        struct ArrowError *error);
#ifdef __cplusplus
}
#endif
#endif
