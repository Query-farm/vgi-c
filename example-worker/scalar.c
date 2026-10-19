#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <math.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>

#include "fixtures.h"

__extension__ typedef __int128 WideInt;
__extension__ typedef unsigned __int128 WideUInt;

static int named(const struct ExampleFunction *function, const char *name) {
    return !strcmp(function->name, name);
}
int ExampleScalarSupported(const char *name) {
    if (ExampleExtraSupported(name)) return 1;
    static const char *names[] = {"upper_case",
                                  "passthru",
                                  "sha256_hex",
                                  "hash_rounds",
                                  "collatz_steps",
                                  "concat_values",
                                  "double",
                                  "add_values",
                                  "multiply",
                                  "sum_values",
                                  "argument_names_probe",
                                  "null_handling",
                                  "conditional_message",
                                  "hash_seed",
                                  "query_seed",
                                  "bernoulli",
                                  "random_int",
                                  "random_bytes",
                                  "multiply_by_setting",
                                  "scale_by_setting",
                                  "test_same_name_bind",
                                  "type_info",
                                  "pair_type",
                                  "any_mixed",
                                  "global_scalar",
                                  "whoami",
                                  "binary_packet",
                                  "geo_distance_struct",
                                  "geo_distance_list",
                                  "geo_distance_fixed",
                                  "geo_centroid_struct",
                                  "geo_centroid_list",
                                  "geo_centroid_fixed",
                                  "cached_double_scalar",
                                  "cached_add_const",
                                  "cached_label",
                                  "test_same_name_catalog",
                                  "unnest_tensor"};
    for (size_t i = 0; i < sizeof(names) / sizeof(*names); ++i)
        if (!strcmp(name, names[i])) return 1;
    return 0;
}
static int signed_width(enum ArrowType type) {
    switch (type) {
        case NANOARROW_TYPE_INT8:
            return 8;
        case NANOARROW_TYPE_INT16:
            return 16;
        case NANOARROW_TYPE_INT32:
            return 32;
        case NANOARROW_TYPE_INT64:
            return 64;
        case NANOARROW_TYPE_UINT8:
            return -8;
        case NANOARROW_TYPE_UINT16:
            return -16;
        case NANOARROW_TYPE_UINT32:
            return -32;
        case NANOARROW_TYPE_UINT64:
            return -64;
        default:
            return 0;
    }
}
static int floating(enum ArrowType type) {
    return type == NANOARROW_TYPE_HALF_FLOAT || type == NANOARROW_TYPE_FLOAT ||
           type == NANOARROW_TYPE_DOUBLE;
}
static enum ArrowType promote(enum ArrowType type) {
    switch (type) {
        case NANOARROW_TYPE_INT8:
            return NANOARROW_TYPE_INT16;
        case NANOARROW_TYPE_INT16:
            return NANOARROW_TYPE_INT32;
        case NANOARROW_TYPE_INT32:
            return NANOARROW_TYPE_INT64;
        case NANOARROW_TYPE_UINT8:
            return NANOARROW_TYPE_UINT16;
        case NANOARROW_TYPE_UINT16:
            return NANOARROW_TYPE_UINT32;
        case NANOARROW_TYPE_UINT32:
            return NANOARROW_TYPE_UINT64;
        case NANOARROW_TYPE_HALF_FLOAT:
        case NANOARROW_TYPE_FLOAT:
            return NANOARROW_TYPE_DOUBLE;
        default:
            return type;
    }
}
int ExampleScalarBind(void *data, struct VgiFunctionCall *call, struct ArrowSchema *output) {
    struct ExampleFunction *function = data;
    if (named(function, "unnest_tensor")) return ExampleTensorBind(call, output);
    if (named(function, "argument_names_probe")) {
        ArrowSchemaInit(output);
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeStruct(output, 1));
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(output->children[0], NANOARROW_TYPE_INT64));
        return ArrowSchemaSetName(output->children[0], "result");
    }
    int dynamic =
        named(function, "double") || named(function, "add_values") || named(function, "sum_values");
    if (named(function, "sum_values") && !call->input_schema.n_children)
        return VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "",
                          "sum_values requires at least 1 value");
    if (!dynamic) return ArrowSchemaDeepCopy(&function->output, output);
    const struct ArrowSchema *first =
        call->input_schema.n_children ? call->input_schema.children[0] : NULL;
    const struct ArrowSchema *second =
        call->input_schema.n_children > 1 ? call->input_schema.children[1] : NULL;
    if (first && first->dictionary) first = first->dictionary;
    if (second && second->dictionary) second = second->dictionary;
    struct ArrowSchemaView a = {0}, b = {0};
    if (first) NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&a, first, &call->rpc->error));
    if (second) NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&b, second, &call->rpc->error));
    if (first && a.type == NANOARROW_TYPE_STRING)
        return VgiRpcFail(call->rpc, "TypeError", "INVALID_ARGUMENT", "",
                          "Numeric function requires numeric arguments");
    if (first && named(function, "double") && !signed_width(a.type) && !floating(a.type) &&
        a.type != NANOARROW_TYPE_DECIMAL128 && a.type != NANOARROW_TYPE_DECIMAL256)
        return VgiRpcFail(call->rpc, "SchemaValidationError", "INVALID_ARGUMENT", "",
                          "argument 'value' does not satisfy _is_multipliable_type");
    enum ArrowType type = first ? a.type : NANOARROW_TYPE_INT64;
    int precision = a.decimal_precision, scale = a.decimal_scale;
    if (named(function, "add_values") && second && a.type != b.type) {
        if (floating(a.type) || floating(b.type))
            type = NANOARROW_TYPE_DOUBLE;
        else if (signed_width(a.type) && signed_width(b.type)) {
            int wa = signed_width(a.type), wb = signed_width(b.type);
            if ((wa < 0) != (wb < 0))
                type = wa == -64 || wb == -64 ? NANOARROW_TYPE_DOUBLE : NANOARROW_TYPE_INT64;
            else
                type = abs(wa) >= abs(wb) ? a.type : b.type;
        } else if (a.type == NANOARROW_TYPE_DECIMAL128 || b.type == NANOARROW_TYPE_DECIMAL128) {
            struct ArrowSchemaView decimal = a.type == NANOARROW_TYPE_DECIMAL128 ? a : b;
            type = NANOARROW_TYPE_DECIMAL128;
            precision = decimal.decimal_precision;
            scale = decimal.decimal_scale;
        } else
            return VgiRpcFail(call->rpc, "TypeError", "INVALID_ARGUMENT", "",
                              "No common numeric type for these arguments");
    }
    if (named(function, "add_values") && a.type == NANOARROW_TYPE_DECIMAL128 &&
        b.type == NANOARROW_TYPE_DECIMAL128) {
        scale = a.decimal_scale > b.decimal_scale ? a.decimal_scale : b.decimal_scale;
        int ai = a.decimal_precision - a.decimal_scale, bi = b.decimal_precision - b.decimal_scale;
        precision = (ai > bi ? ai : bi) + scale;
    }
    ArrowSchemaInit(output);
    int rc = ArrowSchemaSetTypeStruct(output, 1);
    if (!rc && type == NANOARROW_TYPE_DECIMAL128)
        rc = ArrowSchemaSetTypeDecimal(output->children[0], type,
                                       precision < 38 ? precision + 1 : 38, scale);
    else if (!rc && first && type == a.type && !signed_width(type) && !floating(type)) {
        ArrowSchemaRelease(output->children[0]);
        rc = ArrowSchemaDeepCopy(first, output->children[0]);
    } else if (!rc)
        rc = ArrowSchemaSetType(output->children[0], promote(type));
    if (!rc) rc = ArrowSchemaSetName(output->children[0], "result");
    if (!rc) output->children[0]->flags |= ARROW_FLAG_NULLABLE;
    return rc;
}
static uint64_t random_next(uint64_t *state) {
    uint64_t value = (*state += UINT64_C(0x9E3779B97F4A7C15));
    value = (value ^ (value >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
    value = (value ^ (value >> 27)) * UINT64_C(0x94D049BB133111EB);
    return value ^ (value >> 31);
}
static int decimal_value(const struct ArrowArrayView *array, const struct ArrowSchema *schema,
                         int64_t row, int32_t scale, WideInt *out) {
    struct ArrowSchemaView type;
    NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&type, schema, NULL));
    WideInt result;
    int32_t current_scale = 0;
    if (type.type == NANOARROW_TYPE_DECIMAL128) {
        struct ArrowDecimal decimal;
        ArrowDecimalInit(&decimal, 128, type.decimal_precision, type.decimal_scale);
        ArrowArrayViewGetDecimalUnsafe(array, row, &decimal);
        ArrowDecimalGetBytes(&decimal, (uint8_t *)&result);
        current_scale = type.decimal_scale;
    } else if (signed_width(type.type) < 0)
        result = (WideInt)ArrowArrayViewGetUIntUnsafe(array, row);
    else if (signed_width(type.type))
        result = (WideInt)ArrowArrayViewGetIntUnsafe(array, row);
    else
        return EINVAL;
    while (current_scale < scale) {
        if (__builtin_mul_overflow(result, (WideInt)10, &result)) return ERANGE;
        ++current_scale;
    }
    while (current_scale > scale) {
        result /= 10;
        --current_scale;
    }
    *out = result;
    return 0;
}
static int arithmetic(const struct ExampleFunction *function, struct VgiFunctionCall *call,
                      const struct VgiBatch *input, struct VgiBatch *output, int64_t row) {
    struct ArrowSchemaView type;
    NANOARROW_RETURN_NOT_OK(
        ArrowSchemaViewInit(&type, output->schema.children[0], &call->rpc->error));
    int64_t count =
        named(function, "double") || named(function, "multiply") ? 1 : input->array.n_children;
    if (count < 1) return EINVAL;
    for (int64_t i = 0; i < count; ++i)
        if (ArrowArrayViewIsNull(input->view.children[i], row))
            return ArrowArrayAppendNull(output->array.children[0], 1);
    if (type.type == NANOARROW_TYPE_DECIMAL128) {
        WideInt total = 0;
        for (int64_t i = 0; i < count; ++i) {
            WideInt value;
            NANOARROW_RETURN_NOT_OK(decimal_value(input->view.children[i],
                                                  input->schema.children[i], row,
                                                  type.decimal_scale, &value));
            if (__builtin_add_overflow(total, value, &total)) return ERANGE;
        }
        if (named(function, "double") && __builtin_mul_overflow(total, (WideInt)2, &total))
            return ERANGE;
        WideUInt maximum = 1;
        for (int i = 0; i < type.decimal_precision; ++i) maximum *= 10;
        WideUInt absolute = total < 0 ? (WideUInt)(-(total + 1)) + 1 : (WideUInt)total;
        if (absolute >= maximum) {
            ArrowErrorSet(&call->rpc->error, "Decimal result does not fit in precision %d",
                          type.decimal_precision);
            call->rpc->error_type = "ValueError";
            call->rpc->error_code = "INVALID_ARGUMENT";
            return ERANGE;
        }
        struct ArrowDecimal decimal;
        ArrowDecimalInit(&decimal, 128, type.decimal_precision, type.decimal_scale);
        ArrowDecimalSetBytes(&decimal, (const uint8_t *)&total);
        return ArrowArrayAppendDecimal(output->array.children[0], &decimal);
    }
    if (floating(type.type)) {
        double total = 0;
        for (int64_t i = 0; i < count; ++i)
            total += ArrowArrayViewGetDoubleUnsafe(input->view.children[i], row);
        if (named(function, "double")) total *= 2;
        return ArrowArrayAppendDouble(output->array.children[0], total);
    }
    uint64_t total = 0;
    for (int64_t i = 0; i < count; ++i)
        total += ArrowArrayViewGetUIntUnsafe(input->view.children[i], row);
    if (named(function, "double")) total *= 2;
    if (named(function, "multiply")) total *= (uint64_t)VgiArgumentInt(call, 1, "factor", 1);
    return signed_width(type.type) < 0
               ? ArrowArrayAppendUInt(output->array.children[0], total)
               : ArrowArrayAppendInt(output->array.children[0], (int64_t)total);
}
static const char *type_label(const struct ArrowSchema *schema) {
    if (!schema) return "unknown";
    if (!strcmp(schema->format, "i")) return "int32";
    if (!strcmp(schema->format, "l")) return "int64";
    if (!strcmp(schema->format, "I")) return "uint32";
    if (!strcmp(schema->format, "L")) return "uint64";
    if (!strcmp(schema->format, "u")) return "varchar";
    return "unknown";
}
static int point(const struct ArrowArrayView *value, const struct ArrowSchema *schema, int64_t row,
                 double out[2]) {
    if (ArrowArrayViewIsNull(value, row)) return 0;
    for (int coordinate = 0; coordinate < 2; ++coordinate) {
        const struct ArrowArrayView *part = NULL;
        const struct ArrowSchema *part_schema = NULL;
        int64_t index;
        if (value->storage_type == NANOARROW_TYPE_STRUCT) {
            int64_t field = VgiFieldIndex(schema, coordinate ? "lon" : "lat");
            if (field < 0) return 0;
            part = value->children[field];
            part_schema = schema->children[field];
            index = row + value->offset;
        } else {
            int64_t start, end;
            if (value->storage_type == NANOARROW_TYPE_FIXED_SIZE_LIST) {
                start = (row + value->offset) * 2;
                end = start + 2;
            } else if (value->storage_type == NANOARROW_TYPE_LIST ||
                       value->storage_type == NANOARROW_TYPE_LARGE_LIST) {
                start = ArrowArrayViewListChildOffset(value, row);
                end = ArrowArrayViewListChildOffset(value, row + 1);
            } else
                return 0;
            if (end - start < 2) return 0;
            part = value->children[0];
            part_schema = schema->children[0];
            index = start + coordinate;
        }
        if (ArrowArrayViewIsNull(part, index)) return 0;
        if (VgiValueIsNumeric(part))
            out[coordinate] = ArrowArrayViewGetDoubleUnsafe(part, index);
        else {
            struct ArrowSchemaView type;
            if (ArrowSchemaViewInit(&type, part_schema, NULL) ||
                type.type != NANOARROW_TYPE_DECIMAL128)
                return 0;
            WideInt integer;
            if (decimal_value(part, part_schema, index, type.decimal_scale, &integer)) return 0;
            out[coordinate] = (double)integer / pow(10, type.decimal_scale);
        }
    }
    return 1;
}
int ExampleScalarProcess(void *data, struct VgiFunctionCall *call, const struct VgiBatch *input,
                         struct VgiBatch *output, struct ArrowBuffer *metadata, int *finished) {
    (void)finished;
    struct ExampleFunction *function = data;
    if (ExampleExtraSupported(function->name))
        return ExampleExtraProcess(function, call, input, output, metadata);
    int rc = VgiBatchInit(output, &call->output_schema, &call->rpc->error);
    if (rc) return rc;
    if (!strncmp(function->name, "cached_", 7)) {
        rc = VgiMetadataSet(metadata, "vgi.cache.ttl", "300");
        if (!rc) rc = VgiMetadataSet(metadata, "vgi.cache.per_value", "1");
        if (!rc) rc = VgiMetadataSet(metadata, "vgi.cache.scope", "catalog");
    }
    struct ArrowArray *out = output->array.children[0];
    const struct ArrowArrayView *first = input->view.n_children ? input->view.children[0] : NULL;
    struct ArrowBuffer text;
    ArrowBufferInit(&text);
    uint64_t random_state = (uint64_t)VgiArgumentInt(call, 0, "seed", 0);
    if (named(function, "bernoulli") || named(function, "random_int"))
        randombytes_buf(&random_state, sizeof(random_state));
    int numeric = named(function, "double") || named(function, "add_values") ||
                  named(function, "multiply") || named(function, "sum_values");
    for (int64_t row = 0; !rc && row < input->array.length; ++row) {
        int null = first && ArrowArrayViewIsNull(first, row);
        text.size_bytes = 0;
        if (named(function, "unnest_tensor"))
            rc = ExampleTensorRow(call, input, output, row);
        else if (numeric)
            rc = arithmetic(function, call, input, output, row);
        else if (named(function, "null_handling"))
            rc = ArrowArrayAppendInt(out, null ? -5000 : ArrowArrayViewGetIntUnsafe(first, row));
        else if (named(function, "bernoulli"))
            rc = ArrowArrayAppendInt(out, (int64_t)(random_next(&random_state) & 1));
        else if (named(function, "hash_seed"))
            rc = ArrowArrayAppendInt(
                out, (int64_t)((uint64_t)VgiArgumentInt(call, 0, "seed", 0) + (uint64_t)row));
        else if (named(function, "random_bytes")) {
            int64_t size = VgiArgumentInt(call, 1, "byte_length", 0);
            if (size < 0) {
                rc = VgiRpcFail(call->rpc, "ValueError", "INVALID_ARGUMENT", "",
                                "byte_length must be >= 0");
                break;
            }
            rc = ArrowBufferResize(&text, size, 0);
            for (int64_t i = 0; !rc && i < size; ++i)
                text.data[i] = (uint8_t)random_next(&random_state);
            if (!rc)
                rc = ArrowArrayAppendBytes(out,
                                           (struct ArrowBufferView){{text.data}, text.size_bytes});
        } else if (named(function, "conditional_message")) {
            struct ArrowStringView message = VgiArgumentString(call, 1, "message");
            int64_t count = VgiArgumentInt(call, 0, "repeat_count", 0);
            if (!null && ArrowArrayViewGetIntUnsafe(first, row))
                for (int64_t i = 0; !rc && i < count; ++i)
                    rc = ArrowBufferAppend(&text, message.data, message.size_bytes);
            if (!rc)
                rc = ArrowArrayAppendString(
                    out, (struct ArrowStringView){(const char *)text.data, text.size_bytes});
        } else if (named(function, "binary_packet")) {
            struct ArrowStringView header = VgiArgumentString(call, 0, "header"),
                                   payload = VgiValueString(first, row);
            const struct ArrowArrayView *config = VgiArgument(call, 2, "config");
            struct ArrowStringView label = config && config->n_children >= 2
                                               ? VgiValueString(config->children[0], 0)
                                               : (struct ArrowStringView){NULL, 0};
            uint8_t version =
                config && config->n_children >= 2 && !ArrowArrayViewIsNull(config->children[1], 0)
                    ? (uint8_t)ArrowArrayViewGetIntUnsafe(config->children[1], 0)
                    : 0;
            rc = ArrowBufferAppend(&text, header.data, header.size_bytes);
            if (!rc) rc = ArrowBufferAppend(&text, payload.data, payload.size_bytes);
            if (!rc) rc = ArrowBufferAppend(&text, label.data, label.size_bytes);
            if (!rc) rc = ArrowBufferAppend(&text, &version, 1);
            if (!rc)
                rc = ArrowArrayAppendBytes(out,
                                           (struct ArrowBufferView){{text.data}, text.size_bytes});
        } else if (named(function, "whoami"))
            rc = ArrowArrayAppendString(
                out, ArrowCharView(call->rpc->principal ? call->rpc->principal : "anonymous"));
        else if (null)
            rc = ArrowArrayAppendNull(out, 1);
        else if (named(function, "cached_double_scalar") || named(function, "cached_add_const")) {
            uint64_t value = (uint64_t)ArrowArrayViewGetIntUnsafe(first, row);
            value = named(function, "cached_double_scalar")
                        ? value * 2
                        : value + (uint64_t)VgiArgumentInt(call, 1, "addend", 0);
            rc = ArrowArrayAppendInt(out, (int64_t)value);
        } else if (named(function, "cached_label")) {
            int64_t value = ArrowArrayViewGetIntUnsafe(first, row);
            char label[64];
            snprintf(label, sizeof(label), "lbl-%" PRId64, value);
            rc = value < 0 ? ArrowArrayAppendNull(out, 1)
                           : ArrowArrayAppendString(out, ArrowCharView(label));
        } else if (!strncmp(function->name, "geo_", 4)) {
            double first_point[2] = {0, 0}, last_point[2] = {0, 0}, total[2] = {0, 0};
            int valid = input->view.n_children > 0;
            for (int64_t i = 0; valid && i < input->view.n_children; ++i) {
                valid = point(input->view.children[i], input->schema.children[i], row, last_point);
                if (!i) {
                    first_point[0] = last_point[0];
                    first_point[1] = last_point[1];
                }
                total[0] += last_point[0];
                total[1] += last_point[1];
            }
            if (!valid)
                rc = ArrowArrayAppendNull(out, 1);
            else if (!strncmp(function->name, "geo_distance", 12))
                rc = ArrowArrayAppendDouble(
                    out, hypot(last_point[0] - first_point[0], last_point[1] - first_point[1]));
            else {
                rc = ArrowArrayAppendDouble(out->children[0],
                                            total[0] / (double)input->view.n_children);
                if (!rc)
                    rc = ArrowArrayAppendDouble(out->children[1],
                                                total[1] / (double)input->view.n_children);
                if (!rc) rc = ArrowArrayFinishElement(out);
            }
        } else if (named(function, "upper_case") || named(function, "passthru")) {
            struct ArrowStringView value = VgiValueString(first, row);
            if (!value.data) {
                rc = EINVAL;
                break;
            }
            rc = ArrowBufferAppend(&text, value.data, value.size_bytes);
            if (named(function, "upper_case"))
                for (int64_t i = 0; i < text.size_bytes; ++i)
                    text.data[i] = (uint8_t)toupper(text.data[i]);
            if (!rc)
                rc = ArrowArrayAppendString(
                    out, (struct ArrowStringView){(const char *)text.data, text.size_bytes});
        } else if (named(function, "sha256_hex") || named(function, "hash_rounds")) {
            struct ArrowStringView value = VgiValueString(first, row);
            int64_t rounds =
                named(function, "sha256_hex") ? 1 : VgiArgumentInt(call, 1, "rounds", 1);
            if (!value.data) {
                rc = EINVAL;
                break;
            }
            unsigned char digest[crypto_hash_sha256_BYTES];
            for (int64_t i = 0; i < rounds; ++i) {
                crypto_hash_sha256(digest, (const unsigned char *)value.data,
                                   (unsigned long long)value.size_bytes);
                value = (struct ArrowStringView){(const char *)digest, sizeof(digest)};
            }
            if (value.size_bytes > (INT64_MAX - 1) / 2) {
                rc = EOVERFLOW;
                break;
            }
            rc = ArrowBufferResize(&text, value.size_bytes * 2 + 1, 0);
            if (!rc)
                sodium_bin2hex((char *)text.data, (size_t)text.size_bytes,
                               (const unsigned char *)value.data, (size_t)value.size_bytes);
            if (!rc)
                rc = ArrowArrayAppendString(
                    out, (struct ArrowStringView){(const char *)text.data, text.size_bytes - 1});
        } else if (named(function, "collatz_steps")) {
            WideInt value = ArrowArrayViewGetIntUnsafe(first, row);
            int64_t steps = 0;
            while (value > 1) {
                value = value % 2 ? 3 * value + 1 : value / 2;
                ++steps;
            }
            rc = ArrowArrayAppendInt(out, steps);
        } else if (named(function, "query_seed") || named(function, "multiply_by_setting")) {
            uint64_t value = (uint64_t)ArrowArrayViewGetIntUnsafe(first, row);
            value = named(function, "query_seed")
                        ? value + 1000
                        : value * (uint64_t)VgiBatchInt(&call->settings, "multiplier", 1);
            rc = ArrowArrayAppendInt(out, (int64_t)value);
        } else if (named(function, "scale_by_setting")) {
            const struct ArrowArrayView *scale = VgiBatchGet(&call->settings, "scale_factor");
            rc = ArrowArrayAppendDouble(
                out, ArrowArrayViewGetDoubleUnsafe(first, row) *
                         (scale && scale->length && !ArrowArrayViewIsNull(scale, 0)
                              ? ArrowArrayViewGetDoubleUnsafe(scale, 0)
                              : 1));
        } else if (named(function, "argument_names_probe")) {
            const struct ArrowArrayView *right = input->view.children[1];
            rc = ArrowArrayViewIsNull(right, row)
                     ? ArrowArrayAppendNull(out, 1)
                     : ArrowArrayAppendInt(
                           out, (int64_t)(((uint64_t)ArrowArrayViewGetIntUnsafe(first, row) +
                                           (uint64_t)ArrowArrayViewGetIntUnsafe(right, row)) *
                                          (uint64_t)VgiArgumentInt(call, 2, "scale", 2)));
        } else if (named(function, "random_int")) {
            const struct ArrowArrayView *right = input->view.children[1];
            int64_t low = ArrowArrayViewGetIntUnsafe(first, row),
                    high = ArrowArrayViewGetIntUnsafe(right, row);
            WideUInt width = (WideUInt)((WideInt)high - (WideInt)low + 1);
            rc =
                ArrowArrayViewIsNull(right, row)
                    ? ArrowArrayAppendNull(out, 1)
                    : ArrowArrayAppendInt(
                          out,
                          high <= low
                              ? low
                              : (int64_t)((WideInt)low +
                                          (WideInt)((WideUInt)random_next(&random_state) % width)));
        } else if (named(function, "concat_values")) {
            uint64_t sum = 0;
            int integer = VgiValueIsNumeric(first);
            for (int64_t i = 0; !rc && i < input->view.n_children; ++i) {
                const struct ArrowArrayView *column = input->view.children[i];
                if (ArrowArrayViewIsNull(column, row)) {
                    null = 1;
                    break;
                }
                if (integer)
                    sum += (uint64_t)ArrowArrayViewGetIntUnsafe(column, row);
                else {
                    struct ArrowStringView value = VgiValueString(column, row);
                    rc = value.data ? ArrowBufferAppend(&text, value.data, value.size_bytes)
                                    : EINVAL;
                }
            }
            if (integer && !null) {
                char digits[32];
                snprintf(digits, sizeof(digits), "%" PRId64, (int64_t)sum);
                rc = ArrowBufferAppend(&text, digits, (int64_t)strlen(digits));
            }
            if (!rc)
                rc = null ? ArrowArrayAppendNull(out, 1)
                          : ArrowArrayAppendString(
                                out,
                                (struct ArrowStringView){(const char *)text.data, text.size_bytes});
        } else if (named(function, "test_same_name_bind") ||
                   named(function, "test_same_name_catalog") || named(function, "global_scalar")) {
            char value[320];
            snprintf(value, sizeof(value), "%s:%" PRId64,
                     named(function, "global_scalar")            ? "global_scalar"
                     : named(function, "test_same_name_catalog") ? function->catalog
                                                                 : function->path,
                     ArrowArrayViewGetIntUnsafe(first, row));
            rc = ArrowArrayAppendString(out, ArrowCharView(value));
        } else if (named(function, "type_info"))
            rc = ArrowArrayAppendString(out,
                                        ArrowCharView(type_label(function->arguments.children[0])));
        else if (named(function, "pair_type")) {
            int a_string = !strcmp(function->arguments.children[0]->format, "u"),
                b_string = !strcmp(function->arguments.children[1]->format, "u");
            char label[16];
            snprintf(label, sizeof(label), "%s+%s", a_string ? "str" : "int",
                     b_string ? "str" : "int");
            rc = ArrowArrayAppendString(out, ArrowCharView(label));
        } else if (named(function, "any_mixed")) {
            const struct ArrowArrayView *right = input->view.children[1];
            if (ArrowArrayViewIsNull(right, row))
                rc = ArrowArrayAppendNull(out, 1);
            else {
                struct ArrowStringView value = VgiValueString(right, row);
                if (value.data) {
                    rc = ArrowBufferAppend(&text, "any+str: ", 9);
                    if (!rc) rc = ArrowBufferAppend(&text, value.data, value.size_bytes);
                } else {
                    char label[64];
                    snprintf(label, sizeof(label), "any+int: %" PRId64,
                             ArrowArrayViewGetIntUnsafe(right, row));
                    rc = ArrowBufferAppend(&text, label, (int64_t)strlen(label));
                }
                if (!rc)
                    rc = ArrowArrayAppendString(
                        out, (struct ArrowStringView){(const char *)text.data, text.size_bytes});
            }
        } else
            rc = ENOTSUP;
        if (!rc) rc = ArrowArrayFinishElement(&output->array);
    }
    if (!rc) rc = VgiBatchFinish(output, &call->rpc->error);
    ArrowBufferReset(&text);
    return rc;
}
