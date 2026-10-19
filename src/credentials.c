#include "vgi/credentials.h"

#include <errno.h>
#include <math.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>

static const char *text(const char *s) { return s ? s : ""; }
void VgiAttachTicketReset(struct VgiAttachTicket *c) {
    free(c->ticket_id);
    free(c->catalog_name);
    free(c->data_version_spec);
    free(c->implementation_version);
    if (c->options.data) sodium_memzero(c->options.data, (size_t)c->options.size_bytes);
    if (c->options.data) ArrowBufferReset(&c->options);
    memset(c, 0, sizeof(*c));
}
static int utf8(const unsigned char *s, size_t n) {
    for (size_t i = 0; i < n;) {
        unsigned x = s[i++];
        if (x < 0x80) {
            if (!x) return 0;
            continue;
        }
        unsigned need, minimum;
        if (x >= 0xc2 && x <= 0xdf) {
            need = 1;
            minimum = 0x80;
            x &= 31;
        } else if (x >= 0xe0 && x <= 0xef) {
            need = 2;
            minimum = 0x800;
            x &= 15;
        } else if (x >= 0xf0 && x <= 0xf4) {
            need = 3;
            minimum = 0x10000;
            x &= 7;
        } else
            return 0;
        if (n - i < need) return 0;
        while (need--) {
            unsigned c = s[i++];
            if ((c & 0xc0) != 0x80) return 0;
            x = (x << 6) | (c & 63);
        }
        if (x < minimum || x > 0x10ffff || (x >= 0xd800 && x <= 0xdfff)) return 0;
    }
    return 1;
}
static int put(struct ArrowBuffer *b, uint64_t x, int n) {
    unsigned char out[8];
    for (int i = 0; i < n; ++i) {
        out[i] = (unsigned char)x;
        x >>= 8;
    }
    return ArrowBufferAppend(b, out, n);
}
static int put_text(struct ArrowBuffer *b, const char *s) {
    s = text(s);
    size_t n = strlen(s);
    if (n > 65535 || !utf8((const unsigned char *)s, n)) return EINVAL;
    NANOARROW_RETURN_NOT_OK(put(b, n, 2));
    return ArrowBufferAppend(b, s, (int64_t)n);
}
struct Reader {
    const unsigned char *p;
    size_t n;
    int rc;
};
static uint64_t take(struct Reader *r, int n) {
    if (r->rc || r->n < (size_t)n) {
        r->rc = EINVAL;
        return 0;
    }
    uint64_t out = 0;
    for (int i = 0; i < n; ++i) out |= (uint64_t)r->p[i] << (8 * i);
    r->p += n;
    r->n -= (size_t)n;
    return out;
}
static char *take_text(struct Reader *r) {
    size_t n = (size_t)take(r, 2);
    if (r->rc || n > r->n || !utf8(r->p, n)) {
        r->rc = EINVAL;
        return NULL;
    }
    char *s = malloc(n + 1);
    if (!s) {
        r->rc = ENOMEM;
        return NULL;
    }
    memcpy(s, r->p, n);
    s[n] = 0;
    r->p += n;
    r->n -= n;
    return s;
}
static int aad(struct ArrowBuffer *b, const char *domain, const void *extra, size_t n,
               const char *subject) {
    NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(b, domain, (int64_t)strlen(domain) + 1));
    if (n) NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(b, extra, (int64_t)n));
    return ArrowBufferAppend(b, text(subject), (int64_t)strlen(text(subject)));
}
static int seal(const unsigned char key[32], const char *prefix, const unsigned char *id,
                const struct ArrowBuffer *payload, const struct ArrowBuffer *ad,
                const unsigned char *nonce, size_t max, struct ArrowBuffer *out) {
    if (sodium_init() < 0) return EIO;
    struct ArrowBuffer raw;
    ArrowBufferInit(&raw);
    size_t head = id ? 9 : 1;
    int rc = ArrowBufferResize(&raw, (int64_t)(head + 24 + 16) + payload->size_bytes, 0);
    if (!rc) {
        if (id) memcpy(raw.data, id, 8);
        raw.data[head - 1] = 1;
        if (nonce)
            memcpy(raw.data + head, nonce, 24);
        else
            randombytes_buf(raw.data + head, 24);
        if (crypto_aead_xchacha20poly1305_ietf_encrypt(
                raw.data + head + 24, NULL, payload->data, (unsigned long long)payload->size_bytes,
                ad->data, (unsigned long long)ad->size_bytes, NULL, raw.data + head, key))
            rc = EINVAL;
    }
    size_t n = !rc ? sodium_base64_ENCODED_LEN((size_t)raw.size_bytes,
                                               sodium_base64_VARIANT_URLSAFE_NO_PADDING)
                   : 0;
    if (!rc && strlen(prefix) + n - 1 > max) rc = EINVAL;
    if (!rc) rc = ArrowBufferResize(out, (int64_t)(strlen(prefix) + n), 0);
    if (!rc) {
        memcpy(out->data, prefix, strlen(prefix));
        sodium_bin2base64((char *)out->data + strlen(prefix), n, raw.data, (size_t)raw.size_bytes,
                          sodium_base64_VARIANT_URLSAFE_NO_PADDING);
        --out->size_bytes;
    }
    ArrowBufferReset(&raw);
    return rc;
}
static int raw_token(const char *token, const char *prefix, size_t max, struct ArrowBuffer *out) {
    if (!token || strncmp(token, prefix, strlen(prefix)) || strlen(token) > max) return EINVAL;
    const char *p = token + strlen(prefix);
    size_t n = strlen(p);
    if (!n) return EINVAL;
    NANOARROW_RETURN_NOT_OK(ArrowBufferResize(out, (int64_t)n, 0));
    size_t size = 0;
    const char *end = NULL;
    if (sodium_base642bin(out->data, n, p, n, NULL, &size, &end,
                          sodium_base64_VARIANT_URLSAFE_NO_PADDING) ||
        end != p + n)
        return EINVAL;
    out->size_bytes = (int64_t)size;
    return 0;
}
static int open_payload(const unsigned char key[32], const struct ArrowBuffer *raw, size_t head,
                        const struct ArrowBuffer *ad, struct ArrowBuffer *out) {
    if (raw->size_bytes < (int64_t)(head + 24 + 16) || raw->data[head - 1] != 1) return EINVAL;
    NANOARROW_RETURN_NOT_OK(ArrowBufferResize(out, raw->size_bytes - (int64_t)(head + 24 + 16), 0));
    return crypto_aead_xchacha20poly1305_ietf_decrypt(
               out->data, NULL, NULL, raw->data + head + 24,
               (unsigned long long)raw->size_bytes - head - 24, ad->data,
               (unsigned long long)ad->size_bytes, raw->data + head, key)
               ? EINVAL
               : 0;
}
static int valid_ticket(const struct VgiAttachTicket *c) {
    if (!c->ticket_id || strlen(c->ticket_id) != 32 || !*text(c->catalog_name) ||
        (c->expires_at && c->expires_at <= c->issued_at) || c->options.size_bytes < 0 ||
        c->options.size_bytes > 16384)
        return 0;
    for (int i = 0; i < 32; ++i)
        if (!((c->ticket_id[i] >= '0' && c->ticket_id[i] <= '9') ||
              (c->ticket_id[i] >= 'a' && c->ticket_id[i] <= 'f')))
            return 0;
    return 1;
}
int VgiAttachTicketMint(const unsigned char key[32], const char *principal,
                        const struct VgiAttachTicket *c, const unsigned char nonce[24],
                        struct ArrowBuffer *out) {
    if (!key || !principal || !*principal || !c || !valid_ticket(c)) return EINVAL;
    struct ArrowBuffer p, a;
    ArrowBufferInit(&p);
    ArrowBufferInit(&a);
    int rc = put(&p, (uint64_t)c->issued_at, 8);
    if (!rc) rc = put(&p, (uint64_t)c->expires_at, 8);
    const char *fields[] = {c->ticket_id, c->catalog_name, c->data_version_spec,
                            c->implementation_version};
    for (int i = 0; !rc && i < 4; ++i) rc = put_text(&p, fields[i]);
    if (!rc) rc = put(&p, (uint64_t)c->options.size_bytes, 4);
    if (!rc) rc = ArrowBufferAppend(&p, c->options.data, c->options.size_bytes);
    if (!rc) rc = aad(&a, "vgi.attach_ticket.v1", NULL, 0, principal);
    if (!rc) rc = seal(key, "vgia1.", NULL, &p, &a, nonce, 32768, out);
    if (p.data) sodium_memzero(p.data, (size_t)p.size_bytes);
    ArrowBufferReset(&p);
    ArrowBufferReset(&a);
    return rc;
}
int VgiAttachTicketOpen(const unsigned char key[32], const char *principal, const char *token,
                        double now, struct VgiAttachTicket *out) {
    if (!key || !principal || !*principal || !isfinite(now)) return EINVAL;
    struct ArrowBuffer raw, p, a;
    ArrowBufferInit(&raw);
    ArrowBufferInit(&p);
    ArrowBufferInit(&a);
    struct VgiAttachTicket c = {0};
    ArrowBufferInit(&c.options);
    int rc = raw_token(token, "vgia1.", 32768, &raw);
    if (!rc) rc = aad(&a, "vgi.attach_ticket.v1", NULL, 0, principal);
    if (!rc) rc = open_payload(key, &raw, 1, &a, &p);
    if (!rc) {
        struct Reader r = {p.data, (size_t)p.size_bytes, 0};
        c.issued_at = (int64_t)take(&r, 8);
        c.expires_at = (int64_t)take(&r, 8);
        c.ticket_id = take_text(&r);
        c.catalog_name = take_text(&r);
        c.data_version_spec = take_text(&r);
        c.implementation_version = take_text(&r);
        uint64_t n = take(&r, 4);
        rc = r.rc;
        if (!rc && (n != r.n || n > 16384)) rc = EINVAL;
        if (!rc) rc = ArrowBufferAppend(&c.options, r.p, (int64_t)n);
        if (!rc && !valid_ticket(&c)) rc = EINVAL;
    }
    if (!rc &&
        ((double)c.issued_at > now + 60 || (c.expires_at && now >= (double)c.expires_at + 60)))
        rc = ESTALE;
    if (!rc) {
        *out = c;
        memset(&c, 0, sizeof(c));
    }
    VgiAttachTicketReset(&c);
    if (p.data) sodium_memzero(p.data, (size_t)p.size_bytes);
    ArrowBufferReset(&raw);
    ArrowBufferReset(&p);
    ArrowBufferReset(&a);
    return rc;
}
