#ifndef VGI_CREDENTIALS_H
#define VGI_CREDENTIALS_H
#include "vgi_rpc/credentials.h"
#ifdef __cplusplus
extern "C" {
#endif
/* All outputs own their storage. Initialize claims to zero and ArrowBuffers
 * with ArrowBufferInit; reset them after use. Tokens include a terminal NUL
 * outside size_bytes. Open returns EINVAL for invalid credentials, ESTALE for
 * authentic expired/not-yet-valid credentials, or ENOMEM. */
struct VgiAttachTicket {
    int64_t issued_at, expires_at;
    char *ticket_id, *catalog_name, *data_version_spec, *implementation_version;
    struct ArrowBuffer options;
};
void VgiAttachTicketReset(struct VgiAttachTicket *);
/* nonce must be NULL in production; a fixed nonce is for test vectors only. */
int VgiAttachTicketMint(const unsigned char key[32], const char *principal,
                        const struct VgiAttachTicket *, const unsigned char nonce[24],
                        struct ArrowBuffer *token);
int VgiAttachTicketOpen(const unsigned char key[32], const char *principal, const char *token,
                        double now, struct VgiAttachTicket *);
#ifdef __cplusplus
}
#endif
#endif
