# Orynq AI Auditability

![Community](https://img.shields.io/badge/OpenHome-Community-orange?style=flat-square)
![Author](https://img.shields.io/badge/Author-@flux--point--studios-lightgrey?style=flat-square)
![Cardano](https://img.shields.io/badge/Blockchain-Cardano-blue?style=flat-square)

## What It Does

Creates **tamper-proof audit trails** for AI conversations. Each message is hashed into a rolling SHA-256 chain where modifying any entry invalidates all subsequent hashes — making tampering immediately detectable.

The hash chain is created locally with **zero setup required**. Optionally, it can be anchored to the **Materios partner chain** for permanent blockchain immutability, with certified receipts batched into Cardano mainnet transactions.

## Suggested Trigger Words

- "audit my AI"
- "create audit trail"
- "blockchain audit"
- "proof of inference"
- "verify AI"
- "AI accountability"
- "audit this conversation"
- "run orynq"
- "anchor this session"
- "anchor this conversation"
- "record AI decision"
- "log this to blockchain"
- "start audit"
- "chain of custody"

## Setup

**No setup required for local audit trails.** Upload the ability and go.

For blockchain anchoring (optional):

| Path | Setup | Who pays fees |
|------|-------|--------------|
| **Sponsored** | Set `MATERIOS_GATEWAY_API_KEY` in `main.py` | FPS (included) |
| **Permissionless** | Create a Materios wallet + get MATRA from faucet via [orynq-sdk](https://github.com/flux-point-studios/orynq-sdk) | You (free from faucet) |

## How It Works

The ability is split in two — passive capture plus on-demand anchoring:

1. **Background daemon (`background.py`)** — starts silently on session connect and polls `get_full_message_history()` every 90 seconds. Every new user or assistant turn is hashed into a rolling SHA-256 chain (`h_i = SHA256(canonical_json({ seq, role, content_hash, prev, ts }))`) and the chain is persisted to user-data file storage (`orynq_audit_chain.json`). Raw content is never stored or uploaded — only the per-message SHA-256 and the chain links.
2. **Trigger handler (`main.py`)** — when the user says a trigger phrase, the ability reads the persisted chain, briefly reports the length and current head, and asks in natural spoken English whether to anchor. An LLM classifies the reply so the user can say anything ("go ahead", "just tell me where we're at", "leave it local for now", "anchor for the next hour", etc.).
3. **Consent flow** — by default consent is per-anchor. The user can also grant a time-limited standing consent ("anchor automatically for the next hour" or "for the next day"), which is persisted as `consent_granted_until`. If active, subsequent triggers anchor silently until the TTL expires; revocation is just another voice command.
4. **Upload** — on consent, the canonical chain is serialised as the v2 Materios envelope (`{p:"materios", v:2, chain:[...], head:"<hex>"}`) and uploaded to `https://materios.fluxpointstudios.com/blobs` using the standard two-step manifest-then-chunk protocol. The cert daemon committee certifies availability and the receipt is batched into a Cardano mainnet transaction under metadata label `8746`.

## Example Conversation

> **(background daemon has been running silently, hashing every turn since session start)**
> **User:** "Run orynq"
> **AI:** "I've captured eighteen entries so far, head 3f8a20c1b4ee. Last anchor was about an hour ago. Want me to anchor what's new?"
> **User:** "Yeah, and just keep anchoring for the next hour."
> **AI:** "Uploading now." *(short pause)* "Uploaded. Content hash 9c1dab72f340. I'll keep anchoring for the next hour."

## Why Auditability Matters

As AI systems make increasingly consequential decisions, organizations need provable records of what AI said and when. Traditional logging can be altered. Hash chain audit trails provide:

- **Tamper evidence** — Any modification breaks the chain
- **Independent verification** — Anyone can recompute the hashes
- **Blockchain immutability** — Optional on-chain anchoring via Cardano
- **Regulatory compliance** — Immutable records for audit requirements

## Technical Details

- **Architecture**: background daemon (`background.py`) for passive capture + interactive trigger (`main.py`) for anchoring
- **Poll interval**: 90 seconds, configurable (`POLL_INTERVAL` in `background.py`)
- **Hash algorithm**: SHA-256 rolling chain (each entry includes the previous hash)
- **Privacy**: only SHA-256 hashes are persisted or uploaded — raw content never leaves the device
- **Persistence**: `orynq_audit_chain.json` in user-data file storage (survives session restarts)
- **Consent**: per-anchor by default; optional time-limited standing consent persisted as `consent_granted_until`
- **Wire format**: v2 Materios envelope — `{p:"materios", v:2, chain:[...], head:"<hex>"}`
- **Blockchain**: Cardano mainnet via [Materios](https://docs.fluxpointstudios.com/materios-partner-chain) batched anchoring (metadata label `8746`)
- **Committee**: 10 independent attestors verify data availability before certification
- **Explorer**: [materios.fluxpointstudios.com/explorer](https://materios.fluxpointstudios.com/explorer/)
- **SDK**: [orynq-sdk](https://github.com/flux-point-studios/orynq-sdk)
