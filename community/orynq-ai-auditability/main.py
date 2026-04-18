import hashlib
import json
import time
from typing import Optional

import requests
from src.agent.capability import MatchingCapability
from src.agent.capability_worker import CapabilityWorker
from src.main import AgentWorker

# =============================================================================
# ORYNQ AI AUDITABILITY — Interactive Trigger Handler
#
# On phrase match, reads the rolling hash chain that background.py has
# been building silently, summarizes it in one sentence, and offers to
# anchor it to the Materios partner chain (which is batched into a
# Cardano mainnet transaction under metadata label 8746).
#
# The user must consent before any upload. Consent can be granted
# per-anchor (default) or for a time window (e.g. "anchor for the next
# hour"). Consent TTL is persisted across sessions.
#
# Raw content is NEVER uploaded — only the canonical chain of hash
# entries is sent to the blob gateway. Anyone can recompute them.
# =============================================================================

CHAIN_FILE = "orynq_audit_chain.json"
CHAIN_TMP_FILE = CHAIN_FILE + ".tmp"   # write-ahead journal, see background.py
ZERO_HASH = "0" * 64                   # genesis prev-hash
MATERIOS_GATEWAY_URL = "https://materios.fluxpointstudios.com/blobs"
MATERIOS_GATEWAY_API_KEY = ""  # Optional — enables sponsored receipt submission

# Consent windows the user can request (seconds). 0 = per-anchor only.
CONSENT_HOUR = 3600
CONSENT_DAY = 86400

# Shared system prompt used on every LLM call whose output is spoken. Keeps
# the model from emitting markdown, lists, URLs, emojis, or stage directions,
# and caps the response length so it sounds like a person on a speaker, not
# a help page being read aloud.
VOICE_STYLE = (
    "You speak on a voice device to a native US English speaker. "
    "Plain conversational spoken English only. "
    "No markdown, no bullet points, no numbered lists, no URLs, no emojis, "
    "no stage directions. "
    "Keep your reply to at most two sentences and under twenty words total."
)


def _canonical_json(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def _hash_str(data: str) -> str:
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def _is_compacted_head(entry) -> bool:
    """True if entry is a synthetic compacted-head marker (see background.py)."""
    return isinstance(entry, dict) and entry.get("type") == "compacted_head"


def _split_chain(chain: list):
    """Separate the compacted_head marker (if any) from real hash entries.

    Returns `(marker_or_None, real_entries)`. The on-disk chain may begin
    with a synthetic marker record when the background daemon has
    compacted older history to stay under MAX_ENTRIES_ON_DISK; the
    marker is not itself a hash entry and must be stripped before most
    downstream operations (verification, upload envelope, seq counting).
    """
    if chain and _is_compacted_head(chain[0]):
        return chain[0], chain[1:]
    return None, list(chain or [])


def _verify_chain(chain: list) -> dict:
    """Replay every hash link in the on-disk chain.

    Returns `{"ok": bool, "checked": int, "error": str|None, "partial":
    bool}`. `partial` is True when history was compacted — replay starts
    from the compacted_head's `prev_head` rather than genesis, so a
    successful verify only proves the chain is consistent from the
    compaction point forward.
    """
    marker, entries = _split_chain(chain)
    partial = marker is not None
    expected_prev = marker.get("prev_head", ZERO_HASH) if marker else ZERO_HASH

    for i, entry in enumerate(entries):
        if not isinstance(entry, dict):
            return {"ok": False, "checked": i, "error": "non-dict entry",
                    "partial": partial}
        prev = entry.get("previous_hash")
        if prev != expected_prev:
            return {"ok": False, "checked": i,
                    "error": "previous_hash mismatch at index " + str(i),
                    "partial": partial}
        # Recompute the canonical payload hash and compare.
        payload = {
            "seq": entry.get("seq"),
            "role": entry.get("role"),
            "content_hash": entry.get("content_hash"),
            "prev": prev,
            "ts": entry.get("timestamp"),
        }
        recomputed = _hash_str(_canonical_json(payload))
        if recomputed != entry.get("chain_hash"):
            return {"ok": False, "checked": i,
                    "error": "chain_hash mismatch at index " + str(i),
                    "partial": partial}
        expected_prev = entry["chain_hash"]

    return {"ok": True, "checked": len(entries), "error": None,
            "partial": partial}


class OrynqAuditabilityCapability(MatchingCapability):
    worker: AgentWorker = None
    capability_worker: CapabilityWorker = None

    # Do not change following tag of register capability
    # {{register_capability}}

    # ------------------------------------------------------------------
    # File I/O — reads the chain file written by background.py
    #
    # The OpenHome SDK has no atomic rename primitive (see background.py
    # for the full rationale), so persistence uses the same write-ahead
    # journal pattern: stage to `.tmp`, verify, overwrite real, delete
    # `.tmp`. On load, if the real file is missing/corrupt but `.tmp` is
    # valid, recover from the journal.
    # ------------------------------------------------------------------

    async def _read_json_file(self, filename: str) -> Optional[dict]:
        """Return parsed JSON from filename, or None on any error."""
        try:
            exists = await self.capability_worker.check_if_file_exists(filename, False)
            if not exists:
                return None
            raw = await self.capability_worker.read_file(filename, False)
            if not raw or not raw.strip():
                return None
            return json.loads(raw)
        except Exception:
            return None

    async def _load_chain(self) -> Optional[dict]:
        try:
            data = await self._read_json_file(CHAIN_FILE)
            tmp_data = await self._read_json_file(CHAIN_TMP_FILE)

            if data is None and tmp_data is not None:
                # Recover from the journal — real file was lost mid-save.
                self._log_info("recovered chain from " + CHAIN_TMP_FILE)
                data = tmp_data
                try:
                    await self.capability_worker.write_file(
                        CHAIN_FILE, json.dumps(data, indent=2), False, mode="w"
                    )
                    if await self.capability_worker.check_if_file_exists(
                        CHAIN_TMP_FILE, False
                    ):
                        await self.capability_worker.delete_file(CHAIN_TMP_FILE, False)
                except Exception as promo_err:
                    self._log_error("tmp promotion failed: " + str(promo_err))
            elif data is not None and tmp_data is not None:
                # Stale journal — real file is authoritative.
                try:
                    await self.capability_worker.delete_file(CHAIN_TMP_FILE, False)
                except Exception:
                    pass

            return data
        except Exception as e:
            self._log_error("load error: " + str(e))
            return None

    async def _save_chain(self, data: dict):
        """Write-ahead journal save — see background.py for the rationale."""
        try:
            serialized = json.dumps(data, indent=2)

            # Step 1: stage to journal.
            if await self.capability_worker.check_if_file_exists(CHAIN_TMP_FILE, False):
                await self.capability_worker.delete_file(CHAIN_TMP_FILE, False)
            await self.capability_worker.write_file(
                CHAIN_TMP_FILE, serialized, False, mode="w"
            )

            # Step 2: verify round-trip before touching the real file.
            verify_raw = await self.capability_worker.read_file(CHAIN_TMP_FILE, False)
            if not verify_raw or len(verify_raw) != len(serialized):
                raise IOError(
                    "journal verify failed (expected "
                    + str(len(serialized)) + " bytes, got "
                    + str(len(verify_raw) if verify_raw else 0) + ")"
                )
            json.loads(verify_raw)

            # Step 3: overwrite the real file.
            if await self.capability_worker.check_if_file_exists(CHAIN_FILE, False):
                await self.capability_worker.delete_file(CHAIN_FILE, False)
            await self.capability_worker.write_file(
                CHAIN_FILE, serialized, False, mode="w"
            )

            # Step 4: clean up the journal.
            if await self.capability_worker.check_if_file_exists(CHAIN_TMP_FILE, False):
                await self.capability_worker.delete_file(CHAIN_TMP_FILE, False)
        except Exception as e:
            self._log_error("save error: " + str(e))

    # ------------------------------------------------------------------
    # Logging helpers
    # ------------------------------------------------------------------

    def _log_info(self, msg: str):
        if self.worker:
            self.worker.editor_logging_handler.info("[OrynqAudit] " + msg)

    def _log_error(self, msg: str):
        if self.worker:
            self.worker.editor_logging_handler.error("[OrynqAudit] " + msg)

    # ------------------------------------------------------------------
    # Intent classification — LLM-based, no hardcoded keyword lists
    # ------------------------------------------------------------------

    def _classify_intent(self, text: str) -> str:
        """
        Returns one of:
          ANCHOR_NOW          — user wants to anchor right now, one-off
          ANCHOR_SESSION      — user wants auto-anchor for this session (~1h)
          ANCHOR_DAY          — user wants auto-anchor for the next 24h
          VERIFY              — user wants status/summary/verification only
          REVOKE              — user wants to revoke standing auto-anchor consent
          CANCEL              — user wants to stop / exit
          UNKNOWN             — anything else
        """
        if not text or not text.strip():
            return "UNKNOWN"

        # This one is NOT VOICE_STYLE — output isn't spoken, it's a single
        # label that the code parses. Still instructs the model to emit one
        # token only so we don't fall back to UNKNOWN.
        system_prompt = (
            "You are an intent classifier for a voice ability that builds "
            "tamper-proof audit trails of AI conversations. Classify the "
            "user's reply into exactly one of the listed labels. "
            "Respond with only the label word, nothing else."
        )
        prompt = (
            "User said: \"" + text.strip() + "\"\n\n"
            "Labels (pick exactly one):\n"
            "ANCHOR_NOW - they want to anchor it once, right now.\n"
            "ANCHOR_SESSION - they want to anchor automatically for about an hour.\n"
            "ANCHOR_DAY - they want to anchor automatically for the next day.\n"
            "VERIFY - they only want a status or summary, not to upload.\n"
            "REVOKE - they want to turn off any standing auto-anchor consent.\n"
            "CANCEL - they are declining, stopping, or dropping it.\n"
            "UNKNOWN - none of the above.\n\n"
            "Label:"
        )
        try:
            raw = self.capability_worker.text_to_text_response(
                prompt, system_prompt=system_prompt
            )
            if not raw:
                return "UNKNOWN"
            label = raw.strip().upper().split()[0] if raw.strip() else "UNKNOWN"
            label = label.strip(".,:'\"`")
            valid = {"ANCHOR_NOW", "ANCHOR_SESSION", "ANCHOR_DAY",
                     "VERIFY", "REVOKE", "CANCEL", "UNKNOWN"}
            return label if label in valid else "UNKNOWN"
        except Exception as e:
            self._log_error("intent classify error: " + str(e))
            return "UNKNOWN"

    # ------------------------------------------------------------------
    # Materios blob upload
    # ------------------------------------------------------------------

    def _build_trace_blob(self, chain: list) -> bytes:
        """
        Wire format: {p:"materios", v:2, chain:[...], head:"<hex>"}
        Shape is kept stable on purpose — this is the schema the cert
        daemon committee already indexes under Cardano metadata label
        8746. The in-memory chain can begin with a synthetic
        `compacted_head` marker (rolling-window compaction), which is
        NOT a hash entry and would confuse v2 indexers, so we strip it
        out of `chain` and surface it as an optional additive top-level
        field. v2-only consumers ignore the extra field; compaction-
        aware consumers use it to know that replay from genesis is not
        possible for this blob.
        """
        marker, real_entries = _split_chain(chain)
        head = real_entries[-1]["chain_hash"] if real_entries else ZERO_HASH
        envelope = {
            "p": "materios",
            "v": 2,
            "chain": real_entries,
            "head": head,
        }
        if marker is not None:
            envelope["compacted_head"] = marker
        return _canonical_json(envelope).encode("utf-8")

    def _anchor_to_materios(self, chain: list) -> Optional[dict]:
        """Two-step upload: manifest then chunk. Returns None on failure."""
        try:
            content = self._build_trace_blob(chain)
            content_hash = hashlib.sha256(content).hexdigest()

            headers = {"Content-Type": "application/json"}
            if MATERIOS_GATEWAY_API_KEY:
                headers["x-api-key"] = MATERIOS_GATEWAY_API_KEY

            manifest = {
                "chunks": [
                    {"index": 0, "sha256": content_hash, "size": len(content)}
                ],
                "total_size": len(content),
            }
            manifest_resp = requests.post(
                MATERIOS_GATEWAY_URL + "/" + content_hash + "/manifest",
                headers=headers,
                json=manifest,
                timeout=30,
            )
            if manifest_resp.status_code not in (200, 201, 409):
                self._log_error(
                    "manifest upload failed: " + str(manifest_resp.status_code)
                )
                return None

            chunk_headers = {"Content-Type": "application/octet-stream"}
            if MATERIOS_GATEWAY_API_KEY:
                chunk_headers["x-api-key"] = MATERIOS_GATEWAY_API_KEY

            chunk_resp = requests.put(
                MATERIOS_GATEWAY_URL + "/" + content_hash + "/chunks/0",
                headers=chunk_headers,
                data=content,
                timeout=30,
            )
            if chunk_resp.status_code not in (200, 201, 409):
                self._log_error(
                    "chunk upload failed: " + str(chunk_resp.status_code)
                )
                return None

            sponsored = bool(MATERIOS_GATEWAY_API_KEY)
            return {
                "content_hash": content_hash,
                "status": "submitted" if sponsored else "uploaded",
                "sponsored": sponsored,
                "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
        except Exception as e:
            self._log_error("materios error: " + str(e))
            return None

    # ------------------------------------------------------------------
    # Spoken summary — LLM, plain English, length-capped
    # ------------------------------------------------------------------

    def _summarize_state(self, chain_len: int, head: str,
                         last_anchor: Optional[dict], auto_active: bool) -> str:
        short = head[:12] if head else "empty"
        last_txt = "none yet"
        if last_anchor:
            last_txt = "hash " + str(last_anchor.get("content_hash", ""))[:12]
        auto_txt = "auto anchoring is on" if auto_active else "auto anchoring is off"

        prompt = (
            "Summarise the audit state and end with a short open question. "
            "Chain length: " + str(chain_len) + " entries. "
            "Head starts with: " + short + ". "
            "Last anchor: " + last_txt + ". "
            + auto_txt + "."
        )
        try:
            return self.capability_worker.text_to_text_response(
                prompt, system_prompt=VOICE_STYLE
            )
        except Exception:
            return (
                "Captured " + str(chain_len) + " entries. Anchor now?"
            )

    def _summarize_anchor(self, result: dict) -> str:
        ch = str(result.get("content_hash", ""))[:12]
        sponsored = bool(result.get("sponsored"))
        if sponsored:
            prompt = (
                "Confirm the audit trail was uploaded and will be batched "
                "into Cardano. The content hash starts with " + ch + "."
            )
        else:
            prompt = (
                "Confirm the audit trail was uploaded. Content hash starts "
                "with " + ch + ". Mention on-chain submission can be "
                "completed later with their own wallet."
            )
        try:
            return self.capability_worker.text_to_text_response(
                prompt, system_prompt=VOICE_STYLE
            )
        except Exception:
            if sponsored:
                return "Uploaded, hash " + ch + ". It will be anchored to Cardano."
            return "Uploaded, hash " + ch + ". Finish on-chain submission later."

    # ------------------------------------------------------------------
    # Anchor flow — performs the upload and persists the last-anchor record
    # ------------------------------------------------------------------

    async def _do_anchor(self, data: dict) -> bool:
        chain = data.get("chain", []) or []
        # Anchoring is only meaningful if there is at least one real hash
        # entry — a lone compacted_head marker is metadata, not history.
        _, real_entries = _split_chain(chain)
        if not real_entries:
            await self.capability_worker.speak(
                "Nothing to anchor yet. Try again later."
            )
            return False

        await self.capability_worker.speak("Uploading now.")
        result = self._anchor_to_materios(chain)
        if not result:
            await self.capability_worker.speak(
                "Couldn't reach the gateway. Local chain is still valid."
            )
            return False

        data["last_anchor"] = result
        await self._save_chain(data)
        await self.capability_worker.speak(self._summarize_anchor(result))
        return True

    # ------------------------------------------------------------------
    # Main run
    # ------------------------------------------------------------------

    async def _run(self):
        try:
            await self.capability_worker.wait_for_complete_transcription()

            data = await self._load_chain()
            if not data:
                await self.capability_worker.speak(
                    "Nothing captured yet. Try again in a minute."
                )
                return

            chain = data.get("chain", []) or []
            # `real_entries` drops the synthetic compacted_head marker
            # that rolling-window compaction prepends; everything the
            # user hears is phrased in terms of real entries only. The
            # spoken summary stays quiet about compaction unless the
            # user explicitly asks for a verification status.
            _marker, real_entries = _split_chain(chain)
            head = data.get("head", "")
            last_anchor = data.get("last_anchor")
            consent_until = int(data.get("consent_granted_until", 0) or 0)
            now = int(time.time())
            auto_active = consent_until > now

            if not real_entries:
                await self.capability_worker.speak(
                    "The audit chain is empty. I'll start capturing from now."
                )
                return

            # If standing auto-consent is active, anchor silently and report.
            if auto_active:
                anchored = await self._do_anchor(data)
                if not anchored:
                    return
                await self.capability_worker.speak(
                    "Anything else, or should I keep this running?"
                )
                reply = await self.capability_worker.user_response()
                intent = self._classify_intent(reply or "")
                if intent == "REVOKE":
                    data["consent_granted_until"] = 0
                    await self._save_chain(data)
                    await self.capability_worker.speak("Turned off auto anchoring.")
                return

            # Otherwise summarize + open-ended question, then classify reply.
            await self.capability_worker.speak(
                self._summarize_state(len(real_entries), head, last_anchor, auto_active)
            )
            reply = await self.capability_worker.user_response()
            intent = self._classify_intent(reply or "")

            if intent == "ANCHOR_NOW":
                await self._do_anchor(data)
                return

            if intent == "ANCHOR_SESSION":
                data["consent_granted_until"] = now + CONSENT_HOUR
                await self._save_chain(data)
                await self._do_anchor(data)
                await self.capability_worker.speak(
                    "I'll keep anchoring for the next hour."
                )
                return

            if intent == "ANCHOR_DAY":
                data["consent_granted_until"] = now + CONSENT_DAY
                await self._save_chain(data)
                await self._do_anchor(data)
                await self.capability_worker.speak(
                    "I'll keep anchoring for the next day."
                )
                return

            if intent == "VERIFY":
                # Actually replay the hash chain. When history has been
                # compacted, verification starts from the compacted_head
                # `prev_head` rather than genesis — the user is told the
                # history is partial only in this explicit-ask path, per
                # the "don't mention compaction unless asked" rule.
                result = _verify_chain(chain)
                short = head[:12] if head else "empty"
                if result["ok"]:
                    if result["partial"]:
                        await self.capability_worker.speak(
                            "Verified " + str(result["checked"])
                            + " entries, head " + short
                            + ". Older history has been compacted."
                        )
                    else:
                        await self.capability_worker.speak(
                            "Verified " + str(result["checked"])
                            + " entries, head " + short + "."
                        )
                else:
                    await self.capability_worker.speak(
                        "Chain failed verification at entry "
                        + str(result["checked"]) + "."
                    )
                return

            if intent == "REVOKE":
                data["consent_granted_until"] = 0
                await self._save_chain(data)
                await self.capability_worker.speak("Turned off auto anchoring.")
                return

            if intent == "CANCEL":
                await self.capability_worker.speak(
                    "No problem, leaving it local for now."
                )
                return

            # Unknown — one clarifying open question. No menu-driven loop.
            await self.capability_worker.speak(
                "Should I anchor it, or leave it local?"
            )
            reply2 = await self.capability_worker.user_response()
            intent2 = self._classify_intent(reply2 or "")
            if intent2 in ("ANCHOR_NOW", "ANCHOR_SESSION", "ANCHOR_DAY"):
                if intent2 == "ANCHOR_SESSION":
                    data["consent_granted_until"] = now + CONSENT_HOUR
                    await self._save_chain(data)
                elif intent2 == "ANCHOR_DAY":
                    data["consent_granted_until"] = now + CONSENT_DAY
                    await self._save_chain(data)
                await self._do_anchor(data)
            else:
                await self.capability_worker.speak("Got it, leaving it local.")

        except Exception as e:
            self._log_error("run error: " + str(e))
            try:
                await self.capability_worker.speak(
                    "Something went wrong. Try again in a moment."
                )
            except Exception:
                pass
        finally:
            self.capability_worker.resume_normal_flow()

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def call(self, worker: AgentWorker):
        self.worker = worker
        self.capability_worker = CapabilityWorker(self.worker)
        self.worker.session_tasks.create(self._run())
