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
MATERIOS_GATEWAY_URL = "https://materios.fluxpointstudios.com/blobs"
MATERIOS_GATEWAY_API_KEY = ""  # Optional — enables sponsored receipt submission

# Consent windows the user can request (seconds). 0 = per-anchor only.
CONSENT_HOUR = 3600
CONSENT_DAY = 86400


def _canonical_json(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


class OrynqAuditabilityCapability(MatchingCapability):
    worker: AgentWorker = None
    capability_worker: CapabilityWorker = None

    # Do not change following tag of register capability
    # {{register_capability}}

    # ------------------------------------------------------------------
    # File I/O — reads the chain file written by background.py
    # ------------------------------------------------------------------

    async def _load_chain(self) -> Optional[dict]:
        try:
            exists = await self.capability_worker.check_if_file_exists(CHAIN_FILE, False)
            if not exists:
                return None
            raw = await self.capability_worker.read_file(CHAIN_FILE, False)
            if not raw or not raw.strip():
                return None
            return json.loads(raw)
        except Exception as e:
            self._log_error("load error: " + str(e))
            return None

    async def _save_chain(self, data: dict):
        try:
            exists = await self.capability_worker.check_if_file_exists(CHAIN_FILE, False)
            if exists:
                await self.capability_worker.delete_file(CHAIN_FILE, False)
            await self.capability_worker.write_file(
                CHAIN_FILE, json.dumps(data, indent=2), False
            )
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

        system_prompt = (
            "You are an intent classifier for a voice ability that builds "
            "tamper-proof audit trails of AI conversations. The user has just "
            "spoken a reply to a question about anchoring the audit trail to "
            "the blockchain. Classify their intent into exactly one label. "
            "Respond with only the label, nothing else."
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
        daemon committee already indexes under Cardano metadata label 8746.
        """
        head = chain[-1]["chain_hash"] if chain else "0" * 64
        envelope = {
            "p": "materios",
            "v": 2,
            "chain": chain,
            "head": head,
        }
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

        system_prompt = (
            "You speak on a voice device to a native US English speaker. "
            "Use plain conversational spoken English. No markdown, no bullet "
            "points, no URLs, no emojis, no stage directions. Keep it under "
            "twenty five words and at most two sentences."
        )
        prompt = (
            "Summarise the audit state and offer to anchor it. "
            "There are " + str(chain_len) + " entries in the hash chain. "
            "The chain head starts with " + short + ". "
            "Last anchor: " + last_txt + ". "
            + auto_txt + ". "
            "End with a short open question asking what the user wants to do."
        )
        try:
            return self.capability_worker.text_to_text_response(
                prompt, system_prompt=system_prompt
            )
        except Exception:
            return (
                "I've captured " + str(chain_len) + " entries so far. "
                "Want me to anchor the chain to the blockchain now?"
            )

    def _summarize_anchor(self, result: dict) -> str:
        ch = str(result.get("content_hash", ""))[:12]
        sponsored = bool(result.get("sponsored"))
        system_prompt = (
            "You speak on a voice device. Plain spoken English, no markdown, "
            "no URLs, no emojis. One sentence, under twenty words."
        )
        if sponsored:
            prompt = (
                "Confirm the audit trail was uploaded and the receipt will be "
                "batched into Cardano. The content hash starts with " + ch + "."
            )
        else:
            prompt = (
                "Confirm the audit trail was uploaded. The content hash starts "
                "with " + ch + ". Mention the user can complete on-chain "
                "submission later with their own wallet."
            )
        try:
            return self.capability_worker.text_to_text_response(
                prompt, system_prompt=system_prompt
            )
        except Exception:
            if sponsored:
                return (
                    "Uploaded. Content hash " + ch
                    + ". It will be anchored to Cardano."
                )
            return (
                "Uploaded. Content hash " + ch
                + ". You can finish on-chain submission later."
            )

    # ------------------------------------------------------------------
    # Anchor flow — performs the upload and persists the last-anchor record
    # ------------------------------------------------------------------

    async def _do_anchor(self, data: dict) -> bool:
        chain = data.get("chain", []) or []
        if not chain:
            await self.capability_worker.speak(
                "Nothing to anchor yet. I'll keep capturing and you can try again later."
            )
            return False

        await self.capability_worker.speak("Uploading now.")
        result = self._anchor_to_materios(chain)
        if not result:
            await self.capability_worker.speak(
                "I couldn't reach the gateway. The local chain is still valid."
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
                    "I haven't captured anything yet. "
                    "Keep chatting and ask again in a minute."
                )
                return

            chain = data.get("chain", []) or []
            head = data.get("head", "")
            last_anchor = data.get("last_anchor")
            consent_until = int(data.get("consent_granted_until", 0) or 0)
            now = int(time.time())
            auto_active = consent_until > now

            if not chain:
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
                self._summarize_state(len(chain), head, last_anchor, auto_active)
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
                short = head[:12] if head else "empty"
                await self.capability_worker.speak(
                    "Local chain is " + str(len(chain))
                    + " entries, head " + short + "."
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

            # Unknown — fall back on a clarifying open question. One round only
            # (no menu-driven loop).
            await self.capability_worker.speak(
                "Do you want me to anchor the chain to the blockchain, "
                "or just leave it local?"
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
