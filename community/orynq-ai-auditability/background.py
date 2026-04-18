import hashlib
import json
import time

from src.agent.capability import MatchingCapability
from src.agent.capability_worker import CapabilityWorker
from src.main import AgentWorker

# =============================================================================
# ORYNQ AI AUDITABILITY — Background Daemon
#
# Passively captures every assistant and user turn into a SHA-256 rolling
# hash chain. Starts on session connect, polls get_full_message_history()
# every POLL_INTERVAL seconds, appends new entries to the chain, and
# persists the chain to a user-data JSON file so it survives session
# restarts and can be read by main.py on trigger.
#
# No speaking, no user interaction — this is pure silent capture. The
# interactive trigger handler (main.py) is what reads the chain and
# offers to anchor it to Materios / Cardano.
#
# Chain recurrence (per message):
#     h_i = SHA256( canonical_json( { seq, role, content_hash, prev, ts } ) )
# where content_hash = SHA256(content). Raw content is never stored or
# uploaded — only hashes. This is the privacy guarantee.
# =============================================================================

CHAIN_FILE = "orynq_audit_chain.json"
POLL_INTERVAL = 90.0            # seconds between polls (reviewer suggested 60-90)
SAVE_EVERY_N_POLLS = 10         # flush to disk at least every N polls even if nothing changed
ZERO_HASH = "0" * 64            # genesis prev-hash


def _canonical_json(obj) -> str:
    """Deterministic JSON encoding used for hash inputs."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def _hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _hash_str(data: str) -> str:
    return _hash_bytes(data.encode("utf-8"))


def _new_state() -> dict:
    """Mutable state held as a local dict (Pydantic blocks arbitrary self.*)."""
    return {
        "last_seen_index": 0,
        "chain": [],          # list of entry dicts
        "head": ZERO_HASH,    # current chain head (last entry's chain_hash)
        "last_anchor": None,  # {content_hash, status, sponsored, ts} or None
        "consent_granted_until": 0,  # epoch seconds; 0 = require consent each anchor
        "polls_since_save": 0,
    }


def _build_entry(role: str, content: str, prev: str, seq: int) -> dict:
    """Build one rolling-hash entry. Raw content is NOT stored — only its SHA-256."""
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    content_hash = _hash_str(content or "")
    payload = {
        "seq": seq,
        "role": role,
        "content_hash": content_hash,
        "prev": prev,
        "ts": timestamp,
    }
    chain_hash = _hash_str(_canonical_json(payload))
    return {
        "seq": seq,
        "role": role,
        "content_hash": content_hash,
        "chain_hash": chain_hash,
        "previous_hash": prev,
        "timestamp": timestamp,
    }


class OrynqAuditabilityBackground(MatchingCapability):
    worker: AgentWorker = None
    capability_worker: CapabilityWorker = None
    background_daemon_mode: bool = False

    # Do not change following tag of register capability
    # {{register_capability}}

    # ------------------------------------------------------------------
    # File I/O (delete + write pattern, per SDK JSON rule)
    # ------------------------------------------------------------------

    async def _load_state(self) -> dict:
        try:
            exists = await self.capability_worker.check_if_file_exists(CHAIN_FILE, False)
            if not exists:
                return _new_state()
            raw = await self.capability_worker.read_file(CHAIN_FILE, False)
            if not raw or not raw.strip():
                return _new_state()
            data = json.loads(raw)
            state = _new_state()
            state.update({
                "last_seen_index": int(data.get("last_seen_index", 0)),
                "chain": data.get("chain", []) or [],
                "head": data.get("head", ZERO_HASH),
                "last_anchor": data.get("last_anchor"),
                "consent_granted_until": int(data.get("consent_granted_until", 0) or 0),
            })
            return state
        except Exception as e:
            self.worker.editor_logging_handler.error(
                "[OrynqAudit] Load error: " + str(e)
            )
            return _new_state()

    async def _save_state(self, state: dict):
        """Persist current chain + metadata. Delete + write (JSON rule)."""
        data = {
            "last_seen_index": state["last_seen_index"],
            "chain": state["chain"],
            "head": state["head"],
            "last_anchor": state["last_anchor"],
            "consent_granted_until": state["consent_granted_until"],
            "chain_length": len(state["chain"]),
            "updated_ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        try:
            exists = await self.capability_worker.check_if_file_exists(CHAIN_FILE, False)
            if exists:
                await self.capability_worker.delete_file(CHAIN_FILE, False)
            await self.capability_worker.write_file(
                CHAIN_FILE, json.dumps(data, indent=2), False
            )
        except Exception as e:
            self.worker.editor_logging_handler.error(
                "[OrynqAudit] Save error: " + str(e)
            )

    # ------------------------------------------------------------------
    # Chain extension
    # ------------------------------------------------------------------

    def _extend_chain(self, state: dict, new_messages: list) -> int:
        """Append hash entries for each new message. Returns number appended."""
        added = 0
        for msg in new_messages:
            role = msg.get("role", "")
            content = msg.get("content", "")
            if not isinstance(content, str):
                # Skip non-text messages (tool calls, audio, etc.) — hashing them
                # would require canonicalising opaque structures we don't control.
                continue
            content = content.strip()
            if not content:
                continue
            if role not in ("user", "assistant", "system"):
                continue

            seq = len(state["chain"])
            entry = _build_entry(role, content, state["head"], seq)
            state["chain"].append(entry)
            state["head"] = entry["chain_hash"]
            added += 1
        return added

    # ------------------------------------------------------------------
    # Watch loop — silent, no speaking
    # ------------------------------------------------------------------

    async def watch_loop(self):
        self.worker.editor_logging_handler.info(
            "[OrynqAudit] daemon started — silent capture every "
            + str(int(POLL_INTERVAL)) + "s"
        )

        state = await self._load_state()

        while True:
            try:
                history = self.capability_worker.get_full_message_history() or []
                current_length = len(history)

                # Pointer hygiene — if history was rewritten or trimmed, back off
                if state["last_seen_index"] > current_length:
                    self.worker.editor_logging_handler.info(
                        "[OrynqAudit] history shrunk, resetting pointer"
                    )
                    state["last_seen_index"] = current_length

                new_messages = history[state["last_seen_index"]:]
                state["last_seen_index"] = current_length

                added = self._extend_chain(state, new_messages)

                state["polls_since_save"] = state.get("polls_since_save", 0) + 1

                if added > 0:
                    self.worker.editor_logging_handler.info(
                        "[OrynqAudit] +" + str(added) + " entries, chain length="
                        + str(len(state["chain"])) + ", head=" + state["head"][:16]
                    )
                    await self._save_state(state)
                    state["polls_since_save"] = 0
                elif state["polls_since_save"] >= SAVE_EVERY_N_POLLS:
                    # Periodic flush catches consent TTL expiry / pointer updates
                    await self._save_state(state)
                    state["polls_since_save"] = 0

            except Exception as e:
                self.worker.editor_logging_handler.error(
                    "[OrynqAudit] loop error: " + str(e)
                )

            await self.worker.session_tasks.sleep(POLL_INTERVAL)

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def call(self, worker: AgentWorker, background_daemon_mode: bool):
        self.worker = worker
        self.background_daemon_mode = background_daemon_mode
        self.capability_worker = CapabilityWorker(self.worker)
        self.worker.editor_logging_handler.info(
            "[OrynqAudit] background.py call() — launching watch_loop"
        )
        self.worker.session_tasks.create(self.watch_loop())
