"""
Deployer blacklist — persistent list of known scam deployers.

Stored in /data/tysmith_deployer_blacklist.json.

Auto-populated when a position is confirmed stuck (honeypot).
Manually managed via /blacklist Telegram command.
"""

import json
import logging
import os
import time

log = logging.getLogger(__name__)

_DATA_DIR      = os.getenv("DATA_DIR", "/data")
BLACKLIST_FILE = os.path.join(_DATA_DIR, "tysmith_deployer_blacklist.json")

# In-memory cache: deployer_address (lowercase) → entry dict
# Loaded once at startup, written on every change.
_bl: dict[str, dict] = {}


def load():
    """Load blacklist from disk. Call once at startup."""
    global _bl
    try:
        if os.path.exists(BLACKLIST_FILE):
            with open(BLACKLIST_FILE) as f:
                _bl = json.load(f)
            log.info(f"Deployer blacklist loaded: {len(_bl)} entries")
        else:
            _bl = {}
    except Exception as e:
        log.warning(f"Could not load deployer blacklist: {e}")
        _bl = {}


def _save():
    try:
        os.makedirs(_DATA_DIR, exist_ok=True)
        with open(BLACKLIST_FILE, "w") as f:
            json.dump(_bl, f, indent=2, ensure_ascii=False)
    except Exception as e:
        log.error(f"Failed to save deployer blacklist: {e}")


def add(deployer_address: str, reason: str = "") -> bool:
    """
    Add a deployer address to the blacklist.
    If already present, increments the hit counter.
    Returns True if newly added, False if already existed.
    """
    addr = deployer_address.lower().strip()
    if not addr.startswith("0x") or len(addr) != 42:
        return False
    already = addr in _bl
    if already:
        _bl[addr]["hits"] = _bl[addr].get("hits", 1) + 1
        _bl[addr]["last_seen"] = time.time()
        if reason and not _bl[addr].get("reason"):
            _bl[addr]["reason"] = reason
    else:
        _bl[addr] = {
            "added_at": time.time(),
            "reason":   reason,
            "hits":     1,
        }
    _save()
    log.info(f"Blacklist {'hit' if already else 'add'}: {addr} | {reason}")
    return not already


def remove(deployer_address: str) -> bool:
    """Remove a deployer from the blacklist. Returns True if it was there."""
    addr = deployer_address.lower().strip()
    if addr in _bl:
        del _bl[addr]
        _save()
        log.info(f"Blacklist removed: {addr}")
        return True
    return False


def is_blacklisted(deployer_address: str) -> bool:
    """Return True if the deployer is on the blacklist."""
    return deployer_address.lower().strip() in _bl


def get_all() -> dict:
    """Return a copy of the full blacklist dict."""
    return dict(_bl)


def count() -> int:
    return len(_bl)
