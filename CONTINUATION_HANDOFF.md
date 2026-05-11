# wyoming-satellite-plus: Conversation Continuation — RESOLVED
# Last updated 2026-05-11 14:32 EDT by Jarvis

## Status: ✅ WORKING

End-to-end conversation continuation works. After Jarvis asks a follow-up
question (e.g. "Which lights?"), the satellite mic auto-opens with no need
to re-say "Hey Jarvis". Conversation context is preserved.

Verified flow:
1. "Hey Jarvis, turn off the lights"
2. Claude → "Which lights?" (TTS plays + done chime)
3. Mic auto-opens (no wake word needed)
4. "Office light"
5. Light turns off ✅

## Root cause (the actual one)

Two separate bugs had to be fixed:

### Bug 1: HA-side custom integration didn't kick off a follow-up pipeline.

Fixed in `custom_components/wyoming_satellite_plus/assist_satellite.py`:
- Capture last TTS text on `intent-end` event into `self._last_tts_text`
- On `Played` event from satellite: if text ends with `?`, schedule
  `_start_conversation_after_question()`
- That method writes `RunPipeline(start_stage=ASR)` to the satellite over
  the existing wyoming TCP connection AND kicks off a local STT pipeline
  via `_run_pipeline_once()`.

Note: the original handoff doc incorrectly claimed the `Played` event was
unreliable. It IS reliably sent — the snd subprocess on the satellite exits
after paplay finishes, which triggers the wyoming-satellite to send `Played`
back to HA. See `wyoming_satellite/satellite.py` line 623 (snd loop).

### Bug 2: wyoming-satellite ignored server-initiated RunPipeline.

`WakeStreamingSatellite.event_from_server` in wyoming-satellite only flips
`self.is_streaming = True` after a real wake-word `Detection` from the
local openwakeword service. A server-pushed `RunPipeline(start_stage=ASR)`
was silently ignored.

Fixed by patching the satellite's `event_from_server` to detect that case
and immediately:
- Set `self.is_streaming = True`
- Call `self.trigger_streaming_start()`
- Return early (skip wake-word path entirely)

Patch lives at `patches/satellite_continuation.patch.py`. Idempotent
(checks for `# WSP_CONTINUATION_PATCH_v1` sentinel).

### ⚠️ CRITICAL GOTCHA

The wyoming-satellite systemd unit runs:

    /opt/wyoming-satellite/.venv/bin/python -m wyoming_satellite

Python imports the `wyoming_satellite` package from the venv's
`site-packages/`, NOT from `/opt/wyoming-satellite/wyoming_satellite/`
(which is the source tree from the git clone).

The path that matters is:

    /opt/wyoming-satellite/.venv/lib/python3.11/site-packages/wyoming_satellite/satellite.py

We initially patched the source tree and the patch did nothing. Lost ~2
hours chasing imaginary TCP bugs. The patch script now hard-codes the
venv path.

## Files

### HA host (10.0.0.54), in docker container `homeassistant`
- `/homeassistant/custom_components/wyoming_satellite_plus/assist_satellite.py`
  (40070 bytes; mirror in `workspace/wyoming-satellite-plus/...` is canonical)

### jarvis-lxc (10.0.0.219)
- `/opt/wyoming-satellite/.venv/lib/python3.11/site-packages/wyoming_satellite/satellite.py`
  (patched in place; backups at `.bak.<timestamp>` in source tree)
- Service: `wyoming-satellite.service` + `wyoming-openwakeword.service`

## Re-applying patches after wyoming-satellite upgrade

If wyoming-satellite is reinstalled or upgraded in the venv, our patch will
be wiped. Re-apply with:

```bash
# Copy patches/satellite_continuation.patch.py to jarvis-lxc, then:
ssh root@10.0.0.219 'python3 /tmp/satellite_continuation.patch.py && systemctl restart wyoming-satellite.service'
```

The script is idempotent (sentinel check) — safe to re-run.

## Reload caveat for HA custom component

`homeassistant.reload_all` and config-entry reload do NOT reload Python
modules from `custom_components/`. After editing `assist_satellite.py`,
you must restart Home Assistant fully:

```bash
curl -X POST -H "Authorization: Bearer $HA_TOKEN" \
  http://10.0.0.54:8123/api/services/homeassistant/restart
```

## Connection hygiene

Observed during debugging: HA restarts can leave a stale TCP connection
in `CLOSE-WAIT` on the satellite side (the satellite holds the FD even
after HA closes its end). Doesn't break things on its own (the satellite
accepts a fresh connection too) but is worth knowing about. A satellite
restart clears it.

Check with:
```bash
ssh root@10.0.0.219 "ss -tnp | grep ':10700'"
```
