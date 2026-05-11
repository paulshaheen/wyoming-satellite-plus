# wyoming-satellite-plus: Conversation Continuation — Handoff Notes
# Generated 2026-05-11 14:08 EDT by Jarvis

## What works
- wake word ("Hey Jarvis") → STT → Claude → TTS → done chime ✅
- "Turn on/off the lights" → Claude asks "Which lights?" ✅  
- supported_features = 3 (ANNOUNCE | START_CONVERSATION) ✅
- GitHub: https://github.com/paulshaheen/wyoming-satellite-plus ✅

## What does NOT work
After Jarvis asks a follow-up question (e.g. "Which lights?"), the satellite
does NOT automatically open the mic. Paul has to say "Hey Jarvis" again,
which loses conversation context and the done chime doesn't play.

## Root cause, diagnosed
The continuation logic lives in `_stream_tts()` inside
`custom_components/wyoming_satellite_plus/assist_satellite.py`.

After sending AudioStop to the satellite, we check if the TTS text ended
with "?" and if so call `_continue_after_playback()`. But that method was
never fully written — the handoff happened mid-edit.

The satellite does NOT reliably send a `Played` event back through the
wyoming TCP connection (the audio plays via a separate SSH paplay process).
So we cannot use `Played` as the trigger. We must trigger from `_stream_tts`
after `AudioStop` is sent.

## What was in progress at handoff
`_stream_tts` was edited to call `self._continue_after_playback(playback_wait)`
after AudioStop — but `_continue_after_playback()` was never written.

Also needed: after `_continue_after_playback` fires, the continuation must
NOT clear `self._conversation_id` (base class attribute) — we need the same
conversation session so Claude has context for the follow-up answer.

## Files involved
All on HA host at: /homeassistant/custom_components/wyoming_satellite_plus/
Primary file: assist_satellite.py
Local copy:   workspace/wyoming-satellite-plus/custom_components/wyoming_satellite_plus/assist_satellite.py

## What needs to be written

### 1. `_continue_after_playback(playback_wait: float)` method
```python
async def _continue_after_playback(self, playback_wait: float) -> None:
    """Wait for pipeline to end + audio to finish playing, then open mic."""
    # Wait for pipeline RUN_END (base class sets _is_pipeline_running=False
    # and fires _pipeline_ended_event on RUN_END pipeline event).
    try:
        async with asyncio.timeout(10):
            await self._pipeline_ended_event.wait()
    except TimeoutError:
        _LOGGER.warning("_continue_after_playback: pipeline did not end in time")
        return

    # Extra buffer so satellite finishes playing the TTS audio
    await asyncio.sleep(playback_wait)

    # Now open the mic for follow-up
    await self._start_conversation_after_question()
```

### 2. `_start_conversation_after_question()` already exists — keep as-is
It sends RunPipeline(ASR) to the satellite and calls _run_pipeline_once().
The base class `async_accept_pipeline_from_satellite` will reuse
`self._conversation_id` automatically (set from the previous pipeline run),
so Claude gets context.

### 3. Verify `_last_tts_text` is captured correctly
In `on_pipeline_event` at `TTS_START`:
    self._last_tts_text = event.data.get("tts_input", "")

In `_stream_tts` after AudioStop is sent:
    tts_text = self._last_tts_text
    if tts_text.rstrip().endswith("?"):
        self._last_tts_text = ""   # consume
        playback_wait = max(total_seconds, 0.5) + 0.5
        self.config_entry.async_create_background_task(
            self.hass,
            self._continue_after_playback(playback_wait),
            "wyoming satellite plus conversation continuation",
        )

## Deploy procedure (NO full HA restart needed)
1. Edit the file locally
2. scp to jarvis-lxc (root@10.0.0.219) → /tmp/assist_satellite.py
3. cat /tmp/assist_satellite.py | ssh noodles@10.0.0.54 'sudo tee /homeassistant/custom_components/wyoming_satellite_plus/assist_satellite.py > /dev/null'
4. ssh noodles@10.0.0.54 'sudo rm -rf /homeassistant/custom_components/wyoming_satellite_plus/__pycache__'
5. Call HA service: homeassistant.reload_all  (NOT a full restart — flushes sys.modules for custom components)

## Key constants / IDs
- HA host: 10.0.0.54:8123
- Satellite host: 10.0.0.219:10700
- Config entry ID: 01KRC0A7SPJZ1XVRZ2X2WN3YVQ
- Entity: assist_satellite.jarvis
- Pipeline: Jarvis (id 01kr8v407r3jehzxmsrhgdnzjy) — STT=Whisper, LLM=Claude via ha-proxy, TTS=Google en-GB-Neural2-D
- Secrets: C:\Users\pwsha\.openclaw\secrets\homeassistant.json

## Other known issues
- `_run_pipeline_once` takes a RunPipeline object. For continuation we pass
  one with start_stage=ASR, end_stage=TTS. This works.
- The satellite sends RunPipeline(ASR) back to HA when we tell it to open mic.
  The `_run_pipeline_loop` has a guard (`_start_conversation_active` flag or
  similar) to avoid double-running the pipeline. Verify this is still in place.
- The wyoming-satellite `--done-wav` plays AFTER the satellite receives AudioStop
  and finishes draining the paplay buffer. The playback_wait calculation
  (total_seconds + 0.5s) should be long enough, but test with a short question
  like "Which lights?" (~0.8s audio) and a longer one (~3s) to confirm.

## Test sequence
1. "Hey Jarvis, turn off the lights"
2. Jarvis: "Which lights?" + done chime
3. [mic should open automatically — awake sound should play]
4. Say "office light"
5. Jarvis should confirm and turn off office light
6. No re-wake needed for steps 3-5
