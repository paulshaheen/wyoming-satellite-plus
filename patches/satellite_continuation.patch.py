#!/usr/bin/env python3
"""Patch the wyoming_satellite package's satellite.py so that when
the WakeStreamingSatellite receives a RunPipeline event from the server with
start_stage=ASR, it bypasses wake-word detection and goes straight into
streaming mode (mimics post-wake-word path). This enables conversation
continuation: HA can reopen the satellite mic after a follow-up question
without forcing the user to re-say "Hey Jarvis".

Idempotent: detects existing patch by sentinel string and exits.

IMPORTANT: the systemd unit runs `/opt/wyoming-satellite/.venv/bin/python -m
wyoming_satellite`, which loads the package from the venv's site-packages.
Patching the source tree at /opt/wyoming-satellite/wyoming_satellite/ does
NOTHING. We must patch the venv copy.
"""
import io, sys, re, os

# The venv-installed copy is the one actually executed by the service.
PATH = "/opt/wyoming-satellite/.venv/lib/python3.11/site-packages/wyoming_satellite/satellite.py"
SENTINEL = "# WSP_CONTINUATION_PATCH_v1"

src = open(PATH).read()
if SENTINEL in src:
    print("Already patched (sentinel found). Nothing to do.")
    sys.exit(0)

# Find "class WakeStreamingSatellite" and inject a pre-handler block at the
# very top of its event_from_server.
needle = (
    "    async def event_from_server(self, event: Event) -> None:\n"
    "        # Only check event types once\n"
    "        is_run_satellite = False\n"
)

if needle not in src:
    print("ERROR: anchor block not found; refusing to patch.", file=sys.stderr)
    sys.exit(2)

inject = (
    "    async def event_from_server(self, event: Event) -> None:\n"
    "        " + SENTINEL + "\n"
    "        # If server tells us to run a pipeline starting at ASR (or WAKE\n"
    "        # with the satellite already in wake mode), treat it as a\n"
    "        # server-initiated mic-open: skip wake-word detection and\n"
    "        # immediately enter streaming mode. Used for conversation\n"
    "        # continuation after an Assist follow-up question.\n"
    "        if RunPipeline.is_type(event.type) and not self.is_streaming and not self._is_paused:\n"
    "            try:\n"
    "                rp = RunPipeline.from_event(event)\n"
    "            except Exception:\n"
    "                rp = None\n"
    "            if rp is not None and rp.start_stage == PipelineStage.ASR:\n"
    "                _LOGGER.info('WSP: server-initiated streaming start (continuation)')\n"
    "                # Stop debug recording (wake), start (stt)\n"
    "                if self.wake_audio_writer is not None:\n"
    "                    self.wake_audio_writer.stop()\n"
    "                self._debug_recording_timestamp = time.monotonic_ns()\n"
    "                if self.stt_audio_writer is not None:\n"
    "                    self.stt_audio_writer.start(timestamp=self._debug_recording_timestamp)\n"
    "                self.is_streaming = True\n"
    "                await self.trigger_streaming_start()\n"
    "                return\n"
    "        # Only check event types once\n"
    "        is_run_satellite = False\n"
)

new = src.replace(needle, inject, 1)
if new == src:
    print("ERROR: replace failed.", file=sys.stderr)
    sys.exit(3)

# Make sure `time` is imported at module scope (it is — wyoming_satellite uses
# time.monotonic). Sanity check:
if "\nimport time" not in new and "import time\n" not in new:
    # add it
    new = new.replace("import asyncio\n", "import asyncio\nimport time\n", 1)

open(PATH, "w").write(new)
print("Patched OK. Size now:", len(new))
