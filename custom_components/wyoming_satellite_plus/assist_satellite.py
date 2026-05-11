"""Assist satellite entity for Wyoming Satellite Plus integration.

This is a fork of homeassistant/components/wyoming/assist_satellite.py with
one key change: we add AssistSatelliteEntityFeature.START_CONVERSATION and
implement async_start_conversation() so HA can trigger conversation continuation
without requiring a wake word re-trigger.

The implementation works by:
1. Playing the start announcement via async_announce (existing path)
2. Sending RunPipeline(start_stage=ASR) directly to the Wyoming satellite,
   bypassing wake word detection, so the satellite immediately opens its mic
   and streams audio to HA starting at STT stage.
"""

import asyncio
from collections.abc import AsyncGenerator
import io
import logging
import time
from typing import Any, Final
import wave

from wyoming.asr import Transcribe, Transcript
from wyoming.audio import AudioChunk, AudioChunkConverter, AudioStart, AudioStop
from wyoming.client import AsyncTcpClient
from wyoming.error import Error
from wyoming.event import Event
from wyoming.info import Describe, Info
from wyoming.ping import Ping, Pong
from wyoming.pipeline import PipelineStage, RunPipeline
from wyoming.satellite import PauseSatellite, RunSatellite
from wyoming.snd import Played
from wyoming.timer import TimerCancelled, TimerFinished, TimerStarted, TimerUpdated
from wyoming.tts import Synthesize, SynthesizeVoice
from wyoming.vad import VoiceStarted, VoiceStopped
from wyoming.wake import Detect, Detection

from homeassistant.components import assist_pipeline, ffmpeg, intent, tts
from homeassistant.components.assist_pipeline import PipelineEvent
from homeassistant.components.assist_satellite import (
    AssistSatelliteAnnouncement,
    AssistSatelliteConfiguration,
    AssistSatelliteEntity,
    AssistSatelliteEntityDescription,
    AssistSatelliteEntityFeature,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.util.ulid import ulid_now

from .const import SAMPLE_CHANNELS, SAMPLE_WIDTH
from .data import WyomingService
from .devices import SatelliteDevice
from .entity import WyomingSatelliteEntity
from .models import WyomingConfigEntry

_LOGGER = logging.getLogger(__name__)

_SAMPLES_PER_CHUNK: Final = 1024
_RECONNECT_SECONDS: Final = 10
_RESTART_SECONDS: Final = 3
_PING_TIMEOUT: Final = 5
_PING_SEND_DELAY: Final = 2
_PIPELINE_FINISH_TIMEOUT: Final = 1
_TTS_SAMPLE_RATE: Final = 22050
_AUDIO_CHUNK_BYTES: Final = 2048  # 1024 samples
_TTS_TIMEOUT_EXTRA: Final = 1.0

# Wyoming stage -> Assist stage
_STAGES: dict[PipelineStage, assist_pipeline.PipelineStage] = {
    PipelineStage.WAKE: assist_pipeline.PipelineStage.WAKE_WORD,
    PipelineStage.ASR: assist_pipeline.PipelineStage.STT,
    PipelineStage.HANDLE: assist_pipeline.PipelineStage.INTENT,
    PipelineStage.TTS: assist_pipeline.PipelineStage.TTS,
}


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: WyomingConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up Wyoming Satellite Plus Assist satellite entity."""
    domain_data = config_entry.runtime_data
    assert domain_data.device is not None

    async_add_entities(
        [
            WyomingAssistSatellite(
                hass, domain_data.service, domain_data.device, config_entry
            )
        ]
    )


class WyomingAssistSatellite(WyomingSatelliteEntity, AssistSatelliteEntity):
    """Assist satellite for Wyoming devices — with START_CONVERSATION support."""

    entity_description = AssistSatelliteEntityDescription(key="assist_satellite")
    _attr_translation_key = "assist_satellite"
    _attr_name = None

    # KEY CHANGE: add START_CONVERSATION so HA exposes the service
    _attr_supported_features = (
        AssistSatelliteEntityFeature.ANNOUNCE
        | AssistSatelliteEntityFeature.START_CONVERSATION
    )

    def __init__(
        self,
        hass: HomeAssistant,
        service: WyomingService,
        device: SatelliteDevice,
        config_entry: WyomingConfigEntry,
    ) -> None:
        """Initialize an Assist satellite."""
        WyomingSatelliteEntity.__init__(self, device)
        AssistSatelliteEntity.__init__(self)

        self.service = service
        self.device = device
        self.config_entry = config_entry

        self.is_running = True

        self._client: AsyncTcpClient | None = None
        self._chunk_converter = AudioChunkConverter(rate=16000, width=2, channels=1)
        self._is_pipeline_running = False
        self._pipeline_ended_event = asyncio.Event()
        self._audio_queue: asyncio.Queue[bytes | None] = asyncio.Queue()
        self._pipeline_id: str | None = None
        self._muted_changed_event = asyncio.Event()

        self._conversation_id: str | None = None
        self._conversation_id_time: float | None = None

        self.device.set_is_muted_listener(self._muted_changed)
        self.device.set_pipeline_listener(self._pipeline_changed)
        self.device.set_audio_settings_listener(self._audio_settings_changed)

        # For announcements
        self._ffmpeg_manager: ffmpeg.FFmpegManager | None = None
        self._played_event_received: asyncio.Event | None = None

        # Randomly set on each pipeline loop run.
        # Used to ensure TTS timeout is acted on correctly.
        self._run_loop_id: str | None = None

        # TTS streaming
        self._tts_stream_token: str | None = None
        self._is_tts_streaming: bool = False

        # For start_conversation: signals the run loop to skip wake word
        self._start_conversation_event: asyncio.Event = asyncio.Event()

    @property
    def pipeline_entity_id(self) -> str | None:
        """Return the entity ID of the pipeline to use for the next conversation."""
        return self.device.get_pipeline_entity_id(self.hass)

    @property
    def vad_sensitivity_entity_id(self) -> str | None:
        """Return the entity ID of the VAD sensitivity to use for the next conversation."""
        return self.device.get_vad_sensitivity_entity_id(self.hass)

    @property
    def tts_options(self) -> dict[str, Any] | None:
        """Options passed for text-to-speech."""
        return {
            tts.ATTR_PREFERRED_FORMAT: "wav",
            tts.ATTR_PREFERRED_SAMPLE_RATE: _TTS_SAMPLE_RATE,
            tts.ATTR_PREFERRED_SAMPLE_CHANNELS: SAMPLE_CHANNELS,
            tts.ATTR_PREFERRED_SAMPLE_BYTES: SAMPLE_WIDTH,
        }

    async def async_added_to_hass(self) -> None:
        """Run when entity about to be added to hass."""
        await super().async_added_to_hass()
        self.start_satellite()

    async def async_will_remove_from_hass(self) -> None:
        """Run when entity will be removed from hass."""
        await super().async_will_remove_from_hass()
        self.stop_satellite()

    @callback
    def async_get_configuration(
        self,
    ) -> AssistSatelliteConfiguration:
        """Get the current satellite configuration."""
        raise NotImplementedError

    async def async_set_configuration(
        self, config: AssistSatelliteConfiguration
    ) -> None:
        """Set the current satellite configuration."""
        raise NotImplementedError

    def on_pipeline_event(self, event: PipelineEvent) -> None:
        """Set state based on pipeline stage."""
        if event.type == assist_pipeline.PipelineEventType.RUN_END:
            self._is_pipeline_running = False
            self._pipeline_ended_event.set()
            self.device.set_is_active(False)
            self._tts_stream_token = None
            self._is_tts_streaming = False

        if self._client is None:
            return

        if event.type == assist_pipeline.PipelineEventType.RUN_START:
            if event.data and (tts_output := event.data["tts_output"]):
                self._tts_stream_token = tts_output["token"]
                self._is_tts_streaming = False
        elif event.type == assist_pipeline.PipelineEventType.WAKE_WORD_START:
            self.config_entry.async_create_background_task(
                self.hass,
                self._client.write_event(Detect().event()),
                f"{self.entity_id} {event.type}",
            )
        elif event.type == assist_pipeline.PipelineEventType.WAKE_WORD_END:
            if event.data and (wake_word_output := event.data.get("wake_word_output")):
                detection = Detection(
                    name=wake_word_output["wake_word_id"],
                    timestamp=wake_word_output.get("timestamp"),
                )
                self.config_entry.async_create_background_task(
                    self.hass,
                    self._client.write_event(detection.event()),
                    f"{self.entity_id} {event.type}",
                )
        elif event.type == assist_pipeline.PipelineEventType.STT_START:
            self.device.set_is_active(True)

            if event.data:
                self.config_entry.async_create_background_task(
                    self.hass,
                    self._client.write_event(
                        Transcribe(language=event.data["metadata"]["language"]).event()
                    ),
                    f"{self.entity_id} {event.type}",
                )
        elif event.type == assist_pipeline.PipelineEventType.STT_VAD_START:
            if event.data:
                self.config_entry.async_create_background_task(
                    self.hass,
                    self._client.write_event(
                        VoiceStarted(timestamp=event.data["timestamp"]).event()
                    ),
                    f"{self.entity_id} {event.type}",
                )
        elif event.type == assist_pipeline.PipelineEventType.STT_VAD_END:
            if event.data:
                self.config_entry.async_create_background_task(
                    self.hass,
                    self._client.write_event(
                        VoiceStopped(timestamp=event.data["timestamp"]).event()
                    ),
                    f"{self.entity_id} {event.type}",
                )
        elif event.type == assist_pipeline.PipelineEventType.STT_END:
            if event.data:
                stt_text = event.data["stt_output"]["text"]
                self.config_entry.async_create_background_task(
                    self.hass,
                    self._client.write_event(Transcript(text=stt_text).event()),
                    f"{self.entity_id} {event.type}",
                )
        elif event.type == assist_pipeline.PipelineEventType.INTENT_PROGRESS:
            if (
                event.data
                and event.data.get("tts_start_streaming")
                and self._tts_stream_token
                and (stream := tts.async_get_stream(self.hass, self._tts_stream_token))
            ):
                self._is_tts_streaming = True
                self.config_entry.async_create_background_task(
                    self.hass,
                    self._stream_tts(stream),
                    f"{self.entity_id} {event.type}",
                )
        elif event.type == assist_pipeline.PipelineEventType.TTS_START:
            if event.data:
                self.config_entry.async_create_background_task(
                    self.hass,
                    self._client.write_event(
                        Synthesize(
                            text=event.data["tts_input"],
                            voice=SynthesizeVoice(
                                name=event.data.get("voice"),
                                language=event.data.get("language"),
                            ),
                        ).event()
                    ),
                    f"{self.entity_id} {event.type}",
                )
        elif event.type == assist_pipeline.PipelineEventType.TTS_END:
            if (
                event.data
                and (tts_output := event.data["tts_output"])
                and not self._is_tts_streaming
                and (stream := tts.async_get_stream(self.hass, tts_output["token"]))
            ):
                self.config_entry.async_create_background_task(
                    self.hass,
                    self._stream_tts(stream),
                    f"{self.entity_id} {event.type}",
                )
        elif event.type == assist_pipeline.PipelineEventType.ERROR:
            if event.data:
                self.config_entry.async_create_background_task(
                    self.hass,
                    self._client.write_event(
                        Error(
                            text=event.data["message"], code=event.data["code"]
                        ).event()
                    ),
                    f"{self.entity_id} {event.type}",
                )

    async def async_announce(self, announcement: AssistSatelliteAnnouncement) -> None:
        """Announce media on the satellite.

        Should block until the announcement is done playing.
        """
        if self._client is None:
            raise ConnectionError("Satellite is not connected")

        if self._ffmpeg_manager is None:
            self._ffmpeg_manager = ffmpeg.get_ffmpeg_manager(self.hass)

        if self._played_event_received is None:
            self._played_event_received = asyncio.Event()

        self._played_event_received.clear()
        await self._client.write_event(
            AudioStart(
                rate=_TTS_SAMPLE_RATE,
                width=SAMPLE_WIDTH,
                channels=SAMPLE_CHANNELS,
                timestamp=0,
            ).event()
        )

        timestamp = 0
        try:
            proc = await asyncio.create_subprocess_exec(
                self._ffmpeg_manager.binary,
                "-i",
                announcement.media_id,
                "-f",
                "s16le",
                "-ac",
                str(SAMPLE_CHANNELS),
                "-ar",
                str(_TTS_SAMPLE_RATE),
                "-nostats",
                "pipe:",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                close_fds=False,
            )
            assert proc.stdout is not None
            while True:
                chunk_bytes = await proc.stdout.read(_AUDIO_CHUNK_BYTES)
                if not chunk_bytes:
                    break

                chunk = AudioChunk(
                    rate=_TTS_SAMPLE_RATE,
                    width=SAMPLE_WIDTH,
                    channels=SAMPLE_CHANNELS,
                    audio=chunk_bytes,
                    timestamp=timestamp,
                )
                await self._client.write_event(chunk.event())

                timestamp += chunk.milliseconds
        finally:
            await self._client.write_event(AudioStop().event())
            if timestamp > 0:
                audio_seconds = timestamp / 1000
                try:
                    async with asyncio.timeout(audio_seconds + 0.5):
                        await self._played_event_received.wait()
                except TimeoutError:
                    _LOGGER.debug("Did not receive played event for announcement")

    async def async_start_conversation(
        self, start_announcement: AssistSatelliteAnnouncement
    ) -> None:
        """Start a conversation without requiring a wake word.

        1. Play the start announcement (preannounce chime + TTS via async_announce).
        2. Send RunPipeline(start_stage=ASR) to the satellite so it skips wake word
           detection and opens the mic immediately.
        3. The satellite streams audio back and async_accept_pipeline_from_satellite
           runs the pipeline from STT stage.

        The base class (AssistSatelliteEntity.async_internal_start_conversation) has
        already set up _conversation_id and _extra_system_prompt before calling here.
        """
        if self._client is None:
            raise ConnectionError("Satellite is not connected")

        # Step 1: play the announcement (blocks until done)
        await self.async_announce(start_announcement)

        # Step 2: ask the satellite to run from ASR (skip wake word).
        # The satellite's on_run_pipeline handler will open the mic and stream
        # back audio chunks which get picked up by our normal _handle_audio loop.
        _LOGGER.debug("Sending RunPipeline(ASR) to satellite for conversation start")
        await self._client.write_event(
            RunPipeline(
                start_stage=PipelineStage.ASR,
                end_stage=PipelineStage.TTS,
            ).event()
        )

        # Step 3: signal the run loop that the next pipeline should start from STT
        self._start_conversation_event.set()

        # Step 4: wait for the pipeline to complete (the run loop will drive it)
        self._pipeline_ended_event.clear()
        try:
            # Give a generous timeout — STT + intent + TTS can take 10-30s
            async with asyncio.timeout(60):
                await self._pipeline_ended_event.wait()
        except TimeoutError:
            _LOGGER.warning("Conversation pipeline timed out")

    # -------------------------------------------------------------------------

    def start_satellite(self) -> None:
        """Start satellite task."""
        self.is_running = True

        self.config_entry.async_create_background_task(
            self.hass, self.run(), "wyoming satellite plus run"
        )

    def stop_satellite(self) -> None:
        """Signal satellite task to stop running."""
        self._audio_queue.put_nowait(None)
        self._send_pause()
        self.is_running = False
        self._muted_changed_event.set()

    # -------------------------------------------------------------------------

    async def run(self) -> None:
        """Run and maintain a connection to satellite."""
        _LOGGER.debug("Running satellite task")

        unregister_timer_handler = intent.async_register_timer_handler(
            self.hass, self.device.device_id, self._handle_timer
        )

        try:
            while self.is_running:
                try:
                    while self.device.is_muted:
                        _LOGGER.debug("Satellite is muted")
                        await self.on_muted()
                        if not self.is_running:
                            return

                    await self._connect_and_loop()
                except asyncio.CancelledError:
                    raise
                except Exception as err:  # noqa: BLE001
                    _LOGGER.debug("%s: %s", err.__class__.__name__, str(err))

                    self._audio_queue.put_nowait(None)
                    await self._cancel_running_pipeline()
                    self.device.set_is_active(False)
                    await self.on_restart()
        finally:
            unregister_timer_handler()
            await self._cancel_running_pipeline()
            self.device.set_is_active(False)
            await self.on_stopped()

    async def on_restart(self) -> None:
        """Block until pipeline loop will be restarted."""
        _LOGGER.warning(
            "Satellite has been disconnected. Reconnecting in %s second(s)",
            _RECONNECT_SECONDS,
        )
        await asyncio.sleep(_RESTART_SECONDS)

    async def on_reconnect(self) -> None:
        """Block until a reconnection attempt should be made."""
        _LOGGER.debug(
            "Failed to connect to satellite. Reconnecting in %s second(s)",
            _RECONNECT_SECONDS,
        )
        await asyncio.sleep(_RECONNECT_SECONDS)

    async def on_muted(self) -> None:
        """Block until device may be unmuted again."""
        await self._muted_changed_event.wait()

    async def on_stopped(self) -> None:
        """Run when run() has fully stopped."""
        _LOGGER.debug("Satellite task stopped")

    # -------------------------------------------------------------------------

    async def _connect_and_loop(self) -> None:
        """Connect to satellite and run event loop."""
        self._client = None

        try:
            async with AsyncTcpClient(self.service.host, self.service.port) as client:
                self._client = client
                _LOGGER.debug("Connected to satellite")

                # Describe -> Info
                await client.write_event(Describe().event())

                while True:
                    event = await client.read_event()
                    if event is None:
                        raise ConnectionError("Satellite disconnected")
                    if Info.is_type(event.type):
                        break

                # Tell satellite to start running
                await client.write_event(RunSatellite().event())

                # Main event loop
                await self._run_loop(client)
        finally:
            self._client = None

    async def _run_loop(self, client: AsyncTcpClient) -> None:
        """Run the main satellite event loop."""
        run_loop_id = ulid_now()
        self._run_loop_id = run_loop_id

        # Start ping task
        ping_task = self.config_entry.async_create_background_task(
            self.hass,
            self._ping_loop(client, run_loop_id),
            f"{self.entity_id} ping",
        )

        try:
            while self.is_running:
                event = await client.read_event()
                if event is None:
                    raise ConnectionError("Satellite disconnected")

                if not await self._handle_event(client, event):
                    break
        finally:
            ping_task.cancel()
            with asyncio.timeout(_PING_TIMEOUT):
                try:
                    await ping_task
                except (asyncio.CancelledError, Exception):
                    pass

    async def _ping_loop(self, client: AsyncTcpClient, run_loop_id: str) -> None:
        """Send periodic pings to keep connection alive."""
        while True:
            await asyncio.sleep(_PING_SEND_DELAY)
            if self._run_loop_id != run_loop_id:
                break
            await client.write_event(Ping().event())

    async def _handle_event(self, client: AsyncTcpClient, event: Event) -> bool:
        """Handle a single event from the satellite. Returns False to stop loop."""
        if RunPipeline.is_type(event.type):
            # Satellite wants to run a pipeline
            run_pipeline = RunPipeline.from_event(event)
            _LOGGER.debug("Received RunPipeline: %s", run_pipeline)

            start_stage = _STAGES.get(
                run_pipeline.start_stage, assist_pipeline.PipelineStage.WAKE_WORD
            )
            end_stage = _STAGES.get(
                run_pipeline.end_stage, assist_pipeline.PipelineStage.TTS
            )

            # If start_conversation was triggered, override to STT
            if self._start_conversation_event.is_set():
                self._start_conversation_event.clear()
                start_stage = assist_pipeline.PipelineStage.STT
                _LOGGER.debug("start_conversation override: starting from STT")

            self._pipeline_ended_event.clear()
            self._is_pipeline_running = True

            # Run pipeline in background
            self.config_entry.async_create_background_task(
                self.hass,
                self._run_pipeline(start_stage, end_stage, run_pipeline),
                f"{self.entity_id} pipeline",
            )

        elif Pong.is_type(event.type) or Ping.is_type(event.type):
            pass  # keep-alive, ignore

        elif AudioChunk.is_type(event.type):
            # Audio from satellite mic — push to queue for pipeline
            chunk = AudioChunk.from_event(event)
            chunk = self._chunk_converter.convert(chunk)
            self._audio_queue.put_nowait(chunk.audio)

        elif AudioStop.is_type(event.type):
            # Satellite stopped sending audio
            self._audio_queue.put_nowait(None)

        elif Played.is_type(event.type):
            # Satellite finished playing audio
            if self._played_event_received is not None:
                self._played_event_received.set()

        elif PauseSatellite.is_type(event.type):
            return False

        elif TimerStarted.is_type(event.type):
            self._handle_timer(TimerStarted.from_event(event))
        elif TimerUpdated.is_type(event.type):
            self._handle_timer(TimerUpdated.from_event(event))
        elif TimerFinished.is_type(event.type):
            self._handle_timer(TimerFinished.from_event(event))
        elif TimerCancelled.is_type(event.type):
            self._handle_timer(TimerCancelled.from_event(event))

        return True

    async def _run_pipeline(
        self,
        start_stage: assist_pipeline.PipelineStage,
        end_stage: assist_pipeline.PipelineStage,
        run_pipeline: RunPipeline,
    ) -> None:
        """Run HA assist pipeline using audio from queue."""
        # Clear any old audio
        while not self._audio_queue.empty():
            self._audio_queue.get_nowait()

        wake_word_phrase: str | None = None

        try:
            await self.async_accept_pipeline_from_satellite(
                audio_stream=self._audio_stream(),
                start_stage=start_stage,
                end_stage=end_stage,
                wake_word_phrase=wake_word_phrase,
            )
        except Exception as err:  # noqa: BLE001
            _LOGGER.exception("Pipeline error: %s", err)
        finally:
            self._is_pipeline_running = False
            self._pipeline_ended_event.set()

    async def _audio_stream(self) -> AsyncGenerator[bytes, None]:
        """Yield audio chunks from the queue."""
        while True:
            chunk = await self._audio_queue.get()
            if chunk is None:
                break
            yield chunk

    async def _stream_tts(self, stream: tts.ResultStream) -> None:
        """Stream TTS audio to satellite."""
        if self._client is None:
            return

        await self._client.write_event(
            AudioStart(
                rate=_TTS_SAMPLE_RATE,
                width=SAMPLE_WIDTH,
                channels=SAMPLE_CHANNELS,
                timestamp=0,
            ).event()
        )

        timestamp = 0
        try:
            async for chunk_bytes in stream:
                if not chunk_bytes:
                    break

                # Convert to WAV then extract raw PCM
                with io.BytesIO(chunk_bytes) as wav_io:
                    try:
                        with wave.open(wav_io, "rb") as wav_file:
                            raw_audio = wav_file.readframes(wav_file.getnframes())
                    except Exception:  # noqa: BLE001
                        # Not WAV — use as-is (shouldn't happen with wav format option)
                        raw_audio = chunk_bytes

                chunk = AudioChunk(
                    rate=_TTS_SAMPLE_RATE,
                    width=SAMPLE_WIDTH,
                    channels=SAMPLE_CHANNELS,
                    audio=raw_audio,
                    timestamp=timestamp,
                )
                if self._client is None:
                    return
                await self._client.write_event(chunk.event())
                timestamp += chunk.milliseconds
        finally:
            if self._client is not None:
                await self._client.write_event(AudioStop().event())

    def _send_pause(self) -> None:
        """Send pause event to satellite (fire and forget)."""
        if self._client is not None:
            self.config_entry.async_create_background_task(
                self.hass,
                self._client.write_event(PauseSatellite().event()),
                f"{self.entity_id} pause",
            )

    @callback
    def _muted_changed(self) -> None:
        """Handle mute state change."""
        self._muted_changed_event.set()
        self._muted_changed_event.clear()

    @callback
    def _pipeline_changed(self) -> None:
        """Handle pipeline change — reconnect to pick up new pipeline."""
        self.stop_satellite()
        self.start_satellite()

    @callback
    def _audio_settings_changed(self) -> None:
        """Handle audio settings change."""
        # No-op: settings are read fresh each pipeline run

    def _handle_timer(self, event: Any) -> None:
        """Handle timer events from satellite (pass-through to HA intent)."""
        # Timer events are handled by the intent timer handler registered in run()
        pass

    async def _cancel_running_pipeline(self) -> None:
        """Cancel any running pipeline."""
        # Audio stop sentinel will cause _audio_stream to exit
        self._audio_queue.put_nowait(None)

        if self._is_pipeline_running:
            # Wait briefly for pipeline to end
            try:
                async with asyncio.timeout(_PIPELINE_FINISH_TIMEOUT):
                    await self._pipeline_ended_event.wait()
            except TimeoutError:
                pass
