# Wyoming Satellite Plus

A custom Home Assistant integration that extends the built-in Wyoming integration to add `START_CONVERSATION` support — enabling true conversation continuation without re-triggering the wake word.

## Why this exists

HA core's `wyoming` integration hardcodes:

```python
_attr_supported_features = AssistSatelliteEntityFeature.ANNOUNCE
```

No matter what the Wyoming satellite advertises, the entity is capped at `supported_features = 1` (ANNOUNCE only). This means `assist_satellite.start_conversation` is unavailable, so follow-up questions after an agent response require the user to re-say the wake word.

This integration forks the wyoming component under domain `wyoming_satellite_plus` and adds:
- `AssistSatelliteEntityFeature.START_CONVERSATION` to `_attr_supported_features`
- A working `async_start_conversation()` implementation that plays the announcement then immediately opens the mic for STT (no wake word required)

## Installation

### Manual (recommended for personal use)

1. Copy `custom_components/wyoming_satellite_plus/` into your HA config's `custom_components/` directory
2. Restart Home Assistant
3. Add integration: Settings → Integrations → Add Integration → "Wyoming Satellite Plus"
4. Point it at your Wyoming satellite host/port (e.g. `10.0.0.219:10700`)

### HACS

Add this repository as a custom HACS repository (Integration category), then install normally.

## What's different from core wyoming

- Domain: `wyoming_satellite_plus` (coexists with core `wyoming`)
- `supported_features` = `ANNOUNCE | START_CONVERSATION` = 3
- `async_start_conversation()`: plays the start announcement, then runs a pipeline cycle starting from STT stage (bypasses wake word)
- Everything else is identical to core wyoming as of HA 2024.x

## Usage

Once installed and an entity is created, you can call:

```yaml
service: assist_satellite.start_conversation
target:
  entity_id: assist_satellite.wyoming_satellite_plus_jarvis
data:
  start_message: "What would you like to know?"
```

Or configure your pipeline's conversation agent to call it automatically after responding.

## Notes

- Requires Wyoming satellite running with `--wake-word-uri` pointing at an OpenWakeWord server
- The existing wyoming satellite + openwakeword services on jarvis-lxc (10.0.0.219) work as-is
- Uses the same pipeline, STT, TTS, and conversation agent configured in HA
