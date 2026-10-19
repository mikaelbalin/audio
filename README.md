# audio

Unified audio capture and transcription layer for the Tauri apps in this
monorepo (`chatbot`, `speech-transcription`). It is the single source of truth
for audio work: capture (microphone / system output), provider selection
(Mistral cloud vs. local models), and model management (catalog, download,
verification).

## What it does

- **Capture** — streams `pcm_s16le` (mono, 16 kHz) from a PulseAudio/PipeWire
  source (`parec` under the hood): either the microphone or a system `.monitor`
  source.
- **Transcription providers**:
  - `MistralProvider` — cloud realtime ASR over WebSocket.
  - `LocalProvider` — offline/streaming ASR in-process via
    [`transcribe-cpp`](https://github.com/handy-computer/transcribe.cpp)
    (GGUF models on the ggml runtime).
- **Model store** — a static catalog of downloadable GGUF models, with
  pinned-revision downloads and SHA-256 verification.
- **Protocol** — the shared `ClientMessage`/`ServerMessage` wire contract used
  by both the Tauri IPC and the standalone WebSocket server.

## Supported local models

All local models are **streaming** and emit punctuation/capitalization natively:

| Catalog id | Model | Languages |
| --- | --- | --- |
| `parakeet-unified-en-0.6b` | Parakeet Unified EN 0.6B | English |
| `nemotron-3.5-asr-streaming-0.6b` | Nemotron Streaming 3.5 | 28 |
| `voxtral-mini-4b-realtime` | Voxtral Mini 4B Realtime | 13 |

Models are hosted under
[`handy-computer`](https://huggingface.co/handy-computer) on Hugging Face. A
model is a single GGUF file downloaded into
`~/.local/share/kaelib/audio/models/<id>/` (override with `AUDIO_MODELS_DIR`).

## Prerequisites

Building this crate compiles the native `transcribe.cpp` C++ core (ggml) from
source, which requires a C++ toolchain, **CMake**, and (for GPU acceleration)
the **Vulkan SDK**. The build enables the Vulkan backend by default (AMD / NVIDIA
/ Intel GPUs on Linux).

### Fedora / Bazzite

Bazzite is an immutable, Fedora-based distribution. Install the build tools via
Homebrew (Linuxbrew), which keeps them in the user's home and does not touch the
immutable base system:

```bash
brew install cmake shaderc vulkan-headers vulkan-loader
```

- `cmake` — builds the native C++ core.
- `shaderc` — provides `glslc`, the Vulkan shader compiler ggml needs.
- `vulkan-headers` — Vulkan API headers.
- `vulkan-loader` — the `libvulkan.so` loader the ggml Vulkan backend links.

`gcc`/`g++` are already present as system packages.

### Other distros

- **Debian/Ubuntu:** `sudo apt install build-essential cmake libvulkan-dev glslang-tools`
- **Fedora:** `sudo dnf install cmake gcc-c++ vulkan-loader-devel glslang`
- **Arch:** `sudo pacman -S cmake base-devel vulkan-headers shaderc`

## Build & test

```bash
cargo check --lib        # type-check the library
cargo test --lib         # unit tests
cargo build --features server   # build the WebSocket server binary
```

## Usage

See [`src/protocol.rs`](src/protocol.rs) for the wire contract and
[`src/lib.rs`](src/lib.rs) for the public module layout.
