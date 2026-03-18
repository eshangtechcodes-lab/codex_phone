# Codex Phone 📱

Mobile control panel for [Codex CLI](https://github.com/openai/codex) — chat with GPT from your phone.

## Features

- 🗣️ **Chat UI** — Clean dark-themed mobile chat interface
- 🔌 **WebSocket Proxy** — Bridges your phone to Codex app-server
- 🎤 **Voice Input** — Speech-to-text via Web Speech API
- 🔄 **Thread Management** — Switch between conversations, auto-restore on refresh
- 🤖 **Model Selection** — Choose from gpt-5.4, gpt-5.4-mini, etc.
- 🌐 **OpenAI-Compatible API** — `POST /v1/chat/completions` for LAN services
- 📱 **PWA** — Add to home screen for native app experience

## Quick Start

```bash
npm install
node server.js
```

Open `http://localhost:3002` on your phone (same WiFi).

## REST API

Other services on your LAN can call this as a drop-in OpenAI replacement:

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://YOUR_IP:3002/v1",
    api_key="not-needed"
)

response = client.chat.completions.create(
    model="gpt-5.4-mini",
    messages=[{"role": "user", "content": "hello"}]
)
print(response.choices[0].message.content)
```

## Prerequisites

- Node.js 18+
- [Codex CLI](https://github.com/openai/codex) installed and logged in
