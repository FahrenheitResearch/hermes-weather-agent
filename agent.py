#!/usr/bin/env python3
"""
Hermes Weather Agent — Interactive AI weather assistant with tool calling.

Uses OpenRouter API with Hermes model for tool-calling, backed by
rustmet + metrust for fast weather data processing.

Usage:
    set OPENROUTER_API_KEY=your_key
    python agent.py
    python agent.py "Build me a 1-week HRRR training dataset with SB3CAPE"
    python agent.py "Show me the current radar for KTLX"
    python agent.py "What does the CAPE look like right now?"
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
MODEL = os.environ.get("OPENROUTER_MODEL", "openrouter/hunter-alpha")

SYSTEM_PROMPT = """You are a meteorological AI agent with weather data tools.

IMPORTANT RULES:
- After rendering a map/image, say NOTHING. Just render it. The user will ask for more if they want.
- Never explain what you did after showing a map. Just show it.
- Be extremely terse. No markdown tables, no bullet lists unless asked.

You can render any HRRR field to terminal, compute derived fields (CAPE, shear, SRH, STP) from native 3D data using metrust, build ML training datasets, show radar and soundings.

For wx_render_terminal, use exact GRIB variable names:
- 'Convective Available Potential Energy' (CAPE)
- 'Convective Inhibition' (CIN)
- 'Storm Relative Helicity' (SRH)
- 'Composite Reflectivity'
- 'Temperature' with level '2 Specified Height Level Above Ground' (2m temp)
- 'Dewpoint Temperature' with level '2 Specified Height Level Above Ground'
- 'Wind Speed (Gust)'

For derived fields computed by metrust from native 3D data: 'sbcape', 'sb3cape', 'mlcape', 'shear_01', 'shear_03', 'shear_06', 'srh_01', 'srh_03', 'stp'

For regional crops, use wx_render_terminal with lat/lon/radius parameters if available, or tell the user to crop isn't supported yet.

Only call ONE tool per user request unless they explicitly ask for multiple."""

# Import tool dispatch
from mcp_server import _dispatch, list_tools

# Build OpenAI-format tool definitions
import asyncio
_tools_list = asyncio.run(list_tools())
TOOLS = []
for t in _tools_list:
    TOOLS.append({
        "type": "function",
        "function": {
            "name": t.name,
            "description": t.description,
            "parameters": t.inputSchema,
        }
    })


def call_openrouter(messages: list[dict], api_key: str) -> dict:
    """Call OpenRouter chat completions API with tools."""
    resp = requests.post(
        OPENROUTER_URL,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": MODEL,
            "messages": messages,
            "tools": TOOLS,
            "tool_choice": "auto",
            "max_tokens": 4096,
        },
        timeout=300,
    )
    resp.raise_for_status()
    return resp.json()


def run_agent(user_input: str, api_key: str):
    """Run the agent loop — call model, execute tools, repeat."""
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_input},
    ]

    print(f"\033[1;36mYou:\033[0m {user_input}\n")

    while True:
        response = call_openrouter(messages, api_key)
        choice = response["choices"][0]
        msg = choice["message"]

        # Check for tool calls
        tool_calls = msg.get("tool_calls")
        if tool_calls:
            messages.append(msg)

            for tc in tool_calls:
                fn_name = tc["function"]["name"]
                fn_args = json.loads(tc["function"]["arguments"])

                print(f"\033[1;33m[Tool: {fn_name}]\033[0m")
                for k, v in fn_args.items():
                    print(f"  {k}: {v}")

                # Execute via dispatch
                try:
                    result = _dispatch(fn_name, fn_args)
                    # If result has ANSI art, print it and don't send the huge string back
                    if isinstance(result, dict) and result.get("rendered"):
                        # Map was already printed to terminal by the tool
                        return
                    else:
                        result_str = json.dumps(result, default=str) if isinstance(result, dict) else str(result)
                        print(f"\033[0;32m  Result: {result_str[:200]}{'...' if len(result_str) > 200 else ''}\033[0m\n")
                except Exception as e:
                    result_str = f"Error: {e}"
                    print(f"\033[0;31m  {result_str}\033[0m\n")

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": result_str,
                })

            continue

        # Regular text response
        content = msg.get("content", "")
        if content:
            safe = content.encode("ascii", "replace").decode("ascii")
            print(f"\033[1;35mAgent:\033[0m {safe}\n")
        break


def interactive_loop(api_key: str):
    """Interactive chat loop."""
    print("\033[1;36m=== Hermes Weather Agent ===\033[0m")
    print("Ask me about weather, radar, soundings, or building training datasets.")
    print("Type 'quit' to exit.\n")

    while True:
        try:
            user_input = input("\033[1;36mYou:\033[0m ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not user_input or user_input.lower() in ("quit", "exit", "q"):
            break
        run_agent(user_input, api_key)


def main():
    # Load .env if present
    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        print("Set OPENROUTER_API_KEY environment variable")
        print("  export OPENROUTER_API_KEY=your_key")
        sys.exit(1)

    if len(sys.argv) > 1:
        # Single command mode
        run_agent(" ".join(sys.argv[1:]), api_key)
    else:
        # Interactive mode
        interactive_loop(api_key)


if __name__ == "__main__":
    main()
