#!/usr/bin/env python3
"""Live weather display viewer — watches /tmp/wx_display.ansi for updates.

Run in a second terminal alongside hermes chat:
    python wx_viewer.py

When hermes renders a weather map, it appears here instantly.
"""
import os
import sys
import time

DISPLAY_FILE = "/tmp/wx_display.ansi"
last_mtime = 0
last_content = ""

print("\033[1;36m=== Weather Display ===\033[0m")
print("Waiting for renders from hermes chat...\n")

try:
    while True:
        if os.path.exists(DISPLAY_FILE):
            mtime = os.path.getmtime(DISPLAY_FILE)
            if mtime != last_mtime:
                last_mtime = mtime
                content = open(DISPLAY_FILE).read()
                if content != last_content:
                    last_content = content
                    # Clear screen and render
                    sys.stdout.write("\033[2J\033[H")
                    sys.stdout.write("\033[1;36m=== Weather Display ===\033[0m\n\n")
                    sys.stdout.write(content)
                    sys.stdout.write("\n")
                    sys.stdout.flush()
        time.sleep(0.3)
except KeyboardInterrupt:
    pass
