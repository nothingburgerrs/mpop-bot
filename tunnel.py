"""Optional Cloudflare Tunnel bootstrap.

Wispbyte gives one locked startup command (python main.py), so we can't run
cloudflared as a separate process from the panel. Instead the bot launches it
itself: if CLOUDFLARED_TOKEN is set, this downloads the cloudflared binary once
and runs the tunnel as a subprocess, exposing the dashboard API (127.0.0.1:8787)
to the public hostname configured in the Cloudflare dashboard.

Entirely opt-in and defensive: no token means it does nothing, and any failure
is logged without taking the bot down. The tunnel dials outbound only, so it
needs no inbound port from Wispbyte.
"""

import asyncio
import os
import platform
import stat
import subprocess
import urllib.request

_process = None  # keep a reference so the subprocess isn't dropped


def _binary_asset():
    machine = platform.machine().lower()
    if "aarch64" in machine or "arm64" in machine:
        return "cloudflared-linux-arm64"
    return "cloudflared-linux-amd64"


def _download_binary(dest):
    asset = _binary_asset()
    url = f"https://github.com/cloudflare/cloudflared/releases/latest/download/{asset}"
    print(f"Tunnel: downloading cloudflared ({asset})...")
    tmp = dest + ".tmp"
    urllib.request.urlretrieve(url, tmp)  # noqa: S310 (fixed, trusted URL)
    os.replace(tmp, dest)
    os.chmod(dest, os.stat(dest).st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    print("Tunnel: cloudflared ready.")


def _launch(token):
    global _process
    binary = os.path.join(os.getcwd(), "cloudflared")
    if not os.path.exists(binary):
        _download_binary(binary)

    # Inherit stdout/stderr so tunnel logs show in the Wispbyte console.
    _process = subprocess.Popen(
        [binary, "tunnel", "--no-autoupdate", "run", "--token", token]
    )
    print("Tunnel: cloudflared started.")


async def start():
    """Start the tunnel if configured. Never raises."""
    token = os.getenv("CLOUDFLARED_TOKEN")
    if not token:
        print("Tunnel: CLOUDFLARED_TOKEN not set — tunnel disabled.")
        return
    try:
        # Download + spawn are blocking; keep them off the event loop.
        await asyncio.to_thread(_launch, token)
    except Exception as e:
        print(f"Tunnel: failed to start ({e}). The bot continues without it.")
