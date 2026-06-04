#!/usr/bin/env python3
"""
Universal Downloader — Backend Server
Requires: yt-dlp, ffmpeg, python3
Run: python3 server.py
"""

import os
import json
import time
import subprocess
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from collections import defaultdict

# ─── Config ───────────────────────────────────────────
PORT         = 5000
RATE_LIMIT   = 10    # requests per minute
RATE_WINDOW  = 60    # seconds

# Auto-detect download directory
_HOME = os.path.expanduser("~")
DOWNLOAD_DIR = os.path.join(_HOME, "storage", "downloads")
if not os.path.isdir(DOWNLOAD_DIR):
    DOWNLOAD_DIR = os.path.join(_HOME, "Downloads")
if not os.path.isdir(DOWNLOAD_DIR):
    DOWNLOAD_DIR = _HOME

# ─── Rate Limiter ──────────────────────────────────────
class RateLimiter:
    def __init__(self):
        self.requests = defaultdict(list)

    def is_allowed(self, ip: str) -> bool:
        now = time.time()
        bucket = self.requests[ip]
        self.requests[ip] = [t for t in bucket if now - t < RATE_WINDOW]
        if len(self.requests[ip]) >= RATE_LIMIT:
            return False
        self.requests[ip].append(now)
        return True

limiter = RateLimiter()

# ─── HTML: serve index.html from same dir ──────────────
def load_html():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    html_path  = os.path.join(script_dir, "index.html")
    if os.path.isfile(html_path):
        with open(html_path, "rb") as f:
            return f.read()
    return b"<h1>index.html not found</h1>"

# ─── Build yt-dlp command ──────────────────────────────
def build_cmd(url: str, fmt: str) -> list:
    cmd = ["yt-dlp", "--no-playlist", "--embed-metadata"]

    if fmt == "mp3":
        cmd += ["-x", "--audio-format", "mp3", "--audio-quality", "0"]
    elif fmt == "m4a":
        cmd += ["-x", "--audio-format", "m4a", "--audio-quality", "0"]
    elif fmt == "opus":
        cmd += ["-x", "--audio-format", "opus", "--audio-quality", "0"]
    elif fmt == "1080p":
        cmd += ["-f", "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080]"]
    elif fmt == "720p":
        cmd += ["-f", "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720]"]
    elif fmt == "480p":
        cmd += ["-f", "bestvideo[height<=480][ext=mp4]+bestaudio[ext=m4a]/best[height<=480]"]
    elif fmt == "360p":
        cmd += ["-f", "bestvideo[height<=360][ext=mp4]+bestaudio[ext=m4a]/best[height<=360]"]
    else:  # best
        cmd += ["-f", "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best"]

    cmd.append(url)
    return cmd

# ─── HTTP Handler ──────────────────────────────────────
class Handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        print(f"  [{self.client_address[0]}] {fmt % args}")

    # Common headers
    def _base_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Security-Policy",
            "default-src 'self'; style-src 'self' 'unsafe-inline'; "
            "script-src 'self' 'unsafe-inline'; "
            "connect-src 'self'; img-src 'self' https: data:;")

    def send_json(self, data: dict, status: int = 200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self._base_headers()
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self._base_headers()
        self.send_header("Allow", "GET, OPTIONS")
        self.end_headers()

    def do_GET(self):
        ip = self.client_address[0]

        # Rate limit
        if not limiter.is_allowed(ip):
            self.send_json({
                "success": False,
                "error": f"Rate limit tercapai. Tunggu {RATE_WINDOW} detik."
            }, 429)
            return

        parsed = urlparse(self.path)
        path   = parsed.path.rstrip("/") or "/"
        params = parse_qs(parsed.query)

        # ── Root: serve frontend ──
        if path == "/":
            html = load_html()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self._base_headers()
            self.end_headers()
            self.wfile.write(html)

        # ── /download ──
        elif path == "/download":
            url = params.get("url", [""])[0].strip()
            fmt = params.get("format", ["best"])[0].strip()

            if not url:
                self.send_json({"success": False, "error": "URL tidak boleh kosong."}, 400)
                return

            if not (url.startswith("http://") or url.startswith("https://")):
                self.send_json({"success": False, "error": "URL tidak valid."}, 400)
                return

            try:
                os.makedirs(DOWNLOAD_DIR, exist_ok=True)
                cmd    = build_cmd(url, fmt)
                result = subprocess.run(
                    cmd,
                    cwd=DOWNLOAD_DIR,
                    capture_output=True,
                    text=True,
                    timeout=300,
                )
                if result.returncode == 0:
                    self.send_json({
                        "success": True,
                        "message": f"File tersimpan di: {DOWNLOAD_DIR} 🎉"
                    })
                else:
                    err = result.stderr.strip()
                    short = err.splitlines()[-1] if err else "Unknown error"
                    self.send_json({"success": False, "error": short[:300]}, 500)

            except subprocess.TimeoutExpired:
                self.send_json({
                    "success": False,
                    "error": "Timeout (>5 menit). Coba format resolusi lebih rendah."
                }, 500)
            except FileNotFoundError:
                self.send_json({
                    "success": False,
                    "error": "yt-dlp tidak ditemukan. Install dengan: pip install yt-dlp"
                }, 500)
            except Exception as e:
                self.send_json({"success": False, "error": str(e)[:300]}, 500)

        # ── /status ──
        elif path == "/status":
            self.send_json({
                "online": True,
                "download_dir": DOWNLOAD_DIR,
                "rate_limit": f"{RATE_LIMIT} req/{RATE_WINDOW}s"
            })

        else:
            self.send_json({"success": False, "error": "Not found."}, 404)

# ─── Main ──────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("  📥 UNIVERSAL DOWNLOADER")
    print(f"  🌐 http://localhost:{PORT}")
    print(f"  📁 Simpan ke: {DOWNLOAD_DIR}")
    print(f"  🛡  Rate limit: {RATE_LIMIT} req/menit")
    print("  Tekan Ctrl+C untuk berhenti.")
    print("=" * 50)

    server = HTTPServer(("0.0.0.0", PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n👋 Server dimatikan.")
        server.server_close()
