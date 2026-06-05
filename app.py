import os
import sys
import json
import time
import uuid
import asyncio
import logging
import hashlib
from urllib.parse import urlparse
from datetime import datetime

# ==========================================
# 1. SETUP & OBSERVABILITY
# ==========================================
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_obj = {
            "ts": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "msg": record.getMessage()
        }
        if record.exc_info:
            log_obj["trace"] = self.formatException(record.exc_info)
        return json.dumps(log_obj)

logger = logging.getLogger("VORTEX")
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)

try:
    from fastapi import FastAPI, Request, HTTPException
    from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
    from fastapi.middleware.cors import CORSMiddleware
    from pydantic import BaseModel
    import httpx
    from dotenv import load_dotenv
except ImportError as e:
    logger.critical(f"Library hilang: {e}. Install: pip install fastapi uvicorn yt-dlp httpx python-dotenv")
    sys.exit(1)

# ==========================================
# 2. CONFIGURATION
# ==========================================
load_dotenv()

# SECRET_KEY opsional untuk development — wajib di production
SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key-change-in-production")
if SECRET_KEY == "dev-secret-key-change-in-production":
    logger.warning("⚠️  SECRET_KEY belum diset. Aman untuk development, WAJIB diubah di production!")

CACHE_VERSION = "1.0.0"
app = FastAPI(title="VORTEX Media Proxy")

# ==========================================
# 3. CORS — IZINKAN FRONTEND TERHUBUNG
# ==========================================
# Sesuaikan origins dengan URL frontend Anda
# Contoh production: ["https://vortex.yoursite.com"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # Dev: semua origin. Production: ganti spesifik
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# 4. CORE STATE
# ==========================================
class AppState:
    def __init__(self):
        self.rl_db = {}
        self.quota_db = {}
        self.cache_db = {}
        self.stream_db = {}
        self.active_streams = 0
        self.lock = asyncio.Lock()
        self.semaphore = asyncio.Semaphore(3)

state = AppState()

LIMIT_YOUTUBE = 5
LIMIT_OTHERS = 20
CACHE_TTL = 900      # 15 Menit
STREAM_TTL = 3600    # 1 Jam

# ==========================================
# 5. SECURITY & VALIDATION
# ==========================================
ALLOWED_DOMAINS = {
    'youtube.com', 'youtu.be',
    'tiktok.com', 'instagram.com',
    'twitter.com', 'x.com', 'facebook.com',
    'vimeo.com', 'dailymotion.com',
}

def is_valid_url(url: str) -> bool:
    if len(url) > 1000:
        return False
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ('http', 'https'):
            return False
        hostname = parsed.hostname.lower() if parsed.hostname else ""
        for domain in ALLOWED_DOMAINS:
            if hostname == domain or hostname.endswith('.' + domain):
                return True
        return False
    except:
        return False

def hash_ip(ip: str) -> str:
    return hashlib.sha256((ip or "unknown").encode('utf-8')).hexdigest()

# ==========================================
# 6. RATE LIMITER & QUOTA
# ==========================================
async def check_rate_limit(ip_hash: str) -> bool:
    now = time.time()
    async with state.lock:
        reqs = state.rl_db.get(ip_hash, [])
        reqs = [t for t in reqs if now - t < 60]
        if len(reqs) >= 20:
            state.rl_db[ip_hash] = reqs
            return False
        reqs.append(now)
        state.rl_db[ip_hash] = reqs
        return True

async def check_and_consume_quota(ip_hash: str, url: str) -> tuple[bool, str]:
    today = datetime.now().strftime('%Y-%m-%d')
    is_yt = 'youtube' in url.lower() or 'youtu.be' in url.lower()
    async with state.lock:
        user_quota = state.quota_db.get(ip_hash, {})
        if user_quota.get('date') != today:
            user_quota = {'date': today, 'yt': 0, 'others': 0}
        if is_yt:
            if user_quota['yt'] >= LIMIT_YOUTUBE:
                return False, "Limit YouTube harian habis (5/hari)."
            user_quota['yt'] += 1
        else:
            if user_quota['others'] >= LIMIT_OTHERS:
                return False, "Limit platform lain harian habis (20/hari)."
            user_quota['others'] += 1
        state.quota_db[ip_hash] = user_quota
        return True, ""

# ==========================================
# 7. EXTRACTOR (yt-dlp async subprocess)
# ==========================================
async def extract_metadata(url: str, fmt: str) -> dict:
    """Jalankan yt-dlp tanpa memblokir event loop Uvicorn"""
    cmd = [
        'yt-dlp',
        '--dump-json',
        '--no-playlist',
        '--match-filter', '!is_live',
        '-f', fmt,
        url
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=25.0)
    except asyncio.TimeoutError:
        proc.kill()
        logger.error(f"Extractor Timeout | URL: {url}")
        raise HTTPException(status_code=504, detail="Timeout saat ekstraksi. Coba lagi.")

    if proc.returncode != 0:
        err_msg = stderr.decode().lower()
        logger.error(f"yt-dlp Error | Code: {proc.returncode} | {err_msg[:300]}")
        if "private" in err_msg:
            raise HTTPException(status_code=400, detail="Video private / tidak bisa diakses.")
        if "sign in" in err_msg or "age" in err_msg:
            raise HTTPException(status_code=400, detail="Video dibatasi umur (age-restricted).")
        if "is live" in err_msg:
            raise HTTPException(status_code=400, detail="Live stream tidak didukung.")
        if "not available" in err_msg:
            raise HTTPException(status_code=400, detail="Video tidak tersedia di region ini.")
        raise HTTPException(status_code=400, detail="Gagal mengekstrak media. Pastikan URL valid dan video bisa diakses.")

    try:
        info = json.loads(stdout.decode())

        # Ambil direct URL dari berbagai struktur yt-dlp
        direct_url = info.get('url')
        if not direct_url:
            formats = info.get('requested_formats', [])
            if formats and isinstance(formats, list):
                direct_url = formats[0].get('url')

        if not direct_url:
            raise ValueError("Direct URL tidak ditemukan.")

        # Format durasi: detik → "MM:SS" atau "HH:MM:SS"
        duration_sec = info.get('duration', 0)
        if duration_sec:
            duration_str = time.strftime('%H:%M:%S' if duration_sec >= 3600 else '%M:%S',
                                          time.gmtime(duration_sec))
        else:
            duration_str = None

        return {
            "direct_url":  direct_url,
            "title":       info.get('title', 'video_media'),
            "ext":         info.get('ext', 'mp4'),
            "thumbnail":   info.get('thumbnail'),
            "duration":    duration_str,
            "uploader":    info.get('uploader') or info.get('channel'),
            "view_count":  info.get('view_count'),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"JSON Parsing Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Gagal memproses data dari extractor.")

# ==========================================
# 8. CLEANUP BACKGROUND TASK
# ==========================================
async def cleanup_daemon():
    while True:
        await asyncio.sleep(600)
        now = time.time()
        async with state.lock:
            stale_cache = [k for k, v in state.cache_db.items() if now - v['ts'] > CACHE_TTL]
            for k in stale_cache:
                del state.cache_db[k]
            stale_streams = [k for k, v in state.stream_db.items() if now - v['ts'] > STREAM_TTL]
            for k in stale_streams:
                del state.stream_db[k]
        logger.info(f"🧹 Cleanup: {len(stale_cache)} cache & {len(stale_streams)} streams dihapus.")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(cleanup_daemon())
    logger.info("🚀 VORTEX Backend siap. Endpoint: /ping, /api/fetch, /stream/{id}")

# ==========================================
# 9. API ROUTES
# ==========================================
class FetchPayload(BaseModel):
    url: str
    format: str = "mp4-720p"

# --- /ping — Health check yang dibutuhkan frontend ---
@app.get("/ping")
async def ping():
    """
    Frontend memanggil ini untuk cek status server.
    Response harus include field 'demo_mode'.
    """
    return {
        "status": "ok",
        "demo_mode": False,
        "active_streams": state.active_streams,
        "cache_size": len(state.cache_db)
    }

# --- /api/fetch — Endpoint utama yang dipanggil frontend ---
@app.post("/api/fetch")
async def api_fetch(req: Request, payload: FetchPayload):
    """
    Frontend VORTEX memanggil POST /api/fetch dengan body:
    { "url": "...", "format": "mp4-720p" }

    Response sukses:
    {
      "success": true,
      "download_url": "https://...",
      "title": "...",
      "thumbnail": "...",
      "duration": "MM:SS",
      "format": "mp4-720p",
      "filename": "judul_video.mp4",
      "demo_mode": false
    }
    """
    raw_ip = req.headers.get("x-forwarded-for") or (req.client.host if req.client else "unknown")
    ip_hash = hash_ip(raw_ip.split(",")[0].strip())
    url = payload.url.strip()

    # Rate limit
    if not await check_rate_limit(ip_hash):
        logger.warning(f"Rate Limit | IP: {ip_hash}")
        raise HTTPException(status_code=429,
                            detail="Terlalu banyak permintaan. Tunggu 1 menit.")

    # Validasi URL
    if not is_valid_url(url):
        logger.warning(f"Invalid URL: {url}")
        raise HTTPException(status_code=400,
                            detail="URL tidak valid atau platform tidak didukung.")

    fmt_pilihan = payload.format
    cache_key = f"{hashlib.md5(url.encode()).hexdigest()}:{fmt_pilihan}"

    # --- Cek Cache ---
    async with state.lock:
        cached = state.cache_db.get(cache_key)
        if cached and cached.get("v") == CACHE_VERSION and time.time() - cached['ts'] < CACHE_TTL:
            ok, err = await check_and_consume_quota(ip_hash, url)
            if not ok:
                raise HTTPException(status_code=403, detail=err)
            logger.info(f"Cache Hit | {url}")
            return JSONResponse(content={
                "success":      True,
                "download_url": f"/stream/{cached['stream_id']}",
                "title":        cached.get('title', 'video_media'),
                "thumbnail":    cached.get('thumbnail'),
                "duration":     cached.get('duration'),
                "format":       fmt_pilihan,
                "filename":     cached.get('filename'),
                "demo_mode":    False,
            })

    # --- Cek Quota ---
    ok, err = await check_and_consume_quota(ip_hash, url)
    if not ok:
        raise HTTPException(status_code=403, detail=err)

    # --- Map format frontend → yt-dlp selector ---
    fmt_mapping = {
        'mp3':       'bestaudio/best',
        'audio':     'bestaudio/best',
        'mp4-1080p': 'bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080][ext=mp4]/best[height<=1080]',
        'mp4-720p':  'bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720][ext=mp4]/best[height<=720]',
        'mp4-360p':  'bestvideo[height<=360][ext=mp4]+bestaudio[ext=m4a]/best[height<=360][ext=mp4]/best[height<=360]',
    }
    ydl_fmt = fmt_mapping.get(fmt_pilihan, 'best[height<=720]')

    # --- Ekstraksi ---
    info = await extract_metadata(url, ydl_fmt)

    # --- Tentukan ekstensi output ---
    if fmt_pilihan == 'mp3' or fmt_pilihan == 'audio':
        out_ext = 'mp3'
    else:
        out_ext = info.get('ext', 'mp4')

    # Nama file aman untuk Content-Disposition header
    raw_title = info.get('title', 'video_media')
    safe_title = "".join(
        c for c in raw_title if c.isalnum() or c in ' -_'
    ).strip()[:80] or "video_media"
    filename = f"{safe_title}.{out_ext}"

    stream_id = uuid.uuid4().hex
    data_payload = {
        "v":           CACHE_VERSION,
        "ts":          time.time(),
        "direct_url":  info['direct_url'],
        "filename":    filename,
        "stream_id":   stream_id,
        "title":       raw_title,
        "thumbnail":   info.get('thumbnail'),
        "duration":    info.get('duration'),
    }

    async with state.lock:
        state.stream_db[stream_id] = data_payload
        state.cache_db[cache_key]  = data_payload

    logger.info(f"Extracted OK | stream={stream_id} | {url}")

    return JSONResponse(content={
        "success":      True,
        "download_url": f"/stream/{stream_id}",
        "title":        raw_title,
        "thumbnail":    info.get('thumbnail'),
        "duration":     info.get('duration'),
        "format":       fmt_pilihan,
        "filename":     filename,
        "demo_mode":    False,
    })

# --- /stream/{id} — Proxy streaming ke sumber asli ---
@app.get("/stream/{stream_id}")
async def stream_media(stream_id: str):
    async with state.lock:
        payload = state.stream_db.get(stream_id)

    if not payload or payload.get("v") != CACHE_VERSION:
        raise HTTPException(status_code=403,
                            detail="Stream token tidak valid atau sudah expired.")

    if state.active_streams >= 3:
        raise HTTPException(status_code=503,
                            detail="Server sedang penuh (max 3 stream bersamaan). Coba beberapa saat lagi.")

    async def generate():
        await state.semaphore.acquire()
        state.active_streams += 1
        logger.info(f"Stream Mulai | id={stream_id} | active={state.active_streams}")
        try:
            async with httpx.AsyncClient(follow_redirects=True) as client:
                async with client.stream(
                    "GET", payload["direct_url"],
                    timeout=httpx.Timeout(15.0, read=60.0)
                ) as resp:
                    resp.raise_for_status()
                    async for chunk in resp.aiter_bytes(chunk_size=256 * 1024):
                        yield chunk
        except httpx.ReadTimeout:
            logger.warning(f"Stream ReadTimeout | id={stream_id}")
        except Exception as e:
            logger.error(f"Stream Error | id={stream_id} | {e}")
        finally:
            state.semaphore.release()
            state.active_streams -= 1
            logger.info(f"Stream Selesai | id={stream_id} | active={state.active_streams}")

    headers = {
        "Content-Disposition": f'attachment; filename="{payload["filename"]}"',
        "Access-Control-Allow-Origin": "*",
    }
    return StreamingResponse(
        generate(),
        media_type="application/octet-stream",
        headers=headers
    )

# --- /health — Status detail server ---
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "active_streams": state.active_streams,
        "cache_size": len(state.cache_db),
        "stream_tokens": len(state.stream_db),
    }

if __name__ == "__main__":
    import uvicorn
    logger.info("▶ Menjalankan VORTEX backend di http://localhost:5000")
    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=True, log_level="error")
