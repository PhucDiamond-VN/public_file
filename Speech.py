#!/usr/bin/env python3
"""
Speech.py — Text-to-speech CLI dùng edge-tts + sounddevice (bản nâng cấp)

Cải tiến so với bản gốc:
  * Cache âm thanh theo hash(text+voice+rate+pitch+volume) -> phát lại tức thì
  * Pipeline: tải câu kế tiếp trong lúc đang phát câu hiện tại
  * Hỗ trợ nhiều nguồn text: đối số dòng lệnh, file, stdin, nhập tương tác
  * Retry có backoff khi lỗi mạng
  * Tuỳ chỉnh giọng, tốc độ, cao độ, âm lượng qua CLI
  * Xuất ra file mp3 thay vì phát (--save)
  * Ngắt bằng Ctrl+C mượt, không traceback rối mắt

Cài đặt:
    pip install edge-tts sounddevice soundfile

Ví dụ:
    python Speech.py "Xin chào"
    python Speech.py -f story.txt --rate +10% --voice vi-VN-HoaiMyNeural
    python Speech.py "Xin chào" --save hello.mp3
    echo "Câu 1
Câu 2" | python Speech.py
vi-VN-HoaiMyNeural
vi-VN-NamMinhNeural
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import io
import logging
import sys
from pathlib import Path
from typing import AsyncIterator, Optional

try:
    import edge_tts
    import sounddevice as sd
    import soundfile as sf
except ImportError as e:
    sys.stderr.write(
        f"Thiếu thư viện: {e.name}\n"
        "Cài đặt bằng: pip install edge-tts sounddevice soundfile\n"
    )
    sys.exit(1)

DEFAULT_VOICE = "vi-VN-HoaiMyNeural"
CACHE_DIR = Path.home() / ".cache" / "speech_py"
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 1.5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("speech")


class TTSError(RuntimeError):
    pass


def cache_key(text: str, voice: str, rate: str, pitch: str, volume: str) -> Path:
    raw = f"{voice}|{rate}|{pitch}|{volume}|{text}".encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()[:24]
    return CACHE_DIR / f"{digest}.mp3"


async def fetch_audio(
    text: str,
    voice: str,
    rate: str,
    pitch: str,
    volume: str,
    use_cache: bool = True,
) -> bytes:
    """Tải audio cho một đoạn text, có cache và retry."""
    text = text.strip()
    if not text:
        return b""

    cache_path = cache_key(text, voice, rate, pitch, volume) if use_cache else None
    if cache_path and cache_path.exists():
        log.debug("Cache hit: %s", text[:30])
        return cache_path.read_bytes()

    last_err: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            audio = bytearray()
            communicate = edge_tts.Communicate(
                text, voice, rate=rate, pitch=pitch, volume=volume
            )
            async for chunk in communicate.stream():
                if chunk["type"] == "audio":
                    audio.extend(chunk["data"])

            if not audio:
                raise TTSError("Không nhận được dữ liệu âm thanh (audio rỗng)")

            data = bytes(audio)
            if cache_path:
                CACHE_DIR.mkdir(parents=True, exist_ok=True)
                cache_path.write_bytes(data)
            return data

        except Exception as exc:  # network hiccup, throttling, v.v.
            last_err = exc
            log.warning(
                "Lỗi khi tải audio (lần %d/%d): %s", attempt, MAX_RETRIES, exc
            )
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_BACKOFF_SECONDS * attempt)

    raise TTSError(f"Không thể tải audio sau {MAX_RETRIES} lần thử: {last_err}")


def play_bytes(audio_bytes: bytes) -> None:
    if not audio_bytes:
        return
    data, samplerate = sf.read(io.BytesIO(audio_bytes), dtype="float32")
    sd.play(data, samplerate)
    sd.wait()


async def producer(
    texts: list[str],
    voice: str,
    rate: str,
    pitch: str,
    volume: str,
    use_cache: bool,
    queue: asyncio.Queue,
) -> None:
    """Tải trước audio của từng câu, đẩy vào queue theo đúng thứ tự."""
    for text in texts:
        try:
            audio = await fetch_audio(text, voice, rate, pitch, volume, use_cache)
        except TTSError as e:
            log.error("Bỏ qua câu do lỗi: %s -> %s", text[:40], e)
            audio = b""
        await queue.put((text, audio))
    await queue.put(None)  # sentinel: hết việc


async def consumer(queue: asyncio.Queue) -> None:
    """Phát audio theo thứ tự nhận từ queue (chạy song song với producer)."""
    loop = asyncio.get_running_loop()
    while True:
        item = await queue.get()
        if item is None:
            break
        text, audio = item
        if not audio:
            continue
        log.info("Đang phát: %s", text[:60])
        # sd.play/sd.wait là blocking -> chạy trong executor để không chặn event loop
        await loop.run_in_executor(None, play_bytes, audio)


async def speak_many(
    texts: list[str], voice: str, rate: str, pitch: str, volume: str, use_cache: bool
) -> None:
    """Phát nhiều câu, tải câu kế tiếp song song trong lúc phát câu hiện tại."""
    queue: asyncio.Queue = asyncio.Queue(maxsize=2)
    await asyncio.gather(
        producer(texts, voice, rate, pitch, volume, use_cache, queue),
        consumer(queue),
    )


async def save_to_file(
    text: str, voice: str, rate: str, pitch: str, volume: str, out_path: Path
) -> None:
    audio = await fetch_audio(text, voice, rate, pitch, volume, use_cache=False)
    if not audio:
        raise TTSError("Không có dữ liệu audio để lưu")
    out_path.write_bytes(audio)
    log.info("Đã lưu: %s (%.1f KB)", out_path, len(audio) / 1024)


def split_into_lines(raw: str) -> list[str]:
    return [line.strip() for line in raw.splitlines() if line.strip()]


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Chuyển văn bản thành giọng nói (edge-tts)."
    )
    p.add_argument("text", nargs="?", help="Văn bản cần đọc (bỏ trống để dùng -f/stdin)")
    p.add_argument("-f", "--file", type=Path, help="Đọc văn bản từ file")
    p.add_argument("--voice", default=DEFAULT_VOICE, help=f"Giọng đọc (mặc định: {DEFAULT_VOICE})")
    p.add_argument("--rate", default="+30%", help="Tốc độ, vd +10%%, -20%%")
    p.add_argument("--pitch", default="+20Hz", help="Cao độ, vd +5Hz, -10Hz")
    p.add_argument("--volume", default="+0%", help="Âm lượng, vd +10%%, -20%%")
    p.add_argument("--save", type=Path, help="Lưu ra file .mp3 thay vì phát")
    p.add_argument("--no-cache", action="store_true", help="Không dùng cache")
    p.add_argument("--list-voices", action="store_true", help="Liệt kê các giọng có sẵn rồi thoát")
    p.add_argument("-v", "--verbose", action="store_true", help="In log chi tiết (debug)")
    return p


async def list_voices() -> None:
    voices = await edge_tts.list_voices()
    vn_voices = [v for v in voices if v["Locale"].startswith("vi-")]
    for v in vn_voices or voices:
        print(f"{v['ShortName']:30s} {v['Gender']:8s} {v['Locale']}")


async def async_main(args: argparse.Namespace) -> int:
    if args.list_voices:
        await list_voices()
        return 0

    if args.text:
        raw = args.text
    elif args.file:
        raw = args.file.read_text(encoding="utf-8")
    elif not sys.stdin.isatty():
        raw = sys.stdin.read()
    else:
        raw = input("Nhập văn bản cần đọc: ")

    texts = split_into_lines(raw) or [raw.strip()]
    if not any(texts):
        log.error("Không có văn bản để đọc.")
        return 1

    if args.save:
        await save_to_file(
            " ".join(texts), args.voice, args.rate, args.pitch, args.volume, args.save
        )
        return 0

    await speak_many(
        texts, args.voice, args.rate, args.pitch, args.volume, use_cache=not args.no_cache
    )
    return 0


def main() -> None:
    args = build_parser().parse_args()
    if args.verbose:
        log.setLevel(logging.DEBUG)

    try:
        exit_code = asyncio.run(async_main(args))
    except KeyboardInterrupt:
        log.info("Đã dừng theo yêu cầu (Ctrl+C).")
        exit_code = 130
    except TTSError as e:
        log.error(str(e))
        exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
