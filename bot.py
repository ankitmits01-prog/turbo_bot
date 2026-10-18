"""
Turbo Forward Bot (Pyrogram / Pyrofork)

Fast path : copy_message  -> Telegram copies the file on its own servers (1-2 sec, any size)
Slow path : parallel multi-session download + upload (for restricted / protected content)

Send the bot:
  * a Telegram message link (t.me/c/123/456 or t.me/channel/456), one or many
  * /batch <first_link> <last_link>   (every message between the two links)
  * or just forward / send a file to the bot
Files go to your destination chat (/setdest), or back to you if none is set.
"""
import asyncio
import json
import logging
import os
import re
import shutil
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from pyrogram import Client, filters, idle
from pyrogram.errors import FloodWait
from pyrogram.handlers import MessageHandler

import turbo

load_dotenv()

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
USER_MODE = os.getenv("MODE", "bot").lower() == "user"   # user = log in with your own account
OWNERS = [int(x) for x in os.getenv("OWNER_IDS", "").replace(" ", "").split(",") if x]
WORKDIR = Path(os.getenv("WORKDIR", "/root/turbo_work"))
JOB_WORKERS = int(os.getenv("JOB_WORKERS", "2"))
SETTINGS_FILE = Path(os.getenv("SETTINGS_FILE", "settings.json"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("bot")

LINK_RE = re.compile(r"(?:https?://)?t\.me/(c/)?([^/\s]+)/(\d+)(?:/(\d+))?")

jobs: asyncio.Queue = asyncio.Queue()


# ---------------------------------------------------------------- helpers
def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.1f} {unit}"
        n /= 1024


def load_settings() -> dict:
    try:
        return json.loads(SETTINGS_FILE.read_text())
    except Exception:
        return {}


def save_settings(data: dict):
    SETTINGS_FILE.write_text(json.dumps(data))


def get_dest(user_id: int, default_chat: int):
    value = load_settings().get(str(user_id))
    if value is None:
        return default_chat
    return int(value) if re.fullmatch(r"-?\d+", str(value)) else value


def parse_link(text: str):
    m = LINK_RE.search(text)
    if not m:
        return None
    is_c, chat, first, second = m.groups()
    mid = int(second or first)
    chat_id = int(f"-100{chat}") if is_c else chat
    return chat_id, mid


def all_links(text: str):
    out = []
    for m in LINK_RE.finditer(text):
        is_c, chat, first, second = m.groups()
        mid = int(second or first)
        out.append((int(f"-100{chat}") if is_c else chat, mid))
    return out


class Prog:
    def __init__(self):
        self.stage = "Queued"
        self.done = 0
        self.total = 0
        self.t0 = time.monotonic()
        self._last_t = self.t0
        self._last_done = 0
        self._speed = 0.0

    def start_stage(self, stage: str, total: int):
        self.stage, self.total, self.done = stage, total, 0
        self._last_t, self._last_done, self._speed = time.monotonic(), 0, 0.0

    def render(self) -> str:
        now = time.monotonic()
        dt = now - self._last_t
        if dt > 0:
            inst = (self.done - self._last_done) / dt
            self._speed = inst if self._speed == 0 else 0.6 * self._speed + 0.4 * inst
            self._last_t, self._last_done = now, self.done
        pct = (self.done / self.total * 100) if self.total else 0
        eta = (self.total - self.done) / self._speed if self._speed > 1 else 0
        bar = "█" * int(pct // 10) + "░" * (10 - int(pct // 10))
        return (
            f"{self.stage}\n{bar} {pct:.0f}%\n"
            f"{human(self.done)} / {human(self.total)}\n"
            f"⚡ {human(self._speed)}/s   ⏱ {int(eta)}s left"
        )

    def cb(self, current, total, *_):
        self.done = current
        self.total = total


async def ticker(status, prog: Prog, stop: asyncio.Event):
    last = ""
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), 3)
        except asyncio.TimeoutError:
            pass
        if stop.is_set() or not prog.total:
            continue
        text = prog.render()
        if text == last:
            continue
        last = text
        try:
            await status.edit_text(text)
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except Exception:
            pass


# ---------------------------------------------------------------- core job
async def fast_copy(client: Client, dest, src_chat, msg) -> bool:
    """Server-side copy. Returns True on success, False if content is restricted."""
    if getattr(msg, "has_protected_content", False) or getattr(msg.chat, "has_protected_content", False):
        return False
    try:
        await client.copy_message(dest, src_chat, msg.id)
        return True
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
        await client.copy_message(dest, src_chat, msg.id)
        return True
    except Exception as e:
        text = str(e).upper()
        if "RESTRICT" in text or "PROTECTED" in text or "FORWARDS" in text:
            return False
        raise


async def slow_transfer(client: Client, dest, msg, prog: Prog, job_dir: Path):
    media = msg.video or msg.document or msg.audio or msg.animation or msg.voice or msg.video_note
    caption = msg.caption or ""
    ents = msg.caption_entities

    if msg.photo:
        path = await client.download_media(msg, file_name=str(job_dir) + "/")
        await client.send_photo(dest, path, caption=caption, caption_entities=ents)
        return
    if not media:
        if msg.text:
            await client.send_message(dest, msg.text, entities=msg.entities)
            return
        raise RuntimeError("Message has no downloadable media")

    size = media.file_size or 0
    name = getattr(media, "file_name", None) or media.file_unique_id
    if msg.video and "." not in name:
        name += ".mp4"
    path = str(job_dir / name)

    # optional thumbnail (tiny)
    thumb = None
    thumbs = getattr(media, "thumbs", None)
    if thumbs:
        try:
            thumb = await client.download_media(thumbs[0].file_id, file_name=str(job_dir / "thumb.jpg"))
        except Exception:
            thumb = None

    common = dict(chat_id=dest, progress=prog.cb)
    cap = dict(caption=caption, caption_entities=ents)

    async def send(src):
        if msg.video:
            await client.send_video(
                video=src, file_name=name, duration=media.duration or 0,
                width=media.width or 0, height=media.height or 0,
                thumb=thumb, supports_streaming=True, **cap, **common,
            )
        elif msg.animation:
            await client.send_animation(
                animation=src, file_name=name, duration=media.duration or 0,
                width=media.width or 0, height=media.height or 0, thumb=thumb, **cap, **common,
            )
        elif msg.audio:
            await client.send_audio(
                audio=src, file_name=name, duration=media.duration or 0,
                performer=media.performer, title=media.title, thumb=thumb, **cap, **common,
            )
        elif msg.voice:
            await client.send_voice(voice=src, duration=media.duration or 0, **cap, **common)
        elif msg.video_note:
            await client.send_video_note(
                video_note=src, duration=media.duration or 0, length=media.length or 0, **common
            )
        else:
            await client.send_document(document=src, file_name=name, thumb=thumb, **cap, **common)

    # 1) RELAY: download + upload happen together, no disk (fastest)
    try:
        prog.start_stage("⚡ Turbo relay (download + upload together)", size)
        await send(turbo.RelayFile(media.file_id, size, name))
        return
    except Exception as e:
        log.warning("relay failed (%s), falling back to disk mode", e)

    # 2) FALLBACK: parallel download to disk, then parallel upload
    msg = await client.get_messages(msg.chat.id, msg.id)  # fresh file reference
    media = msg.video or msg.document or msg.audio or msg.animation or msg.voice or msg.video_note
    path = str(job_dir / name)
    prog.start_stage("🔽 Downloading (fallback)", size)
    await turbo.turbo_download(client, media.file_id, size, path, prog)
    prog.start_stage("🔼 Uploading (fallback)", size)
    await send(path)


async def run_job(client: Client, job: dict):
    status = job["status"]
    prog = Prog()
    stop = asyncio.Event()
    tick = asyncio.create_task(ticker(status, prog, stop))
    job_dir = WORKDIR / f"{job['user']}_{int(time.time() * 1000)}"
    t0 = time.monotonic()
    try:
        try:
            msg = await client.get_messages(job["src_chat"], job["src_mid"])
        except Exception as e:
            raise RuntimeError(
                f"Source message nahi mila ({e}). Bot ko us channel/group me add karo."
            )
        if msg.empty:
            raise RuntimeError("Message empty/deleted hai ya bot ke liye accessible nahi.")

        dest = job["dest"]
        if await fast_copy(client, dest, job["src_chat"], msg):
            await status.edit_text(f"✅ Copied instantly in {time.monotonic() - t0:.1f}s")
            return

        await status.edit_text("🔒 Content restricted, turbo download/upload shuru kar raha hoon...")
        job_dir.mkdir(parents=True, exist_ok=True)
        await slow_transfer(client, dest, msg, prog, job_dir)
        took = time.monotonic() - t0
        size = prog.total or 0
        avg = size / took if took else 0
        await status.edit_text(f"✅ Done: {human(size)} in {took:.0f}s (avg {human(avg)}/s)")
    except Exception as e:
        log.exception("job failed")
        try:
            await status.edit_text(f"❌ Failed: {e}")
        except Exception:
            pass
    finally:
        stop.set()
        await tick
        shutil.rmtree(job_dir, ignore_errors=True)


async def job_worker(client: Client):
    while True:
        job = await jobs.get()
        try:
            await run_job(client, job)
        finally:
            jobs.task_done()


def need_dest(message) -> bool:
    """In user mode, files sent back into Saved Messages would re-trigger the bot (loop)."""
    return USER_MODE and get_dest(message.from_user.id, message.chat.id) == message.chat.id


NEED_DEST_TEXT = "⚠️ Pehle destination set karo: /setdest -100xxxxxxxxxx  (ya @channel). Saved Messages me result nahi bhej sakta."


async def enqueue(message, src_chat, src_mid):
    if need_dest(message):
        return await message.reply_text(NEED_DEST_TEXT)
    status = await message.reply_text("⏳ Queued...", quote=True)
    await jobs.put(
        dict(
            user=message.from_user.id,
            status=status,
            src_chat=src_chat,
            src_mid=src_mid,
            dest=get_dest(message.from_user.id, message.chat.id),
        )
    )


# ---------------------------------------------------------------- handlers
async def start_cmd(client, message):
    await message.reply_text(
        "🚀 Turbo Forward Bot\n\n"
        "• Message link bhejo (ek ya zyada)\n"
        "• /batch <first_link> <last_link> : poori range\n"
        "• Koi file forward karo : destination par copy ho jayegi\n"
        "• /setdest <chat_id ya @username> : destination (bot wahan admin ho)\n"
        "• /mydest, /cleardest, /queue"
    )


async def setdest_cmd(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Use: /setdest -1001234567890  ya  /setdest @mychannel")
    value = message.command[1]
    try:
        chat = await client.get_chat(int(value) if re.fullmatch(r"-?\d+", value) else value)
    except Exception as e:
        return await message.reply_text(f"❌ Chat nahi mila: {e}")
    data = load_settings()
    data[str(message.from_user.id)] = chat.id
    save_settings(data)
    await message.reply_text(f"✅ Destination set: {chat.title or chat.first_name} ({chat.id})")


async def mydest_cmd(client, message):
    value = load_settings().get(str(message.from_user.id))
    await message.reply_text(f"Destination: {value}" if value else "Destination set nahi hai (files aapko hi wapas milengi).")


async def cleardest_cmd(client, message):
    data = load_settings()
    data.pop(str(message.from_user.id), None)
    save_settings(data)
    await message.reply_text("✅ Destination clear.")


async def queue_cmd(client, message):
    await message.reply_text(f"Queue me {jobs.qsize()} job(s) hain.")


async def batch_cmd(client, message):
    if need_dest(message):
        return await message.reply_text(NEED_DEST_TEXT)
    links = all_links(message.text)
    if len(links) < 2 or links[0][0] != links[1][0]:
        return await message.reply_text("Use: /batch <first_link> <last_link> (same channel ke)")
    chat, a = links[0]
    b = links[1][1]
    lo, hi = min(a, b), max(a, b)
    if hi - lo > 300:
        return await message.reply_text("Ek baar me max 300 messages.")
    status = await message.reply_text(f"⏳ {hi - lo + 1} message queue kar raha hoon...", quote=True)
    dest = get_dest(message.from_user.id, message.chat.id)
    for mid in range(lo, hi + 1):
        s = await message.reply_text(f"⏳ Queued #{mid}", quote=False)
        await jobs.put(dict(user=message.from_user.id, status=s, src_chat=chat, src_mid=mid, dest=dest))
    await status.edit_text(f"✅ {hi - lo + 1} job queue me daal diye.")


async def link_handler(client, message):
    links = all_links(message.text or "")
    if not links:
        return
    for chat, mid in links[:50]:
        await enqueue(message, chat, mid)


async def media_handler(client, message):
    await enqueue(message, message.chat.id, message.id)


async def deny_handler(client, message):
    await message.reply_text(f"⛔ Not allowed. Your ID: {message.from_user.id}\n(OWNER_IDS me add karo)")


# ---------------------------------------------------------------- main
async def main():
    WORKDIR.mkdir(parents=True, exist_ok=True)
    common = dict(api_id=API_ID, api_hash=API_HASH, sleep_threshold=60, max_concurrent_transmissions=4)
    if USER_MODE:
        # Your own account. First run asks phone + code (+ 2FA) and saves turbo_user.session
        app = Client("turbo_user", **common)
        private = filters.chat("me")          # commands/links only from your Saved Messages
    else:
        app = Client("turbo_bot", bot_token=BOT_TOKEN, **common)
        owner = filters.user(OWNERS) if OWNERS else filters.user([])
        private = filters.private & owner
    turbo.install()

    media = filters.document | filters.video | filters.audio | filters.animation | filters.voice | filters.video_note | filters.photo

    app.add_handler(MessageHandler(start_cmd, private & filters.command("start")))
    app.add_handler(MessageHandler(setdest_cmd, private & filters.command("setdest")))
    app.add_handler(MessageHandler(mydest_cmd, private & filters.command("mydest")))
    app.add_handler(MessageHandler(cleardest_cmd, private & filters.command("cleardest")))
    app.add_handler(MessageHandler(queue_cmd, private & filters.command("queue")))
    app.add_handler(MessageHandler(batch_cmd, private & filters.command("batch")))
    app.add_handler(MessageHandler(link_handler, private & filters.text & ~filters.command(["start", "setdest", "mydest", "cleardest", "queue", "batch"])))
    app.add_handler(MessageHandler(media_handler, private & media))
    if not USER_MODE:
        app.add_handler(MessageHandler(deny_handler, filters.private & ~owner))

    await app.start()
    log.info("Started as %s", app.me.username or app.me.first_name)
    if "--login" in sys.argv:
        log.info("Login saved. Session file created.")
        await app.stop()
        return
    workers = [asyncio.create_task(job_worker(app)) for _ in range(JOB_WORKERS)]
    try:
        await idle()
    finally:
        for w in workers:
            w.cancel()
        await turbo.close_all()
        await app.stop()


if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass
    asyncio.run(main())
