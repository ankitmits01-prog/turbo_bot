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
import copy
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
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait
from pyrogram.handlers import MessageHandler

import sp_turbo as turbo

load_dotenv()

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
USER_MODE = os.getenv("MODE", "bot").lower() == "user"   # user = log in with your own account
OWNERS = [int(x) for x in os.getenv("OWNER_IDS", "").replace(" ", "").split(",") if x]
WORKDIR = Path(os.getenv("WORKDIR", "/root/turbo_work"))
JOB_WORKERS = int(os.getenv("JOB_WORKERS", "1"))
AUTO_DELAY = int(os.getenv("AUTO_DELAY", "30"))              # seconds between auto links
AUTO_STOP_AFTER_FAILS = int(os.getenv("AUTO_STOP_AFTER_FAILS", "5"))
AUTO_MARK = "⏩"
MY_TAG = os.getenv("MY_TAG", "@raonemybro")        # goes after "Extracted by"
BACKUP_LINK = os.getenv("BACKUP_LINK", "")          # optional: added next to the tag
BACKUP_LABEL = os.getenv("BACKUP_LABEL", "Backup")
SEND_INTERVAL = float(os.getenv("SEND_INTERVAL", "2"))   # min seconds between sends/copies (anti-flood)
SETTINGS_FILE = Path(os.getenv("SETTINGS_FILE", "settings.json"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("bot")

LINK_RE = re.compile(r"(?:https?://)?t\.me/(c/)?([^/\s]+)/(\d+)(?:/(\d+))?")

jobs: asyncio.Queue = asyncio.Queue()
current_tasks: set = set()
auto_tasks: set = set()

# ---------------------------------------------------------------- anti-flood
_pace_lock = asyncio.Lock()
_last_send = 0.0
flood_until = 0.0


async def pace():
    """Global limiter: at most one send/copy/reply every SEND_INTERVAL sec, and wait out any FloodWait."""
    global _last_send
    async with _pace_lock:
        now = time.monotonic()
        wait = max(_last_send + SEND_INTERVAL - now, flood_until - now, 0)
        if wait:
            await asyncio.sleep(wait)
        _last_send = time.monotonic()


async def flood_safe(fn, *args, paced=True, **kwargs):
    """Call a Telegram method; on FloodWait remember it globally and retry after waiting."""
    global flood_until
    for _ in range(5):
        if paced:
            await pace()
        try:
            return await fn(*args, **kwargs)
        except FloodWait as e:
            flood_until = max(flood_until, time.monotonic() + e.value + 2)
            log.warning("FloodWait %ss - sab kuch ruk gaya hai itni der", e.value)
            await asyncio.sleep(e.value + 2)
    raise RuntimeError("FloodWait baar baar aa raha hai, thodi der baad try karo")


class Batch:
    """One shared status message for many jobs (no per-file reply spam)."""

    def __init__(self, total, status):
        self.total, self.status = total, status
        self.ok = self.fail = self.cancelled = 0
        self.prog = None
        self.last_error = ""


async def batch_reporter(batch: "Batch"):
    last = ""
    while batch.ok + batch.fail + batch.cancelled < batch.total:
        await asyncio.sleep(6)
        text = f"📦 Batch: {batch.ok} done, {batch.fail} failed / {batch.total}\n(rokne ke liye /cancel bhejo)"
        if batch.prog and batch.prog.total:
            text += "\n\n" + batch.prog.render()
        if batch.last_error:
            text += f"\n\n⚠️ Last error: {batch.last_error[:150]}"
        if text == last:
            continue
        last = text
        try:
            await batch.status.edit_text(text)
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except Exception:
            pass
    try:
        head = "🛑 Batch cancelled" if batch.cancelled else "✅ Batch complete"
        await batch.status.edit_text(
            f"{head}: {batch.ok} done, {batch.fail} failed, {batch.cancelled} cancelled / {batch.total}"
        )
    except Exception:
        pass



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
            await asyncio.wait_for(stop.wait(), 5)
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


# ---------------------------------------------------------------- caption rewrite
# "Extracted by ..." / "Downloaded by ..."  -> everything after it on that line becomes our tag
EB_PAT = re.compile(r"(?:extracted|downloaded|powered)\s*by", re.I)
EB_SEP = re.compile(r"[ \t:\-\u2013\u2014|=>~.]*")
# any @username anywhere in the caption
USER_RE = re.compile(r"(?<![\w@])@[A-Za-z][A-Za-z0-9_]{3,31}(?![A-Za-z0-9_])")
# t.me/username style links (not t.me/c/..., invite links, sticker packs)
_TME = r"(?:https?://)?(?:www\.)?(?:t|telegram)\.me/(?!c/|joinchat|\+|addstickers|addemoji|share)[A-Za-z][A-Za-z0-9_]{3,31}"
TME_RE = re.compile(_TME + r"(?:/[^\s]*)?", re.I)
TME_URL_RE = re.compile("^" + _TME, re.I)
LINK_KINDS = {"text_link", "url", "mention", "text_mention"}


def _u16_positions(text: str):
    pos = [0]
    for ch in text:
        pos.append(pos[-1] + (2 if ord(ch) > 0xFFFF else 1))
    return pos


def _kind(entity) -> str:
    t = entity.type
    return str(getattr(t, "name", t)).lower()


def _rewrite(text, entities, tag):
    text = str(text)      # Pyrogram's Str slices by UTF-16 units, which breaks on emoji -> plain str
    ents_in = list(entities or [])
    regions, taken = [], []          # regions: (start, end, replacement) in python-string indices

    def free(s, e):
        return all(e <= ts or s >= te for ts, te in taken)

    def add(s, e, repl):
        regions.append((s, e, repl))
        taken.append((s, e))

    # 1) "Extracted by" / "Downloaded by": rest of the line becomes our tag
    last_end = -1
    for m in EB_PAT.finditer(text):
        if m.start() < last_end:
            continue
        start = EB_SEP.match(text, m.end()).end()
        nl = text.find("\n", start)
        end = len(text) if nl == -1 else nl
        last_end = end
        full = tag + (f" | {BACKUP_LABEL} : {BACKUP_LINK}" if BACKUP_LINK else "")   # username + backup link
        if text[start:end].strip() == full and not any(_kind(e) in LINK_KINDS for e in ents_in):
            taken.append((start, end))                      # already ours
            continue
        add(start, end, (" " + full) if text[start - 1].isalnum() else full)

    # 2) t.me/username links anywhere
    for m in TME_RE.finditer(text):
        if free(m.start(), m.end()):
            add(m.start(), m.end(), tag)

    # 3) any @username anywhere
    for m in USER_RE.finditer(text):
        if m.group().lower() == tag.lower():
            continue
        if free(m.start(), m.end()):
            add(m.start(), m.end(), tag)

    # 4) hyperlinks whose target is a t.me/username page
    pos = _u16_positions(text)
    inv = {p: i for i, p in enumerate(pos)}
    for e in ents_in:
        if _kind(e) == "text_link" and getattr(e, "url", None) and TME_URL_RE.match(e.url):
            s, en = inv.get(e.offset), inv.get(e.offset + e.length)
            if s is not None and en is not None and free(s, en):
                add(s, en, tag)

    if not regions:
        return text, entities, False

    ents = ents_in
    for s_py, e_py, repl in sorted(regions, reverse=True):
        pos = _u16_positions(text)
        rs, re_ = pos[s_py], pos[e_py]
        delta = len(repl.encode("utf-16-le")) // 2 - (re_ - rs)
        kept = []
        for e in ents:
            o, end = e.offset, e.offset + e.length
            if end <= rs:
                kept.append(e)                                    # before the region
            elif o >= re_:
                n = copy.copy(e)
                n.offset = o + delta                              # after the region
                kept.append(n)
            elif _kind(e) in LINK_KINDS:
                continue                                          # hyperlink / username: remove
            elif o <= rs and end >= re_:
                n = copy.copy(e)
                n.length = e.length + delta                       # formatting around it: resize
                kept.append(n)
            elif o < rs:
                n = copy.copy(e)
                n.length = rs - o                                 # starts before, ends inside
                kept.append(n)
        ents = kept
        text = text[:s_py] + repl + text[e_py:]
    return text, ents, True


def rewrite_caption(text, entities, tag=None):
    """
    Returns (text, entities, changed). If anything goes wrong or nothing is detected
    (e.g. fancy-font usernames) the caption is left exactly as it was - never fails the job.
    """
    if not text:
        return text, entities, False
    try:
        return _rewrite(text, entities, tag or MY_TAG)
    except Exception as e:
        log.warning("caption rewrite skipped: %s", e)
        return text, entities, False


# ---------------------------------------------------------------- core job
async def fast_copy(client: Client, dest, src_chat, msg) -> bool:
    """Server-side copy. Returns True on success, False if content is restricted."""
    if getattr(msg, "has_protected_content", False) or getattr(msg.chat, "has_protected_content", False):
        return False
    try:
        kw = {}
        new_cap, new_ents, changed = rewrite_caption(msg.caption, msg.caption_entities)
        if changed:
            kw = dict(caption=new_cap, caption_entities=new_ents or None, parse_mode=ParseMode.DISABLED)
        try:
            await flood_safe(client.copy_message, dest, src_chat, msg.id, **kw)
        except Exception as e:
            if kw and "ENTITY" in str(e).upper():       # formatting bounds rejected: keep our text, drop formatting
                kw.pop("caption_entities", None)
                await flood_safe(client.copy_message, dest, src_chat, msg.id, **kw)
            else:
                raise
        return True
    except Exception as e:
        text = str(e).upper()
        if "RESTRICT" in text or "PROTECTED" in text or "FORWARDS" in text:
            return False
        raise


async def slow_transfer(client: Client, dest, msg, prog: Prog, job_dir: Path, dl_client: Client = None, extra_dl=None):
    # client = account that UPLOADS/sends; dl_client = account that can read the source (default: same)
    dl_client = dl_client or client
    media = msg.video or msg.document or msg.audio or msg.animation or msg.voice or msg.video_note
    caption, ents, _ = rewrite_caption(msg.caption or "", msg.caption_entities)

    if msg.photo:
        path = await dl_client.download_media(msg, file_name=str(job_dir) + "/")
        await client.send_photo(dest, path, caption=caption, caption_entities=ents or None, parse_mode=ParseMode.DISABLED)
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
            thumb = await dl_client.download_media(thumbs[0].file_id, file_name=str(job_dir / "thumb.jpg"))
        except Exception:
            thumb = None

    common = dict(chat_id=dest, progress=prog.cb)
    cap = dict(caption=caption, caption_entities=ents or None, parse_mode=ParseMode.DISABLED)

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
        t_send = time.monotonic()
        extras = []                                    # more accounts that can read the source = more download capacity
        for ec in (extra_dl or []):
            try:
                m2 = await ec.get_messages(msg.chat.id, msg.id)
                md2 = m2.video or m2.document or m2.audio or m2.animation or m2.voice or m2.video_note
                if md2 and getattr(md2, "file_id", None):
                    extras.append((ec, md2.file_id))
            except Exception as e:
                log.warning("extra downloader skipped: %s", e)
        await send(turbo.RelayFile(media.file_id, size, name, src_client=dl_client, extra_sources=extras))
        log.info("Send complete (upload + posting in channel): %.0fs", time.monotonic() - t_send)
        return
    except Exception as e:
        log.warning("relay failed (%s), falling back to disk mode", e)

    # 2) FALLBACK: parallel download to disk, then parallel upload
    msg = await dl_client.get_messages(msg.chat.id, msg.id)  # fresh file reference
    media = msg.video or msg.document or msg.audio or msg.animation or msg.voice or msg.video_note
    path = str(job_dir / name)
    prog.start_stage("🔽 Downloading (fallback)", size)
    await turbo.turbo_download(dl_client, media.file_id, size, path, prog)
    prog.start_stage("🔼 Uploading (fallback)", size)
    await send(path)


async def run_job(client: Client, job: dict):
    global flood_until
    status = job.get("status")
    batch = job.get("batch")
    prog = Prog()
    if batch:
        batch.prog = prog
    stop = asyncio.Event()
    tick = asyncio.create_task(ticker(status, prog, stop)) if status else None
    job_dir = WORKDIR / f"{job['user']}_{int(time.time() * 1000)}"
    t0 = time.monotonic()

    async def say(text):
        if status is None:
            return
        try:
            await status.edit_text(text)
        except Exception:
            pass

    try:
        try:
            dl = job.get("dl") or client
            msg = await flood_safe(dl.get_messages, job["src_chat"], job["src_mid"], paced=False)
        except Exception as e:
            raise RuntimeError(f"Source message nahi mila ({e}). Kya tumhara account us channel me member hai?")
        if msg.empty:
            raise RuntimeError("Message empty/deleted hai ya accessible nahi.")

        dest = job["dest"]
        if dl is client and await fast_copy(client, dest, job["src_chat"], msg):
            await say(f"✅ Copied instantly in {time.monotonic() - t0:.1f}s")
            job["result"] = "ok"
            if batch:
                batch.ok += 1
            return

        await say("🔒 Content restricted, turbo download/upload shuru kar raha hoon...")
        job_dir.mkdir(parents=True, exist_ok=True)
        await slow_transfer(client, dest, msg, prog, job_dir, dl_client=dl, extra_dl=job.get("dl_extra"))
        took = time.monotonic() - t0
        size = prog.total or 0
        avg = size / took if took else 0
        await say(f"✅ Done: {human(size)} in {took:.0f}s (avg {human(avg)}/s)")
        job["result"] = "ok"
        if batch:
            batch.ok += 1
    except asyncio.CancelledError:
        job["result"] = "cancel"
        await say("🛑 Cancelled")
        if batch:
            batch.cancelled += 1
        raise
    except Exception as e:
        job["result"] = "fail"
        log.exception("job failed")
        if isinstance(e, FloodWait):
            flood_until = max(flood_until, time.monotonic() + e.value + 2)
            await say(f"⏳ FloodWait {e.value}s - bot itni der ruk kar aage badhega")
        else:
            await say(f"❌ Failed: {e}")
        if batch:
            batch.fail += 1
            batch.last_error = str(e)
    finally:
        stop.set()
        if tick:
            await tick
        shutil.rmtree(job_dir, ignore_errors=True)
        if job.get("done"):
            job["done"].set()


async def job_worker(client: Client, dl: Client = None, extra_dl=None):
    while True:
        job = await jobs.get()
        task = None
        try:
            wait = flood_until - time.monotonic()
            if wait > 0:
                log.info("FloodWait chal raha hai, %.0fs ruk raha hoon", wait)
                await asyncio.sleep(wait)
            if dl is not None:
                job["dl"] = dl                  # split mode: this account downloads
                job["dl_extra"] = extra_dl or []
            task = asyncio.create_task(run_job(client, job))
            current_tasks.add(task)
            await asyncio.wait({task})          # does not raise if /cancel kills the job
        except asyncio.CancelledError:
            if task:
                task.cancel()
            raise
        finally:
            if task:
                current_tasks.discard(task)
            jobs.task_done()


def need_dest(message) -> bool:
    """In user mode, files sent back into Saved Messages would re-trigger the bot (loop)."""
    return USER_MODE and get_dest(message.from_user.id, message.chat.id) == message.chat.id


NEED_DEST_TEXT = "⚠️ Pehle destination set karo: /setdest -100xxxxxxxxxx  (ya @channel). Saved Messages me result nahi bhej sakta."


async def enqueue(message, src_chat, src_mid):
    if need_dest(message):
        return await message.reply_text(NEED_DEST_TEXT)
    status = await flood_safe(message.reply_text, "⏳ Queued...", quote=True)
    await jobs.put(
        dict(
            user=message.from_user.id,
            status=status,
            src_chat=src_chat,
            src_mid=src_mid,
            dest=get_dest(message.from_user.id, message.chat.id),
        )
    )


async def enqueue_batch(message, items):
    """items = [(chat, mid), ...]  ->  ONE status message for the whole batch."""
    if need_dest(message):
        return await message.reply_text(NEED_DEST_TEXT)
    status = await flood_safe(message.reply_text, f"📦 Batch: 0 done, 0 failed / {len(items)}\n(rokne ke liye /cancel bhejo)", quote=True)
    batch = Batch(len(items), status)
    dest = get_dest(message.from_user.id, message.chat.id)
    for chat, mid in items:
        await jobs.put(dict(user=message.from_user.id, status=None, batch=batch,
                            src_chat=chat, src_mid=mid, dest=dest))
    asyncio.create_task(batch_reporter(batch))


# ---------------------------------------------------------------- handlers
async def start_cmd(client, message):
    await message.reply_text(
        "🚀 Turbo Forward Bot\n\n"
        "• Message link bhejo (ek ya zyada)\n"
        "• /batch <first_link> <last_link> : poori range\n"
        "• Koi file forward karo : destination par copy ho jayegi\n"
        "• /setdest <chat_id ya @username> : destination (bot wahan admin ho)\n"
        "• /auto <link> <N> [delay] : link + uske aage ke N links, ek ek karke\n• /mydest, /cleardest, /queue\n• /cancel : sab kuch rok do (chalti + queue wali jobs)"
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


async def cancel_cmd(client, message):
    """Stop the running job(s) and throw away everything waiting in the queue."""
    dropped = 0
    while True:
        try:
            job = jobs.get_nowait()
        except asyncio.QueueEmpty:
            break
        jobs.task_done()
        dropped += 1
        if job.get("batch"):
            job["batch"].cancelled += 1
        job["result"] = "cancel"
        if job.get("done"):
            job["done"].set()
    autos = [t for t in auto_tasks if not t.done()]
    for t in autos:
        t.cancel()
    running = [t for t in current_tasks if not t.done()]
    for t in running:
        t.cancel()
    await message.reply_text(
        f"🛑 Cancel ho gaya: {len(running)} chalti job band, {dropped} queue wali hata di"
        f"{', auto mode band' if autos else ''}.\n"
        "Naya kaam ab seedha bhej sakte ho."
    )


async def queue_cmd(client, message):
    await message.reply_text(f"Queue me {jobs.qsize()} job(s) hain.")


def make_link(chat, mid) -> str:
    if isinstance(chat, int):                       # private/numeric channel  -100xxxxxxxxxx
        return f"https://t.me/c/{str(chat)[4:]}/{mid}"
    return f"https://t.me/{chat}/{mid}"


async def auto_run(client, message, chat, first, n, delay):
    user_id = message.from_user.id
    dest = get_dest(user_id, message.chat.id)
    total = n + 1
    ok = fail = streak = 0
    reason = "complete"
    try:
        for i in range(total):
            mid = first + i
            posted = await flood_safe(client.send_message, message.chat.id,
                                      f"{AUTO_MARK} #{i + 1}/{total}  {make_link(chat, mid)}")
            status = await flood_safe(posted.reply_text, "⏳ Queued...", quote=True)
            job = dict(user=user_id, status=status, src_chat=chat, src_mid=mid, dest=dest,
                       done=asyncio.Event())
            await jobs.put(job)
            await job["done"].wait()
            if job.get("result") == "ok":
                ok += 1
                streak = 0
                await asyncio.sleep(delay)              # the 30s gap, only after a real transfer
            else:
                fail += 1
                streak += 1
                if streak >= AUTO_STOP_AFTER_FAILS:
                    reason = f"{streak} message lagatar fail hue (shayad channel ke end tak pahunch gaye)"
                    break
                await asyncio.sleep(3)
    except asyncio.CancelledError:
        reason = "cancelled"
        raise
    finally:
        try:
            await message.reply_text(f"🤖 Auto {reason}: {ok} done, {fail} failed (total {total} links)", quote=False)
        except Exception:
            pass


async def auto_cmd(client, message):
    links = all_links(message.text or "")
    nums = [int(a) for a in message.command[1:] if a.isdigit()]
    if not links or not nums:
        return await message.reply_text(
            "Use: /auto <link> <kitne_aur_links> [delay_sec]\n"
            "Example: /auto https://t.me/c/123456/100 100 30\n"
            "Pehle ye link, phir iske aage ke 100 links, har ek ke baad 30 sec ruk kar."
        )
    if need_dest(message):
        return await message.reply_text(NEED_DEST_TEXT)
    if any(not t.done() for t in auto_tasks):
        return await message.reply_text("Ek auto already chal raha hai. Pehle /cancel bhejo.")
    n = min(nums[0], 500)
    delay = max(nums[1], 5) if len(nums) > 1 else AUTO_DELAY
    chat, first = links[0]
    task = asyncio.create_task(auto_run(client, message, chat, first, n, delay))
    auto_tasks.add(task)
    task.add_done_callback(auto_tasks.discard)
    await message.reply_text(
        f"🤖 Auto shuru: #{first} se aage {n} links (total {n + 1}), har ek ke baad {delay}s ka gap.\n"
        "Rokne ke liye /cancel bhejo."
    )


async def batch_cmd(client, message):
    links = all_links(message.text)
    if len(links) < 2 or links[0][0] != links[1][0]:
        return await message.reply_text("Use: /batch <first_link> <last_link> (same channel ke)")
    chat, a = links[0]
    last = links[1][1]
    lo, hi = min(a, last), max(a, last)
    if hi - lo > 300:
        return await message.reply_text("Ek baar me max 300 messages.")
    await enqueue_batch(message, [(chat, mid) for mid in range(lo, hi + 1)])


async def link_handler(client, message):
    if (message.text or "").startswith(AUTO_MARK):     # posted by /auto itself
        return
    links = all_links(message.text or "")
    if not links:
        return
    links = links[:50]
    if len(links) == 1:
        await enqueue(message, *links[0])
    else:
        await enqueue_batch(message, links)


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
    app.add_handler(MessageHandler(cancel_cmd, private & filters.command("cancel")))
    app.add_handler(MessageHandler(auto_cmd, private & filters.command("auto")))
    app.add_handler(MessageHandler(batch_cmd, private & filters.command("batch")))
    app.add_handler(MessageHandler(link_handler, private & filters.text & ~filters.command(["start", "setdest", "mydest", "cleardest", "queue", "batch", "cancel", "auto"])))
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
