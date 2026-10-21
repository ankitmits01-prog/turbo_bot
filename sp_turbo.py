"""
turbo.py - multi-session parallel download + upload engine for Pyrogram / Pyrofork.

Why it is fast
  * Telegram throttles each MTProto connection, so we open several sessions to the same DC
    and spread the chunks across all of them (several requests in flight per session).
  * RELAY mode: every 1 MB chunk is uploaded the moment it has been downloaded (no disk).
  * Two accounts can share the work: one downloads (RelayFile.src_client), the other uploads.

Why it stays fast (v3)
  * A request that hangs is abandoned after TURBO_TIMEOUT seconds and re-sent on ANOTHER session
    (before, a dead connection kept eating requests -> slow tails and "no speed at all").
  * Sessions that went stale while the bot was idle are health-checked and restarted.
  * End-game: when only a few chunks are left, idle workers duplicate them, first copy wins.
"""
import asyncio
import inspect
import io
import logging
import math
import os
import random
import time
from pathlib import PurePath

from pyrogram import Client, raw
from pyrogram.errors import AuthBytesInvalid, FloodWait
try:
    from pyrogram.errors import FloodPremiumWait
except ImportError:                    # older forks
    FloodPremiumWait = FloodWait
FLOOD_ERRORS = (FloodWait, FloodPremiumWait)
from pyrogram.file_id import FileId, FileType
from pyrogram.session import Session
from pyrogram.session.auth import Auth

log = logging.getLogger("turbo")

try:                                   # settings in .env must be visible before we read them below
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

SESSIONS = int(os.getenv("TURBO_SESSIONS", "4"))              # connections per DC
INFLIGHT = int(os.getenv("TURBO_INFLIGHT", "2"))              # requests in flight per connection
ADAPTIVE = os.getenv("TURBO_ADAPTIVE", "0") == "1"            # 1 = auto-slow-down on flood waits
REQ_TIMEOUT = float(os.getenv("TURBO_TIMEOUT", "12"))         # seconds before a hung request is re-sent
DL_CHUNK = 1024 * 1024                                        # max allowed for upload.GetFile
UL_CHUNK = 512 * 1024                                         # max allowed for SaveBigFilePart
BIG_FILE = 10 * 1024 * 1024                                   # below this, use stock Pyrogram
RETRIES = 6

STATS = {"timeouts": 0, "dups": 0}
_pools: dict = {}
_pool_locks: dict = {}
_pool_seen: dict = {}
_auth_cache: dict = {}
_original_save_file = None

try:
    _INV_PARAMS = set(inspect.signature(Session.invoke).parameters)
except Exception:
    _INV_PARAMS = set()
_CAN_NO_SLEEP = "sleep_threshold" in _INV_PARAMS


def _kw(**want):
    """Only pass keyword args this Pyrogram version's Session.invoke understands."""
    return {k: v for k, v in want.items() if k in _INV_PARAMS}


# --------------------------------------------------------------------------
# session pool
# --------------------------------------------------------------------------
async def _build_pool(client: Client, dc_id: int, n: int):
    key = (id(client), dc_id)
    home_dc = await client.storage.dc_id()
    test_mode = await client.storage.test_mode()
    need_import = dc_id != home_dc
    cached = need_import and key in _auth_cache

    if need_import:
        auth_key = _auth_cache[key] if cached else await Auth(client, dc_id, test_mode).create()
    else:
        auth_key = await client.storage.auth_key()

    sessions = []
    try:
        first = Session(client, dc_id, auth_key, test_mode, is_media=True)
        await first.start()
        sessions.append(first)

        if need_import and not cached:
            for _ in range(3):
                exported = await client.invoke(raw.functions.auth.ExportAuthorization(dc_id=dc_id))
                try:
                    await first.invoke(
                        raw.functions.auth.ImportAuthorization(id=exported.id, bytes=exported.bytes)
                    )
                    break
                except AuthBytesInvalid:
                    continue
            else:
                raise AuthBytesInvalid
            _auth_cache[key] = auth_key

        rest = [Session(client, dc_id, auth_key, test_mode, is_media=True) for _ in range(n - 1)]
        await asyncio.gather(*(s.start() for s in rest))
        sessions.extend(rest)
    except BaseException:
        await asyncio.gather(*(s.stop() for s in sessions), return_exceptions=True)
        raise
    log.info("Turbo pool ready: DC%s x %s sessions", dc_id, len(sessions))
    return sessions


async def _ping(session) -> bool:
    try:
        await session.invoke(raw.functions.Ping(ping_id=random.getrandbits(31)), **_kw(retries=0, timeout=4))
        return True
    except Exception:
        return False


async def _heal(pool):
    """Restart sessions that went stale (e.g. Telegram closed the idle connection)."""
    ok = await asyncio.gather(*(_ping(s) for s in pool))
    for s, good in zip(pool, ok):
        if good:
            continue
        log.warning("Turbo: stale session found - restarting it")
        try:
            if hasattr(s, "restart"):
                await s.restart()
            else:
                await s.stop()
                await s.start()
        except Exception as e:
            log.warning("Turbo: session restart failed: %s", e)


async def get_pool(client: Client, dc_id: int):
    key = (id(client), dc_id)
    lock = _pool_locks.setdefault(key, asyncio.Lock())
    async with lock:
        pool = _pools.get(key)
        if pool is None:
            pool = await _build_pool(client, dc_id, SESSIONS)
            _pools[key] = pool
        elif time.monotonic() - _pool_seen.get(key, 0) > 8:      # was idle: make sure it still works
            await _heal(pool)
        _pool_seen[key] = time.monotonic()
        return pool


async def drop_pool(client: Client, dc_id: int):
    key = (id(client), dc_id)
    pool = _pools.pop(key, None)
    _auth_cache.pop(key, None)
    _pool_seen.pop(key, None)
    if pool:
        await asyncio.gather(*(s.stop() for s in pool), return_exceptions=True)


async def close_all():
    for key in list(_pools):
        pool = _pools.pop(key, None)
        if pool:
            await asyncio.gather(*(s.stop() for s in pool), return_exceptions=True)
    _auth_cache.clear()
    _pool_seen.clear()


# --------------------------------------------------------------------------
# adaptive limiter (only used when TURBO_ADAPTIVE=1)
# --------------------------------------------------------------------------
class Limiter:
    """
    Adaptive concurrency limiter. When Telegram answers with a flood wait we (once per burst):
    pause ALL workers together, remember the level that caused it as a ceiling, and drop below it.
    We then creep back up to just under that ceiling and only probe higher after a few quiet
    minutes - so we hover just under what Telegram tolerates.
    """

    PROBE_EVERY = 60       # seconds of clean running before trying one level higher

    def __init__(self, name: str, maxn: int):
        self.name, self.max, self.limit = name, maxn, maxn
        self.floor = max(2, maxn // 4)
        self.ceiling = maxn
        self.ceiling_t = time.monotonic()
        self.active = 0
        self.ok_streak = 0
        self.blocked_until = 0.0

    async def acquire(self):
        while True:
            wait = self.blocked_until - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
                continue
            if self.active < self.limit:
                self.active += 1
                return
            await asyncio.sleep(0.05)

    def release(self):
        self.active -= 1

    def on_ok(self):
        now = time.monotonic()
        if self.ceiling < self.max and now - self.ceiling_t > self.PROBE_EVERY:
            self.ceiling += 1
            self.ceiling_t = now
        self.ok_streak += 1
        if self.ok_streak >= 8 and self.limit < min(self.ceiling, self.max):
            self.limit += 1
            self.ok_streak = 0

    def on_flood(self, seconds: float, kind: str = "FloodWait"):
        now = time.monotonic()
        if now >= self.blocked_until:              # first flood of this burst only
            self.ceiling = max(self.floor, self.limit - 1)
            self.ceiling_t = now
            self.limit = max(self.floor, int(self.limit * 0.6))
        self.ok_streak = 0
        self.blocked_until = max(self.blocked_until, now + seconds + 1 + random.random())
        log.warning("%s %ss on %s -> parallel requests now %s (ceiling %s)", kind, seconds, self.name, self.limit, self.ceiling)


DL_LIM = Limiter("download", SESSIONS * INFLIGHT)
UL_LIM = Limiter("upload", SESSIONS * INFLIGHT)


# --------------------------------------------------------------------------
# request helper: timeout -> re-send on ANOTHER session
# --------------------------------------------------------------------------
async def _call(session, query):
    kw = _kw(retries=0, timeout=REQ_TIMEOUT)
    if ADAPTIVE:
        kw.update(_kw(sleep_threshold=0))      # raise FloodWait so the Limiter can slow everything down
    return await session.invoke(query, **kw)


async def _invoke_retry(sessions, query, limiter: "Limiter" = None, start: int = 0):
    if not isinstance(sessions, (list, tuple)):
        sessions = [sessions]
    if not ADAPTIVE:
        limiter = None
    errors = floods = 0
    while True:
        session = sessions[(start + errors) % len(sessions)]      # a failure moves us to the next session
        if limiter:
            await limiter.acquire()
        try:
            r = await _call(session, query)
            if limiter:
                limiter.on_ok()
            return r
        except FLOOD_ERRORS as e:
            floods += 1
            wait = getattr(e, "value", 1)
            if wait > 120 or floods > 40:      # long ban: fail fast, bot pauses everything
                raise
            if limiter:
                limiter.on_flood(wait, type(e).__name__)
            else:
                await asyncio.sleep(wait + 1)
        except asyncio.CancelledError:
            raise
        except Exception as e:                 # timeouts, connection resets, etc.
            errors += 1
            STATS["timeouts"] += 1
            if errors >= RETRIES:
                raise
            log.debug("request failed on a session (%s: %s) - trying another", type(e).__name__, e)
            await asyncio.sleep(min(0.3 * errors, 2))
        finally:
            if limiter:
                limiter.release()


# --------------------------------------------------------------------------
# chunk scheduling with end-game duplication
# --------------------------------------------------------------------------
class Chunks:
    def __init__(self, total: int):
        self.total = total
        self.next = 0
        self.claims = {}
        self.finished = set()
        self.done_evt = asyncio.Event()
        if total == 0:
            self.done_evt.set()

    async def claim(self):
        """Next chunk index; at the end, a chunk someone else is still working on (first copy wins)."""
        while True:
            if self.next < self.total:
                c = self.next
                self.next += 1
            else:
                if len(self.finished) >= self.total:
                    return None
                pend = [k for k, n in self.claims.items() if k not in self.finished and n < 2]
                if not pend:
                    await asyncio.sleep(0.05)
                    continue
                c = random.choice(pend)
                STATS["dups"] += 1
            self.claims[c] = self.claims.get(c, 0) + 1
            return c

    def finish(self, c: int) -> bool:
        if c in self.finished:
            return False
        self.finished.add(c)
        if len(self.finished) >= self.total:
            self.done_evt.set()
        return True


async def _run_workers(count: int, worker, done: asyncio.Event):
    """Run workers until every chunk is finished; cancel stragglers; surface the first error."""
    loop = asyncio.get_running_loop()
    tasks = {loop.create_task(worker(i)) for i in range(count)}
    waiter = loop.create_task(done.wait())
    pending = set(tasks) | {waiter}
    try:
        while pending:
            finished, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for t in finished:
                if t is waiter:
                    return
                exc = t.exception()
                if exc:
                    raise exc
    finally:
        everything = tasks | {waiter}
        for t in everything:
            if not t.done():
                t.cancel()
        await asyncio.gather(*everything, return_exceptions=True)


def _location(fid):
    return raw.types.InputDocumentFileLocation(
        id=fid.media_id,
        access_hash=fid.access_hash,
        file_reference=fid.file_reference,
        thumb_size=fid.thumbnail_size,
    )


# --------------------------------------------------------------------------
# parallel download to disk (fallback path)
# --------------------------------------------------------------------------
async def turbo_download(client: Client, file_id: str, file_size: int, dest_path: str, prog=None):
    """Download a Telegram document/video/audio into dest_path using many sessions."""
    fid = FileId.decode(file_id)
    if fid.file_type in (FileType.PHOTO, FileType.CHAT_PHOTO):
        raise ValueError("turbo_download only handles documents/videos/audio")
    location = _location(fid)

    for build_try in range(2):
        try:
            pool = await get_pool(client, fid.dc_id)
            break
        except Exception:
            await drop_pool(client, fid.dc_id)
            if build_try == 1:
                raise

    loop = asyncio.get_running_loop()
    total = math.ceil(file_size / DL_CHUNK)
    chunks = Chunks(total)

    os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)
    fd = os.open(dest_path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    os.ftruncate(fd, file_size)

    async def worker(idx):
        while True:
            c = await chunks.claim()
            if c is None:
                return
            offset = c * DL_CHUNK
            r = await _invoke_retry(
                pool, raw.functions.upload.GetFile(location=location, offset=offset, limit=DL_CHUNK), DL_LIM, idx
            )
            if not isinstance(r, raw.types.upload.File):
                raise RuntimeError("CDN redirect not supported, use normal download")
            data = r.bytes
            if len(data) < min(DL_CHUNK, file_size - offset):
                raise RuntimeError(f"Short chunk at part {c}: {len(data)}")
            await loop.run_in_executor(None, os.pwrite, fd, data, offset)
            if chunks.finish(c) and prog is not None:
                prog.done += len(data)

    try:
        await _run_workers(SESSIONS * INFLIGHT, worker, chunks.done_evt)
    except asyncio.CancelledError:
        raise
    except BaseException:
        await drop_pool(client, fid.dc_id)     # a broken pool is the usual cause of repeated failures
        raise
    finally:
        os.close(fd)
    return dest_path


# --------------------------------------------------------------------------
# parallel upload (patches Client.save_file, so send_video/send_document use it)
# --------------------------------------------------------------------------
def _reporter(progress, progress_args, size, state):
    async def report(final=False):
        if progress is None:
            return
        now = time.monotonic()
        if not final and now - state["last_report"] < 0.5:
            return
        state["last_report"] = now
        res = progress(min(state["done"], size), size, *progress_args)
        if asyncio.iscoroutine(res):
            await res
    return report


async def _turbo_save_file(
    self: Client,
    path,
    file_id: int = None,
    file_part: int = 0,
    progress=None,
    progress_args: tuple = (),
):
    if isinstance(path, RelayFile):
        return await _relay(self, path, progress, progress_args)

    # Only take over big uploads from a real path; everything else stays stock.
    if (
        path is None
        or file_id is not None
        or not isinstance(path, (str, PurePath))
        or os.path.getsize(path) <= BIG_FILE
    ):
        return await _original_save_file(self, path, file_id, file_part, progress, progress_args)

    path = str(path)
    file_size = os.path.getsize(path)
    limit_mib = 4000 if getattr(self.me, "is_premium", False) else 2000
    if file_size > limit_mib * 1024 * 1024:
        raise ValueError(f"Can't upload files bigger than {limit_mib} MiB")

    total_parts = math.ceil(file_size / UL_CHUNK)
    big_id = self.rnd_id()
    name = os.path.basename(path)
    home_dc = await self.storage.dc_id()
    pool = await get_pool(self, home_dc)

    loop = asyncio.get_running_loop()
    fd = os.open(path, os.O_RDONLY)
    state = {"done": 0, "last_report": 0.0}
    report = _reporter(progress, progress_args, file_size, state)
    chunks = Chunks(total_parts)

    async def worker(idx):
        while True:
            c = await chunks.claim()
            if c is None:
                return
            data = await loop.run_in_executor(None, os.pread, fd, UL_CHUNK, c * UL_CHUNK)
            await _invoke_retry(
                pool,
                raw.functions.upload.SaveBigFilePart(
                    file_id=big_id, file_part=c, file_total_parts=total_parts, bytes=data
                ),
                UL_LIM,
                idx,
            )
            if chunks.finish(c):
                state["done"] += len(data)
                await report()

    try:
        await _run_workers(SESSIONS * INFLIGHT, worker, chunks.done_evt)
    except asyncio.CancelledError:
        raise
    except BaseException:
        await drop_pool(self, home_dc)
        raise
    finally:
        os.close(fd)

    await report(final=True)
    return raw.types.InputFileBig(id=big_id, parts=total_parts, name=name)


# --------------------------------------------------------------------------
# RELAY: download and upload at the same time, no disk, no waiting
# --------------------------------------------------------------------------
class RelayFile(io.IOBase):
    """
    Pass this to send_video/send_document instead of a path. save_file then streams the
    source Telegram file chunk by chunk straight into the upload: every 1 MB chunk is
    uploaded the moment it arrives, so total time ~ max(download, upload), not the sum.
    """

    def __init__(self, file_id: str, file_size: int, name: str, src_client=None):
        super().__init__()
        self.file_id = file_id
        self.file_size = file_size
        self.name = name
        self.src_client = src_client      # account that can read the source (None = same account)


async def _relay(self: Client, rf: RelayFile, progress, progress_args):
    src = rf.src_client or self           # downloads use `src`, uploads use `self`
    fid = FileId.decode(rf.file_id)
    size = rf.file_size
    limit_mib = 4000 if getattr(self.me, "is_premium", False) else 2000
    if size > limit_mib * 1024 * 1024:
        raise ValueError(f"Can't upload files bigger than {limit_mib} MiB")

    location = _location(fid)
    home_dc = await self.storage.dc_id()
    log.info("Relay %.0f MB: source file on DC%s -> upload via DC%s%s", size / 1048576, fid.dc_id, home_dc,
             " (2 accounts)" if src is not self else "")
    dl_pool = await get_pool(src, fid.dc_id)
    ul_pool = await get_pool(self, home_dc)

    total_dl = math.ceil(size / DL_CHUNK)
    total_parts = math.ceil(size / UL_CHUNK)
    big_id = self.rnd_id()
    state = {"done": 0, "last_report": 0.0}
    report = _reporter(progress, progress_args, size, state)
    chunks = Chunks(total_dl)
    t0, s0 = time.monotonic(), dict(STATS)

    async def worker(idx):
        while True:
            c = await chunks.claim()
            if c is None:
                return
            offset = c * DL_CHUNK
            r = await _invoke_retry(
                dl_pool, raw.functions.upload.GetFile(location=location, offset=offset, limit=DL_CHUNK), DL_LIM, idx
            )
            if not isinstance(r, raw.types.upload.File):
                raise RuntimeError("CDN redirect not supported")
            data = r.bytes
            if len(data) < min(DL_CHUNK, size - offset):
                raise RuntimeError(f"Short chunk at {offset}")
            ops = [
                _invoke_retry(
                    ul_pool,
                    raw.functions.upload.SaveBigFilePart(
                        file_id=big_id,
                        file_part=(offset + k) // UL_CHUNK,
                        file_total_parts=total_parts,
                        bytes=data[k:k + UL_CHUNK],
                    ),
                    UL_LIM,
                    idx + 1 + k // UL_CHUNK,
                )
                for k in range(0, len(data), UL_CHUNK)
            ]
            await asyncio.gather(*ops)
            if chunks.finish(c):
                state["done"] += len(data)
                await report()

    try:
        await _run_workers(SESSIONS * INFLIGHT, worker, chunks.done_evt)
    except asyncio.CancelledError:
        raise
    except BaseException:
        await drop_pool(src, fid.dc_id)
        await drop_pool(self, home_dc)
        raise
    await report(final=True)
    took = max(time.monotonic() - t0, 0.001)
    log.info("Relay done: %.0f MB in %.0fs (%.1f MB/s) | re-sent after timeout: %d | end-game duplicates: %d",
             size / 1048576, took, size / 1048576 / took,
             STATS["timeouts"] - s0["timeouts"], STATS["dups"] - s0["dups"])
    return raw.types.InputFileBig(id=big_id, parts=total_parts, name=rf.name)


def install(client_cls=Client):
    """Call once at start-up: makes every big upload use the parallel engine."""
    global _original_save_file
    if _original_save_file is not None:
        return
    _original_save_file = client_cls.save_file
    client_cls.save_file = _turbo_save_file
    log.info("Turbo settings ACTIVE: sessions=%s inflight=%s adaptive=%s timeout=%ss (no-sleep supported: %s)",
             SESSIONS, INFLIGHT, ADAPTIVE, REQ_TIMEOUT, _CAN_NO_SLEEP)
