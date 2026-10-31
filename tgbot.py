"""
TURBO CONTROL BOT - everything from a Telegram bot chat (no PC, no terminal after setup).

  /menu          buttons: add account, choose accounts, mode, destination, caption, queue, cancel
  /addaccount    log in ANY account from the chat (phone -> code -> 2FA)
  Mode SPLIT     one account downloads (member of the source), another uploads (admin of the destination)
  Mode SINGLE    one account does both (also gives the instant server-side copy when it works)
  Caption        after "Extracted by / Downloaded by / Powered by": your @username (+ your backup link)
  Links          send message links, /batch <first> <last>, /auto <link> <N> [delay], /cancel, /queue
"""
import asyncio
import copy
import filecmp
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import time
from pathlib import Path

from pyrogram import Client, filters, idle
from pyrogram.enums import ParseMode
from pyrogram.errors import (FloodWait, PasswordHashInvalid, PhoneCodeExpired, PhoneCodeInvalid,
                             PhoneNumberInvalid, SessionPasswordNeeded)
from pyrogram.handlers import CallbackQueryHandler, MessageHandler
from pyrogram.types import InlineKeyboardButton as Btn
from pyrogram.types import InlineKeyboardMarkup as Markup

import sp_core as core
import sp_turbo as turbo

log = logging.getLogger("tgbot")

HERE = Path(__file__).resolve().parent
CFG_FILE = HERE / "config.json"
ACC_FILE = HERE / "accounts.json"
CTRL = "turbo_ctrl"                                   # the bot's own session
NAME_RE = re.compile(r"[A-Za-z0-9_]{2,20}")
TAG_RE = re.compile(r"@?[A-Za-z][A-Za-z0-9_]{3,31}")
COMMANDS = ["start", "menu", "help", "addaccount", "accounts", "removeaccount", "setdest", "mydest", "cleardest",
            "setusername", "setbackup", "clearbackup", "caption", "queue", "cancel", "batch", "auto",
            "update", "restart", "rollback", "logs", "flood", "settings", "set", "speed", "parallel", "settransit", "cleartransit", "lasterror"]

GIT_REPO = os.getenv("GIT_REPO", "https://github.com/ankitmits01-prog/turbo_bot.git")
CODE_FILES = ["tgbot.py", "sp_core.py", "sp_turbo.py"]       # launcher.py is deliberately never auto-updated
SERVICE = "turbo-tgbot"
ENV_FILE = HERE / ".env"
BACKUP_DIR = HERE / "backup_code"

# speed presets: total parallel requests = SESSIONS x INFLIGHT (per account, per direction)
PRESETS = {
    "max": {"TURBO_SESSIONS": 8, "TURBO_INFLIGHT": 3, "TURBO_ADAPTIVE": 0, "TURBO_START": 12},
    "fast": {"TURBO_SESSIONS": 6, "TURBO_INFLIGHT": 3, "TURBO_ADAPTIVE": 1, "TURBO_START": 8},
    "safe": {"TURBO_SESSIONS": 4, "TURBO_INFLIGHT": 2, "TURBO_ADAPTIVE": 1, "TURBO_START": 4},
}
# settings you may change from the chat: name -> (min, max)
TUNABLE = {"TURBO_SESSIONS": (1, 12), "TURBO_INFLIGHT": (1, 6), "TURBO_ADAPTIVE": (0, 1), "TURBO_START": (1, 12),
           "TURBO_FRESH": (0, 1), "TURBO_TIMEOUT": (4, 60), "AUTO_DELAY": (5, 300), "SEND_INTERVAL": (0, 30)}

# ---------------------------------------------------------------- saved settings
DEFAULTS = {"mode": "split", "dl": "", "ul": "", "single": "", "helpers": [], "parallel": 1, "transit": None, "dest": None, "tag": "", "backup": "", "label": "Backup"}


def load_cfg() -> dict:
    try:
        data = json.loads(CFG_FILE.read_text())
    except Exception:
        data = {}
    cfg = {**DEFAULTS, **data}
    cfg["helpers"] = list(cfg.get("helpers") or [])
    if not cfg["tag"]:
        cfg["tag"] = core.MY_TAG
    if not cfg["backup"] and core.BACKUP_LINK:
        cfg["backup"] = core.BACKUP_LINK
    return cfg


CFG = load_cfg()


def save_cfg():
    CFG_FILE.write_text(json.dumps(CFG, indent=1, ensure_ascii=False))
    try:
        CFG_FILE.chmod(0o600)
    except Exception:
        pass


def sync_caption():
    core.MY_TAG, core.BACKUP_LINK, core.BACKUP_LABEL = CFG["tag"], CFG["backup"], CFG["label"]


def caption_text() -> str:
    return CFG["tag"] + (f" | {CFG['label']} : {CFG['backup']}" if CFG["backup"] else "")


# bot.py's helpers read these at call time -> one global destination, must be set first
core.USER_MODE = False
core.need_dest = lambda message: not CFG.get("dest")
core.NEED_DEST_TEXT = "⚠️ Pehle destination set karo: /setdest -100xxxxxxxxxx (ya @channel). Account wahan admin ho."
core.get_dest = lambda user_id, default_chat: CFG.get("dest") or default_chat


# ---------------------------------------------------------------- accounts
def read_accounts() -> dict:
    try:
        return json.loads(ACC_FILE.read_text())
    except Exception:
        return {}


def write_accounts(data: dict):
    ACC_FILE.write_text(json.dumps(data, indent=1, ensure_ascii=False))
    try:
        ACC_FILE.chmod(0o600)
    except Exception:
        pass


def remember(name: str, me):
    data = read_accounts()
    data[name] = {"id": me.id, "name": me.first_name, "username": me.username,
                  "premium": bool(getattr(me, "is_premium", False))}
    write_accounts(data)


def session_names() -> list:
    return sorted(p.stem for p in HERE.glob("*.session") if p.stem != CTRL)


def label(name: str) -> str:
    a = read_accounts().get(name)
    if not a:
        return name
    return f"{name} · {a.get('name') or '?'}{' ⭐' if a.get('premium') else ''}"


def busy() -> bool:
    return (not core.jobs.empty()) or any(not t.done() for t in core.current_tasks) \
        or any(not t.done() for t in core.auto_tasks)


class Runtime:
    """Keeps the chosen accounts logged in; switching roles needs no restart."""

    def __init__(self):
        self.clients = {}
        self.lock = asyncio.Lock()

    def wanted(self) -> list:
        names = [CFG["single"]] if CFG["mode"] == "single" else [CFG["dl"], CFG["ul"]] + list(CFG["helpers"])
        return list(dict.fromkeys(n for n in names if n))

    async def _start(self, name: str):
        if name in self.clients:
            return self.clients[name]
        if not (HERE / f"{name}.session").exists():
            raise RuntimeError(f"'{name}' ka session nahi mila. /addaccount se jodo.")
        c = Client(name, api_id=core.API_ID, api_hash=core.API_HASH, workdir=str(HERE),
                   sleep_threshold=60, max_concurrent_transmissions=4)
        try:
            await c.start()
        except Exception as e:
            if "AUTH_KEY" in str(e).upper() or type(e).__name__ in ("AuthKeyUnregistered", "SessionRevoked", "UserDeactivated"):
                raise RuntimeError(f"'{name}' ka login Telegram ne hata diya hai. /removeaccount {name} karke /addaccount se dobara jodo.")
            raise
        remember(name, c.me)
        try:                                           # let this session know the account's chats
            async for _ in c.get_dialogs():
                pass
        except Exception as e:
            log.warning("get_dialogs(%s) failed: %s", name, e)
        self.clients[name] = c
        return c

    async def apply(self):
        async with self.lock:
            want = self.wanted()
            for name in list(self.clients):
                if name not in want:
                    c = self.clients.pop(name)
                    try:
                        await c.stop()
                    except Exception as e:
                        log.warning("stop(%s): %s", name, e)
            for name in want:
                await self._start(name)
            ids = [self.clients[n].me.id for n in want]
            if len(set(ids)) != len(ids):
                raise RuntimeError("Do roles me same account hai - har role ke liye alag alag account chuno.")
        await turbo.close_all()

    def pair(self):
        """(uploader, downloader) clients, or None if not ready."""
        if CFG["mode"] == "single":
            c = self.clients.get(CFG["single"])
            return (c, c) if c else None
        ul, dl = self.clients.get(CFG["ul"]), self.clients.get(CFG["dl"])
        return (ul, dl) if ul and dl else None

    def helpers(self) -> list:
        """Extra download accounts: they only help with files they can read themselves."""
        if CFG["mode"] == "single":
            return []
        return [self.clients[n] for n in CFG["helpers"] if n in self.clients]

    async def stop_all(self):
        for c in list(self.clients.values()):
            try:
                await c.stop()
            except Exception:
                pass
        self.clients.clear()


RT = Runtime()


ACTIVE = 0                 # worker slots in use; at most CFG["parallel"] transfers run at the same time
MAX_PARALLEL = 3


def parallel_limit() -> int:
    try:
        return max(1, min(MAX_PARALLEL, int(CFG.get("parallel", 1))))
    except (TypeError, ValueError):
        return 1


async def worker():
    """Takes jobs from the queue and runs them with the CURRENTLY chosen accounts.
    MAX_PARALLEL of these run; only `parallel` of them may be active, so the limit can change live."""
    global ACTIVE
    while True:
        while ACTIVE >= parallel_limit():
            await asyncio.sleep(0.5)
        ACTIVE += 1                                    # reserve a slot (no await between check and reserve)
        got, task = False, None
        try:
            try:
                job = await asyncio.wait_for(core.jobs.get(), 2)
            except asyncio.TimeoutError:
                continue                               # idle: release the slot and re-check the limit
            got = True
            wait = core.flood_until - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            pair = RT.pair()
            if pair is None:
                job["result"] = "fail"
                if job.get("status"):
                    try:
                        await job["status"].edit_text("❌ Account set nahi hai. /menu se account chuno.")
                    except Exception:
                        pass
                if job.get("batch"):
                    job["batch"].fail += 1
                if job.get("done"):
                    job["done"].set()
                continue
            ul, dl = pair
            job["dl"], job["dl_extra"] = dl, RT.helpers()
            job["transit"] = CFG.get("transit")
            task = asyncio.create_task(core.run_job(ul, job))
            core.current_tasks.add(task)
            await asyncio.wait({task})
        except asyncio.CancelledError:
            if task:
                task.cancel()
            raise
        finally:
            ACTIVE -= 1
            if task:
                core.current_tasks.discard(task)
            if got:
                core.jobs.task_done()


# ---------------------------------------------------------------- login from chat
LOGIN: dict = {}        # owner id -> state of the running login conversation


async def login_abort(uid: int, reason: str = ""):
    st = LOGIN.pop(uid, None)
    if not st:
        return
    c = st.get("client")
    if c is not None:
        try:
            await c.disconnect()
        except Exception:
            pass
    if st.get("name") and not st.get("done"):
        for f in HERE.glob(f"{st['name']}.session*"):
            f.unlink(missing_ok=True)
    if reason:
        log.info("login aborted: %s", reason)


async def login_begin(client, uid: int, chat_id: int):
    await login_abort(uid)
    LOGIN[uid] = {"step": "name", "chat": chat_id, "msgs": [], "client": None, "name": "", "done": False}
    await client.send_message(
        chat_id,
        "➕ Naya account jodna hai.\n\nPehle is account ka ek chhota naam likho (letters/numbers, jaise dl1, up1, raone).\n"
        "Beech me rokne ke liye /cancel.")


def parse_phone(text: str) -> str:
    digits = re.sub(r"\D", "", text)
    return ("+" + digits) if digits else ""


async def login_done(client, message, uid: int):
    st = LOGIN[uid]
    c = st["client"]
    me = await c.get_me()
    data = read_accounts()
    dup = next((n for n, a in data.items() if a.get("id") == me.id), None)
    if dup is not None:
        await login_abort(uid)
        return await message.reply_text(
            f"❌ Ye account ({me.first_name}) pehle se '{dup}' naam se jura hai. Doosre phone number se /addaccount karo.")
    st["done"] = True
    remember(st["name"], me)
    try:
        await c.storage.save()
    except Exception:
        pass
    try:
        await c.disconnect()
    except Exception:
        pass
    try:                                              # remove the code / password messages from the chat
        await client.delete_messages(st["chat"], st["msgs"])
    except Exception:
        pass
    LOGIN.pop(uid, None)
    await message.reply_text(
        f"✅ '{st['name']}' jur gaya: {me.first_name}{' ⭐ Premium' if getattr(me, 'is_premium', False) else ' (Premium nahi)'}\n\n"
        "Ab /menu se isse Download ya Upload account banao.", reply_markup=menu_kb())


async def login_step(client, message):
    uid = message.from_user.id
    st = LOGIN[uid]
    text = (message.text or "").strip()
    st["msgs"].append(message.id)
    step = st["step"]

    if step == "name":
        if not NAME_RE.fullmatch(text):
            return await message.reply_text("Naam sirf letters/numbers/_ (2 se 20 akshar). Dobara likho.")
        if text in session_names() or text == CTRL:
            return await message.reply_text("Ye naam pehle se hai. Koi doosra naam likho.")
        st["name"], st["step"] = text, "phone"
        return await message.reply_text("📱 Us account ka phone number bhejo (country code ke saath, jaise +919876543210).")

    if step == "phone":
        phone = parse_phone(text)
        if len(phone) < 8:
            return await message.reply_text("Phone number sahi nahi lag raha. +91... ke saath dobara bhejo.")
        c = Client(st["name"], api_id=core.API_ID, api_hash=core.API_HASH, workdir=str(HERE))
        st["client"] = c
        try:
            await c.connect()
            sent = await c.send_code(phone)
        except PhoneNumberInvalid:
            await login_abort(uid)
            return await message.reply_text("❌ Ye phone number invalid hai. /addaccount se dobara shuru karo.")
        except FloodWait as e:
            await login_abort(uid)
            return await message.reply_text(f"⏳ Telegram ne {e.value} second ruk ne ko kaha. Baad me /addaccount karo.")
        except Exception as e:
            await login_abort(uid)
            return await message.reply_text(f"❌ Code nahi bhej paaya: {e}")
        st.update(phone=phone, hash=sent.phone_code_hash, step="code")
        return await message.reply_text(
            "📨 Telegram ne us account ke app me code bheja hai (**Telegram** naam ki official chat me).\n\n"
            "⚠️ Code ko **spaces ke saath** bhejo, jaise `1 2 3 4 5`. Seedha `12345` bhejoge to Telegram use "
            "kharab kar deta hai.")

    if step == "code":
        code = re.sub(r"\D", "", text)
        if len(code) < 4:
            return await message.reply_text("Code me kam se kam 4 ank hote hain. `1 2 3 4 5` jaise bhejo.")
        try:
            r = await st["client"].sign_in(st["phone"], st["hash"], code)
        except SessionPasswordNeeded:
            st["step"] = "password"
            hint = ""
            try:
                h = await st["client"].get_password_hint()
                hint = f"\n💡 Hint: {h}" if h else ""
            except Exception:
                pass
            return await message.reply_text("🔐 Is account par Two-Step (2FA) password laga hai. Wo password yahin chat me bhejo; "
                                            "login hote hi ye message chat se hata diya jaayega." + hint)
        except PhoneCodeInvalid:
            return await message.reply_text("❌ Code galat hai. Dobara bhejo (`1 2 3 4 5` jaise).")
        except PhoneCodeExpired:
            await login_abort(uid)
            return await message.reply_text("⌛ Code expire ho gaya. /addaccount se dobara shuru karo, aur code spaces ke saath bhejna.")
        except Exception as e:
            await login_abort(uid)
            return await message.reply_text(f"❌ Login fail: {e}")
        if not hasattr(r, "id"):
            await login_abort(uid)
            return await message.reply_text("❌ Ye number Telegram par registered nahi lagta. Pehle app me account banao.")
        return await login_done(client, message, uid)

    if step == "password":
        try:
            await st["client"].check_password(text)
        except PasswordHashInvalid:
            return await message.reply_text("❌ Password galat hai. Dobara bhejo.")
        except Exception as e:
            await login_abort(uid)
            return await message.reply_text(f"❌ Login fail: {e}")
        return await login_done(client, message, uid)



# ---------------------------------------------------------------- tools: update / restart / logs / settings
async def sh(*args, cwd=None, timeout=120):
    p = await asyncio.create_subprocess_exec(*args, cwd=cwd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    try:
        out, _ = await asyncio.wait_for(p.communicate(), timeout)
    except asyncio.TimeoutError:
        p.kill()
        return 124, "timeout"
    return p.returncode, out.decode(errors="replace")


def restart_soon(delay: float = 2.0):
    """Exit; launcher.py (or systemd) starts the bot again with the new code/settings."""
    asyncio.get_running_loop().call_later(delay, os._exit, 0)


async def import_check(stage: Path):
    """Can the new code at least be imported? (catches broken updates before they go live)"""
    return await sh(sys.executable, "-c", "import tgbot", cwd=str(stage), timeout=60)


def latest_backup():
    dirs = sorted(d for d in BACKUP_DIR.glob("*") if d.is_dir() and not d.name.endswith(".used")) if BACKUP_DIR.exists() else []
    return dirs[-1] if dirs else None


async def do_update(reply, force: bool = False):
    if busy() and not force:
        return await reply("⚠️ Jobs chal rahi hain; update se bot restart hoga aur wo ruk jaayengi.\n"
                           "Pehle 🛑 Cancel karo, ya `/update force` bhejo.")
    work = Path(tempfile.mkdtemp(prefix="upd_"))
    try:
        await reply("⏳ GitHub se naya code la raha hoon...")
        rc, out = await sh("git", "clone", "--depth", "1", GIT_REPO, str(work / "repo"))
        if rc != 0:
            return await reply("❌ GitHub se code nahi aaya:\n" + out[-500:])
        stage = work / "stage"
        stage.mkdir()
        changed, missing = [], []
        for f in CODE_FILES:
            new, old = work / "repo" / f, HERE / f
            if new.exists():
                shutil.copy2(new, stage / f)
                if not old.exists() or not filecmp.cmp(new, old, shallow=False):
                    changed.append(f)
            else:
                missing.append(f)
                if old.exists():
                    shutil.copy2(old, stage / f)
        if not changed:
            return await reply("✅ Koi naya code nahi: sab pehle se latest hai."
                               + (f"\n(GitHub par ye files nahi mili: {', '.join(missing)})" if missing else ""))
        for f in changed:                                               # 1) syntax
            rc, out = await sh(sys.executable, "-m", "py_compile", str(stage / f))
            if rc != 0:
                return await reply(f"❌ {f} me galti hai, update roka (purana code chal raha hai):\n{out[-400:]}")
        rc, out = await import_check(stage)                              # 2) imports
        if rc != 0:
            return await reply("❌ Naya code chal nahi paaya, update roka (purana code chal raha hai):\n" + out[-500:])
        bk = BACKUP_DIR / time.strftime("%Y%m%d_%H%M%S")
        bk.mkdir(parents=True, exist_ok=True)
        for f in changed:
            if (HERE / f).exists():
                shutil.copy2(HERE / f, bk / f)
            shutil.copy2(stage / f, HERE / f)
        await reply(f"✅ Update ho gaya: {', '.join(changed)}\n🔄 Bot 10 second me restart hoga. Phir /menu bhejo.\n"
                    "(Naya code na chale to bot khud purana code wapas laga dega.)")
        restart_soon()
    finally:
        shutil.rmtree(work, ignore_errors=True)


async def do_rollback(reply):
    bk = latest_backup()
    if bk is None:
        return await reply("Koi purana backup nahi mila.")
    for f in bk.glob("*.py"):
        shutil.copy2(f, HERE / f.name)
    bk.rename(bk.with_name(bk.name + ".used"))
    await reply(f"↩️ Purana code wapas laga diya ({bk.name}).\n🔄 Bot restart ho raha hai.")
    restart_soon()


def set_env(updates: dict):
    lines = ENV_FILE.read_text().splitlines() if ENV_FILE.exists() else []
    seen, out = set(), []
    for ln in lines:
        k = ln.split("=", 1)[0].strip()
        if k in updates:
            out.append(f"{k}={updates[k]}")
            seen.add(k)
        else:
            out.append(ln)
    out += [f"{k}={v}" for k, v in updates.items() if k not in seen]
    ENV_FILE.write_text("\n".join(out) + "\n")


def effective() -> dict:
    return {"TURBO_SESSIONS": turbo.SESSIONS, "TURBO_INFLIGHT": turbo.INFLIGHT, "TURBO_ADAPTIVE": int(turbo.ADAPTIVE),
            "TURBO_START": turbo.START, "TURBO_FRESH": int(turbo.FRESH), "TURBO_TIMEOUT": turbo.REQ_TIMEOUT,
            "AUTO_DELAY": core.AUTO_DELAY, "SEND_INTERVAL": core.SEND_INTERVAL}


def current_preset():
    cur = effective()
    return next((n for n, p in PRESETS.items() if all(cur.get(k) == v for k, v in p.items())), None)


def settings_text() -> str:
    cur = effective()
    body = "\n".join(f"• {k} = {cur[k]}   (allowed {lo}-{hi})" for k, (lo, hi) in TUNABLE.items())
    return ("⚙️ Abhi ki settings:\n" + body + "\n\nBadalne ke liye: /set NAAM QIMAT\nExample: /set TURBO_SESSIONS 6\n"
            "(badalne par bot khud restart hota hai)\n\nMatlab: SESSIONS x INFLIGHT = ek saath kitni requests, "
            "ADAPTIVE=1 flood aane par khud slow, AUTO_DELAY = /auto me do links ke beech gap (second).\n"
            f"Ek saath transfers: {CFG['parallel']} (badalne ke liye /parallel 1-3).\nAasan tareeka: /speed")


def speed_text() -> str:
    cur = current_preset()
    return ("🚀 Speed\n\n"
            "🔥 MAX: 8 connection × 3 = 24 requests ek saath, bina auto-brake (ye wahi hai jisme pehle 1 GB ≈ 70-80 second tha)\n"
            "⚡ FAST: 6 × 3 = 18, flood aaye to auto-brake\n"
            "🛡 SAFE: 4 × 2 = 8, flood aaye to auto-brake (sabse dheema, sabse surakshit)\n\n"
            f"Abhi: {cur.upper() if cur else 'custom (/settings dekho)'}\n\n"
            "Dhyan: speed ki asli seema Telegram ka flood hai. MAX par flood aaye to speed gir jaati hai. "
            "🌊 Flood me dekho aur FAST ya SAFE par aa jao.")


def speed_kb():
    cur = current_preset()
    mark = lambda n: "✅ " if cur == n else ""
    return Markup([[Btn(mark("max") + "🔥 MAX", callback_data="preset:max"), Btn(mark("fast") + "⚡ FAST", callback_data="preset:fast"),
                    Btn(mark("safe") + "🛡 SAFE", callback_data="preset:safe")],
                   [Btn("⬅️ Back", callback_data="tools")]])


_LOG_RE = re.compile(r"^\w+\s+\d+\s+(\d\d:\d\d:\d\d)\s+\S+\s+\S+?\[\d+\]:\s+(?:\d{4}-\d\d-\d\d\s+[\d:,]+\s+)?(.*)$")


def short_log_line(line: str) -> str:
    m = _LOG_RE.match(line)
    return f"{m.group(1)} {m.group(2)}"[:230] if m else line[:230]


async def journal(since: str = None, n: int = 600) -> str:
    args = ["journalctl", "-u", SERVICE, "--no-pager", "-n", str(n)]
    if since:
        args += ["--since", since]
    rc, out = await sh(*args, timeout=30)
    return out if rc == 0 else ""


async def logs_text(limit: int = 20) -> str:
    out = await journal()
    keep = [short_log_line(l) for l in out.splitlines()
            if re.search(r"Relay|Send complete|Flood|gave up|Traceback|Error|stale|Turbo settings|Accounts ready|Control bot", l)]
    return "\n".join(keep[-limit:])[-3800:] or "Abhi koi khaas log nahi mila."


async def flood_text() -> str:
    out = await journal("2 hours ago", 4000)
    counts = {}
    for m in re.finditer(r"(Flood(?:Premium)?Wait) \d+s on (upload|download)", out):
        counts[(m.group(2), m.group(1))] = counts.get((m.group(2), m.group(1)), 0) + 1
    last = re.findall(r"on (?:upload|download)\S* -> parallel requests now \d+ \(ceiling \d+\)", out)[-3:]
    done = re.findall(r"Relay done: [^\n]*", out)[-3:]
    if not counts:
        head = "✅ Pichhle 2 ghante me koi flood nahi aaya."
    else:
        head = "⚠️ Pichhle 2 ghante ke flood:\n" + "\n".join(f"• {side}: {kind} × {n}" for (side, kind), n in sorted(counts.items()))
    tail = ("\n\nLimiter ka haal:\n" + "\n".join(last)) if last else ""
    spd = ("\n\nAakhri transfers:\n" + "\n".join(d[:200] for d in done)) if done else ""
    return head + tail + spd


def tools_kb():
    return Markup([[Btn("⬆️ Update (GitHub)", callback_data="update"), Btn("🔄 Restart", callback_data="restart")],
                   [Btn("📜 Logs", callback_data="logs"), Btn("🌊 Flood", callback_data="flood")],
                   [Btn("🚀 Speed", callback_data="speed"), Btn("⚙️ Settings", callback_data="settings")],
                   [Btn("🐞 Last error", callback_data="lasterr"), Btn("↩️ Rollback", callback_data="rollback")],
                   [Btn("⬅️ Back", callback_data="menu")]])


def confirm_kb(yes: str):
    return Markup([[Btn("✅ Haan", callback_data=yes), Btn("❌ Nahi", callback_data="tools")]])


async def update_cmd(client, message):
    await do_update(message.reply_text, force=len(message.command) > 1 and message.command[1] == "force")


async def restart_cmd(client, message):
    await message.reply_text("🔄 Bot restart karna hai? Queue aur chalti files hat jaayengi.", reply_markup=confirm_kb("restart_yes"))


async def rollback_cmd(client, message):
    await message.reply_text("↩️ Pichhla update undo karna hai?", reply_markup=confirm_kb("rollback_yes"))


async def logs_cmd(client, message):
    await message.reply_text(await logs_text(), parse_mode=ParseMode.DISABLED)


async def flood_cmd(client, message):
    await message.reply_text(await flood_text(), parse_mode=ParseMode.DISABLED)


async def settings_cmd(client, message):
    await message.reply_text(settings_text(), parse_mode=ParseMode.DISABLED)


def lasterror_text() -> str:
    if not core.LAST_ERRORS:
        return "✅ Abhi tak koi file fail nahi hui (jab se bot chalu hua)."
    parts = []
    for t, link, short, tb in reversed(core.LAST_ERRORS[-3:]):
        parts.append(f"⏱ {t}  link: {link}\n❌ {short}\n{tb[-600:]}")
    return "\n\n———\n\n".join(parts)[-3800:]


async def lasterror_cmd(client, message):
    await message.reply_text(lasterror_text(), parse_mode=ParseMode.DISABLED)


async def settransit_cmd(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Use: /settransit -1001234567890\n(ek private group/channel jisme dono accounts hon, aur download account post kar sake)")
    if CFG["mode"] == "single":
        return await message.reply_text("Transit sirf split mode me kaam aata hai (single mode me seedha copy hota hai).")
    pair = RT.pair()
    if pair is None:
        return await message.reply_text("Pehle /menu se Download aur Upload account chuno.")
    ul, dl = pair
    value = message.command[1]
    target = int(value) if re.fullmatch(r"-?\d+", value) else value
    chat = None
    for who, c in (("Download", dl), ("Upload", ul)):
        try:
            async for _ in c.get_dialogs(limit=300):      # refresh this account's chat list (new groups are not known yet)
                pass
            chat = await c.get_chat(target)
        except Exception as e:
            return await message.reply_text(f"❌ {who} account ko ye chat nahi mili: {e}\nDono accounts us chat ke member hone chahiye.")
    CFG["transit"] = chat.id
    save_cfg()
    await message.reply_text(f"✅ Transit set: {chat.title or chat.first_name} ({chat.id})\nAb forwarding ON wali files 1-3 second me copy hongi.")


async def cleartransit_cmd(client, message):
    CFG["transit"] = None
    save_cfg()
    await message.reply_text("✅ Transit hata diya.")


async def parallel_cmd(client, message):
    args = message.command[1:]
    if not args or not args[0].isdigit() or not 1 <= int(args[0]) <= MAX_PARALLEL:
        return await message.reply_text(
            f"Ek saath abhi {parallel_limit()} transfer. Badalne ke liye: /parallel 1, 2 ya 3\n"
            "Zyada transfers lagane par har ek ki speed kam ho jaati hai (total request utni hi rehti hai, isliye flood ka risk nahi badhta).")
    CFG["parallel"] = int(args[0])
    save_cfg()
    await message.reply_text(f"✅ Ek saath {CFG['parallel']} transfer. Speed unme bant jaayegi.")


async def speed_cmd(client, message):
    args = message.command[1:]
    if args and args[0].lower() in PRESETS:
        name = args[0].lower()
        set_env(PRESETS[name])
        await message.reply_text(f"✅ Speed: {name.upper()}\n🔄 Bot restart ho raha hai, 10 second me /menu bhejo.")
        return restart_soon()
    await message.reply_text(speed_text(), reply_markup=speed_kb(), parse_mode=ParseMode.DISABLED)


async def set_cmd(client, message):
    args = message.command[1:]
    if len(args) != 2 or args[0].upper() not in TUNABLE:
        return await message.reply_text(settings_text(), parse_mode=ParseMode.DISABLED)
    key = args[0].upper()
    lo, hi = TUNABLE[key]
    try:
        val = float(args[1])
    except ValueError:
        return await message.reply_text("Qimat ek number honi chahiye. Example: /set TURBO_SESSIONS 6")
    if not lo <= val <= hi:
        return await message.reply_text(f"{key} {lo} se {hi} ke beech hona chahiye.")
    set_env({key: int(val) if val == int(val) else val})
    await message.reply_text(f"✅ {key} = {int(val) if val == int(val) else val}\n🔄 Bot restart ho raha hai, 10 second me /menu bhejo.")
    restart_soon()

# ---------------------------------------------------------------- menu

def fast_state() -> str:
    if CFG["mode"] == "single":
        return "ON (forwarding ON wale channel 1-3 second me copy)"
    t = CFG.get("transit")
    return "ON: seedha (download account destination me ho to)" + (f" ya transit {t}" if t else "")


def fast_help() -> str:
    return ("⚡ Instant copy\n\nJis channel me forwarding ON hai (restricted nahi), wahan file download-upload hoti hi nahi: "
            "Telegram apne server par 1 se 3 second me copy kar deta hai, size kuch bhi ho. Restricted channel ho to apne aap "
            "normal tez download-upload chalta hai.\n\n"
            "Single mode: ye apne aap chalta hai.\n\nSplit mode me 2 tareeke:\n"
            "1) Sabse aasan: Download account (A) ko destination channel me admin bana do (post karne ki permission). Phir seedha copy.\n"
            "2) Agar A ko destination me nahi daal sakte: ek private group/channel banao jisme DONO accounts hon (A post kar sake), "
            "phir /settransit -100xxxxxxxxxx bhejo. File A se transit me, wahan se B se destination me copy hogi (transit me kuch jama nahi rehta).\n\n"
            f"Abhi transit: {CFG.get('transit') or 'set nahi'}   (/cleartransit se hatao)")


def menu_text() -> str:
    if CFG["mode"] == "single":
        mode = "Single (ek account se download + upload)"
        acc = f"👤 Account: {label(CFG['single']) if CFG['single'] else '❌ chuna nahi'}"
    else:
        mode = "Split (ek account download, doosra upload)"
        acc = (f"⬇️ Download: {label(CFG['dl']) if CFG['dl'] else '❌ chuna nahi'}\n"
               f"⬆️ Upload: {label(CFG['ul']) if CFG['ul'] else '❌ chuna nahi'}")
    if CFG["mode"] == "split" and CFG["helpers"]:
        acc += "\n🤝 Madadgaar: " + ", ".join(label(n) for n in CFG["helpers"])
    ready = "✅ Taiyaar" if RT.pair() else "⚠️ Account abhi taiyaar nahi (chuno / start nahi hua)"
    dest = CFG["dest"] if CFG["dest"] else "❌ set nahi (/setdest)"
    return (f"🚀 Turbo Bot\n\nMode: {mode}\n{acc}\nStatus: {ready}\n🎯 Destination: {dest}\n"
            f"✍️ Caption: {caption_text()}\n⚡ Instant copy: {fast_state()}\n🔀 Ek saath transfers: {parallel_limit()}"
            f"{' (speed aapas me bant jaati hai)' if parallel_limit() > 1 else ''}\n📊 Queue: {core.jobs.qsize()} job(s)\n\n"
            "Link bhejo, ya /batch, /auto use karo. Update/restart: 🛠 Tools.")


def menu_kb():
    rows = [[Btn("➕ Account jodo", callback_data="addacc"), Btn("👥 Accounts", callback_data="accs")]]
    if CFG["mode"] == "single":
        rows.append([Btn("👤 Account chuno", callback_data="pick:single")])
    else:
        rows.append([Btn("⬇️ Download account", callback_data="pick:dl"), Btn("⬆️ Upload account", callback_data="pick:ul")])
        rows.append([Btn("🤝 Madadgaar download accounts", callback_data="pick:helper")])
    rows.append([Btn("🔁 Mode: " + ("Split → Single" if CFG["mode"] == "split" else "Single → Split"), callback_data="mode")])
    rows.append([Btn("🎯 Destination", callback_data="dest"), Btn("✍️ Caption", callback_data="cap")])
    rows.append([Btn("⚡ Instant copy (forwarding ON wale channel)", callback_data="fast")])
    rows.append([Btn(f"🔀 Ek saath transfers: {parallel_limit()}", callback_data="par")])
    rows.append([Btn("📊 Queue", callback_data="queue"), Btn("🛑 Cancel", callback_data="cancel")])
    rows.append([Btn("🛠 Tools (update / restart / logs)", callback_data="tools")])
    return Markup(rows)


def pick_kb(role: str):
    cur = CFG.get(role, "")
    rows = [[Btn(("✅ " if n == cur else "") + label(n), callback_data=f"set:{role}:{n}")] for n in session_names()]
    rows.append([Btn("➕ Naya account jodo", callback_data="addacc")])
    rows.append([Btn("⬅️ Back", callback_data="menu")])
    return Markup(rows)


def helper_kb():
    rows = [[Btn(("✅ " if n in CFG["helpers"] else "") + label(n), callback_data=f"help:{n}")]
            for n in session_names() if n not in (CFG["dl"], CFG["ul"])]
    rows.append([Btn("⬅️ Back", callback_data="menu")])
    return Markup(rows)


def back_kb():
    return Markup([[Btn("⬅️ Back", callback_data="menu")]])


ROLE_NAME = {"dl": "⬇️ Download account chuno", "ul": "⬆️ Upload account chuno", "single": "👤 Account chuno"}


async def change_setup(cq, mutate):
    """Apply a change to the chosen accounts; undo it if the accounts can't start."""
    if busy():
        return await cq.answer("Jobs chal rahi hain. Pehle /cancel karo ya khatam hone do.", show_alert=True)
    old = copy.deepcopy(CFG)
    mutate()
    await cq.answer("Account start ho raha hai...")
    try:
        await RT.apply()
    except Exception as e:
        CFG.clear()
        CFG.update(old)
        try:
            await RT.apply()
        except Exception:
            pass
        return await cq.message.edit_text(f"❌ {e}\n\n" + menu_text(), reply_markup=menu_kb())
    save_cfg()
    await cq.message.edit_text(menu_text(), reply_markup=menu_kb())


async def on_callback(client, cq):
    if cq.from_user.id not in core.OWNERS:
        return await cq.answer("Allowed nahi", show_alert=True)
    d = cq.data or ""
    msg = cq.message
    if d == "menu":
        await cq.answer()
        return await msg.edit_text(menu_text(), reply_markup=menu_kb())
    if d == "addacc":
        await cq.answer()
        return await login_begin(client, cq.from_user.id, msg.chat.id)
    if d == "accs":
        await cq.answer()
        names = session_names()
        body = "\n".join("• " + label(n) + ("  ← Download" if n == CFG["dl"] and CFG["mode"] == "split" else
                                           "  ← Upload" if n == CFG["ul"] and CFG["mode"] == "split" else
                                           "  ← Account" if n == CFG["single"] and CFG["mode"] == "single" else "")
                         for n in names) or "Koi account nahi. ➕ se jodo."
        return await msg.edit_text(f"👥 Saved accounts:\n{body}\n\nHataane ke liye: /removeaccount <naam>", reply_markup=back_kb())
    if d == "pick:helper":
        await cq.answer()
        return await msg.edit_text("🤝 Madadgaar download accounts (optional)\n\nYe accounts sirf un files me madad karte hain jo wo khud padh sakte "
                                   "hain (same channel ke member ho). Isse download ki capacity badhti hai. Jo file wo nahi padh sakte, wahan skip.",
                                   reply_markup=helper_kb())
    if d.startswith("help:"):
        name = d[5:]
        def toggle():
            CFG["helpers"] = [n for n in CFG["helpers"] if n != name] if name in CFG["helpers"] else CFG["helpers"] + [name]
        return await change_setup(cq, toggle)
    if d == "speed":
        await cq.answer()
        return await msg.edit_text(speed_text(), reply_markup=speed_kb())
    if d.startswith("preset:"):
        name = d.split(":", 1)[1]
        if name not in PRESETS:
            return await cq.answer()
        set_env(PRESETS[name])
        await cq.answer("Speed badal rahi hai...")
        await msg.edit_text(f"✅ Speed: {name.upper()}\n🔄 Bot restart ho raha hai, 10 second me /menu bhejo.")
        return restart_soon()
    if d.startswith("pick:"):
        await cq.answer()
        role = d.split(":", 1)[1]
        return await msg.edit_text(ROLE_NAME.get(role, "Account chuno") + ":", reply_markup=pick_kb(role))
    if d.startswith("set:"):
        _, role, name = d.split(":", 2)
        return await change_setup(cq, lambda: CFG.__setitem__(role, name))
    if d == "mode":
        return await change_setup(cq, lambda: CFG.__setitem__("mode", "single" if CFG["mode"] == "split" else "split"))
    if d == "dest":
        await cq.answer()
        return await msg.edit_text(
            f"🎯 Destination abhi: {CFG['dest'] or 'set nahi'}\n\nBadalne ke liye bhejo:\n/setdest -1001234567890\n"
            "(ya /setdest @channelname). Upload wala account us channel me admin hona chahiye.", reply_markup=back_kb())
    if d == "fast":
        await cq.answer()
        return await msg.edit_text(fast_help(), reply_markup=back_kb())
    if d == "cap":
        await cq.answer()
        return await msg.edit_text(caption_help(), reply_markup=back_kb())
    if d == "par":
        CFG["parallel"] = parallel_limit() % MAX_PARALLEL + 1
        save_cfg()
        await cq.answer(f"Ek saath {CFG['parallel']} transfer")
        return await msg.edit_text(menu_text(), reply_markup=menu_kb())
    if d == "tools":
        await cq.answer()
        return await msg.edit_text("🛠 Tools\n\n⬆️ Update: GitHub se naya code lekar bot restart\n🔄 Restart\n📜 Logs / 🌊 Flood: bot ka haal\n"
                                   "⚙️ Settings: speed settings badlo\n↩️ Rollback: pichhla update undo", reply_markup=tools_kb())
    if d == "update":
        await cq.answer()
        return await do_update(msg.reply_text)
    if d == "restart":
        await cq.answer()
        return await msg.edit_text("🔄 Bot restart karna hai? Queue aur chalti files hat jaayengi.", reply_markup=confirm_kb("restart_yes"))
    if d == "restart_yes":
        await cq.answer("Restart ho raha hai...")
        await msg.edit_text("🔄 Bot restart ho raha hai, 10 second me /menu bhejo.")
        return restart_soon()
    if d == "rollback":
        await cq.answer()
        return await msg.edit_text("↩️ Pichhla update undo karna hai?", reply_markup=confirm_kb("rollback_yes"))
    if d == "rollback_yes":
        await cq.answer()
        return await do_rollback(msg.reply_text)
    if d == "lasterr":
        await cq.answer()
        return await msg.reply_text(lasterror_text(), parse_mode=ParseMode.DISABLED)
    if d == "logs":
        await cq.answer()
        return await msg.reply_text(await logs_text(), parse_mode=ParseMode.DISABLED)
    if d == "flood":
        await cq.answer()
        return await msg.reply_text(await flood_text(), parse_mode=ParseMode.DISABLED)
    if d == "settings":
        await cq.answer()
        return await msg.reply_text(settings_text(), parse_mode=ParseMode.DISABLED)
    if d == "queue":
        await cq.answer()
        return await core.queue_cmd(client, msg)
    if d == "cancel":
        await cq.answer()
        return await cancel_cmd(client, msg, uid=cq.from_user.id)
    await cq.answer()


def caption_help() -> str:
    sample, _, _ = core.rewrite_caption("Title\nExtracted By : Koi Aur", [], None)
    return ("✍️ Caption\n\n'Extracted by / Downloaded by / Powered by' ke baad ka sab kuch badal kar ye aata hai:\n"
            f"{caption_text()}\n\nKahin bhi koi @username ho to wo bhi {CFG['tag']} ban jaata hai.\n\nExample:\n{sample}\n\n"
            "Badalne ke liye:\n/setusername @naya\n/setbackup <link> [naam]   (jaise /setbackup https://t.me/xyz Backup)\n/clearbackup")


# ---------------------------------------------------------------- commands
async def menu_cmd(client, message):
    await message.reply_text(menu_text(), reply_markup=menu_kb())


async def addaccount_cmd(client, message):
    await login_begin(client, message.from_user.id, message.chat.id)


async def accounts_cmd(client, message):
    names = session_names()
    body = "\n".join("• " + label(n) for n in names) or "Koi account nahi. /addaccount se jodo."
    await message.reply_text(f"👥 Saved accounts:\n{body}")


async def removeaccount_cmd(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Use: /removeaccount <naam>")
    name = message.command[1]
    if name not in session_names():
        return await message.reply_text("Is naam ka account nahi mila. /accounts dekho.")
    if name in (CFG["dl"], CFG["ul"], CFG["single"]) or name in RT.clients:
        return await message.reply_text("Ye account abhi use me hai. Pehle /menu se doosra account chuno.")
    for f in HERE.glob(f"{name}.session*"):
        f.unlink(missing_ok=True)
    data = read_accounts()
    data.pop(name, None)
    write_accounts(data)
    await message.reply_text(f"🗑 '{name}' hata diya. (Telegram ke Settings → Devices me VPS ka session chahe to wahan se bhi hata sakte ho.)")


async def setdest_cmd(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Use: /setdest -1001234567890  ya  /setdest @mychannel")
    pair = RT.pair()
    if pair is None:
        return await message.reply_text("Pehle /menu se account chuno (upload wala account destination dekh kar check hota hai).")
    value = message.command[1]
    try:
        chat = await pair[0].get_chat(int(value) if re.fullmatch(r"-?\d+", value) else value)
    except Exception as e:
        return await message.reply_text(f"❌ Upload account ko ye chat nahi mili: {e}")
    CFG["dest"] = chat.id
    save_cfg()
    await message.reply_text(f"✅ Destination set: {chat.title or chat.first_name} ({chat.id})")


async def mydest_cmd(client, message):
    await message.reply_text(f"🎯 Destination: {CFG['dest']}" if CFG["dest"] else "Destination set nahi hai: /setdest -100xxxxxxxxxx")


async def cleardest_cmd(client, message):
    CFG["dest"] = None
    save_cfg()
    await message.reply_text("✅ Destination clear.")


async def setusername_cmd(client, message):
    if len(message.command) < 2 or not TAG_RE.fullmatch(message.command[1]):
        return await message.reply_text("Use: /setusername @tumhara_username")
    tag = message.command[1]
    CFG["tag"] = tag if tag.startswith("@") else "@" + tag
    save_cfg()
    sync_caption()
    await message.reply_text(f"✅ Username: {CFG['tag']}\nCaption ab aisa banega: {caption_text()}")


async def setbackup_cmd(client, message):
    args = message.command[1:]
    if not args or not re.match(r"(https?://|t\.me/|telegram\.me/)", args[0], re.I):
        return await message.reply_text("Use: /setbackup <link> [naam]\nExample: /setbackup https://t.me/mybackup Backup")
    CFG["backup"] = args[0]
    CFG["label"] = " ".join(args[1:]).strip() or "Backup"
    save_cfg()
    sync_caption()
    await message.reply_text(f"✅ Backup link set.\nCaption ab aisa banega: {caption_text()}")


async def clearbackup_cmd(client, message):
    CFG["backup"] = ""
    save_cfg()
    sync_caption()
    await message.reply_text(f"✅ Backup link hata diya.\nCaption: {caption_text()}")


async def caption_cmd(client, message):
    await message.reply_text(caption_help())


async def cancel_cmd(client, message, uid=None):
    uid = uid or (message.from_user.id if message.from_user else None)
    if uid in LOGIN:                                   # /cancel during login only stops the login
        await login_abort(uid, "cancelled by user")
        return await message.reply_text("🛑 Account jodna roka.")
    await core.cancel_cmd(client, message)


async def text_router(client, message):
    if message.from_user.id in LOGIN:
        return await login_step(client, message)
    await core.link_handler(client, message)


async def other_media(client, message):
    await message.reply_text("File forward se kaam nahi hota. Source message ka **link** bhejo.")


# ---------------------------------------------------------------- main
async def main():
    if not core.BOT_TOKEN or not core.OWNERS:
        sys.exit("BOT_TOKEN aur OWNER_IDS .env me chahiye (BotFather se token, @userinfobot se apna ID).")
    core.WORKDIR.mkdir(parents=True, exist_ok=True)
    turbo.install()
    sync_caption()

    ctrl = Client(CTRL, api_id=core.API_ID, api_hash=core.API_HASH, bot_token=core.BOT_TOKEN,
                  workdir=str(HERE), sleep_threshold=60)
    owner = filters.user(core.OWNERS)
    private = filters.private & owner
    add = ctrl.add_handler
    for names, fn in ((["start", "menu", "help"], menu_cmd), (["addaccount"], addaccount_cmd),
                      (["accounts"], accounts_cmd), (["removeaccount"], removeaccount_cmd),
                      (["setdest"], setdest_cmd), (["mydest"], mydest_cmd), (["cleardest"], cleardest_cmd),
                      (["setusername"], setusername_cmd), (["setbackup"], setbackup_cmd),
                      (["clearbackup"], clearbackup_cmd), (["caption"], caption_cmd),
                      (["queue"], core.queue_cmd), (["cancel"], cancel_cmd),
                      (["batch"], core.batch_cmd), (["auto"], core.auto_cmd),
                      (["update"], update_cmd), (["restart"], restart_cmd), (["rollback"], rollback_cmd),
                      (["logs"], logs_cmd), (["flood"], flood_cmd), (["settings"], settings_cmd), (["set"], set_cmd),
                      (["speed"], speed_cmd), (["parallel"], parallel_cmd),
                      (["settransit"], settransit_cmd), (["cleartransit"], cleartransit_cmd),
                      (["lasterror"], lasterror_cmd)):
        add(MessageHandler(fn, private & filters.command(names)))
    add(MessageHandler(text_router, private & filters.text & ~filters.command(COMMANDS)))
    add(MessageHandler(other_media, private & ~filters.text))
    add(CallbackQueryHandler(on_callback, owner))
    add(MessageHandler(core.deny_handler, filters.private & ~owner))

    await ctrl.start()
    log.info("Control bot started: @%s", ctrl.me.username)
    try:
        await RT.apply()
        log.info("Accounts ready: %s", ", ".join(RT.clients) or "none yet")
    except Exception as e:
        log.warning("Accounts not started: %s", e)
    runners = [asyncio.create_task(worker()) for _ in range(MAX_PARALLEL)]
    for uid in core.OWNERS:
        try:
            await ctrl.send_message(uid, "🚀 Bot chalu ho gaya.\n\n" + menu_text(), reply_markup=menu_kb())
        except Exception:
            pass
    try:
        await idle()
    finally:
        for r in runners:
            r.cancel()
        await turbo.close_all()
        await RT.stop_all()
        await ctrl.stop()


if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass
    asyncio.run(main())
