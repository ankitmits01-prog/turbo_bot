"""
SPLIT mode - two accounts share the work:

  Account A  (member of the SOURCE channel)      -> DOWNLOADS the file chunks
  Account B  (admin of the DESTINATION, Premium) -> UPLOADS the chunks and posts the file

Both run at the same time and every chunk is handed over directly (no disk), so Telegram's
per-account limits are used twice. Flood waits on A and B are handled separately.

Control: send links / commands in the Saved Messages of account A.
  /setdest <chat_id|@username>   (resolved with account B, so B must be in that chat)
  /auto <link> <N> [delay]   /batch <l1> <l2>   /cancel   /queue   /mydest

Note: server-side "instant copy" is not possible between two accounts, so every file is
relayed (A -> B). Use the normal single-account mode when copy works for you.
"""
import asyncio
import logging
import re
import sys

from pyrogram import Client, filters, idle
from pyrogram.handlers import MessageHandler

import sp_core as core      # re-uses job queue, anti-flood, /cancel, /auto, caption rewrite
import sp_turbo as turbo

core.USER_MODE = True          # require /setdest (never post back into Saved Messages)
log = logging.getLogger("split")

COMMANDS = ["start", "setdest", "mydest", "cleardest", "queue", "batch", "cancel", "auto"]


async def start_cmd(client, message):
    await message.reply_text(
        "🚀 Turbo Split Mode (A downloads, B uploads)\n\n"
        "• /setdest <chat_id ya @username> : destination (account B wahan admin ho)\n"
        "• Message link bhejo, ya /batch <first> <last>\n"
        "• /auto <link> <N> [delay] : link + uske aage ke N links, ek ek karke\n"
        "• /mydest, /cleardest, /queue, /cancel"
    )


def make_setdest(ul: Client):
    async def setdest_cmd(client, message):
        if len(message.command) < 2:
            return await message.reply_text("Use: /setdest -1001234567890  ya  /setdest @mychannel")
        value = message.command[1]
        try:
            chat = await ul.get_chat(int(value) if re.fullmatch(r"-?\d+", value) else value)
        except Exception as e:
            return await message.reply_text(f"❌ Account B ko ye chat nahi mili: {e}")
        data = core.load_settings()
        data[str(message.from_user.id)] = chat.id
        core.save_settings(data)
        await message.reply_text(f"✅ Destination set: {chat.title or chat.first_name} ({chat.id})")

    return setdest_cmd


async def main():
    core.WORKDIR.mkdir(parents=True, exist_ok=True)
    common = dict(api_id=core.API_ID, api_hash=core.API_HASH, sleep_threshold=60, max_concurrent_transmissions=4)
    dl = Client("turbo_dl", **common)      # account A
    ul = Client("turbo_ul", **common)      # account B
    turbo.install()

    private = filters.chat("me")           # Saved Messages of account A
    media = (filters.document | filters.video | filters.audio | filters.animation
             | filters.voice | filters.video_note | filters.photo)
    dl.add_handler(MessageHandler(start_cmd, private & filters.command("start")))
    dl.add_handler(MessageHandler(make_setdest(ul), private & filters.command("setdest")))
    dl.add_handler(MessageHandler(core.mydest_cmd, private & filters.command("mydest")))
    dl.add_handler(MessageHandler(core.cleardest_cmd, private & filters.command("cleardest")))
    dl.add_handler(MessageHandler(core.queue_cmd, private & filters.command("queue")))
    dl.add_handler(MessageHandler(core.cancel_cmd, private & filters.command("cancel")))
    dl.add_handler(MessageHandler(core.batch_cmd, private & filters.command("batch")))
    dl.add_handler(MessageHandler(core.auto_cmd, private & filters.command("auto")))
    dl.add_handler(MessageHandler(core.link_handler, private & filters.text & ~filters.command(COMMANDS)))
    dl.add_handler(MessageHandler(core.media_handler, private & media))

    print("\n=== LOGIN 1/2: ACCOUNT A  (jo SOURCE channel ka member hai, DOWNLOAD karega) ===")
    await dl.start()
    print("\n=== LOGIN 2/2: ACCOUNT B  (jo DESTINATION ka admin hai + Premium, UPLOAD karega) ===")
    await ul.start()
    log.info("A (download): %s | B (upload): %s | B premium: %s",
             dl.me.first_name, ul.me.first_name, getattr(ul.me, "is_premium", False))
    if dl.me.id == ul.me.id:
        log.warning("A aur B same account hain! Alag alag accounts se login karo.")

    if "--login" in sys.argv:
        log.info("Both logins saved.")
        await ul.stop()
        await dl.stop()
        return

    for c in (dl, ul):                     # make each account's chats known to its session
        try:
            async for _ in c.get_dialogs():
                pass
        except Exception as e:
            log.warning("get_dialogs failed: %s", e)

    workers = [asyncio.create_task(core.job_worker(ul, dl)) for _ in range(core.JOB_WORKERS)]
    try:
        await idle()
    finally:
        for w in workers:
            w.cancel()
        await turbo.close_all()
        await ul.stop()
        await dl.stop()


if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass
    asyncio.run(main())
