#!/usr/bin/env bash
# One-time installer for the control bot:  bash setup_tgbot.sh
set -e
cd "$(dirname "$(readlink -f "$0")")"
DIR="$PWD"

# never let the old modes run on the same accounts
systemctl stop turbo-bot turbo-split 2>/dev/null || true
systemctl disable turbo-bot turbo-split 2>/dev/null || true

[ -f .env ] || { echo ".env nahi mila (API_ID aur API_HASH wali file). Pehle purana setup chalao."; exit 1; }
getenv() { grep -E "^$1=" .env | tail -1 | cut -d= -f2-; }
setenv() { if grep -q "^$1=" .env; then sed -i "s|^$1=.*|$1=$2|" .env; else echo "$1=$2" >> .env; fi; }
[ -n "$(getenv API_ID)" ] && [ -n "$(getenv API_HASH)" ] || { echo ".env me API_ID / API_HASH nahi hain."; exit 1; }
if [ -z "$(getenv BOT_TOKEN)" ]; then read -rp "BOT_TOKEN (BotFather se, jaise 123456:ABC...): " T; setenv BOT_TOKEN "$T"; fi
if [ -z "$(getenv OWNER_IDS)" ]; then read -rp "OWNER_IDS (tumhara Telegram user id, @userinfobot se): " O; setenv OWNER_IDS "$O"; fi
chmod 600 .env *.session config.json accounts.json 2>/dev/null || true

cat > /etc/systemd/system/turbo-tgbot.service <<EOF
[Unit]
Description=Turbo Control Bot
After=network-online.target

[Service]
WorkingDirectory=$DIR
ExecStart=$DIR/venv/bin/python launcher.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now turbo-tgbot
echo
echo "Done. Ab Telegram me apne bot ko /start bhejo."
echo "Logs: journalctl -u turbo-tgbot -f"
