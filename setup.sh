#!/usr/bin/env bash
# One-command installer: bash setup.sh
set -e
cd "$(dirname "$(readlink -f "$0")")"
DIR="$PWD"

apt-get update -y
apt-get install -y python3-venv python3-pip
python3 -m venv venv
./venv/bin/pip install -q -r requirements.txt

if [ ! -f .env ]; then
  read -rp "API_ID (my.telegram.org se): " A
  read -rp "API_HASH: " H
  cat > .env <<EOF
API_ID=$A
API_HASH=$H
MODE=user
WORKDIR=$DIR/work
TURBO_SESSIONS=8
TURBO_INFLIGHT=3
JOB_WORKERS=2
EOF
fi

echo
echo "=== Telegram login: phone number (+91...), phir code, phir 2FA password (agar hai) ==="
./venv/bin/python bot.py --login
chmod 600 .env *.session

cat > /etc/systemd/system/turbo-bot.service <<EOF
[Unit]
Description=Turbo Forward Bot
After=network-online.target

[Service]
WorkingDirectory=$DIR
ExecStart=$DIR/venv/bin/python bot.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now turbo-bot
echo
echo "Done. Bot chal raha hai. Logs: journalctl -u turbo-bot -f"
echo "Ab Telegram ke Saved Messages me /setdest -100xxxxxxxxxx bhejo."
