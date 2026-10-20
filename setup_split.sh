#!/usr/bin/env bash
# Switch this folder to SPLIT mode (2 accounts): bash setup_split.sh
set -e
cd "$(dirname "$(readlink -f "$0")")"
DIR="$PWD"

systemctl stop turbo-bot 2>/dev/null || true
systemctl disable turbo-bot 2>/dev/null || true       # never run two modes on the same accounts

echo
./venv/bin/python split.py --login
chmod 600 .env *.session

cat > /etc/systemd/system/turbo-split.service <<EOF
[Unit]
Description=Turbo Split Bot (2 accounts)
After=network-online.target

[Service]
WorkingDirectory=$DIR
ExecStart=$DIR/venv/bin/python split.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now turbo-split
echo
echo "Done. Logs: journalctl -u turbo-split -f"
echo "Account A ke Saved Messages me: /setdest -100xxxxxxxxxx  phir link ya /auto bhejo."
