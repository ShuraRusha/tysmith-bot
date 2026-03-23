#!/bin/bash
# One-time setup script — run this on the server as root
set -e

echo "=== Installing dependencies ==="
apt-get update -q
apt-get install -y python3 python3-pip git

echo "=== Cloning repository ==="
cd /root
if [ -d "tysmith-bot" ]; then
  echo "Directory already exists, pulling latest..."
  cd tysmith-bot && git pull origin main
else
  git clone https://github.com/ShuraRusha/tysmith-bot.git
  cd tysmith-bot
fi

echo "=== Installing Python packages ==="
pip install -r requirements.txt

echo "=== Creating systemd service ==="
cat > /etc/systemd/system/tysmith-bot.service << 'EOF'
[Unit]
Description=TySmith Telegram Bot
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/tysmith-bot
ExecStart=/usr/bin/python3 /root/tysmith-bot/bot.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

echo "=== Enabling and starting service ==="
systemctl daemon-reload
systemctl enable tysmith-bot
systemctl start tysmith-bot

echo ""
echo "Done! Bot status:"
systemctl status tysmith-bot --no-pager
