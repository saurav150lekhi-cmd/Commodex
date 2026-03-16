#!/bin/bash
# Commodex EC2 setup script
# Run once on a fresh Ubuntu 22.04 EC2 instance as root:
#   sudo bash setup.sh

set -e

echo "==> Updating system..."
apt-get update -y && apt-get upgrade -y

echo "==> Installing dependencies..."
apt-get install -y python3 python3-pip python3-venv nginx git

echo "==> Cloning repo..."
cd /opt
git clone https://github.com/saurav150lekhi-cmd/Commodex commodex
cd commodex

echo "==> Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo "==> Setting up .env..."
if [ ! -f .env ]; then
    cp .env.example .env
    echo ""
    echo "*** Edit /opt/commodex/.env and add your API keys, then run:"
    echo "    sudo systemctl start commodex"
    echo ""
fi

echo "==> Installing systemd services..."
cp deploy/commodex.service /etc/systemd/system/commodex.service
cp deploy/commodex-worker.service /etc/systemd/system/commodex-worker.service
systemctl daemon-reload
systemctl enable commodex
systemctl enable commodex-worker

echo "==> Installing nginx config..."
cp deploy/nginx.conf /etc/nginx/sites-available/commodex
ln -sf /etc/nginx/sites-available/commodex /etc/nginx/sites-enabled/commodex
rm -f /etc/nginx/sites-enabled/default
nginx -t && systemctl restart nginx

echo ""
echo "==> Done. Next steps:"
echo "    1. Edit /opt/commodex/.env with your API keys"
echo "    2. sudo systemctl start commodex commodex-worker"
echo "    3. sudo systemctl status commodex commodex-worker"
echo ""
