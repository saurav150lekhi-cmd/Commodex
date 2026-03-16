#!/bin/bash
# Commodex EC2 setup script
# Run once on a fresh Ubuntu 22.04 EC2 instance:
#   sudo bash setup.sh
#
# After running:
#   1. Edit /opt/commodex/.env with your API keys + DB URL
#   2. sudo systemctl start commodex commodex-worker
#   3. (Optional) sudo certbot --nginx -d YOUR_DOMAIN  ← for HTTPS

set -e

# ── System packages ────────────────────────────────────────────────────────────
echo "==> Updating system..."
apt-get update -y && apt-get upgrade -y

echo "==> Installing dependencies..."
apt-get install -y python3 python3-pip python3-venv nginx git \
    certbot python3-certbot-nginx \
    libpq-dev python3-dev build-essential

# ── App ────────────────────────────────────────────────────────────────────────
echo "==> Cloning repo..."
cd /opt
if [ -d commodex ]; then
    echo "  /opt/commodex already exists — pulling latest..."
    cd commodex && git pull
else
    git clone https://github.com/saurav150lekhi-cmd/Commodex commodex
    cd commodex
fi

echo "==> Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# ── .env ───────────────────────────────────────────────────────────────────────
if [ ! -f .env ]; then
    cp .env.example .env
    echo ""
    echo "*** IMPORTANT: Edit /opt/commodex/.env before starting services."
    echo ""
    echo "    Required keys:"
    echo "      ANTHROPIC_API_KEY=sk-ant-..."
    echo "      JWT_SECRET_KEY=<random 32+ char string>"
    echo ""
    echo "    For PostgreSQL on RDS (recommended):"
    echo "      DATABASE_URL=postgresql://user:password@YOUR-RDS-ENDPOINT:5432/commodex"
    echo ""
    echo "    For local SQLite (dev/testing only):"
    echo "      DATABASE_URL=sqlite:///commodex.db"
    echo ""
fi

# ── systemd ────────────────────────────────────────────────────────────────────
echo "==> Installing systemd services..."
cp deploy/commodex.service        /etc/systemd/system/commodex.service
cp deploy/commodex-worker.service /etc/systemd/system/commodex-worker.service
systemctl daemon-reload
systemctl enable commodex
systemctl enable commodex-worker

# ── nginx ──────────────────────────────────────────────────────────────────────
echo "==> Installing nginx config..."
cp deploy/nginx.conf /etc/nginx/sites-available/commodex
ln -sf /etc/nginx/sites-available/commodex /etc/nginx/sites-enabled/commodex
rm -f /etc/nginx/sites-enabled/default
nginx -t && systemctl restart nginx

# ACME challenge dir for certbot
mkdir -p /var/www/certbot

# ── Summary ────────────────────────────────────────────────────────────────────
echo ""
echo "=========================================================="
echo "  Setup complete!"
echo "=========================================================="
echo ""
echo "  Next steps:"
echo ""
echo "  1. Edit /opt/commodex/.env"
echo "       ANTHROPIC_API_KEY=sk-ant-..."
echo "       JWT_SECRET_KEY=<random 32+ char string>"
echo "       DATABASE_URL=postgresql://user:pass@host:5432/commodex"
echo "       EIA_API_KEY=<optional>"
echo ""
echo "  2. Start services:"
echo "       sudo systemctl start commodex commodex-worker"
echo "       sudo systemctl status commodex commodex-worker"
echo ""
echo "  3. (Optional) HTTPS with Let's Encrypt:"
echo "       sudo certbot --nginx -d YOUR_DOMAIN"
echo "       Then uncomment the HTTPS block in /etc/nginx/sites-available/commodex"
echo ""
echo "  4. AWS Security Group — open inbound ports:"
echo "       22   (SSH)"
echo "       80   (HTTP)"
echo "       443  (HTTPS)"
echo ""
echo "  5. If using RDS — open port 5432 in the RDS security group"
echo "     and allow inbound from the EC2 instance's security group."
echo ""
echo "  Logs:"
echo "       sudo journalctl -u commodex -f"
echo "       sudo journalctl -u commodex-worker -f"
echo ""
