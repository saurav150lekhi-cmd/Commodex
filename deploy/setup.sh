#!/bin/bash
# Commodex Vultr setup script
# Run once on a fresh Ubuntu 22.04 VPS as root:
#   bash setup.sh
#
# After running:
#   1. Edit /opt/commodex/.env with your API keys
#   2. sudo systemctl start commodex commodex-worker
#   3. (Optional) sudo certbot --nginx -d YOUR_DOMAIN  ← for HTTPS

set -e

echo ""
echo "=========================================================="
echo "  Commodex — Vultr VPS Setup"
echo "=========================================================="
echo ""

# ── System packages ────────────────────────────────────────────────────────────
echo "==> Updating system..."
apt-get update -y && apt-get upgrade -y

echo "==> Installing dependencies..."
apt-get install -y python3 python3-pip python3-venv nginx git \
    certbot python3-certbot-nginx \
    postgresql postgresql-contrib \
    libpq-dev python3-dev build-essential ufw

# ── PostgreSQL ─────────────────────────────────────────────────────────────────
echo "==> Setting up PostgreSQL..."
systemctl start postgresql
systemctl enable postgresql

# Create DB and user
sudo -u postgres psql -c "CREATE USER commodex WITH PASSWORD 'commodex_pass';" 2>/dev/null || echo "  User already exists."
sudo -u postgres psql -c "CREATE DATABASE commodex OWNER commodex;" 2>/dev/null || echo "  Database already exists."
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE commodex TO commodex;" 2>/dev/null

echo "  PostgreSQL ready: postgresql://commodex:commodex_pass@localhost:5432/commodex"
echo "  *** Change the password in .env before going live ***"

# ── Firewall ───────────────────────────────────────────────────────────────────
echo "==> Configuring firewall..."
ufw allow 22/tcp   # SSH
ufw allow 80/tcp   # HTTP
ufw allow 443/tcp  # HTTPS
ufw --force enable

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
    echo "*** .env created from template. Edit it now: ***"
    echo "    nano /opt/commodex/.env"
    echo ""
fi

# ── Logs ───────────────────────────────────────────────────────────────────────
touch /var/log/commodex-access.log /var/log/commodex-error.log

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
mkdir -p /var/www/certbot
nginx -t && systemctl restart nginx

# ── Summary ────────────────────────────────────────────────────────────────────
echo ""
echo "=========================================================="
echo "  Setup complete!"
echo "=========================================================="
echo ""
echo "  NEXT STEPS:"
echo ""
echo "  1. Edit your environment variables:"
echo "       nano /opt/commodex/.env"
echo ""
echo "     Required:"
echo "       ANTHROPIC_API_KEY=sk-ant-..."
echo "       JWT_SECRET_KEY=<random 32+ char string>"
echo "       DATABASE_URL=postgresql://commodex:commodex_pass@localhost:5432/commodex"
echo "       APP_URL=http://139.84.223.215   (or your domain)"
echo ""
echo "     For email alerts (SMTP):"
echo "       SMTP_HOST=smtp.gmail.com"
echo "       SMTP_PORT=587"
echo "       SMTP_USER=your@gmail.com"
echo "       SMTP_PASS=your_app_password"
echo "       FROM_EMAIL=your@gmail.com"
echo ""
echo "     Optional:"
echo "       EIA_API_KEY=<from eia.gov>"
echo "       FRED_API_KEY=<from fred.stlouisfed.org>"
echo ""
echo "  2. Start services:"
echo "       systemctl start commodex commodex-worker"
echo "       systemctl status commodex commodex-worker"
echo ""
echo "  3. Promote yourself to admin:"
echo "       First register at http://139.84.223.215/app"
echo "       Then: curl -X POST http://localhost:5000/admin/api/setup \\"
echo "               -H 'Content-Type: application/json' \\"
echo "               -d '{\"email\":\"your@email.com\"}'"
echo ""
echo "  4. (Optional) HTTPS with Let's Encrypt:"
echo "       certbot --nginx -d YOUR_DOMAIN"
echo ""
echo "  5. Watch logs:"
echo "       journalctl -u commodex -f"
echo "       journalctl -u commodex-worker -f"
echo ""
