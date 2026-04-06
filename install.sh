#!/bin/bash
set -e

PROJ=/home/plenox/workspace/train_tracker

echo "==> Installing Python dependencies..."
"$PROJ/venv/bin/pip" install flask --quiet

echo "==> Installing systemd services..."
cp "$PROJ/systemd/train-display.service" /etc/systemd/system/
cp "$PROJ/systemd/train-manager.service" /etc/systemd/system/
systemctl daemon-reload

# Manager starts display — disable direct enable of display service
systemctl disable train-display.service 2>/dev/null || true
systemctl enable train-manager.service

echo "==> Starting train-manager.service..."
systemctl start train-manager.service

echo ""
echo "Done. Check status with:"
echo "  systemctl status train-manager.service"
echo "  systemctl status train-display.service"
echo "  journalctl -u train-manager -f"
