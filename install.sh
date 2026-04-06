#!/bin/bash
set -e

PROJ=/home/plenox/workspace/train_tracker

echo "==> Installing train-display.service..."
sudo cp "$PROJ/systemd/train-display.service" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable train-display.service

echo "==> Starting train-display.service..."
sudo systemctl start train-display.service

echo ""
echo "Done. Check status with:"
echo "  sudo systemctl status train-display.service"
echo "  journalctl -u train-display -f"
