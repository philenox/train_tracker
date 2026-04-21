#!/bin/bash
set -e

PROJ=/home/plenox/workspace/train_tracker
MATRIX_DIR=/home/plenox/rpi-rgb-led-matrix

# ---------------------------------------------------------------------------
# rpi-rgb-led-matrix — clone, build C library, install Python bindings
# ---------------------------------------------------------------------------

echo "==> Installing build dependencies..."
apt-get install -y python3-dev cython3 --quiet

# Clone as the invoking user so pip can write Cython-generated files back into the source tree.
# install.sh is run via sudo, so SUDO_USER tells us who actually invoked it.
REAL_USER="${SUDO_USER:-$USER}"

if [ ! -d "$MATRIX_DIR" ]; then
    echo "==> Cloning rpi-rgb-led-matrix as $REAL_USER..."
    sudo -u "$REAL_USER" git clone https://github.com/hzeller/rpi-rgb-led-matrix.git "$MATRIX_DIR"
else
    echo "==> rpi-rgb-led-matrix already cloned, skipping clone"
    # Ensure ownership is correct regardless
    chown -R "$REAL_USER":"$REAL_USER" "$MATRIX_DIR"
fi

echo "==> Building and installing Python bindings (as $REAL_USER)..."
sudo -u "$REAL_USER" "$PROJ/venv/bin/pip" install "$MATRIX_DIR"

# ---------------------------------------------------------------------------
# Python dependencies
# ---------------------------------------------------------------------------

echo "==> Installing Python dependencies..."
"$PROJ/venv/bin/pip" install flask --quiet

# ---------------------------------------------------------------------------
# systemd services
# ---------------------------------------------------------------------------

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
echo ""
echo "Note: LED font files are in $MATRIX_DIR/fonts/"
echo "      Set LED_FONT_DIR in .env to override the default font path."
