"""
Captive portal web app for train tracker setup mode.
Serves WiFi configuration and API credential management.
Runs as a daemon thread inside mode_manager.py.
"""

import os
import subprocess
import threading
from pathlib import Path

from dotenv import dotenv_values
from flask import Flask, jsonify, render_template, request

ENV_PATH = Path(__file__).parent.parent / ".env"

CREDENTIAL_FIELDS = [
    "LDBWS_CONSUMER_KEY",
    "LDBWS_CONSUMER_SECRET",
    "TD_KAFKA_BOOTSTRAP",
    "TD_KAFKA_USERNAME",
    "TD_KAFKA_PASSWORD",
    "TD_KAFKA_GROUP",
    "TD_KAFKA_TOPIC",
]

app = Flask(__name__)

# Set by mode_manager after a successful WiFi connect so the portal
# can report "connected" status back to the polling JS.
wifi_connect_result = {"status": "idle", "message": ""}
_connect_lock = threading.Lock()


def _connect_wifi(ssid: str, password: str):
    global wifi_connect_result
    with _connect_lock:
        wifi_connect_result = {"status": "connecting", "message": ""}
    try:
        result = subprocess.run(
            ["nmcli", "dev", "wifi", "connect", ssid,
             "password", password, "ifname", "wlan0"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            with _connect_lock:
                wifi_connect_result = {"status": "connected", "message": ssid}
        else:
            with _connect_lock:
                wifi_connect_result = {
                    "status": "failed",
                    "message": result.stderr.strip() or result.stdout.strip()
                }
    except subprocess.TimeoutExpired:
        with _connect_lock:
            wifi_connect_result = {"status": "failed", "message": "Connection timed out"}


@app.route("/")
def index():
    current_creds = dotenv_values(ENV_PATH)
    return render_template("index.html",
                           fields=CREDENTIAL_FIELDS,
                           creds=current_creds)


@app.route("/scan")
def scan():
    try:
        result = subprocess.run(
            ["nmcli", "-t", "-f", "SSID,SIGNAL,SECURITY", "dev", "wifi", "list"],
            capture_output=True, text=True, timeout=10
        )
        networks = []
        seen = set()
        for line in result.stdout.splitlines():
            parts = line.split(":")
            if len(parts) >= 2:
                ssid = parts[0].strip()
                if ssid and ssid not in seen:
                    seen.add(ssid)
                    networks.append({
                        "ssid": ssid,
                        "signal": parts[1] if len(parts) > 1 else "",
                        "security": parts[2] if len(parts) > 2 else "",
                    })
        return jsonify(sorted(networks, key=lambda n: int(n["signal"] or 0), reverse=True))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/wifi", methods=["POST"])
def connect_wifi():
    ssid = request.form.get("ssid", "").strip()
    password = request.form.get("password", "").strip()
    if not ssid:
        return jsonify({"error": "SSID is required"}), 400
    threading.Thread(target=_connect_wifi, args=(ssid, password), daemon=True).start()
    return jsonify({"status": "connecting"})


@app.route("/wifi/status")
def wifi_status():
    with _connect_lock:
        return jsonify(wifi_connect_result)


@app.route("/credentials", methods=["POST"])
def save_credentials():
    lines = []
    for field in CREDENTIAL_FIELDS:
        val = request.form.get(field, "").strip()
        lines.append(f"{field}={val}\n")

    # Write atomically
    tmp = ENV_PATH.with_suffix(".tmp")
    tmp.write_text("".join(lines))
    os.replace(tmp, ENV_PATH)

    # Restart display service to pick up new credentials (only if it's running)
    r = subprocess.run(["systemctl", "is-active", "train-display.service"],
                       capture_output=True, text=True)
    display_active = r.stdout.strip() == "active"
    if display_active:
        subprocess.run(["systemctl", "restart", "train-display.service"],
                       capture_output=True)

    return jsonify({"status": "saved", "display_restarted": display_active})
