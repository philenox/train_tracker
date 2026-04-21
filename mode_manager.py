#!/usr/bin/env python3
"""
Train Tracker mode manager.

Decides whether to run in normal mode (display.py shows trains) or setup
mode (hotspot + captive portal). Monitors a physical button on GPIO 25
to allow the user to force setup mode at any time.

Must run as root (for nmcli hotspot management and binding port 80).
"""

import os
import subprocess
import sys
import threading
import time
import RPi.GPIO as GPIO
from rgbmatrix import RGBMatrix, RGBMatrixOptions, graphics

FONT_DIR = os.environ.get("LED_FONT_DIR", "/home/plenox/rpi-rgb-led-matrix/fonts")

BUTTON_PIN = 25          # GPIO 25, Pi header pin 22 — connect to GND
BUTTON_HOLD_SECS = 3     # hold duration to trigger setup mode (button on GPIO 25)
WIFI_TIMEOUT_SECS = 30   # wait for WiFi on boot before falling back to hotspot
HOTSPOT_CON_NAME = "TrainTrackerHotspot"
HOTSPOT_IP = "192.168.4.1"
PORTAL_PORT = 80

_setup_requested = threading.Event()
_portal_started = False


# ---------------------------------------------------------------------------
# GPIO
# ---------------------------------------------------------------------------

def init_gpio():
    GPIO.setwarnings(False)
    GPIO.setmode(GPIO.BCM)
    GPIO.setup(BUTTON_PIN, GPIO.IN, pull_up_down=GPIO.PUD_UP)

    def _poll():
        press_start = None
        while True:
            if GPIO.input(BUTTON_PIN) == GPIO.LOW:
                if press_start is None:
                    press_start = time.monotonic()
                elif (time.monotonic() - press_start) >= BUTTON_HOLD_SECS:
                    if not _setup_requested.is_set():
                        print("Button held — requesting setup mode")
                        _setup_requested.set()
            else:
                press_start = None
            time.sleep(0.1)

    threading.Thread(target=_poll, daemon=True, name="button-poll").start()


def button_held_at_boot() -> bool:
    """Return True if the button is already pressed when we start up."""
    return GPIO.input(BUTTON_PIN) == GPIO.LOW


# ---------------------------------------------------------------------------
# LED Matrix
# ---------------------------------------------------------------------------

def make_matrix():
    options = RGBMatrixOptions()
    options.rows = 64
    options.cols = 128
    options.hardware_mapping = 'adafruit-hat'
    options.led_rgb_sequence = 'BRG'
    options.gpio_slowdown = 5
    options.multiplexing = 0
    options.disable_hardware_pulsing = False
    options.drop_privileges = False
    return RGBMatrix(options=options)


def show_setup_screen(matrix):
    font = graphics.Font()
    font.LoadFont(os.path.join(FONT_DIR, "6x10.bdf"))
    amber = graphics.Color(255, 170, 0)
    white = graphics.Color(255, 255, 255)
    c = matrix.CreateFrameCanvas()
    c.Clear()
    graphics.DrawText(c, font,  0, 12, amber, "-- Setup Mode --")
    graphics.DrawText(c, font,  0, 28, white, "WiFi: TrainTracker")
    graphics.DrawText(c, font,  0, 44, white, "Pass: traintracker")
    graphics.DrawText(c, font,  0, 60, white, f"Go: {HOTSPOT_IP}")
    matrix.SwapOnVSync(c)


def clear_display(matrix):
    matrix.Clear()


# ---------------------------------------------------------------------------
# WiFi / hotspot
# ---------------------------------------------------------------------------

def wifi_is_connected() -> bool:
    r = subprocess.run(
        ["nmcli", "-t", "-f", "STATE", "general"],
        capture_output=True, text=True
    )
    return r.stdout.strip() == "connected"


def wait_for_wifi(timeout: int) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if wifi_is_connected():
            return True
        time.sleep(2)
    return False


def ensure_hotspot_profile():
    """Create the NM hotspot connection profile if it doesn't exist."""
    r = subprocess.run(
        ["nmcli", "con", "show", HOTSPOT_CON_NAME],
        capture_output=True
    )
    if r.returncode == 0:
        return  # already exists
    print(f"Creating hotspot profile '{HOTSPOT_CON_NAME}'...")
    subprocess.run([
        "nmcli", "con", "add",
        "type", "wifi",
        "ifname", "wlan0",
        "con-name", HOTSPOT_CON_NAME,
        "ssid", "TrainTracker",
        "802-11-wireless.mode", "ap",
        "802-11-wireless-security.key-mgmt", "wpa-psk",
        "802-11-wireless-security.psk", "traintracker",
        "ipv4.method", "shared",
        "ipv4.addresses", f"{HOTSPOT_IP}/24",
        "connection.autoconnect", "no",
    ], check=True)


def start_hotspot():
    print("Starting hotspot...")
    subprocess.run(["nmcli", "con", "up", HOTSPOT_CON_NAME], check=True)


def stop_hotspot():
    print("Stopping hotspot...")
    subprocess.run(["nmcli", "con", "down", HOTSPOT_CON_NAME],
                   capture_output=True)  # don't raise — may already be down


# ---------------------------------------------------------------------------
# Display service
# ---------------------------------------------------------------------------

def start_display_service():
    subprocess.run(["systemctl", "start", "train-display.service"], check=True)


def stop_display_service():
    subprocess.run(["systemctl", "stop", "train-display.service"],
                   capture_output=True)


# ---------------------------------------------------------------------------
# Portal
# ---------------------------------------------------------------------------

def start_portal():
    global _portal_started
    if _portal_started:
        return
    _portal_started = True
    from portal.app import app as flask_app
    t = threading.Thread(
        target=lambda: flask_app.run(
            host=HOTSPOT_IP, port=PORTAL_PORT, debug=False, use_reloader=False
        ),
        daemon=True,
        name="portal",
    )
    t.start()


# ---------------------------------------------------------------------------
# Mode loops
# ---------------------------------------------------------------------------

def normal_mode():
    print("Entering normal mode")
    # Clear any stale request that may have accumulated during setup mode
    _setup_requested.clear()
    start_display_service()

    # Wait for a setup request from the button
    _setup_requested.wait()
    _setup_requested.clear()

    print("Setup requested — stopping display service")
    stop_display_service()
    time.sleep(2)  # allow display.py to release the matrix driver


def setup_mode():
    print("Entering setup mode")
    ensure_hotspot_profile()
    start_hotspot()

    matrix = make_matrix()
    show_setup_screen(matrix)
    start_portal()

    # Poll until WiFi connects (triggered by portal /wifi POST)
    print("Waiting for WiFi connection via portal...")
    while True:
        if wifi_is_connected():
            print("WiFi connected — leaving setup mode")
            break
        time.sleep(5)

    stop_hotspot()
    clear_display(matrix)
    del matrix   # release the LED matrix driver before display.py starts
    time.sleep(1)  # allow NM to stabilise before display starts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if subprocess.run(["id", "-u"], capture_output=True, text=True).stdout.strip() != "0":
        print("Error: mode_manager.py must run as root", file=sys.stderr)
        sys.exit(1)

    init_gpio()

    # Boot-time check: button held or WiFi unavailable → setup mode first
    if button_held_at_boot():
        print("Button held at boot")
        setup_mode()
    elif not wait_for_wifi(WIFI_TIMEOUT_SECS):
        print(f"WiFi not connected after {WIFI_TIMEOUT_SECS}s")
        setup_mode()

    # Main loop — alternate between normal and setup as needed
    while True:
        normal_mode()
        setup_mode()


if __name__ == "__main__":
    main()
