#!/usr/bin/env python3
"""
Train Tracker mode manager.

Decides whether to run in normal mode (display.py shows trains) or setup
mode (hotspot + captive portal). Monitors a physical button on GPIO 17
to allow the user to force setup mode at any time.

Must run as root (for nmcli hotspot management and binding port 80).
"""

import subprocess
import sys
import threading
import time
import RPi.GPIO as GPIO
from luma.core.interface.serial import spi
from luma.oled.device import ssd1322
from luma.core.render import canvas

BUTTON_PIN = 17          # GPIO 17, Pi header pin 11 — connect to GND
BUTTON_HOLD_SECS = 3     # hold duration to trigger setup mode
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
# OLED
# ---------------------------------------------------------------------------

def make_device():
    serial = spi(device=0, port=0, bus_speed_hz=2000000, transfer_size=4096,
                 gpio_DC=24, gpio_RST=25)
    device = ssd1322(serial, width=256, height=64, rotate=0, mode='1')
    device.contrast(255)
    return device


def show_setup_screen(device):
    with canvas(device) as draw:
        draw.text((0,  0), "-- Setup Mode --",   fill='white')
        draw.text((0, 16), "WiFi: TrainTracker",  fill='white')
        draw.text((0, 32), "Pass: traintracker",  fill='white')
        draw.text((0, 48), f"Visit: {HOTSPOT_IP}", fill='white')


def clear_display(device):
    with canvas(device) as draw:
        draw.rectangle([0, 0, 255, 63], fill='black')


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
    time.sleep(1)  # allow display.py to release SPI/GPIO


def setup_mode():
    print("Entering setup mode")
    ensure_hotspot_profile()
    start_hotspot()

    device = make_device()
    show_setup_screen(device)
    start_portal()

    # Poll until WiFi connects (triggered by portal /wifi POST)
    print("Waiting for WiFi connection via portal...")
    while True:
        if wifi_is_connected():
            print("WiFi connected — leaving setup mode")
            break
        time.sleep(5)

    stop_hotspot()
    clear_display(device)
    device.cleanup()
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
