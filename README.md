# Train Tracker

A real-time train tracker for a specific section of track visible from a window just west of Reading station. Displays approaching and passing trains on a 256×64 SSD1322 OLED display using live Network Rail TD feed data.

## How it works

All data comes from the **Network Rail TD (Train Describer) feed** via STOMP. No Darwin/departure board data is used.

Four berths are watched:

| Berth | Direction | Role |
|-------|-----------|------|
| 1733 | Westbound | Trigger — train detected here, ETA computed |
| 1757 | Westbound | Visible — train is passing the window |
| 1772 | Eastbound | Trigger — train detected here, ETA computed |
| 1724 | Eastbound | Visible — train is passing the window |

When a train crosses a trigger berth, the display shows a live countdown to when it will reach the visible berth (~52s westbound, ~174s eastbound). When it crosses the visible berth, the display updates to show the actual pass time.

## Display layout

```
┌─────────────────────────────────────────┐
│ WB  HH:MM:SS  headcode  in Xs           │  ← WB approaching (ETA from 1733)
├─────────────────────────────────────────┤
│ WB  HH:MM:SS  headcode  (Xm ago)        │  ← WB last seen at 1757
├─────────────────────────────────────────┤
│ EB  HH:MM:SS  headcode  in Xs / Xm ago  │  ← EB approaching or last seen
└─────────────────────────────────────────┘
```

## Hardware

- Raspberry Pi 4B
- SSD1322-based 256×64 SPI OLED display (yellow/amber)

## Setup

### 1. Enable SPI on the Pi

```
sudo raspi-config
# Interface Options → SPI → Yes
```

Then activate without rebooting:

```
sudo dtparam spi=on
```

### 2. Install dependencies

```
python3 -m venv venv
venv/bin/pip install stomp.py python-dotenv luma.oled RPi.GPIO spidev flask
```

### 3. Configure credentials

```
cp .env.example .env
```

Edit `.env` with your credentials:

| Variable | Description |
|---|---|
| `NR_USERNAME` | Network Rail Open Data email |
| `NR_PASSWORD` | Network Rail Open Data password |

Register at [Network Rail Open Data](https://datafeeds.networkrail.co.uk). Subscribe to **TD_ALL_SIG_AREA** (free, no approval required).

## Usage

Run the OLED display (auto-refreshes every second):

```
venv/bin/python display.py
```

Watch berth events in the terminal:

```
venv/bin/python td_listen.py              # watch trigger + visible berths only
venv/bin/python td_listen.py --all        # log all Reading area berth steps
venv/bin/python td_listen.py --csv FILE   # record all steps to CSV
```

## Auto-boot (systemd)

The display starts automatically on boot via two systemd services:

- `train-manager.service` — runs as root, checks WiFi on boot, starts display or hotspot setup mode
- `train-display.service` — runs the OLED display as the `plenox` user

Install:
```
sudo bash install.sh
```

On first boot without WiFi configured, the Pi creates a hotspot (`TrainTrackerHotspot`) and serves a setup page at `http://192.168.4.1` where you can enter WiFi credentials and API keys.

## Recording TD data

To record all berth steps to CSV for later analysis (persists across SSH disconnects via tmux):

```
tmux new -s td-record
venv/bin/python -u td_listen.py --csv td_data.csv
# Ctrl+B then D to detach
```

## Display wiring

| Display pin | Label | Raspberry Pi pin | GPIO |
|---|---|---|---|
| 1 | VSS | Pin 6 | GND |
| 2 | VCC_IN | Pin 2 | 5V |
| 4 | D0/CLK | Pin 23 | GPIO 11 (SPI SCLK) |
| 5 | DI/DIN | Pin 19 | GPIO 10 (SPI MOSI) |
| 14 | D/C# | Pin 18 | GPIO 24 |
| 15 | RES# | Pin 22 | GPIO 25 |
| 16 | CS# | Pin 24 | GPIO 8 (SPI CE0) |

The display must be configured for 4-wire SPI mode via its solder jumpers (R5 + R8 on the tested module).

> **Note:** VCC_IN requires 5V (not 3.3V) — the module has an onboard boost converter that needs 5V input.
