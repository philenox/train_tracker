# Train Tracker

A real-time train tracker for a section of track visible from a window just west of Reading station. Predicts and displays upcoming trains using live Network Rail data, with a 3-tier prediction engine that gets more accurate as a train approaches.

## Hardware

- Raspberry Pi 4B
- 128×64 RGB LED matrix panel with Adafruit RGB Matrix HAT

## How it works

### Data sources

Two Network Rail feeds are consumed simultaneously via STOMP:

| Feed | Topic | Used for |
|------|-------|----------|
| TD (Train Describer) | `TD_ALL_SIG_AREA` | Real-time train positions in the Reading signal area (D1/D2) |
| TRUST (Train Movement) | `TRAIN_MVT_ALL_TOC` | Reported delays at timing points |

Schedule data comes from the **Network Rail CIF timetable**, downloaded daily and stored in a local SQLite database (`schedules.db`).

### Prediction engine

Predictions use a 3-tier hierarchy — the highest-confidence source available wins:

| Source | When used | How ETA is calculated |
|--------|-----------|----------------------|
| **TD** | Train has been seen at a known berth in the Reading area | `position timestamp + routing table eta_mean` |
| **TRUST** | Train not yet in TD area, but has a reported delay | Schedule time ± TRUST delay |
| **SCHED** | No real-time data available | Raw CIF schedule time |

For TD and TRUST/SCHED predictions, an **ETA floor** is applied: if a train hasn't been seen anywhere in the Reading TD area, its ETA can't be sooner than the furthest-out berth in the routing table (~12 min westbound, ~7 min eastbound). This prevents optimistic schedule ETAs from showing trains as imminent before they've entered the area.

Trains with no TD position, no TRUST data, and a schedule ETA already past the floor are dropped from the display entirely.

### Routing table

`routing_table.json` is built from collected TD data by `analyse_routes.py`. It maps each observed berth to statistics about how often trains from that berth reach the visible window, and how long it takes:

```
berth1621__WB → { p_visible: 1.0, eta_mean: 640s, eta_std: 184s, n_trains: 143 }
berth1743__WB → { p_visible: 1.0, eta_mean: 21s,  eta_std: 5s,   n_trains: 202 }
```

The westbound chain covers ~12 minutes of advance warning (30+ berths). The eastbound chain covers ~7 minutes (12 berths). Berths that consistently lead to platforms or sidings rather than the visible window are flagged as off-path and used to suppress those trains from the display.

Regenerate the routing table after collecting more data:
```
venv/bin/python analyse_routes.py
```

### Display layout

```
┌──────────────────────────┐
│ WB 22:29 Bristol TM~     │
├──────────────────────────┤
│ EB 22:30 London Pad~     │
├──────────────────────────┤
│ WB 22:45 Oxford          │
├──────────────────────────┤
│ EB 22:51 London Pad~     │
├──────────────────────────┤
│        22:29:45          │  ← live clock
└──────────────────────────┘
```

Long journey strings scroll after a static period. ETA is shown as `HH:MM` when more than 90s away, or a live `XXs` countdown when imminent.

## Files

| File | Purpose |
|------|---------|
| `display.py` | LED matrix display driver — runs on the Pi |
| `monitor.py` | Terminal-based monitor — for development/MacBook use |
| `predict.py` | Prediction engine — schedule + TRUST + TD routing |
| `td_client.py` | Background TD STOMP client, tracks positions for all D1/D2 berths |
| `trust_client.py` | Background TRUST STOMP client, tracks delays by headcode |
| `schedule_db.py` | CIF schedule database — downloads, imports, and queries |
| `routing.py` | Loads and queries `routing_table.json` |
| `analyse_routes.py` | Analyses collected TD data to build the routing table |
| `collect.py` | Long-running logger — writes TD and TRUST data to daily CSV files |
| `mode_manager.py` | Boot manager — starts display or setup mode, monitors setup button |
| `portal/app.py` | Flask setup portal — WiFi config and credential management |

## Setup

### 1. Install dependencies

**On the Pi** (LED matrix display):
```bash
python3 -m venv venv
venv/bin/pip install stomp.py python-dotenv flask requests
# rgbmatrix is installed from the rpi-rgb-led-matrix source — see below
```

**On a Mac** (terminal monitor only):
```bash
uv venv --python 3.12 venv
venv/bin/pip install stomp.py python-dotenv flask requests pandas tabulate
```

The `rgbmatrix` Python bindings are built from the [rpi-rgb-led-matrix](https://github.com/hzeller/rpi-rgb-led-matrix) library. Clone it on the Pi and build the Python extension:
```bash
git clone https://github.com/hzeller/rpi-rgb-led-matrix
cd rpi-rgb-led-matrix
make build-python PYTHON=$(which python3)
sudo make install-python PYTHON=$(which python3)
```

Set the font directory path if it differs from the default:
```bash
echo "LED_FONT_DIR=/home/plenox/rpi-rgb-led-matrix/fonts" >> .env
```

### 2. Configure credentials

```bash
cp .env.example .env
```

Edit `.env`:

| Variable | Description |
|----------|-------------|
| `NR_USERNAME` | Network Rail Open Data email |
| `NR_PASSWORD` | Network Rail Open Data password |

Register at [Network Rail Open Data](https://datafeeds.networkrail.co.uk). Subscribe to **TD_ALL_SIG_AREA** and **TRAIN_MVT_ALL_TOC** (free, no approval required).

### 3. Download the schedule database

```bash
venv/bin/python -c "import schedule_db; schedule_db.refresh_if_stale(max_age_hours=0)"
```

This downloads the current CIF timetable (~100MB) and imports it into `schedules.db`. The schedule is refreshed automatically if it's more than 20 hours old when the display or monitor starts.

### 4. Build the routing table

The routing table requires collected TD data. Either copy an existing `routing_table.json` from another instance, or collect data first (see below) then run:

```bash
venv/bin/python analyse_routes.py
```

## Usage

**Run the terminal monitor** (MacBook / development):
```bash
caffeinate -i venv/bin/python monitor.py
```

**Run the LED matrix display** (Pi, requires root for GPIO):
```bash
sudo venv/bin/python display.py
```

**Collect TD + TRUST data to CSV** (for routing table analysis):
```bash
caffeinate -i venv/bin/python -u collect.py
```

Data is written to `data/td_YYYY-MM-DD.csv` and `data/trust_YYYY-MM-DD.csv`, rotating at midnight. Run for at least a few peak-hour periods before regenerating the routing table.

## Auto-boot (systemd)

Two services manage the Pi:

- `train-manager.service` — runs `mode_manager.py` as root on boot
- `train-display.service` — runs `display.py` as the `plenox` user

```bash
sudo bash install.sh
```

`mode_manager.py` decides which mode to run:

1. **Normal mode** — WiFi is connected and button not held at boot → starts `train-display.service`
2. **Setup mode** — no WiFi after 30s, or button held at boot, or button held for 3s at runtime → stops the display, starts a hotspot and serves the setup portal

### Setup mode

When setup mode activates the LED matrix shows connection instructions:

```
-- Setup Mode --
WiFi: TrainTracker
Pass: traintracker
Go: 192.168.4.1
```

Connect to the `TrainTracker` hotspot, open `http://192.168.4.1` in a browser, and configure WiFi credentials and Network Rail API keys. Once WiFi connects successfully the Pi exits setup mode and restarts the display automatically.

### Setup button

A momentary push button lets you trigger setup mode at any time without SSH access — useful for changing WiFi networks or re-entering credentials.

| Connection | Detail |
|-----------|--------|
| GPIO pin | GPIO 25 (Pi header pin 22) |
| Other leg | GND (any GND pin, e.g. header pin 20) |
| Hold duration | 3 seconds to activate setup mode |

The pin is configured with an internal pull-up, so no external resistor is needed — just wire the button directly between GPIO 25 and GND.

## LED matrix wiring

The display uses an **Adafruit RGB Matrix HAT** which handles all GPIO wiring. Connect the HAT to the Pi's 40-pin header and attach the LED matrix panel to the HAT's output connector per the [Adafruit wiring guide](https://learn.adafruit.com/adafruit-rgb-matrix-plus-real-time-clock-hat-for-raspberry-pi).

Key settings used in `display.py`:

| Option | Value |
|--------|-------|
| `rows` | 64 |
| `cols` | 128 |
| `hardware_mapping` | `adafruit-hat` |
| `led_rgb_sequence` | `BRG` |
| `gpio_slowdown` | 5 |
