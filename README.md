# Train Tracker

A real-time train tracker for a specific section of track visible from a window just west of Reading station. Displays upcoming westbound trains on a 256×64 SSD1322 OLED display.

## How it works

- **Darwin LDBWS** is queried for all departures from Reading, filtered to westbound services only (eastbound and southbound trains are excluded by checking each service's first calling point and destination)
- The next 3 trains predicted to pass berth 1757 (~2 minutes west of Reading station) are shown on the display, with departure time and estimated pass time
- A separate Kafka listener connects to the Network Rail Combined TD feed and prints real-time events when a train steps into or out of berth 1757

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
venv/bin/pip install requests python-dotenv luma.oled RPi.GPIO spidev confluent-kafka
```

### 3. Configure credentials

```
cp .env.example .env
```

Edit `.env` with your API credentials:

| Variable | Description |
|---|---|
| `LDBWS_CONSUMER_KEY` | Rail Data Marketplace LDBWS consumer key |
| `TD_KAFKA_BOOTSTRAP` | Confluent Cloud bootstrap server |
| `TD_KAFKA_USERNAME` | TD feed consumer username |
| `TD_KAFKA_PASSWORD` | TD feed consumer password |
| `TD_KAFKA_GROUP` | TD feed consumer group |
| `TD_KAFKA_TOPIC` | TD feed Kafka topic |

Register for credentials at [Rail Data Marketplace](https://www.raildata.org.uk). You need:
- **LDBWS** — Live Departure Boards Web Service
- **Combined TD Feed** — real-time Train Describer berth data

## Usage

Show next 3 predicted trains in the terminal:

```
venv/bin/python trains.py
```

Run the OLED display (refreshes every 60 seconds):

```
venv/bin/python display.py
```

Listen for real-time berth 1757 events from the TD feed:

```
venv/bin/python td_listen.py
```

## Display wiring

| Display pin | Label | Raspberry Pi pin | GPIO |
|---|---|---|---|
| 1 | VSS | Pin 6 | GND |
| 2 | VCC_IN | Pin 1 | 3.3V |
| 4 | D0/CLK | Pin 23 | GPIO 11 (SPI SCLK) |
| 5 | DI/DIN | Pin 19 | GPIO 10 (SPI MOSI) |
| 14 | D/C# | Pin 18 | GPIO 24 |
| 15 | RES# | Pin 22 | GPIO 25 |
| 16 | CS# | Pin 24 | GPIO 8 (SPI CE0) |

The display must be configured for 4-wire SPI mode via its solder jumpers (R5 + R8 on the tested module).
