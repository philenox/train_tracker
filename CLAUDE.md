Train Tracker Project
Goal
Build a real-time train tracker that detects when trains cross a section of track visible from the window, just west of Reading station. Display upcoming and passing trains on a miniature replica of a UK platform departure board.
Hardware

Raspberry Pi 4B (4GB RAM, Debian 12 Bookworm aarch64, 2GB swap configured)
Display: SSD1322-based 256x64 SPI OLED display (yellow/amber for authentic UK dot-matrix look)
Raspberry Pi Pico available for potential future use as a dedicated low-power display driver, but not part of initial development

Development Environment

Claude Code runs over SSH from a Windows laptop (WSL) to the Pi
SSH config: ssh rpi → plenox@192.168.1.125
Use tmux for persistent sessions on the Pi

Data Sources
Primary: Network Rail Open Data (for real-time track detection)

TD (Train Describer) feed for the Reading TV signal area — shows trains moving between signal berths, which maps to specific track sections
Train Movement (TRUST) feed — reports of trains passing timing points
SCHEDULE feed — full timetable to match train IDs to services
Register at: https://datafeeds.networkrail.co.uk
Connection method: STOMP streaming protocol
Python library: stomp.py
Community wiki: https://wiki.openraildata.com

Secondary: National Rail Darwin API

Real-time departure/arrival data for Reading station
Python library: nredarwin or zeep (SOAP)
Register at: https://www.nationalrail.co.uk/developers/

Train identification

Match headcode/train ID from TD feed against schedule data to get operator, origin/destination, and rolling stock class (e.g. Class 800, Class 387)
Realtime Trains API (realtimetrains.co.uk) provides rolling stock/formation data

Display
Existing open-source projects to reference/fork

chrisys/train-departure-display (GitHub) — most fully-featured, uses SSD1322, has 3D-printable cases, dual display support
DanielHartUK/Dot-Matrix-Typeface (GitHub) — authentic UK dot-matrix fonts
tomwardio/pico_train_display — MicroPython version for Pico W (reference for potential future Pico port)

Approach
Fork or adapt the display rendering from existing projects, but replace the data source: instead of Darwin station departures, feed from Network Rail TD data showing trains on our specific track section.
Suggested Development Order

Connect to Network Rail TD feed — subscribe to the TV (Reading) signal area, print berth step messages to terminal
Identify relevant berths — work out which berth codes correspond to the track section visible from the window (west of Reading station). The Open Rail Data community and wiki can help here.
Cross-reference with schedule data — match train headcodes to services, get operator/destination/rolling stock info
Predict upcoming trains — use schedule data to show what's coming next, not just what's passing now
Drive the OLED display — render train info in authentic UK departure board style using the SSD1322 display and dot-matrix fonts
Polish — 3D print a case, refine the display layout, handle edge cases
