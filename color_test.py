#!/usr/bin/env python3
"""Quick color channel diagnostic — cycles through pure R, G, B, then our amber."""

import time
import os
from rgbmatrix import RGBMatrix, RGBMatrixOptions, graphics
from dotenv import load_dotenv

load_dotenv()

STEPS = [
    ("RED   (255,   0,   0)", graphics.Color(255,   0,   0)),
    ("GREEN (  0, 255,   0)", graphics.Color(  0, 255,   0)),
    ("BLUE  (  0,   0, 255)", graphics.Color(  0,   0, 255)),
    ("AMBER (255, 140,   0)", graphics.Color(255, 140,   0)),
]
HOLD_SECS = 4

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

matrix = make_matrix()
canvas = matrix.CreateFrameCanvas()
font = graphics.Font()
font.LoadFont(os.path.join(
    os.environ.get("LED_FONT_DIR", "/home/plenox/rpi-rgb-led-matrix/fonts"),
    "5x7.bdf"
))

for label, color in STEPS:
    canvas.Clear()
    # Fill top half solid so the colour is obvious
    for y in range(32):
        graphics.DrawLine(canvas, 0, y, 127, y, color)
    # Label in the bottom half
    graphics.DrawText(canvas, font, 2, 48, color, label[:21])
    canvas = matrix.SwapOnVSync(canvas)
    print(f"Showing: {label}  — what colour do you see?")
    time.sleep(HOLD_SECS)

matrix.Clear()
print("Done.")
