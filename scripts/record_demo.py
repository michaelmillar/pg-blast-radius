#!/usr/bin/env python3
"""Generate demo video for pg-blast-radius.

Requires: pip install playwright Pillow
Then:      playwright install chromium
And:       ffmpeg must be on PATH

Usage:     python scripts/record_demo.py
Output:    demo.mp4 in project root
"""

import html
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

try:
    from PIL import Image  # noqa: F401
except ImportError:
    raise SystemExit("pip install Pillow")

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    raise SystemExit("pip install playwright && playwright install chromium")

ROOT = Path(__file__).resolve().parent.parent
BINARY = ROOT / "target" / "release" / "pg-blast-radius"
DEMO_SQL = ROOT / "testdata" / "demo-migration.sql"
DEMO_STATS = ROOT / "testdata" / "demo-stats.json"
WIDTH = 1280
HEIGHT = 720
FPS = 2
BG = "#1a1b26"
FG = "#c0caf5"

ANSI_TO_CSS = {
    "30": "color:#414868", "31": "color:#f7768e", "32": "color:#9ece6a",
    "33": "color:#e0af68", "34": "color:#7aa2f7", "35": "color:#bb9af7",
    "36": "color:#7dcfff", "37": "color:#c0caf5", "39": f"color:{FG}",
    "90": "color:#565f89", "91": "color:#ff9e9e",
    "1": "font-weight:bold", "2": "opacity:0.6", "4": "text-decoration:underline",
    "22": "font-weight:normal;opacity:1",
    "0": f"color:{FG};font-weight:normal;opacity:1;text-decoration:none;background:none",
    "41": f"background:#f7768e;color:#1a1b26", "42": f"background:#9ece6a;color:#1a1b26",
    "43": f"background:#e0af68;color:#1a1b26", "101": f"background:#ff9e9e;color:#1a1b26",
}


def ansi_to_html(text):
    escaped = html.escape(text)
    result = []
    open_spans = 0
    for chunk in re.split(r"\x1b\[([0-9;]+)m", escaped):
        if re.match(r"^[0-9;]+$", chunk):
            codes = chunk.split(";")
            styles = []
            for code in codes:
                if code in ANSI_TO_CSS:
                    styles.append(ANSI_TO_CSS[code])
            if "0" in codes:
                result.append("</span>" * open_spans)
                open_spans = 0
            if styles:
                result.append(f'<span style="{";".join(styles)}">')
                open_spans += 1
        else:
            result.append(chunk)
    result.append("</span>" * open_spans)
    return "".join(result)


def wrap_html(body, title=None):
    title_block = ""
    if title:
        title_block = f"""
        <div style="text-align:center;padding:40px 0 20px 0">
            <div style="font-size:48px;font-weight:bold;color:#7aa2f7;letter-spacing:-1px">{title}</div>
        </div>"""
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:{BG};color:{FG};font-family:'JetBrains Mono','Fira Code','Cascadia Code',monospace;font-size:13px;width:{WIDTH}px;height:{HEIGHT}px;overflow:hidden">
{title_block}
<pre style="margin:20px 40px;line-height:1.5;white-space:pre-wrap">{body}</pre>
</body></html>"""


def title_card_html():
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:{BG};color:{FG};font-family:'JetBrains Mono','Fira Code','Cascadia Code',monospace;width:{WIDTH}px;height:{HEIGHT}px;overflow:hidden;display:flex;flex-direction:column;align-items:center;justify-content:center">
    <div style="font-size:56px;font-weight:bold;color:#7aa2f7;letter-spacing:-1px">pg-blast-radius</div>
    <div style="font-size:20px;color:#565f89;margin-top:16px">workload-aware blast radius forecaster for PostgreSQL migrations</div>
    <div style="font-size:14px;color:#414868;margin-top:32px">v0.2.0</div>
</body></html>"""


def run_demo():
    if not BINARY.exists():
        subprocess.run(["cargo", "build", "--release"], cwd=ROOT, check=True)

    static_result = subprocess.run(
        [str(BINARY), "analyse", str(DEMO_SQL)],
        capture_output=True, text=True
    )
    static_output = static_result.stdout + static_result.stderr

    workload_result = subprocess.run(
        [str(BINARY), "analyse", str(DEMO_SQL), "--stats-file", str(DEMO_STATS)],
        capture_output=True, text=True
    )
    workload_output = workload_result.stdout + workload_result.stderr

    return static_output, workload_output


def render_frames(static_output, workload_output):
    frames = []
    tmpdir = tempfile.mkdtemp(prefix="pgbr_demo_")

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": WIDTH, "height": HEIGHT})

        title_path = os.path.join(tmpdir, "title.html")
        with open(title_path, "w") as f:
            f.write(title_card_html())
        page.goto(f"file://{title_path}")
        for _ in range(FPS * 4):
            frame_path = os.path.join(tmpdir, f"frame_{len(frames):04d}.png")
            page.screenshot(path=frame_path)
            frames.append(frame_path)

        prompt = f"<span style='color:#9ece6a'>$</span> pg-blast-radius analyse migration.sql"
        for line_count in [3, 8, len(static_output.split("\n"))]:
            lines = static_output.split("\n")[:line_count]
            partial = "\n".join(lines)
            body = prompt + "\n\n" + ansi_to_html(partial)
            html_path = os.path.join(tmpdir, f"static_{line_count}.html")
            with open(html_path, "w") as f:
                f.write(wrap_html(body, "static analysis"))
            page.goto(f"file://{html_path}")
            hold = FPS * 2 if line_count == len(static_output.split("\n")) else FPS
            for _ in range(hold):
                frame_path = os.path.join(tmpdir, f"frame_{len(frames):04d}.png")
                page.screenshot(path=frame_path)
                frames.append(frame_path)

        prompt2 = f"<span style='color:#9ece6a'>$</span> pg-blast-radius analyse migration.sql --stats-file prod-stats.json"
        workload_lines = workload_output.split("\n")
        cutpoints = [10, 20, 30, len(workload_lines)]
        for cp in cutpoints:
            partial = "\n".join(workload_lines[:cp])
            body = prompt2 + "\n\n" + ansi_to_html(partial)
            html_path = os.path.join(tmpdir, f"workload_{cp}.html")
            with open(html_path, "w") as f:
                f.write(wrap_html(body, "workload-aware analysis"))
            page.goto(f"file://{html_path}")
            hold = FPS * 4 if cp == len(workload_lines) else FPS * 2
            for _ in range(hold):
                frame_path = os.path.join(tmpdir, f"frame_{len(frames):04d}.png")
                page.screenshot(path=frame_path)
                frames.append(frame_path)

        browser.close()

    return frames, tmpdir


def compile_video(frames, tmpdir):
    output = ROOT / "demo.mp4"
    list_path = os.path.join(tmpdir, "frames.txt")
    with open(list_path, "w") as f:
        for frame in frames:
            f.write(f"file '{frame}'\n")
            f.write(f"duration {1/FPS}\n")

    subprocess.run([
        "ffmpeg", "-y", "-f", "concat", "-safe", "0",
        "-i", list_path,
        "-vf", f"scale={WIDTH}:{HEIGHT}",
        "-c:v", "libx264", "-pix_fmt", "yuv420p",
        "-r", str(FPS * 2),
        str(output),
    ], check=True, capture_output=True)

    shutil.rmtree(tmpdir)
    print(f"wrote {output} ({output.stat().st_size // 1024} kB)")
    return output


def main():
    print("building release binary...")
    subprocess.run(["cargo", "build", "--release"], cwd=ROOT, check=True)

    print("running demo commands...")
    static_output, workload_output = run_demo()

    print("rendering frames...")
    frames, tmpdir = render_frames(static_output, workload_output)
    print(f"  {len(frames)} frames")

    print("compiling video...")
    compile_video(frames, tmpdir)


if __name__ == "__main__":
    main()
