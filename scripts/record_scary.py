#!/usr/bin/env python3
"""Generate a 20 second scary demo for pg-blast-radius with continuous motion.

One ALTER COLUMN TYPE statement on a large hot table. Rendered as a single
HTML page with CSS animations; Playwright records the viewport to video.

Requires: pip install playwright Pillow
Then:      playwright install chromium
And:       ffmpeg must be on PATH

Usage:     python scripts/record_scary.py
Output:    scary.mp4 in project root
"""

import glob
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
STATS = ROOT / "testdata" / "demo-stats.json"
WIDTH = 1280
HEIGHT = 720
BG = "#1a1b26"
FG = "#c0caf5"
ACCENT = "#7dcfff"

SCARY_SQL = "ALTER TABLE orders ALTER COLUMN total TYPE numeric(12,2);"
TRUNCATE_AT_LINE = 17

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


def run_tool():
    if not BINARY.exists():
        subprocess.run(["cargo", "build", "--release"], cwd=ROOT, check=True)
    tmp = Path(tempfile.mkdtemp(prefix="pgbr_scary_sql_"))
    sql_path = tmp / "scary.sql"
    sql_path.write_text(SCARY_SQL + "\n")
    result = subprocess.run(
        [str(BINARY), "analyse", "scary.sql", "--stats-file", str(STATS)],
        capture_output=True, text=True, cwd=tmp,
    )
    shutil.rmtree(tmp)
    return result.stdout + result.stderr


def output_lines(text):
    raw = text.split("\n")
    kept = raw[:TRUNCATE_AT_LINE]
    kept.append("")
    kept.append("  \x1b[1m\x1b[91mEXTREME RISK\x1b[39m\x1b[0m | Confidence: ESTIMATED")
    return kept


def build_html(output):
    lines = output_lines(output)
    sql_chars = len(SCARY_SQL)

    lines_html = []
    for i, ln in enumerate(lines):
        body = ansi_to_html(ln) if ln.strip() else "&nbsp;"
        lines_html.append(
            f'<div class="line" style="--i:{i}">{body}</div>'
        )
    output_block = "\n".join(lines_html)

    stagger = 0.35
    line_dur = 0.3
    s3_start = 6.0
    s3_reveal_start = s3_start + 0.4
    s3_end = 18.0

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  html, body {{
    margin: 0; padding: 0;
    background: {BG}; color: {FG};
    width: {WIDTH}px; height: {HEIGHT}px;
    overflow: hidden;
    font-family: 'Inter', -apple-system, 'Segoe UI', 'Roboto', sans-serif;
  }}
  .scene {{
    position: absolute;
    inset: 0;
    opacity: 0;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    text-align: center;
  }}
  .scene-term {{
    align-items: flex-start;
    justify-content: flex-start;
    padding: 40px 56px;
    box-sizing: border-box;
    text-align: left;
  }}
  .h1 {{
    font-size: 44px;
    font-weight: 600;
    letter-spacing: -0.5px;
    max-width: 960px;
    color: {FG};
  }}
  .sub {{
    font-size: 22px;
    color: {ACCENT};
    margin-top: 22px;
    font-family: 'JetBrains Mono', 'Fira Code', monospace;
  }}
  .sql-wrap {{
    margin-top: 22px;
    font-family: 'JetBrains Mono', 'Fira Code', monospace;
    font-size: 22px;
    color: {ACCENT};
    display: inline-block;
    overflow: hidden;
    white-space: nowrap;
    border-right: 2px solid {ACCENT};
    width: 0;
    animation:
      type 1.4s steps({sql_chars}) 3.4s forwards,
      caret 0.55s step-end infinite 3.4s,
      caret-off 0.01s linear 5.9s forwards;
  }}
  @keyframes type {{
    from {{ width: 0; }}
    to   {{ width: {sql_chars}ch; }}
  }}
  @keyframes caret {{
    50% {{ border-color: transparent; }}
  }}
  @keyframes caret-off {{
    to {{ border-color: transparent; }}
  }}
  .term {{
    font-family: 'JetBrains Mono', 'Fira Code', monospace;
    font-size: 14px;
    line-height: 1.55;
    white-space: pre-wrap;
    width: 100%;
  }}
  .prompt {{
    color: #9ece6a;
    opacity: 0;
    animation: line-in 0.35s forwards;
    animation-delay: {s3_reveal_start - 0.15}s;
  }}
  .prompt::after {{
    content: '';
    display: inline-block;
    width: 8px;
    height: 15px;
    background: {FG};
    margin-left: 4px;
    vertical-align: -2px;
    animation: caret 0.6s step-end infinite {s3_reveal_start}s;
  }}
  .output {{ margin-top: 8px; }}
  .line {{
    opacity: 0;
    transform: translateY(6px);
    animation: line-in {line_dur}s forwards;
    animation-delay: calc({s3_reveal_start + 0.25}s + var(--i) * {stagger}s);
  }}
  @keyframes line-in {{
    to {{ opacity: 1; transform: translateY(0); }}
  }}
  #s1 {{ animation: scene 3s 0s forwards; }}
  #s2 {{ animation: scene 3s 3s forwards; }}
  #s3 {{ animation: scene {s3_end - s3_start}s {s3_start}s forwards; }}
  #s4 {{ animation: scene-out 2s {s3_end}s forwards; }}
  @keyframes scene {{
    0%   {{ opacity: 0; }}
    13%  {{ opacity: 1; }}
    87%  {{ opacity: 1; }}
    100% {{ opacity: 0; }}
  }}
  @keyframes scene-out {{
    0%   {{ opacity: 0; }}
    25%  {{ opacity: 1; }}
    100% {{ opacity: 1; }}
  }}
  #s1 .h1, #s2 .h1, #s4 .h1, #s4 .sub {{
    opacity: 0;
    transform: translateY(6px);
  }}
  #s1.in .h1 {{ animation: line-in 0.55s 0.2s forwards; }}
  #s2.in .h1 {{ animation: line-in 0.55s 0.2s forwards; }}
  #s4.in .h1 {{ animation: line-in 0.6s 0.15s forwards; }}
  #s4.in .sub {{ animation: line-in 0.6s 0.45s forwards; }}
</style>
</head>
<body>
  <div id="s1" class="scene in">
    <div class="h1">your innocent migration.</div>
  </div>
  <div id="s2" class="scene in">
    <div class="h1">what Postgres will actually do.</div>
    <div class="sql-wrap">{html.escape(SCARY_SQL)}</div>
  </div>
  <div id="s3" class="scene scene-term">
    <div class="term">
      <div class="prompt">$ pg-blast-radius analyse scary.sql --stats-file prod-stats.json</div>
      <div class="output">{output_block}</div>
    </div>
  </div>
  <div id="s4" class="scene in">
    <div class="h1">cargo install pg-blast-radius</div>
    <div class="sub">github.com/michaelmillar/pg-blast-radius</div>
  </div>
</body>
</html>
"""


def record(output_ansi):
    tmpdir = tempfile.mkdtemp(prefix="pgbr_scary_vid_")
    html_doc = build_html(output_ansi)
    html_path = os.path.join(tmpdir, "scary.html")
    with open(html_path, "w") as f:
        f.write(html_doc)

    with sync_playwright() as p:
        browser = p.chromium.launch()
        context = browser.new_context(
            viewport={"width": WIDTH, "height": HEIGHT},
            record_video_dir=tmpdir,
            record_video_size={"width": WIDTH, "height": HEIGHT},
        )
        page = context.new_page()
        page.goto(f"file://{html_path}")
        page.wait_for_timeout(20500)
        page.close()
        context.close()
        browser.close()

    webm_files = sorted(glob.glob(os.path.join(tmpdir, "*.webm")))
    if not webm_files:
        raise SystemExit("no webm produced")
    return webm_files[0], tmpdir


def compile_video(webm_path, tmpdir):
    output = ROOT / "scary.mp4"
    subprocess.run([
        "ffmpeg", "-y", "-i", webm_path,
        "-t", "20",
        "-vf", f"scale={WIDTH}:{HEIGHT}",
        "-c:v", "libx264", "-pix_fmt", "yuv420p",
        "-r", "30",
        "-movflags", "+faststart",
        str(output),
    ], check=True, capture_output=True)
    shutil.rmtree(tmpdir)
    print(f"wrote {output} ({output.stat().st_size // 1024} kB)")
    return output


def main():
    print("building release binary...")
    subprocess.run(["cargo", "build", "--release"], cwd=ROOT, check=True)
    print("running scary migration...")
    output = run_tool()
    print(f"  {len(output.splitlines())} output lines (truncating to {TRUNCATE_AT_LINE} + summary)")
    print("recording page with CSS animations...")
    webm, tmpdir = record(output)
    print(f"  captured {Path(webm).stat().st_size // 1024} kB webm")
    print("transcoding to mp4...")
    compile_video(webm, tmpdir)


if __name__ == "__main__":
    main()
