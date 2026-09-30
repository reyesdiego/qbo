#!/usr/bin/env bash
# Builds docs/guia-video-entrevista.pdf from docs/guia-video-entrevista.md. First regenerates both
# diagrams it includes, so they're always up to date: docs/system-design.png (Excalidraw style, from
# docs/system-design/generate.mjs) and docs/architecture.png (from docs/architecture.svg).
#
# Usage:  docs/build-pdf.sh
# Needs:  Node 22 (npx; `marked` is fetched on the fly, not added to the project), python3, Google Chrome.
#         Another Chrome binary can be given with CHROME=/path/to/chrome.
set -euo pipefail

DOCS="$(cd "$(dirname "$0")" && pwd)"
CHROME="${CHROME:-/Applications/Google Chrome.app/Contents/MacOS/Google Chrome}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

if [ ! -x "$CHROME" ]; then
  echo "Google Chrome not found at: $CHROME (set CHROME=/path/to/chrome)" >&2
  exit 1
fi

echo "1/3 Diagrams: system-design (Excalidraw style) and architecture.svg -> PNG"
CHROME="$CHROME" node "$DOCS/system-design/generate.mjs" >/dev/null
"$CHROME" --headless=new --disable-gpu --hide-scrollbars --window-size=1600,1100 \
  --screenshot="$DOCS/architecture.png" "file://$DOCS/architecture.svg" >/dev/null 2>&1

echo "2/3 Guide: Markdown -> HTML"
npx -y marked@15 --gfm -i "$DOCS/guia-video-entrevista.md" -o "$WORK/body.html"

python3 - "$WORK/body.html" "$WORK/guia.html" "$DOCS/system-design.png" "$DOCS/architecture.png" <<'PY'
import sys
body_path, out_path, overview, detailed = sys.argv[1:5]
body = open(body_path, encoding='utf-8').read()

# The diagrams go after the explanation of their numbered arrows (section 1), each on its own landscape
# page so they're as large as possible: the Excalidraw-style overview, then the detailed one.
anchor = 'Remarcar las garantías'
end = body.find('</p>', body.find(anchor))
if anchor not in body or end == -1:
    sys.exit('Could not find where to place the diagrams (the paragraph starting "Remarcar las garantías")')
end += len('</p>')
figures = (
    f'\n<figure class="diagram-page"><img src="file://{overview}" alt="System design">'
    '<figcaption>Vista general: docs/system-design.png (editable: docs/system-design.excalidraw)</figcaption></figure>'
    f'\n<figure class="diagram-page"><img src="file://{detailed}" alt="Arquitectura detallada">'
    '<figcaption>Detalle: docs/architecture.png</figcaption></figure>'
)
body = body[:end] + figures + body[end:]

css = """
@page { size: A4; margin: 16mm 14mm 18mm 14mm; }
* { box-sizing: border-box; }
body { font-family: -apple-system, 'Segoe UI', Helvetica, Arial, sans-serif; font-size: 10.5pt; line-height: 1.5; color: #1e293b; }
h1 { font-size: 22pt; color: #0f172a; border-bottom: 3px solid #2563eb; padding-bottom: 6px; margin: 0 0 10px; }
h2 { font-size: 15pt; color: #0f172a; margin: 22px 0 8px; padding-top: 4px; border-bottom: 1px solid #e2e8f0; page-break-after: avoid; }
h3 { font-size: 12pt; color: #1d4ed8; margin: 16px 0 6px; page-break-after: avoid; }
p, li { orphans: 3; widows: 3; }
blockquote { margin: 8px 0; padding: 8px 12px; background: #eff6ff; border-left: 4px solid #2563eb; color: #1e3a8a; border-radius: 4px; }
blockquote p { margin: 0; }
code { font-family: 'SF Mono', Menlo, Consolas, monospace; font-size: 9pt; background: #f1f5f9; padding: 1px 4px; border-radius: 3px; }
pre { background: #0f172a; color: #e2e8f0; padding: 10px 12px; border-radius: 6px; font-size: 8.6pt; line-height: 1.45; white-space: pre-wrap; page-break-inside: avoid; }
pre code { background: none; color: inherit; padding: 0; font-size: inherit; }
table { width: 100%; border-collapse: collapse; margin: 8px 0 12px; font-size: 9pt; }
tr { page-break-inside: avoid; }
th { background: #f1f5f9; text-align: left; font-weight: 700; }
th, td { border: 1px solid #cbd5e1; padding: 5px 7px; vertical-align: top; }
td code { font-size: 8.3pt; }
ul, ol { padding-left: 20px; }
li { margin: 2px 0; }
input[type=checkbox] { margin-right: 6px; transform: translateY(1px); }
ul:has(input[type=checkbox]) { list-style: none; padding-left: 4px; }
hr { border: none; border-top: 1px solid #e2e8f0; margin: 18px 0; }
figure { margin: 10px 0 14px; text-align: center; page-break-inside: avoid; }
figure img { width: 100%; border: 1px solid #e2e8f0; border-radius: 6px; }
/* Each diagram on its own landscape page, as large as it fits */
@page diagram { size: A4 landscape; margin: 10mm; }
figure.diagram-page { page: diagram; break-before: page; break-after: page; margin: 0; }
figure.diagram-page img { width: auto; max-width: 100%; max-height: 180mm; }
figcaption { font-size: 8.5pt; color: #64748b; margin-top: 4px; }
strong { color: #0f172a; }
"""
html = ('<!doctype html><html lang="es"><head><meta charset="utf-8">'
        '<title>Guía para el video de la entrevista</title>'
        f'<style>{css}</style></head><body>{body}</body></html>')
open(out_path, 'w', encoding='utf-8').write(html)
PY

echo "3/3 PDF: HTML -> guia-video-entrevista.pdf"
"$CHROME" --headless=new --disable-gpu --allow-file-access-from-files --no-pdf-header-footer \
  --print-to-pdf="$DOCS/guia-video-entrevista.pdf" "file://$WORK/guia.html" >/dev/null 2>&1

PAGES=$(python3 -c "import re,sys; print(len(re.findall(rb'/Type\s*/Page[^s]', open(sys.argv[1],'rb').read())))" "$DOCS/guia-video-entrevista.pdf")
echo "Done: docs/guia-video-entrevista.pdf ($PAGES pages), docs/system-design.png, docs/architecture.png"
