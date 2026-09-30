// Generates the hand-drawn (Excalidraw-style) system design diagram from one layout definition:
//   docs/system-design.excalidraw  -> open/edit it at https://excalidraw.com (File > Open)
//   docs/system-design.svg / .png  -> drawn with rough.js (the library Excalidraw uses) and the Virgil font
//
// Usage: npm run docs:diagram      (CHROME=/path/to/chrome to use another Chrome for the PNG)
import { execFileSync } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import rough from 'roughjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const DOCS = join(HERE, '..');
const WIDTH = 1800;
const HEIGHT = 1160;

// Excalidraw's palette
const INK = '#1e1e1e';
const BLUE = '#1971c2';
const GREEN = '#2f9e44';
const VIOLET = '#6741d9';
const ORANGE = '#e8590c';
const FILL = { blue: '#a5d8ff', lightBlue: '#d0ebff', yellow: '#ffec99', green: '#b2f2bb', lightGreen: '#d3f9d8', violet: '#d0bfff', orange: '#ffd8a8', white: '#ffffff' };

// ---------------------------------------------------------------------------------------------
// Layout: shapes (rect / ellipse / line), text, arrows. Coordinates in px on a 1800x1160 canvas.
// ---------------------------------------------------------------------------------------------
const elements = [];
const rect = (x, y, width, height, backgroundColor, extra = {}) =>
  elements.push({ type: 'rectangle', x, y, width, height, backgroundColor, ...extra });
const ellipse = (x, y, width, height, backgroundColor, extra = {}) =>
  elements.push({ type: 'ellipse', x, y, width, height, backgroundColor, ...extra });
const line = (points, extra = {}) => elements.push({ type: 'line', points, ...extra });
const text = (x, y, value, fontSize = 20, extra = {}) => elements.push({ type: 'text', x, y, text: value, fontSize, ...extra });
const arrow = (points, extra = {}) => elements.push({ type: 'arrow', points, ...extra });
// A numbered circle, like the steps on a whiteboard
const step = (cx, cy, n, color) => {
  ellipse(cx - 17, cy - 17, 34, 34, color, { strokeColor: color, fillStyle: 'solid' });
  text(cx - 6, cy - 14, String(n), 22, { strokeColor: '#ffffff' });
};
const sticky = (x, y, title, body, angle = 0) => {
  const height = Math.max(118, 56 + body.split('\n').length * 21); // grows with its lines
  rect(x, y, 320, height, FILL.yellow, { angle, roundness: false, strokeColor: '#e0a800' });
  text(x + 16, y + 12, title, 22, { angle });
  text(x + 16, y + 46, body, 17, { angle });
};

// Title
text(60, 36, 'Invoices ⇄ QuickBooks Online', 40);
text(60, 90, 'two-way sync · PostgreSQL is the queue (transactional outbox) · no message broker', 20, { strokeColor: '#495057' });

// Client
rect(60, 205, 260, 110, FILL.white);
text(84, 222, 'Client', 28);
text(84, 262, 'curl · client.http · apps', 18);
text(84, 288, 'Idempotency-Key required', 16, { strokeColor: '#495057' });

// API
rect(460, 165, 420, 205, FILL.lightBlue, { strokeColor: BLUE });
text(486, 182, 'API  (Express 5)', 30);
text(486, 230, 'REST: invoices · payments · /sync', 19);
text(486, 262, 'POST /quickbooks/webhooks', 19);
text(486, 288, '   HMAC verified, 200 after commit', 17, { strokeColor: '#495057' });
text(486, 320, 'OAuth connect → tokens in Postgres', 19);

// PostgreSQL (a cylinder: bottom ellipse, body without top/bottom border, side lines, top ellipse)
const pg = { x: 460, y: 470, w: 420, h: 280, cap: 56 };
ellipse(pg.x, pg.y + pg.h - pg.cap / 2, pg.w, pg.cap, FILL.yellow, { strokeColor: ORANGE });
rect(pg.x, pg.y, pg.w, pg.h, FILL.yellow, { strokeColor: 'transparent', roundness: false });
line([[pg.x, pg.y], [pg.x, pg.y + pg.h]], { strokeColor: ORANGE });
line([[pg.x + pg.w, pg.y], [pg.x + pg.w, pg.y + pg.h]], { strokeColor: ORANGE });
ellipse(pg.x, pg.y - pg.cap / 2, pg.w, pg.cap, FILL.yellow, { strokeColor: ORANGE });
text(pg.x + 26, pg.y + 42, 'PostgreSQL', 30);
text(pg.x + 26, pg.y + 92, 'invoices · invoice_payments', 19);
text(pg.x + 26, pg.y + 124, 'sync_jobs   ← the queue', 19, { strokeColor: ORANGE });
text(pg.x + 26, pg.y + 156, 'sync_events (webhook log)', 19);
text(pg.x + 26, pg.y + 188, 'accounts · connection (tokens)', 19);
text(pg.x + 26, pg.y + 226, 'PENDING → PROCESSING → COMPLETED', 16, { strokeColor: '#495057' });

// Worker
rect(460, 860, 420, 190, FILL.violet, { strokeColor: VIOLET });
text(486, 877, 'Worker(s)', 30);
text(486, 925, 'wakes on NOTIFY (ms) · polls 5 s', 19);
text(486, 957, 'claim: SKIP LOCKED + lease', 19);
text(486, 989, 'push · fetch · reconcile (CDC)', 19);
text(486, 1017, 'consistency check · every 24 h', 19, { strokeColor: VIOLET });

// QuickBooks
rect(1200, 165, 440, 520, FILL.lightGreen, { strokeColor: GREEN });
text(1226, 182, 'QuickBooks Online', 32);
text(1226, 226, 'system of record for payments', 17, { strokeColor: '#495057' });
rect(1230, 270, 380, 100, FILL.green, { strokeColor: GREEN });
text(1252, 285, 'Accounting API', 24);
text(1252, 322, 'invoices · payments · SyncToken', 17);
rect(1230, 400, 380, 100, FILL.green, { strokeColor: GREEN });
text(1252, 415, 'Webhooks', 24);
text(1252, 452, 'signed · may repeat / arrive late', 17);
rect(1230, 530, 380, 100, FILL.green, { strokeColor: GREEN });
text(1252, 545, 'CDC', 24);
text(1252, 582, 'what changed since the cursor', 17);

// Arrows (numbered)
arrow([[320, 260], [456, 260]], { strokeColor: INK });
step(388, 228, 1, INK);
text(334, 280, 'write', 17);

arrow([[670, 372], [670, 440]], { strokeColor: BLUE });
step(640, 406, 2, BLUE);
text(690, 386, 'change + job + NOTIFY', 18, { strokeColor: BLUE });
text(690, 410, 'in ONE transaction', 18, { strokeColor: BLUE });

arrow([[610, 778], [610, 856]], { strokeColor: VIOLET });
step(580, 816, 3, VIOLET);
text(630, 786, 'NOTIFY or 5 s poll', 18, { strokeColor: VIOLET });
text(630, 810, '→ claim job', 18, { strokeColor: VIOLET });
arrow([[845, 856], [845, 782]], { strokeColor: VIOLET, strokeStyle: 'dashed' });
text(860, 808, 'save result', 17, { strokeColor: VIOLET });

arrow([[884, 900], [1060, 820], [1226, 330]], { strokeColor: GREEN });
step(1100, 700, 4, GREEN);
text(905, 912, 'push: requestid · SyncToken', 18, { strokeColor: GREEN });

arrow([[1226, 452], [1040, 420], [884, 300]], { strokeColor: GREEN });
step(1060, 378, 5, GREEN);
text(935, 452, 'webhook → event + job', 18, { strokeColor: GREEN });

arrow([[1330, 634], [1290, 960], [884, 1010]], { strokeColor: GREEN, strokeStyle: 'dashed' });
step(1318, 800, 6, GREEN);
text(960, 1030, 'fetch current entity · CDC', 18, { strokeColor: GREEN });

// Sticky notes
sticky(60, 400, 'Nothing lost', 'change + job commit together\nwebhook 200 only after saving\nlost NOTIFY → 5 s poll', -0.02);
sticky(60, 560, 'No duplicates', 'Idempotency-Key required (400)\nrequestid · event_key\nUNKNOWN → reconcile', 0.015);
sticky(60, 720, 'No silent overwrite', 'claim token · 3-way conflicts\nSyncToken only from own writes\nchanged mid-sync: still pushed\ntaxed in QBO: edit it there', -0.01);
sticky(60, 880, 'Payments', 'partial · paid · delete\nmirrored from QuickBooks\nonly a local "paid" pays', 0.02);
sticky(1420, 712, 'Failures', 'retries + jitter · lease recovery\n429 · 5xx · timeouts classified\nexpired auth → waits to reconnect', 0.015);
sticky(1420, 852, 'Consistency check', 'every 24 h + on demand\nre-imports rows deleted by hand\ndeletes only if a GET confirms', -0.01);
sticky(1420, 1000, 'Observability', '/sync/stats · /sync/jobs\nJSON logs · 161 tests\nrace tests · 5 also in sandbox', -0.015);

// ---------------------------------------------------------------------------------------------
// Excalidraw file
// ---------------------------------------------------------------------------------------------
const measure = (value, fontSize) => {
  const lines = value.split('\n');
  return { width: Math.max(...lines.map((l) => l.length)) * fontSize * 0.6, height: lines.length * fontSize * 1.25 };
};

const excalidrawElements = elements.map((el, i) => {
  const base = {
    id: `el-${i}`,
    type: el.type,
    x: el.x ?? 0,
    y: el.y ?? 0,
    width: el.width ?? 0,
    height: el.height ?? 0,
    angle: el.angle ?? 0,
    strokeColor: el.strokeColor ?? INK,
    backgroundColor: el.backgroundColor ?? 'transparent',
    fillStyle: el.fillStyle ?? 'solid',
    strokeWidth: 2,
    strokeStyle: el.strokeStyle ?? 'solid',
    roughness: 1,
    opacity: 100,
    groupIds: [],
    frameId: null,
    roundness: el.type === 'rectangle' && el.roundness !== false ? { type: 3 } : el.type === 'arrow' && el.points.length > 2 ? { type: 2 } : null,
    seed: 1000 + i,
    version: 1,
    versionNonce: 2000 + i,
    isDeleted: false,
    boundElements: null,
    updated: 1790000000000,
    link: null,
    locked: false,
  };
  if (el.type === 'text') {
    const size = measure(el.text, el.fontSize);
    return {
      ...base, ...size, backgroundColor: 'transparent',
      text: el.text, originalText: el.text, fontSize: el.fontSize, fontFamily: 1,
      textAlign: 'left', verticalAlign: 'top', containerId: null, lineHeight: 1.25, autoResize: true,
    };
  }
  if (el.type === 'line' || el.type === 'arrow') {
    const [x0, y0] = el.points[0];
    const points = el.points.map(([x, y]) => [x - x0, y - y0]);
    const xs = points.map((p) => p[0]);
    const ys = points.map((p) => p[1]);
    return {
      ...base, x: x0, y: y0, width: Math.max(...xs) - Math.min(...xs), height: Math.max(...ys) - Math.min(...ys),
      backgroundColor: 'transparent', points, lastCommittedPoint: null, startBinding: null, endBinding: null,
      startArrowhead: null, endArrowhead: el.type === 'arrow' ? 'arrow' : null,
    };
  }
  return base;
});

writeFileSync(
  join(DOCS, 'system-design.excalidraw'),
  JSON.stringify({
    type: 'excalidraw',
    version: 2,
    source: 'https://excalidraw.com',
    elements: excalidrawElements,
    appState: { viewBackgroundColor: '#ffffff', gridSize: null },
    files: {},
  }, null, 2),
);

// ---------------------------------------------------------------------------------------------
// SVG with rough.js (same elements, same seeds)
// ---------------------------------------------------------------------------------------------
const generator = rough.generator();
const esc = (s) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

const roundedRect = (x, y, w, h, r) =>
  `M${x + r},${y} L${x + w - r},${y} Q${x + w},${y} ${x + w},${y + r} L${x + w},${y + h - r} Q${x + w},${y + h} ${x + w - r},${y + h} ` +
  `L${x + r},${y + h} Q${x},${y + h} ${x},${y + h - r} L${x},${y + r} Q${x},${y} ${x + r},${y} Z`;

const toSvg = (drawable) =>
  generator.toPaths(drawable).map((p) =>
    `<path d="${p.d}" stroke="${p.stroke}" stroke-width="${p.strokeWidth}" fill="${p.fill ?? 'none'}"${drawable.options.strokeLineDash ? ` stroke-dasharray="${drawable.options.strokeLineDash.join(' ')}"` : ''}/>`,
  ).join('');

const rotate = (el, svg) => {
  if (!el.angle) return svg;
  const cx = el.x + (el.width ?? measure(el.text ?? '', el.fontSize ?? 20).width) / 2;
  const cy = el.y + (el.height ?? measure(el.text ?? '', el.fontSize ?? 20).height) / 2;
  return `<g transform="rotate(${(el.angle * 180) / Math.PI} ${cx} ${cy})">${svg}</g>`;
};

const shapes = elements.map((el, i) => {
  const stroke = el.strokeColor ?? INK;
  const options = {
    seed: 1000 + i,
    roughness: 1,
    stroke: stroke === 'transparent' ? 'none' : stroke,
    strokeWidth: 2,
    fill: el.backgroundColor,
    fillStyle: el.fillStyle ?? 'solid',
    strokeLineDash: el.strokeStyle === 'dashed' ? [10, 8] : undefined,
    preserveVertices: true,
  };
  if (el.type === 'rectangle') {
    const r = el.roundness === false ? 0 : Math.min(18, el.width / 6, el.height / 6);
    const d = r ? generator.path(roundedRect(el.x, el.y, el.width, el.height, r), options) : generator.rectangle(el.x, el.y, el.width, el.height, options);
    return rotate(el, toSvg(d));
  }
  if (el.type === 'ellipse') return toSvg(generator.ellipse(el.x + el.width / 2, el.y + el.height / 2, el.width, el.height, options));
  if (el.type === 'line') return toSvg(generator.linearPath(el.points, { ...options, fill: undefined }));
  if (el.type === 'arrow') {
    const body = el.points.length > 2 ? generator.curve(el.points, { ...options, fill: undefined }) : generator.linearPath(el.points, { ...options, fill: undefined });
    const [[x1, y1], [x2, y2]] = el.points.slice(-2);
    const angle = Math.atan2(y2 - y1, x2 - x1);
    const head = (a) => [x2 - 18 * Math.cos(angle - a), y2 - 18 * Math.sin(angle - a)];
    const heads = [0.45, -0.45].map((a) => generator.linearPath([head(a), [x2, y2]], { ...options, fill: undefined, strokeLineDash: undefined }));
    return toSvg(body) + heads.map(toSvg).join('');
  }
  // text
  const color = el.strokeColor ?? INK;
  const lines = el.text.split('\n').map((l, n) =>
    `<tspan x="${el.x}" dy="${n === 0 ? el.fontSize : el.fontSize * 1.25}">${esc(l)}</tspan>`).join('');
  return rotate(el, `<text x="${el.x}" y="${el.y}" font-family="Virgil" font-size="${el.fontSize}" fill="${color}" xml:space="preserve">${lines}</text>`);
});

const font = readFileSync(join(HERE, 'Virgil.woff2')).toString('base64');
const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${WIDTH}" height="${HEIGHT}" viewBox="0 0 ${WIDTH} ${HEIGHT}">
<defs><style>@font-face { font-family: 'Virgil'; src: url(data:font/woff2;base64,${font}) format('woff2'); }</style></defs>
<rect width="${WIDTH}" height="${HEIGHT}" fill="#ffffff"/>
${shapes.join('\n')}
</svg>`;
writeFileSync(join(DOCS, 'system-design.svg'), svg);

// PNG through headless Chrome
const chrome = process.env.CHROME ?? '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
execFileSync(chrome, [
  '--headless=new', '--disable-gpu', '--hide-scrollbars', `--window-size=${WIDTH},${HEIGHT}`,
  `--screenshot=${join(DOCS, 'system-design.png')}`, `file://${join(DOCS, 'system-design.svg')}`,
], { stdio: 'ignore' });

console.log(`Done: docs/system-design.excalidraw (${excalidrawElements.length} elements), docs/system-design.svg, docs/system-design.png`);
