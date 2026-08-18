/**
 * Regenerates the raster brand assets from `docs/public/logo-mark.svg`:
 *
 *   docs/public/favicon.png   512x512, transparent — the PNG icon fallback
 *   docs/public/logo.png      1200x630 — the og:image / twitter:image card
 *
 * Run it after changing the mark:  node scripts/brand.js
 *
 * macOS only, and deliberately dependency-free: rendering goes through
 * `qlmanage`, the system QuickLook thumbnailer (WebKit under the hood), because
 * this package has no dependencies and a brand asset is not a reason to grow
 * one. On any other platform, render the two SVGs this script writes with
 * whatever you have (rsvg-convert, resvg, a headless browser) at those sizes.
 */

'use strict';

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const ROOT = path.join(__dirname, '..');
const PUBLIC = path.join(ROOT, 'docs', 'public');
const MARK = path.join(PUBLIC, 'logo-mark.svg');

const INK = '#1B1C1E';
const GREEN = '#5FA04E';
const GREEN_LIGHT = '#9BD08C';
const MUTED = '#8B8D90';

/** The mark's own drawing, without its <svg> wrapper, so it can be nested. */
const markBody = () => {
  const svg = fs.readFileSync(MARK, 'utf8');
  const open = svg.indexOf('>', svg.indexOf('<svg')) + 1;
  return svg.slice(open, svg.lastIndexOf('</svg>')).trim();
};

// 1200x630 is the size every social card scaler expects; anything else is
// re-cropped by the platform, usually through the wordmark.
//
// Authored on a 1200x1200 canvas with the card drawn in the middle band, because
// qlmanage's -s is a square bounding box: a 1200x630 source would be scaled to
// FIT that square (1.9x too big) before anything could be cropped. Square in,
// square out at 1:1, then the padding comes off.
const ogCard = (body) => `<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="1200" viewBox="0 0 1200 1200">
  <defs>
    <radialGradient id="glow" cx="18%" cy="50%" r="62%">
      <stop offset="0%" stop-color="${GREEN}" stop-opacity="0.22"/>
      <stop offset="100%" stop-color="${GREEN}" stop-opacity="0"/>
    </radialGradient>
  </defs>
  <rect width="1200" height="1200" fill="${INK}"/>
  <g transform="translate(0 285)">
    <rect width="1200" height="630" fill="url(#glow)"/>
    <rect y="614" width="1200" height="16" fill="${GREEN}"/>
    <g transform="translate(104 196) scale(7.4)">${body}</g>
    <g font-family="Helvetica Neue, Helvetica, Arial, sans-serif" fill="#FFFFFF">
      <text x="392" y="300" font-size="132" font-weight="700" letter-spacing="-4">wrpc</text>
      <text x="396" y="368" font-size="36" font-weight="500" fill="${GREEN_LIGHT}">Fast, zero-dependency WebSocket RPC</text>
      <text x="396" y="424" font-size="27" font-weight="400" fill="${MUTED}">Node.js and the browser · 0 runtime dependencies</text>
    </g>
  </g>
</svg>
`;

// qlmanage's -s is a SQUARE bounding box: a 1200x630 source comes back as a
// 1200x1200 canvas with the render centred in it. `crop` asks sips to cut the
// padding back off, centred, which is exactly where qlmanage put the content.
const render = (svg, size, out, crop = null) => {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'wrpc-brand-'));
  const source = path.join(temp, `${path.basename(out, '.png')}.svg`);
  fs.writeFileSync(source, svg);
  const result = spawnSync('qlmanage', ['-t', '-s', String(size), '-o', temp, source], { encoding: 'utf8' });
  const produced = path.join(temp, `${path.basename(source)}.png`);
  if (result.status !== 0 || !fs.existsSync(produced)) {
    throw new Error(`qlmanage failed for ${out}\n${result.stderr ?? ''}`);
  }
  fs.copyFileSync(produced, out);
  fs.rmSync(temp, { recursive: true, force: true });
  if (crop) {
    const [width, height] = crop;
    const cropped = spawnSync('sips', ['-c', String(height), String(width), out], { encoding: 'utf8' });
    if (cropped.status !== 0) throw new Error(`sips failed for ${out}\n${cropped.stderr ?? ''}`);
  }
  console.log(`${path.relative(ROOT, out)}  ${(fs.statSync(out).size / 1024).toFixed(1)} KB`);
};

const body = markBody();
render(fs.readFileSync(MARK, 'utf8'), 512, path.join(PUBLIC, 'favicon.png'));
render(ogCard(body), 1200, path.join(PUBLIC, 'logo.png'), [1200, 630]);
