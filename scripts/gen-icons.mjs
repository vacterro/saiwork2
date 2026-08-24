// Generates SAIWORK2 app icons (32/128/256 PNG + Windows ICO) using only
// node built-ins (zlib). Run: node scripts/gen-icons.mjs
//
// Design: vintage parchment field, accent "S" monogram, dark border. The
// glyph is defined once on a 12x16 grid and scaled up.

import { deflateSync } from "node:zlib";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const OUT = join(ROOT, "apps", "desktop", "src-tauri", "icons");
mkdirSync(OUT, { recursive: true });

// 12x16 monogram (1 = ink "S" on parchment).
const GLYPH = [
  "111111111111",
  "111111111111",
  "110000000011",
  "100000000001",
  "100000000001",
  "100111111111",
  "100111111111",
  "100000000011",
  "100000000001",
  "111111111101",
  "111111111101",
  "100000000001",
  "100000000001",
  "100000000001",
  "111111111111",
  "111111111111",
];

const PARCHMENT = [232, 224, 208]; // #e8e0d0
const INK = [30, 27, 22]; // #1e1b16
const ACCENT = [201, 162, 39]; // #c9a227

function renderPixels(size) {
  const px = new Array(size * size * 4);
  const cell = size / 16;
  for (let y = 0; y < size; y++) {
    for (let x = 0; x < size; x++) {
      const gx = Math.min(11, Math.floor(x / cell));
      const gy = Math.min(15, Math.floor(y / cell));
      const on = GLYPH[gy][gx] === "1";
      const i = (y * size + x) * 4;
      let color = on ? INK : PARCHMENT;
      // accent bar across the top quarter of the glyph area
      if (gy === 2 && gx >= 3 && gx <= 8 && !on) color = ACCENT;
      px[i] = color[0];
      px[i + 1] = color[1];
      px[i + 2] = color[2];
      px[i + 3] = 255;
    }
  }
  return Buffer.from(px);
}

// ---- minimal PNG encoder ----

function crc32(buf) {
  let crc = 0xffffffff;
  for (const b of buf) {
    crc ^= b;
    for (let k = 0; k < 8; k++) {
      crc = crc & 1 ? (crc >>> 1) ^ 0xedb88320 : crc >>> 1;
    }
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function chunk(type, data) {
  const len = Buffer.alloc(4);
  len.writeUInt32BE(data.length);
  const typeBuf = Buffer.from(type, "ascii");
  const crc = Buffer.alloc(4);
  crc.writeUInt32BE(crc32(Buffer.concat([typeBuf, data])));
  return Buffer.concat([len, typeBuf, data, crc]);
}

function encodePng(rgba, width, height) {
  const sig = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(width, 0);
  ihdr.writeUInt32BE(height, 4);
  ihdr[8] = 8; // bit depth
  ihdr[9] = 6; // RGBA
  // raw scanlines with filter byte 0
  const stride = width * 4;
  const raw = Buffer.alloc((stride + 1) * height);
  for (let y = 0; y < height; y++) {
    raw[y * (stride + 1)] = 0;
    rgba.copy(raw, y * (stride + 1) + 1, y * stride, (y + 1) * stride);
  }
  return Buffer.concat([
    sig,
    chunk("IHDR", ihdr),
    chunk("IDAT", deflateSync(raw, { level: 9 })),
    chunk("IEND", Buffer.alloc(0)),
  ]);
}

function encodeIco(png, size) {
  const header = Buffer.alloc(6);
  header.writeUInt16LE(0, 0);
  header.writeUInt16LE(1, 2); // type: icon
  header.writeUInt16LE(1, 4); // count
  const entry = Buffer.alloc(16);
  entry[0] = size >= 256 ? 0 : size;
  entry[1] = size >= 256 ? 0 : size;
  entry.writeUInt16LE(1, 4); // planes
  entry.writeUInt16LE(32, 6); // bpp
  entry.writeUInt32LE(png.length, 8);
  entry.writeUInt32LE(22, 12); // offset
  return Buffer.concat([header, entry, png]);
}

for (const size of [32, 128, 256]) {
  const png = encodePng(renderPixels(size), size, size);
  writeFileSync(join(OUT, `${size}x${size}.png`), png);
  if (size === 128) {
    writeFileSync(join(OUT, "128x128@2x.png"), encodePng(renderPixels(256), 256, 256));
  }
}
writeFileSync(join(OUT, "icon.ico"), encodeIco(encodePng(renderPixels(256), 256, 256), 256));
writeFileSync(join(OUT, "icon.png"), encodePng(renderPixels(512), 512, 512));

console.log("icons written to", OUT);
