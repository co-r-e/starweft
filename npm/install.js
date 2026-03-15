#!/usr/bin/env node
"use strict";

const { execSync } = require("child_process");
const fs = require("fs");
const path = require("path");
const https = require("https");
const { createHash } = require("crypto");

const REPO = "co-r-e/starweft";
const VERSION = require("./package.json").version;
const TAG = `v${VERSION}`;

const PLATFORM_MAP = {
  darwin: { x64: "x86_64-apple-darwin", arm64: "aarch64-apple-darwin" },
  linux: { x64: "x86_64-unknown-linux-gnu", arm64: "aarch64-unknown-linux-gnu" },
  win32: { x64: "x86_64-pc-windows-msvc" },
};

function getTarget() {
  const platform = PLATFORM_MAP[process.platform];
  if (!platform) {
    throw new Error(`Unsupported platform: ${process.platform}`);
  }
  const target = platform[process.arch];
  if (!target) {
    throw new Error(`Unsupported architecture: ${process.arch} on ${process.platform}`);
  }
  return target;
}

function fetch(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { "User-Agent": "starweft-npm" } }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetch(res.headers.location).then(resolve, reject);
      }
      if (res.statusCode !== 200) {
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }
      const chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => resolve(Buffer.concat(chunks)));
      res.on("error", reject);
    }).on("error", reject);
  });
}

async function main() {
  const target = getTarget();
  const isWindows = process.platform === "win32";
  const ext = isWindows ? "zip" : "tar.gz";
  const archive = `starweft-${TAG}-${target}.${ext}`;
  const baseUrl = `https://github.com/${REPO}/releases/download/${TAG}`;

  console.log(`Downloading starweft ${TAG} for ${target}...`);

  const [binary, sha256Text] = await Promise.all([
    fetch(`${baseUrl}/${archive}`),
    fetch(`${baseUrl}/${archive}.sha256`),
  ]);

  const expectedHash = sha256Text.toString("utf8").trim().split(/\s+/)[0];
  const actualHash = createHash("sha256").update(binary).digest("hex");

  if (actualHash !== expectedHash) {
    throw new Error(`Checksum mismatch: expected ${expectedHash}, got ${actualHash}`);
  }
  console.log("Checksum verified.");

  const binDir = path.join(__dirname, "bin");
  const tmpDir = path.join(__dirname, ".tmp");
  fs.mkdirSync(tmpDir, { recursive: true });

  const archivePath = path.join(tmpDir, archive);
  fs.writeFileSync(archivePath, binary);

  if (isWindows) {
    execSync(`powershell -Command "Expand-Archive -Force '${archivePath}' '${tmpDir}'"`, { stdio: "inherit" });
  } else {
    execSync(`tar xzf "${archivePath}" -C "${tmpDir}"`, { stdio: "inherit" });
  }

  const extractedName = isWindows ? "starweft.exe" : "starweft";
  const installedName = isWindows ? "starweft.exe" : "starweft-bin";
  const src = path.join(tmpDir, extractedName);
  const dest = path.join(binDir, installedName);

  fs.mkdirSync(binDir, { recursive: true });
  fs.copyFileSync(src, dest);

  if (!isWindows) {
    fs.chmodSync(dest, 0o755);
  }

  fs.rmSync(tmpDir, { recursive: true, force: true });
  console.log(`starweft ${TAG} installed successfully!`);
}

main().catch((err) => {
  console.error(`Failed to install starweft: ${err.message}`);
  process.exit(1);
});
