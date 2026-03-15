#!/bin/sh
# Starweft installer — downloads the latest release binary for your platform.
# Usage: curl -fsSL https://raw.githubusercontent.com/co-r-e/starweft/main/scripts/install.sh | sh
set -eu

REPO="co-r-e/starweft"
INSTALL_DIR="${STARWEFT_INSTALL_DIR:-/usr/local/bin}"

# Detect download tool once at startup
if command -v curl >/dev/null 2>&1; then
  FETCH="curl"
elif command -v wget >/dev/null 2>&1; then
  FETCH="wget"
else
  echo "Error: curl or wget is required" >&2
  exit 1
fi

detect_platform() {
  os="$(uname -s)"
  arch="$(uname -m)"

  case "$os" in
    Linux)  os="unknown-linux-gnu" ;;
    Darwin) os="apple-darwin" ;;
    *)      echo "Error: unsupported OS: $os" >&2; exit 1 ;;
  esac

  case "$arch" in
    x86_64|amd64)  arch="x86_64" ;;
    aarch64|arm64) arch="aarch64" ;;
    *)             echo "Error: unsupported architecture: $arch" >&2; exit 1 ;;
  esac

  echo "${arch}-${os}"
}

get_latest_version() {
  if [ "$FETCH" = "curl" ]; then
    curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p'
  else
    wget -qO- "https://api.github.com/repos/${REPO}/releases/latest" | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p'
  fi
}

download() {
  url="$1"
  dest="$2"
  if [ "$FETCH" = "curl" ]; then
    curl -fsSL --connect-timeout 30 --max-time 300 -o "$dest" "$url"
  else
    wget -qO "$dest" --timeout=30 "$url"
  fi
}

main() {
  platform="$(detect_platform)"
  version="$(get_latest_version)"

  if [ -z "$version" ]; then
    echo "Error: could not determine latest version" >&2
    exit 1
  fi

  archive="starweft-${version}-${platform}.tar.gz"
  url="https://github.com/${REPO}/releases/download/${version}/${archive}"
  sha_url="${url}.sha256"

  tmpdir="$(mktemp -d)"
  trap 'rm -rf "$tmpdir"' EXIT

  echo "Downloading starweft ${version} for ${platform}..."
  download "$url" "${tmpdir}/${archive}"
  download "$sha_url" "${tmpdir}/${archive}.sha256"

  echo "Verifying checksum..."
  expected_hash="$(awk '{print $1}' "${tmpdir}/${archive}.sha256")"
  if command -v sha256sum >/dev/null 2>&1; then
    actual_hash="$(sha256sum "${tmpdir}/${archive}" | awk '{print $1}')"
  elif command -v shasum >/dev/null 2>&1; then
    actual_hash="$(shasum -a 256 "${tmpdir}/${archive}" | awk '{print $1}')"
  else
    echo "Error: sha256sum or shasum is required for checksum verification" >&2
    exit 1
  fi
  if [ "$actual_hash" != "$expected_hash" ]; then
    echo "Error: checksum mismatch" >&2
    echo "  expected: $expected_hash" >&2
    echo "  actual:   $actual_hash" >&2
    exit 1
  fi

  echo "Extracting..."
  cd "$tmpdir"
  tar xzf "$archive"

  echo "Installing to ${INSTALL_DIR}..."
  if [ -w "$INSTALL_DIR" ]; then
    mv starweft "$INSTALL_DIR/"
  else
    sudo mv starweft "$INSTALL_DIR/"
  fi

  echo "starweft ${version} installed successfully!"
  echo "Run 'starweft --version' to verify."
}

main
