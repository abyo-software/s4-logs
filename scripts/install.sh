#!/bin/sh
# S4 Logs installer — POSIX sh (not bash-only).
#
#   curl -fsSL https://raw.githubusercontent.com/abyo-software/s4-logs/main/scripts/install.sh | sh
#
# What it does:
#   - detects OS (linux only) + arch (x86_64 / aarch64)
#   - resolves the release tag (latest via GitHub API, or $S4LOGS_VERSION)
#   - downloads the matching tar.gz + .sha256, verifies the checksum
#   - extracts the s4logs binary into an install dir, chmod +x
#   - prints the installed version and a next-step hint
#
# Env overrides:
#   S4LOGS_VERSION       pin a release tag (e.g. v0.3.0). Default: latest.
#   S4LOGS_INSTALL_DIR   target dir. Default: ~/.local/bin
#
# Idempotent: re-running re-downloads and overwrites the binary in place.
set -eu

REPO="abyo-software/s4-logs"
# Targets we actually ship release artifacts for (must match release.yml).
# linux/x86_64 -> x86_64-unknown-linux-musl
# linux/aarch64 -> aarch64-unknown-linux-musl

err() {
  printf 'install.sh: %s\n' "$1" >&2
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || err "required command not found: $1"
}

# --- preflight: required tools -------------------------------------------------
need curl
need tar

# sha256: coreutils sha256sum (Linux) or BSD/macOS shasum -a 256.
if command -v sha256sum >/dev/null 2>&1; then
  SHA_CMD="sha256sum"
elif command -v shasum >/dev/null 2>&1; then
  SHA_CMD="shasum -a 256"
else
  err "need sha256sum or shasum to verify the download"
fi

# --- detect OS -----------------------------------------------------------------
OS="$(uname -s)"
case "$OS" in
  Linux) ;;
  Darwin)
    err "macOS prebuilt binaries are not shipped yet. Install from source:
  cargo install --git https://github.com/${REPO} s4logs-cli
or build the repo directly (see README → Development)."
    ;;
  *)
    err "unsupported OS: ${OS}. Build from source:
  cargo install --git https://github.com/${REPO} s4logs-cli"
    ;;
esac

# --- detect arch ---------------------------------------------------------------
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64 | amd64) TARGET="x86_64-unknown-linux-musl" ;;
  aarch64 | arm64) TARGET="aarch64-unknown-linux-musl" ;;
  *)
    err "unsupported architecture: ${ARCH} (shipped: x86_64, aarch64). Build from source:
  cargo install --git https://github.com/${REPO} s4logs-cli"
    ;;
esac

# --- resolve version -----------------------------------------------------------
VERSION="${S4LOGS_VERSION:-}"
if [ -z "$VERSION" ]; then
  # GitHub API: latest published release tag. Parse tag_name without jq.
  API_URL="https://api.github.com/repos/${REPO}/releases/latest"
  VERSION="$(
    curl -fsSL "$API_URL" \
      | grep '"tag_name"' \
      | head -n 1 \
      | sed -e 's/.*"tag_name"[[:space:]]*:[[:space:]]*"//' -e 's/".*//'
  )" || err "failed to query latest release from ${API_URL}"
  [ -n "$VERSION" ] || err "could not determine latest release tag (is there a published release?). Pin one with S4LOGS_VERSION=vX.Y.Z."
fi

# --- install dir ---------------------------------------------------------------
INSTALL_DIR="${S4LOGS_INSTALL_DIR:-${HOME}/.local/bin}"

# --- download + verify ---------------------------------------------------------
STAGE="s4logs-${VERSION}-${TARGET}"
TARBALL="${STAGE}.tar.gz"
BASE_URL="https://github.com/${REPO}/releases/download/${VERSION}"

TMP="$(mktemp -d 2>/dev/null || mktemp -d -t s4logs)"
trap 'rm -rf "$TMP"' EXIT INT TERM

printf 'install.sh: downloading %s (%s)\n' "$VERSION" "$TARGET" >&2
curl -fsSL -o "${TMP}/${TARBALL}" "${BASE_URL}/${TARBALL}" \
  || err "download failed: ${BASE_URL}/${TARBALL} (is ${VERSION} a real release with ${TARGET} assets?)"
curl -fsSL -o "${TMP}/${TARBALL}.sha256" "${BASE_URL}/${TARBALL}.sha256" \
  || err "checksum download failed: ${BASE_URL}/${TARBALL}.sha256"

# The .sha256 file is "<hash>  <name>"; the name inside is the build-time
# path, so compare against the hash field only (don't trust the embedded name).
EXPECTED="$(awk '{print $1}' "${TMP}/${TARBALL}.sha256")"
[ -n "$EXPECTED" ] || err "empty checksum file"
ACTUAL="$(cd "$TMP" && $SHA_CMD "$TARBALL" | awk '{print $1}')"
if [ "$EXPECTED" != "$ACTUAL" ]; then
  err "checksum mismatch for ${TARBALL}
  expected: ${EXPECTED}
  actual:   ${ACTUAL}"
fi
printf 'install.sh: checksum OK\n' >&2

# --- extract -------------------------------------------------------------------
tar -xzf "${TMP}/${TARBALL}" -C "$TMP" \
  || err "failed to extract ${TARBALL}"

SRC_BIN="${TMP}/${STAGE}/s4logs"
[ -f "$SRC_BIN" ] || err "archive did not contain the expected s4logs binary at ${STAGE}/s4logs"

mkdir -p "$INSTALL_DIR" || err "cannot create install dir: ${INSTALL_DIR}"
DEST="${INSTALL_DIR}/s4logs"
# cp then chmod (overwrites an existing install — idempotent).
cp "$SRC_BIN" "$DEST" || err "failed to install to ${DEST} (permission? set S4LOGS_INSTALL_DIR)"
chmod +x "$DEST"

# --- report --------------------------------------------------------------------
printf 'install.sh: installed s4logs -> %s\n' "$DEST" >&2
INSTALLED_VERSION="$("$DEST" --version 2>/dev/null || echo 's4logs (version check failed)')"
printf '\n  %s\n\n' "$INSTALLED_VERSION"

# PATH hint if the install dir isn't on PATH.
case ":${PATH}:" in
  *":${INSTALL_DIR}:"*) ;;
  *)
    printf 'Note: %s is not on your PATH. Add it, e.g.:\n' "$INSTALL_DIR" >&2
    # shellcheck disable=SC2016  # literal $PATH is intentional in the hint.
    printf '  export PATH="%s:$PATH"\n\n' "$INSTALL_DIR" >&2
    ;;
esac

printf 'Next: see your projected savings (read-only, no AWS charges):\n' >&2
printf '  s4logs plan --all\n' >&2
