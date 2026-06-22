#!/usr/bin/env bash
set -euo pipefail

REPO="${OXIDE_REPO:-mrfelipemartins/oxide}"
VERSION="${OXIDE_VERSION:-latest}"
ARCH="${OXIDE_ARCH:-$(uname -m)}"
OS="$(uname -s)"

PREFIX="${OXIDE_PREFIX:-/usr/local}"
BIN_DIR="${OXIDE_BIN_DIR:-$PREFIX/bin}"
CONFIG_DIR="${OXIDE_CONFIG_DIR:-/etc/oxide}"
DATA_DIR="${OXIDE_DATA_DIR:-/var/lib/oxide}"
SERVICE_NAME="${OXIDE_SERVICE_NAME:-oxide}"
USER="${OXIDE_USER:-oxide}"
GROUP="${OXIDE_GROUP:-oxide}"
LISTEN_ADDR="${OXIDE_LISTEN_ADDR:-127.0.0.1:7379}"
FSYNC_MODE="${OXIDE_WAL_FSYNC_MODE:-batch}"
TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

usage() {
  cat <<'USAGE'
Install Oxide and register it as a systemd service.

Usage:
  curl -fsSL https://raw.githubusercontent.com/mrfelipemartins/oxide/main/scripts/install.sh | sudo bash

Environment:
  OXIDE_REPO=owner/repo             GitHub repository. Default: mrfelipemartins/oxide
  OXIDE_VERSION=latest             Release version or "latest". Default: latest
  OXIDE_ARCH=x86_64                Release architecture. Default: uname -m
  OXIDE_PREFIX=/usr/local          Install prefix. Default: /usr/local
  OXIDE_CONFIG_DIR=/etc/oxide      Config directory. Default: /etc/oxide
  OXIDE_DATA_DIR=/var/lib/oxide    Data directory. Default: /var/lib/oxide
  OXIDE_SERVICE_NAME=oxide         systemd service name. Default: oxide
  OXIDE_USER=oxide                 Service user. Default: oxide
  OXIDE_GROUP=oxide                Service group. Default: oxide
  OXIDE_LISTEN_ADDR=127.0.0.1:7379 Listen address written to config.
  OXIDE_WAL_FSYNC_MODE=batch       WAL fsync mode: always, batch, or async.
  OXIDE_BINARY=/path/to/oxide      Install this local binary instead of downloading.
  OXIDE_SKIP_SERVICE=1             Install binary/config only.
USAGE
}

require_root() {
  if [ "$(id -u)" -ne 0 ]; then
    echo "error: run as root, for example with sudo" >&2
    exit 1
  fi
}

require_linux() {
  if [ "$OS" != "Linux" ]; then
    echo "error: service installation currently supports Linux/systemd servers" >&2
    exit 1
  fi
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: missing required command: $1" >&2
    exit 1
  fi
}

asset_arch() {
  case "$ARCH" in
    x86_64|amd64) echo "x86_64-unknown-linux-gnu" ;;
    aarch64|arm64) echo "aarch64-unknown-linux-gnu" ;;
    *)
      echo "error: unsupported architecture: $ARCH" >&2
      exit 1
      ;;
  esac
}

download_url() {
  local target="$1"
  if [ "$VERSION" = "latest" ]; then
    echo "https://github.com/${REPO}/releases/latest/download/oxide-${target}.tar.gz"
  else
    echo "https://github.com/${REPO}/releases/download/${VERSION}/oxide-${target}.tar.gz"
  fi
}

install_user() {
  if ! getent group "$GROUP" >/dev/null; then
    groupadd --system "$GROUP"
  fi

  if ! id -u "$USER" >/dev/null 2>&1; then
    useradd \
      --system \
      --gid "$GROUP" \
      --home-dir "$DATA_DIR" \
      --shell /usr/sbin/nologin \
      "$USER"
  fi
}

install_binary() {
  install -d -m 0755 "$BIN_DIR"

  if [ -n "${OXIDE_BINARY:-}" ]; then
    install -m 0755 "$OXIDE_BINARY" "$BIN_DIR/oxide"
    return
  fi

  require_command curl
  require_command tar

  local target archive url
  target="$(asset_arch)"
  archive="$TMP_DIR/oxide.tar.gz"
  url="$(download_url "$target")"

  echo "Downloading $url"
  curl -fsSL "$url" -o "$archive"
  tar -xzf "$archive" -C "$TMP_DIR"
  install -m 0755 "$TMP_DIR/oxide-$target/oxide" "$BIN_DIR/oxide"
}

write_config() {
  install -d -m 0755 "$CONFIG_DIR"
  install -d -m 0750 -o "$USER" -g "$GROUP" "$DATA_DIR"

  if [ -f "$CONFIG_DIR/oxide.toml" ]; then
    echo "Keeping existing config: $CONFIG_DIR/oxide.toml"
    return
  fi

  cat > "$CONFIG_DIR/oxide.toml" <<EOF
listen_addr = "$LISTEN_ADDR"
data_dir = "$DATA_DIR"
max_frame_bytes = 67108864
default_claim_wait_ms = 30000
graceful_shutdown_ms = 5000
log_level = "info"

[wal]
segment_bytes = 134217728
fsync_mode = "$FSYNC_MODE"
batch_bytes = 4194304
batch_interval_ms = 10

[stream_defaults]
partitions = 1
policy = "priority"
max_payload_bytes = 1048576
default_ttr_ms = 30000
dedupe_ttl_ms = 86400000
EOF

  chmod 0644 "$CONFIG_DIR/oxide.toml"
}

install_service() {
  if [ "${OXIDE_SKIP_SERVICE:-0}" = "1" ]; then
    return
  fi

  require_command systemctl

  cat > "/etc/systemd/system/${SERVICE_NAME}.service" <<EOF
[Unit]
Description=Oxide work-stream engine
Documentation=https://github.com/${REPO}
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$USER
Group=$GROUP
ExecStart=$BIN_DIR/oxide serve --config $CONFIG_DIR/oxide.toml
Restart=on-failure
RestartSec=2
LimitNOFILE=1048576
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=true
ReadWritePaths=$DATA_DIR

[Install]
WantedBy=multi-user.target
EOF

  systemctl daemon-reload
  systemctl enable "$SERVICE_NAME"
  systemctl restart "$SERVICE_NAME"
}

main() {
  if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
    usage
    exit 0
  fi

  require_root
  require_linux
  install_user
  install_binary
  write_config
  "$BIN_DIR/oxide" validate-config --config "$CONFIG_DIR/oxide.toml"
  install_service

  echo "Oxide installed."
  echo "Binary:  $BIN_DIR/oxide"
  echo "Config:  $CONFIG_DIR/oxide.toml"
  echo "Data:    $DATA_DIR"

  if [ "${OXIDE_SKIP_SERVICE:-0}" != "1" ]; then
    echo "Service: $SERVICE_NAME"
    echo "Status:  systemctl status $SERVICE_NAME"
  fi
}

main "$@"
