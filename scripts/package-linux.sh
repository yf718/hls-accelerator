#!/bin/sh

set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
DIST_DIR="$ROOT_DIR/dist"
APP_NAME="hls-accel"
ENTRYPOINT="./cmd/server/main.go"

build_package() {
    arch="$1"
    binary_name="$APP_NAME-linux-$arch"
    package_name="$binary_name-package"
    package_dir="$DIST_DIR/$package_name"

    echo "Building $package_name..."
    rm -rf "$package_dir"
    mkdir -p "$package_dir"

    env CGO_ENABLED=0 GOOS=linux GOARCH="$arch" \
        go build -o "$package_dir/$APP_NAME" "$ENTRYPOINT"

    cp "$ROOT_DIR/README.md" "$package_dir/README.md"
    cp -R "$ROOT_DIR/web" "$package_dir/web"
}

rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"

build_package amd64
build_package arm64

echo "Linux package directories are available in $DIST_DIR"
