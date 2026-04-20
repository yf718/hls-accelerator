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
    binary_path="$DIST_DIR/$binary_name"
    package_dir="$DIST_DIR/$package_name"
    archive_path="$DIST_DIR/$binary_name.tar.gz"

    echo "Building $binary_name..."
    env CGO_ENABLED=0 GOOS=linux GOARCH="$arch" \
        go build -o "$binary_path" "$ENTRYPOINT"

    rm -rf "$package_dir"
    mkdir -p "$package_dir"

    cp "$binary_path" "$package_dir/$APP_NAME"
    cp "$ROOT_DIR/README.md" "$package_dir/README.md"
    cp -R "$ROOT_DIR/web" "$package_dir/web"

    tar -czf "$archive_path" -C "$DIST_DIR" "$package_name"
    echo "Created $archive_path"
}

mkdir -p "$DIST_DIR"

build_package amd64
build_package arm64

echo "Linux packages are available in $DIST_DIR"
