#!/bin/sh
# 启动脚本：同时启动 Aria2 RPC 和 hls-accel

set -e

# 启动 Aria2 RPC 服务器（后台运行）
# 如果设置了 ARIA2_SECRET 环境变量，则使用它
echo "Starting Aria2 RPC server..."
if [ -n "$ARIA2_SECRET" ]; then
    aria2c --enable-rpc \
           --rpc-listen-port=6800 \
           --rpc-allow-origin-all \
           --rpc-secret="$ARIA2_SECRET" \
           --dir=/app/cache \
           --daemon=true \
           --log=/dev/stdout \
           --console-log-level=notice
else
    aria2c --enable-rpc \
           --rpc-listen-port=6800 \
           --rpc-allow-origin-all \
           --dir=/app/cache \
           --daemon=true \
           --log=/dev/stdout \
           --console-log-level=notice
fi

# 等待 Aria2 RPC 服务启动
echo "Waiting for Aria2 RPC to be ready..."
sleep 3

# 启动 hls-accel（前台运行，作为主进程）
echo "Starting hls-accel..."
exec /app/hls-accel
