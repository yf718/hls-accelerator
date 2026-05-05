#!/bin/sh

set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

TARGET_HOST="${TARGET_HOST:-192.168.31.2}"
TARGET_USER="${TARGET_USER:-root}"
TARGET_PORT="${TARGET_PORT:-22}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_rsa}"

PACKAGE_DIR="${PACKAGE_DIR:-$ROOT_DIR/dist/hls-accel-linux-amd64-package}"
CONTAINER_NAME="${CONTAINER_NAME:-hls-accel}"
IMAGE_NAME="${IMAGE_NAME:-hls-accel:latest}"
HOST_CACHE_DIR="${HOST_CACHE_DIR:-/fy/hls-accel/cache}"
HOST_PORT="${HOST_PORT:-8084}"
REMOTE_BASE_DIR="${REMOTE_BASE_DIR:-/tmp}"
REMOTE_DEPLOY_DIR="$REMOTE_BASE_DIR/${CONTAINER_NAME}-deploy-$$"

ssh_base() {
    ssh -i "$SSH_KEY" -p "$TARGET_PORT" -o BatchMode=yes "$TARGET_USER@$TARGET_HOST" "$@"
}

scp_base() {
    scp -i "$SSH_KEY" -P "$TARGET_PORT" -o BatchMode=yes -r "$@"
}

require_file() {
    if [ ! -e "$1" ]; then
        echo "missing required file: $1" >&2
        exit 1
    fi
}

require_file "$PACKAGE_DIR/hls-accel"
require_file "$PACKAGE_DIR/web/index.html"
require_file "$ROOT_DIR/aria2.conf"

STAGE_DIR=$(mktemp -d)
cleanup_local() {
    rm -rf "$STAGE_DIR"
}
trap cleanup_local EXIT INT TERM

mkdir -p "$STAGE_DIR/web"
cp "$PACKAGE_DIR/hls-accel" "$STAGE_DIR/hls-accel"
cp -R "$PACKAGE_DIR/web/." "$STAGE_DIR/web/"
cp "$ROOT_DIR/aria2.conf" "$STAGE_DIR/aria2.conf"

cat > "$STAGE_DIR/docker-entrypoint.sh" <<'EOF'
#!/bin/sh

set -e

ARIA2_CONF_PATH="${ARIA2_CONF_PATH:-/app/aria2.conf}"

echo "Starting Aria2 RPC server with config: ${ARIA2_CONF_PATH}"
if [ -n "${ARIA2_SECRET:-}" ]; then
    aria2c \
        --conf-path="${ARIA2_CONF_PATH}" \
        --rpc-secret="${ARIA2_SECRET}" \
        --dir=/app/cache \
        --daemon=true \
        --log=/dev/stdout
else
    aria2c \
        --conf-path="${ARIA2_CONF_PATH}" \
        --dir=/app/cache \
        --daemon=true \
        --log=/dev/stdout
fi

echo "Waiting for Aria2 RPC to be ready..."
sleep 3

echo "Starting hls-accel..."
exec /app/hls-accel
EOF

cat > "$STAGE_DIR/Dockerfile" <<'EOF'
FROM alpine:latest

RUN apk add --no-cache \
    aria2 \
    ca-certificates \
    && rm -rf /var/cache/apk/*

WORKDIR /app

COPY docker-entrypoint.sh /app/docker-entrypoint.sh
COPY aria2.conf /app/aria2.conf
COPY hls-accel /app/hls-accel
COPY web /app/web

RUN mkdir -p /app/cache && \
    chmod +x /app/docker-entrypoint.sh /app/hls-accel

EXPOSE 8084 6800

ENV CACHE_DIR=/app/cache
ENV ARIA2_CONF_PATH=/app/aria2.conf

ENTRYPOINT ["/app/docker-entrypoint.sh"]
EOF

chmod +x "$STAGE_DIR/docker-entrypoint.sh"

echo "Staging deploy files to $TARGET_USER@$TARGET_HOST:$REMOTE_DEPLOY_DIR"
ssh_base "rm -rf '$REMOTE_DEPLOY_DIR' && mkdir -p '$REMOTE_DEPLOY_DIR'"
scp_base "$STAGE_DIR/." "$TARGET_USER@$TARGET_HOST:$REMOTE_DEPLOY_DIR/"

echo "Deploying container $CONTAINER_NAME on $TARGET_HOST"
ssh_base "sh -s" <<EOF
set -eu

container_name='$CONTAINER_NAME'
image_name='$IMAGE_NAME'
remote_dir='$REMOTE_DEPLOY_DIR'
default_cache_dir='$HOST_CACHE_DIR'
default_port='$HOST_PORT'

current_cache_dir="\$default_cache_dir"
current_port="\$default_port"
current_secret=""

if docker inspect "\$container_name" >/dev/null 2>&1; then
    detected_cache_dir=\$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/app/cache"}}{{.Source}}{{end}}{{end}}' "\$container_name" || true)
    detected_port=\$(docker inspect -f '{{with index (index .HostConfig.PortBindings "8084/tcp") 0}}{{.HostPort}}{{end}}' "\$container_name" || true)
    detected_secret=\$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "\$container_name" | grep '^ARIA2_SECRET=' | head -n 1 | cut -d= -f2- || true)

    if [ -n "\$detected_cache_dir" ]; then
        current_cache_dir="\$detected_cache_dir"
    fi
    if [ -n "\$detected_port" ]; then
        current_port="\$detected_port"
    fi
    if [ -n "\$detected_secret" ]; then
        current_secret="\$detected_secret"
    fi
fi

mkdir -p "\$current_cache_dir"

if docker image inspect "\$image_name" >/dev/null 2>&1; then
    build_container="\${container_name}-build"
    docker rm -f "\$build_container" >/dev/null 2>&1 || true
    docker create --name "\$build_container" "\$image_name" >/dev/null
    docker cp "\$remote_dir/hls-accel" "\$build_container:/app/hls-accel"
    docker cp "\$remote_dir/docker-entrypoint.sh" "\$build_container:/app/docker-entrypoint.sh"
    docker cp "\$remote_dir/aria2.conf" "\$build_container:/app/aria2.conf"
    docker cp "\$remote_dir/web/." "\$build_container:/app/web/"
    docker commit \
        --change 'ENTRYPOINT ["/app/docker-entrypoint.sh"]' \
        --change 'ENV ARIA2_CONF_PATH=/app/aria2.conf' \
        "\$build_container" "\$image_name" >/dev/null
    docker rm -f "\$build_container" >/dev/null
else
    docker build -t "\$image_name" "\$remote_dir"
fi

docker rm -f "\$container_name" >/dev/null 2>&1 || true

if [ -n "\$current_secret" ]; then
    docker run -d \
        --name "\$container_name" \
        --restart unless-stopped \
        -p "\$current_port:8084" \
        -v "\$current_cache_dir:/app/cache" \
        -e "ARIA2_SECRET=\$current_secret" \
        "\$image_name" >/dev/null
else
    docker run -d \
        --name "\$container_name" \
        --restart unless-stopped \
        -p "\$current_port:8084" \
        -v "\$current_cache_dir:/app/cache" \
        "\$image_name" >/dev/null
fi

sleep 3

echo "Container status:"
docker ps --filter "name=^/\$container_name\$" --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'

echo "--- logs ---"
docker logs --tail 50 "\$container_name"

echo "--- app probe ---"
docker exec "\$container_name" wget -qO- http://127.0.0.1:8084/ >/dev/null
echo "http://127.0.0.1:8084 ok"

echo "--- listener probe ---"
docker exec "\$container_name" sh -lc 'ss -lnt 2>/dev/null || netstat -lnt 2>/dev/null || true'

rm -rf "\$remote_dir"
EOF

echo "Deployment finished."
