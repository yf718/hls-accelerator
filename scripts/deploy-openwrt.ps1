Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RootDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

$TargetHost = if ($env:TARGET_HOST) { $env:TARGET_HOST } else { "192.168.31.2" }
$TargetUser = if ($env:TARGET_USER) { $env:TARGET_USER } else { "root" }
$TargetPort = if ($env:TARGET_PORT) { $env:TARGET_PORT } else { "22" }
$SshKey = if ($env:SSH_KEY) { $env:SSH_KEY } else { Join-Path $HOME ".ssh/id_rsa" }

$PackageDir = if ($env:PACKAGE_DIR) { $env:PACKAGE_DIR } else { Join-Path $RootDir "dist/hls-accel-linux-amd64-package" }
$ContainerName = if ($env:CONTAINER_NAME) { $env:CONTAINER_NAME } else { "hls-accel" }
$ImageName = if ($env:IMAGE_NAME) { $env:IMAGE_NAME } else { "hls-accel:latest" }
$HostCacheDir = if ($env:HOST_CACHE_DIR) { $env:HOST_CACHE_DIR } else { "/fy/hls-accel/cache" }
$HostHlsDir = if ($env:HOST_HLS_DIR) { $env:HOST_HLS_DIR } else { "/output/mydav/hls" }
$HostPort = if ($env:HOST_PORT) { $env:HOST_PORT } else { "8084" }
$RemoteBaseDir = if ($env:REMOTE_BASE_DIR) { $env:REMOTE_BASE_DIR } else { "/tmp" }
$RemoteDeployDir = "$RemoteBaseDir/$ContainerName-deploy-$PID"

function Require-File([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) {
        throw "missing required file: $Path"
    }
}

function Write-UnixTextFile([string]$Path, [string]$Content) {
    $normalized = $Content -replace "`r`n", "`n"
    $enc = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $normalized, $enc)
}

Require-File (Join-Path $PackageDir "hls-accel")
Require-File (Join-Path $PackageDir "web/index.html")
Require-File (Join-Path $RootDir "aria2.conf")

$StageDir = Join-Path ([System.IO.Path]::GetTempPath()) ("hls-accel-deploy-" + [System.Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $StageDir | Out-Null
try {
    New-Item -ItemType Directory -Path (Join-Path $StageDir "web") | Out-Null
    Copy-Item -LiteralPath (Join-Path $PackageDir "hls-accel") -Destination (Join-Path $StageDir "hls-accel")
    Copy-Item -LiteralPath (Join-Path $PackageDir "web/*") -Destination (Join-Path $StageDir "web") -Recurse
    Copy-Item -LiteralPath (Join-Path $RootDir "aria2.conf") -Destination (Join-Path $StageDir "aria2.conf")

    Write-UnixTextFile (Join-Path $StageDir "docker-entrypoint.sh") @'
#!/bin/sh
set -e
ARIA2_CONF_PATH="${ARIA2_CONF_PATH:-/app/aria2.conf}"
echo "Starting Aria2 RPC server with config: ${ARIA2_CONF_PATH}"
if [ -n "${ARIA2_SECRET:-}" ]; then
  aria2c --conf-path="${ARIA2_CONF_PATH}" --rpc-secret="${ARIA2_SECRET}" --dir=/app/cache --daemon=true --log=/dev/stdout
else
  aria2c --conf-path="${ARIA2_CONF_PATH}" --dir=/app/cache --daemon=true --log=/dev/stdout
fi
echo "Waiting for Aria2 RPC to be ready..."
sleep 3
echo "Starting hls-accel..."
exec /app/hls-accel
'@

    Write-UnixTextFile (Join-Path $StageDir "Dockerfile") @'
FROM alpine:latest
RUN apk add --no-cache aria2 ca-certificates && rm -rf /var/cache/apk/*
WORKDIR /app
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
COPY aria2.conf /app/aria2.conf
COPY hls-accel /app/hls-accel
COPY web /app/web
RUN mkdir -p /app/cache && chmod +x /app/docker-entrypoint.sh /app/hls-accel
EXPOSE 8084 6800
ENV CACHE_DIR=/app/cache
ENV ARIA2_CONF_PATH=/app/aria2.conf
ENTRYPOINT ["/app/docker-entrypoint.sh"]
'@

    Write-Host "Staging deploy files to ${TargetUser}@${TargetHost}:$RemoteDeployDir"
    & ssh -i $SshKey -p $TargetPort -o BatchMode=yes "$TargetUser@$TargetHost" "rm -rf '$RemoteDeployDir' && mkdir -p '$RemoteDeployDir'"
    & scp -i $SshKey -P $TargetPort -o BatchMode=yes -r "$StageDir/." "$TargetUser@$TargetHost`:$RemoteDeployDir/"

    $RemoteScriptPath = Join-Path $StageDir "remote-deploy.sh"
    $RemoteScriptContent = @"
set -eu
container_name='$ContainerName'
image_name='$ImageName'
remote_dir='$RemoteDeployDir'
default_cache_dir='$HostCacheDir'
default_hls_dir='$HostHlsDir'
default_port='$HostPort'
current_cache_dir="`$default_cache_dir"
current_hls_dir="`$default_hls_dir"
current_port="`$default_port"
current_secret=""
if docker inspect "`$container_name" >/dev/null 2>&1; then
  detected_cache_dir=`$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/app/cache"}}{{.Source}}{{end}}{{end}}' "`$container_name" || true)
  detected_hls_dir=`$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/app/hls"}}{{.Source}}{{end}}{{end}}' "`$container_name" || true)
  detected_port=`$(docker inspect -f '{{with index (index .HostConfig.PortBindings "8084/tcp") 0}}{{.HostPort}}{{end}}' "`$container_name" || true)
  detected_secret=`$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "`$container_name" | grep '^ARIA2_SECRET=' | head -n 1 | cut -d= -f2- || true)
  if [ -n "`$detected_cache_dir" ]; then current_cache_dir="`$detected_cache_dir"; fi
  if [ -n "`$detected_hls_dir" ]; then current_hls_dir="`$detected_hls_dir"; fi
  if [ -n "`$detected_port" ]; then current_port="`$detected_port"; fi
  if [ -n "`$detected_secret" ]; then current_secret="`$detected_secret"; fi
fi
mkdir -p "`$current_cache_dir" "`$current_hls_dir"
if docker image inspect "`$image_name" >/dev/null 2>&1; then
  build_container="`${container_name}-build"
  docker rm -f "`$build_container" >/dev/null 2>&1 || true
  docker create --name "`$build_container" --entrypoint /bin/sh "`$image_name" -c "sleep 600" >/dev/null
  docker cp "`$remote_dir/hls-accel" "`$build_container:/app/hls-accel"
  docker cp "`$remote_dir/docker-entrypoint.sh" "`$build_container:/app/docker-entrypoint.sh"
  docker cp "`$remote_dir/aria2.conf" "`$build_container:/app/aria2.conf"
  docker cp "`$remote_dir/web/." "`$build_container:/app/web/"
  docker start "`$build_container" >/dev/null
  docker exec "`$build_container" chmod +x /app/docker-entrypoint.sh /app/hls-accel
  docker stop "`$build_container" >/dev/null
  docker commit --change 'ENTRYPOINT ["/app/docker-entrypoint.sh"]' --change 'ENV ARIA2_CONF_PATH=/app/aria2.conf' "`$build_container" "`$image_name" >/dev/null
  docker rm -f "`$build_container" >/dev/null
else
  docker build -t "`$image_name" "`$remote_dir"
fi
docker rm -f "`$container_name" >/dev/null 2>&1 || true
if [ -n "`$current_secret" ]; then
  docker run -d --name "`$container_name" --restart unless-stopped -p "`$current_port:8084" -v "`$current_cache_dir:/app/cache" -v "`$current_hls_dir:/app/hls" -e "ARIA2_SECRET=`$current_secret" "`$image_name" >/dev/null
else
  docker run -d --name "`$container_name" --restart unless-stopped -p "`$current_port:8084" -v "`$current_cache_dir:/app/cache" -v "`$current_hls_dir:/app/hls" "`$image_name" >/dev/null
fi
sleep 3
echo "Container status:"
docker ps --filter "name=^/`$container_name`$" --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'
echo "--- logs ---"
docker logs --tail 50 "`$container_name"
echo "--- app probe ---"
docker exec "`$container_name" wget -qO- http://127.0.0.1:8084/ >/dev/null
echo "http://127.0.0.1:8084 ok"
echo "--- listener probe ---"
docker exec "`$container_name" sh -lc 'ss -lnt 2>/dev/null || netstat -lnt 2>/dev/null || true'
rm -rf "`$remote_dir"
"@
    Write-UnixTextFile $RemoteScriptPath $RemoteScriptContent

    Write-Host "Deploying container $ContainerName on $TargetHost"
    & scp -i $SshKey -P $TargetPort -o BatchMode=yes $RemoteScriptPath "$TargetUser@$TargetHost`:$RemoteDeployDir/remote-deploy.sh"
    & ssh -i $SshKey -p $TargetPort -o BatchMode=yes "$TargetUser@$TargetHost" "sh '$RemoteDeployDir/remote-deploy.sh'"
    Write-Host "Deployment finished."
}
finally {
    if (Test-Path -LiteralPath $StageDir) {
        Remove-Item -LiteralPath $StageDir -Recurse -Force
    }
}
