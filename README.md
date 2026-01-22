# HLS Accelerator

> **Note**: This is a practice project created during vibe coding sessions. It serves as a learning exercise for building HLS streaming acceleration tools.

A local proxy tool to accelerate HLS (HTTP Live Streaming) video playback and downloading using Aria2. This tool acts as an intermediary between your video player and HLS streams, providing parallel segment downloading, intelligent caching, and automatic playlist rewriting for improved playback performance.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Architecture](#architecture)
- [Troubleshooting](#troubleshooting)

## Features

- **M3U8 Rewrite**: Automatically rewrites Master and Variant playlists to route segments through the proxy
- **Aria2 Integration**: Parallel downloading of video segments and encryption keys for faster buffering
- **Intelligent Caching**: Serves cached content from local disk when available, falls back to live proxy for missing segments
- **Header Forwarding**: Preserves custom headers (User-Agent, Referer, etc.) for anti-stealing token compatibility
- **Task Management**: Tracks download tasks and manages segment lifecycle
- **SQLite Database**: Stores task metadata and download status

## Requirements

- **Go** 1.18+ (tested with Go 1.24+)
- **Aria2** (must be installed and running in RPC mode)
  - Download from [Aria2 official website](https://aria2.github.io/) or use package manager
  - Windows: `choco install aria2` or download from releases
  - Linux: `sudo apt-get install aria2` or `sudo yum install aria2`
  - macOS: `brew install aria2`

## Installation

### 1. Install Aria2

**Windows:**
```bash
# Using Chocolatey
choco install aria2

# Or download from https://github.com/aria2/aria2/releases
```

**Linux:**
```bash
sudo apt-get update && sudo apt-get install aria2
# or
sudo yum install aria2
```

**macOS:**
```bash
brew install aria2
```

### 2. Build HLS Accelerator

1. Clone or download the source code:
   ```bash
   git clone <repository-url>
   cd hls-accelerator
   ```

2. Build the server:
   ```bash
   # Windows
   go build -o hls-accel.exe cmd/server/main.go
   
   # Linux/macOS
   go build -o hls-accel cmd/server/main.go
   ```

## Usage

### Step 1: Start Aria2 RPC Server

Start Aria2 with RPC enabled. You can run it in the background or in a separate terminal:

```bash
# Basic RPC server (allows all origins)
aria2c --enable-rpc --rpc-allow-origin-all

# With RPC secret (recommended for security)
aria2c --enable-rpc --rpc-allow-origin-all --rpc-secret=your-secret-token

# With custom RPC port
aria2c --enable-rpc --rpc-listen-port=6800 --rpc-allow-origin-all
```

**Note**: Keep this terminal/process running while using HLS Accelerator.

### Step 2: Start HLS Accelerator

Run the built executable:

```bash
# Windows
./hls-accel.exe

# Linux/macOS
./hls-accel
```

The server will start on the default port `8084` (configurable via `config.json`).

### Step 3: Configure Your Video Player

Configure your video player (VLC, MPV, PotPlayer, etc.) to use the local proxy.

#### URL Format

```
http://localhost:8084/proxy/m3u8/<URL_ENCODED_M3U8_URL>
```

#### URL Encoding

You need to URL-encode the original M3U8 URL. Here are examples:

**Original URL:**
```
https://example.com/path/to/video.m3u8
```

**Encoded URL:**
```
https%3A%2F%2Fexample.com%2Fpath%2Fto%2Fvideo.m3u8
```

**Final Player URL:**
```
http://localhost:8084/proxy/m3u8/https%3A%2F%2Fexample.com%2Fpath%2Fto%2Fvideo.m3u8
```

#### Encoding Tools

- **Online**: Use any URL encoder (e.g., [urlencoder.org](https://www.urlencoder.org/))
- **Command Line**: 
  ```bash
  # Python
  python -c "import urllib.parse; print(urllib.parse.quote('YOUR_URL', safe=''))"
  
  # PowerShell (Windows)
  [System.Web.HttpUtility]::UrlEncode('YOUR_URL')
  ```

#### Player Configuration Examples

**VLC Media Player:**
1. Open VLC
2. Media → Open Network Stream
3. Enter the proxy URL
4. Click Play

**MPV:**
```bash
mpv "http://localhost:8084/proxy/m3u8/https%3A%2F%2Fexample.com%2Fvideo.m3u8"
```

## Configuration

Create a `config.json` file in the same directory as the executable to override default settings:

```json
{
  "headers": {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://example.com",
    "Cookie": "session=abc123"
  },
  "aria2_rpc_url": "http://localhost:6800/jsonrpc",
  "aria2_secret": "",
  "proxy_port": 8084,
  "cache_dir": "./cache"
}
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `headers` | object | `{"User-Agent": "Mozilla/5.0..."}` | HTTP headers to send with requests |
| `aria2_rpc_url` | string | `"http://localhost:6800/jsonrpc"` | Aria2 RPC endpoint URL |
| `aria2_secret` | string | `""` | Aria2 RPC secret token (if configured) |
| `proxy_port` | integer | `8084` | Port for the proxy server |
| `cache_dir` | string | `"./cache"` | Directory for caching downloaded segments |

### Default Headers

If not specified in `config.json`, the default User-Agent is:
```
Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36
```

## Architecture

```
┌─────────────┐
│ Video Player│
└──────┬──────┘
       │ HTTP Request
       ▼
┌─────────────────────┐
│  HLS Accelerator    │
│  (Proxy Server)     │
│  Port: 8084         │
└──────┬──────────────┘
       │
       ├──► M3U8 Rewriter ──► Rewrites playlist URLs
       │
       ├──► Cache Manager ──► Checks local cache
       │
       ├──► Task Manager ───► Manages download tasks
       │
       └──► Aria2 Client ───► Parallel downloads
                              │
                              ▼
                       ┌──────────────┐
                       │ Aria2 RPC    │
                       │ Port: 6800   │
                       └──────────────┘
```

### Components

- **Proxy Server**: HTTP server that handles M3U8 and segment requests
- **M3U8 Rewriter**: Parses and rewrites playlist files to route through proxy
- **Cache Manager**: Manages local file cache for downloaded segments
- **Task Manager**: Tracks and manages download tasks
- **Aria2 Client**: Communicates with Aria2 RPC for parallel downloads
- **SQLite Database**: Stores task metadata and status

## Troubleshooting

### Aria2 Connection Issues

**Problem**: `Failed to connect to Aria2 RPC`

**Solutions**:
1. Verify Aria2 is running: Check if the process is active
2. Check RPC URL: Ensure `aria2_rpc_url` in `config.json` matches your Aria2 RPC port
3. Check RPC secret: If Aria2 uses `--rpc-secret`, set it in `config.json`
4. Test Aria2 RPC manually:
   ```bash
   curl http://localhost:6800/jsonrpc -X POST -d '{"jsonrpc":"2.0","method":"aria2.getVersion","id":1}'
   ```

### Playback Issues

**Problem**: Video doesn't play or buffers frequently

**Solutions**:
1. Check cache directory permissions: Ensure write access to `cache_dir`
2. Verify URL encoding: Double-check the encoded URL is correct
3. Check network connectivity: Ensure the original M3U8 URL is accessible
4. Review headers: Some sites require specific headers (Referer, Cookie, etc.)

### Port Already in Use

**Problem**: `bind: address already in use`

**Solutions**:
1. Change `proxy_port` in `config.json` to an available port
2. Find and close the process using port 8084:
   ```bash
   # Windows
   netstat -ano | findstr :8084
   
   # Linux/macOS
   lsof -i :8084
   ```

### Cache Issues

**Problem**: Cache not working or corrupted

**Solutions**:
1. Clear cache: Delete the `cache` directory and restart
2. Check disk space: Ensure sufficient space for cached segments
3. Verify permissions: Ensure read/write access to cache directory

## License

This is a practice/learning project. Use at your own discretion.
