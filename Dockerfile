# 使用 Alpine Linux 作为基础镜像（最小化）
FROM alpine:latest

# 安装必要的依赖
# - aria2: 下载工具
# - ca-certificates: HTTPS 连接所需
RUN apk add --no-cache \
    aria2 \
    ca-certificates \
    && rm -rf /var/cache/apk/*

# 设置工作目录
WORKDIR /app

# 复制启动脚本
COPY docker-entrypoint.sh /app/docker-entrypoint.sh

# 复制编译好的二进制文件
COPY hls-accel /app/hls-accel

# COPY config.json /app/config.json

# 复制 web 静态文件目录
COPY web /app/web

# 创建缓存目录并设置执行权限
RUN mkdir -p /app/cache && \
    chmod +x /app/docker-entrypoint.sh

# 暴露端口
# 8084: HLS Accelerator 代理端口
# 6800: Aria2 RPC 端口
EXPOSE 8084 6800

# 设置环境变量
ENV CACHE_DIR=/app/cache

# 使用启动脚本作为入口点
ENTRYPOINT ["/app/docker-entrypoint.sh"]
