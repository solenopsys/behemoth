FROM alpine:3.23.4 AS runtime
WORKDIR /app

RUN apk add --no-cache libstdc++ \
    && mkdir -p /app/lib
COPY ./zig-out/x86_64-musl/bin/storage /app/storage
COPY ./zig-out/x86_64-musl/lib/libvalkey.so /app/lib/libvalkey.so
COPY ./zig-out/x86_64-musl/lib/libzimq.so /app/lib/libzimq.so
RUN chmod +x /app/storage && mkdir -p /app/data /app/socket && chown -R 1000:1000 /app
RUN adduser -D -u 1000 default || true
USER 1000

EXPOSE 9000 6379

CMD ["/app/storage", "start", "--data-dir", "/app/data", "--tcp", "0.0.0.0:9000"]
