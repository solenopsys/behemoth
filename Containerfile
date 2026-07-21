FROM alpine:3.23.4 AS runtime
WORKDIR /app

RUN apk add --no-cache libstdc++ \
    && mkdir -p /app/lib
COPY ./zig-out/bin/storage-x86_64-musl /app/storage
COPY ./zig-out/lib/libvalkey-x86_64-musl.so /app/lib/libvalkey.so
COPY ./.container-libs/libzimq.so /app/lib/libzimq.so
RUN chmod +x /app/storage && mkdir -p /app/data /app/socket && chown -R 1000:1000 /app
RUN adduser -D -u 1000 default || true
USER 1000

EXPOSE 9000 6379

CMD ["/app/storage", "start", "--data-dir", "/app/data", "--tcp", "0.0.0.0:9000"]
