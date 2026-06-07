# Build the standalone relay binary. Build context must be the repo root so the
# replace directive for ../internal/kubetunnel resolves:
#   docker build -t kubetunnelrelay:poc -f kubetunnelrelay/Dockerfile .
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY internal/kubetunnel ./internal/kubetunnel
COPY kubetunnelrelay ./kubetunnelrelay
WORKDIR /src/kubetunnelrelay
RUN CGO_ENABLED=0 go build -o /relay ./cmd/relay

FROM alpine:3.21 AS certs
RUN apk --update add ca-certificates

FROM scratch
ARG USER_UID=10001
USER ${USER_UID}
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build --chmod=755 /relay /relay
ENTRYPOINT ["/relay"]
EXPOSE 4317 8080
