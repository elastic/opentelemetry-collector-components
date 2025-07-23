FROM alpine:3.22 as certs
RUN apk --update add ca-certificates

FROM scratch

ARG USER_UID=10001
USER ${USER_UID}

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --chmod=755 _build/elastic-collector-components /elastic-collector-components
COPY distributions/elastic-components/config.yaml /etc/elastic-collector-components/config.yaml
ENTRYPOINT ["/elastic-collector-components"]
CMD ["--config", "/etc/elastic-collector-components/config.yaml"]
EXPOSE 4317 55678 55679