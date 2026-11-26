FROM alpine:3.21 AS certs
RUN apk --update add ca-certificates

FROM scratch

ARG USER_UID=10001
USER ${USER_UID}

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --chmod=755 _build/otelsoak /otelsoak
COPY distributions/otelsoak/config.yaml /etc/otelsoak/config.yaml
ENTRYPOINT ["/otelsoak"]
CMD ["--config", "/etc/otelsoak/config.yaml"]
