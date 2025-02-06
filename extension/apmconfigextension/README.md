# APM Central configuration extension

## Extension sample configuration

```
extensions:
  basicauth:
    client_auth:
      username: changeme
      password: changeme

  apmconfig:
   providers:
    elasticsearch:
      endpoint: "https://127.0.0.1:9200"
      tls:
        ca_file: path_to_ca-cert.pem
      auth:
        authenticator: basicauth
   opamp:
    server:
      endpoint: ":4320"
```
