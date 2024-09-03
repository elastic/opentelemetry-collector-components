# APM Central configuration extension

## Extension sample configuration

```
extensions:
  apmconfig:
   centralconfig:
    elastic:
      apm:
        secret_token: YOUR_SECRET_TOKEN
        server:
          urls: ["YOUR_APM_SERVER_URL"]
   opamp:
    server:
      endpoint: ":4320"
```
