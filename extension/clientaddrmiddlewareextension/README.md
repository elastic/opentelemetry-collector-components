# Client Address Middleware Extension

> [!WARNING]
> 🚧 This component is a work in progress

The client address middleware provides the ability to set the [`client.Info.Addr`](https://github.com/open-telemetry/opentelemetry-collector/blob/client/v1.47.0/client/client.go#L95) based on the values from a list of metadata keys. 

## Configuration
The middleware requires only one configuration `clientaddrmiddleware` which is a list of metadata keys.
Keys are processed in order, the first valid value is used to set the client address. If there are no valid addresses found, the client address is not updated.

Receivers should be configured with `include_metadata: true`, so that the context includes client metadata keys.

### Example
The following example configures the middleware to set the client address based on the the `x-forwareded-for` metadata key (header). 
The middleware is added to the `otlp` and `http` receivers. 
```yaml
extensions:
  clientaddrmiddleware:
    client_address_metadata_keys:
      - x-forwarded-for
receivers:
  otlp:
    protocols:
      http:
        include_metadata: true
        middlewares:
          - id: clientaddrmiddleware
      grpc:
        include_metadata: true
        middlewares:
        - id: clientaddrmiddleware
```