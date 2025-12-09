# Client Address Middleware Extension

> [!WARNING]
> 🚧 This component is a work in progress

The client address middleware provides the ability to set the [`client.Info.Addr`](https://github.com/open-telemetry/opentelemetry-collector/blob/client/v1.47.0/client/client.go#L95) based on the following metadata keys:
- `forwarded`
- `x-real-ip`
- `x-forwarded-for`

Keys are processed in the above order, the first valid value is used to set the client address. If there are no valid addresses found, the client address is not updated.

## Configuration
Receivers should be configured with `include_metadata: true`, so that the context includes client metadata keys.

### Example
The following example configures both the otlp `grpc` and `http` receivers with the client address middleware.
```yaml
extensions:
  clientaddrmiddleware: {}
    
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