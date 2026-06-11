# AWS Authenticator Extension

This extension resolves an explicit AWS identity once, centrally, and shares it with
AWS-SDK-based components. Instead of each AWS receiver/exporter duplicating credential
configuration (static keys, profiles, role assumption), they reference this extension by ID.

The extension is an *override*: AWS components already use the default SDK credential chain
when no `auth` is configured, and that behavior is untouched. Consequently the extension
requires an explicit credential source — an empty config is rejected at startup rather than
silently falling back to the ambient identity.

Note: this is different from the `sigv4auth` extension, which signs outgoing HTTP requests
for components built on `confighttp`. This extension instead serves components that use the
AWS SDK directly (e.g. the `awscloudwatch` receiver), where the SDK does its own signing and
needs an `aws.CredentialsProvider`.

## Configuration

At least one of `credentials`, `assume_role`, or `profile` must be set.

| Parameter       | Type   | Description |
| --------------- | ------ | ----------- |
| `region`        | String | Region hint for credential resolution, and default region for the STS client when assuming a role. |
| `profile`       | String | Narrows the default chain to a named shared-config profile. Mutually exclusive with `credentials`. |
| `imds_endpoint` | String | Custom EC2 IMDS endpoint for the default chain. |
| `credentials`   | Object | Static credentials: `access_key_id`, `secret_access_key` (both required), `session_token` (optional). The secret fields are redacted in config dumps. |
| `assume_role`   | Object | STS role assumption: `arn` (required), `external_id`, `session_name`, `sts_region`. |

Static credentials and role assumption compose: when both are set, the static credentials
are the base identity used to assume the role. When only `assume_role` is set, the default
SDK chain (environment variables, shared config files, EC2/ECS roles, IRSA, ...) provides
the base identity for the AssumeRole call — the resolved identity is still explicitly the
assumed role.

### Example

```yaml
extensions:
  awsauth:
    region: us-east-1
    credentials:
      access_key_id: ${env:AWS_ACCESS_KEY_ID}
      secret_access_key: ${env:AWS_SECRET_ACCESS_KEY}
    assume_role:
      arn: arn:aws:iam::123456789012:role/monitoring
      external_id: my-external-id

receivers:
  awscloudwatch:
    region: us-west-2
    auth: awsauth
    ...

service:
  extensions: [awsauth]
```

## Consuming the extension from a component

Components resolve the extension from the host at start and type-assert it to the
`Provider` interface (importing this package, or declaring a structurally identical
local interface to avoid the module dependency):

```go
ext, ok := host.GetExtensions()[cfg.Auth]
if !ok {
    return fmt.Errorf("unknown auth extension %q", cfg.Auth)
}
provider, ok := ext.(interface {
    GetCredentialsProvider() aws.CredentialsProvider
})
if !ok {
    return fmt.Errorf("extension %q is not an AWS credentials provider", cfg.Auth)
}
awsCfg.Credentials = provider.GetCredentialsProvider()
```
