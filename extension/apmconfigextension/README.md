# Elastic APM Central configuration extension

This extension provides a mechanism for OpAMP based agents to retrieve its
remote configuration set via the [APM Central
Configuration](https://www.elastic.co/guide/en/observability/current/apm-agent-configuration.html).

An OpAMP server will be started with a local endpoint where the OpAMP agents can
connect into. The extension will reply with an [OpAMP ServerToAgent
message](https://github.com/open-telemetry/opamp-spec/blob/main/specification.md#servertoagent-message)
with the corresponding remote configuration fields.

Central configuration was built for Elastic's APM agents which are identified by
the
[service.name](https://www.elastic.co/guide/en/ecs/1.12/ecs-service.html#field-service-name)
and
[service.environment](https://www.elastic.co/guide/en/ecs/1.12/ecs-service.html#field-service-environment)
(optional) attributes. The equivalent OpenTelemetry Semantic Conventions
attributes are:
  - [service.name](https://github.com/open-telemetry/semantic-conventions/blob/v1.32.0/docs/attributes-registry/service.md)
  - [deployment.environment.name](https://github.com/open-telemetry/semantic-conventions/blob/v1.32.0/docs/attributes-registry/deployment.md)

These attributes (`service.name` and optionally `deployment.environment.name`) **must** be set on the
[AgentDescription.identifying_attributes](https://github.com/open-telemetry/opamp-spec/blob/main/specification.md#agentdescriptionidentifying_attributes)
field during the first send
[AgentToServer](https://github.com/open-telemetry/opamp-spec/blob/main/specification.md#agenttoserver-message)
message. As the `AgentDescription` should not be sent if not changed, the
extension will maintain an internal mapping between the `Agent.instance_uid` and
its service identifing attributes.

The [ServerToAgent.ReportFullState
flag](https://github.com/open-telemetry/opamp-spec/blob/main/specification.md#servertoagentflags)
will be set in the following cases:

- The agent did not include the `service.name` identifing attributes during the
first message.
- The OpAMP server was not able to identify the agent (undefined
`Agent.instance_uid`).

The agent **must** return a message with the corresponding
`AgentDescription.identifying_attributes`.

## OpAMP Remote Config

The OpAMP protocol defines a
[AgentRemoteConfig](https://github.com/open-telemetry/opamp-spec/blob/v0.11.0/proto/opamp.proto#L913)
structure within the
[ServerToAgent](https://github.com/open-telemetry/opamp-spec/blob/v0.11.0/proto/opamp.proto#L187)
to share a configuration that should be applied by the connected agent.

- The `AgentRemoteConfig` structure contains a map of configurations, where each
key represents a file name or subsection. This extension assumes that connected
agents use only a single configuration file or section, meaning the map will
contain only one entry—and in this case, the key may be an empty string.
- Since the configuration is encoded in JSON, the
[content_type](https://github.com/open-telemetry/opamp-spec/blob/v0.11.0/proto/opamp.proto#L948C12-L948C24)
field in the `AgentRemoteConfig` is set to `application/json`.
- Each `AgentRemoteConfig` message should contain a [hash
identifier](https://github.com/open-telemetry/opamp-spec/blob/v0.11.0/proto/opamp.proto#L929)
that the Agent SHOULD include value in subsequent
[RemoteConfigStatus](https://github.com/open-telemetry/opamp-spec/blob/v0.11.0/proto/opamp.proto#L751)
messages in the `last_remote_config_hash` field. The server decides on which
hash function to use, this extension will use the `etag` associated to each
unique remote configuration.

![Extension workflow](./extension-workflow.png "Extension workflow")

## Getting started

All that is required to enable the apmconfig extension is to include it in the extensions definitions:

```
extensions:
  bearertokenauth:
    scheme: "APIKey"
    token: "<ENCODED_ELASTICSEACH_APIKEY>"

  apmconfig:
    agent_config:
     elasticsearch:
       endpoint: "<ELASTICSEACH_ENDPOINT>"
       auth:
         authenticator: bearertokenauth
    opamp:
      protocols:
        http:
          endpoint: ":4320"
```


## Advanced configuration

The apmconfig extension embeds the [confighttp.ServerConfig](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.125.0/config/confighttp/README.md), which means it supports standard HTTP server configuration, including TLS/mTLS and authentication.

### TLS and mTLS settings

You can enable TLS or mutual TLS to encrypt data in transit between OpAMP clients and the extension.

Example configuration:

```yaml
extensions:
  apmconfig:
    opamp:
      protocols:
        http:
          endpoint: ":4320"
          tls:
            cert_file: server.crt
            key_file: server.key
   ...
```

📚 OpenTelemetry TLS server configuration:
https://github.com/open-telemetry/opentelemetry-collector/blob/main/config/configtls/README.md#server-configuration

### Authentication settings

In addition to TLS, you can configure authentication to ensure that only authorized agents can communicate with the extension.

The apmconfig extension supports any [configauth authenticator](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.125.0/config/configauth/README.md). We recommend using the [apikeyauth extension](https://github.com/elastic/opentelemetry-collector-components/tree/main/extension/apikeyauthextension) to authenticate with Elastic APM API keys (HTTP headers must include a valid API Key):

```yaml
extensions:
  apikeyauth:
    endpoint: "<YOUR_ELASTICSEARCH_ENDPOINT>"
    application_privileges:
      - application: "apm"
        privileges:
          - "event:write"
        resources:
          - "-"
  apmconfig:
    opamp:
      protocols:
        http:
          endpoint: ":4320"
          tls:
            cert_file: server.crt
            key_file: server.key
          auth:
            authenticator: apikeyauth
   ...
```
