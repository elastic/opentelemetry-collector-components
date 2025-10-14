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

All that is required to enable the apmconfig extension is to include it in the extensions definitions of the collector configuration:

```yaml
extensions:
  bearertokenauth:
    scheme: "APIKey"
    token: "<YOUR_ENCODED_ELASTICSEACH_APIKEY>"

  apmconfig:
    source:
     elasticsearch:
       endpoint: "<YOUR_ELASTICSEACH_ENDPOINT>"
       auth:
         authenticator: bearertokenauth
    opamp:
      protocols:
        http:
          endpoint: ":4320"
```

The snippet above configures the `bearertokenauth` authenticator as client authenticator to be used with the Elasticsearch endpoint. An Elasticsearch API key is used as secret token. The `apmconfig` section defines the Elasticsearch `endpoint` for reading the EDOT SDK configuration and the `authenticator` that should be used with the endpoint. The `opamp` section configures the OpAMP endpoint to provide an HTTP endpoint on port 4320. The EDOT SDKs are connecting to this endpoint to fetch configuration messages. Authentication between the the OpAMP endpoint and the EDOT SDKs is not configured in the snippet. More information on securing the communication between the apmconfig extension and the EDOT SDKs are given in the section [Secure the OpAMP endpoint](#secure-the-opamp-endpoint).

## Advanced configuration

There are more configuration settings available to configure the Elasticsearch client and the OpAMP server of the apmconfig extension. The following sections go into more details.

### Configure the Elasticsearch client

The apmconfig extension retrieves remote configuration data from an Elasticsearch cluster. The Elasticsearch client for accessing the Elasticsearch cluster is configured under the `source::elasticsearch` section in the snippet above. The snippet shows a basic configuration.

All available Elasticsearch client configuration options can be found [here](https://github.com/elastic/opentelemetry-lib/blob/v0.18.0/config/configelasticsearch/configclient.go#L69). The configuration embeds the [configauth authenticator](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.125.0/config/configauth/README.md), allowing the use of standard authentication extensions such as [bearertokenauth](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.125.0/extension/bearertokenauthextension) and [basicauth](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.125.0/extension/basicauthextension).

### Secure the OpAMP endpoint

The apmconfig extension embeds the [confighttp.ServerConfig](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.125.0/config/confighttp/README.md), which means it supports standard HTTP server configuration, including TLS/mTLS and authentication.

#### Enable TLS and mTLS for the OpAMP endpoint

You can enable TLS or mutual TLS to encrypt data in transit between OpAMP clients and the OpAMP server provided by the apmconfig extension. The snippet below configures TLS for the OpAMP endpoint. It uses `cert_file` and the `key_file` setting to specify the path to the server certificate file `your/path/to/server.crt` and key pass file `your/path/to/server.key`. 

Example configuration:

```yaml
extensions:
  apmconfig:
    opamp:
      protocols:
        http:
          endpoint: ":4320"
          tls:
            cert_file: your/path/to/server.crt
            key_file: your/path/to/server.key
   ...
```

More information is available in the 📚 [OpenTelemetry TLS server configuration documentation](https://github.com/open-telemetry/opentelemetry-collector/blob/main/config/configtls/README.md#server-configuration).

#### Enable authentication for the OpAMP endpoint

In addition to TLS, you can configure authentication for the OpAMP endpoint to ensure that only authorized EDOT SDKs can communicate with the `apmconfig` extension and retrieve their corresponding remote configurations.

The `apmconfig` extension supports any [configauth authenticator](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.125.0/config/configauth/README.md). We recommend using the [apikeyauth extension](https://github.com/elastic/opentelemetry-collector-components/tree/main/extension/apikeyauthextension) to authenticate with Elasticsearch API keys:

```yaml
extensions:
  apikeyauth:
    endpoint: "<YOUR_ELASTICSEARCH_ENDPOINT>"
    application_privileges:
      - application: "apm"
        privileges:
          - "config_agent:read"
        resources:
          - "-"
  apmconfig:
    opamp:
      protocols:
        http:
          auth:
            authenticator: apikeyauth
   ...
```

The server will expect incoming HTTP requests to include an API key with sufficient privileges, using the following header format:

```
Authorization: ApiKey <base64(id:api_key)>
```

An API key with the minimum required application permissions (as verified with the configuration above) can be created via Kibana by navigating to: `Observability → Applications → Settings → Agent Keys`, or by using the Elasticsearch Security API:

```json
POST /_security/api_key
{
  "name": "apmconfig-opamp-test-sdk",
  "metadata": {
    "application": "apm"
  },
  "role_descriptors": {
    "apm": {
      "cluster": [],
      "indices": [],
      "applications": [
        {
          "application": "apm",
          "privileges": [
            "config_agent:read"
          ],
          "resources": [
            "*"
          ]
        }
      ],
      "run_as": [],
      "metadata": {}
    }
  }
}
```

The following `curl` command sends the request to the `_security/api_key` API. Replace `<your-elasticsearch-endpoint>` with the Elasticsearch endpoint and `<base64-encoded-api-key>` with an existing API key.

```json
curl -X POST "https://<your-elasticsearch-endpoint>:9200/_security/api_key" \
-H "Content-Type: application/json" \
-H "Authorization: ApiKey <base64-encoded-api-key>" \
-d '{
  "name": "apmconfig-opamp-test-sdk",
  "metadata": {
    "application": "apm"
  },
  "role_descriptors": {
    "apm": {
      "cluster": [],
      "indices": [],
      "applications": [
        {
          "application": "apm",
          "privileges": [
            "config_agent:read"
          ],
          "resources": [
            "*"
          ]
        }
      ],
      "run_as": [],
      "metadata": {}
    }
  }}'
```

### Advanced configuration example

Combining the configuration examples in the advanced configuration section results in the following:

```yaml
extensions:
  bearertokenauth:
    scheme: "APIKey"
    token: "<YOUR_ENCODED_ELASTICSEACH_APIKEY>"
  apikeyauth:
    endpoint: "<YOUR_ELASTICSEARCH_ENDPOINT>"
    application_privileges:
      - application: "apm"
        privileges:
          - "config_agent:read"
        resources:
          - "-"
  source:
     elasticsearch:
       endpoint: "<YOUR_ELASTICSEACH_ENDPOINT>"
       auth:
         authenticator: bearertokenauth
  apmconfig:
    opamp:
      protocols:
        http:
          endpoint: ":4320"
          auth:
            authenticator: apikeyauth
          tls:
            cert_file: your/path/to/server.crt
            key_file: your/path/to/server.key
```

The configuration snippet configures the `bearertokenauth` authenticator for the authentication of the Elasticsearch client, the `apikeyauth` authenticator for the OpAMP server, the Elasticsearch endpoint, and TLS for securing the connection between the OpAMP server and EDOT SDKs being the OpAMP client.