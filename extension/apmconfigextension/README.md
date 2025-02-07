# Elastic APM Central configuration extension

This extension provides a mechanism for OpAMP based agents to retrieve its
remote configuration set via the [APM Central Configuration](https://www.elastic.co/guide/en/observability/current/apm-agent-configuration.html).

An OpAMP server will be started with a local endpoint where the OpAMP agents can connect into. The
extension will reply with an [OpAMP ServerToAgent message](https://github.com/open-telemetry/opamp-spec/blob/main/specification.md#servertoagent-message) with the
corresponding remote configuration fields.

Central configuration was build for Elastic's APM agents which are identified by
the
[service.name](https://www.elastic.co/guide/en/ecs/1.12/ecs-service.html#field-service-name)
and
[service.environment](https://www.elastic.co/guide/en/ecs/1.12/ecs-service.html#field-service-environment)
(optional) attributes. These attributes **must** be set on the [AgentDescription.identifying_attributes](https://github.com/open-telemetry/opamp-spec/blob/main/specification.md#agentdescriptionidentifying_attributes) field during the first send [AgentToServer](https://github.com/open-telemetry/opamp-spec/blob/main/specification.md#agenttoserver-message) message. As the `AgentDescription` should not be send if not changed, the extension will maintain and internal mapping between the `agent.instance_id` and its service identifing attributes.

The [ServerToAgent.ReportFullState flag](https://github.com/open-telemetry/opamp-spec/blob/main/specification.md#servertoagentflags) will be set in the following cases:

- The agent did not include the `service.name` identifing attributes during the
  first message.
- The OpAMP server does not have that data.

The agent **must** return a message with the corresponding `AgentDescription.identifying_attributes`.

![Extension workflow](./extension-workflow.png "Extension workflow")

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
