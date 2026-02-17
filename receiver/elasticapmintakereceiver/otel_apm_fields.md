SemConv Package Versions:
• go.opentelemetry.io/otel/semconv/v1.22.0 (imported as semconv22)
• go.opentelemetry.io/otel/semconv/v1.27.0 (imported as semconv)

Note: Fields not explicitly mapped in the elasticsearch exporter's conversion maps will pass through as-is (when SemConv field names match Elastic field names). Fields that require explicit mappings in the exporter are those where SemConv names differ from Elastic field names.

| OTEL | APM | Level |
  |------|-----|-------|
| semconv.ServiceNameKey | event.Service.Name | resource |
| semconv.ServiceVersionKey | event.Service.Version | resource |
| semconv.TelemetrySDKLanguageKey | event.Service.Language.Name (translated) | resource |
| semconv.TelemetrySDKNameKey | "ElasticAPM" (hardcoded) | resource |
| semconv22.DeploymentEnvironmentKey | event.Service.Environment | resource |
| semconv.DeploymentEnvironmentNameKey | event.Service.Environment | resource |
| semconv.ServiceInstanceIDKey | event.Service.Node.Name | resource |
| semconv.HostNameKey | event.Host.Name | resource |
| semconv.HostIDKey | event.Host.Id | resource |
| semconv.HostArchKey | event.Host.Architecture | resource |
| semconv.OSNameKey | event.Host.Os.Name | resource |
| semconv.OSVersionKey | event.Host.Os.Version | resource |
| semconv.UserAgentOriginalKey | event.UserAgent.Original | resource |
| semconv.CloudProviderKey | event.Cloud.Provider | resource |
| semconv.CloudRegionKey | event.Cloud.Region | resource |
| semconv.CloudAvailabilityZoneKey | event.Cloud.AvailabilityZone | resource |
| semconv.CloudAccountIDKey | event.Cloud.AccountId | resource |
| semconv.CloudPlatformKey | event.Cloud.ServiceName | resource |
| semconv.ContainerIDKey | event.Container.Id | resource |
| semconv.ContainerNameKey | event.Container.Name | resource |
| semconv.ContainerRuntimeKey | event.Container.Runtime | resource |
| semconv.ContainerImageNameKey | event.Container.ImageName | resource |
| semconv.ContainerImageTagsKey | event.Container.ImageTag | resource |
| semconv.K8SNamespaceNameKey | event.Kubernetes.Namespace | resource |
| semconv.K8SNodeNameKey | event.Kubernetes.NodeName | resource |
| semconv.K8SPodNameKey | event.Kubernetes.PodName | resource |
| semconv.K8SPodUIDKey | event.Kubernetes.PodUid | resource |
| semconv.ProcessPIDKey | event.Process.Pid | resource |
| semconv.ProcessParentPIDKey | event.Process.Ppid | resource |
| semconv.ProcessExecutableNameKey | event.Process.Title | resource |
| semconv.ProcessCommandLineKey | event.Process.Argv (joined) | resource |
| semconv.ProcessExecutablePathKey | event.Process.Executable | resource |
| semconv.UserIDKey | event.User.Id | resource |
| semconv.UserEmailKey | event.User.Email | resource |
| semconv.UserNameKey | event.User.Name | resource |
| semconv.NetworkConnectionTypeKey | event.Network.Connection.Type | resource |
| semconv.NetworkConnectionSubtypeKey | event.Network.Connection.Subtype | resource |
| semconv.NetworkCarrierNameKey | event.Network.Carrier.Name | resource |
| semconv.NetworkCarrierMccKey | event.Network.Carrier.Mcc | resource |
| semconv.NetworkCarrierMncKey | event.Network.Carrier.Mnc | resource |
| semconv.NetworkCarrierIccKey | event.Network.Carrier.Icc | resource |
| semconv.ClientAddressKey | event.Client.Ip (converted to string) | resource |
| semconv.ClientPortKey | event.Client.Port | resource |
| semconv.SourceAddressKey | event.Source.Ip (converted to string) | resource |
| semconv.SourcePortKey | event.Source.Port | resource |
| semconv.FaaSInstanceKey | event.Faas.Id | resource |
| semconv.FaaSNameKey | event.Faas.Name | resource |
| semconv.FaaSVersionKey | event.Faas.Version | resource |
| semconv.FaaSTriggerKey | event.Faas.TriggerType | resource |
| semconv.FaaSColdstartKey | event.Faas.ColdStart | resource |
| semconv.HTTPRequestMethodKey | event.Http.Request.Method | span,log |
| semconv.HTTPResponseStatusCodeKey | event.Http.Response.StatusCode | span,log |
| semconv.HTTPResponseBodySizeKey | event.Http.Response.EncodedBodySize | span,log |
| semconv.URLOriginalKey | event.Url.Original | span,log |
| semconv.URLSchemeKey | event.Url.Scheme | span,log |
| semconv.URLFullKey | event.Url.Full | span,log |
| semconv.URLDomainKey | event.Url.Domain | span,log |
| semconv.URLPathKey | event.Url.Path | span,log |
| semconv.URLQueryKey | event.Url.Query | span,log |
| semconv.URLFragmentKey | event.Url.Fragment | span,log |
| semconv.URLPortKey | event.Url.Port | span,log |
| semconv.MessagingDestinationNameKey | event.Transaction.Message.QueueName | span |
| semconv.MessagingDestinationNameKey | event.Span.Message.QueueName | span |
| semconv.MessagingSystemKey | event.Span.Subtype | span |
| semconv.MessagingOperationNameKey | event.Span.Action | span |
| semconv.DBSystemKey | event.Span.Db.Type | span |
| semconv.DBNamespaceKey | event.Span.Db.Instance | span |
| semconv.DBQueryTextKey | event.Span.Db.Statement | span |
| semconv.DestinationAddressKey | event.Destination.Address | span |
| semconv.DestinationPortKey | event.Destination.Port | span |

## Fields Explicitly Mapped in Elasticsearch Exporter

The following fields are explicitly mapped in the elasticsearch exporter's conversion maps (where SemConv names differ from Elastic field names):

**Resource-level (resourceAttrsConversionMap):**
- `semconv.ServiceInstanceIDKey` → `service.node.name`
- `semconv22.DeploymentEnvironmentKey` → `service.environment`
- `semconv.DeploymentEnvironmentNameKey` → `service.environment`
- `semconv.CloudPlatformKey` → `cloud.service.name`
- `semconv.ContainerImageTagsKey` → `container.image.tag`
- `semconv.HostNameKey` → `host.hostname`
- `semconv.HostArchKey` → `host.architecture`
- `semconv.ProcessParentPIDKey` → `process.parent.pid`
- `semconv.ProcessExecutableNameKey` → `process.title`
- `semconv.ProcessExecutablePathKey` → `process.executable`
- `semconv.ProcessCommandLineKey` → `process.args`
- `semconv.OSNameKey` → `host.os.name`
- `semconv.OSVersionKey` → `host.os.version`
- `semconv.ClientAddressKey` → `client.ip`
- `semconv.SourceAddressKey` → `source.ip`
- `semconv.FaaSInstanceKey` → `faas.id`
- `semconv.FaaSTriggerKey` → `faas.trigger.type`
- Kubernetes fields (K8SNamespaceNameKey, K8SNodeNameKey, K8SPodNameKey, K8SPodUIDKey, etc.)

**Span-level (spanAttrsConversionMap):**
- `semconv.MessagingDestinationNameKey` → `span.message.queue.name` or `transaction.message.queue.name`
- `semconv.MessagingOperationNameKey` → `span.action`
- `semconv.MessagingSystemKey` → `span.subtype`
- `semconv22.DBSystemKey` → `span.db.type`
- `semconv.DBNamespaceKey` → `span.db.instance`
- `semconv.DBQueryTextKey` → `span.db.statement`
- `semconv.HTTPResponseBodySizeKey` → `http.response.encoded_body_size`

**Log-level (recordAttrsConversionMap):**
- `semconv.HTTPResponseBodySizeKey` → `http.response.encoded_body_size`
- Exception fields (ExceptionMessageKey, ExceptionStacktraceKey, ExceptionTypeKey, ExceptionEscapedKey)

## Fields Not Explicitly Mapped (Pass Through)

The following fields are set by the intake receiver but are NOT explicitly mapped in the exporter's conversion maps. They pass through as-is because SemConv field names match Elastic/ECS field names:

**Resource-level (Verified - SemConv name matches ECS name):**
- `semconv.ServiceNameKey` → `service.name` ✓
- `semconv.ServiceVersionKey` → `service.version` ✓
- `semconv.HostIDKey` → `host.id` ✓
- `semconv.UserAgentOriginalKey` → `user_agent.original` ✓
- `semconv.CloudProviderKey` → `cloud.provider` ✓
- `semconv.CloudRegionKey` → `cloud.region` ✓
- `semconv.CloudAvailabilityZoneKey` → `cloud.availability_zone` ✓
- `semconv.CloudAccountIDKey` → `cloud.account.id` ✓
- `semconv.ContainerIDKey` → `container.id` ✓
- `semconv.ContainerNameKey` → `container.name` ✓
- `semconv.ContainerRuntimeKey` → `container.runtime` ✓
- `semconv.ContainerImageNameKey` → `container.image.name` ✓
- `semconv.ProcessPIDKey` → `process.pid` ✓
- `semconv.UserIDKey` → `user.id` ✓
- `semconv.UserEmailKey` → `user.email` ✓
- `semconv.UserNameKey` → `user.name` ✓
- `semconv.NetworkConnectionTypeKey` → `network.connection.type` ✓
- `semconv.NetworkConnectionSubtypeKey` → `network.connection.subtype` ✓
- `semconv.NetworkCarrierNameKey` → `network.carrier.name` ✓
- `semconv.NetworkCarrierMccKey` → `network.carrier.mcc` ✓
- `semconv.NetworkCarrierMncKey` → `network.carrier.mnc` ✓
- `semconv.NetworkCarrierIccKey` → `network.carrier.icc` ✓
- `semconv.ClientPortKey` → `client.port` ✓
- `semconv.SourcePortKey` → `source.port` ✓
- `semconv.FaaSNameKey` → `faas.name` ✓
- `semconv.FaaSVersionKey` → `faas.version` ✓
- `semconv.FaaSColdstartKey` → `faas.coldstart` ✓

**Span/Log-level (Verified - SemConv name matches ECS name):**
- `semconv.HTTPRequestMethodKey` → `http.request.method` ✓
- `semconv.HTTPResponseStatusCodeKey` → `http.response.status_code` ✓
- `semconv.URLOriginalKey` → `url.original` ✓
- `semconv.URLSchemeKey` → `url.scheme` ✓
- `semconv.URLFullKey` → `url.full` ✓
- `semconv.URLDomainKey` → `url.domain` ✓
- `semconv.URLPathKey` → `url.path` ✓
- `semconv.URLQueryKey` → `url.query` ✓
- `semconv.URLFragmentKey` → `url.fragment` ✓
- `semconv.URLPortKey` → `url.port` ✓
- `semconv.DestinationAddressKey` → `destination.address` ✓
- `semconv.DestinationPortKey` → `destination.port` ✓

All fields listed above have been verified to have matching SemConv and Elastic/ECS field names, so they correctly pass through without explicit mappings in the exporter.