## Speed insights schema

Speed insights schema can be found in https://vercel.com/docs/drains/reference/speed-insights#speed-insights-schema.

| Original Vercel schema name | OTel attribute name                        | In semantic conventions | Context    |
|-----------------------------|--------------------------------------------|:-----------------------:|------------|
| `schema`                    | `encoding.format`                          |                         | scope      |
| `timestamp`                 | `data_point.time_unix_nano`                |                         | data point |
| `projectId`                 | `vercel.project.id`                        |                         | resource   |
| `ownerId`                   | `vercel.owner.id`                          |                         | resource   |
| `deviceId`                  | `device.id`                                |            Y            | data point |
| `metricType`                | `metric.name`                              |                         | metric     |
| `value`                     | `data_point.value`                         |                         | data point |
| `origin`                    | `url.full`<br>`url.scheme`<br>`url.domain` |            Y            | data point |
| `path`                      | `url.path`                                 |            Y            | data point |
| `route`                     | `http.route`                               |            Y            | data point |
| `country`                   | `geo.country.iso_code`                     |            Y            | data point |
| `region`                    | `geo.region.iso_code`                      |            Y            | data point |
| `city`                      | `geo.locality.name`                        |            Y            | data point |
| `osName`                    | `user_agent.os.name`                       |            Y            | data point |
| `osVersion`                 | `user_agent.os.version`                    |            Y            | data point |
| `clientName`                | `user_agent.name`                          |            Y            | data point |
| `clientType`                | `vercel.client.type`                       |                         | data point |
| `clientVersion`             | `user_agent.version`                       |            Y            | data point |
| `deviceType`                | `vercel.device.type`                       |                         | data point |
| `deviceBrand`               | `device.manufacturer`                      |            Y            | data point |
| `connectionSpeed`           | `vercel.connection.speed`                  |                         | data point |
| `browserEngine`             | `vercel.browser.engine.name`               |                         | data point |
| `browserEngineVersion`      | `vercel.browser.engine.version`            |                         | data point |
| `scriptVersion`             | `vercel.speed_insights.script.version`     |                         | scope      |
| `sdkVersion`                | `vercel.speed_insights.sdk.version`        |                         | scope      |
| `sdkName`                   | `vercel.speed_insights.sdk.name`           |                         | scope      |
| `vercelEnvironment`         | `deployment.environment.name`              |            Y            | resource   |
| `vercelUrl`                 | `vercel.url`                               |                         | resource   |
| `deploymentId`              | `deployment.id`                            |            Y            | resource   |
| `attribution`               | `vercel.speed_insights.attribution`        |                         | data point |

The Speed Insights `timestamp` accepts RFC 3339 timestamps as shown in Vercel's
schema reference and ISO local date-times with either a `T` or space separator.
Because a local date-time has no timezone, it is interpreted as UTC.

## Web analytics schema

Web analytics schema can be found in https://vercel.com/docs/drains/reference/analytics#web-analytics-schema.

| Original Vercel schema name | OTel attribute name                        | In semantic conventions | Context  |
|-----------------------------|--------------------------------------------|:-----------------------:|----------|
| `schema`                    | `encoding.format`                          |                         | scope    |
| `eventType`                 | `vercel.analytics.event.type`              |                         | record   |
| `eventName`                 | `vercel.analytics.event.name`              |                         | record   |
| `eventData`                 | `vercel.analytics.event.data`              |                         | record   |
| `timestamp`                 | `data_point.time_unix_nano`                |                         | record   |
| `projectId`                 | `vercel.project.id`                        |                         | resource |
| `ownerId`                   | `vercel.owner.id`                          |                         | resource |
| `deviceId`                  | `device.id`                                |            Y            | record   |
| `origin`                    | `url.full`<br>`url.scheme`<br>`url.domain` |            Y            | record   |
| `path`                      | `url.path`                                 |            Y            | record   |
| `referrer`                  | `vercel.referrer`                          |                         | record   |
| `queryParams`               | `url.query`                                |            Y            | record   |
| `route`                     | `http.route`                               |            Y            | record   |
| `country`                   | `geo.country.iso_code`                     |            Y            | record   |
| `region`                    | `geo.region.iso_code`                      |            Y            | record   |
| `city`                      | `geo.locality.name`                        |            Y            | record   |
| `osName`                    | `user_agent.os.name`                       |            Y            | record   |
| `osVersion`                 | `user_agent.os.version`                    |            Y            | record   |
| `clientName`                | `user_agent.name`                          |            Y            | record   |
| `clientType`                | `vercel.client.type`                       |                         | record   |
| `clientVersion`             | `user_agent.version`                       |            Y            | record   |
| `deviceType`                | `vercel.device.type`                       |                         | record   |
| `deviceBrand`               | `device.manufacturer`                      |            Y            | record   |
| `deviceModel`               | `device.model.name`                        |            Y            | record   |
| `browserEngine`             | `vercel.browser.engine.name`               |                         | record   |
| `browserEngineVersion`      | `vercel.browser.engine.version`            |                         | record   |
| `sdkVersion`                | `vercel.web_analytics.sdk.version`         |                         | scope    |
| `sdkName`                   | `vercel.web_analytics.sdk.name`            |                         | scope    |
| `sdkVersionFull`            | `vercel.web_analytics.sdk.version_full`    |                         | scope    |
| `vercelEnvironment`         | `deployment.environment.name`              |            Y            | resource |
| `vercelUrl`                 | `vercel.url`                               |                         | resource |
| `flags`                     | `vercel.analytics.flags`                   |                         | record   |
| `deployment`                | `deployment.id`                            |            Y            | resource |

## Logs schema

Logs schema can be found in [Vercel's docs](https://vercel.com/docs/drains/reference/logs#logs-schema).

Top-level `host` emits `server.address`. Top-level `path` represents the Vercel route shape and emits `http.route`.
Proxy-specific fields are preserved under the nested `vercel.proxy` map. Fields promoted to semconv attributes are not duplicated there:
`proxy.method`, `proxy.userAgent`, `proxy.clientIp`, `proxy.scheme`, and `proxy.responseByteSize`.
The `proxy.path` is the concrete request path. It is preserved as `vercel.proxy.path` and split into `url.path` and `url.query`.
Multiple `proxy.userAgent` values are joined into `user_agent.original`.

| Original Vercel schema name | OTel attribute name                   | In semantic conventions | Context  |
|-----------------------------|---------------------------------------|:-----------------------:|----------|
| `schema`                    | `encoding.format`                     |                         | scope    |
| `id`                        | `log.record.uid`                      |            Y            | record   |
| `deploymentId`              | `deployment.id`                       |            Y            | resource |
| `source`                    | `vercel.log.source`                   |                         | record   |
| `host`                      | `server.address`                      |            Y            | record   |
| `timestamp`                 | `log_record.time_unix_nano`           |                         | record   |
| `projectId`                 | `vercel.project.id`                   |                         | resource |
| `level`                     | `severity_number`, `severity_text`    |                         | record   |
| `message`                   | `body`                                |                         | record   |
| `buildId`                   | `vercel.log.build.id`                 |                         | record   |
| `entrypoint`                | `vercel.log.entrypoint`               |                         | record   |
| `destination`               | `vercel.log.destination`              |                         | record   |
| `path`                      | `http.route`                          |            Y            | record   |
| `type`                      | `vercel.log.type`                     |                         | record   |
| `statusCode`                | `http.response.status_code`           |            Y            | record   |
| `requestId`                 | `vercel.log.request.id`               |                         | record   |
| `environment`               | `deployment.environment.name`         |            Y            | resource |
| `branch`                    | `vercel.log.branch`                   |                         | record   |
| `ja3Digest`                 | `tls.client.ja3`                      |            Y            | record   |
| `ja4Digest`                 | `tls.client.ja4`                      |                         | record   |
| `edgeType`                  | `vercel.log.edge.type`                |                         | record   |
| `projectName`               | `vercel.project.name`                 |                         | resource |
| `executionRegion`           | `vercel.execution.region`             |                         | record   |
| `traceId` / `trace.id`      | `trace_id`                            |                         | record   |
| `spanId` / `span.id`        | `span_id`                             |                         | record   |
| `proxy.timestamp`           | `vercel.proxy.timestamp`              |                         | record   |
| `proxy.method`              | `http.request.method`                 |            Y            | record   |
| `proxy.host`                | `vercel.proxy.host`                   |                         | record   |
| `proxy.path`                | `url.path`, `url.query`               |            Y            | record   |
| `proxy.userAgent`           | `user_agent.original`                 |            Y            | record   |
| `proxy.referer`             | `vercel.proxy.referer`                |                         | record   |
| `proxy.region`              | `vercel.proxy.region`                 |                         | record   |
| `proxy.statusCode`          | `vercel.proxy.status_code`            |                         | record   |
| `proxy.clientIp`            | `client.address`                      |            Y            | record   |
| `proxy.scheme`              | `url.scheme`                          |            Y            | record   |
| `proxy.responseByteSize`    | `http.response.body.size`             |            Y            | record   |
| `proxy.cacheId`             | `vercel.proxy.cache_id`               |                         | record   |
| `proxy.pathType`            | `vercel.proxy.path_type`              |                         | record   |
| `proxy.pathTypeVariant`     | `vercel.proxy.path_type_variant`      |                         | record   |
| `proxy.vercelId`            | `vercel.proxy.vercel_id`              |                         | record   |
| `proxy.vercelCache`         | `vercel.proxy.vercel_cache`           |                         | record   |
| `proxy.lambdaRegion`        | `vercel.proxy.lambda_region`          |                         | record   |
| `proxy.wafAction`           | `vercel.proxy.waf_action`             |                         | record   |
| `proxy.wafRuleId`           | `vercel.proxy.waf_rule_id`            |                         | record   |

## Audit logs schema

Audit logs schema can be found in https://vercel.com/docs/drains/reference/audit-logs#audit-log-schema.

| Original Vercel schema name | OTel attribute name         | In semantic conventions | Context    |
|-----------------------------|-----------------------------|:-----------------------:|------------|
| `schema`                    | `encoding.format`           |                         | scope      |
| `id`                        | `log.record.uid`            |            Y            | log record |
| `teamId`                    | `vercel.team.id`            |                         | resource   |
| `projectId`                 | `vercel.project.id`         |                         | resource   |
| `action`                    | `eventName`                 |            Y            | log record |
| `timestamp`                 | `log_record.time_unix_nano` |                         | log record |
| `actor.type`                | `vercel.actor.type`         |                         | log record |
| `actor.id`                  | `user.id`                   |            Y            | log record |
| `actor.name`                | `user.name`                 |            Y            | log record |
| `actor.email`               | `user.email`                |            Y            | log record |
| `via[].type`                | `vercel.via[].type`         |                         | log record |
| `via[].id`                  | `vercel.via[].id`           |                         | log record |
| `via[].name`                | `vercel.via[].name`         |                         | log record |
| `via[].email`               | `vercel.via[].email`        |                         | log record |
| `requestId`                 | `vercel.request.id`         |                         | log record |
| `userAgent`                 | `user_agent.original`       |            Y            | log record |
| `ipAddress`                 | `client.address`            |            Y            | log record |
| `tokenId`                   | `vercel.token.id`           |                         | log record |
| `payload`                   | `vercel.audit_log.payload`  |                         | log record |

The `payload` object is action-specific and unbounded, so it is stored opaquely as a nested map under `vercel.audit_log.payload` rather than mapped field by field. The `action` value (`eventName`) is the discriminator a consumer uses to interpret it. Payload keys are normalized to snake_case, so `drainUrl` is written as `drain_url`. The `via` delegation chain is stored as an array of objects, preserving every entry.
