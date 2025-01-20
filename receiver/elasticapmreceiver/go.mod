module github.com/elastic/opentelemetry-collector-components/receiver/elasticapmreceiver

go 1.23.4

require (
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/elastic/apm-data v1.14.1
	github.com/elastic/opentelemetry-collector-components/internal/agentcfg v0.0.0-00010101000000-000000000000
	github.com/elastic/opentelemetry-collector-components/internal/elasticsearch v0.0.0-00010101000000-000000000000
	github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter v0.117.0
	github.com/stretchr/testify v1.10.0
	go.opentelemetry.io/collector/component v0.117.0
	go.opentelemetry.io/collector/component/componentstatus v0.111.0
	go.opentelemetry.io/collector/component/componenttest v0.117.0
	go.opentelemetry.io/collector/config/confighttp v0.117.0
	go.opentelemetry.io/collector/confmap v1.23.0
	go.opentelemetry.io/collector/consumer v1.23.0
	go.opentelemetry.io/collector/consumer/consumertest v0.117.0
	go.opentelemetry.io/collector/pdata v1.23.0
	go.opentelemetry.io/collector/receiver v0.117.0
	go.opentelemetry.io/collector/receiver/receivertest v0.117.0
	go.uber.org/goleak v1.3.0
	go.uber.org/zap v1.27.0
	golang.org/x/sync v0.10.0
)

require (
	github.com/armon/go-radix v1.0.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/elastic/beats/v7 v7.17.27 // indirect
	github.com/elastic/elastic-agent-libs v0.18.1 // indirect
	github.com/elastic/elastic-transport-go/v8 v8.6.0 // indirect
	github.com/elastic/go-docappender/v2 v2.3.3 // indirect
	github.com/elastic/go-elasticsearch/v7 v7.17.10 // indirect
	github.com/elastic/go-elasticsearch/v8 v8.17.0 // indirect
	github.com/elastic/go-structform v0.0.12 // indirect
	github.com/elastic/go-sysinfo v1.14.0 // indirect
	github.com/elastic/go-ucfg v0.8.6 // indirect
	github.com/elastic/go-windows v1.0.1 // indirect
	github.com/elastic/opentelemetry-lib v0.12.0 // indirect
	github.com/elastic/pkcs8 v1.0.0 // indirect
	github.com/fatih/color v1.16.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.8.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-viper/mapstructure/v2 v2.2.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-version v1.7.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/knadh/koanf/maps v0.1.1 // indirect
	github.com/knadh/koanf/providers/confmap v0.1.0 // indirect
	github.com/knadh/koanf/v2 v2.1.2 // indirect
	github.com/lestrrat-go/strftime v1.1.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/common v0.117.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal v0.117.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rs/cors v1.11.1 // indirect
	go.elastic.co/apm/module/apmzap/v2 v2.6.2 // indirect
	go.elastic.co/apm/v2 v2.6.2 // indirect
	go.elastic.co/ecszap v1.0.2 // indirect
	go.elastic.co/fastjson v1.4.0 // indirect
	go.opentelemetry.io/collector/client v1.23.0 // indirect
	go.opentelemetry.io/collector/config/configauth v0.117.0 // indirect
	go.opentelemetry.io/collector/config/configcompression v1.23.0 // indirect
	go.opentelemetry.io/collector/config/configopaque v1.23.0 // indirect
	go.opentelemetry.io/collector/config/configretry v1.23.0 // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.117.0 // indirect
	go.opentelemetry.io/collector/config/configtls v1.23.0 // indirect
	go.opentelemetry.io/collector/consumer/consumererror v0.117.0 // indirect
	go.opentelemetry.io/collector/consumer/xconsumer v0.117.0 // indirect
	go.opentelemetry.io/collector/exporter v0.117.0 // indirect
	go.opentelemetry.io/collector/extension v0.117.0 // indirect
	go.opentelemetry.io/collector/extension/auth v0.117.0 // indirect
	go.opentelemetry.io/collector/extension/xextension v0.117.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.23.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.117.0 // indirect
	go.opentelemetry.io/collector/pipeline v0.117.0 // indirect
	go.opentelemetry.io/collector/receiver/xreceiver v0.117.0 // indirect
	go.opentelemetry.io/collector/semconv v0.117.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.56.0 // indirect
	go.opentelemetry.io/otel v1.32.0 // indirect
	go.opentelemetry.io/otel/metric v1.32.0 // indirect
	go.opentelemetry.io/otel/sdk v1.32.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.32.0 // indirect
	go.opentelemetry.io/otel/trace v1.32.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.32.0 // indirect
	golang.org/x/net v0.34.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241104194629-dd2ea8efbc28 // indirect
	google.golang.org/grpc v1.69.2 // indirect
	google.golang.org/protobuf v1.36.2 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	howett.net/plist v1.0.1 // indirect
)

replace github.com/elastic/opentelemetry-collector-components/internal/elasticsearch => ../../internal/elasticsearch/

replace github.com/elastic/opentelemetry-collector-components/internal/agentcfg => ../../internal/agentcfg/
