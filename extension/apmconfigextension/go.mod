module github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension

go 1.22.7

toolchain go1.23.4

require (
	github.com/elastic/opentelemetry-lib v0.14.1-0.20250203070458-17aa3e31dddf
	github.com/stretchr/testify v1.10.0
	go.opentelemetry.io/collector/component v0.118.0
	go.opentelemetry.io/collector/component/componenttest v0.118.0
	go.opentelemetry.io/collector/confmap v1.24.0
	go.opentelemetry.io/collector/extension v0.118.0
	go.opentelemetry.io/collector/extension/extensiontest v0.117.0
	go.uber.org/goleak v1.3.0
)

require (
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/elastic/elastic-transport-go/v8 v8.6.0 // indirect
	github.com/elastic/go-elasticsearch/v8 v8.17.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.8.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/rs/cors v1.11.1 // indirect
	go.opentelemetry.io/collector/client v1.24.0 // indirect
	go.opentelemetry.io/collector/config/configauth v0.118.0 // indirect
	go.opentelemetry.io/collector/config/configcompression v1.24.0 // indirect
	go.opentelemetry.io/collector/config/confighttp v0.118.0 // indirect
	go.opentelemetry.io/collector/config/configopaque v1.24.0 // indirect
	go.opentelemetry.io/collector/config/configtls v1.24.0 // indirect
	go.opentelemetry.io/collector/extension/auth v0.118.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.56.0 // indirect
	go.opentelemetry.io/otel/sdk v1.32.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.32.0 // indirect
)

require (
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/go-viper/mapstructure/v2 v2.2.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/knadh/koanf/maps v0.1.1 // indirect
	github.com/knadh/koanf/providers/confmap v0.1.0 // indirect
	github.com/knadh/koanf/v2 v2.1.2 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/open-telemetry/opamp-go v0.19.0
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.118.0 // indirect
	go.opentelemetry.io/collector/pdata v1.24.0 // indirect
	go.opentelemetry.io/collector/semconv v0.118.0
	go.opentelemetry.io/otel v1.32.0 // indirect
	go.opentelemetry.io/otel/metric v1.32.0 // indirect
	go.opentelemetry.io/otel/trace v1.32.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0
	golang.org/x/net v0.34.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250102185135-69823020774d // indirect
	google.golang.org/grpc v1.69.4 // indirect
	google.golang.org/protobuf v1.36.3 // indirect
	gopkg.in/yaml.v3 v3.0.1
)

replace github.com/open-telemetry/opamp-go => /home/neck/Documents/github/rogercoll/opamp-go/
