module github.com/elastic/opentelemetry-collector-components/connector/elasticapmconnector

go 1.24.0

require (
	github.com/elastic/opentelemetry-collector-components/internal/sharedcomponent v0.0.0-20250220025958-386ba0c4bced
	github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor v0.8.0
	github.com/open-telemetry/opentelemetry-collector-contrib/connector/signaltometricsconnector v0.137.0
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden v0.137.0
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest v0.137.0
	github.com/stretchr/testify v1.11.1
	go.opentelemetry.io/collector/client v1.43.0
	go.opentelemetry.io/collector/component v1.43.0
	go.opentelemetry.io/collector/component/componenttest v0.137.0
	go.opentelemetry.io/collector/config/configoptional v1.43.0
	go.opentelemetry.io/collector/confmap v1.43.0
	go.opentelemetry.io/collector/confmap/xconfmap v0.137.0
	go.opentelemetry.io/collector/connector v0.137.0
	go.opentelemetry.io/collector/connector/connectortest v0.137.0
	go.opentelemetry.io/collector/consumer v1.43.0
	go.opentelemetry.io/collector/consumer/consumertest v0.137.0
	go.opentelemetry.io/collector/pdata v1.43.0
	go.opentelemetry.io/collector/pipeline v1.43.0
	go.opentelemetry.io/collector/processor v1.43.0
	go.uber.org/goleak v1.3.0
)

require (
	github.com/DataDog/zstd v1.5.6 // indirect
	github.com/alecthomas/participle/v2 v2.1.4 // indirect
	github.com/antchfx/xmlquery v1.4.4 // indirect
	github.com/antchfx/xpath v1.3.5 // indirect
	github.com/axiomhq/hyperloglog v0.2.5 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cockroachdb/errors v1.11.3 // indirect
	github.com/cockroachdb/fifo v0.0.0-20240816210425-c5d0cb0b6fc0 // indirect
	github.com/cockroachdb/logtags v0.0.0-20241215232642-bb51bb14a506 // indirect
	github.com/cockroachdb/pebble v1.1.5 // indirect
	github.com/cockroachdb/redact v1.1.6 // indirect
	github.com/cockroachdb/tokenbucket v0.0.0-20230807174530-cc333fc44b06 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgryski/go-metro v0.0.0-20180109044635-280f6062b5bc // indirect
	github.com/elastic/go-grok v0.3.1 // indirect
	github.com/elastic/lunes v0.1.0 // indirect
	github.com/getsentry/sentry-go v0.31.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/go-version v1.7.0 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/iancoleman/strcase v0.3.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kamstrup/intmap v0.5.1 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/knadh/koanf/maps v0.1.2 // indirect
	github.com/knadh/koanf/providers/confmap v1.0.0 // indirect
	github.com/knadh/koanf/v2 v2.3.0 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lightstep/go-expohisto v1.0.0 // indirect
	github.com/magefile/mage v1.15.0 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal v0.137.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl v0.137.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil v0.137.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/sampling v0.137.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_golang v1.20.5 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.62.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rogpeppe/go-internal v1.13.1 // indirect
	github.com/twmb/murmur3 v1.1.8 // indirect
	github.com/ua-parser/uap-go v0.0.0-20240611065828-3a4781585db6 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/collector/component/componentstatus v0.137.0 // indirect
	go.opentelemetry.io/collector/connector/xconnector v0.137.0 // indirect
	go.opentelemetry.io/collector/consumer/xconsumer v0.137.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.43.0 // indirect
	go.opentelemetry.io/collector/internal/fanoutconsumer v0.137.0 // indirect
	go.opentelemetry.io/collector/internal/telemetry v0.137.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.137.0 // indirect
	go.opentelemetry.io/collector/pipeline/xpipeline v0.137.0 // indirect
	go.opentelemetry.io/contrib/bridges/otelzap v0.13.0 // indirect
	go.opentelemetry.io/otel v1.38.0 // indirect
	go.opentelemetry.io/otel/log v0.14.0 // indirect
	go.opentelemetry.io/otel/metric v1.38.0 // indirect
	go.opentelemetry.io/otel/sdk v1.38.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.38.0 // indirect
	go.opentelemetry.io/otel/trace v1.38.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/exp v0.0.0-20250215185904-eff6e970281f // indirect
	golang.org/x/net v0.44.0 // indirect
	golang.org/x/sys v0.36.0 // indirect
	golang.org/x/text v0.29.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250707201910-8d1bb00bc6a7 // indirect
	google.golang.org/grpc v1.75.1 // indirect
	google.golang.org/protobuf v1.36.9 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/elastic/opentelemetry-collector-components/internal/sharedcomponent => ../../internal/sharedcomponent

replace github.com/elastic/opentelemetry-collector-components/processor/elastictraceprocessor => ../../processor/elastictraceprocessor

replace github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor => ../../processor/lsmintervalprocessor
