module github.com/elastic/opentelemetry-collector-components/loadgen

go 1.23.8

require (
	github.com/elastic/go-elasticsearch/v8 v8.18.1
	github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor v0.0.0-00010101000000-000000000000
	github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver v0.0.0-00010101000000-000000000000
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor v0.130.0
	go.opentelemetry.io/collector/component v1.36.0
	go.opentelemetry.io/collector/confmap v1.36.0
	go.opentelemetry.io/collector/confmap/provider/envprovider v1.36.0
	go.opentelemetry.io/collector/confmap/provider/fileprovider v1.36.0
	go.opentelemetry.io/collector/confmap/provider/httpprovider v1.36.0
	go.opentelemetry.io/collector/confmap/provider/httpsprovider v1.36.0
	go.opentelemetry.io/collector/confmap/provider/yamlprovider v1.36.0
	go.opentelemetry.io/collector/connector v0.130.0
	go.opentelemetry.io/collector/exporter/debugexporter v0.130.0
	go.opentelemetry.io/collector/exporter/nopexporter v0.130.0
	go.opentelemetry.io/collector/exporter/otlpexporter v0.130.0
	go.opentelemetry.io/collector/exporter/otlphttpexporter v0.130.0
	go.opentelemetry.io/collector/extension v1.36.0
	go.opentelemetry.io/collector/otelcol v0.130.0
	go.opentelemetry.io/collector/processor v1.36.0
	go.opentelemetry.io/collector/receiver v1.36.0
	go.opentelemetry.io/collector/receiver/nopreceiver v0.130.0
)

require (
	github.com/OneOfOne/xxhash v1.2.8 // indirect
	github.com/alecthomas/participle/v2 v2.1.4 // indirect
	github.com/antchfx/xmlquery v1.4.4 // indirect
	github.com/antchfx/xpath v1.3.4 // indirect
	github.com/armon/go-metrics v0.4.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v5 v5.0.2 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/ebitengine/purego v0.8.4 // indirect
	github.com/elastic/elastic-transport-go/v8 v8.7.0 // indirect
	github.com/elastic/go-grok v0.3.1 // indirect
	github.com/elastic/lunes v0.1.0 // indirect
	github.com/elastic/opentelemetry-collector-components/internal/sharedcomponent v0.0.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/foxboron/go-tpm-keyfiles v0.0.0-20250323135004-b31fac66206e // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-viper/mapstructure/v2 v2.3.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/btree v1.1.1 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/go-tpm v0.9.5 // indirect
	github.com/google/gofuzz v1.1.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/googleapis/gnostic v0.5.5 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.1 // indirect
	github.com/gubernator-io/gubernator/v2 v2.13.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-msgpack v1.1.5 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-sockaddr v1.0.2 // indirect
	github.com/hashicorp/go-version v1.7.0 // indirect
	github.com/hashicorp/golang-lru v0.6.0 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/hashicorp/memberlist v0.5.0 // indirect
	github.com/iancoleman/strcase v0.3.0 // indirect
	github.com/imdario/mergo v0.3.5 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/knadh/koanf/maps v0.1.2 // indirect
	github.com/knadh/koanf/providers/confmap v1.0.0 // indirect
	github.com/knadh/koanf/v2 v2.2.1 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/magefile/mage v1.15.0 // indirect
	github.com/mailgun/errors v0.1.5 // indirect
	github.com/mailgun/holster/v4 v4.19.0 // indirect
	github.com/miekg/dns v1.1.50 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mostynb/go-grpc-compression v1.2.3 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal v0.130.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter v0.130.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/pdatautil v0.130.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl v0.130.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil v0.130.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_golang v1.22.0 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.65.0 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/rs/cors v1.11.1 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/segmentio/fasthash v1.0.2 // indirect
	github.com/shirou/gopsutil/v4 v4.25.6 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/spf13/cobra v1.9.1 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	github.com/stretchr/testify v1.10.0 // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/twmb/murmur3 v1.1.8 // indirect
	github.com/ua-parser/uap-go v0.0.0-20240611065828-3a4781585db6 // indirect
	github.com/uptrace/opentelemetry-go-extra/otellogrus v0.2.1 // indirect
	github.com/uptrace/opentelemetry-go-extra/otelutil v0.2.1 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.etcd.io/etcd/api/v3 v3.5.5 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.5 // indirect
	go.etcd.io/etcd/client/v3 v3.5.5 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/collector v0.130.0 // indirect
	go.opentelemetry.io/collector/client v1.36.0 // indirect
	go.opentelemetry.io/collector/component/componentstatus v0.130.0 // indirect
	go.opentelemetry.io/collector/component/componenttest v0.130.0 // indirect
	go.opentelemetry.io/collector/config/configauth v0.130.0 // indirect
	go.opentelemetry.io/collector/config/configcompression v1.36.0 // indirect
	go.opentelemetry.io/collector/config/configgrpc v0.130.0 // indirect
	go.opentelemetry.io/collector/config/confighttp v0.130.0 // indirect
	go.opentelemetry.io/collector/config/configmiddleware v0.130.0 // indirect
	go.opentelemetry.io/collector/config/confignet v1.36.0 // indirect
	go.opentelemetry.io/collector/config/configopaque v1.36.0 // indirect
	go.opentelemetry.io/collector/config/configoptional v0.130.0 // indirect
	go.opentelemetry.io/collector/config/configretry v1.36.0 // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.130.0 // indirect
	go.opentelemetry.io/collector/config/configtls v1.36.0 // indirect
	go.opentelemetry.io/collector/confmap/xconfmap v0.130.0 // indirect
	go.opentelemetry.io/collector/connector/connectortest v0.130.0 // indirect
	go.opentelemetry.io/collector/connector/xconnector v0.130.0 // indirect
	go.opentelemetry.io/collector/consumer v1.36.0 // indirect
	go.opentelemetry.io/collector/consumer/consumererror v0.130.0 // indirect
	go.opentelemetry.io/collector/consumer/consumererror/xconsumererror v0.130.0 // indirect
	go.opentelemetry.io/collector/consumer/consumertest v0.130.0 // indirect
	go.opentelemetry.io/collector/consumer/xconsumer v0.130.0 // indirect
	go.opentelemetry.io/collector/exporter v0.130.0 // indirect
	go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper v0.130.0 // indirect
	go.opentelemetry.io/collector/exporter/exportertest v0.130.0 // indirect
	go.opentelemetry.io/collector/exporter/xexporter v0.130.0 // indirect
	go.opentelemetry.io/collector/extension/extensionauth v1.36.0 // indirect
	go.opentelemetry.io/collector/extension/extensioncapabilities v0.130.0 // indirect
	go.opentelemetry.io/collector/extension/extensionmiddleware v0.130.0 // indirect
	go.opentelemetry.io/collector/extension/extensiontest v0.130.0 // indirect
	go.opentelemetry.io/collector/extension/xextension v0.130.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.36.0 // indirect
	go.opentelemetry.io/collector/internal/fanoutconsumer v0.130.0 // indirect
	go.opentelemetry.io/collector/internal/telemetry v0.130.0 // indirect
	go.opentelemetry.io/collector/pdata v1.36.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.130.0 // indirect
	go.opentelemetry.io/collector/pdata/testdata v0.130.0 // indirect
	go.opentelemetry.io/collector/pdata/xpdata v0.130.0 // indirect
	go.opentelemetry.io/collector/pipeline v0.130.0 // indirect
	go.opentelemetry.io/collector/pipeline/xpipeline v0.130.0 // indirect
	go.opentelemetry.io/collector/processor/processorhelper v0.130.0 // indirect
	go.opentelemetry.io/collector/processor/processorhelper/xprocessorhelper v0.130.0 // indirect
	go.opentelemetry.io/collector/processor/processortest v0.130.0 // indirect
	go.opentelemetry.io/collector/processor/xprocessor v0.130.0 // indirect
	go.opentelemetry.io/collector/receiver/receivertest v0.130.0 // indirect
	go.opentelemetry.io/collector/receiver/xreceiver v0.130.0 // indirect
	go.opentelemetry.io/collector/service v0.130.0 // indirect
	go.opentelemetry.io/collector/service/hostcapabilities v0.130.0 // indirect
	go.opentelemetry.io/contrib/bridges/otelzap v0.12.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.62.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.62.0 // indirect
	go.opentelemetry.io/contrib/otelconf v0.17.0 // indirect
	go.opentelemetry.io/contrib/propagators/b3 v1.37.0 // indirect
	go.opentelemetry.io/otel v1.37.0 // indirect
	go.opentelemetry.io/otel/exporters/jaeger v1.17.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc v0.13.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp v0.13.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.37.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.37.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.37.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.37.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.37.0 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.59.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutlog v0.13.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.37.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.37.0 // indirect
	go.opentelemetry.io/otel/log v0.13.0 // indirect
	go.opentelemetry.io/otel/metric v1.37.0 // indirect
	go.opentelemetry.io/otel/sdk v1.37.0 // indirect
	go.opentelemetry.io/otel/sdk/log v0.13.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.37.0 // indirect
	go.opentelemetry.io/otel/trace v1.37.0 // indirect
	go.opentelemetry.io/proto/otlp v1.7.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	go.yaml.in/yaml/v2 v2.4.2 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/crypto v0.39.0 // indirect
	golang.org/x/exp v0.0.0-20240506185415-9bf2ced13842 // indirect
	golang.org/x/mod v0.25.0 // indirect
	golang.org/x/net v0.41.0 // indirect
	golang.org/x/oauth2 v0.30.0 // indirect
	golang.org/x/sync v0.15.0 // indirect
	golang.org/x/sys v0.34.0 // indirect
	golang.org/x/term v0.32.0 // indirect
	golang.org/x/text v0.26.0 // indirect
	golang.org/x/time v0.11.0 // indirect
	golang.org/x/tools v0.33.0 // indirect
	gonum.org/v1/gonum v0.16.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250603155806-513f23925822 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250603155806-513f23925822 // indirect
	google.golang.org/grpc v1.73.0 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/api v0.23.3 // indirect
	k8s.io/apimachinery v0.23.3 // indirect
	k8s.io/client-go v0.23.3 // indirect
	k8s.io/klog/v2 v2.120.1 // indirect
	k8s.io/kube-openapi v0.0.0-20211115234752-e816edb12b65 // indirect
	k8s.io/utils v0.0.0-20211116205334-6203023598ed // indirect
	sigs.k8s.io/json v0.0.0-20211020170558-c049b76a60c6 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.2.1 // indirect
	sigs.k8s.io/yaml v1.5.0 // indirect
)

replace (
	github.com/elastic/opentelemetry-collector-components/internal/sharedcomponent => ../../../internal/sharedcomponent
	github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor => ../../../processor/ratelimitprocessor
	github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver => ../../../receiver/loadgenreceiver
)
