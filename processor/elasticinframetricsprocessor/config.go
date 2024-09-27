// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package elasticinframetricsprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/elasticinframetricsprocessor"

type Config struct {
	AddSystemMetrics bool `mapstructure:"add_system_metrics"`
	AddK8sMetrics    bool `mapstructure:"add_k8s_metrics"`
	Override         bool `mapstructure:"override"`
}

func (c *Config) Validate() error {
	return nil
}
