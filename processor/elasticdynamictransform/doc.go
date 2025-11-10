package elasticdynamictransform
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

// Package elasticdynamictransform provides a processor that dynamically loads
// transform processor configurations from the elasticpipeline extension.
//
// This processor acts as a wrapper around the transform processor, allowing
// transform configurations to be updated at runtime without restarting the
// OpenTelemetry Collector. It connects to the elasticpipeline extension to
// fetch processor configurations and watches for updates.
//
// Configuration example:
//
//	processors:
//	  elasticdynamictransform/stream_processing:
//	    extension: elasticpipeline
//	    processor_key: "transform/stream_processing"
//	    reload_interval: 30s
//	    fallback_mode: passthrough
package elasticdynamictransform
