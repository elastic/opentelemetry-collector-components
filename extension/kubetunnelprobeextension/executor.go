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

package kubetunnelprobeextension // import "github.com/elastic/opentelemetry-collector-components/extension/kubetunnelprobeextension"

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	memory "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
	corev1 "k8s.io/api/core/v1"

	kubetunnel "github.com/elastic/opentelemetry-collector-components/internal/kubetunnel"
)

// executor performs read-only Kubernetes queries. By construction it only ever
// gets, lists, or reads logs — there is no write path. The dynamic client +
// RESTMapper let it serve any resource type, the equivalent of `kubectl get -o json`.
type executor struct {
	dyn       dynamic.Interface
	clientset kubernetes.Interface
	discovery discovery.DiscoveryInterface
	mapper    *restmapper.DeferredDiscoveryRESTMapper

	allowedNamespaces map[string]struct{}
}

// newExecutor builds Kubernetes clients. An empty kubeconfig path uses in-cluster
// configuration.
func newExecutor(kubeconfig string, allowedNamespaces []string) (*executor, error) {
	cfg, err := restConfig(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("building kube config: %w", err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("dynamic client: %w", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("clientset: %w", err)
	}
	dc := cs.Discovery()
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc))

	var allowed map[string]struct{}
	if len(allowedNamespaces) > 0 {
		allowed = make(map[string]struct{}, len(allowedNamespaces))
		for _, ns := range allowedNamespaces {
			allowed[ns] = struct{}{}
		}
	}
	return &executor{dyn: dyn, clientset: cs, discovery: dc, mapper: mapper, allowedNamespaces: allowed}, nil
}

func restConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig == "" {
		return rest.InClusterConfig()
	}
	return clientcmd.BuildConfigFromFlags("", kubeconfig)
}

// serverVersion returns the cluster's Kubernetes version, or "" if unavailable.
func (e *executor) serverVersion() string {
	v, err := e.discovery.ServerVersion()
	if err != nil {
		return ""
	}
	return v.GitVersion
}

// Execute dispatches a read request. Anything outside the read-only allowlist is
// rejected before any API call is made.
func (e *executor) Execute(ctx context.Context, req kubetunnel.ReadRequest) kubetunnel.ReadResult {
	if req.Namespace != "" && e.allowedNamespaces != nil {
		if _, ok := e.allowedNamespaces[req.Namespace]; !ok {
			return errResult(fmt.Errorf("namespace %q is not in the allowed list", req.Namespace))
		}
	}
	switch req.Operation {
	case kubetunnel.OpGet, kubetunnel.OpList:
		return e.getOrList(ctx, req)
	case kubetunnel.OpLogs:
		return e.logs(ctx, req)
	default:
		return errResult(fmt.Errorf("operation %q is not allowed (read-only: get/list/logs)", req.Operation))
	}
}

func (e *executor) getOrList(ctx context.Context, req kubetunnel.ReadRequest) kubetunnel.ReadResult {
	if req.Resource == "" {
		return errResult(fmt.Errorf("resource is required for %s", req.Operation))
	}
	gvr, err := e.resolve(req.Resource)
	if err != nil {
		return errResult(err)
	}
	ri := e.dyn.Resource(gvr)
	var nri dynamic.ResourceInterface = ri
	if req.Namespace != "" {
		nri = ri.Namespace(req.Namespace)
	}

	if req.Operation == kubetunnel.OpGet {
		if req.Name == "" {
			return errResult(fmt.Errorf("name is required for get"))
		}
		obj, err := nri.Get(ctx, req.Name, metav1.GetOptions{})
		if err != nil {
			return errResult(err)
		}
		return marshalResult(obj)
	}

	list, err := nri.List(ctx, metav1.ListOptions{
		LabelSelector: req.LabelSelector,
		FieldSelector: req.FieldSelector,
	})
	if err != nil {
		return errResult(err)
	}
	return marshalResult(list)
}

// resolve turns a resource string ("pods", "deployments.apps") into a fully
// qualified GroupVersionResource using the discovery-backed RESTMapper.
func (e *executor) resolve(resource string) (schema.GroupVersionResource, error) {
	gr := schema.ParseGroupResource(resource)
	gvr, err := e.mapper.ResourceFor(gr.WithVersion(""))
	if err != nil {
		return schema.GroupVersionResource{}, fmt.Errorf("resolving resource %q: %w", resource, err)
	}
	return gvr, nil
}

func (e *executor) logs(ctx context.Context, req kubetunnel.ReadRequest) kubetunnel.ReadResult {
	if req.Namespace == "" || req.Name == "" {
		return errResult(fmt.Errorf("namespace and name (pod) are required for logs"))
	}
	opts := &corev1.PodLogOptions{Container: req.Container}
	if req.TailLines > 0 {
		tl := req.TailLines
		opts.TailLines = &tl
	}
	stream, err := e.clientset.CoreV1().Pods(req.Namespace).GetLogs(req.Name, opts).Stream(ctx)
	if err != nil {
		return errResult(err)
	}
	defer stream.Close()
	data, err := io.ReadAll(stream)
	if err != nil {
		return errResult(err)
	}
	// Logs are raw text; wrap as a JSON string so ReadResult.JSON is always valid
	// JSON.
	encoded, err := json.Marshal(string(data))
	if err != nil {
		return errResult(err)
	}
	return kubetunnel.ReadResult{JSON: encoded}
}

type jsonMarshaler interface{ MarshalJSON() ([]byte, error) }

func marshalResult(obj jsonMarshaler) kubetunnel.ReadResult {
	data, err := obj.MarshalJSON()
	if err != nil {
		return errResult(err)
	}
	return kubetunnel.ReadResult{JSON: data}
}

func errResult(err error) kubetunnel.ReadResult {
	return kubetunnel.ReadResult{Error: err.Error()}
}
