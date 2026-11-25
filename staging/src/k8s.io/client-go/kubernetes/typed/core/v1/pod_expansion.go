/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	"context"

	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/net"
	"k8s.io/client-go/kubernetes/scheme"
	restclient "k8s.io/client-go/rest"
)

// The PodExpansion interface allows manually adding extra methods to the PodInterface.
//
// PodExpansion 用于向代码生成的 PodInterface 追加手写方法。这些操作要么是
// 非标准的 REST 动词（如绑定、驱逐），要么端点位于外部服务（logs、proxy），
// 因此不适合由 openapi 生成器统一产出。
type PodExpansion interface {
	// Bind 把 Binding 提交到 pod 的 binding 子资源，由调度器用来完成绑定。
	Bind(ctx context.Context, binding *v1.Binding, opts metav1.CreateOptions) error
	// Evict submits a policy/v1beta1 Eviction request to the pod's eviction subresource.
	// Equivalent to calling EvictV1beta1.
	// Deprecated: 请改用 EvictV1()（1.22+ 起支持）或 EvictV1beta1()。
	Evict(ctx context.Context, eviction *policyv1beta1.Eviction) error
	// EvictV1 submits a policy/v1 Eviction request to the pod's eviction subresource.
	// Supported in 1.22+.
	EvictV1(ctx context.Context, eviction *policyv1.Eviction) error
	// EvictV1beta1 submits a policy/v1beta1 Eviction request to the pod's eviction subresource.
	// Supported in 1.22+.
	EvictV1beta1(ctx context.Context, eviction *policyv1beta1.Eviction) error
	// GetLogs 构造读取容器日志的请求，调用方需自行调用 Do 或 Stream。
	GetLogs(name string, opts *v1.PodLogOptions) *restclient.Request
	// ProxyGet 通过 kubelet 的 pod proxy 子资源发起 GET 请求。
	ProxyGet(scheme, name, port, path string, params map[string]string) restclient.ResponseWrapper
}

// Bind applies the provided binding to the named pod in the current namespace (binding.Namespace is ignored).
//
// 使用 binding.Name 而非 binding.Namespace 定位 Pod，因为 binding 子资源
// 的路由已经由客户端的命名空间确定；Binding 自身的 namespace 字段不参与路由。
func (c *pods) Bind(ctx context.Context, binding *v1.Binding, opts metav1.CreateOptions) error {
	return c.GetClient().Post().Namespace(c.GetNamespace()).Resource("pods").Name(binding.Name).VersionedParams(&opts, scheme.ParameterCodec).SubResource("binding").Body(binding).Do(ctx).Error()
}

// Evict submits a policy/v1beta1 Eviction request to the pod's eviction subresource.
// Equivalent to calling EvictV1beta1.
// Deprecated: 请改用 EvictV1()（1.22+ 起支持）或 EvictV1beta1()。
func (c *pods) Evict(ctx context.Context, eviction *policyv1beta1.Eviction) error {
	return c.GetClient().Post().Namespace(c.GetNamespace()).Resource("pods").Name(eviction.Name).SubResource("eviction").Body(eviction).Do(ctx).Error()
}

// EvictV1beta1 以 policy/v1beta1 Eviction 请求驱逐 Pod。
//
// 与 Evict 行为一致，保留独立方法是为了调用方可以显式表达所用的 API 版本。
func (c *pods) EvictV1beta1(ctx context.Context, eviction *policyv1beta1.Eviction) error {
	return c.GetClient().Post().Namespace(c.GetNamespace()).Resource("pods").Name(eviction.Name).SubResource("eviction").Body(eviction).Do(ctx).Error()
}

// EvictV1 以 policy/v1 Eviction 请求驱逐 Pod（1.22+ 推荐写法）。
func (c *pods) EvictV1(ctx context.Context, eviction *policyv1.Eviction) error {
	return c.GetClient().Post().Namespace(c.GetNamespace()).Resource("pods").Name(eviction.Name).SubResource("eviction").Body(eviction).Do(ctx).Error()
}

// Get constructs a request for getting the logs for a pod
//
// 这里只构造请求而不发送：调用方可选择 Do（一次性读取）或 Stream（跟随日志）。
func (c *pods) GetLogs(name string, opts *v1.PodLogOptions) *restclient.Request {
	return c.GetClient().Get().Namespace(c.GetNamespace()).Name(name).Resource("pods").SubResource("log").VersionedParams(opts, scheme.ParameterCodec)
}

// ProxyGet returns a response of the pod by calling it through the proxy.
//
// proxy 子资源的路径形如 /pods/{scheme}:{name}:{port}/{path}，因此需要把
// scheme、name、port 组合进 Name 字段；额外查询参数逐个追加到请求上。
func (c *pods) ProxyGet(scheme, name, port, path string, params map[string]string) restclient.ResponseWrapper {
	request := c.GetClient().Get().
		Namespace(c.GetNamespace()).
		Resource("pods").
		SubResource("proxy").
		Name(net.JoinSchemeNamePort(scheme, name, port)).
		Suffix(path)
	for k, v := range params {
		request = request.Param(k, v)
	}
	return request
}
