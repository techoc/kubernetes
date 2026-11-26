/*
Copyright 2017 The Kubernetes Authors.

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

package apiserver

import (
	"net/url"

	"k8s.io/apiserver/pkg/util/proxy"
	listersv1 "k8s.io/client-go/listers/core/v1"
)

// ServiceResolver 负责把 Service 解析成可访问的 URL。
// 把“Service 引用 -> 可访问的 URL”抽象成接口，是为了让代理层不必关心
// 到底走 EndpointSlice 直连、ClusterIP 还是 loopback；不同部署形态可注入不同实现。
type ServiceResolver interface {
	ResolveEndpoint(namespace, name string, port int32) (*url.URL, error)
}

// NewEndpointServiceResolver 返回一个从 Service 端点中选一个进行访问的解析器。
// 走 EndpointSlice 直连后端 Pod，绕过 Service 的 ClusterIP 与 kube-proxy；
// 需要同时拿到 Service（判断端口）和 EndpointSlice（找可用地址）两个 lister。
func NewEndpointServiceResolver(services listersv1.ServiceLister, endpointSliceGetter proxy.EndpointSliceGetter) ServiceResolver {
	return &aggregatorEndpointRouting{
		services:            services,
		endpointSliceGetter: endpointSliceGetter,
	}
}

type aggregatorEndpointRouting struct {
	services            listersv1.ServiceLister
	endpointSliceGetter proxy.EndpointSliceGetter
}

func (r *aggregatorEndpointRouting) ResolveEndpoint(namespace, name string, port int32) (*url.URL, error) {
	return proxy.ResolveEndpoint(r.services, r.endpointSliceGetter, namespace, name, port)
}

// NewClusterIPServiceResolver 返回一个直接访问 Service cluster IP 的解析器。
// 直接访问 Service 的 ClusterIP，依赖 kube-proxy 做负载均衡；
// 在无法或无需列举 EndpointSlice 的场景下使用，只需要 Service lister。
func NewClusterIPServiceResolver(services listersv1.ServiceLister) ServiceResolver {
	return &aggregatorClusterRouting{
		services: services,
	}
}

type aggregatorClusterRouting struct {
	services listersv1.ServiceLister
}

func (r *aggregatorClusterRouting) ResolveEndpoint(namespace, name string, port int32) (*url.URL, error) {
	return proxy.ResolveCluster(r.services, namespace, name, port)
}

// NewLoopbackServiceResolver 返回一个把 kubernetes/default:443 路由到 loopback 的解析器。
// default/kubernetes:443 指向的其实就是当前 apiserver 自身，
// 再走 Service 网络会绕远甚至在有网络分区时失败，因此直接回环到本地 host。
// 其余 Service 仍交给 delegate 按常规方式解析。
func NewLoopbackServiceResolver(delegate ServiceResolver, host *url.URL) ServiceResolver {
	return &loopbackResolver{
		delegate: delegate,
		host:     host,
	}
}

type loopbackResolver struct {
	delegate ServiceResolver
	host     *url.URL
}

// ResolveEndpoint 对 default/kubernetes:443 直接返回本地地址，避免无谓的网络跳转；
// 其余情况原样委托给底层 resolver。
func (r *loopbackResolver) ResolveEndpoint(namespace, name string, port int32) (*url.URL, error) {
	if namespace == "default" && name == "kubernetes" && port == 443 {
		return r.host, nil
	}
	return r.delegate.ResolveEndpoint(namespace, name, port)
}
