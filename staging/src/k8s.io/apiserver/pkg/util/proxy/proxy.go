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

package proxy

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/audit"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	listersv1 "k8s.io/client-go/listers/core/v1"
)

const (
	// taken from https://github.com/kubernetes/kubernetes/blob/release-1.27/staging/src/k8s.io/kube-aggregator/pkg/apiserver/handler_proxy.go#L47
	// discovery 请求被假定为廉价且快速；短超时可避免某个故障的聚合 apiserver
	// 拖慢所有客户端的发现过程。
	aggregatedDiscoveryTimeout = 5 * time.Second
)

// findServicePort finds the service port by name or numerically.
func findServicePort(svc *v1.Service, port int32) (*v1.ServicePort, error) {
	for _, svcPort := range svc.Spec.Ports {
		if svcPort.Port == port {
			return &svcPort, nil
		}
	}
	return nil, errors.NewServiceUnavailable(fmt.Sprintf("no service port %d found for service %q", port, svc.Name))
}

// ResolveEndpoint returns a URL to which one can send traffic for the specified service.
// If the service is dual-stack, the URL will preferentially point to an endpoint of the
// service's primary IP family.
//
// ResolveEndpoint 走 EndpointSlice 而不是 Service ClusterIP：
// 这样流量可以直接打到某个后端 Pod，绕过 kube-proxy，便于聚合层做请求分发。
func ResolveEndpoint(services listersv1.ServiceLister, endpointSlices EndpointSliceGetter, namespace, id string, port int32) (*url.URL, error) {
	svc, err := services.Services(namespace).Get(id)
	if err != nil {
		return nil, err
	}

	switch {
	case svc.Spec.Type == v1.ServiceTypeClusterIP, svc.Spec.Type == v1.ServiceTypeLoadBalancer, svc.Spec.Type == v1.ServiceTypeNodePort:
		// these are fine
		// ExternalName 没有可解析的后端，因此不支持。
	default:
		return nil, fmt.Errorf("unsupported service type %q", svc.Spec.Type)
	}

	svcPort, err := findServicePort(svc, port)
	if err != nil {
		return nil, err
	}

	if endpointSlices == nil {
		return nil, errors.NewServiceUnavailable(fmt.Sprintf("no endpoints available for service %q", svc.Name))
	}
	slices, err := endpointSlices.GetEndpointSlices(namespace, svc.Name)
	if err != nil {
		return nil, err
	}
	preferredIPFamily := v1.IPFamilyUnknown
	// 双栈 Service 优先使用第一（主）IP 族，保证与客户端所在网络一致。
	if len(svc.Spec.IPFamilies) > 0 {
		preferredIPFamily = svc.Spec.IPFamilies[0]
	}
	hostPort, err := RandomEndpoint(svc.Name, slices, svcPort.Name, preferredIPFamily, nil)
	if err != nil {
		return nil, err
	}

	// 聚合层统一使用 https；证书校验由调用方的 transport 负责。
	return &url.URL{
		Scheme: "https",
		Host:   hostPort,
	}, nil
}

// RandomEndpoint returns an "IPv4:port"/"[IPv6]:port" from a randomly-selected endpoint
// in slices that serves svcPortName, preferably of preferredIPFamily. If validEndpoint is
// non-nil then it must also accept the selected endpoint.
//
// RandomEndpoint 的随机化用于在多个后端之间分摊负载；
// 优先族优先保证双栈环境下不会随机选到不可达的地址族。
func RandomEndpoint(svcName string, slices []*discoveryv1.EndpointSlice, svcPortName string, preferredIPFamily v1.IPFamily, validEndpoint func(*discoveryv1.Endpoint) bool) (string, error) {
	if len(slices) > 1 {
		// If there are multiple slices, we want to look at them in a random
		// order, but we need to look at all of the slices of the preferred IP
		// family first.
		// 排序键 = (是否为主 IP 族, 随机序号)，一次排序同时满足两个偏好。
		preferredAddressType := discoveryv1.AddressType(preferredIPFamily)
		randomOrder := rand.Perm(len(slices))
		sort.Slice(slices, func(i, j int) bool {
			if slices[i].AddressType != slices[j].AddressType {
				return slices[i].AddressType == preferredAddressType
			}
			return randomOrder[i] < randomOrder[j]
		})
	}

	// Find a slice that has the port.
	for _, slice := range slices {
		for i := range slice.Ports {
			if slice.Ports[i].Name == nil || *slice.Ports[i].Name != svcPortName {
				continue
			}
			if slice.Ports[i].Port == nil {
				continue
			}
			if len(slice.Endpoints) == 0 {
				continue
			}

			// Starting from a random index, find a Ready endpoint
			// 从随机偏移开始遍历，使同一 slice 内各端点也被均匀选中。
			offset := rand.Intn(len(slice.Endpoints))
			for epi := range slice.Endpoints {
				ep := &slice.Endpoints[(epi+offset)%len(slice.Endpoints)]
				// Ready==nil 视为就绪（兼容旧端点），显式 false 则跳过。
				if ep.Conditions.Ready != nil && !*ep.Conditions.Ready {
					continue
				}
				if validEndpoint != nil && !validEndpoint(ep) {
					continue
				}

				// (Addresses is an array but only Addresses[0] is used.)
				ip := ep.Addresses[0]
				port := int(*slice.Ports[i].Port)
				return net.JoinHostPort(ip, strconv.Itoa(port)), nil
			}
		}
	}

	return "", errors.NewServiceUnavailable(fmt.Sprintf("no endpoints available for service %q", svcName))
}

// ResolveCluster 返回指向 Service ClusterIP（或 ExternalName）的 URL，
// 适合不需要直接寻址后端 Pod 的场景。
func ResolveCluster(services listersv1.ServiceLister, namespace, id string, port int32) (*url.URL, error) {
	svc, err := services.Services(namespace).Get(id)
	if err != nil {
		return nil, err
	}

	switch {
	case svc.Spec.Type == v1.ServiceTypeClusterIP && svc.Spec.ClusterIP == v1.ClusterIPNone:
		// headless service 没有 ClusterIP，无法按此方式路由。
		return nil, fmt.Errorf(`cannot route to service with ClusterIP "None"`)
	// use IP from a clusterIP for these service types
	case svc.Spec.Type == v1.ServiceTypeClusterIP, svc.Spec.Type == v1.ServiceTypeLoadBalancer, svc.Spec.Type == v1.ServiceTypeNodePort:
		svcPort, err := findServicePort(svc, port)
		if err != nil {
			return nil, err
		}
		return &url.URL{
			Scheme: "https",
			Host:   net.JoinHostPort(svc.Spec.ClusterIP, fmt.Sprintf("%d", svcPort.Port)),
		}, nil
	case svc.Spec.Type == v1.ServiceTypeExternalName:
		// ExternalName 直接使用 DNS 名并保留请求的 port（无法从 Service 定义中解析端口）。
		return &url.URL{
			Scheme: "https",
			Host:   net.JoinHostPort(svc.Spec.ExternalName, fmt.Sprintf("%d", port)),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported service type %q", svc.Spec.Type)
	}
}

// NewRequestForProxy returns a shallow copy of the original request with a context that may include a timeout for discovery requests
//
// NewRequestForProxy 创建转发请求：复制 header 以允许修改而不影响原请求，
// 并把目标 URL/Host 指向被代理服务。返回的 cancel 必须由调用方在响应结束后调用，
// 否则 discovery 的超时 context 会泄漏。
func NewRequestForProxy(location *url.URL, req *http.Request) (*http.Request, context.CancelFunc) {
	newCtx := req.Context()
	cancelFn := func() {}

	if requestInfo, ok := genericapirequest.RequestInfoFrom(req.Context()); ok {
		// trim leading and trailing slashes. Then "/apis/group/version" requests are for discovery, so if we have exactly three
		// segments that we are going to proxy, we have a discovery request.
		// 仅非资源请求且路径形如 /apis/<group>/<version> 时才施加短超时。
		if !requestInfo.IsResourceRequest && len(strings.Split(strings.Trim(requestInfo.Path, "/"), "/")) == 3 {
			// discovery requests are used by kubectl and others to determine which resources a server has.  This is a cheap call that
			// should be fast for every aggregated apiserver.  Latency for aggregation is expected to be low (as for all extensions)
			// so forcing a short timeout here helps responsiveness of all clients.
			newCtx, cancelFn = context.WithTimeout(newCtx, aggregatedDiscoveryTimeout)
		}
	}

	// WithContext creates a shallow clone of the request with the same context.
	newReq := req.WithContext(newCtx)
	newReq.Header = utilnet.CloneHeader(req.Header)
	newReq.URL = location
	newReq.Host = location.Host

	// If the original request has an audit ID, let's make sure we propagate this
	// to the aggregated server.
	// 传播审计 ID 让聚合层与后端 apiserver 的日志可以关联同一次用户请求。
	if auditID, found := audit.AuditIDFrom(req.Context()); found {
		newReq.Header.Set(auditinternal.HeaderAuditID, string(auditID))
	}

	return newReq, cancelFn
}
