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

package healthcheck

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/lithammer/dedent"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2"
	api "k8s.io/kubernetes/pkg/apis/core"
	proxyutil "k8s.io/kubernetes/pkg/proxy/util"
)

// ServiceHealthServer 为每个 Service 提供 HTTP 健康检查端点，结果取决于本地端点数量：
// 没有端点时返回 503 Service Unavailable（告知负载均衡器不要使用本节点），
// 存在一个或多个端点时返回 200 OK。
type ServiceHealthServer interface {
	// SyncServices 使新的 Service 集合生效：关闭已不需要的、开启新增的，
	// 保留仍在新集合中的；map 的值为要监听的健康检查端口。
	SyncServices(newServices map[types.NamespacedName]uint16) error
	// SyncEndpoints 更新各 Service 的本地端点数；不存在于服务集合中的端点会被忽略。
	SyncEndpoints(newEndpoints map[types.NamespacedName]int) error
}

// proxyHealthChecker 提供代理的健康状态查询能力。
type proxyHealthChecker interface {
	// Health 返回代理的健康状态及上次更新时间。
	Health() ProxyHealth
}

// newServiceHealthServer 根据 IP 族选择监听地址和网络类型，构造可注入依赖的服务健康检查服务器。
func newServiceHealthServer(nodeName string, recorder events.EventRecorder, listener listener, factory httpServerFactory, nodePortAddresses *proxyutil.NodePortAddresses, healthzServer proxyHealthChecker, serviceFamily v1.IPFamily) ServiceHealthServer {
	// 监听地址与网络类型由 Service 的 IP 族决定；未知时交给系统选择。
	var nodeIPs []net.IP
	var network string

	switch serviceFamily {
	case v1.IPv4Protocol:
		nodeIPs = []net.IP{net.IPv4zero}
		network = "tcp4"
	case v1.IPv6Protocol:
		nodeIPs = []net.IP{net.IPv6zero}
		network = "tcp6"
	default:
		// 未知 IP 族时让系统自行选择双栈或其他可用网络。
		nodeIPs = []net.IP{net.IPv4zero}
		network = "tcp"
	}

	// 配置了 NodePort 地址范围时只监听匹配的节点 IP，减少暴露面。
	if !nodePortAddresses.MatchAll() {
		ips, err := nodePortAddresses.GetNodeIPs(proxyutil.RealNetwork{})
		if err == nil {
			nodeIPs = ips
		} else {
			klog.ErrorS(err, "Failed to get node ip address matching node port addresses, health check port will listen to all node addresses", "nodePortAddresses", nodePortAddresses)
		}
	}

	return &server{
		nodeName:      nodeName,
		recorder:      recorder,
		listener:      listener,
		httpFactory:   factory,
		healthzServer: healthzServer,
		services:      map[types.NamespacedName]*hcInstance{},
		nodeIPs:       nodeIPs,
		network:       network,
	}
}

// NewServiceHealthServer 使用标准网络实现创建服务健康检查服务器管理器。
func NewServiceHealthServer(nodeName string, recorder events.EventRecorder, nodePortAddresses *proxyutil.NodePortAddresses, healthzServer proxyHealthChecker, serviceFamily v1.IPFamily) ServiceHealthServer {
	return newServiceHealthServer(nodeName, recorder, stdNetListener{}, stdHTTPServerFactory{}, nodePortAddresses, healthzServer, serviceFamily)
}

// server 是 ServiceHealthServer 的默认实现，管理每个 Service 对应的健康检查监听器。
type server struct {
	nodeName string
	// 健康检查端口所监听的节点地址。
	nodeIPs     []net.IP
	recorder    events.EventRecorder // can be nil
	listener    listener
	httpFactory httpServerFactory

	healthzServer proxyHealthChecker

	lock     sync.RWMutex
	services map[types.NamespacedName]*hcInstance

	// 健康检查监听套接字使用的网络类型（tcp/tcp4/tcp6）。
	network string
}

// SyncServices 调整监听集合：关闭端口变化或已删除的 Service，为新增的 Service 开启监听。
func (hcs *server) SyncServices(newServices map[types.NamespacedName]uint16) error {
	hcs.lock.Lock()
	defer hcs.lock.Unlock()

	// 先清理不再需要的监听器，避免同一端口被重复占用。
	for nsn, svc := range hcs.services {
		if port, found := newServices[nsn]; !found || port != svc.port {
			klog.V(2).InfoS("Closing healthcheck", "service", nsn, "port", svc.port)

			// 关闭错误由 closeAll() 统一记录，这里无需重复处理。
			_ = svc.closeAll()

			delete(hcs.services, nsn)

		}
	}

	// 再为尚未监听的 Service 启动健康检查。
	for nsn, port := range newServices {
		if hcs.services[nsn] != nil {
			klog.V(3).InfoS("Existing healthcheck", "service", nsn, "port", port)
			continue
		}

		klog.V(2).InfoS("Opening healthcheck", "service", nsn, "port", port)

		svc := &hcInstance{nsn: nsn, port: port}
		err := svc.listenAndServeAll(hcs)

		if err != nil {
			// 单个 Service 监听失败不应阻断其他 Service 的同步，记录事件后继续。
			msg := fmt.Sprintf("node %s failed to start healthcheck %q on port %d: %v", hcs.nodeName, nsn.String(), port, err)

			if hcs.recorder != nil {
				hcs.recorder.Eventf(
					&v1.ObjectReference{
						APIVersion: "v1",
						Kind:       "Service",
						Namespace:  nsn.Namespace,
						Name:       nsn.Name,
					}, nil, api.EventTypeWarning, "FailedToStartServiceHealthcheck", "Listen", msg)
			}
			klog.ErrorS(err, "Failed to start healthcheck", "node", hcs.nodeName, "service", nsn, "port", port)
			continue
		}
		hcs.services[nsn] = svc
	}
	return nil
}

// hcInstance 保存单个 Service 的健康检查监听器及其当前本地端点数。
type hcInstance struct {
	nsn  types.NamespacedName
	port uint16

	httpServers []httpServer

	endpoints int // 该 Service 在本节点上的端点数量
}

// listenAndServeAll 在给定的每个节点地址上开启健康检查端口并启动 HTTP 服务。
func (hcI *hcInstance) listenAndServeAll(hcs *server) error {
	var err error
	var listener net.Listener

	hcI.httpServers = make([]httpServer, 0, len(hcs.nodeIPs))

	// 每个节点地址独立监听；任一地址失败都要回滚已打开的连接，便于重试。
	for _, ip := range hcs.nodeIPs {
		addr := net.JoinHostPort(ip.String(), fmt.Sprint(hcI.port))
		// 创建绑定到该 Service 处理逻辑的 HTTP 服务器。
		httpSrv := hcs.httpFactory.New(hcHandler{name: hcI.nsn, hcs: hcs})
		// 在指定地址端口上开始监听。
		listener, err = hcs.listener.Listen(context.TODO(), hcs.network, addr)
		if err != nil {
			// 失败时关闭此前已打开的所有监听，避免端口被占用阻碍后续重试。
			_ = hcI.closeAll()
			return err
		}

		// 在独立协程中提供服务，Serve 会阻塞直到服务器被关闭。
		go func(hcI *hcInstance, listener net.Listener, httpSrv httpServer) {
			// 正常关闭时 Serve 返回 ErrServerClosed，不应视为错误。
			klog.V(3).InfoS("Starting goroutine for healthcheck", "service", hcI.nsn, "address", listener.Addr())
			if err := httpSrv.Serve(listener); err != nil && err != http.ErrServerClosed {
				klog.ErrorS(err, "Healthcheck closed", "service", hcI.nsn)
				return
			}
			klog.V(3).InfoS("Healthcheck closed", "service", hcI.nsn, "address", listener.Addr())
		}(hcI, listener, httpSrv)

		hcI.httpServers = append(hcI.httpServers, httpSrv)
	}

	return nil
}

// closeAll 关闭该 Service 的所有健康检查 HTTP 服务器，聚合关闭过程中出现的错误。
func (hcI *hcInstance) closeAll() error {
	errors := []error{}
	for _, server := range hcI.httpServers {
		if err := server.Close(); err != nil {
			klog.ErrorS(err, "Error closing server for health check service", "service", hcI.nsn)
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return utilerrors.NewAggregate(errors)
	}

	return nil
}

// hcHandler 是单个 Service 健康检查端口的 HTTP 请求处理器。
type hcHandler struct {
	name types.NamespacedName
	hcs  *server
}

// 编译期断言 hcHandler 满足 http.Handler 接口。
var _ http.Handler = hcHandler{}

// ServeHTTP 返回当前 Service 的本地端点数与代理健康状态：
// 两者都正常时返回 200，否则返回 503，让负载均衡器避开本节点。
func (h hcHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	// 使用读锁读取端点数；服务可能已被 SyncServices 删除。
	h.hcs.lock.RLock()
	svc, ok := h.hcs.services[h.name]
	if !ok || svc == nil {
		h.hcs.lock.RUnlock()
		klog.ErrorS(nil, "Received request for closed healthcheck", "service", h.name)
		return
	}
	count := svc.endpoints
	h.hcs.lock.RUnlock()
	// 代理自身不健康时，即使有本地端点也应返回不可用。
	kubeProxyHealthy := h.hcs.healthzServer.Health().Healthy

	resp.Header().Set("Content-Type", "application/json")
	resp.Header().Set("X-Content-Type-Options", "nosniff")
	resp.Header().Set("X-Load-Balancing-Endpoint-Weight", strconv.Itoa(count))

	// 端点权重通过响应头暴露给负载均衡器，供其按端点数分配流量。
	if count != 0 && kubeProxyHealthy {
		resp.WriteHeader(http.StatusOK)
	} else {
		resp.WriteHeader(http.StatusServiceUnavailable)
	}
	fmt.Fprint(resp, strings.Trim(dedent.Dedent(fmt.Sprintf(`
		{
			"service": {
				"namespace": %q,
				"name": %q
			},
			"localEndpoints": %d,
			"serviceProxyHealthy": %v
		}
		`, h.name.Namespace, h.name.Name, count, kubeProxyHealthy)), "\n"))
}

// SyncEndpoints 更新各 Service 的本地端点数；未出现在新映射中的 Service 端点数归零。
func (hcs *server) SyncEndpoints(newEndpoints map[types.NamespacedName]int) error {
	hcs.lock.Lock()
	defer hcs.lock.Unlock()

	for nsn, count := range newEndpoints {
		// 健康检查端口尚未开启的 Service 无需记录端点数。
		if hcs.services[nsn] == nil {
			continue
		}
		klog.V(3).InfoS("Reporting endpoints for healthcheck", "endpointCount", count, "service", nsn)
		hcs.services[nsn].endpoints = count
	}
	// 新映射中缺失的 Service 说明其端点已全部消失，需归零以触发 503。
	for nsn, hci := range hcs.services {
		if _, found := newEndpoints[nsn]; !found {
			hci.endpoints = 0
		}
	}
	return nil
}

// FakeServiceHealthServer 是供测试使用的 ServiceHealthServer 空实现。
type FakeServiceHealthServer struct{}

// NewFakeServiceHealthServer 创建一个新的伪服务健康检查服务器管理器。
func NewFakeServiceHealthServer() ServiceHealthServer {
	return FakeServiceHealthServer{}
}

// SyncServices 实现 ServiceHealthServer；伪实现不做任何事。
func (fake FakeServiceHealthServer) SyncServices(_ map[types.NamespacedName]uint16) error {
	return nil
}

// SyncEndpoints 实现 ServiceHealthServer；伪实现不做任何事。
func (fake FakeServiceHealthServer) SyncEndpoints(_ map[types.NamespacedName]int) error {
	return nil
}
