/*
Copyright 2015 The Kubernetes Authors.

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

package kubemark

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/events"
	proxyapp "k8s.io/kubernetes/cmd/kube-proxy/app"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
	"k8s.io/utils/ptr"
)

// HollowProxy 是 kubemark 中模拟 kube-proxy 的空心代理，
// 复用真实的 ProxyServer 控制循环但注入空操作的 Proxier，
// 只上报状态与事件，不配置任何实际的转发规则。
type HollowProxy struct {
	ProxyServer *proxyapp.ProxyServer
}

// FakeProxier 是 proxy.Proxy 的空实现：所有数据面操作都不执行，
// 仅用于让 ProxyServer 的信息同步循环在 hollow 节点上正常空转。
type FakeProxier struct{}

// Sync 是空操作；hollow 代理无需同步任何转发规则。
func (*FakeProxier) Sync() {}

// SyncLoop 永久阻塞，模拟真实 proxier 持续运行直到进程退出；
// 空的 select 会让 goroutine 一直挂起且不消耗 CPU。
func (*FakeProxier) SyncLoop() {
	select {}
}

// OnServiceAdd 是空操作：hollow 代理不维护 Service 转发规则。
func (*FakeProxier) OnServiceAdd(service *v1.Service) {}

// OnServiceUpdate 是空操作：Service 变更不会映射到任何数据面。
func (*FakeProxier) OnServiceUpdate(oldService, service *v1.Service) {}

// OnServiceDelete 是空操作：删除 Service 无需清理转发规则。
func (*FakeProxier) OnServiceDelete(service *v1.Service) {}

// OnServiceSynced 是空操作：无需处理 Service 全量同步完成事件。
func (*FakeProxier) OnServiceSynced() {}

// OnEndpointSliceAdd 是空操作：EndpointSlice 不参与 hollow 代理的数据面。
func (*FakeProxier) OnEndpointSliceAdd(slice *discoveryv1.EndpointSlice) {}

// OnEndpointSliceUpdate 是空操作：EndpointSlice 变更无需同步。
func (*FakeProxier) OnEndpointSliceUpdate(oldSlice, slice *discoveryv1.EndpointSlice) {}

// OnEndpointSliceDelete 是空操作：删除 EndpointSlice 无需清理。
func (*FakeProxier) OnEndpointSliceDelete(slice *discoveryv1.EndpointSlice) {}

// OnEndpointSlicesSynced 是空操作：无需处理 EndpointSlice 全量同步完成事件。
func (*FakeProxier) OnEndpointSlicesSynced() {}

// OnServiceCIDRsChanged 是空操作：Service CIDR 变化不影响 hollow 代理。
func (*FakeProxier) OnServiceCIDRsChanged(_ []string) {}

// OnTopologyChange 是空操作：拓扑信息不参与 hollow 代理的转发决策。
func (*FakeProxier) OnTopologyChange(_ map[string]string) {}

// NewHollowProxy 构造一个 hollow 代理：使用 fake 模式和空 Proxier，
// 其余字段沿用真实 ProxyServer 的配置，使其能正常连接 API Server 并执行信息同步循环。
// eventClient 当前未使用，保留在签名中以兼容调用方。
func NewHollowProxy(
	nodeName string,
	client clientset.Interface,
	eventClient v1core.EventsGetter,
	broadcaster events.EventBroadcaster,
	recorder events.EventRecorder,
) *HollowProxy {
	return &HollowProxy{
		ProxyServer: &proxyapp.ProxyServer{
			Config: &kubeproxyconfig.KubeProxyConfiguration{
				// fake 模式不会创建任何真实规则，正好匹配 FakeProxier。
				Mode:             kubeproxyconfig.ProxyMode("fake"),
				ConfigSyncPeriod: metav1.Duration{Duration: 30 * time.Second},
				Linux: kubeproxyconfig.KubeProxyLinuxConfiguration{
					// OOM 分数保持默认 0，避免 hollow 节点被宿主 OOM killer 优先杀掉。
					OOMScoreAdj: ptr.To[int32](0),
				},
			},

			Client:      client,
			Proxier:     &FakeProxier{},
			Broadcaster: broadcaster,
			Recorder:    recorder,
			NodeRef: &v1.ObjectReference{
				APIVersion: "v1",
				Kind:       "Node",
				Name:       nodeName,
				Namespace:  "",
			},
		},
	}
}

// Run 启动 hollow 代理的 ProxyServer；
// 用 context.TODO() 传递一个空上下文，proxy 本身不提供取消入口。
func (hp *HollowProxy) Run() error {

	if err := hp.ProxyServer.Run(context.TODO()); err != nil {
		// 包装原始错误以保留底层原因，便于排查 ProxyServer 启动失败的具体原因。
		return fmt.Errorf("Error while running proxy: %w", err)
	}
	return nil
}
