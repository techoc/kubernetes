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

package v1alpha1

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	kubeproxyconfigv1alpha1 "k8s.io/kube-proxy/config/v1alpha1"

	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/kubernetes/pkg/cluster/ports"
	"k8s.io/kubernetes/pkg/kubelet/qos"
	proxyutil "k8s.io/kubernetes/pkg/proxy/util"
	netutils "k8s.io/utils/net"
	"k8s.io/utils/ptr"
)

// addDefaultingFuncs 向 scheme 注册本包的默认值生成函数。
func addDefaultingFuncs(scheme *kruntime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_KubeProxyConfiguration 为 KubeProxyConfiguration 中未设置的字段填充默认值。
func SetDefaults_KubeProxyConfiguration(obj *kubeproxyconfigv1alpha1.KubeProxyConfiguration) {

	// 未指定绑定地址时监听所有 IPv4 地址。
	if len(obj.BindAddress) == 0 {
		obj.BindAddress = "0.0.0.0"
	}

	defaultHealthzAddress, defaultMetricsAddress := getDefaultAddresses(obj.BindAddress)

	// healthz/metrics 默认地址跟随绑定地址的 IP 族，但仅监听本地回环。
	if obj.HealthzBindAddress == "" {
		obj.HealthzBindAddress = fmt.Sprintf("%s:%v", defaultHealthzAddress, ports.ProxyHealthzPort)
	} else {
		obj.HealthzBindAddress = proxyutil.AppendPortIfNeeded(obj.HealthzBindAddress, ports.ProxyHealthzPort)
	}
	if obj.MetricsBindAddress == "" {
		obj.MetricsBindAddress = fmt.Sprintf("%s:%v", defaultMetricsAddress, ports.ProxyStatusPort)
	} else {
		obj.MetricsBindAddress = proxyutil.AppendPortIfNeeded(obj.MetricsBindAddress, ports.ProxyStatusPort)
	}

	// OOM 分值沿用 kubelet 的默认设置，保证关键组件优先级一致。
	if obj.OOMScoreAdj == nil {
		temp := int32(qos.KubeProxyOOMScoreAdj)
		obj.OOMScoreAdj = &temp
	}
	if obj.IPTables.SyncPeriod.Duration == 0 {
		obj.IPTables.SyncPeriod = metav1.Duration{Duration: 30 * time.Second}
	}
	if obj.IPTables.MinSyncPeriod.Duration == 0 {
		obj.IPTables.MinSyncPeriod = metav1.Duration{Duration: 1 * time.Second}
	}
	if obj.IPTables.LocalhostNodePorts == nil {
		obj.IPTables.LocalhostNodePorts = ptr.To(true)
	}
	if obj.IPVS.SyncPeriod.Duration == 0 {
		obj.IPVS.SyncPeriod = metav1.Duration{Duration: 30 * time.Second}
	}
	if obj.NFTables.SyncPeriod.Duration == 0 {
		obj.NFTables.SyncPeriod = metav1.Duration{Duration: 30 * time.Second}
	}
	if obj.NFTables.MinSyncPeriod.Duration == 0 {
		obj.NFTables.MinSyncPeriod = metav1.Duration{Duration: 1 * time.Second}
	}

	// Conntrack 容量按每核数设置，并设下限以覆盖小规模节点。
	if obj.Conntrack.MaxPerCore == nil {
		obj.Conntrack.MaxPerCore = ptr.To[int32](32 * 1024)
	}
	if obj.Conntrack.Min == nil {
		obj.Conntrack.Min = ptr.To[int32](128 * 1024)
	}

	if obj.IPTables.MasqueradeBit == nil {
		temp := int32(14)
		obj.IPTables.MasqueradeBit = &temp
	}
	if obj.NFTables.MasqueradeBit == nil {
		temp := int32(14)
		obj.NFTables.MasqueradeBit = &temp
	}
	if obj.Conntrack.TCPEstablishedTimeout == nil {
		obj.Conntrack.TCPEstablishedTimeout = &metav1.Duration{Duration: 24 * time.Hour} // 1 day (1/5 default)
	}
	if obj.Conntrack.TCPCloseWaitTimeout == nil {
		// 参见 https://github.com/kubernetes/kubernetes/issues/32551。
		// CLOSE_WAIT 出现在内核收到远端 FIN 且本地套未关闭时（半关闭状态），
		// 通常在本地套被延迟回收时才会发生。若该 conntrack 记录过期，本地发出的
		// FIN 将无法正确 SNAT 而到不了对端；若对端 FIN_WAIT* 的超时长于此值，
		// 双方连接状态会不一致，复用同一 SNAT（源地址、端口）的新连接可能被 RST，
		// 导致 connect(2) 返回 ECONNREFUSED。故默认设为 1 小时以匹配常见的服务端超时。
		obj.Conntrack.TCPCloseWaitTimeout = &metav1.Duration{Duration: 1 * time.Hour}
	}
	// 配置同步周期默认 15 分钟，兼顾配置更新及时性与 API 压力。
	if obj.ConfigSyncPeriod.Duration == 0 {
		obj.ConfigSyncPeriod.Duration = 15 * time.Minute
	}

	// 默认使用 protobuf 与 API Server 通信，减少序列化开销。
	if len(obj.ClientConnection.ContentType) == 0 {
		obj.ClientConnection.ContentType = "application/vnd.kubernetes.protobuf"
	}
	if obj.ClientConnection.QPS == 0.0 {
		obj.ClientConnection.QPS = 5.0
	}
	if obj.ClientConnection.Burst == 0 {
		obj.ClientConnection.Burst = 10
	}
	if obj.FeatureGates == nil {
		obj.FeatureGates = make(map[string]bool)
	}
	// 日志配置采用组件库推荐的默认项。
	logsapi.SetRecommendedLoggingConfiguration(&obj.Logging)
}

// getDefaultAddresses 根据绑定地址的 IP 族返回 healthz 和 metrics 的默认监听地址；
// IPv6 地址会用方括号包裹，以便后续追加端口。
func getDefaultAddresses(bindAddress string) (defaultHealthzAddress, defaultMetricsAddress string) {
	// IPv4 使用通配地址与回环地址；IPv6 则对应全零与回环地址。
	if netutils.ParseIPSloppy(bindAddress).To4() != nil {
		return "0.0.0.0", "127.0.0.1"
	}
	return "[::]", "[::1]"
}
