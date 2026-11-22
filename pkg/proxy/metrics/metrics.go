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

// Package metrics 定义 kube-proxy 的 Prometheus 指标及注册逻辑。
// 除各后端共用的同步延迟和变更计数外，还按 proxy 模式（iptables、ipvs、nftables、winkernel）
// 注册特定于数据面的指标。
package metrics

import (
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/kubernetes/pkg/features"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
)

// kubeProxySubsystem 是所有 kube-proxy 指标共享的子系统前缀。
const kubeProxySubsystem = "kubeproxy"

var (
	// SyncProxyRulesLatency 记录 kube-proxy 一轮代理规则同步的耗时。
	// 对 iptables 代理而言，完整同步和部分同步都计入其中。
	SyncProxyRulesLatency = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "sync_proxy_rules_duration_seconds",
			Help:           "SyncProxyRules latency in seconds",
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 15),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"ip_family"},
	)

	// SyncFullProxyRulesLatency 记录一轮完整规则同步的耗时。
	SyncFullProxyRulesLatency = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "sync_full_proxy_rules_duration_seconds",
			Help:           "SyncProxyRules latency in seconds for full resyncs",
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 15),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"ip_family"},
	)

	// SyncPartialProxyRulesLatency 记录一轮部分规则同步的耗时。
	SyncPartialProxyRulesLatency = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "sync_partial_proxy_rules_duration_seconds",
			Help:           "SyncProxyRules latency in seconds for partial resyncs",
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 15),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"ip_family"},
	)

	// SyncProxyRulesLastTimestamp 记录代理规则最后一次成功同步的时间戳。
	SyncProxyRulesLastTimestamp = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "sync_proxy_rules_last_timestamp_seconds",
			Help:           "The last time proxy rules were successfully synced",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"ip_family"},
	)

	// NetworkProgrammingLatency 定义为编排网络所花费的时间：从 service 或 pod 发生变化，
	// 到该变更传播出去、相应的 kube-proxy 规则完成同步为止。
	// 对参与本次规则同步的每个 endpoints 对象分别导出。
	// See https://github.com/kubernetes/community/blob/master/sig-scalability/slos/network_programming_latency.md
	// 注意该指标部分依赖 master 上 endpoints controller 导出的时间；
	// 若节点与 master 之间存在时钟漂移，测量结果可能不准确。
	NetworkProgrammingLatency = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem: kubeProxySubsystem,
			Name:      "network_programming_duration_seconds",
			Help:      "In Cluster Network Programming Latency in seconds",
			Buckets: metrics.MergeBuckets(
				metrics.LinearBuckets(0.25, 0.25, 2), // 0.25 秒、0.50 秒
				metrics.LinearBuckets(1, 1, 59),      // 1 秒、2 秒、3 秒、……59 秒
				metrics.LinearBuckets(60, 5, 12),     // 60 秒、65 秒、70 秒、……115 秒
				metrics.LinearBuckets(120, 30, 7),    // 2 分钟、2.5 分钟、3 分钟、……5 分钟
			),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"ip_family"},
	)

	// EndpointChangesPending 记录尚未同步到代理的 endpoint 变更数量。
	EndpointChangesPending = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "sync_proxy_rules_endpoint_changes_pending",
			Help:           "Pending proxy rules Endpoint changes",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// EndpointChangesTotal 记录代理观察到的 endpoint 变更总数。
	EndpointChangesTotal = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "sync_proxy_rules_endpoint_changes_total",
			Help:           "Cumulative proxy rules Endpoint changes",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// ServiceChangesPending 记录尚未同步到代理的 Service 变更数量。
	ServiceChangesPending = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "sync_proxy_rules_service_changes_pending",
			Help:           "Pending proxy rules Service changes",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// ServiceChangesTotal 记录代理观察到的 Service 变更总数。
	ServiceChangesTotal = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "sync_proxy_rules_service_changes_total",
			Help:           "Cumulative proxy rules Service changes",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// iptablesCTStateInvalidDroppedPacketsDescription 描述被 iptables 丢弃的报文数指标，
	// 这些报文被 conntrack 标记为 INVALID。
	iptablesCTStateInvalidDroppedPacketsDescription = metrics.NewDesc(
		"kubeproxy_iptables_ct_state_invalid_dropped_packets_total",
		"packets dropped by iptables to work around conntrack problems",
		nil, nil, metrics.ALPHA, "")
	IPTablesCTStateInvalidDroppedNFAcctCounter = "ct_state_invalid_dropped_pkts"

	// IPTablesRestoreFailuresTotal 记录代理遇到的 iptables-restore 失败次数。
	IPTablesRestoreFailuresTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "sync_proxy_rules_iptables_restore_failures_total",
			Help:           "Cumulative proxy iptables restore failures",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"ip_family"},
	)

	// IPTablesPartialRestoreFailuresTotal 记录代理遇到的 iptables *部分* restore 失败次数
	// （这种失败会导致回退到完整 restore）。
	IPTablesPartialRestoreFailuresTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "sync_proxy_rules_iptables_partial_restore_failures_total",
			Help:           "Cumulative proxy iptables partial restore failures",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"ip_family"},
	)

	// IPTablesRulesTotal 记录 iptables 代理已安装的 iptables 规则总数。
	IPTablesRulesTotal = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "sync_proxy_rules_iptables_total",
			Help:           "Total number of iptables rules owned by kube-proxy",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"table", "ip_family"},
	)

	// IPTablesRulesLastSync 记录 iptables 代理在上一次同步中更新的 iptables 规则数量。
	IPTablesRulesLastSync = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "sync_proxy_rules_iptables_last",
			Help:           "Number of iptables rules written by kube-proxy in last sync",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"table", "ip_family"},
	)

	// NFTablesSyncFailuresTotal 记录代理遇到的 nftables 同步失败次数。
	NFTablesSyncFailuresTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "sync_proxy_rules_nftables_sync_failures_total",
			Help:           "Cumulative proxy nftables sync failures",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"ip_family"},
	)

	// NFTablesCleanupFailuresTotal 记录代理清理 nftables 过期链失败的次数。
	NFTablesCleanupFailuresTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "sync_proxy_rules_nftables_cleanup_failures_total",
			Help:           "Cumulative proxy nftables cleanup failures",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"ip_family"},
	)

	// ProxyHealthzTotal 记录每次 healthz 探针返回的 HTTP 状态码次数。
	ProxyHealthzTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "proxy_healthz_total",
			Help:           "Cumulative proxy healthz HTTP status",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"code"},
	)

	// ProxyLivezTotal 记录每次 livez 探针返回的 HTTP 状态码次数。
	ProxyLivezTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "proxy_livez_total",
			Help:           "Cumulative proxy livez HTTP status",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"code"},
	)

	// SyncProxyRulesLastQueuedTimestamp 记录最近一次请求同步代理规则的时间。
	// 如果它远大于 kubeproxy_sync_proxy_rules_last_timestamp_seconds，说明同步流程可能卡住。
	SyncProxyRulesLastQueuedTimestamp = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "sync_proxy_rules_last_queued_timestamp_seconds",
			Help:           "The last time a sync of proxy rules was queued",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"ip_family"},
	)

	// SyncProxyRulesNoLocalEndpointsTotal 记录没有可用 endpoint 的规则总数，
	// 通常是 internal 流量策略下没有可用的本地工作负载导致的。
	SyncProxyRulesNoLocalEndpointsTotal = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "sync_proxy_rules_no_local_endpoints_total",
			Help:           "Number of services with a Local traffic policy and no endpoints",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"traffic_policy", "ip_family"},
	)

	// WinKernelLBCreateFailure 记录代理遇到的 winkernel 负载均衡器创建失败次数。
	WinKernelLBCreateFailure = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "sync_proxy_rules_winkernel_lb_create_failures_total",
			Help:           "Cumulative proxy winkernel lb create failures",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"ip_family", "lb_type", "error"},
	)

	// WinKernelLBUpdateFailure 记录代理遇到的 winkernel 负载均衡器更新失败次数。
	WinKernelLBUpdateFailure = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "sync_proxy_rules_winkernel_lb_update_failures_total",
			Help:           "Cumulative proxy winkernel lb update failures",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"ip_family", "lb_type", "error"},
	)

	// WinKernelLBDeleteFailure 记录代理遇到的 winkernel 负载均衡器删除失败次数。
	WinKernelLBDeleteFailure = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "sync_proxy_rules_winkernel_lb_delete_failures_total",
			Help:           "Cumulative proxy winkernel lb delete failures",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"ip_family", "lb_type", "error"},
	)

	// localhostNodePortsAcceptedPacketsDescription 描述被 iptables 接受的、目标是回环接口 NodePort 的报文数指标。
	localhostNodePortsAcceptedPacketsDescription = metrics.NewDesc(
		"kubeproxy_iptables_localhost_nodeports_accepted_packets_total",
		"Number of packets accepted on nodeports of loopback interface",
		nil, nil, metrics.ALPHA, "")
	LocalhostNodePortAcceptedNFAcctCounter = "localhost_nps_accepted_pkts"

	// LocalhostNodePortListeners 记录 kube-proxy 当前绑定的活跃 localhost NodePort 用户态代理监听器数量。
	LocalhostNodePortListeners = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "nftables_localhost_nodeport_listeners",
			Help:           "Number of active localhost NodePort userspace proxy listeners.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"ip_family"},
	)

	// LocalhostNodePortListenerCreationFailuresTotal 记录 kube-proxy 绑定 localhost NodePort 用户态代理监听器失败的次数。
	LocalhostNodePortListenerCreationFailuresTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "nftables_localhost_nodeport_listener_creation_failures_total",
			Help:           "Number of localhost NodePort userspace proxy listener creation failures.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"ip_family"},
	)

	// localhostNodePortRejectedPacketsDescription 描述在 localhost NodePort 上被拒绝的报文计数器。
	localhostNodePortRejectedPacketsDescription = metrics.NewDesc(
		"kubeproxy_nftables_localhost_nodeport_rejected_packets_total",
		"Number of packets rejected on localhost NodePorts. UDP and SCTP are always rejected; TCP is rejected when the localhost NodePort proxy is not running",
		[]string{"ip_family", "protocol"}, nil, metrics.ALPHA, "")

	// ReconcileConntrackFlowsLatency 记录 kube-proxy 一轮 conntrack 流对账的耗时。
	ReconcileConntrackFlowsLatency = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "conntrack_reconciler_sync_duration_seconds",
			Help:           "ReconcileConntrackFlowsLatency latency in seconds",
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 15),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"ip_family"},
	)

	// ReconcileConntrackFlowsDeletedEntriesTotal 记录 conntrack 对账器删除的表项数量。
	ReconcileConntrackFlowsDeletedEntriesTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      kubeProxySubsystem,
			Name:           "conntrack_reconciler_deleted_entries_total",
			Help:           "Cumulative conntrack flows deleted by conntrack reconciler",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"ip_family"},
	)
)

// LocalhostNodePortRejectedProtocols 是用户态代理无法服务的 NodePort 协议；
// 这些协议的 localhost 流量会被拒绝并计入指标。
var LocalhostNodePortRejectedProtocols = []v1.Protocol{v1.ProtocolUDP, v1.ProtocolSCTP}

// LocalhostNodePortRejectableProtocols 是 kube-proxy 可能拒绝并计数的全部 NodePort 协议。
// UDP / SCTP 总是被拒绝；localhost NodePort 代理未运行时 TCP 也会被拒绝。
var LocalhostNodePortRejectableProtocols = []v1.Protocol{v1.ProtocolTCP, v1.ProtocolUDP, v1.ProtocolSCTP}

// LocalhostNodePortRejectedCounterName 返回 nftables 命名计数器的名字；
// kube-proxy 表用它统计因协议不受支持而被拒绝的 localhost NodePort 报文。
func LocalhostNodePortRejectedCounterName(protocol v1.Protocol) string {
	return "localhost-nodeport-rejected-" + strings.ToLower(string(protocol))
}

var registerMetricsOnce sync.Once

// RegisterMetrics 注册 kube-proxy 指标。
// 通用指标只注册一次，后端专属指标则根据传入的 proxy 模式选择注册。
func RegisterMetrics(mode kubeproxyconfig.ProxyMode) {
	// 进程内只允许注册一次：legacyregistry 对重复注册同名指标会直接报错。
	registerMetricsOnce.Do(func() {
		// 所有后端共用的核心 kube-proxy 指标
		legacyregistry.MustRegister(SyncProxyRulesLatency)
		legacyregistry.MustRegister(SyncProxyRulesLastQueuedTimestamp)
		legacyregistry.MustRegister(SyncProxyRulesLastTimestamp)
		legacyregistry.MustRegister(EndpointChangesPending)
		legacyregistry.MustRegister(EndpointChangesTotal)
		legacyregistry.MustRegister(ServiceChangesPending)
		legacyregistry.MustRegister(ServiceChangesTotal)
		legacyregistry.MustRegister(ProxyHealthzTotal)
		legacyregistry.MustRegister(ProxyLivezTotal)

		// FIXME: winkernel 未实现这些指标
		legacyregistry.MustRegister(NetworkProgrammingLatency)
		legacyregistry.MustRegister(SyncProxyRulesNoLocalEndpointsTotal)

		switch mode {
		case kubeproxyconfig.ProxyModeIPTables:
			iptablesCTStateInvalidDroppedMetricCollector := newNFAcctMetricCollector(IPTablesCTStateInvalidDroppedNFAcctCounter, iptablesCTStateInvalidDroppedPacketsDescription)
			if iptablesCTStateInvalidDroppedMetricCollector != nil {
				legacyregistry.CustomMustRegister(iptablesCTStateInvalidDroppedMetricCollector)
			}
			localhostNodePortsAcceptedMetricsCollector := newNFAcctMetricCollector(LocalhostNodePortAcceptedNFAcctCounter, localhostNodePortsAcceptedPacketsDescription)
			if localhostNodePortsAcceptedMetricsCollector != nil {
				legacyregistry.CustomMustRegister(localhostNodePortsAcceptedMetricsCollector)
			}
			legacyregistry.MustRegister(SyncFullProxyRulesLatency)
			legacyregistry.MustRegister(SyncPartialProxyRulesLatency)
			legacyregistry.MustRegister(IPTablesRestoreFailuresTotal)
			legacyregistry.MustRegister(IPTablesPartialRestoreFailuresTotal)
			legacyregistry.MustRegister(IPTablesRulesTotal)
			legacyregistry.MustRegister(IPTablesRulesLastSync)
			legacyregistry.MustRegister(ReconcileConntrackFlowsLatency)
			legacyregistry.MustRegister(ReconcileConntrackFlowsDeletedEntriesTotal)

		case kubeproxyconfig.ProxyModeIPVS:
			legacyregistry.MustRegister(IPTablesRestoreFailuresTotal)
			legacyregistry.MustRegister(ReconcileConntrackFlowsLatency)
			legacyregistry.MustRegister(ReconcileConntrackFlowsDeletedEntriesTotal)

		case kubeproxyconfig.ProxyModeNFTables:
			legacyregistry.MustRegister(SyncFullProxyRulesLatency)
			legacyregistry.MustRegister(SyncPartialProxyRulesLatency)
			legacyregistry.MustRegister(NFTablesSyncFailuresTotal)
			legacyregistry.MustRegister(NFTablesCleanupFailuresTotal)
			if utilfeature.DefaultFeatureGate.Enabled(features.KubeProxyNFTablesLocalhostNodePorts) {
				legacyregistry.MustRegister(LocalhostNodePortListeners)
				legacyregistry.MustRegister(LocalhostNodePortListenerCreationFailuresTotal)
				rejectedCounters := make(map[string]string, len(LocalhostNodePortRejectableProtocols))
				for _, protocol := range LocalhostNodePortRejectableProtocols {
					rejectedCounters[LocalhostNodePortRejectedCounterName(protocol)] = string(protocol)
				}
				if c := newNFTablesCounterMetricCollector(rejectedCounters, localhostNodePortRejectedPacketsDescription); c != nil {
					legacyregistry.CustomMustRegister(c)
				}
			}
			legacyregistry.MustRegister(ReconcileConntrackFlowsLatency)
			legacyregistry.MustRegister(ReconcileConntrackFlowsDeletedEntriesTotal)

		case kubeproxyconfig.ProxyModeKernelspace:
			// Winkernel 的 Prometheus 指标。
			// WinKernelLBCreateFailure、WinKernelLBUpdateFailure 和 WinKernelLBDeleteFailure 的基数：
			// ip_family(2) x lb_type(5) x error(5) 最多 50；实际远低于此，
			// 因为单个 proxier 只使用一种 ip_family，且多数 error/lb_type 组合不会出现。
			legacyregistry.MustRegister(WinKernelLBCreateFailure)
			legacyregistry.MustRegister(WinKernelLBUpdateFailure)
			legacyregistry.MustRegister(WinKernelLBDeleteFailure)
		}
	})
}

// SinceInSeconds 返回从指定起始时刻到现在的秒数。
func SinceInSeconds(start time.Time) float64 {
	return time.Since(start).Seconds()
}
