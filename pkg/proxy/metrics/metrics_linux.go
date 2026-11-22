//go:build linux

/*
Copyright The Kubernetes Authors.

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

package metrics

import (
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/component-base/metrics"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/proxy/util/nfacct"
	"sigs.k8s.io/knftables"
)

// 编译期断言：nfacctMetricCollector 必须实现 StableCollector，
// 保证它可以注册进组件指标注册表。
var _ metrics.StableCollector = &nfacctMetricCollector{}

// FIXME: The metrics code should not know details about how the iptables and nftables
// backends work. The logic of `nfacctMetricCollector` and `nftCounterMetricCollector`
// needs to move into their respective backends.
// newNFAcctMetricCollector 构造 nfacct 计数器采集器；客户端初始化失败时返回 nil，
// 调用方可据此跳过注册，而不是让整个 kube-proxy 启动失败。
func newNFAcctMetricCollector(counter string, description *metrics.Desc) *nfacctMetricCollector {
	// nfacct 依赖内核 netlink 能力，不可用时只记录日志并放弃采集。
	client, err := nfacct.New()
	if err != nil {
		klog.ErrorS(err, "failed to initialize nfacct client")
		return nil
	}
	// 记录客户端、计数器名称和描述符，供后续抓取时读取并生成指标。
	return &nfacctMetricCollector{
		client:      client,
		counter:     counter,
		description: description,
	}
}

// nfacctMetricCollector 通过 nfacct 从内核读取指定命名计数器，
// 并将其转换为指标描述符对应的固定计数器指标。
type nfacctMetricCollector struct {
	metrics.BaseStableCollector
	client      nfacct.Interface
	counter     string
	description *metrics.Desc
}

// DescribeWithStability 实现 metrics.StableCollector 接口，
// 将描述符交给注册表，让其知道本采集器会产出哪种指标。
func (n *nfacctMetricCollector) DescribeWithStability(ch chan<- *metrics.Desc) {
	ch <- n.description
}

// CollectWithStability 实现 metrics.StableCollector 接口，
// 从 nfacct 读取报文计数并生成 CounterValue 指标；读取或构造失败时只记录日志，
// 避免单次抓取失败影响其他指标的采集。
func (n *nfacctMetricCollector) CollectWithStability(ch chan<- metrics.Metric) {
	// 构造函数可能在初始化客户端时失败，nil 表示本采集器不可用。
	if n.client != nil {
		// 向内核查询命名计数器的当前值。
		counter, err := n.client.Get(n.counter)
		if err != nil {
			klog.ErrorS(err, "failed to collect nfacct counter", "counter", n.counter)
		} else {
			// 只上报报文数；构造固定指标时校验描述符与标签是否匹配。
			metric, err := metrics.NewConstMetric(n.description, metrics.CounterValue, float64(counter.Packets))
			if err != nil {
				klog.ErrorS(err, "failed to create constant metric")
			} else {
				ch <- metric
			}
		}
	}
}

// nftCounterListTimeout 限制单次指标抓取中列出 nftables 计数器的耗时，
// 防止 nft 命令阻塞拖慢整个抓取过程。
const nftCounterListTimeout = 2 * time.Second

// 编译期断言：nftCounterMetricCollector 必须实现 StableCollector。
var _ metrics.StableCollector = &nftCounterMetricCollector{}

// nftCounterMetricCollector reports named nftables counters from the kube-proxy
// table of each IP family as const counter metrics labeled by ip_family and
// protocol. counters maps each nftables counter name to its protocol label.
type nftCounterMetricCollector struct {
	metrics.BaseStableCollector
	clients     map[v1.IPFamily]knftables.Interface
	counters    map[string]string
	description *metrics.Desc
}

// newNFTablesCounterMetricCollector 为 IPv4/IPv6 两个地址族各建一个 kube-proxy 表的
// nftables 客户端，用于采集命名计数器；两个地址族都建不起来时返回 nil。
// counters 把 nftables 计数器名映射到指标的 protocol 标签值。
func newNFTablesCounterMetricCollector(counters map[string]string, description *metrics.Desc) *nftCounterMetricCollector {
	// IPv4 与 IPv6 各有独立的表，需要分别建立客户端。
	families := map[v1.IPFamily]knftables.Family{
		v1.IPv4Protocol: knftables.IPv4Family,
		v1.IPv6Protocol: knftables.IPv6Family,
	}
	clients := make(map[v1.IPFamily]knftables.Interface, len(families))
	// 逐个地址族初始化客户端；某个地址族不可用时只记日志并跳过，
	// 尽量让另一个地址族的指标仍然可用。
	for family, nftFamily := range families {
		nft, err := knftables.New(nftFamily, "kube-proxy")
		if err != nil {
			klog.ErrorS(err, "failed to initialize nftables client for metrics", "ipFamily", family)
			continue
		}
		clients[family] = nft
	}
	// 一个客户端都没有时返回 nil，调用方据此跳过注册。
	if len(clients) == 0 {
		return nil
	}
	// 保存可用客户端及计数器名到 protocol 标签的映射，供抓取时使用。
	return &nftCounterMetricCollector{
		clients:     clients,
		counters:    counters,
		description: description,
	}
}

// DescribeWithStability 实现 metrics.StableCollector 接口，
// 向注册表声明本采集器产出的指标描述符。
func (n *nftCounterMetricCollector) DescribeWithStability(ch chan<- *metrics.Desc) {
	ch <- n.description
}

// CollectWithStability 实现 metrics.StableCollector 接口，
// 遍历两个地址族的 nftables 计数器，为关注的名字生成带 ip_family 和 protocol
// 标签的 CounterValue 指标；表不存在或列取失败时跳过该地址族。
func (n *nftCounterMetricCollector) CollectWithStability(ch chan<- metrics.Metric) {
	for family, client := range n.clients {
		// 用超时上下文约束列取操作，cancel 必须调用以释放定时器资源。
		ctx, cancel := context.WithTimeout(context.Background(), nftCounterListTimeout)
		counters, err := client.ListCounters(ctx)
		cancel()
		if err != nil {
			// 表还不存在属于正常情况（规则尚未下发），无需记错误日志。
			// A not-found error means the table doesn't exist
			if !knftables.IsNotFound(err) {
				klog.ErrorS(err, "failed to list nftables counters", "ipFamily", family)
			}
			continue
		}
		for _, c := range counters {
			// 计数器为空或没有报文数时无法生成指标，直接跳过。
			if c == nil || c.Packets == nil {
				continue
			}
			// 只上报配置中关注的计数器，其余忽略。
			protocol, ok := n.counters[c.Name]
			if !ok {
				continue
			}
			// 用地址族和 protocol 标签构造固定指标，标签顺序须与描述符一致。
			metric, err := metrics.NewConstMetric(n.description, metrics.CounterValue, float64(*c.Packets), string(family), protocol)
			if err != nil {
				klog.ErrorS(err, "failed to create constant metric")
				continue
			}
			ch <- metric
		}
	}
}
