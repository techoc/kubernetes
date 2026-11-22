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
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/component-base/metrics/testutil"
	"sigs.k8s.io/knftables"
)

// TestNFTCounterMetricCollector 验证 nftables 计数器指标按协议和 IP 族导出正确的包数。
func TestNFTCounterMetricCollector(t *testing.T) {
	udpCounter := LocalhostNodePortRejectedCounterName(v1.ProtocolUDP)
	sctpCounter := LocalhostNodePortRejectedCounterName(v1.ProtocolSCTP)

	fake := knftables.NewFake(knftables.IPv4Family, "kube-proxy")
	tx := fake.NewTransaction()
	// fake nftables 必须先在事务中创建表和计数器，收集器才有可读取的对象。
	tx.Add(&knftables.Table{})
	tx.Add(&knftables.Counter{Name: udpCounter})
	tx.Add(&knftables.Counter{Name: sctpCounter})
	if err := fake.Run(context.Background(), tx); err != nil {
		t.Fatalf("failed to set up fake nftables: %v", err)
	}
	// 写入非零包数，确保校验的是收集器读到的真实值而非零值。
	fake.Table.Counters[udpCounter].Packets = new(uint64(7))
	fake.Table.Counters[sctpCounter].Packets = new(uint64(3))

	// 收集器依赖协议到计数器名的映射来决定读取哪些计数器。
	c := &nftCounterMetricCollector{
		clients: map[v1.IPFamily]knftables.Interface{v1.IPv4Protocol: fake},
		counters: map[string]string{
			udpCounter:  string(v1.ProtocolUDP),
			sctpCounter: string(v1.ProtocolSCTP),
		},
		description: localhostNodePortRejectedPacketsDescription,
	}

	const metricName = "kubeproxy_nftables_localhost_nodeport_rejected_packets_total"
	want := `
		# HELP kubeproxy_nftables_localhost_nodeport_rejected_packets_total [ALPHA] Number of packets rejected on localhost NodePorts. UDP and SCTP are always rejected; TCP is rejected when the localhost NodePort proxy is not running
		# TYPE kubeproxy_nftables_localhost_nodeport_rejected_packets_total counter
		kubeproxy_nftables_localhost_nodeport_rejected_packets_total{ip_family="IPv4",protocol="SCTP"} 3
		kubeproxy_nftables_localhost_nodeport_rejected_packets_total{ip_family="IPv4",protocol="UDP"} 7
		`
	if err := testutil.CustomCollectAndCompare(c, strings.NewReader(want), metricName); err != nil {
		t.Error(err)
	}
}
