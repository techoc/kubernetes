//go:build linux

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

package iptables

import (
	"bytes"
	"fmt"
	"net"
	"reflect"
	"regexp"
	stdruntime "runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/lithammer/dedent"
	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/klog/v2"
	klogtesting "k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/proxy"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
	"k8s.io/kubernetes/pkg/proxy/conntrack"
	"k8s.io/kubernetes/pkg/proxy/healthcheck"
	"k8s.io/kubernetes/pkg/proxy/metrics"
	"k8s.io/kubernetes/pkg/proxy/runner"
	proxyutil "k8s.io/kubernetes/pkg/proxy/util"
	proxyutiltest "k8s.io/kubernetes/pkg/proxy/util/testing"
	utiliptables "k8s.io/kubernetes/pkg/util/iptables"
	iptablestest "k8s.io/kubernetes/pkg/util/iptables/testing"
	netutils "k8s.io/utils/net"
	"k8s.io/utils/ptr"
)

// 使用 NewFakeProxier 的测试约定：
//
// Pod IP：10.0.0.0/8
// Service ClusterIP：172.30.0.0/16
// Node IP：192.168.0.0/24
// 本机 Node IP：192.168.0.2
// Service ExternalIP：192.168.99.0/24
// LoadBalancer IP：1.2.3.4、5.6.7.8、9.10.11.12
// 集群外 IP：203.0.113.0/24
// LB 来源范围：203.0.113.0/25

const testNodeName = "test-node"
const testNodeIP = "192.168.0.2"
const testNodeIPAlt = "192.168.1.2"
const testExternalIP = "192.168.99.11"
const testNodeIPv6 = "2001:db8::1"
const testNodeIPv6Alt = "2001:db8:1::2"
const testExternalClient = "203.0.113.2"
const testExternalClientBlocked = "203.0.113.130"

var testNodeIPs = []string{testNodeIP, testNodeIPAlt, testExternalIP, testNodeIPv6, testNodeIPv6Alt}

// NewFakeProxier 构造测试用 Proxier，使用 FakeIPTables 与模拟网络接口，
// 并预置 IP 族、伪装标记、conntrack 和健康检查服务等最小可用状态。
// 由于依赖 goroutine 的初始化逻辑尚未拆分，这里手工组装而不调用 NewProxier。
func NewFakeProxier(ipt utiliptables.Interface) *Proxier {
	// TODO: 将 goroutine 启动逻辑拆到 Run() 方法后，改为调用 NewProxier。
	ipfamily := v1.IPv4Protocol
	podCIDR := "10.0.0.0/8"
	if ipt.IsIPv6() {
		ipfamily = v1.IPv6Protocol
		podCIDR = "fd00:10::/64"
	}
	// 用 Pod CIDR 判定流量来源是否本地，CIDR 随 IP 族而变化。
	detectLocal := proxyutil.NewDetectLocalByCIDR(podCIDR)

	// 伪造回环及节点网络接口，供 NodePort 地址过滤等逻辑查询。
	networkInterfacer := proxyutiltest.NewFakeNetwork()
	itf := net.Interface{Index: 0, MTU: 0, Name: "lo", HardwareAddr: nil, Flags: 0}
	addrs := []net.Addr{
		&net.IPNet{IP: netutils.ParseIPSloppy("127.0.0.1"), Mask: net.CIDRMask(8, 32)},
		&net.IPNet{IP: netutils.ParseIPSloppy("::1/128"), Mask: net.CIDRMask(128, 128)},
	}
	networkInterfacer.AddInterfaceAddr(&itf, addrs)
	itf1 := net.Interface{Index: 1, MTU: 0, Name: "eth0", HardwareAddr: nil, Flags: 0}
	addrs1 := []net.Addr{
		&net.IPNet{IP: netutils.ParseIPSloppy(testNodeIP), Mask: net.CIDRMask(24, 32)},
		&net.IPNet{IP: netutils.ParseIPSloppy(testNodeIPAlt), Mask: net.CIDRMask(24, 32)},
		&net.IPNet{IP: netutils.ParseIPSloppy(testExternalIP), Mask: net.CIDRMask(24, 32)},
		&net.IPNet{IP: netutils.ParseIPSloppy(testNodeIPv6), Mask: net.CIDRMask(64, 128)},
		&net.IPNet{IP: netutils.ParseIPSloppy(testNodeIPv6Alt), Mask: net.CIDRMask(64, 128)},
	}
	networkInterfacer.AddInterfaceAddr(&itf1, addrs1)

	// 手工装配 Proxier：缓冲区、变更跟踪器等都必须是测试可用的内存实现。
	p := &Proxier{
		ipFamily:                 ipfamily,
		svcPortMap:               make(proxy.ServicePortMap),
		serviceChanges:           proxy.NewServiceChangeTracker(ipfamily, newServiceInfo, nil),
		endpointsMap:             make(proxy.EndpointsMap),
		endpointsChanges:         proxy.NewEndpointsChangeTracker(ipfamily, testNodeName, newEndpointInfo, nil),
		needFullSync:             true,
		iptables:                 ipt,
		masqueradeMark:           "0x4000",
		conntrack:                conntrack.NewFake(),
		localDetector:            detectLocal,
		nodeName:                 testNodeName,
		serviceHealthServer:      healthcheck.NewFakeServiceHealthServer(),
		precomputedProbabilities: make([]string, 0, 1001),
		iptablesData:             bytes.NewBuffer(nil),
		existingFilterChainsData: bytes.NewBuffer(nil),
		filterChains:             proxyutil.NewLineBuffer(),
		filterRules:              proxyutil.NewLineBuffer(),
		natChains:                proxyutil.NewLineBuffer(),
		natRules:                 proxyutil.NewLineBuffer(),
		nodeIP:                   netutils.ParseIPSloppy(testNodeIP),
		localhostNodePorts:       true,
		nodePortAddresses:        proxyutil.NewNodePortAddresses(ipfamily, nil),
		networkInterfacer:        networkInterfacer,
		nfAcctCounters: map[string]bool{
			metrics.IPTablesCTStateInvalidDroppedNFAcctCounter: true,
			metrics.LocalhostNodePortAcceptedNFAcctCounter:     true,
		},
	}
	// 标记为已初始化，否则 syncProxyRules 会提前返回。
	p.setInitialized(true)
	// 使用零最小间隔的同步器，便于测试中立即触发同步。
	p.syncRunner = runner.NewBoundedFrequencyRunner("test-sync-runner", p.syncProxyRules, 0, 30*time.Second, time.Minute)
	return p
}

// parseIPTablesData 解析 iptables-save 的输出，返回表名到该表规则行数组的映射。
func parseIPTablesData(ruleData string) (map[string][]string, error) {
	// 按 "COMMIT" 行切分 ruleData：合法输入下每个表对应一个元素，
	// 由于输入以 "COMMIT" 行结尾，末尾还会多出一个空元素。
	// （因为输入以 "COMMIT" 行结尾）。
	rawTables := strings.Split(strings.TrimPrefix(ruleData, "\n"), "COMMIT\n")
	nTables := len(rawTables) - 1
	if nTables < 2 || rawTables[nTables] != "" {
		return nil, fmt.Errorf("bad ruleData (%d tables)\n%s", nTables, ruleData)
	}

	tables := make(map[string][]string, nTables)
	for i, table := range rawTables[:nTables] {
		lines := strings.Split(strings.Trim(table, "\n"), "\n")
		// 首行应为 "*nat" 或 "*filter" 这样的表头。
		if lines[0][0] != '*' {
			return nil, fmt.Errorf("bad ruleData (table %d starts with %q)", i+1, lines[0])
		}
		// 切分时丢掉了 "COMMIT" 行，这里补回以保持输出完整。
		lines = append(lines, "COMMIT")
		tables[lines[0][1:]] = lines
	}

	if tables["nat"] == nil {
		return nil, fmt.Errorf("bad ruleData (no %q table)", "nat")
	}
	if tables["filter"] == nil {
		return nil, fmt.Errorf("bad ruleData (no %q table)", "filter")
	}
	return tables, nil
}

// TestParseIPTablesData 验证 iptables-save 输出解析：覆盖正常解析，以及表数量不足、
// 尾部多余内容、表头格式错误、缺少 nat/filter 表等错误场景。
func TestParseIPTablesData(t *testing.T) {
	for _, tc := range []struct {
		name   string
		input  string
		output map[string][]string
		error  string
	}{
		{
			name: "basic test",
			input: dedent.Dedent(`
				*filter
				:KUBE-SERVICES - [0:0]
				:KUBE-EXTERNAL-SERVICES - [0:0]
				:KUBE-FORWARD - [0:0]
				:KUBE-NODEPORTS - [0:0]
				-A KUBE-NODEPORTS -m comment --comment "ns2/svc2:p80 health check node port" -m tcp -p tcp --dport 30000 -j ACCEPT
				-A KUBE-FORWARD -m conntrack --ctstate INVALID -j DROP
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
				COMMIT
				*nat
				:KUBE-SERVICES - [0:0]
				:KUBE-NODEPORTS - [0:0]
				:KUBE-POSTROUTING - [0:0]
				:KUBE-MARK-MASQ - [0:0]
				:KUBE-SVC-XPGD46QRK7WJZT7O - [0:0]
				:KUBE-SEP-SXIVWICOYRO3J4NJ - [0:0]
				-A KUBE-POSTROUTING -m mark ! --mark 0x4000/0x4000 -j RETURN
				-A KUBE-POSTROUTING -j MARK --xor-mark 0x4000
				-A KUBE-POSTROUTING -m comment --comment "kubernetes service traffic requiring SNAT" -j MASQUERADE --random-fully
				-A KUBE-MARK-MASQ -j MARK --or-mark 0x4000
				-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 10.20.30.41 --dport 80 -j KUBE-SVC-XPGD46QRK7WJZT7O
				-A KUBE-SVC-XPGD46QRK7WJZT7O -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 10.20.30.41 --dport 80 ! -s 10.0.0.0/24 -j KUBE-MARK-MASQ
				-A KUBE-SVC-XPGD46QRK7WJZT7O -m comment --comment ns1/svc1:p80 -j KUBE-SEP-SXIVWICOYRO3J4NJ
				-A KUBE-SEP-SXIVWICOYRO3J4NJ -m comment --comment ns1/svc1:p80 -s 10.180.0.1 -j KUBE-MARK-MASQ
				-A KUBE-SEP-SXIVWICOYRO3J4NJ -m comment --comment ns1/svc1:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.1:80
				-A KUBE-SERVICES -m comment --comment "kubernetes service nodeports; NOTE: this must be the last rule in this chain" -m addrtype --dst-type LOCAL -j KUBE-NODEPORTS
				COMMIT
				`),
			output: map[string][]string{
				"filter": {
					`*filter`,
					`:KUBE-SERVICES - [0:0]`,
					`:KUBE-EXTERNAL-SERVICES - [0:0]`,
					`:KUBE-FORWARD - [0:0]`,
					`:KUBE-NODEPORTS - [0:0]`,
					`-A KUBE-NODEPORTS -m comment --comment "ns2/svc2:p80 health check node port" -m tcp -p tcp --dport 30000 -j ACCEPT`,
					`-A KUBE-FORWARD -m conntrack --ctstate INVALID -j DROP`,
					`-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT`,
					`-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT`,
					`COMMIT`,
				},
				"nat": {
					`*nat`,
					`:KUBE-SERVICES - [0:0]`,
					`:KUBE-NODEPORTS - [0:0]`,
					`:KUBE-POSTROUTING - [0:0]`,
					`:KUBE-MARK-MASQ - [0:0]`,
					`:KUBE-SVC-XPGD46QRK7WJZT7O - [0:0]`,
					`:KUBE-SEP-SXIVWICOYRO3J4NJ - [0:0]`,
					`-A KUBE-POSTROUTING -m mark ! --mark 0x4000/0x4000 -j RETURN`,
					`-A KUBE-POSTROUTING -j MARK --xor-mark 0x4000`,
					`-A KUBE-POSTROUTING -m comment --comment "kubernetes service traffic requiring SNAT" -j MASQUERADE --random-fully`,
					`-A KUBE-MARK-MASQ -j MARK --or-mark 0x4000`,
					`-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 10.20.30.41 --dport 80 -j KUBE-SVC-XPGD46QRK7WJZT7O`,
					`-A KUBE-SVC-XPGD46QRK7WJZT7O -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 10.20.30.41 --dport 80 ! -s 10.0.0.0/24 -j KUBE-MARK-MASQ`,
					`-A KUBE-SVC-XPGD46QRK7WJZT7O -m comment --comment ns1/svc1:p80 -j KUBE-SEP-SXIVWICOYRO3J4NJ`,
					`-A KUBE-SEP-SXIVWICOYRO3J4NJ -m comment --comment ns1/svc1:p80 -s 10.180.0.1 -j KUBE-MARK-MASQ`,
					`-A KUBE-SEP-SXIVWICOYRO3J4NJ -m comment --comment ns1/svc1:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.1:80`,
					`-A KUBE-SERVICES -m comment --comment "kubernetes service nodeports; NOTE: this must be the last rule in this chain" -m addrtype --dst-type LOCAL -j KUBE-NODEPORTS`,
					`COMMIT`,
				},
			},
		},
		{
			name: "not enough tables",
			input: dedent.Dedent(`
				*filter
				:KUBE-SERVICES - [0:0]
				:KUBE-EXTERNAL-SERVICES - [0:0]
				:KUBE-FORWARD - [0:0]
				:KUBE-NODEPORTS - [0:0]
				-A KUBE-NODEPORTS -m comment --comment "ns2/svc2:p80 health check node port" -m tcp -p tcp --dport 30000 -j ACCEPT
				-A KUBE-FORWARD -m conntrack --ctstate INVALID -j DROP
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
				COMMIT
				`),
			error: "bad ruleData (1 tables)",
		},
		{
			name: "trailing junk",
			input: dedent.Dedent(`
				*filter
				:KUBE-SERVICES - [0:0]
				:KUBE-EXTERNAL-SERVICES - [0:0]
				:KUBE-FORWARD - [0:0]
				:KUBE-NODEPORTS - [0:0]
				-A KUBE-NODEPORTS -m comment --comment "ns2/svc2:p80 health check node port" -m tcp -p tcp --dport 30000 -j ACCEPT
				-A KUBE-FORWARD -m conntrack --ctstate INVALID -j DROP
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
				COMMIT
				*nat
				:KUBE-SERVICES - [0:0]
				:KUBE-EXTERNAL-SERVICES - [0:0]
				:KUBE-FORWARD - [0:0]
				:KUBE-NODEPORTS - [0:0]
				-A KUBE-NODEPORTS -m comment --comment "ns2/svc2:p80 health check node port" -m tcp -p tcp --dport 30000 -j ACCEPT
				-A KUBE-FORWARD -m conntrack --ctstate INVALID -j DROP
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
				COMMIT
				junk
				`),
			error: "bad ruleData (2 tables)",
		},
		{
			name: "bad start line",
			input: dedent.Dedent(`
				*filter
				:KUBE-SERVICES - [0:0]
				:KUBE-EXTERNAL-SERVICES - [0:0]
				:KUBE-FORWARD - [0:0]
				:KUBE-NODEPORTS - [0:0]
				-A KUBE-NODEPORTS -m comment --comment "ns2/svc2:p80 health check node port" -m tcp -p tcp --dport 30000 -j ACCEPT
				-A KUBE-FORWARD -m conntrack --ctstate INVALID -j DROP
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
				COMMIT
				:KUBE-SERVICES - [0:0]
				:KUBE-EXTERNAL-SERVICES - [0:0]
				:KUBE-FORWARD - [0:0]
				:KUBE-NODEPORTS - [0:0]
				-A KUBE-NODEPORTS -m comment --comment "ns2/svc2:p80 health check node port" -m tcp -p tcp --dport 30000 -j ACCEPT
				-A KUBE-FORWARD -m conntrack --ctstate INVALID -j DROP
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
				COMMIT
				`),
			error: `bad ruleData (table 2 starts with ":KUBE-SERVICES - [0:0]")`,
		},
		{
			name: "no nat",
			input: dedent.Dedent(`
				*filter
				:KUBE-SERVICES - [0:0]
				:KUBE-EXTERNAL-SERVICES - [0:0]
				:KUBE-FORWARD - [0:0]
				:KUBE-NODEPORTS - [0:0]
				-A KUBE-NODEPORTS -m comment --comment "ns2/svc2:p80 health check node port" -m tcp -p tcp --dport 30000 -j ACCEPT
				-A KUBE-FORWARD -m conntrack --ctstate INVALID -j DROP
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
				COMMIT
				*mangle
				:KUBE-SERVICES - [0:0]
				:KUBE-EXTERNAL-SERVICES - [0:0]
				:KUBE-FORWARD - [0:0]
				:KUBE-NODEPORTS - [0:0]
				-A KUBE-NODEPORTS -m comment --comment "ns2/svc2:p80 health check node port" -m tcp -p tcp --dport 30000 -j ACCEPT
				-A KUBE-FORWARD -m conntrack --ctstate INVALID -j DROP
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
				COMMIT
				`),
			error: `bad ruleData (no "nat" table)`,
		},
		{
			name: "no filter",
			input: dedent.Dedent(`
				*mangle
				:KUBE-SERVICES - [0:0]
				:KUBE-EXTERNAL-SERVICES - [0:0]
				:KUBE-FORWARD - [0:0]
				:KUBE-NODEPORTS - [0:0]
				-A KUBE-NODEPORTS -m comment --comment "ns2/svc2:p80 health check node port" -m tcp -p tcp --dport 30000 -j ACCEPT
				-A KUBE-FORWARD -m conntrack --ctstate INVALID -j DROP
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
				COMMIT
				*nat
				:KUBE-SERVICES - [0:0]
				:KUBE-EXTERNAL-SERVICES - [0:0]
				:KUBE-FORWARD - [0:0]
				:KUBE-NODEPORTS - [0:0]
				-A KUBE-NODEPORTS -m comment --comment "ns2/svc2:p80 health check node port" -m tcp -p tcp --dport 30000 -j ACCEPT
				-A KUBE-FORWARD -m conntrack --ctstate INVALID -j DROP
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
				COMMIT
				`),
			error: `bad ruleData (no "filter" table)`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 逐用例解析并比对结果：期望错误只校验前缀，避免依赖完整错误文本。
			out, err := parseIPTablesData(tc.input)
			if err == nil {
				if tc.error != "" {
					t.Errorf("unexpectedly did not get error")
				} else {
					assert.Equal(t, tc.output, out)
				}
			} else {
				if tc.error == "" {
					t.Errorf("got unexpected error: %v", err)
				} else if !strings.HasPrefix(err.Error(), tc.error) {
					t.Errorf("got wrong error: %v (expected %q)", err, tc.error)
				}
			}
		})
	}
}

// countRules 统计指定表中所有链的规则总数，解析或查表失败时返回 -1 以便测试区分。
func countRules(logger klog.Logger, tableName utiliptables.Table, ruleData string) int {
	dump, err := iptablestest.ParseIPTablesDump(ruleData)
	if err != nil {
		logger.Error(err, "error parsing iptables rules")
		return -1
	}

	rules := 0
	table, err := dump.GetTable(tableName)
	if err != nil {
		logger.Error(err, "can't find table", "table", tableName)
		return -1
	}

	for _, c := range table.Chains {
		rules += len(c.Rules)
	}
	return rules
}

// countRulesFromMetric 从 IPTablesRulesTotal 指标读取指定表的规则数；指标未注册时返回 -1。
func countRulesFromMetric(logger klog.Logger, tableName utiliptables.Table, ipFamily string) int {
	numRulesFloat, err := testutil.GetGaugeMetricValue(metrics.IPTablesRulesTotal.WithLabelValues(string(tableName), ipFamily))
	if err != nil {
		logger.Error(err, "metrics are not registered?")
		return -1
	}
	return int(numRulesFloat)
}

// countRulesFromLastSyncMetric 从 IPTablesRulesLastSync 指标读取上次同步的规则数，未注册返回 -1。
func countRulesFromLastSyncMetric(logger klog.Logger, tableName utiliptables.Table, ipFamily string) int {
	numRulesFloat, err := testutil.GetGaugeMetricValue(metrics.IPTablesRulesLastSync.WithLabelValues(string(tableName), ipFamily))
	if err != nil {
		logger.Error(err, "metrics are not registered?")
		return -1
	}
	return int(numRulesFloat)
}

// findAllMatches 提取所有匹配项：pattern 必须恰好包含一个捕获组，
// 返回该捕获组的全部唯一匹配，并排序以保证结果稳定。
func findAllMatches(lines []string, pattern string) []string {
	regex := regexp.MustCompile(pattern)
	allMatches := sets.New[string]()
	for _, line := range lines {
		match := regex.FindStringSubmatch(line)
		if len(match) == 2 {
			allMatches.Insert(match[1])
		}
	}
	return sets.List(allMatches)
}

// checkIPTablesRuleJumps 检查规则中的所有 -j 跳转目标都是已创建且写入过规则的链，
// 否则 iptables-restore 加载时会因跳转目标不存在而失败。
func checkIPTablesRuleJumps(ruleData string) error {
	tables, err := parseIPTablesData(ruleData)
	if err != nil {
		return err
	}

	for tableName, lines := range tables {
		// 形如 ":KUBE-SERVICES" 的行表示 iptables-restore 加载时会创建的链。
		// 这些链定义供 iptables-restore 加载时创建。
		createdChains := sets.New[string](findAllMatches(lines, `^:([^ ]*)`)...)
		// 形如 "-X KUBE-SERVICES ..." 的行表示待删除的废弃链，
		// 这些链不应算作已创建。
		// 需要从已创建集合中将它们剔除。
		createdChains = createdChains.Delete(findAllMatches(lines, `-X ([^ ]*)`)...)

		// 形如 "-A KUBE-SERVICES ..." 的行表示该链至少写入了一条规则。
		// 表示该链至少写入了一条规则。
		filledChains := sets.New[string](findAllMatches(lines, `-A ([^ ]*)`)...)

		// 收集所有规则跳转的目标链，
		// 以便与已创建和已填充的链做校验。
		jumpedChains := sets.New[string](findAllMatches(lines, `-j ([^ ]*)`)...)
		// ACCEPT/DROP 等是内核内置目标，无需 kube-proxy 创建，
		// 因此不参与自定义链的一致性检查。
		jumpedChains.Delete("ACCEPT", "REJECT", "DROP", "MARK", "RETURN", "DNAT", "SNAT", "MASQUERADE")

		// 存在 "-A FOO ... -j BAR" 但没有 ":BAR"，
		// 说明跳转到了未创建的链，这会导致 restore 失败。
		missingChains := jumpedChains.Difference(createdChains)
		missingChains = missingChains.Union(filledChains.Difference(createdChains))
		if len(missingChains) > 0 {
			return fmt.Errorf("some chains in %s are used but were not created: %v", tableName, missingChains.UnsortedList())
		}

		// 有跳转但没有对应规则通常意味着漏写规则；
		// 例外是 KUBE-SERVICES 总会跳转到 KUBE-NODEPORTS，
		// 即使没有任何 NodePort 规则也是如此。
		// KUBE-NODEPORTS 例外：即使没有 NodePort 规则也会被跳转。
		emptyChains := jumpedChains.Difference(filledChains)
		emptyChains.Delete(string(kubeNodePortsChain))
		if len(emptyChains) > 0 {
			return fmt.Errorf("some chains in %s are jumped to but have no rules: %v", tableName, emptyChains.UnsortedList())
		}

		// 创建了链却无人跳转，说明是无用链，同样属于异常；
		// 下面排除 kube-proxy 预置的、允许为空的链。
		extraChains := createdChains.Difference(jumpedChains)
		extraChains.Delete(string(kubeServicesChain), string(kubeExternalServicesChain), string(kubeNodePortsChain), string(kubePostroutingChain), string(kubeForwardChain), string(kubeMarkMasqChain), string(kubeProxyFirewallChain), string(kubeletFirewallChain))
		if len(extraChains) > 0 {
			return fmt.Errorf("some chains in %s are created but not used: %v", tableName, extraChains.UnsortedList())
		}
	}

	return nil
}

// TestCheckIPTablesRuleJumps 测试跳转一致性检查：既覆盖合法规则集，
// 也覆盖跳转到未创建链、跳转到空链以及创建无用链等异常。
func TestCheckIPTablesRuleJumps(t *testing.T) {
	// 表格覆盖四种一致性问题：正常规则、跳转到未创建/空链、创建未使用的链。
	for _, tc := range []struct {
		name  string
		input string
		error string
	}{
		{
			name: "valid",
			input: dedent.Dedent(`
				*filter
				COMMIT
				*nat
				:KUBE-MARK-MASQ - [0:0]
				:KUBE-SERVICES - [0:0]
				:KUBE-SVC-XPGD46QRK7WJZT7O - [0:0]
				-A KUBE-MARK-MASQ -j MARK --or-mark 0x4000
				-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 10.20.30.41 --dport 80 -j KUBE-SVC-XPGD46QRK7WJZT7O
				-A KUBE-SVC-XPGD46QRK7WJZT7O -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 10.20.30.41 --dport 80 ! -s 10.0.0.0/24 -j KUBE-MARK-MASQ
				COMMIT
				`),
			error: "",
		},
		{
			name: "can't jump to chain that wasn't created",
			input: dedent.Dedent(`
				*filter
				COMMIT
				*nat
				:KUBE-SERVICES - [0:0]
				-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 -j KUBE-SVC-XPGD46QRK7WJZT7O
				COMMIT
				`),
			error: "some chains in nat are used but were not created: [KUBE-SVC-XPGD46QRK7WJZT7O]",
		},
		{
			name: "can't jump to chain that has no rules",
			input: dedent.Dedent(`
				*filter
				COMMIT
				*nat
				:KUBE-SERVICES - [0:0]
				:KUBE-SVC-XPGD46QRK7WJZT7O - [0:0]
				-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 -j KUBE-SVC-XPGD46QRK7WJZT7O
				COMMIT
				`),
			error: "some chains in nat are jumped to but have no rules: [KUBE-SVC-XPGD46QRK7WJZT7O]",
		},
		{
			name: "can't add rules to a chain that wasn't created",
			input: dedent.Dedent(`
				*filter
				COMMIT
				*nat
				:KUBE-MARK-MASQ - [0:0]
				:KUBE-SERVICES - [0:0]
				-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" ...
				-A KUBE-SVC-XPGD46QRK7WJZT7O -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
				COMMIT
				`),
			error: "some chains in nat are used but were not created: [KUBE-SVC-XPGD46QRK7WJZT7O]",
		},
		{
			name: "can't jump to chain that wasn't created",
			input: dedent.Dedent(`
				*filter
				COMMIT
				*nat
				:KUBE-SERVICES - [0:0]
				-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 -j KUBE-SVC-XPGD46QRK7WJZT7O
				COMMIT
				`),
			error: "some chains in nat are used but were not created: [KUBE-SVC-XPGD46QRK7WJZT7O]",
		},
		{
			name: "can't jump to chain that has no rules",
			input: dedent.Dedent(`
				*filter
				COMMIT
				*nat
				:KUBE-SERVICES - [0:0]
				:KUBE-SVC-XPGD46QRK7WJZT7O - [0:0]
				-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 -j KUBE-SVC-XPGD46QRK7WJZT7O
				COMMIT
				`),
			error: "some chains in nat are jumped to but have no rules: [KUBE-SVC-XPGD46QRK7WJZT7O]",
		},
		{
			name: "can't add rules to a chain that wasn't created",
			input: dedent.Dedent(`
				*filter
				COMMIT
				*nat
				:KUBE-MARK-MASQ - [0:0]
				:KUBE-SERVICES - [0:0]
				-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" ...
				-A KUBE-SVC-XPGD46QRK7WJZT7O -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
				COMMIT
				`),
			error: "some chains in nat are used but were not created: [KUBE-SVC-XPGD46QRK7WJZT7O]",
		},
		{
			name: "can't create chain and then not use it",
			input: dedent.Dedent(`
				*filter
				COMMIT
				*nat
				:KUBE-MARK-MASQ - [0:0]
				:KUBE-SERVICES - [0:0]
				:KUBE-SVC-XPGD46QRK7WJZT7O - [0:0]
				-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" ...
				COMMIT
				`),
			error: "some chains in nat are created but not used: [KUBE-SVC-XPGD46QRK7WJZT7O]",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 仅校验错误前缀，使断言不依赖完整错误文本。
			err := checkIPTablesRuleJumps(tc.input)
			if err == nil {
				if tc.error != "" {
					t.Errorf("unexpectedly did not get error")
				}
			} else {
				if tc.error == "" {
					t.Errorf("got unexpected error: %v", err)
				} else if !strings.HasPrefix(err.Error(), tc.error) {
					t.Errorf("got wrong error: %v (expected %q)", err, tc.error)
				}
			}
		})
	}
}

// orderByCommentServiceName 按规则注释中的服务名对两条 iptables 规则排序。
// 若任一规则没有注释，返回值无意义。
// orderByCommentServiceName 按规则注释中的服务名对两条 iptables 规则排序，
// 使期望规则的比较不受服务处理顺序影响。
func orderByCommentServiceName(rule1, rule2 *iptablestest.Rule) bool {
	if rule1.Comment == nil || rule2.Comment == nil {
		return false
	}
	name1, name2 := rule1.Comment.Value, rule2.Comment.Value

	// 服务名是注释中第一个空格或冒号之前的部分。
	i := strings.IndexAny(name1, " :")
	if i != -1 {
		name1 = name1[:i]
	}
	i = strings.IndexAny(name2, " :")
	if i != -1 {
		name2 = name2[:i]
	}

	return name1 < name2
}

// sortIPTablesRules 对 iptables-restore 的输出排序，消除服务处理顺序带来的差异，
// 同时保留相关规则之间的相对次序，便于测试进行稳定的字符串比较。
// sortIPTablesRules 对 iptables-restore 的输出进行规范化排序：
// 按表、链和服务名排序，但保留同一服务内规则的相对次序。
func sortIPTablesRules(ruleData string) (string, error) {
	dump, err := iptablestest.ParseIPTablesDump(ruleData)
	if err != nil {
		return "", err
	}

	// 先按表、再按链分组排序。
	sort.Slice(dump.Tables, func(i, j int) bool {
		return dump.Tables[i].Name < dump.Tables[j].Name
	})

	// KUBE-NODEPORTS 排在最前。
	for t := range dump.Tables {
		table := &dump.Tables[t]
		sort.Slice(table.Chains, func(i, j int) bool {
			switch {
			case table.Chains[i].Name == kubeNodePortsChain:
				// KUBE-NODEPORTS 排在其他链之前。
				return true
			case table.Chains[j].Name == kubeNodePortsChain:
				// 其他链排在 KUBE-NODEPORTS 之后。
				return false
			case table.Chains[i].Name == kubeServicesChain:
				// KUBE-SERVICES 排在除 KUBE-NODEPORTS 之外的链之前。
				return true
			case table.Chains[j].Name == kubeServicesChain:
				// 其他链排在 KUBE-SERVICES 之后。
				return false
			case strings.HasPrefix(string(table.Chains[i].Name), "KUBE-") && !strings.HasPrefix(string(table.Chains[j].Name), "KUBE-"):
				// KUBE-* 链排在非 KUBE-* 链之前。
				return true
			case !strings.HasPrefix(string(table.Chains[i].Name), "KUBE-") && strings.HasPrefix(string(table.Chains[j].Name), "KUBE-"):
				// 非 KUBE-* 链排在 KUBE-* 链之后。
				return false
			default:
				// 两条链同为 KUBE-* 或同为非 KUBE-* 时，
				// 按名称字典序排列，保证顺序确定。
				return table.Chains[i].Name < table.Chains[j].Name
			}
		})
	}

	// 按服务名对 KUBE-NODEPORTS 链中的规则排序。
	chain, _ := dump.GetChain(utiliptables.TableFilter, kubeNodePortsChain)
	if chain != nil {
		sort.SliceStable(chain.Rules, func(i, j int) bool {
			return orderByCommentServiceName(chain.Rules[i], chain.Rules[j])
		})
	}
	chain, _ = dump.GetChain(utiliptables.TableNAT, kubeNodePortsChain)
	if chain != nil {
		sort.SliceStable(chain.Rules, func(i, j int) bool {
			return orderByCommentServiceName(chain.Rules[i], chain.Rules[j])
		})
	}

	// 按服务名对 KUBE-SERVICES 链中的规则排序，但 "nat" 表中的
	// "must be the last rule" 规则必须保持在最后。
	chain, _ = dump.GetChain(utiliptables.TableFilter, kubeServicesChain)
	if chain != nil {
		sort.SliceStable(chain.Rules, func(i, j int) bool {
			return orderByCommentServiceName(chain.Rules[i], chain.Rules[j])
		})
	}
	chain, _ = dump.GetChain(utiliptables.TableNAT, kubeServicesChain)
	if chain != nil {
		sort.SliceStable(chain.Rules, func(i, j int) bool {
			if chain.Rules[i].Comment != nil && strings.Contains(chain.Rules[i].Comment.Value, "must be the last rule") {
				return false
			} else if chain.Rules[j].Comment != nil && strings.Contains(chain.Rules[j].Comment.Value, "must be the last rule") {
				return true
			}
			return orderByCommentServiceName(chain.Rules[i], chain.Rules[j])
		})
	}

	return dump.String(), nil
}

// TestSortIPTablesRules 验证排序函数能消除服务处理顺序差异，
// 同时维持 KUBE-NODEPORTS 在前、"must be last" 规则在后的语义约束。
func TestSortIPTablesRules(t *testing.T) {
	// 表格中的输入刻意打乱规则顺序，期望输出为规范化后的稳定顺序。
	for _, tc := range []struct {
		name   string
		input  string
		output string
		error  string
	}{
		{
			name: "basic test using each match type",
			input: dedent.Dedent(`
				*filter
				:KUBE-SERVICES - [0:0]
				:KUBE-EXTERNAL-SERVICES - [0:0]
				:KUBE-FIREWALL - [0:0]
				:KUBE-FORWARD - [0:0]
				:KUBE-NODEPORTS - [0:0]
				:KUBE-PROXY-FIREWALL - [0:0]
				-A KUBE-NODEPORTS -m comment --comment "ns2/svc2:p80 health check node port" -m tcp -p tcp --dport 30000 -j ACCEPT
				-A KUBE-EXTERNAL-SERVICES -m comment --comment "ns2/svc2:p80 has no local endpoints" -m tcp -p tcp -d 192.168.99.22 --dport 80 -j DROP
				-A KUBE-EXTERNAL-SERVICES -m comment --comment "ns2/svc2:p80 has no local endpoints" -m tcp -p tcp -d 1.2.3.4 --dport 80 -j DROP
				-A KUBE-EXTERNAL-SERVICES -m comment --comment "ns2/svc2:p80 has no local endpoints" -m addrtype --dst-type LOCAL -m tcp -p tcp --dport 3001 -j DROP
				-A KUBE-FIREWALL -m comment --comment "block incoming localnet connections" -d 127.0.0.0/8 ! -s 127.0.0.0/8 -m conntrack ! --ctstate RELATED,ESTABLISHED,DNAT -j DROP
				-A KUBE-FORWARD -m conntrack --ctstate INVALID -j DROP
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
				-A KUBE-PROXY-FIREWALL -m comment --comment "ns5/svc5:p80 traffic not accepted by KUBE-FW-NUKIZ6OKUXPJNT4C" -m tcp -p tcp -d 5.6.7.8 --dport 80 -j DROP
				COMMIT
				*nat
				:KUBE-SERVICES - [0:0]
				:KUBE-NODEPORTS - [0:0]
				:KUBE-POSTROUTING - [0:0]
				:KUBE-MARK-MASQ - [0:0]
				:KUBE-SVC-XPGD46QRK7WJZT7O - [0:0]
				:KUBE-SEP-SXIVWICOYRO3J4NJ - [0:0]
				:KUBE-SVC-GNZBNJ2PO5MGZ6GT - [0:0]
				:KUBE-EXT-GNZBNJ2PO5MGZ6GT - [0:0]
				:KUBE-SVL-GNZBNJ2PO5MGZ6GT - [0:0]
				:KUBE-FW-GNZBNJ2PO5MGZ6GT - [0:0]
				:KUBE-SEP-RS4RBKLTHTF2IUXJ - [0:0]
				:KUBE-SVC-X27LE4BHSL4DOUIK - [0:0]
				:KUBE-SEP-OYPFS5VJICHGATKP - [0:0]
				:KUBE-SVC-4SW47YFZTEDKD3PK - [0:0]
				:KUBE-SEP-UKSFD7AGPMPPLUHC - [0:0]
				:KUBE-SEP-C6EBXVWJJZMIWKLZ - [0:0]
				-A KUBE-POSTROUTING -m mark ! --mark 0x4000/0x4000 -j RETURN
				-A KUBE-POSTROUTING -j MARK --xor-mark 0x4000
				-A KUBE-POSTROUTING -m comment --comment "kubernetes service traffic requiring SNAT" -j MASQUERADE --random-fully
				-A KUBE-MARK-MASQ -j MARK --or-mark 0x4000
				-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 -j KUBE-SVC-XPGD46QRK7WJZT7O
				-A KUBE-SVC-XPGD46QRK7WJZT7O -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
				-A KUBE-SVC-XPGD46QRK7WJZT7O -m comment --comment ns1/svc1:p80 -j KUBE-SEP-SXIVWICOYRO3J4NJ
				-A KUBE-SEP-SXIVWICOYRO3J4NJ -m comment --comment ns1/svc1:p80 -s 10.180.0.1 -j KUBE-MARK-MASQ
				-A KUBE-SEP-SXIVWICOYRO3J4NJ -m comment --comment ns1/svc1:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.1:80
				-A KUBE-SERVICES -m comment --comment "ns2/svc2:p80 cluster IP" -m tcp -p tcp -d 172.30.0.42 --dport 80 -j KUBE-SVC-GNZBNJ2PO5MGZ6GT
				-A KUBE-SERVICES -m comment --comment "ns2/svc2:p80 external IP" -m tcp -p tcp -d 192.168.99.11 --dport 80 -j KUBE-EXT-GNZBNJ2PO5MGZ6GT
				-A KUBE-SERVICES -m comment --comment "ns2/svc2:p80 loadbalancer IP" -m tcp -p tcp -d 1.2.3.4 --dport 80 -j KUBE-FW-GNZBNJ2PO5MGZ6GT
				-A KUBE-SVC-GNZBNJ2PO5MGZ6GT -m comment --comment "ns2/svc2:p80 cluster IP" -m tcp -p tcp -d 172.30.0.42 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
				-A KUBE-SVC-GNZBNJ2PO5MGZ6GT -m comment --comment ns2/svc2:p80 -j KUBE-SEP-RS4RBKLTHTF2IUXJ
				-A KUBE-SEP-RS4RBKLTHTF2IUXJ -m comment --comment ns2/svc2:p80 -s 10.180.0.2 -j KUBE-MARK-MASQ
				-A KUBE-SEP-RS4RBKLTHTF2IUXJ -m comment --comment ns2/svc2:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.2:80
				-A KUBE-FW-GNZBNJ2PO5MGZ6GT -m comment --comment "ns2/svc2:p80 loadbalancer IP" -s 203.0.113.0/25 -j KUBE-EXT-GNZBNJ2PO5MGZ6GT
				-A KUBE-FW-GNZBNJ2PO5MGZ6GT -m comment --comment "other traffic to s2/svc2:p80 will be dropped by KUBE-PROXY-FIREWALL"
				-A KUBE-NODEPORTS -m comment --comment ns2/svc2:p80 -m tcp -p tcp --dport 3001 -j KUBE-EXT-GNZBNJ2PO5MGZ6GT
				-A KUBE-EXT-GNZBNJ2PO5MGZ6GT -m comment --comment "Redirect pods trying to reach external loadbalancer VIP to clusterIP" -s 10.0.0.0/8 -j KUBE-SVC-GNZBNJ2PO5MGZ6GT
				-A KUBE-EXT-GNZBNJ2PO5MGZ6GT -m comment --comment "masquerade LOCAL traffic for ns2/svc2:p80 LB IP" -m addrtype --src-type LOCAL -j KUBE-MARK-MASQ
				-A KUBE-EXT-GNZBNJ2PO5MGZ6GT -m comment --comment "route LOCAL traffic for ns2/svc2:p80 LB IP to service chain" -m addrtype --src-type LOCAL -j KUBE-SVC-GNZBNJ2PO5MGZ6GT
				-A KUBE-EXT-GNZBNJ2PO5MGZ6GT -j KUBE-SVL-GNZBNJ2PO5MGZ6GT
				-A KUBE-SVL-GNZBNJ2PO5MGZ6GT -m comment --comment "ns2/svc2:p80 has no local endpoints" -j KUBE-MARK-DROP
				-A KUBE-SERVICES -m comment --comment "ns3/svc3:p80 cluster IP" -m tcp -p tcp -d 172.30.0.43 --dport 80 -j KUBE-SVC-X27LE4BHSL4DOUIK
				-A KUBE-SVC-X27LE4BHSL4DOUIK -m comment --comment "ns3/svc3:p80 cluster IP" -m tcp -p tcp -d 172.30.0.43 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
				-A KUBE-NODEPORTS -m comment --comment ns3/svc3:p80 -m tcp -p tcp --dport 3002 -j KUBE-SVC-X27LE4BHSL4DOUIK
				-A KUBE-SVC-X27LE4BHSL4DOUIK -m comment --comment ns3/svc3:p80 -m tcp -p tcp --dport 3002 -j KUBE-MARK-MASQ
				-A KUBE-SVC-X27LE4BHSL4DOUIK -m comment --comment ns3/svc3:p80 -j KUBE-SEP-OYPFS5VJICHGATKP
				-A KUBE-SEP-OYPFS5VJICHGATKP -m comment --comment ns3/svc3:p80 -s 10.180.0.3 -j KUBE-MARK-MASQ
				-A KUBE-SEP-OYPFS5VJICHGATKP -m comment --comment ns3/svc3:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.3:80
				-A KUBE-SERVICES -m comment --comment "ns4/svc4:p80 cluster IP" -m tcp -p tcp -d 172.30.0.44 --dport 80 -j KUBE-SVC-4SW47YFZTEDKD3PK
				-A KUBE-SERVICES -m comment --comment "ns4/svc4:p80 external IP" -m tcp -p tcp -d 192.168.99.22 --dport 80 -j KUBE-SVC-4SW47YFZTEDKD3PK
				-A KUBE-SVC-4SW47YFZTEDKD3PK -m comment --comment "ns4/svc4:p80 cluster IP" -m tcp -p tcp -d 172.30.0.44 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
				-A KUBE-SVC-4SW47YFZTEDKD3PK -m comment --comment "ns4/svc4:p80 external IP" -m tcp -p tcp -d 192.168.99.22 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
				-A KUBE-SVC-4SW47YFZTEDKD3PK -m comment --comment ns4/svc4:p80 -m statistic --mode random --probability 0.5000000000 -j KUBE-SEP-UKSFD7AGPMPPLUHC
				-A KUBE-SVC-4SW47YFZTEDKD3PK -m comment --comment ns4/svc4:p80 -j KUBE-SEP-C6EBXVWJJZMIWKLZ
				-A KUBE-SEP-UKSFD7AGPMPPLUHC -m comment --comment ns4/svc4:p80 -s 10.180.0.4 -j KUBE-MARK-MASQ
				-A KUBE-SEP-UKSFD7AGPMPPLUHC -m comment --comment ns4/svc4:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.4:80
				-A KUBE-SEP-C6EBXVWJJZMIWKLZ -m comment --comment ns4/svc4:p80 -s 10.180.0.5 -j KUBE-MARK-MASQ
				-A KUBE-SEP-C6EBXVWJJZMIWKLZ -m comment --comment ns4/svc4:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.5:80
				-A KUBE-SERVICES -m comment --comment "kubernetes service nodeports; NOTE: this must be the last rule in this chain" -m addrtype --dst-type LOCAL -j KUBE-NODEPORTS
				COMMIT
				`),
			output: dedent.Dedent(`
				*filter
				:KUBE-NODEPORTS - [0:0]
				:KUBE-SERVICES - [0:0]
				:KUBE-EXTERNAL-SERVICES - [0:0]
				:KUBE-FIREWALL - [0:0]
				:KUBE-FORWARD - [0:0]
				:KUBE-PROXY-FIREWALL - [0:0]
				-A KUBE-NODEPORTS -m comment --comment "ns2/svc2:p80 health check node port" -m tcp -p tcp --dport 30000 -j ACCEPT
				-A KUBE-EXTERNAL-SERVICES -m comment --comment "ns2/svc2:p80 has no local endpoints" -m tcp -p tcp -d 192.168.99.22 --dport 80 -j DROP
				-A KUBE-EXTERNAL-SERVICES -m comment --comment "ns2/svc2:p80 has no local endpoints" -m tcp -p tcp -d 1.2.3.4 --dport 80 -j DROP
				-A KUBE-EXTERNAL-SERVICES -m comment --comment "ns2/svc2:p80 has no local endpoints" -m addrtype --dst-type LOCAL -m tcp -p tcp --dport 3001 -j DROP
				-A KUBE-FIREWALL -m comment --comment "block incoming localnet connections" -d 127.0.0.0/8 ! -s 127.0.0.0/8 -m conntrack ! --ctstate RELATED,ESTABLISHED,DNAT -j DROP
				-A KUBE-FORWARD -m conntrack --ctstate INVALID -j DROP
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
				-A KUBE-PROXY-FIREWALL -m comment --comment "ns5/svc5:p80 traffic not accepted by KUBE-FW-NUKIZ6OKUXPJNT4C" -m tcp -p tcp -d 5.6.7.8 --dport 80 -j DROP
				COMMIT
				*nat
				:KUBE-NODEPORTS - [0:0]
				:KUBE-SERVICES - [0:0]
				:KUBE-EXT-GNZBNJ2PO5MGZ6GT - [0:0]
				:KUBE-FW-GNZBNJ2PO5MGZ6GT - [0:0]
				:KUBE-MARK-MASQ - [0:0]
				:KUBE-POSTROUTING - [0:0]
				:KUBE-SEP-C6EBXVWJJZMIWKLZ - [0:0]
				:KUBE-SEP-OYPFS5VJICHGATKP - [0:0]
				:KUBE-SEP-RS4RBKLTHTF2IUXJ - [0:0]
				:KUBE-SEP-SXIVWICOYRO3J4NJ - [0:0]
				:KUBE-SEP-UKSFD7AGPMPPLUHC - [0:0]
				:KUBE-SVC-4SW47YFZTEDKD3PK - [0:0]
				:KUBE-SVC-GNZBNJ2PO5MGZ6GT - [0:0]
				:KUBE-SVC-X27LE4BHSL4DOUIK - [0:0]
				:KUBE-SVC-XPGD46QRK7WJZT7O - [0:0]
				:KUBE-SVL-GNZBNJ2PO5MGZ6GT - [0:0]
				-A KUBE-NODEPORTS -m comment --comment ns2/svc2:p80 -m tcp -p tcp --dport 3001 -j KUBE-EXT-GNZBNJ2PO5MGZ6GT
				-A KUBE-NODEPORTS -m comment --comment ns3/svc3:p80 -m tcp -p tcp --dport 3002 -j KUBE-SVC-X27LE4BHSL4DOUIK
				-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 -j KUBE-SVC-XPGD46QRK7WJZT7O
				-A KUBE-SERVICES -m comment --comment "ns2/svc2:p80 cluster IP" -m tcp -p tcp -d 172.30.0.42 --dport 80 -j KUBE-SVC-GNZBNJ2PO5MGZ6GT
				-A KUBE-SERVICES -m comment --comment "ns2/svc2:p80 external IP" -m tcp -p tcp -d 192.168.99.11 --dport 80 -j KUBE-EXT-GNZBNJ2PO5MGZ6GT
				-A KUBE-SERVICES -m comment --comment "ns2/svc2:p80 loadbalancer IP" -m tcp -p tcp -d 1.2.3.4 --dport 80 -j KUBE-FW-GNZBNJ2PO5MGZ6GT
				-A KUBE-SERVICES -m comment --comment "ns3/svc3:p80 cluster IP" -m tcp -p tcp -d 172.30.0.43 --dport 80 -j KUBE-SVC-X27LE4BHSL4DOUIK
				-A KUBE-SERVICES -m comment --comment "ns4/svc4:p80 cluster IP" -m tcp -p tcp -d 172.30.0.44 --dport 80 -j KUBE-SVC-4SW47YFZTEDKD3PK
				-A KUBE-SERVICES -m comment --comment "ns4/svc4:p80 external IP" -m tcp -p tcp -d 192.168.99.22 --dport 80 -j KUBE-SVC-4SW47YFZTEDKD3PK
				-A KUBE-SERVICES -m comment --comment "kubernetes service nodeports; NOTE: this must be the last rule in this chain" -m addrtype --dst-type LOCAL -j KUBE-NODEPORTS
				-A KUBE-EXT-GNZBNJ2PO5MGZ6GT -m comment --comment "Redirect pods trying to reach external loadbalancer VIP to clusterIP" -s 10.0.0.0/8 -j KUBE-SVC-GNZBNJ2PO5MGZ6GT
				-A KUBE-EXT-GNZBNJ2PO5MGZ6GT -m comment --comment "masquerade LOCAL traffic for ns2/svc2:p80 LB IP" -m addrtype --src-type LOCAL -j KUBE-MARK-MASQ
				-A KUBE-EXT-GNZBNJ2PO5MGZ6GT -m comment --comment "route LOCAL traffic for ns2/svc2:p80 LB IP to service chain" -m addrtype --src-type LOCAL -j KUBE-SVC-GNZBNJ2PO5MGZ6GT
				-A KUBE-EXT-GNZBNJ2PO5MGZ6GT -j KUBE-SVL-GNZBNJ2PO5MGZ6GT
				-A KUBE-FW-GNZBNJ2PO5MGZ6GT -m comment --comment "ns2/svc2:p80 loadbalancer IP" -s 203.0.113.0/25 -j KUBE-EXT-GNZBNJ2PO5MGZ6GT
				-A KUBE-FW-GNZBNJ2PO5MGZ6GT -m comment --comment "other traffic to s2/svc2:p80 will be dropped by KUBE-PROXY-FIREWALL"
				-A KUBE-MARK-MASQ -j MARK --or-mark 0x4000
				-A KUBE-POSTROUTING -m mark ! --mark 0x4000/0x4000 -j RETURN
				-A KUBE-POSTROUTING -j MARK --xor-mark 0x4000
				-A KUBE-POSTROUTING -m comment --comment "kubernetes service traffic requiring SNAT" -j MASQUERADE --random-fully
				-A KUBE-SEP-C6EBXVWJJZMIWKLZ -m comment --comment ns4/svc4:p80 -s 10.180.0.5 -j KUBE-MARK-MASQ
				-A KUBE-SEP-C6EBXVWJJZMIWKLZ -m comment --comment ns4/svc4:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.5:80
				-A KUBE-SEP-OYPFS5VJICHGATKP -m comment --comment ns3/svc3:p80 -s 10.180.0.3 -j KUBE-MARK-MASQ
				-A KUBE-SEP-OYPFS5VJICHGATKP -m comment --comment ns3/svc3:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.3:80
				-A KUBE-SEP-RS4RBKLTHTF2IUXJ -m comment --comment ns2/svc2:p80 -s 10.180.0.2 -j KUBE-MARK-MASQ
				-A KUBE-SEP-RS4RBKLTHTF2IUXJ -m comment --comment ns2/svc2:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.2:80
				-A KUBE-SEP-SXIVWICOYRO3J4NJ -m comment --comment ns1/svc1:p80 -s 10.180.0.1 -j KUBE-MARK-MASQ
				-A KUBE-SEP-SXIVWICOYRO3J4NJ -m comment --comment ns1/svc1:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.1:80
				-A KUBE-SEP-UKSFD7AGPMPPLUHC -m comment --comment ns4/svc4:p80 -s 10.180.0.4 -j KUBE-MARK-MASQ
				-A KUBE-SEP-UKSFD7AGPMPPLUHC -m comment --comment ns4/svc4:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.4:80
				-A KUBE-SVC-4SW47YFZTEDKD3PK -m comment --comment "ns4/svc4:p80 cluster IP" -m tcp -p tcp -d 172.30.0.44 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
				-A KUBE-SVC-4SW47YFZTEDKD3PK -m comment --comment "ns4/svc4:p80 external IP" -m tcp -p tcp -d 192.168.99.22 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
				-A KUBE-SVC-4SW47YFZTEDKD3PK -m comment --comment ns4/svc4:p80 -m statistic --mode random --probability 0.5000000000 -j KUBE-SEP-UKSFD7AGPMPPLUHC
				-A KUBE-SVC-4SW47YFZTEDKD3PK -m comment --comment ns4/svc4:p80 -j KUBE-SEP-C6EBXVWJJZMIWKLZ
				-A KUBE-SVC-GNZBNJ2PO5MGZ6GT -m comment --comment "ns2/svc2:p80 cluster IP" -m tcp -p tcp -d 172.30.0.42 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
				-A KUBE-SVC-GNZBNJ2PO5MGZ6GT -m comment --comment ns2/svc2:p80 -j KUBE-SEP-RS4RBKLTHTF2IUXJ
				-A KUBE-SVC-X27LE4BHSL4DOUIK -m comment --comment "ns3/svc3:p80 cluster IP" -m tcp -p tcp -d 172.30.0.43 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
				-A KUBE-SVC-X27LE4BHSL4DOUIK -m comment --comment ns3/svc3:p80 -m tcp -p tcp --dport 3002 -j KUBE-MARK-MASQ
				-A KUBE-SVC-X27LE4BHSL4DOUIK -m comment --comment ns3/svc3:p80 -j KUBE-SEP-OYPFS5VJICHGATKP
				-A KUBE-SVC-XPGD46QRK7WJZT7O -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
				-A KUBE-SVC-XPGD46QRK7WJZT7O -m comment --comment ns1/svc1:p80 -j KUBE-SEP-SXIVWICOYRO3J4NJ
				-A KUBE-SVL-GNZBNJ2PO5MGZ6GT -m comment --comment "ns2/svc2:p80 has no local endpoints" -j KUBE-MARK-DROP
				COMMIT
				`),
		},
		{
			name: "extra tables",
			input: dedent.Dedent(`
				*filter
				:KUBE-SERVICES - [0:0]
				:KUBE-EXTERNAL-SERVICES - [0:0]
				:KUBE-FORWARD - [0:0]
				:KUBE-NODEPORTS - [0:0]
				-A KUBE-NODEPORTS -m comment --comment "ns2/svc2:p80 health check node port" -m tcp -p tcp --dport 30000 -j ACCEPT
				-A KUBE-FORWARD -m conntrack --ctstate INVALID -j DROP
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
				COMMIT
				*nat
				:KUBE-SERVICES - [0:0]
				:KUBE-EXTERNAL-SERVICES - [0:0]
				:KUBE-FORWARD - [0:0]
				:KUBE-NODEPORTS - [0:0]
				-A KUBE-NODEPORTS -m comment --comment "ns2/svc2:p80 health check node port" -m tcp -p tcp --dport 30000 -j ACCEPT
				-A KUBE-FORWARD -m conntrack --ctstate INVALID -j DROP
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
				COMMIT
				*mangle
				:KUBE-SERVICES - [0:0]
				:KUBE-EXTERNAL-SERVICES - [0:0]
				:KUBE-FORWARD - [0:0]
				:KUBE-NODEPORTS - [0:0]
				-A KUBE-NODEPORTS -m comment --comment "ns2/svc2:p80 health check node port" -m tcp -p tcp --dport 30000 -j ACCEPT
				-A KUBE-FORWARD -m conntrack --ctstate INVALID -j DROP
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
				COMMIT
				`),
			output: dedent.Dedent(`
				*filter
				:KUBE-NODEPORTS - [0:0]
				:KUBE-SERVICES - [0:0]
				:KUBE-EXTERNAL-SERVICES - [0:0]
				:KUBE-FORWARD - [0:0]
				-A KUBE-NODEPORTS -m comment --comment "ns2/svc2:p80 health check node port" -m tcp -p tcp --dport 30000 -j ACCEPT
				-A KUBE-FORWARD -m conntrack --ctstate INVALID -j DROP
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
				COMMIT
				*mangle
				:KUBE-NODEPORTS - [0:0]
				:KUBE-SERVICES - [0:0]
				:KUBE-EXTERNAL-SERVICES - [0:0]
				:KUBE-FORWARD - [0:0]
				-A KUBE-NODEPORTS -m comment --comment "ns2/svc2:p80 health check node port" -m tcp -p tcp --dport 30000 -j ACCEPT
				-A KUBE-FORWARD -m conntrack --ctstate INVALID -j DROP
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
				COMMIT
				*nat
				:KUBE-NODEPORTS - [0:0]
				:KUBE-SERVICES - [0:0]
				:KUBE-EXTERNAL-SERVICES - [0:0]
				:KUBE-FORWARD - [0:0]
				-A KUBE-NODEPORTS -m comment --comment "ns2/svc2:p80 health check node port" -m tcp -p tcp --dport 30000 -j ACCEPT
				-A KUBE-FORWARD -m conntrack --ctstate INVALID -j DROP
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
				COMMIT
				`),
		},
		{
			name: "correctly match same service name in different styles of comments",
			input: dedent.Dedent(`
				*filter
				COMMIT
				*nat
				:KUBE-SERVICES - [0:0]
				-A KUBE-SERVICES -m comment --comment "ns2/svc2:p80 cluster IP" svc2 line 1
				-A KUBE-SERVICES -m comment --comment ns2/svc2 svc2 line 2
				-A KUBE-SERVICES -m comment --comment "ns2/svc2 blah" svc2 line 3
				-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" svc1 line 1
				-A KUBE-SERVICES -m comment --comment ns1/svc1 svc1 line 2
				-A KUBE-SERVICES -m comment --comment "ns1/svc1 blah" svc1 line 3
				-A KUBE-SERVICES -m comment --comment ns4/svc4 svc4 line 1
				-A KUBE-SERVICES -m comment --comment "ns4/svc4:p80 cluster IP" svc4 line 2
				-A KUBE-SERVICES -m comment --comment "ns4/svc4 blah" svc4 line 3
				-A KUBE-SERVICES -m comment --comment "ns3/svc3:p80 cluster IP" svc3 line 1
				-A KUBE-SERVICES -m comment --comment "ns3/svc3 blah" svc3 line 2
				-A KUBE-SERVICES -m comment --comment ns3/svc3 svc3 line 3
				COMMIT
				`),
			output: dedent.Dedent(`
				*filter
				COMMIT
				*nat
				:KUBE-SERVICES - [0:0]
				-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" svc1 line 1
				-A KUBE-SERVICES -m comment --comment ns1/svc1 svc1 line 2
				-A KUBE-SERVICES -m comment --comment "ns1/svc1 blah" svc1 line 3
				-A KUBE-SERVICES -m comment --comment "ns2/svc2:p80 cluster IP" svc2 line 1
				-A KUBE-SERVICES -m comment --comment ns2/svc2 svc2 line 2
				-A KUBE-SERVICES -m comment --comment "ns2/svc2 blah" svc2 line 3
				-A KUBE-SERVICES -m comment --comment "ns3/svc3:p80 cluster IP" svc3 line 1
				-A KUBE-SERVICES -m comment --comment "ns3/svc3 blah" svc3 line 2
				-A KUBE-SERVICES -m comment --comment ns3/svc3 svc3 line 3
				-A KUBE-SERVICES -m comment --comment ns4/svc4 svc4 line 1
				-A KUBE-SERVICES -m comment --comment "ns4/svc4:p80 cluster IP" svc4 line 2
				-A KUBE-SERVICES -m comment --comment "ns4/svc4 blah" svc4 line 3
				COMMIT
				`),
		},
		{
			name: "unexpected junk lines are preserved",
			input: dedent.Dedent(`
				*filter
				COMMIT
				*nat
				:KUBE-SERVICES - [0:0]
				:KUBE-SEP-RS4RBKLTHTF2IUXJ - [0:0]
				:KUBE-AAAAA - [0:0]
				:KUBE-ZZZZZ - [0:0]
				:WHY-IS-THIS-CHAIN-HERE - [0:0]
				-A KUBE-SERVICES -m comment --comment "ns2/svc2:p80 cluster IP" svc2 line 1
				-A KUBE-SEP-RS4RBKLTHTF2IUXJ -m comment --comment ns2/svc2:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.2:80
				-A KUBE-ZZZZZ -m comment --comment "mystery chain number 1"
				-A KUBE-SERVICES -m comment --comment ns2/svc2 svc2 line 2
				-A WHY-IS-THIS-CHAIN-HERE -j ACCEPT
				-A KUBE-SERVICES -m comment --comment "ns2/svc2 blah" svc2 line 3
				-A KUBE-AAAAA -m comment --comment "mystery chain number 2"
				COMMIT
				`),
			output: dedent.Dedent(`
				*filter
				COMMIT
				*nat
				:KUBE-SERVICES - [0:0]
				:KUBE-AAAAA - [0:0]
				:KUBE-SEP-RS4RBKLTHTF2IUXJ - [0:0]
				:KUBE-ZZZZZ - [0:0]
				:WHY-IS-THIS-CHAIN-HERE - [0:0]
				-A KUBE-SERVICES -m comment --comment "ns2/svc2:p80 cluster IP" svc2 line 1
				-A KUBE-SERVICES -m comment --comment ns2/svc2 svc2 line 2
				-A KUBE-SERVICES -m comment --comment "ns2/svc2 blah" svc2 line 3
				-A KUBE-AAAAA -m comment --comment "mystery chain number 2"
				-A KUBE-SEP-RS4RBKLTHTF2IUXJ -m comment --comment ns2/svc2:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.2:80
				-A KUBE-ZZZZZ -m comment --comment "mystery chain number 1"
				-A WHY-IS-THIS-CHAIN-HERE -j ACCEPT
				COMMIT
				`),
		},
	} {
		// 逐用例排序并比较；期望输出去掉前导换行以匹配 Dedent 的结果。
		t.Run(tc.name, func(t *testing.T) {
			out, err := sortIPTablesRules(tc.input)
			if err == nil {
				if tc.error != "" {
					t.Errorf("unexpectedly did not get error")
				} else {
					assert.Equal(t, strings.TrimPrefix(tc.output, "\n"), out)
				}
			} else {
				if tc.error == "" {
					t.Errorf("got unexpected error: %v", err)
				} else if !strings.HasPrefix(err.Error(), tc.error) {
					t.Errorf("got wrong error: %v (expected %q)", err, tc.error)
				}
			}
		})
	}
}

// getLine 返回调用方的行号（如可取得），供断言辅助函数在失败信息中定位用例位置。
func getLine() int {
	_, _, line, ok := stdruntime.Caller(1)
	if ok {
		return line
	}
	return 0
}

// assertIPTablesRulesEqual 断言 result 中的规则与 expected 一致，忽略无关紧要的顺序差异。
// 默认还会检查规则的一致性（例如不存在指向未定义链的跳转）；
// 若只传入部分规则，可传入 false 关闭该检查。
// assertIPTablesRulesEqual 比较规则文本；失败时通过调用行号定位具体用例。
func assertIPTablesRulesEqual(t *testing.T, line int, checkConsistency bool, expected, result string) {
	expected = strings.TrimLeft(expected, " \t\n")

	result, err := sortIPTablesRules(strings.TrimLeft(result, " \t\n"))
	if err != nil {
		t.Fatalf("%s", err)
	}

	lineStr := ""
	if line != 0 {
		lineStr = fmt.Sprintf(" (from line %d)", line)
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("rules do not match%s:\ndiff:\n%s\nfull result:\n```\n%s```", lineStr, diff, result)
	}

	if checkConsistency {
		err = checkIPTablesRuleJumps(expected)
		if err != nil {
			t.Fatalf("%s%s", err, lineStr)
		}
	}
}

// assertIPTablesChainEqual 断言 result 中指定表的指定链恰好包含 expected 中的规则，
// 且顺序一致。
// assertIPTablesChainEqual 比较单条链的规则文本；失败时打印调用行号便于定位。
func assertIPTablesChainEqual(t *testing.T, line int, table utiliptables.Table, chain utiliptables.Chain, expected, result string) {
	expected = strings.TrimLeft(expected, " \t\n")

	dump, err := iptablestest.ParseIPTablesDump(strings.TrimLeft(result, " \t\n"))
	if err != nil {
		t.Fatalf("%s", err)
	}

	result = ""
	if ch, _ := dump.GetChain(table, chain); ch != nil {
		for _, rule := range ch.Rules {
			result += rule.Raw + "\n"
		}
	}

	lineStr := ""
	if line != 0 {
		lineStr = fmt.Sprintf(" (from line %d)", line)
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("rules do not match%s:\ndiff:\n%s\nfull result:\n```\n%s```", lineStr, diff, result)
	}
}

// addressMatches 判断诸如 "! -s 192.168.0.0/16" 的 iptables 规则是否匹配 ipStr。
// addressMatches 处理带 "!" 取反的地址匹配，以及 IP 与 CIDR 两种值形式。
func addressMatches(t *testing.T, address *iptablestest.IPTablesValue, ipStr string) bool {
	ip := netutils.ParseIPSloppy(ipStr)
	if ip == nil {
		t.Fatalf("Bad IP in test case: %s", ipStr)
	}

	var matches bool
	if strings.Contains(address.Value, "/") {
		_, cidr, err := netutils.ParseCIDRSloppy(address.Value)
		if err != nil {
			t.Errorf("Bad CIDR in kube-proxy output: %v", err)
		}
		matches = cidr.Contains(ip)
	} else {
		ip2 := netutils.ParseIPSloppy(address.Value)
		if ip2 == nil {
			t.Errorf("Bad IP/CIDR in kube-proxy output: %s", address.Value)
		}
		matches = ip.Equal(ip2)
	}
	return (!address.Negated && matches) || (address.Negated && !matches)
}

// iptablesTracer 保存虚拟数据包穿越 iptables 规则时所需的状态。
type iptablesTracer struct {
	ipt      *iptablestest.FakeIPTables
	localIPs sets.Set[string]
	t        *testing.T

	// matches 记录匹配到的规则列表，仅用于调试输出。
	matches []string

	// outputs 记录匹配到的终结目标（端点 IP:端口或 "REJECT" 等特殊目标），
	// 最终用于构造 tracePacket 的返回值。
	// 这些目标最终构成 tracePacket 的返回值。
	outputs []string

	// markMasq 记录数据包是否已被标记需要伪装（MASQUERADE）。
	markMasq bool
}

// newIPTablesTracer 创建 iptablesTracer。nodeIPs 用于模拟 --src-type/--dst-type LOCAL 的匹配。
func newIPTablesTracer(t *testing.T, ipt *iptablestest.FakeIPTables, nodeIPs []string) *iptablesTracer {
	localIPs := sets.New("127.0.0.1", "::1")
	localIPs.Insert(nodeIPs...)

	return &iptablesTracer{
		ipt:      ipt,
		localIPs: localIPs,
		t:        t,
	}
}

// ruleMatches 检查给定规则是否（至少在概率意义上）匹配具有指定源 IP、目的 IP、
// 目的端口的数据包。
// ruleMatches 逐个子条件检查规则是否匹配模拟数据包。
func (tracer *iptablesTracer) ruleMatches(rule *iptablestest.Rule, sourceIP, protocol, destIP, destPort string) bool {
	// 一条规则内的各子条件是「与」关系，只要有任一子条件不匹配，
	// 整条规则就不匹配；因此逐个子规则检查并在失配时立即返回。

	if rule.SourceAddress != nil && !addressMatches(tracer.t, rule.SourceAddress, sourceIP) {
		return false
	}
	if rule.SourceType != nil {
		addrtype := "not-matched"
		if tracer.localIPs.Has(sourceIP) {
			addrtype = "LOCAL"
		}
		if !rule.SourceType.Matches(addrtype) {
			return false
		}
	}

	if rule.Protocol != nil && !rule.Protocol.Matches(protocol) {
		return false
	}

	if rule.DestinationAddress != nil && !addressMatches(tracer.t, rule.DestinationAddress, destIP) {
		return false
	}
	if rule.DestinationType != nil {
		addrtype := "not-matched"
		if tracer.localIPs.Has(destIP) {
			addrtype = "LOCAL"
		}
		if !rule.DestinationType.Matches(addrtype) {
			return false
		}
	}
	if rule.DestinationPort != nil && !rule.DestinationPort.Matches(destPort) {
		return false
	}

	// 任何依赖历史状态/连接状态的检查都不匹配。
	if rule.AffinityCheck != nil || rule.MarkCheck != nil || rule.CTStateCheck != nil {
		return false
	}

	// 其他条件默认视为匹配，因为模拟器不跟踪这些状态。
	return true
}

// runChain 让数据包依次通过指定表和链中的规则，并相应更新 tracer 的内部状态。
// 若命中终结动作则返回 true。
// runChain 递归执行一条链的规则，处理跳转、MARK、MASQUERADE 和 DNAT 等动作。
func (tracer *iptablesTracer) runChain(table utiliptables.Table, chain utiliptables.Chain, sourceIP, protocol, destIP, destPort string) bool {
	c, _ := tracer.ipt.Dump.GetChain(table, chain)
	if c == nil {
		return false
	}

	for _, rule := range c.Rules {
		if rule.Jump == nil {
			continue
		}

		if !tracer.ruleMatches(rule, sourceIP, protocol, destIP, destPort) {
			continue
		}
		// 记录命中的规则，便于调试。
		tracer.matches = append(tracer.matches, rule.Raw)

		switch rule.Jump.Value {
		case "KUBE-MARK-MASQ":
			tracer.markMasq = true
			continue

		case "ACCEPT", "REJECT", "DROP":
			// 仅在 filter 表中有效。
			tracer.outputs = append(tracer.outputs, rule.Jump.Value)
			return true

		case "DNAT":
			// 仅在 nat 表中有效。
			tracer.outputs = append(tracer.outputs, rule.DNATDestination.Value)
			return true

		default:
			// 跳转到 KUBE-XXX 链时，递归处理该链。
			terminated := tracer.runChain(table, utiliptables.Chain(rule.Jump.Value), sourceIP, protocol, destIP, destPort)

			// 若子链命中终结规则且跳转自身的规则不含概率条件，
			// 则当前链也终止；但若是通过 --probability 跳转过去的，
			// 还要继续拿后续规则与本包匹配，以模拟负载均衡的随机分流。
			if terminated && rule.Probability == nil {
				return true
			}
		}
	}

	return false
}

// tracePacket 模拟具有给定源 IP、协议、目的 IP 和目的端口的数据包在指定 iptables
// 规则中的走向。nodeIPs 是视为本地节点的 IP 列表（用于 "LOCAL" 匹配）。
// 协议值应为 iptables 规则中的小写形式，而非 corev1 中的大写形式。
//
// 返回值为：命中的规则列表（便于调试）、最终目的地址
// （逗号分隔的 IP 列表，或 "ACCEPT"、"DROP"、"REJECT" 等特殊目标），
// 以及数据包是否会被伪装。
// tracePacket 模拟数据包依次经过 nat:PREROUTING、filter 与 nat:POSTROUTING 的走向。
func tracePacket(t *testing.T, ipt *iptablestest.FakeIPTables, sourceIP, protocol, destIP, destPort string, nodeIPs []string) ([]string, string, bool) {
	tracer := newIPTablesTracer(t, ipt, nodeIPs)

	// nat:PREROUTING 的规则最先执行。
	tracer.runChain(utiliptables.TableNAT, utiliptables.ChainPrerouting, sourceIP, protocol, destIP, destPort)

	// PREROUTING 结束后处理待执行的 DNAT，这会影响后续规则匹配到的目的 IP。
	if len(tracer.outputs) != 0 {
		destIP = strings.Split(tracer.outputs[0], ":")[0]
	}

	// 接下来执行 filter 规则；具体执行哪些规则取决于数据包是入站、出站还是本机内部流量，
	// 而模拟器无法得知这一点，因此手工选择相关的链执行。
	// 理论上未来这可能导致与真实行为冲突，届时需要更复杂的模拟。
	tracer.runChain(utiliptables.TableFilter, kubeServicesChain, sourceIP, protocol, destIP, destPort)
	tracer.runChain(utiliptables.TableFilter, kubeExternalServicesChain, sourceIP, protocol, destIP, destPort)
	tracer.runChain(utiliptables.TableFilter, kubeNodePortsChain, sourceIP, protocol, destIP, destPort)
	tracer.runChain(utiliptables.TableFilter, kubeProxyFirewallChain, sourceIP, protocol, destIP, destPort)

	// 最后执行 nat:POSTROUTING；此处唯一关心的是伪装标记转化为实际伪装。

	return tracer.matches, strings.Join(tracer.outputs, ", "), tracer.markMasq
}

type packetFlowTest struct {
	name     string
	sourceIP string
	protocol v1.Protocol
	destIP   string
	destPort int
	output   string
	masq     bool
}

// runPacketFlowTests 对一批模拟流量场景逐一执行 tracePacket，并核对目的与伪装结果。
func runPacketFlowTests(t *testing.T, line int, ipt *iptablestest.FakeIPTables, nodeIPs []string, testCases []packetFlowTest) {
	lineStr := ""
	if line != 0 {
		lineStr = fmt.Sprintf(" (from line %d)", line)
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			protocol := strings.ToLower(string(tc.protocol))
			if protocol == "" {
				protocol = "tcp"
			}
			matches, output, masq := tracePacket(t, ipt, tc.sourceIP, protocol, tc.destIP, fmt.Sprintf("%d", tc.destPort), nodeIPs)
			var errors []string
			if output != tc.output {
				errors = append(errors, fmt.Sprintf("wrong output: expected %q got %q", tc.output, output))
			}
			if masq != tc.masq {
				errors = append(errors, fmt.Sprintf("wrong masq: expected %v got %v", tc.masq, masq))
			}
			if errors != nil {
				t.Errorf("Test %q of a %s packet from %s to %s:%d%s got result:\n%s\n\nBy matching:\n%s\n\n",
					tc.name, protocol, tc.sourceIP, tc.destIP, tc.destPort, lineStr, strings.Join(errors, "\n"), strings.Join(matches, "\n"))
			}
		})
	}
}

// 使用静态数据测试 tracePacket，确保模拟结果符合预期。
// TestTracePacket 使用静态规则集验证数据包模拟的各类匹配路径。
func TestTracePacket(t *testing.T) {
	// 静态规则集覆盖 ACCEPT/DROP/REJECT、健康检查端口以及各服务的 DNAT 路径。
	rules := dedent.Dedent(`
		*filter
		:INPUT - [0:0]
		:FORWARD - [0:0]
		:OUTPUT - [0:0]
		:KUBE-EXTERNAL-SERVICES - [0:0]
		:KUBE-FIREWALL - [0:0]
		:KUBE-FORWARD - [0:0]
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-PROXY-FIREWALL - [0:0]
		-A INPUT -m comment --comment kubernetes health check service ports -j KUBE-NODEPORTS
		-A INPUT -m conntrack --ctstate NEW -m comment --comment kubernetes externally-visible service portals -j KUBE-EXTERNAL-SERVICES
		-A FORWARD -m comment --comment kubernetes forwarding rules -j KUBE-FORWARD
		-A FORWARD -m conntrack --ctstate NEW -m comment --comment kubernetes service portals -j KUBE-SERVICES
		-A FORWARD -m conntrack --ctstate NEW -m comment --comment kubernetes externally-visible service portals -j KUBE-EXTERNAL-SERVICES
		-A OUTPUT -m conntrack --ctstate NEW -m comment --comment kubernetes service portals -j KUBE-SERVICES
		-A KUBE-NODEPORTS -m comment --comment "ns2/svc2:p80 health check node port" -m tcp -p tcp --dport 30000 -j ACCEPT
		-A KUBE-SERVICES -m comment --comment "ns6/svc6:p80 has no endpoints" -m tcp -p tcp -d 172.30.0.46 --dport 80 -j REJECT
		-A KUBE-EXTERNAL-SERVICES -m comment --comment "ns2/svc2:p80 has no local endpoints" -m tcp -p tcp -d 192.168.99.22 --dport 80 -j DROP
		-A KUBE-EXTERNAL-SERVICES -m comment --comment "ns2/svc2:p80 has no local endpoints" -m tcp -p tcp -d 1.2.3.4 --dport 80 -j DROP
		-A KUBE-EXTERNAL-SERVICES -m comment --comment "ns2/svc2:p80 has no local endpoints" -m addrtype --dst-type LOCAL -m tcp -p tcp --dport 3001 -j DROP
		-A KUBE-FIREWALL -m comment --comment "block incoming localnet connections" -d 127.0.0.0/8 ! -s 127.0.0.0/8 -m conntrack ! --ctstate RELATED,ESTABLISHED,DNAT -j DROP
		-A KUBE-FORWARD -m conntrack --ctstate INVALID -j DROP
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
		-A KUBE-PROXY-FIREWALL -m comment --comment "ns5/svc5:p80 traffic not accepted by KUBE-FW-NUKIZ6OKUXPJNT4C" -m tcp -p tcp -d 5.6.7.8 --dport 80 -j DROP
		COMMIT
		*nat
		:PREROUTING - [0:0]
		:INPUT - [0:0]
		:OUTPUT - [0:0]
		:POSTROUTING - [0:0]
		:KUBE-EXT-4SW47YFZTEDKD3PK - [0:0]
		:KUBE-EXT-GNZBNJ2PO5MGZ6GT - [0:0]
		:KUBE-EXT-NUKIZ6OKUXPJNT4C - [0:0]
		:KUBE-EXT-X27LE4BHSL4DOUIK - [0:0]
		:KUBE-FW-NUKIZ6OKUXPJNT4C - [0:0]
		:KUBE-MARK-MASQ - [0:0]
		:KUBE-NODEPORTS - [0:0]
		:KUBE-POSTROUTING - [0:0]
		:KUBE-SEP-C6EBXVWJJZMIWKLZ - [0:0]
		:KUBE-SEP-I77PXRDZVX7PMWMN - [0:0]
		:KUBE-SEP-OYPFS5VJICHGATKP - [0:0]
		:KUBE-SEP-RS4RBKLTHTF2IUXJ - [0:0]
		:KUBE-SEP-SXIVWICOYRO3J4NJ - [0:0]
		:KUBE-SEP-UKSFD7AGPMPPLUHC - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-SVC-4SW47YFZTEDKD3PK - [0:0]
		:KUBE-SVC-GNZBNJ2PO5MGZ6GT - [0:0]
		:KUBE-SVC-NUKIZ6OKUXPJNT4C - [0:0]
		:KUBE-SVC-X27LE4BHSL4DOUIK - [0:0]
		:KUBE-SVC-XPGD46QRK7WJZT7O - [0:0]
		-A PREROUTING -m comment --comment kubernetes service portals -j KUBE-SERVICES
		-A OUTPUT -m comment --comment kubernetes service portals -j KUBE-SERVICES
		-A POSTROUTING -m comment --comment kubernetes postrouting rules -j KUBE-POSTROUTING
		-A KUBE-POSTROUTING -m mark ! --mark 0x4000/0x4000 -j RETURN
		-A KUBE-POSTROUTING -j MARK --xor-mark 0x4000
		-A KUBE-POSTROUTING -m comment --comment "kubernetes service traffic requiring SNAT" -j MASQUERADE --random-fully
		-A KUBE-MARK-MASQ -j MARK --or-mark 0x4000
		-A KUBE-NODEPORTS -m comment --comment ns2/svc2:p80 -m tcp -p tcp --dport 3001 -j KUBE-EXT-GNZBNJ2PO5MGZ6GT
		-A KUBE-NODEPORTS -m comment --comment ns3/svc3:p80 -m tcp -p tcp --dport 3003 -j KUBE-EXT-X27LE4BHSL4DOUIK
		-A KUBE-NODEPORTS -m comment --comment ns5/svc5:p80 -m tcp -p tcp --dport 3002 -j KUBE-EXT-NUKIZ6OKUXPJNT4C
		-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 -j KUBE-SVC-XPGD46QRK7WJZT7O
		-A KUBE-SERVICES -m comment --comment "ns2/svc2:p80 cluster IP" -m tcp -p tcp -d 172.30.0.42 --dport 80 -j KUBE-SVC-GNZBNJ2PO5MGZ6GT
		-A KUBE-SERVICES -m comment --comment "ns2/svc2:p80 external IP" -m tcp -p tcp -d 192.168.99.22 --dport 80 -j KUBE-EXT-GNZBNJ2PO5MGZ6GT
		-A KUBE-SERVICES -m comment --comment "ns2/svc2:p80 loadbalancer IP" -m tcp -p tcp -d 1.2.3.4 --dport 80 -j KUBE-EXT-GNZBNJ2PO5MGZ6GT
		-A KUBE-SERVICES -m comment --comment "ns3/svc3:p80 cluster IP" -m tcp -p tcp -d 172.30.0.43 --dport 80 -j KUBE-SVC-X27LE4BHSL4DOUIK
		-A KUBE-SERVICES -m comment --comment "ns4/svc4:p80 cluster IP" -m tcp -p tcp -d 172.30.0.44 --dport 80 -j KUBE-SVC-4SW47YFZTEDKD3PK
		-A KUBE-SERVICES -m comment --comment "ns4/svc4:p80 external IP" -m tcp -p tcp -d 192.168.99.33 --dport 80 -j KUBE-EXT-4SW47YFZTEDKD3PK
		-A KUBE-SERVICES -m comment --comment "ns5/svc5:p80 cluster IP" -m tcp -p tcp -d 172.30.0.45 --dport 80 -j KUBE-SVC-NUKIZ6OKUXPJNT4C
		-A KUBE-SERVICES -m comment --comment "ns5/svc5:p80 loadbalancer IP" -m tcp -p tcp -d 5.6.7.8 --dport 80 -j KUBE-FW-NUKIZ6OKUXPJNT4C
		-A KUBE-SERVICES -m comment --comment "kubernetes service nodeports; NOTE: this must be the last rule in this chain" -m addrtype --dst-type LOCAL -j KUBE-NODEPORTS
		-A KUBE-EXT-4SW47YFZTEDKD3PK -m comment --comment "masquerade traffic for ns4/svc4:p80 external destinations" -j KUBE-MARK-MASQ
		-A KUBE-EXT-4SW47YFZTEDKD3PK -j KUBE-SVC-4SW47YFZTEDKD3PK
		-A KUBE-EXT-GNZBNJ2PO5MGZ6GT -m comment --comment "pod traffic for ns2/svc2:p80 external destinations" -s 10.0.0.0/8 -j KUBE-SVC-GNZBNJ2PO5MGZ6GT
		-A KUBE-EXT-GNZBNJ2PO5MGZ6GT -m comment --comment "masquerade LOCAL traffic for ns2/svc2:p80 external destinations" -m addrtype --src-type LOCAL -j KUBE-MARK-MASQ
		-A KUBE-EXT-GNZBNJ2PO5MGZ6GT -m comment --comment "route LOCAL traffic for ns2/svc2:p80 external destinations" -m addrtype --src-type LOCAL -j KUBE-SVC-GNZBNJ2PO5MGZ6GT
		-A KUBE-EXT-NUKIZ6OKUXPJNT4C -m comment --comment "masquerade traffic for ns5/svc5:p80 external destinations" -j KUBE-MARK-MASQ
		-A KUBE-EXT-NUKIZ6OKUXPJNT4C -j KUBE-SVC-NUKIZ6OKUXPJNT4C
		-A KUBE-EXT-X27LE4BHSL4DOUIK -m comment --comment "masquerade traffic for ns3/svc3:p80 external destinations" -j KUBE-MARK-MASQ
		-A KUBE-EXT-X27LE4BHSL4DOUIK -j KUBE-SVC-X27LE4BHSL4DOUIK
		-A KUBE-FW-NUKIZ6OKUXPJNT4C -m comment --comment "ns5/svc5:p80 loadbalancer IP" -s 203.0.113.0/25 -j KUBE-EXT-NUKIZ6OKUXPJNT4C
		-A KUBE-FW-NUKIZ6OKUXPJNT4C -m comment --comment "other traffic to ns5/svc5:p80 will be dropped by KUBE-PROXY-FIREWALL"
		-A KUBE-SEP-C6EBXVWJJZMIWKLZ -m comment --comment ns4/svc4:p80 -s 10.180.0.5 -j KUBE-MARK-MASQ
		-A KUBE-SEP-C6EBXVWJJZMIWKLZ -m comment --comment ns4/svc4:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.5:80
		-A KUBE-SEP-I77PXRDZVX7PMWMN -m comment --comment ns5/svc5:p80 -s 10.180.0.3 -j KUBE-MARK-MASQ
		-A KUBE-SEP-I77PXRDZVX7PMWMN -m comment --comment ns5/svc5:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.3:80
		-A KUBE-SEP-OYPFS5VJICHGATKP -m comment --comment ns3/svc3:p80 -s 10.180.0.3 -j KUBE-MARK-MASQ
		-A KUBE-SEP-OYPFS5VJICHGATKP -m comment --comment ns3/svc3:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.3:80
		-A KUBE-SEP-RS4RBKLTHTF2IUXJ -m comment --comment ns2/svc2:p80 -s 10.180.0.2 -j KUBE-MARK-MASQ
		-A KUBE-SEP-RS4RBKLTHTF2IUXJ -m comment --comment ns2/svc2:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.2:80
		-A KUBE-SEP-SXIVWICOYRO3J4NJ -m comment --comment ns1/svc1:p80 -s 10.180.0.1 -j KUBE-MARK-MASQ
		-A KUBE-SEP-SXIVWICOYRO3J4NJ -m comment --comment ns1/svc1:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.1:80
		-A KUBE-SEP-UKSFD7AGPMPPLUHC -m comment --comment ns4/svc4:p80 -s 10.180.0.4 -j KUBE-MARK-MASQ
		-A KUBE-SEP-UKSFD7AGPMPPLUHC -m comment --comment ns4/svc4:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.4:80
		-A KUBE-SVC-4SW47YFZTEDKD3PK -m comment --comment "ns4/svc4:p80 cluster IP" -m tcp -p tcp -d 172.30.0.44 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
		-A KUBE-SVC-4SW47YFZTEDKD3PK -m comment --comment "ns4/svc4:p80 -> 10.180.0.4:80" -m statistic --mode random --probability 0.5000000000 -j KUBE-SEP-UKSFD7AGPMPPLUHC
		-A KUBE-SVC-4SW47YFZTEDKD3PK -m comment --comment "ns4/svc4:p80 -> 10.180.0.5:80" -j KUBE-SEP-C6EBXVWJJZMIWKLZ
		-A KUBE-SVC-GNZBNJ2PO5MGZ6GT -m comment --comment "ns2/svc2:p80 cluster IP" -m tcp -p tcp -d 172.30.0.42 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
		-A KUBE-SVC-GNZBNJ2PO5MGZ6GT -m comment --comment "ns2/svc2:p80 -> 10.180.0.2:80" -j KUBE-SEP-RS4RBKLTHTF2IUXJ
		-A KUBE-SVC-NUKIZ6OKUXPJNT4C -m comment --comment "ns5/svc5:p80 cluster IP" -m tcp -p tcp -d 172.30.0.45 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
		-A KUBE-SVC-NUKIZ6OKUXPJNT4C -m comment --comment "ns5/svc5:p80 -> 10.180.0.3:80" -j KUBE-SEP-I77PXRDZVX7PMWMN
		-A KUBE-SVC-X27LE4BHSL4DOUIK -m comment --comment "ns3/svc3:p80 cluster IP" -m tcp -p tcp -d 172.30.0.43 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
		-A KUBE-SVC-X27LE4BHSL4DOUIK -m comment --comment "ns3/svc3:p80 -> 10.180.0.3:80" -j KUBE-SEP-OYPFS5VJICHGATKP
		-A KUBE-SVC-XPGD46QRK7WJZT7O -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
		-A KUBE-SVC-XPGD46QRK7WJZT7O -m comment --comment "ns1/svc1:p80 -> 10.180.0.1:80" -j KUBE-SEP-SXIVWICOYRO3J4NJ
		COMMIT
		`)

	ipt := iptablestest.NewFake()
	err := ipt.RestoreAll([]byte(rules), utiliptables.NoFlushTables, utiliptables.RestoreCounters)
	if err != nil {
		t.Fatalf("Restore of test data failed: %v", err)
	}

	runPacketFlowTests(t, getLine(), ipt, testNodeIPs, []packetFlowTest{
		{
			name:     "no match",
			sourceIP: "10.0.0.2",
			destIP:   "10.0.0.3",
			destPort: 80,
			output:   "",
		},
		{
			name:     "single endpoint",
			sourceIP: "10.0.0.2",
			destIP:   "172.30.0.41",
			destPort: 80,
			output:   "10.180.0.1:80",
		},
		{
			name:     "multiple endpoints",
			sourceIP: "10.0.0.2",
			destIP:   "172.30.0.44",
			destPort: 80,
			output:   "10.180.0.4:80, 10.180.0.5:80",
		},
		{
			name:     "LOCAL, KUBE-MARK-MASQ",
			sourceIP: testNodeIP,
			destIP:   "192.168.99.22",
			destPort: 80,
			output:   "10.180.0.2:80",
			masq:     true,
		},
		{
			name:     "DROP",
			sourceIP: testExternalClient,
			destIP:   "192.168.99.22",
			destPort: 80,
			output:   "DROP",
		},
		{
			name:     "ACCEPT (NodePortHealthCheck)",
			sourceIP: testNodeIP,
			destIP:   testNodeIP,
			destPort: 30000,
			output:   "ACCEPT",
		},
		{
			// 无端点的 ClusterIP 应 REJECT，而不是静默丢弃。
			name:     "REJECT",
			sourceIP: "10.0.0.2",
			destIP:   "172.30.0.46",
			destPort: 80,
			output:   "REJECT",
		},
	})
}

// TestOverallIPTablesRules 创建多种类型的 Service，并校验生成的 iptables 规则完全符合预期。
// 每个用例独立创建 Proxier，避免规则状态在场景之间相互污染。
func TestOverallIPTablesRules(t *testing.T) {
	logger, _ := klogtesting.NewTestContext(t)
	ipt := iptablestest.NewFake()
	fp := NewFakeProxier(ipt)
	metrics.RegisterMetrics(kubeproxyconfig.ProxyModeIPTables)

	makeServiceMap(fp,
		// 创建 ClusterIP 类型的 Service。
		makeTestService("ns1", "svc1", func(svc *v1.Service) {
			svc.Spec.ClusterIP = "172.30.0.41"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
			}}
		}),
		// 创建流量策略为 Local 的 LoadBalancer Service。
		makeTestService("ns2", "svc2", func(svc *v1.Service) {
			svc.Spec.Type = "LoadBalancer"
			svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
			svc.Spec.ClusterIP = "172.30.0.42"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
				NodePort: 3001,
			}}
			svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
				IP: "1.2.3.4",
			}}
			svc.Spec.ExternalIPs = []string{"192.168.99.22"}
			svc.Spec.HealthCheckNodePort = 30000
		}),
		// 创建 NodePort 类型的 Service。
		makeTestService("ns3", "svc3", func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = "172.30.0.43"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
				NodePort: 3003,
			}}
		}),
		// 创建带 ExternalIP 的 Service。
		makeTestService("ns4", "svc4", func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = "172.30.0.44"
			svc.Spec.ExternalIPs = []string{"192.168.99.33"}
			svc.Spec.Ports = []v1.ServicePort{{
				Name:       "p80",
				Port:       80,
				Protocol:   v1.ProtocolTCP,
				TargetPort: intstr.FromInt32(80),
			}}
		}),
		// 创建流量策略为 Cluster、带来源范围和会话亲和性的 LoadBalancer Service。
		// 同时验证会话亲和性。
		makeTestService("ns5", "svc5", func(svc *v1.Service) {
			svc.Spec.Type = "LoadBalancer"
			svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyCluster
			svc.Spec.ClusterIP = "172.30.0.45"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
				NodePort: 3002,
			}}
			svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
				IP: "5.6.7.8",
			}}
			svc.Spec.HealthCheckNodePort = 30000
			// 故意插入多余空白，验证兼容性：非法格式不会导致 panic，
			// 只会被忽略。
			svc.Spec.LoadBalancerSourceRanges = []string{" 203.0.113.0/25"}

			svc.Spec.SessionAffinity = v1.ServiceAffinityClientIP
			svc.Spec.SessionAffinityConfig = &v1.SessionAffinityConfig{
				ClientIP: &v1.ClientIPConfig{
					TimeoutSeconds: ptr.To[int32](10800),
				},
			}
		}),
		// 创建没有端点的 ClusterIP Service。
		makeTestService("ns6", "svc6", func(svc *v1.Service) {
			svc.Spec.Type = "ClusterIP"
			svc.Spec.ClusterIP = "172.30.0.46"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:       "p80",
				Port:       80,
				Protocol:   v1.ProtocolTCP,
				TargetPort: intstr.FromInt32(80),
			}}
		}),
	)
	populateEndpointSlices(fp,
		// 创建 ClusterIP Service 的端点。
		makeTestEndpointSlice("ns1", "svc1", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.180.0.1"},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p80"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		// 创建 Local 策略 LoadBalancer 的端点；由于未设置 NodeName，
		// 该端点会被视为非本地端点而忽略。
		makeTestEndpointSlice("ns2", "svc2", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.180.0.2"},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p80"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		// 创建 NodePort Service 的端点。
		makeTestEndpointSlice("ns3", "svc3", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.180.0.3"},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p80"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		// 创建 ExternalIP Service 的端点。
		makeTestEndpointSlice("ns4", "svc4", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.180.0.4"},
			}, {
				Addresses: []string{"10.180.0.5"},
				NodeName:  ptr.To(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p80"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		// 创建 Cluster 策略 LoadBalancer 的端点。
		makeTestEndpointSlice("ns5", "svc5", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.180.0.3"},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p80"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	fp.syncProxyRules()

	expected := dedent.Dedent(`
		*filter
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-EXTERNAL-SERVICES - [0:0]
		:KUBE-FIREWALL - [0:0]
		:KUBE-FORWARD - [0:0]
		:KUBE-PROXY-FIREWALL - [0:0]
		-A KUBE-NODEPORTS -m comment --comment "ns2/svc2:p80 health check node port" -m tcp -p tcp --dport 30000 -j ACCEPT
		-A KUBE-SERVICES -m comment --comment "ns6/svc6:p80 has no endpoints" -m tcp -p tcp -d 172.30.0.46 --dport 80 -j REJECT
		-A KUBE-EXTERNAL-SERVICES -m comment --comment "ns2/svc2:p80 has no local endpoints" -m tcp -p tcp -d 192.168.99.22 --dport 80 -j DROP
		-A KUBE-EXTERNAL-SERVICES -m comment --comment "ns2/svc2:p80 has no local endpoints" -m tcp -p tcp -d 1.2.3.4 --dport 80 -j DROP
		-A KUBE-EXTERNAL-SERVICES -m comment --comment "ns2/svc2:p80 has no local endpoints" -m addrtype --dst-type LOCAL -m tcp -p tcp --dport 3001 -j DROP
		-A KUBE-FIREWALL -m comment --comment "block incoming localnet connections" -d 127.0.0.0/8 ! -s 127.0.0.0/8 -m conntrack ! --ctstate RELATED,ESTABLISHED,DNAT -j DROP
		-A KUBE-FORWARD -m conntrack --ctstate INVALID -m nfacct --nfacct-name ct_state_invalid_dropped_pkts -j DROP
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
		-A KUBE-PROXY-FIREWALL -m comment --comment "ns5/svc5:p80 traffic not accepted by KUBE-FW-NUKIZ6OKUXPJNT4C" -m tcp -p tcp -d 5.6.7.8 --dport 80 -j DROP
		COMMIT
		*nat
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-EXT-4SW47YFZTEDKD3PK - [0:0]
		:KUBE-EXT-GNZBNJ2PO5MGZ6GT - [0:0]
		:KUBE-EXT-NUKIZ6OKUXPJNT4C - [0:0]
		:KUBE-EXT-X27LE4BHSL4DOUIK - [0:0]
		:KUBE-FW-NUKIZ6OKUXPJNT4C - [0:0]
		:KUBE-MARK-MASQ - [0:0]
		:KUBE-POSTROUTING - [0:0]
		:KUBE-SEP-C6EBXVWJJZMIWKLZ - [0:0]
		:KUBE-SEP-I77PXRDZVX7PMWMN - [0:0]
		:KUBE-SEP-OYPFS5VJICHGATKP - [0:0]
		:KUBE-SEP-RS4RBKLTHTF2IUXJ - [0:0]
		:KUBE-SEP-SXIVWICOYRO3J4NJ - [0:0]
		:KUBE-SEP-UKSFD7AGPMPPLUHC - [0:0]
		:KUBE-SVC-4SW47YFZTEDKD3PK - [0:0]
		:KUBE-SVC-GNZBNJ2PO5MGZ6GT - [0:0]
		:KUBE-SVC-NUKIZ6OKUXPJNT4C - [0:0]
		:KUBE-SVC-X27LE4BHSL4DOUIK - [0:0]
		:KUBE-SVC-XPGD46QRK7WJZT7O - [0:0]
		-A KUBE-NODEPORTS -m comment --comment ns2/svc2:p80 -m tcp -p tcp -d 127.0.0.0/8 --dport 3001 -m nfacct --nfacct-name localhost_nps_accepted_pkts -j KUBE-EXT-GNZBNJ2PO5MGZ6GT
		-A KUBE-NODEPORTS -m comment --comment ns2/svc2:p80 -m tcp -p tcp --dport 3001 -j KUBE-EXT-GNZBNJ2PO5MGZ6GT
		-A KUBE-NODEPORTS -m comment --comment ns3/svc3:p80 -m tcp -p tcp -d 127.0.0.0/8 --dport 3003 -m nfacct --nfacct-name localhost_nps_accepted_pkts -j KUBE-EXT-X27LE4BHSL4DOUIK
		-A KUBE-NODEPORTS -m comment --comment ns3/svc3:p80 -m tcp -p tcp --dport 3003 -j KUBE-EXT-X27LE4BHSL4DOUIK
		-A KUBE-NODEPORTS -m comment --comment ns5/svc5:p80 -m tcp -p tcp -d 127.0.0.0/8 --dport 3002 -m nfacct --nfacct-name localhost_nps_accepted_pkts -j KUBE-EXT-NUKIZ6OKUXPJNT4C
		-A KUBE-NODEPORTS -m comment --comment ns5/svc5:p80 -m tcp -p tcp --dport 3002 -j KUBE-EXT-NUKIZ6OKUXPJNT4C
		-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 -j KUBE-SVC-XPGD46QRK7WJZT7O
		-A KUBE-SERVICES -m comment --comment "ns2/svc2:p80 cluster IP" -m tcp -p tcp -d 172.30.0.42 --dport 80 -j KUBE-SVC-GNZBNJ2PO5MGZ6GT
		-A KUBE-SERVICES -m comment --comment "ns2/svc2:p80 external IP" -m tcp -p tcp -d 192.168.99.22 --dport 80 -j KUBE-EXT-GNZBNJ2PO5MGZ6GT
		-A KUBE-SERVICES -m comment --comment "ns2/svc2:p80 loadbalancer IP" -m tcp -p tcp -d 1.2.3.4 --dport 80 -j KUBE-EXT-GNZBNJ2PO5MGZ6GT
		-A KUBE-SERVICES -m comment --comment "ns3/svc3:p80 cluster IP" -m tcp -p tcp -d 172.30.0.43 --dport 80 -j KUBE-SVC-X27LE4BHSL4DOUIK
		-A KUBE-SERVICES -m comment --comment "ns4/svc4:p80 cluster IP" -m tcp -p tcp -d 172.30.0.44 --dport 80 -j KUBE-SVC-4SW47YFZTEDKD3PK
		-A KUBE-SERVICES -m comment --comment "ns4/svc4:p80 external IP" -m tcp -p tcp -d 192.168.99.33 --dport 80 -j KUBE-EXT-4SW47YFZTEDKD3PK
		-A KUBE-SERVICES -m comment --comment "ns5/svc5:p80 cluster IP" -m tcp -p tcp -d 172.30.0.45 --dport 80 -j KUBE-SVC-NUKIZ6OKUXPJNT4C
		-A KUBE-SERVICES -m comment --comment "ns5/svc5:p80 loadbalancer IP" -m tcp -p tcp -d 5.6.7.8 --dport 80 -j KUBE-FW-NUKIZ6OKUXPJNT4C
		-A KUBE-SERVICES -m comment --comment "kubernetes service nodeports; NOTE: this must be the last rule in this chain" -m addrtype --dst-type LOCAL -j KUBE-NODEPORTS
		-A KUBE-EXT-4SW47YFZTEDKD3PK -m comment --comment "masquerade traffic for ns4/svc4:p80 external destinations" -j KUBE-MARK-MASQ
		-A KUBE-EXT-4SW47YFZTEDKD3PK -j KUBE-SVC-4SW47YFZTEDKD3PK
		-A KUBE-EXT-GNZBNJ2PO5MGZ6GT -m comment --comment "pod traffic for ns2/svc2:p80 external destinations" -s 10.0.0.0/8 -j KUBE-SVC-GNZBNJ2PO5MGZ6GT
		-A KUBE-EXT-GNZBNJ2PO5MGZ6GT -m comment --comment "masquerade LOCAL traffic for ns2/svc2:p80 external destinations" -m addrtype --src-type LOCAL -j KUBE-MARK-MASQ
		-A KUBE-EXT-GNZBNJ2PO5MGZ6GT -m comment --comment "route LOCAL traffic for ns2/svc2:p80 external destinations" -m addrtype --src-type LOCAL -j KUBE-SVC-GNZBNJ2PO5MGZ6GT
		-A KUBE-EXT-NUKIZ6OKUXPJNT4C -m comment --comment "masquerade traffic for ns5/svc5:p80 external destinations" -j KUBE-MARK-MASQ
		-A KUBE-EXT-NUKIZ6OKUXPJNT4C -j KUBE-SVC-NUKIZ6OKUXPJNT4C
		-A KUBE-EXT-X27LE4BHSL4DOUIK -m comment --comment "masquerade traffic for ns3/svc3:p80 external destinations" -j KUBE-MARK-MASQ
		-A KUBE-EXT-X27LE4BHSL4DOUIK -j KUBE-SVC-X27LE4BHSL4DOUIK
		-A KUBE-FW-NUKIZ6OKUXPJNT4C -m comment --comment "ns5/svc5:p80 loadbalancer IP" -s 203.0.113.0/25 -j KUBE-EXT-NUKIZ6OKUXPJNT4C
		-A KUBE-FW-NUKIZ6OKUXPJNT4C -m comment --comment "other traffic to ns5/svc5:p80 will be dropped by KUBE-PROXY-FIREWALL"
		-A KUBE-MARK-MASQ -j MARK --or-mark 0x4000
		-A KUBE-POSTROUTING -m mark ! --mark 0x4000/0x4000 -j RETURN
		-A KUBE-POSTROUTING -j MARK --xor-mark 0x4000
		-A KUBE-POSTROUTING -m comment --comment "kubernetes service traffic requiring SNAT" -j MASQUERADE --random-fully
		-A KUBE-SEP-C6EBXVWJJZMIWKLZ -m comment --comment ns4/svc4:p80 -s 10.180.0.5 -j KUBE-MARK-MASQ
		-A KUBE-SEP-C6EBXVWJJZMIWKLZ -m comment --comment ns4/svc4:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.5:80
		-A KUBE-SEP-I77PXRDZVX7PMWMN -m comment --comment ns5/svc5:p80 -s 10.180.0.3 -j KUBE-MARK-MASQ
		-A KUBE-SEP-I77PXRDZVX7PMWMN -m comment --comment ns5/svc5:p80 -m recent --name KUBE-SEP-I77PXRDZVX7PMWMN --set -m tcp -p tcp -j DNAT --to-destination 10.180.0.3:80
		-A KUBE-SEP-OYPFS5VJICHGATKP -m comment --comment ns3/svc3:p80 -s 10.180.0.3 -j KUBE-MARK-MASQ
		-A KUBE-SEP-OYPFS5VJICHGATKP -m comment --comment ns3/svc3:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.3:80
		-A KUBE-SEP-RS4RBKLTHTF2IUXJ -m comment --comment ns2/svc2:p80 -s 10.180.0.2 -j KUBE-MARK-MASQ
		-A KUBE-SEP-RS4RBKLTHTF2IUXJ -m comment --comment ns2/svc2:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.2:80
		-A KUBE-SEP-SXIVWICOYRO3J4NJ -m comment --comment ns1/svc1:p80 -s 10.180.0.1 -j KUBE-MARK-MASQ
		-A KUBE-SEP-SXIVWICOYRO3J4NJ -m comment --comment ns1/svc1:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.1:80
		-A KUBE-SEP-UKSFD7AGPMPPLUHC -m comment --comment ns4/svc4:p80 -s 10.180.0.4 -j KUBE-MARK-MASQ
		-A KUBE-SEP-UKSFD7AGPMPPLUHC -m comment --comment ns4/svc4:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.4:80
		-A KUBE-SVC-4SW47YFZTEDKD3PK -m comment --comment "ns4/svc4:p80 cluster IP" -m tcp -p tcp -d 172.30.0.44 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
		-A KUBE-SVC-4SW47YFZTEDKD3PK -m comment --comment "ns4/svc4:p80 -> 10.180.0.4:80" -m statistic --mode random --probability 0.5000000000 -j KUBE-SEP-UKSFD7AGPMPPLUHC
		-A KUBE-SVC-4SW47YFZTEDKD3PK -m comment --comment "ns4/svc4:p80 -> 10.180.0.5:80" -j KUBE-SEP-C6EBXVWJJZMIWKLZ
		-A KUBE-SVC-GNZBNJ2PO5MGZ6GT -m comment --comment "ns2/svc2:p80 cluster IP" -m tcp -p tcp -d 172.30.0.42 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
		-A KUBE-SVC-GNZBNJ2PO5MGZ6GT -m comment --comment "ns2/svc2:p80 -> 10.180.0.2:80" -j KUBE-SEP-RS4RBKLTHTF2IUXJ
		-A KUBE-SVC-NUKIZ6OKUXPJNT4C -m comment --comment "ns5/svc5:p80 cluster IP" -m tcp -p tcp -d 172.30.0.45 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
		-A KUBE-SVC-NUKIZ6OKUXPJNT4C -m comment --comment "ns5/svc5:p80 -> 10.180.0.3:80" -m recent --name KUBE-SEP-I77PXRDZVX7PMWMN --rcheck --seconds 10800 --reap -j KUBE-SEP-I77PXRDZVX7PMWMN
		-A KUBE-SVC-NUKIZ6OKUXPJNT4C -m comment --comment "ns5/svc5:p80 -> 10.180.0.3:80" -j KUBE-SEP-I77PXRDZVX7PMWMN
		-A KUBE-SVC-X27LE4BHSL4DOUIK -m comment --comment "ns3/svc3:p80 cluster IP" -m tcp -p tcp -d 172.30.0.43 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
		-A KUBE-SVC-X27LE4BHSL4DOUIK -m comment --comment "ns3/svc3:p80 -> 10.180.0.3:80" -j KUBE-SEP-OYPFS5VJICHGATKP
		-A KUBE-SVC-XPGD46QRK7WJZT7O -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
		-A KUBE-SVC-XPGD46QRK7WJZT7O -m comment --comment "ns1/svc1:p80 -> 10.180.0.1:80" -j KUBE-SEP-SXIVWICOYRO3J4NJ
		COMMIT
		`)

	assertIPTablesRulesEqual(t, getLine(), true, expected, fp.iptablesData.String())

	nNatRules := countRulesFromMetric(logger, utiliptables.TableNAT, string(fp.ipFamily))
	expectedNatRules := countRules(logger, utiliptables.TableNAT, fp.iptablesData.String())

	if nNatRules != expectedNatRules {
		t.Fatalf("Wrong number of nat rules: expected %d received %d", expectedNatRules, nNatRules)
	}
}

// TestNoEndpointsReject 验证没有端点时，ClusterIP、ExternalIP、NodePort 和 LoadBalancer IP
// 上的连接都会被 REJECT。
// 分别验证四种外部入口在无端点时都被 REJECT，而不是丢弃或放行。
func TestNoEndpointsReject(t *testing.T) {
	// 同一 Service 同时暴露 ClusterIP、ExternalIP、NodePort 和 LB IP，
	ipt := iptablestest.NewFake()
	// 以便用一组数据包场景覆盖四类入口的无端点行为。
	fp := NewFakeProxier(ipt)
	svcIP := "172.30.0.41"
	svcPort := 80
	svcNodePort := 3001
	svcExternalIPs := "192.168.99.11"
	svcLBIP := "1.2.3.4"
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
	}

	makeServiceMap(fp,
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeLoadBalancer
			svc.Spec.ClusterIP = svcIP
			svc.Spec.ExternalIPs = []string{svcExternalIPs}
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName.Port,
				Protocol: v1.ProtocolTCP,
				Port:     int32(svcPort),
				NodePort: int32(svcNodePort),
			}}
			svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
				IP: svcLBIP,
			}}
		}),
	)
	// 只建 Service 不建端点，模拟服务刚创建、后端尚未就绪的状态。
	fp.syncProxyRules()

	runPacketFlowTests(t, getLine(), ipt, testNodeIPs, []packetFlowTest{
		{
			name:     "pod to cluster IP with no endpoints",
			sourceIP: "10.0.0.2",
			destIP:   svcIP,
			destPort: svcPort,
			output:   "REJECT",
		},
		{
			name:     "external to external IP with no endpoints",
			sourceIP: testExternalClient,
			destIP:   svcExternalIPs,
			destPort: svcPort,
			output:   "REJECT",
		},
		{
			name:     "pod to NodePort with no endpoints",
			sourceIP: "10.0.0.2",
			destIP:   testNodeIP,
			destPort: svcNodePort,
			output:   "REJECT",
		},
		{
			name:     "external to NodePort with no endpoints",
			sourceIP: testExternalClient,
			destIP:   testNodeIP,
			destPort: svcNodePort,
			output:   "REJECT",
		},
		{
			name:     "pod to LoadBalancer IP with no endpoints",
			sourceIP: "10.0.0.2",
			destIP:   svcLBIP,
			destPort: svcPort,
			output:   "REJECT",
		},
		{
			name:     "external to LoadBalancer IP with no endpoints",
			sourceIP: testExternalClient,
			destIP:   svcLBIP,
			destPort: svcPort,
			output:   "REJECT",
		},
	})
}

// TestClusterIPGeneral 测试 ClusterIP Service 的各项基本特性。
// 覆盖 ClusterIP 规则、端口协议差异、会话亲和性与端点变更等场景。
// 同一 Service 声明 TCP/UDP/SCTP 端口，便于验证协议匹配的规则差异。
func TestClusterIPGeneral(t *testing.T) {
	ipt := iptablestest.NewFake()
	fp := NewFakeProxier(ipt)

	makeServiceMap(fp,
		makeTestService("ns1", "svc1", func(svc *v1.Service) {
			svc.Spec.ClusterIP = "172.30.0.41"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "http",
				Port:     80,
				Protocol: v1.ProtocolTCP,
			}}
		}),
		makeTestService("ns2", "svc2", func(svc *v1.Service) {
			svc.Spec.ClusterIP = "172.30.0.42"
			svc.Spec.Ports = []v1.ServicePort{
				{
					Name:     "http",
					Port:     80,
					Protocol: v1.ProtocolTCP,
				},
				{
					Name:       "https",
					Port:       443,
					Protocol:   v1.ProtocolTCP,
					TargetPort: intstr.FromInt32(8443),
				},
				{
					Name:     "dns-udp",
					Port:     53,
					Protocol: v1.ProtocolUDP,
				},
				{
					Name:     "dns-tcp",
					Port:     53,
					Protocol: v1.ProtocolTCP,
					// TCP 使用 TargetPort 而 UDP/SCTP 不使用，
					// 以便在期望输出中区分协议。
					TargetPort: intstr.FromInt32(5353),
				},
				{
					Name:     "dns-sctp",
					Port:     53,
					Protocol: v1.ProtocolSCTP,
				},
			}
		}),
	)

	populateEndpointSlices(fp,
		makeTestEndpointSlice("ns1", "svc1", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.180.0.1"},
				NodeName:  ptr.To(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("http"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		makeTestEndpointSlice("ns2", "svc2", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{
				{
					Addresses: []string{"10.180.0.1"},
					NodeName:  ptr.To(testNodeName),
				},
				{
					Addresses: []string{"10.180.2.1"},
					NodeName:  ptr.To("node2"),
				},
			}
			eps.Ports = []discovery.EndpointPort{
				{
					Name:     ptr.To("http"),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				},
				{
					Name:     ptr.To("https"),
					Port:     ptr.To[int32](8443),
					Protocol: ptr.To(v1.ProtocolTCP),
				},
				{
					Name:     ptr.To("dns-udp"),
					Port:     ptr.To[int32](53),
					Protocol: ptr.To(v1.ProtocolUDP),
				},
				{
					Name:     ptr.To("dns-tcp"),
					Port:     ptr.To[int32](5353),
					Protocol: ptr.To(v1.ProtocolTCP),
				},
				{
					Name:     ptr.To("dns-sctp"),
					Port:     ptr.To[int32](53),
					Protocol: ptr.To(v1.ProtocolSCTP),
				},
			}
		}),
	)

	fp.syncProxyRules()

	runPacketFlowTests(t, getLine(), ipt, testNodeIPs, []packetFlowTest{
		{
			name:     "simple clusterIP",
			sourceIP: "10.180.0.2",
			destIP:   "172.30.0.41",
			destPort: 80,
			output:   "10.180.0.1:80",
			masq:     false,
		},
		{
			name:     "hairpin to cluster IP",
			sourceIP: "10.180.0.1",
			destIP:   "172.30.0.41",
			destPort: 80,
			output:   "10.180.0.1:80",
			masq:     true,
		},
		{
			name:     "clusterIP with multiple endpoints",
			sourceIP: "10.180.0.2",
			destIP:   "172.30.0.42",
			destPort: 80,
			output:   "10.180.0.1:80, 10.180.2.1:80",
			masq:     false,
		},
		{
			name:     "clusterIP with TargetPort",
			sourceIP: "10.180.0.2",
			destIP:   "172.30.0.42",
			destPort: 443,
			output:   "10.180.0.1:8443, 10.180.2.1:8443",
			masq:     false,
		},
		{
			name:     "clusterIP with TCP, UDP, and SCTP on same port (TCP)",
			sourceIP: "10.180.0.2",
			protocol: v1.ProtocolTCP,
			destIP:   "172.30.0.42",
			destPort: 53,
			output:   "10.180.0.1:5353, 10.180.2.1:5353",
			masq:     false,
		},
		{
			name:     "clusterIP with TCP, UDP, and SCTP on same port (UDP)",
			sourceIP: "10.180.0.2",
			protocol: v1.ProtocolUDP,
			destIP:   "172.30.0.42",
			destPort: 53,
			output:   "10.180.0.1:53, 10.180.2.1:53",
			masq:     false,
		},
		{
			name:     "clusterIP with TCP, UDP, and SCTP on same port (SCTP)",
			sourceIP: "10.180.0.2",
			protocol: v1.ProtocolSCTP,
			destIP:   "172.30.0.42",
			destPort: 53,
			output:   "10.180.0.1:53, 10.180.2.1:53",
			masq:     false,
		},
		{
			name:     "TCP-only port does not match UDP traffic",
			sourceIP: "10.180.0.2",
			protocol: v1.ProtocolUDP,
			destIP:   "172.30.0.42",
			destPort: 80,
			output:   "",
		},
		{
			name:     "svc1 does not accept svc2's ports",
			sourceIP: "10.180.0.2",
			destIP:   "172.30.0.41",
			destPort: 443,
			output:   "",
		},
	})
}

// TestLoadBalancer 验证 LoadBalancer Service 的规则生成，包括来源范围、会话亲和性和
// 本地/集群流量策略下的差异。
func TestLoadBalancer(t *testing.T) {
	ipt := iptablestest.NewFake()
	fp := NewFakeProxier(ipt)
	svcIP := "172.30.0.41"
	svcPort := 80
	svcNodePort := 3001
	svcLBIP1 := "1.2.3.4"
	svcLBIP2 := "5.6.7.8"
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	makeServiceMap(fp,
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.Type = "LoadBalancer"
			svc.Spec.ClusterIP = svcIP
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName.Port,
				Port:     int32(svcPort),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(svcNodePort),
			}}
			svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{
				{IP: svcLBIP1},
				{IP: svcLBIP2},
			}
			svc.Spec.LoadBalancerSourceRanges = []string{
				"192.168.0.0/24",

				// 回归测试：多余的空白应被忽略，而不是生成额外规则。
				" 203.0.113.0/25",
			}
		}),
	)

	epIP := "10.180.0.1"
	populateEndpointSlices(fp,
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIP},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	fp.syncProxyRules()

	runPacketFlowTests(t, getLine(), ipt, testNodeIPs, []packetFlowTest{
		{
			name:     "pod to cluster IP",
			sourceIP: "10.0.0.2",
			destIP:   svcIP,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d", epIP, svcPort),
			masq:     false,
		},
		{
			name:     "external to nodePort",
			sourceIP: testExternalClient,
			destIP:   testNodeIP,
			destPort: svcNodePort,
			output:   fmt.Sprintf("%s:%d", epIP, svcPort),
			masq:     true,
		},
		{
			name:     "nodePort bypasses LoadBalancerSourceRanges",
			sourceIP: testExternalClientBlocked,
			destIP:   testNodeIP,
			destPort: svcNodePort,
			output:   fmt.Sprintf("%s:%d", epIP, svcPort),
			masq:     true,
		},
		{
			name:     "accepted external to LB1",
			sourceIP: testExternalClient,
			destIP:   svcLBIP1,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d", epIP, svcPort),
			masq:     true,
		},
		{
			name:     "accepted external to LB2",
			sourceIP: testExternalClient,
			destIP:   svcLBIP2,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d", epIP, svcPort),
			masq:     true,
		},
		{
			name:     "blocked external to LB1",
			sourceIP: testExternalClientBlocked,
			destIP:   svcLBIP1,
			destPort: svcPort,
			output:   "DROP",
		},
		{
			name:     "blocked external to LB2",
			sourceIP: testExternalClientBlocked,
			destIP:   svcLBIP2,
			destPort: svcPort,
			output:   "DROP",
		},
		{
			name:     "pod to LB1 (blocked by LoadBalancerSourceRanges)",
			sourceIP: "10.0.0.2",
			destIP:   svcLBIP1,
			destPort: svcPort,
			output:   "DROP",
		},
		{
			name:     "pod to LB2 (blocked by LoadBalancerSourceRanges)",
			sourceIP: "10.0.0.2",
			destIP:   svcLBIP2,
			destPort: svcPort,
			output:   "DROP",
		},
		{
			name:     "node to LB1 (allowed by LoadBalancerSourceRanges)",
			sourceIP: testNodeIP,
			destIP:   svcLBIP1,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d", epIP, svcPort),
			masq:     true,
		},
		{
			name:     "node to LB2 (allowed by LoadBalancerSourceRanges)",
			sourceIP: testNodeIP,
			destIP:   svcLBIP2,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d", epIP, svcPort),
			masq:     true,
		},

		// LB 规则假定从节点访问 LB IP 时，kube-proxy 之外还会有组件把连接
		// SNAT 成 LB IP；因此如果 LoadBalancerSourceRanges 包含节点 IP，
		// 还要额外放行来自 LB IP 本身的流量，避免自己被规则拒绝。
		{
			name:     "same node to LB1, SNATted to LB1 (implicitly allowed)",
			sourceIP: svcLBIP1,
			destIP:   svcLBIP1,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d", epIP, svcPort),
			masq:     true,
		},
		{
			name:     "same node to LB2, SNATted to LB2 (implicitly allowed)",
			sourceIP: svcLBIP2,
			destIP:   svcLBIP2,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d", epIP, svcPort),
			masq:     true,
		},
	})
}

// TestNodePorts 在 --nodeport-addresses 与 --localhost-nodeports 的各种组合下测试 NodePort Service。
// 通过表格覆盖 --nodeport-addresses 与 --localhost-nodeports 的组合，
// 并分别为 IPv4/IPv6 构造期望规则。
func TestNodePorts(t *testing.T) {
	testCases := []struct {
		name string

		family             v1.IPFamily
		localhostNodePorts bool
		nodePortAddresses  []string

		// allowAltNodeIP 表示期望备用节点 IP 上的 NodePort 流量被接受。
		allowAltNodeIP bool

		// expectFirewall 表示期望 KUBE-FIREWALL 链中包含反伪造（anti-martian）规则。
		expectFirewall bool
	}{
		{
			name: "ipv4, localhost-nodeports enabled",

			family:             v1.IPv4Protocol,
			localhostNodePorts: true,
			nodePortAddresses:  nil,

			allowAltNodeIP: true,
			expectFirewall: true,
		},
		{
			name: "ipv4, localhost-nodeports disabled",

			family:             v1.IPv4Protocol,
			localhostNodePorts: false,
			nodePortAddresses:  nil,

			allowAltNodeIP: true,
			expectFirewall: false,
		},
		{
			name: "ipv4, localhost-nodeports disabled, localhost in nodeport-addresses",

			family:             v1.IPv4Protocol,
			localhostNodePorts: false,
			nodePortAddresses:  []string{"192.168.0.0/24", "127.0.0.1/32"},

			allowAltNodeIP: false,
			expectFirewall: false,
		},
		{
			name: "ipv4, localhost-nodeports enabled, multiple nodeport-addresses",

			family:             v1.IPv4Protocol,
			localhostNodePorts: false,
			nodePortAddresses:  []string{"192.168.0.0/24", "192.168.1.0/24", "2001:db8::/64"},

			allowAltNodeIP: true,
			expectFirewall: false,
		},
		{
			name: "ipv6, localhost-nodeports enabled",

			family:             v1.IPv6Protocol,
			localhostNodePorts: true,
			nodePortAddresses:  nil,

			allowAltNodeIP: true,
			expectFirewall: false,
		},
		{
			name: "ipv6, localhost-nodeports disabled",

			family:             v1.IPv6Protocol,
			localhostNodePorts: false,
			nodePortAddresses:  nil,

			allowAltNodeIP: true,
			expectFirewall: false,
		},
		{
			name: "ipv6, localhost-nodeports disabled, multiple nodeport-addresses",

			family:             v1.IPv6Protocol,
			localhostNodePorts: false,
			nodePortAddresses:  []string{"192.168.0.0/24", "192.168.1.0/24", "2001:db8::/64"},

			allowAltNodeIP: false,
			expectFirewall: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var ipt *iptablestest.FakeIPTables
			var svcIP, epIP1, epIP2 string
			if tc.family == v1.IPv4Protocol {
				ipt = iptablestest.NewFake()
				svcIP = "172.30.0.41"
				epIP1 = "10.180.0.1"
				epIP2 = "10.180.2.1"
			} else {
				ipt = iptablestest.NewIPv6Fake()
				svcIP = "fd00:172:30::41"
				epIP1 = "fd00:10:180::1"
				epIP2 = "fd00:10:180::2:1"
			}
			fp := NewFakeProxier(ipt)
			fp.localhostNodePorts = tc.localhostNodePorts
			if tc.nodePortAddresses != nil {
				fp.nodePortAddresses = proxyutil.NewNodePortAddresses(tc.family, tc.nodePortAddresses)
			}

			makeServiceMap(fp,
				makeTestService("ns1", "svc1", func(svc *v1.Service) {
					svc.Spec.Type = v1.ServiceTypeNodePort
					svc.Spec.ClusterIP = svcIP
					svc.Spec.Ports = []v1.ServicePort{{
						Name:     "p80",
						Port:     80,
						Protocol: v1.ProtocolTCP,
						NodePort: 3001,
					}}
				}),
			)

			populateEndpointSlices(fp,
				makeTestEndpointSlice("ns1", "svc1", 1, func(eps *discovery.EndpointSlice) {
					if tc.family == v1.IPv4Protocol {
						eps.AddressType = discovery.AddressTypeIPv4
					} else {
						eps.AddressType = discovery.AddressTypeIPv6
					}
					eps.Endpoints = []discovery.Endpoint{{
						Addresses: []string{epIP1},
						NodeName:  nil,
					}, {
						Addresses: []string{epIP2},
						NodeName:  ptr.To(testNodeName),
					}}
					eps.Ports = []discovery.EndpointPort{{
						Name:     ptr.To("p80"),
						Port:     ptr.To[int32](80),
						Protocol: ptr.To(v1.ProtocolTCP),
					}}
				}),
			)

			fp.syncProxyRules()

			var podIP, externalClientIP, nodeIP, altNodeIP, localhostIP string
			if tc.family == v1.IPv4Protocol {
				podIP = "10.0.0.2"
				externalClientIP = testExternalClient
				nodeIP = testNodeIP
				altNodeIP = testNodeIPAlt
				localhostIP = "127.0.0.1"
			} else {
				podIP = "fd00:10::2"
				externalClientIP = "2600:5200::1"
				nodeIP = testNodeIPv6
				altNodeIP = testNodeIPv6Alt
				localhostIP = "::1"
			}
			output := net.JoinHostPort(epIP1, "80") + ", " + net.JoinHostPort(epIP2, "80")

			// 各用例的基础规则一致，先统一构造。
			runPacketFlowTests(t, getLine(), ipt, testNodeIPs, []packetFlowTest{
				{
					name:     "pod to cluster IP",
					sourceIP: podIP,
					destIP:   svcIP,
					destPort: 80,
					output:   output,
					masq:     false,
				},
				{
					name:     "external to nodePort",
					sourceIP: externalClientIP,
					destIP:   nodeIP,
					destPort: 3001,
					output:   output,
					masq:     true,
				},
				{
					name:     "node to nodePort",
					sourceIP: nodeIP,
					destIP:   nodeIP,
					destPort: 3001,
					output:   output,
					masq:     true,
				},
			})

			// 仅 IPv4 允许从 localhost 访问 NodePort，且未被显式禁用时才行。
			if tc.family == v1.IPv4Protocol && tc.localhostNodePorts {
				runPacketFlowTests(t, getLine(), ipt, testNodeIPs, []packetFlowTest{
					{
						name:     "localhost to nodePort gets masqueraded",
						sourceIP: localhostIP,
						destIP:   localhostIP,
						destPort: 3001,
						output:   output,
						masq:     true,
					},
				})
			} else {
				runPacketFlowTests(t, getLine(), ipt, testNodeIPs, []packetFlowTest{
					{
						name:     "localhost to nodePort is ignored",
						sourceIP: localhostIP,
						destIP:   localhostIP,
						destPort: 3001,
						output:   "",
					},
				})
			}

			// 除非 nodePortAddresses 排除了备用 IP，否则备用 IP 上的 NodePort 应可用。
			if tc.allowAltNodeIP {
				runPacketFlowTests(t, getLine(), ipt, testNodeIPs, []packetFlowTest{
					{
						name:     "external to nodePort on secondary IP",
						sourceIP: externalClientIP,
						destIP:   altNodeIP,
						destPort: 3001,
						output:   output,
						masq:     true,
					},
				})
			} else {
				runPacketFlowTests(t, getLine(), ipt, testNodeIPs, []packetFlowTest{
					{
						name:     "secondary nodeIP ignores NodePorts",
						sourceIP: externalClientIP,
						destIP:   altNodeIP,
						destPort: 3001,
						output:   "",
					},
				})
			}

			// 防火墙规则需手工检查：数据包模拟器不实现 conntrack 状态判断，
			// 因此无法通过 runPacketFlowTests 验证。
			var expected string
			if tc.expectFirewall {
				expected = "-A KUBE-FIREWALL -m comment --comment \"block incoming localnet connections\" -d 127.0.0.0/8 ! -s 127.0.0.0/8 -m conntrack ! --ctstate RELATED,ESTABLISHED,DNAT -j DROP\n"
			}
			assertIPTablesChainEqual(t, getLine(), utiliptables.TableFilter, kubeletFirewallChain, expected, fp.iptablesData.String())
		})
	}
}

// TestHealthCheckNodePort 验证健康检查 NodePort 服务生成专用的 ACCEPT 规则。
func TestHealthCheckNodePort(t *testing.T) {
	ipt := iptablestest.NewFake()
	fp := NewFakeProxier(ipt)
	fp.nodePortAddresses = proxyutil.NewNodePortAddresses(v1.IPv4Protocol, []string{"127.0.0.0/8"})

	svcIP := "172.30.0.42"
	svcPort := 80
	svcNodePort := 3001
	svcHealthCheckNodePort := 30000
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	svc := makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
		svc.Spec.Type = "LoadBalancer"
		svc.Spec.ClusterIP = svcIP
		svc.Spec.Ports = []v1.ServicePort{{
			Name:     svcPortName.Port,
			Port:     int32(svcPort),
			Protocol: v1.ProtocolTCP,
			NodePort: int32(svcNodePort),
		}}
		svc.Spec.HealthCheckNodePort = int32(svcHealthCheckNodePort)
		svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
	})
	makeServiceMap(fp, svc)
	fp.syncProxyRules()

	runPacketFlowTests(t, getLine(), ipt, testNodeIPs, []packetFlowTest{
		{
			name:     "firewall accepts HealthCheckNodePort",
			sourceIP: "1.2.3.4",
			destIP:   testNodeIP,
			destPort: svcHealthCheckNodePort,
			output:   "ACCEPT",
			masq:     false,
		},
	})

	fp.OnServiceDelete(svc)
	fp.syncProxyRules()

	runPacketFlowTests(t, getLine(), ipt, testNodeIPs, []packetFlowTest{
		{
			name:     "HealthCheckNodePort no longer has any rule",
			sourceIP: "1.2.3.4",
			destIP:   testNodeIP,
			destPort: svcHealthCheckNodePort,
			output:   "",
		},
	})
}

// TestDropInvalidRule 验证用于丢弃 INVALID 状态连接的规则按配置生成。
func TestDropInvalidRule(t *testing.T) {
	kubeForwardChainRules := dedent.Dedent(`
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
				-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
	`)

	testCases := []struct {
		nfacctEnsured  bool
		tcpLiberal     bool
		dropRule       string
		nfAcctCounters map[string]bool
	}{
		{
			nfacctEnsured:  false,
			tcpLiberal:     false,
			nfAcctCounters: map[string]bool{},
			dropRule:       "-A KUBE-FORWARD -m conntrack --ctstate INVALID -j DROP",
		},
		{
			nfacctEnsured: true,
			tcpLiberal:    false,
			nfAcctCounters: map[string]bool{
				metrics.IPTablesCTStateInvalidDroppedNFAcctCounter: true,
			},
			dropRule: fmt.Sprintf("-A KUBE-FORWARD -m conntrack --ctstate INVALID -m nfacct --nfacct-name %s -j DROP", metrics.IPTablesCTStateInvalidDroppedNFAcctCounter),
		},
		{
			nfacctEnsured:  false,
			tcpLiberal:     true,
			nfAcctCounters: map[string]bool{},
			dropRule:       "",
		},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("tcpLiberal is %t and nfacctEnsured is %t", tc.tcpLiberal, tc.nfacctEnsured), func(t *testing.T) {
			ipt := iptablestest.NewFake()
			fp := NewFakeProxier(ipt)
			fp.conntrackTCPLiberal = tc.tcpLiberal
			fp.nfAcctCounters = tc.nfAcctCounters
			fp.syncProxyRules()

			expected := tc.dropRule + kubeForwardChainRules
			assertIPTablesChainEqual(t, getLine(), utiliptables.TableFilter, kubeForwardChain, expected, fp.iptablesData.String())
		})
	}
}

// TestExternalTrafficPolicyLocal 验证 Local 策略下访问外部 IP 的流量不会被伪装。
// 对外部来源的流量，这意味着只能路由到本地端点；
// 对内部来源的流量，则仍可路由到所有端点。
// 组合外部/内部来源与本地/集群端点分布，逐场景模拟数据包验证路由和伪装行为。
func TestExternalTrafficPolicyLocal(t *testing.T) {
	ipt := iptablestest.NewFake()
	fp := NewFakeProxier(ipt)

	svcIP := "172.30.0.41"
	svcPort := 80
	svcNodePort := 3001
	svcHealthCheckNodePort := 30000
	svcExternalIPs := "192.168.99.11"
	svcLBIP := "1.2.3.4"
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
	}

	makeServiceMap(fp,
		// eTP:Local 要求节点上启用健康检查 NodePort。
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeLoadBalancer
			svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
			svc.Spec.ClusterIP = svcIP
			svc.Spec.ExternalIPs = []string{svcExternalIPs}
			svc.Spec.Ports = []v1.ServicePort{{
				Name:       svcPortName.Port,
				Port:       int32(svcPort),
				Protocol:   v1.ProtocolTCP,
				NodePort:   int32(svcNodePort),
				TargetPort: intstr.FromInt32(int32(svcPort)),
			}}
			svc.Spec.HealthCheckNodePort = int32(svcHealthCheckNodePort)
			svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
				IP: svcLBIP,
			}}
		}),
	)

	// 本地端点 epIP1 与远端端点 epIP2 并存，用于区分内外部来源的路由差异。
	epIP1 := "10.180.0.1"
	epIP2 := "10.180.2.1"
	// 两次同步之间修改端点分布，验证规则随端点状态变化而变化。
	populateEndpointSlices(fp,
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIP1},
			}, {
				Addresses: []string{epIP2},
				NodeName:  ptr.To(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	fp.syncProxyRules()

	runPacketFlowTests(t, getLine(), ipt, testNodeIPs, []packetFlowTest{
		{
			name:     "pod to cluster IP hits both endpoints, unmasqueraded",
			sourceIP: "10.0.0.2",
			destIP:   svcIP,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d, %s:%d", epIP1, svcPort, epIP2, svcPort),
			masq:     false,
		},
		{
			name:     "pod to external IP hits both endpoints, unmasqueraded",
			sourceIP: "10.0.0.2",
			destIP:   svcExternalIPs,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d, %s:%d", epIP1, svcPort, epIP2, svcPort),
			masq:     false,
		},
		{
			name:     "external to external IP hits only local endpoint, unmasqueraded",
			sourceIP: testExternalClient,
			destIP:   svcExternalIPs,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d", epIP2, svcPort),
			masq:     false,
		},
		{
			name:     "pod to LB IP hits only both endpoints, unmasqueraded",
			sourceIP: "10.0.0.2",
			destIP:   svcLBIP,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d, %s:%d", epIP1, svcPort, epIP2, svcPort),
			masq:     false,
		},
		{
			name:     "external to LB IP hits only local endpoint, unmasqueraded",
			sourceIP: testExternalClient,
			destIP:   svcLBIP,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d", epIP2, svcPort),
			masq:     false,
		},
		{
			name:     "pod to NodePort hits both endpoints, unmasqueraded",
			sourceIP: "10.0.0.2",
			destIP:   testNodeIP,
			destPort: svcNodePort,
			output:   fmt.Sprintf("%s:%d, %s:%d", epIP1, svcPort, epIP2, svcPort),
			masq:     false,
		},
		{
			name:     "external to NodePort hits only local endpoint, unmasqueraded",
			sourceIP: testExternalClient,
			destIP:   testNodeIP,
			destPort: svcNodePort,
			output:   fmt.Sprintf("%s:%d", epIP2, svcPort),
			masq:     false,
		},
	})
}

// TestExternalTrafficPolicyCluster 验证 Cluster 策略下访问外部 IP 的流量会被伪装。
// 用数据包模拟确认外部来源流量被伪装，且可按策略负载均衡到所有端点。
func TestExternalTrafficPolicyCluster(t *testing.T) {
	ipt := iptablestest.NewFake()
	// 与 Local 策略使用相同的服务拓扑，便于对比两种策略的伪装与路由差异。
	fp := NewFakeProxier(ipt)

	svcIP := "172.30.0.41"
	svcPort := 80
	svcNodePort := 3001
	svcExternalIPs := "192.168.99.11"
	svcLBIP := "1.2.3.4"
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
	}

	makeServiceMap(fp,
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeLoadBalancer
			svc.Spec.ClusterIP = svcIP
			svc.Spec.ExternalIPs = []string{svcExternalIPs}
			svc.Spec.Ports = []v1.ServicePort{{
				Name:       svcPortName.Port,
				Port:       int32(svcPort),
				Protocol:   v1.ProtocolTCP,
				NodePort:   int32(svcNodePort),
				TargetPort: intstr.FromInt32(int32(svcPort)),
			}}
			svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
				IP: svcLBIP,
			}}
			svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyCluster
		}),
	)
	// Cluster 策略下内外来源都可到达全部端点，但来自集群外的流量必须伪装。

	epIP1 := "10.180.0.1"
	epIP2 := "10.180.2.1"
	populateEndpointSlices(fp,
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIP1},
				NodeName:  nil,
			}, {
				Addresses: []string{epIP2},
				NodeName:  ptr.To(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	fp.syncProxyRules()

	runPacketFlowTests(t, getLine(), ipt, testNodeIPs, []packetFlowTest{
		{
			name:     "pod to cluster IP hits both endpoints, unmasqueraded",
			sourceIP: "10.0.0.2",
			destIP:   svcIP,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d, %s:%d", epIP1, svcPort, epIP2, svcPort),
			masq:     false,
		},
		{
			name:     "pod to external IP hits both endpoints, masqueraded",
			sourceIP: "10.0.0.2",
			destIP:   svcExternalIPs,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d, %s:%d", epIP1, svcPort, epIP2, svcPort),
			masq:     true,
		},
		{
			name:     "external to external IP hits both endpoints, masqueraded",
			sourceIP: testExternalClient,
			destIP:   svcExternalIPs,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d, %s:%d", epIP1, svcPort, epIP2, svcPort),
			masq:     true,
		},
		{
			name:     "pod to LB IP hits both endpoints, masqueraded",
			sourceIP: "10.0.0.2",
			destIP:   svcLBIP,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d, %s:%d", epIP1, svcPort, epIP2, svcPort),
			masq:     true,
		},
		{
			name:     "external to LB IP hits both endpoints, masqueraded",
			sourceIP: testExternalClient,
			destIP:   svcLBIP,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d, %s:%d", epIP1, svcPort, epIP2, svcPort),
			masq:     true,
		},
		{
			name:     "pod to NodePort hits both endpoints, masqueraded",
			sourceIP: "10.0.0.2",
			destIP:   testNodeIP,
			destPort: svcNodePort,
			output:   fmt.Sprintf("%s:%d, %s:%d", epIP1, svcPort, epIP2, svcPort),
			masq:     true,
		},
		{
			name:     "external to NodePort hits both endpoints, masqueraded",
			sourceIP: testExternalClient,
			destIP:   testNodeIP,
			destPort: svcNodePort,
			output:   fmt.Sprintf("%s:%d, %s:%d", epIP1, svcPort, epIP2, svcPort),
			masq:     true,
		},
	})
}

// TestComputeProbability 验证预计算的分流概率字符串在端点数量变化时保持正确。
func TestComputeProbability(t *testing.T) {
	expectedProbabilities := map[int]string{
		1:      "1.0000000000",
		2:      "0.5000000000",
		10:     "0.1000000000",
		100:    "0.0100000000",
		1000:   "0.0010000000",
		10000:  "0.0001000000",
		100000: "0.0000100000",
		100001: "0.0000099999",
	}

	for num, expected := range expectedProbabilities {
		actual := computeProbability(num)
		if actual != expected {
			t.Errorf("Expected computeProbability(%d) to be %s, got: %s", num, expected, actual)
		}
	}

	prevProbability := float64(0)
	for i := 100000; i > 1; i-- {
		currProbability, err := strconv.ParseFloat(computeProbability(i), 64)
		if err != nil {
			t.Fatalf("Error parsing float probability for %d: %v", i, err)
		}
		if currProbability <= prevProbability {
			t.Fatalf("Probability unexpectedly <= to previous probability for %d: (%0.10f <= %0.10f)", i, currProbability, prevProbability)
		}
		prevProbability = currProbability
	}
}

// makeTestService 构造用于测试的 Service，允许通过 svcFunc 按需修改字段。
func makeTestService(namespace, name string, svcFunc func(*v1.Service)) *v1.Service {
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: map[string]string{},
		},
		Spec:   v1.ServiceSpec{},
		Status: v1.ServiceStatus{},
	}
	svcFunc(svc)
	return svc
}

// addTestPort 向端口切片追加一个指定名称、协议、端口和 NodePort 的测试端口。
func addTestPort(array []v1.ServicePort, name string, protocol v1.Protocol, port, nodeport int32, targetPort int) []v1.ServicePort {
	svcPort := v1.ServicePort{
		Name:       name,
		Protocol:   protocol,
		Port:       port,
		NodePort:   nodeport,
		TargetPort: intstr.FromInt32(int32(targetPort)),
	}
	return append(array, svcPort)
}

// TestBuildServiceMapAddRemove 验证 Service 变更跟踪器能正确处理服务的添加与删除。
func TestBuildServiceMapAddRemove(t *testing.T) {
	ipt := iptablestest.NewFake()
	fp := NewFakeProxier(ipt)

	services := []*v1.Service{
		makeTestService("somewhere-else", "cluster-ip", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = "172.30.55.4"
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "something", "UDP", 1234, 4321, 0)
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "somethingelse", "UDP", 1235, 5321, 0)
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "sctpport", "SCTP", 1236, 6321, 0)
		}),
		makeTestService("somewhere-else", "node-port", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeNodePort
			svc.Spec.ClusterIP = "172.30.55.10"
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "blahblah", "UDP", 345, 678, 0)
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "moreblahblah", "TCP", 344, 677, 0)
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "muchmoreblah", "SCTP", 343, 676, 0)
		}),
		makeTestService("somewhere", "load-balancer", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeLoadBalancer
			svc.Spec.ClusterIP = "172.30.55.11"
			svc.Spec.LoadBalancerIP = "1.2.3.4"
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "foobar", "UDP", 8675, 30061, 7000)
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "baz", "UDP", 8676, 30062, 7001)
			svc.Status.LoadBalancer = v1.LoadBalancerStatus{
				Ingress: []v1.LoadBalancerIngress{
					{IP: "1.2.3.4"},
				},
			}
		}),
		makeTestService("somewhere", "only-local-load-balancer", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeLoadBalancer
			svc.Spec.ClusterIP = "172.30.55.12"
			svc.Spec.LoadBalancerIP = "5.6.7.8"
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "foobar2", "UDP", 8677, 30063, 7002)
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "baz", "UDP", 8678, 30064, 7003)
			svc.Status.LoadBalancer = v1.LoadBalancerStatus{
				Ingress: []v1.LoadBalancerIngress{
					{IP: "5.6.7.8"},
				},
			}
			svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
			svc.Spec.HealthCheckNodePort = 345
		}),
	}

	for i := range services {
		fp.OnServiceAdd(services[i])
	}
	fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 10 {
		t.Errorf("expected service map length 10, got %v", fp.svcPortMap)
	}

	// 只有 loadBalancerSourceRanges 为 only-local 的服务会生成健康检查端口。
	healthCheckNodePorts := fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 1 {
		t.Errorf("expected 1 healthcheck port, got %v", healthCheckNodePorts)
	} else {
		nsn := makeNSN("somewhere", "only-local-load-balancer")
		if port, found := healthCheckNodePorts[nsn]; !found || port != 345 {
			t.Errorf("expected healthcheck port [%q]=345: got %v", nsn, healthCheckNodePorts)
		}
	}

	// oneService 由 services[0] 删除首个端口后构造，用于测试端口删除。
	oneService := makeTestService("somewhere-else", "cluster-ip", func(svc *v1.Service) {
		svc.Spec.Type = v1.ServiceTypeClusterIP
		svc.Spec.ClusterIP = "172.30.55.4"
		svc.Spec.Ports = addTestPort(svc.Spec.Ports, "somethingelse", "UDP", 1235, 5321, 0)
	})

	fp.OnServiceUpdate(services[0], oneService)
	fp.OnServiceDelete(services[1])
	fp.OnServiceDelete(services[2])
	fp.OnServiceDelete(services[3])

	fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 1 {
		t.Errorf("expected service map length 1, got %v", fp.svcPortMap)
	}

	healthCheckNodePorts = fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 0 {
		t.Errorf("expected 0 healthcheck ports, got %v", healthCheckNodePorts)
	}
}

// TestBuildServiceMapServiceHeadless 验证 Headless Service 不会进入代理的 ServiceMap。
func TestBuildServiceMapServiceHeadless(t *testing.T) {
	ipt := iptablestest.NewFake()
	fp := NewFakeProxier(ipt)

	makeServiceMap(fp,
		makeTestService("somewhere-else", "headless", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = v1.ClusterIPNone
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "rpc", "UDP", 1234, 0, 0)
		}),
		makeTestService("somewhere-else", "headless-without-port", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = v1.ClusterIPNone
		}),
	)

	// Headless Service 没有 ClusterIP，应被忽略。
	fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 0 {
		t.Errorf("expected service map length 0, got %d", len(fp.svcPortMap))
	}

	// 没有代理 Service，因此无需健康检查端口。
	healthCheckNodePorts := fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 0 {
		t.Errorf("expected healthcheck ports length 0, got %d", len(healthCheckNodePorts))
	}
}

// TestBuildServiceMapServiceTypeExternalName 验证 ExternalName 类型的 Service 被忽略。
func TestBuildServiceMapServiceTypeExternalName(t *testing.T) {
	ipt := iptablestest.NewFake()
	fp := NewFakeProxier(ipt)

	makeServiceMap(fp,
		makeTestService("somewhere-else", "external-name", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeExternalName
			svc.Spec.ClusterIP = "172.30.55.4" // 应被忽略
			svc.Spec.ExternalName = "foo2.bar.com"
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "blah", "UDP", 1235, 5321, 0)
		}),
	)

	fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 0 {
		t.Errorf("expected service map length 0, got %v", fp.svcPortMap)
	}
	// 没有代理 Service，因此无需健康检查端口。
	healthCheckNodePorts := fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 0 {
		t.Errorf("expected healthcheck ports length 0, got %v", healthCheckNodePorts)
	}
}

// TestBuildServiceMapServiceUpdate 验证服务更新后 ServiceMap 与健康检查服务的状态正确。
func TestBuildServiceMapServiceUpdate(t *testing.T) {
	ipt := iptablestest.NewFake()
	fp := NewFakeProxier(ipt)

	servicev1 := makeTestService("somewhere", "some-service", func(svc *v1.Service) {
		svc.Spec.Type = v1.ServiceTypeClusterIP
		svc.Spec.ClusterIP = "172.30.55.4"
		svc.Spec.Ports = addTestPort(svc.Spec.Ports, "something", "UDP", 1234, 4321, 0)
		svc.Spec.Ports = addTestPort(svc.Spec.Ports, "somethingelse", "TCP", 1235, 5321, 0)
	})
	servicev2 := makeTestService("somewhere", "some-service", func(svc *v1.Service) {
		svc.Spec.Type = v1.ServiceTypeLoadBalancer
		svc.Spec.ClusterIP = "172.30.55.4"
		svc.Spec.LoadBalancerIP = "1.2.3.4"
		svc.Spec.Ports = addTestPort(svc.Spec.Ports, "something", "UDP", 1234, 4321, 7002)
		svc.Spec.Ports = addTestPort(svc.Spec.Ports, "somethingelse", "TCP", 1235, 5321, 7003)
		svc.Status.LoadBalancer = v1.LoadBalancerStatus{
			Ingress: []v1.LoadBalancerIngress{
				{IP: "1.2.3.4"},
			},
		}
		svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
		svc.Spec.HealthCheckNodePort = 345
	})

	fp.OnServiceAdd(servicev1)

	fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 2 {
		t.Errorf("expected service map length 2, got %v", fp.svcPortMap)
	}
	healthCheckNodePorts := fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 0 {
		t.Errorf("expected healthcheck ports length 0, got %v", healthCheckNodePorts)
	}

	// 将 Service 改为 LoadBalancer 类型。
	fp.OnServiceUpdate(servicev1, servicev2)
	fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 2 {
		t.Errorf("expected service map length 2, got %v", fp.svcPortMap)
	}
	healthCheckNodePorts = fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 1 {
		t.Errorf("expected healthcheck ports length 1, got %v", healthCheckNodePorts)
	}

	// 无变更：确认 ServiceMap 与健康检查端口都不发生改变。
	// 这是防止 Update 误清理状态的回归检查。
	fp.OnServiceUpdate(servicev2, servicev2)
	fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 2 {
		t.Errorf("expected service map length 2, got %v", fp.svcPortMap)
	}
	healthCheckNodePorts = fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 1 {
		t.Errorf("expected healthcheck ports length 1, got %v", healthCheckNodePorts)
	}

	// 再将 Service 改回 ClusterIP。
	fp.OnServiceUpdate(servicev2, servicev1)
	fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 2 {
		t.Errorf("expected service map length 2, got %v", fp.svcPortMap)
	}
	healthCheckNodePorts = fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 0 {
		t.Errorf("expected healthcheck ports length 0, got %v", healthCheckNodePorts)
	}
}

// populateEndpointSlices 将给定的 EndpointSlice 批量写入 proxier 的端点变更跟踪器。
func populateEndpointSlices(proxier *Proxier, allEndpointSlices ...*discovery.EndpointSlice) {
	for i := range allEndpointSlices {
		proxier.OnEndpointSliceAdd(allEndpointSlices[i])
	}
}

// makeTestEndpointSlice 构造测试用 EndpointSlice，sliceNum 用于生成稳定的名称。
func makeTestEndpointSlice(namespace, name string, sliceNum int, epsFunc func(*discovery.EndpointSlice)) *discovery.EndpointSlice {
	eps := &discovery.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", name, sliceNum),
			Namespace: namespace,
			Labels:    map[string]string{discovery.LabelServiceName: name},
		},
	}
	epsFunc(eps)
	return eps
}

// makeNSN 构造 namespace/name 形式的 NamespacedName，便于编写期望值。
func makeNSN(namespace, name string) types.NamespacedName {
	return types.NamespacedName{Namespace: namespace, Name: name}
}

// makeServicePortName 构造 ServicePortName，供构造期望的端点映射使用。
func makeServicePortName(ns, name, port string, protocol v1.Protocol) proxy.ServicePortName {
	return proxy.ServicePortName{
		NamespacedName: makeNSN(ns, name),
		Port:           port,
		Protocol:       protocol,
	}
}

// makeServiceMap 将一组 Service 写入 proxier 的服务变更跟踪器并触发同步。
func makeServiceMap(proxier *Proxier, allServices ...*v1.Service) {
	for i := range allServices {
		proxier.OnServiceAdd(allServices[i])
	}

	proxier.mu.Lock()
	defer proxier.mu.Unlock()
	proxier.servicesSynced = true
}

type endpointExpectation struct {
	endpoint string
	isLocal  bool
}

// checkEndpointExpectations 逐个端口核对实际端点与期望端点是否一致。
func checkEndpointExpectations(t *testing.T, tci int, newMap proxy.EndpointsMap, expected map[proxy.ServicePortName][]endpointExpectation) {
	if len(newMap) != len(expected) {
		t.Errorf("[%d] expected %d results, got %d: %v", tci, len(expected), len(newMap), newMap)
	}
	for x := range expected {
		if len(newMap[x]) != len(expected[x]) {
			t.Errorf("[%d] expected %d endpoints for %v, got %d", tci, len(expected[x]), x, len(newMap[x]))
		} else {
			for i := range expected[x] {
				newEp := newMap[x][i]
				if newEp.String() != expected[x][i].endpoint ||
					newEp.IsLocal() != expected[x][i].isLocal {
					t.Errorf("[%d] expected new[%v][%d] to be %v, got %v", tci, x, i, expected[x][i], newEp)
				}
			}
		}
	}
}

// TestUpdateEndpointsMap 验证端点变更跟踪器在 EndpointSlice 增删、端口变化等场景下的行为。
func TestUpdateEndpointsMap(t *testing.T) {
	emptyEndpointSlices := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1, func(*discovery.EndpointSlice) {}),
	}
	subset1 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.1.1.1"},
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p11"),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	subset2 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.1.1.2"},
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p12"),
			Port:     ptr.To[int32](12),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	namedPortLocal := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1,
			func(eps *discovery.EndpointSlice) {
				eps.AddressType = discovery.AddressTypeIPv4
				eps.Endpoints = []discovery.Endpoint{{
					Addresses: []string{"10.1.1.1"},
					NodeName:  ptr.To(testNodeName),
				}}
				eps.Ports = []discovery.EndpointPort{{
					Name:     ptr.To("p11"),
					Port:     ptr.To[int32](11),
					Protocol: ptr.To(v1.ProtocolUDP),
				}}
			}),
	}
	namedPort := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1, subset1),
	}
	namedPortRenamed := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1,
			func(eps *discovery.EndpointSlice) {
				eps.AddressType = discovery.AddressTypeIPv4
				eps.Endpoints = []discovery.Endpoint{{
					Addresses: []string{"10.1.1.1"},
				}}
				eps.Ports = []discovery.EndpointPort{{
					Name:     ptr.To("p11-2"),
					Port:     ptr.To[int32](11),
					Protocol: ptr.To(v1.ProtocolUDP),
				}}
			}),
	}
	namedPortRenumbered := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1,
			func(eps *discovery.EndpointSlice) {
				eps.AddressType = discovery.AddressTypeIPv4
				eps.Endpoints = []discovery.Endpoint{{
					Addresses: []string{"10.1.1.1"},
				}}
				eps.Ports = []discovery.EndpointPort{{
					Name:     ptr.To("p11"),
					Port:     ptr.To[int32](22),
					Protocol: ptr.To(v1.ProtocolUDP),
				}}
			}),
	}
	namedPortsLocalNoLocal := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1,
			func(eps *discovery.EndpointSlice) {
				eps.AddressType = discovery.AddressTypeIPv4
				eps.Endpoints = []discovery.Endpoint{{
					Addresses: []string{"10.1.1.1"},
				}, {
					Addresses: []string{"10.1.1.2"},
					NodeName:  ptr.To(testNodeName),
				}}
				eps.Ports = []discovery.EndpointPort{{
					Name:     ptr.To("p11"),
					Port:     ptr.To[int32](11),
					Protocol: ptr.To(v1.ProtocolUDP),
				}, {
					Name:     ptr.To("p12"),
					Port:     ptr.To[int32](12),
					Protocol: ptr.To(v1.ProtocolUDP),
				}}
			}),
	}
	multipleSubsets := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1, subset1),
		makeTestEndpointSlice("ns1", "ep1", 2, subset2),
	}
	subsetLocal := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.1.1.2"},
			NodeName:  ptr.To(testNodeName),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p12"),
			Port:     ptr.To[int32](12),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	multipleSubsetsWithLocal := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1, subset1),
		makeTestEndpointSlice("ns1", "ep1", 2, subsetLocal),
	}
	subsetMultiplePortsLocal := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.1.1.1"},
			NodeName:  ptr.To(testNodeName),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p11"),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}, {
			Name:     ptr.To("p12"),
			Port:     ptr.To[int32](12),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	subset3 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.1.1.3"},
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p13"),
			Port:     ptr.To[int32](13),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	multipleSubsetsMultiplePortsLocal := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1, subsetMultiplePortsLocal),
		makeTestEndpointSlice("ns1", "ep1", 2, subset3),
	}
	subsetMultipleIPsPorts1 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.1.1.1"},
		}, {
			Addresses: []string{"10.1.1.2"},
			NodeName:  ptr.To(testNodeName),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p11"),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}, {
			Name:     ptr.To("p12"),
			Port:     ptr.To[int32](12),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	subsetMultipleIPsPorts2 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.1.1.3"},
		}, {
			Addresses: []string{"10.1.1.4"},
			NodeName:  ptr.To(testNodeName),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p13"),
			Port:     ptr.To[int32](13),
			Protocol: ptr.To(v1.ProtocolUDP),
		}, {
			Name:     ptr.To("p14"),
			Port:     ptr.To[int32](14),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	subsetMultipleIPsPorts3 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.2.2.1"},
		}, {
			Addresses: []string{"10.2.2.2"},
			NodeName:  ptr.To(testNodeName),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p21"),
			Port:     ptr.To[int32](21),
			Protocol: ptr.To(v1.ProtocolUDP),
		}, {
			Name:     ptr.To("p22"),
			Port:     ptr.To[int32](22),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	multipleSubsetsIPsPorts := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1, subsetMultipleIPsPorts1),
		makeTestEndpointSlice("ns1", "ep1", 2, subsetMultipleIPsPorts2),
		makeTestEndpointSlice("ns2", "ep2", 1, subsetMultipleIPsPorts3),
	}
	complexSubset1 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.2.2.2"},
			NodeName:  ptr.To(testNodeName),
		}, {
			Addresses: []string{"10.2.2.22"},
			NodeName:  ptr.To(testNodeName),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p22"),
			Port:     ptr.To[int32](22),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexSubset2 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.2.2.3"},
			NodeName:  ptr.To(testNodeName),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p23"),
			Port:     ptr.To[int32](23),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexSubset3 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.4.4.4"},
			NodeName:  ptr.To(testNodeName),
		}, {
			Addresses: []string{"10.4.4.5"},
			NodeName:  ptr.To(testNodeName),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p44"),
			Port:     ptr.To[int32](44),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexSubset4 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.4.4.6"},
			NodeName:  ptr.To(testNodeName),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p45"),
			Port:     ptr.To[int32](45),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexSubset5 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.1.1.1"},
		}, {
			Addresses: []string{"10.1.1.11"},
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p11"),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexSubset6 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.1.1.2"},
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p12"),
			Port:     ptr.To[int32](12),
			Protocol: ptr.To(v1.ProtocolUDP),
		}, {
			Name:     ptr.To("p122"),
			Port:     ptr.To[int32](122),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexSubset7 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.3.3.3"},
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p33"),
			Port:     ptr.To[int32](33),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexSubset8 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.4.4.4"},
			NodeName:  ptr.To(testNodeName),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p44"),
			Port:     ptr.To[int32](44),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexBefore := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1, subset1),
		nil,
		makeTestEndpointSlice("ns2", "ep2", 1, complexSubset1),
		makeTestEndpointSlice("ns2", "ep2", 2, complexSubset2),
		nil,
		makeTestEndpointSlice("ns4", "ep4", 1, complexSubset3),
		makeTestEndpointSlice("ns4", "ep4", 2, complexSubset4),
	}
	complexAfter := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1, complexSubset5),
		makeTestEndpointSlice("ns1", "ep1", 2, complexSubset6),
		nil,
		nil,
		makeTestEndpointSlice("ns3", "ep3", 1, complexSubset7),
		makeTestEndpointSlice("ns4", "ep4", 1, complexSubset8),
		nil,
	}

	testCases := []struct {
		// previousEndpoints 与 currentEndpoints 决定调用哪个 OnEndpoints* 回调
		// （依据对应值为 nil 与否），两者长度必须相同。
		name                             string
		previousEndpoints                []*discovery.EndpointSlice
		currentEndpoints                 []*discovery.EndpointSlice
		oldEndpoints                     map[proxy.ServicePortName][]endpointExpectation
		expectedResult                   map[proxy.ServicePortName][]endpointExpectation
		expectedConntrackCleanupRequired bool
		expectedLocalEndpoints           map[types.NamespacedName]int
	}{{
		// Case[0]：无任何变更。
		name:                             "nothing",
		oldEndpoints:                     map[proxy.ServicePortName][]endpointExpectation{},
		expectedResult:                   map[proxy.ServicePortName][]endpointExpectation{},
		expectedConntrackCleanupRequired: false,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
	}, {
		// Case[1]：无变更，命名端口，本地端点。
		name:              "no change, named port, local",
		previousEndpoints: namedPortLocal,
		currentEndpoints:  namedPortLocal,
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: true},
			},
		},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: true},
			},
		},
		expectedConntrackCleanupRequired: false,
		expectedLocalEndpoints: map[types.NamespacedName]int{
			makeNSN("ns1", "ep1"): 1,
		},
	}, {
		// Case[2]：无变更，多个 subset。
		name:              "no change, multiple subsets",
		previousEndpoints: multipleSubsets,
		currentEndpoints:  multipleSubsets,
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{endpoint: "10.1.1.2:12", isLocal: false},
			},
		},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{endpoint: "10.1.1.2:12", isLocal: false},
			},
		},
		expectedConntrackCleanupRequired: false,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
	}, {
		// Case[3]：无变更，多个 subset、多个端口、本地端点。
		name:              "no change, multiple subsets, multiple ports, local",
		previousEndpoints: multipleSubsetsMultiplePortsLocal,
		currentEndpoints:  multipleSubsetsMultiplePortsLocal,
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:12", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p13", v1.ProtocolUDP): {
				{endpoint: "10.1.1.3:13", isLocal: false},
			},
		},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:12", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p13", v1.ProtocolUDP): {
				{endpoint: "10.1.1.3:13", isLocal: false},
			},
		},
		expectedConntrackCleanupRequired: false,
		expectedLocalEndpoints: map[types.NamespacedName]int{
			makeNSN("ns1", "ep1"): 1,
		},
	}, {
		// Case[4]：无变更，多个端点、subset、IP 和端口。
		name:              "no change, multiple endpoints, subsets, IPs, and ports",
		previousEndpoints: multipleSubsetsIPsPorts,
		currentEndpoints:  multipleSubsetsIPsPorts,
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
				{endpoint: "10.1.1.2:11", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:12", isLocal: false},
				{endpoint: "10.1.1.2:12", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p13", v1.ProtocolUDP): {
				{endpoint: "10.1.1.3:13", isLocal: false},
				{endpoint: "10.1.1.4:13", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p14", v1.ProtocolUDP): {
				{endpoint: "10.1.1.3:14", isLocal: false},
				{endpoint: "10.1.1.4:14", isLocal: true},
			},
			makeServicePortName("ns2", "ep2", "p21", v1.ProtocolUDP): {
				{endpoint: "10.2.2.1:21", isLocal: false},
				{endpoint: "10.2.2.2:21", isLocal: true},
			},
			makeServicePortName("ns2", "ep2", "p22", v1.ProtocolUDP): {
				{endpoint: "10.2.2.1:22", isLocal: false},
				{endpoint: "10.2.2.2:22", isLocal: true},
			},
		},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
				{endpoint: "10.1.1.2:11", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:12", isLocal: false},
				{endpoint: "10.1.1.2:12", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p13", v1.ProtocolUDP): {
				{endpoint: "10.1.1.3:13", isLocal: false},
				{endpoint: "10.1.1.4:13", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p14", v1.ProtocolUDP): {
				{endpoint: "10.1.1.3:14", isLocal: false},
				{endpoint: "10.1.1.4:14", isLocal: true},
			},
			makeServicePortName("ns2", "ep2", "p21", v1.ProtocolUDP): {
				{endpoint: "10.2.2.1:21", isLocal: false},
				{endpoint: "10.2.2.2:21", isLocal: true},
			},
			makeServicePortName("ns2", "ep2", "p22", v1.ProtocolUDP): {
				{endpoint: "10.2.2.1:22", isLocal: false},
				{endpoint: "10.2.2.2:22", isLocal: true},
			},
		},
		expectedConntrackCleanupRequired: false,
		expectedLocalEndpoints: map[types.NamespacedName]int{
			makeNSN("ns1", "ep1"): 2,
			makeNSN("ns2", "ep2"): 1,
		},
	}, {
		// Case[5]：新增一个 Endpoints。
		name:              "add an Endpoints",
		previousEndpoints: []*discovery.EndpointSlice{nil},
		currentEndpoints:  namedPortLocal,
		oldEndpoints:      map[proxy.ServicePortName][]endpointExpectation{},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: true},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints: map[types.NamespacedName]int{
			makeNSN("ns1", "ep1"): 1,
		},
	}, {
		// Case[6]：删除一个 Endpoints。
		name:              "remove an Endpoints",
		previousEndpoints: namedPortLocal,
		currentEndpoints:  []*discovery.EndpointSlice{nil},
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: true},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedResult:                   map[proxy.ServicePortName][]endpointExpectation{},
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
	}, {
		// Case[7]：增加一个 IP 和端口。
		name:              "add an IP and port",
		previousEndpoints: namedPort,
		currentEndpoints:  namedPortsLocalNoLocal,
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
		},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
				{endpoint: "10.1.1.2:11", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:12", isLocal: false},
				{endpoint: "10.1.1.2:12", isLocal: true},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints: map[types.NamespacedName]int{
			makeNSN("ns1", "ep1"): 1,
		},
	}, {
		// Case[8]：删除一个 IP 和端口。
		name:              "remove an IP and port",
		previousEndpoints: namedPortsLocalNoLocal,
		currentEndpoints:  namedPort,
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
				{endpoint: "10.1.1.2:11", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:12", isLocal: false},
				{endpoint: "10.1.1.2:12", isLocal: true},
			},
		},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
	}, {
		// Case[9]：增加一个 subset。
		name:              "add a subset",
		previousEndpoints: []*discovery.EndpointSlice{namedPort[0], nil},
		currentEndpoints:  multipleSubsetsWithLocal,
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
		},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{endpoint: "10.1.1.2:12", isLocal: true},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints: map[types.NamespacedName]int{
			makeNSN("ns1", "ep1"): 1,
		},
	}, {
		// Case[10]：删除一个 subset。
		name:              "remove a subset",
		previousEndpoints: multipleSubsets,
		currentEndpoints:  []*discovery.EndpointSlice{namedPort[0], nil},
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{endpoint: "10.1.1.2:12", isLocal: false},
			},
		},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
	}, {
		// Case[11]：重命名端口。
		name:              "rename a port",
		previousEndpoints: namedPort,
		currentEndpoints:  namedPortRenamed,
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
		},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11-2", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
	}, {
		// Case[12]：修改端口编号。
		name:              "renumber a port",
		previousEndpoints: namedPort,
		currentEndpoints:  namedPortRenumbered,
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
		},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:22", isLocal: false},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
	}, {
		// Case[13]：复杂的增删混合场景。
		name:              "complex add and remove",
		previousEndpoints: complexBefore,
		currentEndpoints:  complexAfter,
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
			makeServicePortName("ns2", "ep2", "p22", v1.ProtocolUDP): {
				{endpoint: "10.2.2.22:22", isLocal: true},
				{endpoint: "10.2.2.2:22", isLocal: true},
			},
			makeServicePortName("ns2", "ep2", "p23", v1.ProtocolUDP): {
				{endpoint: "10.2.2.3:23", isLocal: true},
			},
			makeServicePortName("ns4", "ep4", "p44", v1.ProtocolUDP): {
				{endpoint: "10.4.4.4:44", isLocal: true},
				{endpoint: "10.4.4.5:44", isLocal: true},
			},
			makeServicePortName("ns4", "ep4", "p45", v1.ProtocolUDP): {
				{endpoint: "10.4.4.6:45", isLocal: true},
			},
		},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.11:11", isLocal: false},
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{endpoint: "10.1.1.2:12", isLocal: false},
			},
			makeServicePortName("ns1", "ep1", "p122", v1.ProtocolUDP): {
				{endpoint: "10.1.1.2:122", isLocal: false},
			},
			makeServicePortName("ns3", "ep3", "p33", v1.ProtocolUDP): {
				{endpoint: "10.3.3.3:33", isLocal: false},
			},
			makeServicePortName("ns4", "ep4", "p44", v1.ProtocolUDP): {
				{endpoint: "10.4.4.4:44", isLocal: true},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints: map[types.NamespacedName]int{
			makeNSN("ns4", "ep4"): 1,
		},
	}, {
		// Case[14]：从 0 个端点地址变为 1 个未命名端口。
		name:              "change from 0 endpoint address to 1 unnamed port",
		previousEndpoints: emptyEndpointSlices,
		currentEndpoints:  namedPort,
		oldEndpoints:      map[proxy.ServicePortName][]endpointExpectation{},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
	},
	}

	for tci, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ipt := iptablestest.NewFake()
			fp := NewFakeProxier(ipt)

			// 先逐个应用历史版本的端点，检查 oldEndpoints 的状态
			// 是否与预期一致。
			for i := range tc.previousEndpoints {
				if tc.previousEndpoints[i] != nil {
					fp.OnEndpointSliceAdd(tc.previousEndpoints[i])
				}
			}
			fp.endpointsMap.Update(fp.endpointsChanges)
			checkEndpointExpectations(t, tci, fp.endpointsMap, tc.oldEndpoints)

			// 再调用相应的回调，把状态推进到目标场景。
			if len(tc.previousEndpoints) != len(tc.currentEndpoints) {
				t.Fatalf("[%d] different lengths of previous and current endpoints", tci)
			}

			for i := range tc.previousEndpoints {
				prev, curr := tc.previousEndpoints[i], tc.currentEndpoints[i]
				switch {
				case prev == nil:
					fp.OnEndpointSliceAdd(curr)
				case curr == nil:
					fp.OnEndpointSliceDelete(prev)
				default:
					fp.OnEndpointSliceUpdate(prev, curr)
				}
			}
			result := fp.endpointsMap.Update(fp.endpointsChanges)
			newMap := fp.endpointsMap
			checkEndpointExpectations(t, tci, newMap, tc.expectedResult)
			if result.ConntrackCleanupRequired != tc.expectedConntrackCleanupRequired {
				t.Errorf("[%d] expected conntrackCleanupRequired to be %t, got %t", tci, tc.expectedConntrackCleanupRequired, result.ConntrackCleanupRequired)
			}
			localReadyEndpoints := fp.endpointsMap.LocalReadyEndpoints()
			if !reflect.DeepEqual(localReadyEndpoints, tc.expectedLocalEndpoints) {
				t.Errorf("[%d] expected local endpoints %v, got %v", tci, tc.expectedLocalEndpoints, localReadyEndpoints)
			}
		})
	}
}

// TestHealthCheckNodePortWhenTerminating 验证所有本地端点都在终止时不会启用健康检查 NodePort。
// 验证 terminating 端点不会触发健康检查端口的创建。
func TestHealthCheckNodePortWhenTerminating(t *testing.T) {
	// 同一 Service 的本地端点先处于正常状态，再全部切换为 terminating。
	ipt := iptablestest.NewFake()
	fp := NewFakeProxier(ipt)
	fp.OnServiceSynced()
	fp.OnEndpointSlicesSynced()

	serviceName := "svc1"
	namespaceName := "ns1"

	// 逐个构建 EndpointSlice，避免手工维护冗长的端点对象字面量。
	fp.OnServiceAdd(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: serviceName, Namespace: namespaceName},
		Spec: v1.ServiceSpec{
			ClusterIP: "172.30.1.1",
			Selector:  map[string]string{"foo": "bar"},
			Ports:     []v1.ServicePort{{Name: "", TargetPort: intstr.FromInt32(80), Protocol: v1.ProtocolTCP}},
		},
	})

	endpointSlice := &discovery.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-1", serviceName),
			Namespace: namespaceName,
			Labels:    map[string]string{discovery.LabelServiceName: serviceName},
		},
		Ports: []discovery.EndpointPort{{
			Name:     ptr.To(""),
			Port:     ptr.To[int32](80),
			Protocol: ptr.To(v1.ProtocolTCP),
		}},
		AddressType: discovery.AddressTypeIPv4,
		Endpoints: []discovery.Endpoint{{
			Addresses:  []string{"10.0.1.1"},
			Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			NodeName:   ptr.To(testNodeName),
		}, {
			Addresses:  []string{"10.0.1.2"},
			Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			NodeName:   ptr.To(testNodeName),
		}, {
			Addresses:  []string{"10.0.1.3"},
			Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			NodeName:   ptr.To(testNodeName),
		}, { // 未就绪端点应被忽略
			Addresses:  []string{"10.0.1.4"},
			Conditions: discovery.EndpointConditions{Ready: ptr.To(false)},
			NodeName:   ptr.To(testNodeName),
		}},
	}

	fp.OnEndpointSliceAdd(endpointSlice)
	_ = fp.endpointsMap.Update(fp.endpointsChanges)
	localReadyEndpoints := fp.endpointsMap.LocalReadyEndpoints()
	if len(localReadyEndpoints) != 1 {
		t.Errorf("unexpected number of local ready endpoints, expected 1 but got: %d", len(localReadyEndpoints))
	}

	// 将所有端点设置为 terminating。
	endpointSliceTerminating := &discovery.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-1", serviceName),
			Namespace: namespaceName,
			Labels:    map[string]string{discovery.LabelServiceName: serviceName},
		},
		Ports: []discovery.EndpointPort{{
			Name:     ptr.To(""),
			Port:     ptr.To[int32](80),
			Protocol: ptr.To(v1.ProtocolTCP),
		}},
		AddressType: discovery.AddressTypeIPv4,
		Endpoints: []discovery.Endpoint{{
			Addresses: []string{"10.0.1.1"},
			Conditions: discovery.EndpointConditions{
				Ready:       ptr.To(false),
				Serving:     ptr.To(true),
				Terminating: ptr.To(false),
			},
			NodeName: ptr.To(testNodeName),
		}, {
			Addresses: []string{"10.0.1.2"},
			Conditions: discovery.EndpointConditions{
				Ready:       ptr.To(false),
				Serving:     ptr.To(true),
				Terminating: ptr.To(true),
			},
			NodeName: ptr.To(testNodeName),
		}, {
			Addresses: []string{"10.0.1.3"},
			Conditions: discovery.EndpointConditions{
				Ready:       ptr.To(false),
				Serving:     ptr.To(true),
				Terminating: ptr.To(true),
			},
			NodeName: ptr.To(testNodeName),
		}, { // 未就绪端点应被忽略
			Addresses: []string{"10.0.1.4"},
			Conditions: discovery.EndpointConditions{
				Ready:       ptr.To(false),
				Serving:     ptr.To(false),
				Terminating: ptr.To(true),
			},
			NodeName: ptr.To(testNodeName),
		}},
	}

	// 全部本地端点都在终止时，应视作没有可用本地端点。
	fp.OnEndpointSliceUpdate(endpointSlice, endpointSliceTerminating)
	_ = fp.endpointsMap.Update(fp.endpointsChanges)
	// 同时断言 LocalReadyEndpoints 为空，确保健康检查端口不会被启用。
	localReadyEndpoints = fp.endpointsMap.LocalReadyEndpoints()
	if len(localReadyEndpoints) != 0 {
		t.Errorf("unexpected number of local ready endpoints, expected 0 but got: %d", len(localReadyEndpoints))
	}
}

// TestProxierMetricsIPTablesTotalRules 验证规则总数指标与实际生成规则数一致。
func TestProxierMetricsIPTablesTotalRules(t *testing.T) {
	// 对比解析出的规则数与指标值，确认计数器在一次同步后保持准确。
	logger, _ := klogtesting.NewTestContext(t)
	ipt := iptablestest.NewFake()
	fp := NewFakeProxier(ipt)

	metrics.RegisterMetrics(kubeproxyconfig.ProxyModeIPTables)

	svcIP := "172.30.0.41"
	svcPort := 80
	nodePort := 31201
	svcPortName := proxy.ServicePortName{
		// 两次同步但规则数不变，验证指标是同步后的快照而非累加值。
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	makeServiceMap(fp,
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.ClusterIP = svcIP
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName.Port,
				Port:     int32(svcPort),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(nodePort),
			}}
		}),
	)
	fp.syncProxyRules()
	iptablesData := fp.iptablesData.String()

	nFilterRules := countRulesFromMetric(logger, utiliptables.TableFilter, string(fp.ipFamily))
	expectedFilterRules := countRules(logger, utiliptables.TableFilter, iptablesData)

	if nFilterRules != expectedFilterRules {
		t.Fatalf("Wrong number of filter rule: expected %d got %d\n%s", expectedFilterRules, nFilterRules, iptablesData)
	}

	nNatRules := countRulesFromMetric(logger, utiliptables.TableNAT, string(fp.ipFamily))
	expectedNatRules := countRules(logger, utiliptables.TableNAT, iptablesData)

	if nNatRules != expectedNatRules {
		t.Fatalf("Wrong number of nat rules: expected %d got %d\n%s", expectedNatRules, nNatRules, iptablesData)
	}

	populateEndpointSlices(fp,
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.0.0.2"},
			}, {
				Addresses: []string{"10.0.0.5"},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	fp.syncProxyRules()
	iptablesData = fp.iptablesData.String()

	nFilterRules = countRulesFromMetric(logger, utiliptables.TableFilter, string(fp.ipFamily))
	expectedFilterRules = countRules(logger, utiliptables.TableFilter, iptablesData)

	if nFilterRules != expectedFilterRules {
		t.Fatalf("Wrong number of filter rule: expected %d got %d\n%s", expectedFilterRules, nFilterRules, iptablesData)
	}

	nNatRules = countRulesFromMetric(logger, utiliptables.TableNAT, string(fp.ipFamily))
	expectedNatRules = countRules(logger, utiliptables.TableNAT, iptablesData)

	if nNatRules != expectedNatRules {
		t.Fatalf("Wrong number of nat rules: expected %d got %d\n%s", expectedNatRules, nNatRules, iptablesData)
	}
}

// TODO(thockin): 为 syncProxyRules() 增加更多测试，或将其拆分后逐块测试。

// 本测试确保 iptables 代理在指定了 internalTrafficPolicy 时，
// 能把端点正确转换成 iptables 规则。
// TestInternalTrafficPolicy 验证 internalTrafficPolicy=Local 时，ClusterIP 流量只转发到本地端点。
func TestInternalTrafficPolicy(t *testing.T) {
	type endpoint struct {
		ip       string
		nodeName string
	}

	testCases := []struct {
		name                  string
		line                  int
		internalTrafficPolicy *v1.ServiceInternalTrafficPolicy
		endpoints             []endpoint
		flowTests             []packetFlowTest
	}{
		{
			name:                  "internalTrafficPolicy is cluster",
			line:                  getLine(),
			internalTrafficPolicy: ptr.To(v1.ServiceInternalTrafficPolicyCluster),
			endpoints: []endpoint{
				{"10.0.1.1", testNodeName},
				{"10.0.1.2", "node1"},
				{"10.0.1.3", "node2"},
			},
			flowTests: []packetFlowTest{
				{
					name:     "pod to ClusterIP hits all endpoints",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "10.0.1.1:80, 10.0.1.2:80, 10.0.1.3:80",
					masq:     false,
				},
			},
		},
		{
			name:                  "internalTrafficPolicy is local and there is one local endpoint",
			line:                  getLine(),
			internalTrafficPolicy: ptr.To(v1.ServiceInternalTrafficPolicyLocal),
			endpoints: []endpoint{
				{"10.0.1.1", testNodeName},
				{"10.0.1.2", "node1"},
				{"10.0.1.3", "node2"},
			},
			flowTests: []packetFlowTest{
				{
					name:     "pod to ClusterIP hits only local endpoint",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "10.0.1.1:80",
					masq:     false,
				},
			},
		},
		{
			name:                  "internalTrafficPolicy is local and there are multiple local endpoints",
			line:                  getLine(),
			internalTrafficPolicy: ptr.To(v1.ServiceInternalTrafficPolicyLocal),
			endpoints: []endpoint{
				{"10.0.1.1", testNodeName},
				{"10.0.1.2", testNodeName},
				{"10.0.1.3", "node2"},
			},
			flowTests: []packetFlowTest{
				{
					name:     "pod to ClusterIP hits all local endpoints",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "10.0.1.1:80, 10.0.1.2:80",
					masq:     false,
				},
			},
		},
		{
			name:                  "internalTrafficPolicy is local and there are no local endpoints",
			line:                  getLine(),
			internalTrafficPolicy: ptr.To(v1.ServiceInternalTrafficPolicyLocal),
			endpoints: []endpoint{
				{"10.0.1.1", "node0"},
				{"10.0.1.2", "node1"},
				{"10.0.1.3", "node2"},
			},
			flowTests: []packetFlowTest{
				{
					name:     "no endpoints",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "DROP",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ipt := iptablestest.NewFake()
			fp := NewFakeProxier(ipt)
			fp.OnServiceSynced()
			fp.OnEndpointSlicesSynced()

			serviceName := "svc1"
			namespaceName := "ns1"

			svc := &v1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: serviceName, Namespace: namespaceName},
				Spec: v1.ServiceSpec{
					ClusterIP: "172.30.1.1",
					Selector:  map[string]string{"foo": "bar"},
					Ports:     []v1.ServicePort{{Name: "", Port: 80, Protocol: v1.ProtocolTCP}},
				},
			}
			if tc.internalTrafficPolicy != nil {
				svc.Spec.InternalTrafficPolicy = tc.internalTrafficPolicy
			}

			fp.OnServiceAdd(svc)

			endpointSlice := &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-1", serviceName),
					Namespace: namespaceName,
					Labels:    map[string]string{discovery.LabelServiceName: serviceName},
				},
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}},
				AddressType: discovery.AddressTypeIPv4,
			}
			for _, ep := range tc.endpoints {
				endpointSlice.Endpoints = append(endpointSlice.Endpoints, discovery.Endpoint{
					Addresses:  []string{ep.ip},
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
					NodeName:   ptr.To(ep.nodeName),
				})
			}

			// 添加端点后同步并模拟流量，然后删除端点再次验证 REJECT 规则恢复。
			fp.OnEndpointSliceAdd(endpointSlice)
			fp.syncProxyRules()
			runPacketFlowTests(t, tc.line, ipt, testNodeIPs, tc.flowTests)

			fp.OnEndpointSliceDelete(endpointSlice)
			fp.syncProxyRules()
			runPacketFlowTests(t, tc.line, ipt, testNodeIPs, []packetFlowTest{
				{
					name:     "endpoints deleted",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "REJECT",
				},
			})
		})
	}
}

// TestTerminatingEndpointsTrafficPolicyLocal 验证 Local 策略下，同时存在就绪端点与
// 就绪+终止中端点时，流量只使用就绪端点。
// 通过数据包模拟验证：就绪端点优先，只有全部就绪端点都在终止中时才回退使用它们。
func TestTerminatingEndpointsTrafficPolicyLocal(t *testing.T) {
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "svc1", Namespace: "ns1"},
		Spec: v1.ServiceSpec{
			ClusterIP:             "172.30.1.1",
			Type:                  v1.ServiceTypeLoadBalancer,
			ExternalTrafficPolicy: v1.ServiceExternalTrafficPolicyLocal,
			Ports: []v1.ServicePort{
				{
					Name:       "",
					TargetPort: intstr.FromInt32(80),
					Port:       80,
					Protocol:   v1.ProtocolTCP,
				},
			},
			HealthCheckNodePort: 30000,
		},
		Status: v1.ServiceStatus{
			LoadBalancer: v1.LoadBalancerStatus{
				Ingress: []v1.LoadBalancerIngress{
					{IP: "1.2.3.4"},
				},
			},
		},
	}

	testcases := []struct {
		name          string
		line          int
		endpointslice *discovery.EndpointSlice
		flowTests     []packetFlowTest
	}{
		{
			name: "ready endpoints exist",
			line: getLine(),
			// 场景：存在本地就绪端点，且另有就绪但终止中的端点。
			endpointslice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-1", "svc1"),
					Namespace: "ns1",
					Labels:    map[string]string{discovery.LabelServiceName: "svc1"},
				},
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}},
				AddressType: discovery.AddressTypeIPv4,
				Endpoints: []discovery.Endpoint{
					{
						Addresses: []string{"10.0.1.1"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						Addresses: []string{"10.0.1.2"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						// 已有就绪且未终止的端点，该端点对外部流量应被忽略。
						Addresses: []string{"10.0.1.3"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(true),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						// 已有就绪且未终止的端点，该端点对外部流量应被忽略。
						Addresses: []string{"10.0.1.4"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(false),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						// 该端点不是本地端点，对外部流量应被忽略。
						Addresses: []string{"10.0.1.5"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						NodeName: ptr.To("node-1"),
					},
				},
			},
			flowTests: []packetFlowTest{
				{
					name:     "pod to clusterIP",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "10.0.1.1:80, 10.0.1.2:80, 10.0.1.5:80",
					masq:     false,
				},
				{
					name:     "external to LB",
					sourceIP: testExternalClient,
					destIP:   "1.2.3.4",
					destPort: 80,
					output:   "10.0.1.1:80, 10.0.1.2:80",
					masq:     false,
				},
			},
		},
		{
			name: "only terminating endpoints exist",
			line: getLine(),
			endpointslice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-1", "svc1"),
					Namespace: "ns1",
					Labels:    map[string]string{discovery.LabelServiceName: "svc1"},
				},
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}},
				AddressType: discovery.AddressTypeIPv4,
				Endpoints: []discovery.Endpoint{
					{
						// 只有就绪的终止中端点可用，因此该端点应被使用。
						Addresses: []string{"10.0.1.2"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(true),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						// 只有就绪的终止中端点可用，因此该端点应被使用。
						Addresses: []string{"10.0.1.3"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(true),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						// 该端点既在终止中又未就绪，不应被使用。
						Addresses: []string{"10.0.1.4"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(false),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						// 该端点不是本地端点，对外部流量应被忽略。
						Addresses: []string{"10.0.1.5"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						NodeName: ptr.To("node-1"),
					},
				},
			},
			flowTests: []packetFlowTest{
				{
					name:     "pod to clusterIP",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "10.0.1.5:80",
					masq:     false,
				},
				{
					name:     "external to LB",
					sourceIP: testExternalClient,
					destIP:   "1.2.3.4",
					destPort: 80,
					output:   "10.0.1.2:80, 10.0.1.3:80",
					masq:     false,
				},
			},
		},
		{
			name: "terminating endpoints on remote node",
			line: getLine(),
			endpointslice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-1", "svc1"),
					Namespace: "ns1",
					Labels:    map[string]string{discovery.LabelServiceName: "svc1"},
				},
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}},
				AddressType: discovery.AddressTypeIPv4,
				Endpoints: []discovery.Endpoint{
					{
						// 该端点不是本地端点，因此不会被使用；
						// 但它的存在会阻止生成 REJECT 规则。
						Addresses: []string{"10.0.1.5"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(true),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To("node-1"),
					},
				},
			},
			flowTests: []packetFlowTest{
				{
					name:     "pod to clusterIP",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "10.0.1.5:80",
				},
				{
					name:     "external to LB, no locally-usable endpoints",
					sourceIP: testExternalClient,
					destIP:   "1.2.3.4",
					destPort: 80,
					output:   "DROP",
				},
			},
		},
		{
			name: "no usable endpoints on any node",
			line: getLine(),
			endpointslice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-1", "svc1"),
					Namespace: "ns1",
					Labels:    map[string]string{discovery.LabelServiceName: "svc1"},
				},
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}},
				AddressType: discovery.AddressTypeIPv4,
				Endpoints: []discovery.Endpoint{
					{
						// 本地但既未就绪也未提供服务的端点。
						Addresses: []string{"10.0.1.5"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(false),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						// 远端且既未就绪也未提供服务的端点。
						Addresses: []string{"10.0.1.5"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(false),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To("node-1"),
					},
				},
			},
			flowTests: []packetFlowTest{
				{
					name:     "pod to clusterIP, no usable endpoints",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "REJECT",
				},
				{
					name:     "external to LB, no usable endpoints",
					sourceIP: testExternalClient,
					destIP:   "1.2.3.4",
					destPort: 80,
					output:   "REJECT",
				},
			},
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			ipt := iptablestest.NewFake()
			fp := NewFakeProxier(ipt)
			fp.OnServiceSynced()
			fp.OnEndpointSlicesSynced()

			fp.OnServiceAdd(service)

			fp.OnEndpointSliceAdd(testcase.endpointslice)
			fp.syncProxyRules()
			runPacketFlowTests(t, testcase.line, ipt, testNodeIPs, testcase.flowTests)

			fp.OnEndpointSliceDelete(testcase.endpointslice)
			fp.syncProxyRules()
			runPacketFlowTests(t, testcase.line, ipt, testNodeIPs, []packetFlowTest{
				{
					name:     "pod to clusterIP after endpoints deleted",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "REJECT",
				},
				{
					name:     "external to LB after endpoints deleted",
					sourceIP: testExternalClient,
					destIP:   "1.2.3.4",
					destPort: 80,
					output:   "REJECT",
				},
			})
		})
	}
}

// TestTerminatingEndpointsTrafficPolicyCluster 验证 Cluster 策略下，同时存在集群级就绪端点与
// 通过数据包模拟验证：就绪端点优先，只有全部就绪端点都在终止中时才回退使用它们。
// TestTerminatingEndpointsTrafficPolicyCluster 验证 Cluster 策略下 terminating 端点的流量处理规则。
func TestTerminatingEndpointsTrafficPolicyCluster(t *testing.T) {
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "svc1", Namespace: "ns1"},
		Spec: v1.ServiceSpec{
			ClusterIP: "172.30.1.1",
			Type:      v1.ServiceTypeLoadBalancer,
			// 场景：存在集群级就绪端点，且另有就绪但终止中的端点。
			ExternalTrafficPolicy: v1.ServiceExternalTrafficPolicyCluster,
			Ports: []v1.ServicePort{
				{
					Name:       "",
					TargetPort: intstr.FromInt32(80),
					Port:       80,
					Protocol:   v1.ProtocolTCP,
				},
			},
			HealthCheckNodePort: 30000,
		},
		Status: v1.ServiceStatus{
			LoadBalancer: v1.LoadBalancerStatus{
				Ingress: []v1.LoadBalancerIngress{
					{IP: "1.2.3.4"},
				},
			},
		},
	}

	testcases := []struct {
		name          string
		line          int
		endpointslice *discovery.EndpointSlice
		flowTests     []packetFlowTest
	}{
		{
			name: "ready endpoints exist",
			line: getLine(),
			endpointslice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-1", "svc1"),
					Namespace: "ns1",
					Labels:    map[string]string{discovery.LabelServiceName: "svc1"},
				},
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}},
				AddressType: discovery.AddressTypeIPv4,
				Endpoints: []discovery.Endpoint{
					{
						Addresses: []string{"10.0.1.1"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						Addresses: []string{"10.0.1.2"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						// 已有就绪且未终止的端点，该端点应被忽略。
						Addresses: []string{"10.0.1.3"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(true),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To("another-node"),
					},
					{
						// 该端点未标记为 serving，应被忽略。
						Addresses: []string{"10.0.1.4"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(false),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To("another-node"),
					},
					{
						Addresses: []string{"10.0.1.5"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						NodeName: ptr.To("another-node"),
					},
				},
			},
			flowTests: []packetFlowTest{
				{
					name:     "pod to clusterIP",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "10.0.1.1:80, 10.0.1.2:80, 10.0.1.5:80",
					masq:     false,
				},
				{
					name:     "external to LB",
					sourceIP: testExternalClient,
					destIP:   "1.2.3.4",
					destPort: 80,
					output:   "10.0.1.1:80, 10.0.1.2:80, 10.0.1.5:80",
					masq:     true,
				},
			},
		},
		{
			name: "only terminating endpoints exist",
			line: getLine(),
			endpointslice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-1", "svc1"),
					Namespace: "ns1",
					Labels:    map[string]string{discovery.LabelServiceName: "svc1"},
				},
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}},
				AddressType: discovery.AddressTypeIPv4,
				Endpoints: []discovery.Endpoint{
					{
						// 只有就绪的终止中端点可用，因此该端点应被使用。
						Addresses: []string{"10.0.1.2"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(true),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						// 只有就绪的终止中端点可用，因此该端点应被使用。
						Addresses: []string{"10.0.1.3"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(true),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						// 该端点既在终止中又未就绪，不应被使用。
						Addresses: []string{"10.0.1.4"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(false),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To("another-node"),
					},
					{
						// 只有就绪的终止中端点可用，因此该端点应被使用。
						Addresses: []string{"10.0.1.5"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(true),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To("another-node"),
					},
				},
			},
			flowTests: []packetFlowTest{
				{
					name:     "pod to clusterIP",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "10.0.1.2:80, 10.0.1.3:80, 10.0.1.5:80",
					masq:     false,
				},
				{
					name:     "external to LB",
					sourceIP: testExternalClient,
					destIP:   "1.2.3.4",
					destPort: 80,
					output:   "10.0.1.2:80, 10.0.1.3:80, 10.0.1.5:80",
					masq:     true,
				},
			},
		},
		{
			name: "terminating endpoints on remote node",
			line: getLine(),
			endpointslice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-1", "svc1"),
					Namespace: "ns1",
					Labels:    map[string]string{discovery.LabelServiceName: "svc1"},
				},
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}},
				AddressType: discovery.AddressTypeIPv4,
				Endpoints: []discovery.Endpoint{
					{
						Addresses: []string{"10.0.1.5"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(true),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To("node-1"),
					},
				},
			},
			flowTests: []packetFlowTest{
				{
					name:     "pod to clusterIP",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "10.0.1.5:80",
					masq:     false,
				},
				{
					name:     "external to LB",
					sourceIP: testExternalClient,
					destIP:   "1.2.3.4",
					destPort: 80,
					output:   "10.0.1.5:80",
					masq:     true,
				},
			},
		},
		{
			name: "no usable endpoints on any node",
			line: getLine(),
			endpointslice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-1", "svc1"),
					Namespace: "ns1",
					Labels:    map[string]string{discovery.LabelServiceName: "svc1"},
				},
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}},
				AddressType: discovery.AddressTypeIPv4,
				Endpoints: []discovery.Endpoint{
					{
						// 本地但既未就绪也未提供服务的端点。
						Addresses: []string{"10.0.1.5"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(false),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						// 远端且既未就绪也未提供服务的端点。
						Addresses: []string{"10.0.1.5"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(false),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To("node-1"),
					},
				},
			},
			flowTests: []packetFlowTest{
				{
					name:     "pod to clusterIP",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "REJECT",
				},
				{
					name:     "external to LB",
					sourceIP: testExternalClient,
					destIP:   "1.2.3.4",
					destPort: 80,
					output:   "REJECT",
				},
			},
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {

			ipt := iptablestest.NewFake()
			fp := NewFakeProxier(ipt)
			fp.OnServiceSynced()
			fp.OnEndpointSlicesSynced()

			fp.OnServiceAdd(service)

			fp.OnEndpointSliceAdd(testcase.endpointslice)
			fp.syncProxyRules()
			runPacketFlowTests(t, testcase.line, ipt, testNodeIPs, testcase.flowTests)

			fp.OnEndpointSliceDelete(testcase.endpointslice)
			fp.syncProxyRules()
			runPacketFlowTests(t, testcase.line, ipt, testNodeIPs, []packetFlowTest{
				{
					name:     "pod to clusterIP after endpoints deleted",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "REJECT",
				},
				{
					name:     "external to LB after endpoints deleted",
					sourceIP: testExternalClient,
					destIP:   "1.2.3.4",
					destPort: 80,
					output:   "REJECT",
				},
			})
		})
	}
}

// TestInternalExternalMasquerade 验证内部/外部流量在 internalTrafficPolicy 与
// externalTrafficPolicy 各取值下是否伪装、选中哪些端点的完整矩阵。
func TestInternalExternalMasquerade(t *testing.T) {
	// 将测试准备代码放进内部函数，使其位于测试场景之前；
	// 这样各场景共享同一套初始 Service/EndpointSlice 状态。
	setupTest := func(fp *Proxier) {
		makeServiceMap(fp,
			makeTestService("ns1", "svc1", func(svc *v1.Service) {
				svc.Spec.Type = "LoadBalancer"
				svc.Spec.ClusterIP = "172.30.0.41"
				svc.Spec.Ports = []v1.ServicePort{{
					Name:     "p80",
					Port:     80,
					Protocol: v1.ProtocolTCP,
					NodePort: int32(3001),
				}}
				svc.Spec.HealthCheckNodePort = 30001
				svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
					IP: "1.2.3.4",
				}}
			}),
			makeTestService("ns2", "svc2", func(svc *v1.Service) {
				svc.Spec.Type = "LoadBalancer"
				svc.Spec.ClusterIP = "172.30.0.42"
				svc.Spec.Ports = []v1.ServicePort{{
					Name:     "p80",
					Port:     80,
					Protocol: v1.ProtocolTCP,
					NodePort: int32(3002),
				}}
				svc.Spec.HealthCheckNodePort = 30002
				svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
				svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
					IP: "5.6.7.8",
				}}
			}),
			makeTestService("ns3", "svc3", func(svc *v1.Service) {
				svc.Spec.Type = "LoadBalancer"
				svc.Spec.ClusterIP = "172.30.0.43"
				svc.Spec.Ports = []v1.ServicePort{{
					Name:     "p80",
					Port:     80,
					Protocol: v1.ProtocolTCP,
					NodePort: int32(3003),
				}}
				svc.Spec.HealthCheckNodePort = 30003
				svc.Spec.InternalTrafficPolicy = ptr.To(v1.ServiceInternalTrafficPolicyLocal)
				svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
					IP: "9.10.11.12",
				}}
			}),
		)

		populateEndpointSlices(fp,
			makeTestEndpointSlice("ns1", "svc1", 1, func(eps *discovery.EndpointSlice) {
				eps.AddressType = discovery.AddressTypeIPv4
				eps.Endpoints = []discovery.Endpoint{
					{
						Addresses: []string{"10.180.0.1"},
						NodeName:  ptr.To(testNodeName),
					},
					{
						Addresses: []string{"10.180.1.1"},
						NodeName:  ptr.To("remote"),
					},
				}
				eps.Ports = []discovery.EndpointPort{{
					Name:     ptr.To("p80"),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}}
			}),
			makeTestEndpointSlice("ns2", "svc2", 1, func(eps *discovery.EndpointSlice) {
				eps.AddressType = discovery.AddressTypeIPv4
				eps.Endpoints = []discovery.Endpoint{
					{
						Addresses: []string{"10.180.0.2"},
						NodeName:  ptr.To(testNodeName),
					},
					{
						Addresses: []string{"10.180.1.2"},
						NodeName:  ptr.To("remote"),
					},
				}
				eps.Ports = []discovery.EndpointPort{{
					Name:     ptr.To("p80"),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}}
			}),
			makeTestEndpointSlice("ns3", "svc3", 1, func(eps *discovery.EndpointSlice) {
				eps.AddressType = discovery.AddressTypeIPv4
				eps.Endpoints = []discovery.Endpoint{
					{
						Addresses: []string{"10.180.0.3"},
						NodeName:  ptr.To(testNodeName),
					},
					{
						Addresses: []string{"10.180.1.3"},
						NodeName:  ptr.To("remote"),
					},
				}
				eps.Ports = []discovery.EndpointPort{{
					Name:     ptr.To("p80"),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}}
			}),
		)

		fp.syncProxyRules()
	}

	// 所有场景共用 flowTests；其中 output/masq 描述的是正常情况
	// （本地检测可用、未开启 masqueradeAll）。
	flowTests := []packetFlowTest{
		{
			name:     "pod to ClusterIP",
			sourceIP: "10.0.0.2",
			destIP:   "172.30.0.41",
			destPort: 80,
			output:   "10.180.0.1:80, 10.180.1.1:80",
			masq:     false,
		},
		{
			name:     "pod to NodePort",
			sourceIP: "10.0.0.2",
			destIP:   testNodeIP,
			destPort: 3001,
			output:   "10.180.0.1:80, 10.180.1.1:80",
			masq:     true,
		},
		{
			name:     "pod to LB",
			sourceIP: "10.0.0.2",
			destIP:   "1.2.3.4",
			destPort: 80,
			output:   "10.180.0.1:80, 10.180.1.1:80",
			masq:     true,
		},
		{
			name:     "node to ClusterIP",
			sourceIP: testNodeIP,
			destIP:   "172.30.0.41",
			destPort: 80,
			output:   "10.180.0.1:80, 10.180.1.1:80",
			masq:     true,
		},
		{
			name:     "node to NodePort",
			sourceIP: testNodeIP,
			destIP:   testNodeIP,
			destPort: 3001,
			output:   "10.180.0.1:80, 10.180.1.1:80",
			masq:     true,
		},
		{
			name:     "localhost to NodePort",
			sourceIP: "127.0.0.1",
			destIP:   "127.0.0.1",
			destPort: 3001,
			output:   "10.180.0.1:80, 10.180.1.1:80",
			masq:     true,
		},
		{
			name:     "node to LB",
			sourceIP: testNodeIP,
			destIP:   "1.2.3.4",
			destPort: 80,
			output:   "10.180.0.1:80, 10.180.1.1:80",
			masq:     true,
		},
		{
			name:     "external to ClusterIP",
			sourceIP: testExternalClient,
			destIP:   "172.30.0.41",
			destPort: 80,
			output:   "10.180.0.1:80, 10.180.1.1:80",
			masq:     true,
		},
		{
			name:     "external to NodePort",
			sourceIP: testExternalClient,
			destIP:   testNodeIP,
			destPort: 3001,
			output:   "10.180.0.1:80, 10.180.1.1:80",
			masq:     true,
		},
		{
			name:     "external to LB",
			sourceIP: testExternalClient,
			destIP:   "1.2.3.4",
			destPort: 80,
			output:   "10.180.0.1:80, 10.180.1.1:80",
			masq:     true,
		},
		{
			name:     "pod to ClusterIP with eTP:Local",
			sourceIP: "10.0.0.2",
			destIP:   "172.30.0.42",
			destPort: 80,

			// externalTrafficPolicy 不适用于 ClusterIP 流量，
			// 因此与「Pod 访问 ClusterIP」相同。
			output: "10.180.0.2:80, 10.180.1.2:80",
			masq:   false,
		},
		{
			name:     "pod to NodePort with eTP:Local",
			sourceIP: "10.0.0.2",
			destIP:   testNodeIP,
			destPort: 3002,

			// 参见下方「Pod 访问 eTP:Local 的 LB」用例的注释。
			// 从集群外直连 NodePort 时其实只会到达本地端点，
			// 所以这里的短路在语义上并不完全等价；
			// 但对实现而言更简单，对用户也更方便，
			// 因此该场景同样按短路处理。
			output: "10.180.0.2:80, 10.180.1.2:80",
			masq:   false,
		},
		{
			name:     "pod to LB with eTP:Local",
			sourceIP: "10.0.0.2",
			destIP:   "5.6.7.8",
			destPort: 80,

			// 短路规则的目的是模拟数据包真的出集群再访问 LB 后返回的效果，
			// 因此流量应被路由到所有端点，而非仅本地端点。
			// 实际上若数据包真的离开集群，需要经过伪装；
			// 但短路场景可以避免伪装，而不伪装对用户更有用，
			// 所以这里选择不伪装。
			// 短路场景可以避免伪装，而不伪装对用户更有用，
			// 所以这里选择不伪装。
			output: "10.180.0.2:80, 10.180.1.2:80",
			masq:   false,
		},
		{
			name:     "node to ClusterIP with eTP:Local",
			sourceIP: testNodeIP,
			destIP:   "172.30.0.42",
			destPort: 80,

			// externalTrafficPolicy 不适用于 ClusterIP 流量，
			// 因此与「节点访问 ClusterIP」相同。
			output: "10.180.0.2:80, 10.180.1.2:80",
			masq:   true,
		},
		{
			name:     "node to NodePort with eTP:Local",
			sourceIP: testNodeIP,
			destIP:   testNodeIP,
			destPort: 3001,

			// 流量被短路，忽略 externalTrafficPolicy，
			// 因此与上文「节点访问 NodePort」相同。
			output: "10.180.0.1:80, 10.180.1.1:80",
			masq:   true,
		},
		{
			name:     "localhost to NodePort with eTP:Local",
			sourceIP: "127.0.0.1",
			destIP:   "127.0.0.1",
			destPort: 3002,

			// 流量被短路，忽略 externalTrafficPolicy，
			// 因此与上文「localhost 访问 NodePort」相同。
			output: "10.180.0.2:80, 10.180.1.2:80",
			masq:   true,
		},
		{
			name:     "node to LB with eTP:Local",
			sourceIP: testNodeIP,
			destIP:   "5.6.7.8",
			destPort: 80,

			// 流量被短路，忽略 externalTrafficPolicy，
			// 因此与上文「节点访问 LB」相同。
			output: "10.180.0.2:80, 10.180.1.2:80",
			masq:   true,
		},
		{
			name:     "external to ClusterIP with eTP:Local",
			sourceIP: testExternalClient,
			destIP:   "172.30.0.42",
			destPort: 80,

			// externalTrafficPolicy 不适用于 ClusterIP 流量，
			// 因此与上文「外部访问 ClusterIP」相同。
			output: "10.180.0.2:80, 10.180.1.2:80",
			masq:   true,
		},
		{
			name:     "external to NodePort with eTP:Local",
			sourceIP: testExternalClient,
			destIP:   testNodeIP,
			destPort: 3002,

			// externalTrafficPolicy 生效：只选中本地端点，且不进行伪装。
			// 只选中本地端点，且不进行伪装。
			output: "10.180.0.2:80",
			masq:   false,
		},
		{
			name:     "external to LB with eTP:Local",
			sourceIP: testExternalClient,
			destIP:   "5.6.7.8",
			destPort: 80,

			// externalTrafficPolicy 生效：只选中本地端点，且不进行伪装。
			// 只选中本地端点，且不进行伪装。
			output: "10.180.0.2:80",
			masq:   false,
		},
		{
			name:     "pod to ClusterIP with iTP:Local",
			sourceIP: "10.0.0.2",
			destIP:   "172.30.0.43",
			destPort: 80,

			// internalTrafficPolicy 生效：只选中本地端点。
			output: "10.180.0.3:80",
			masq:   false,
		},
		{
			name:     "pod to NodePort with iTP:Local",
			sourceIP: "10.0.0.2",
			destIP:   testNodeIP,
			destPort: 3003,

			// internalTrafficPolicy 不适用于 NodePort 流量，
			// 因此与「Pod 访问 NodePort」相同。
			output: "10.180.0.3:80, 10.180.1.3:80",
			masq:   true,
		},
		{
			name:     "pod to LB with iTP:Local",
			sourceIP: "10.0.0.2",
			destIP:   "9.10.11.12",
			destPort: 80,

			// internalTrafficPolicy 不适用于 LoadBalancer 流量，
			// 因此与「Pod 访问 LB」相同。
			output: "10.180.0.3:80, 10.180.1.3:80",
			masq:   true,
		},
		{
			name:     "node to ClusterIP with iTP:Local",
			sourceIP: testNodeIP,
			destIP:   "172.30.0.43",
			destPort: 80,

			// internalTrafficPolicy 生效：只选中本地端点。
			// 与「节点访问 ClusterIP」一样会被伪装，
			// 因为 internalTrafficPolicy 不影响是否伪装。
			output: "10.180.0.3:80",
			masq:   true,
		},
		{
			name:     "node to NodePort with iTP:Local",
			sourceIP: testNodeIP,
			destIP:   testNodeIP,
			destPort: 3003,

			// internalTrafficPolicy 不适用于 NodePort 流量，
			// 因此与「节点访问 NodePort」相同。
			output: "10.180.0.3:80, 10.180.1.3:80",
			masq:   true,
		},
		{
			name:     "localhost to NodePort with iTP:Local",
			sourceIP: "127.0.0.1",
			destIP:   "127.0.0.1",
			destPort: 3003,

			// internalTrafficPolicy 不适用于 NodePort 流量，
			// 因此与「localhost 访问 NodePort」相同。
			output: "10.180.0.3:80, 10.180.1.3:80",
			masq:   true,
		},
		{
			name:     "node to LB with iTP:Local",
			sourceIP: testNodeIP,
			destIP:   "9.10.11.12",
			destPort: 80,

			// internalTrafficPolicy 不适用于 LoadBalancer 流量，
			// 因此与「节点访问 LB」相同。
			output: "10.180.0.3:80, 10.180.1.3:80",
			masq:   true,
		},
		{
			name:     "external to ClusterIP with iTP:Local",
			sourceIP: testExternalClient,
			destIP:   "172.30.0.43",
			destPort: 80,

			// internalTrafficPolicy 生效：只选中本地端点。
			// 与「外部访问 ClusterIP」一样会被伪装，
			// 因为 internalTrafficPolicy 不影响是否伪装。
			output: "10.180.0.3:80",
			masq:   true,
		},
		{
			name:     "external to NodePort with iTP:Local",
			sourceIP: testExternalClient,
			destIP:   testNodeIP,
			destPort: 3003,

			// internalTrafficPolicy 不适用于 NodePort 流量，
			// 因此与「外部访问 NodePort」相同。
			output: "10.180.0.3:80, 10.180.1.3:80",
			masq:   true,
		},
		{
			name:     "external to LB with iTP:Local",
			sourceIP: testExternalClient,
			destIP:   "9.10.11.12",
			destPort: 80,

			// internalTrafficPolicy 不适用于 LoadBalancer 流量，
			// 因此与「外部访问 LB」相同。
			output: "10.180.0.3:80, 10.180.1.3:80",
			masq:   true,
		},
	}

	type packetFlowTestOverride struct {
		output *string
		masq   *bool
	}

	testCases := []struct {
		name          string
		line          int
		masqueradeAll bool
		localDetector bool
		overrides     map[string]packetFlowTestOverride
	}{
		{
			name:          "base",
			line:          getLine(),
			masqueradeAll: false,
			localDetector: true,
			overrides:     nil,
		},
		{
			name:          "no LocalTrafficDetector",
			line:          getLine(),
			masqueradeAll: false,
			localDetector: false,
			overrides: map[string]packetFlowTestOverride{
				// 注入空 LocalTrafficDetector 时，所有访问 ClusterIP 的流量
				// 都被当作来自 Pod，因此无需伪装。
				"node to ClusterIP": {
					masq: ptr.To(false),
				},
				"node to ClusterIP with eTP:Local": {
					masq: ptr.To(false),
				},
				"node to ClusterIP with iTP:Local": {
					masq: ptr.To(false),
				},
				"external to ClusterIP": {
					masq: ptr.To(false),
				},
				"external to ClusterIP with eTP:Local": {
					masq: ptr.To(false),
				},
				"external to ClusterIP with iTP:Local": {
					masq: ptr.To(false),
				},

				// 此时也不存在 eTP:Local 对 Pod 流量的短路，
				// 因此 Pod 只能访问本地端点。
				"pod to NodePort with eTP:Local": {
					output: ptr.To("10.180.0.2:80"),
				},
				"pod to LB with eTP:Local": {
					output: ptr.To("10.180.0.2:80"),
				},
			},
		},
		{
			name:          "masqueradeAll",
			line:          getLine(),
			masqueradeAll: true,
			localDetector: true,
			overrides: map[string]packetFlowTestOverride{
				// 开启 --masquerade-all 后，所有访问 ClusterIP 的流量都会被伪装。
				"pod to ClusterIP": {
					masq: ptr.To(true),
				},
				"pod to ClusterIP with eTP:Local": {
					masq: ptr.To(true),
				},
				"pod to ClusterIP with iTP:Local": {
					masq: ptr.To(true),
				},
			},
		},
		{
			name:          "masqueradeAll, no LocalTrafficDetector",
			line:          getLine(),
			masqueradeAll: true,
			localDetector: false,
			overrides: map[string]packetFlowTestOverride{
				// 即使有 eTP:Local，Pod 流量也不会触发短路，因此只选中本地端点。
				// 与 "masqueradeAll" 场景一致：
				"pod to ClusterIP": {
					masq: ptr.To(true),
				},
				"pod to ClusterIP with eTP:Local": {
					masq: ptr.To(true),
				},
				"pod to ClusterIP with iTP:Local": {
					masq: ptr.To(true),
				},

				// 同「注入空 LocalTrafficDetector」场景。
				"pod to NodePort with eTP:Local": {
					output: ptr.To("10.180.0.2:80"),
				},
				"pod to LB with eTP:Local": {
					output: ptr.To("10.180.0.2:80"),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ipt := iptablestest.NewFake()
			fp := NewFakeProxier(ipt)
			fp.masqueradeAll = tc.masqueradeAll
			if !tc.localDetector {
				fp.localDetector = proxyutil.NewNoOpLocalDetector()
			}
			setupTest(fp)

			// 合并基础 flowTests 与每个场景特有的覆盖值。
			tcFlowTests := make([]packetFlowTest, len(flowTests))
			overridesApplied := 0
			for i := range flowTests {
				tcFlowTests[i] = flowTests[i]
				if overrides, set := tc.overrides[flowTests[i].name]; set {
					overridesApplied++
					if overrides.masq != nil {
						if tcFlowTests[i].masq == *overrides.masq {
							t.Errorf("%q override value for masq is same as base value", flowTests[i].name)
						}
						tcFlowTests[i].masq = *overrides.masq
					}
					if overrides.output != nil {
						if tcFlowTests[i].output == *overrides.output {
							t.Errorf("%q override value for output is same as base value", flowTests[i].name)
						}
						tcFlowTests[i].output = *overrides.output
					}
				}
			}
			if overridesApplied != len(tc.overrides) {
				t.Errorf("%d overrides did not match any test case name!", len(tc.overrides)-overridesApplied)
			}
			runPacketFlowTests(t, tc.line, ipt, testNodeIPs, tcFlowTests)
		})
	}
}

// countEndpointsAndComments 统计规则文本中匹配给定端点的规则数与注释数。
func countEndpointsAndComments(iptablesData string, matchEndpoint string) (string, int, int) {
	var numEndpoints, numComments int
	var matched string
	for _, line := range strings.Split(iptablesData, "\n") {
		if strings.HasPrefix(line, "-A KUBE-SEP-") && strings.Contains(line, "-j DNAT") {
			numEndpoints++
			if strings.Contains(line, "--comment") {
				numComments++
			}
			if strings.Contains(line, matchEndpoint) {
				matched = line
			}
		}
	}
	return matched, numEndpoints, numComments
}

// TestSyncProxyRulesLargeClusterMode 验证大集群模式下只同步变化过的服务，
// 以及全量同步与延迟清理过时链的行为。
func TestSyncProxyRulesLargeClusterMode(t *testing.T) {
	ipt := iptablestest.NewFake()
	fp := NewFakeProxier(ipt)
	fp.masqueradeAll = true
	fp.syncPeriod = 30 * time.Second

	makeServiceMap(fp,
		makeTestService("ns1", "svc1", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = "172.30.0.41"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
			}}
		}),
		makeTestService("ns2", "svc2", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = "172.30.0.42"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p8080",
				Port:     8080,
				Protocol: v1.ProtocolTCP,
			}}
		}),
		makeTestService("ns3", "svc3", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = "172.30.0.43"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p8081",
				Port:     8081,
				Protocol: v1.ProtocolTCP,
			}}
		}),
	)

	populateEndpointSlices(fp,
		makeTestEndpointSlice("ns1", "svc1", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = make([]discovery.Endpoint, largeClusterEndpointsThreshold/2-1)
			for i := range eps.Endpoints {
				eps.Endpoints[i].Addresses = []string{fmt.Sprintf("10.0.%d.%d", i%256, i/256)}
			}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p80"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		makeTestEndpointSlice("ns2", "svc2", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = make([]discovery.Endpoint, largeClusterEndpointsThreshold/2-1)
			for i := range eps.Endpoints {
				eps.Endpoints[i].Addresses = []string{fmt.Sprintf("10.1.%d.%d", i%256, i/256)}
			}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p8080"),
				Port:     ptr.To[int32](8080),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	fp.syncProxyRules()
	expectedEndpoints := 2 * (largeClusterEndpointsThreshold/2 - 1)

	firstEndpoint, numEndpoints, numComments := countEndpointsAndComments(fp.iptablesData.String(), "10.0.0.0")
	assert.Equal(t, "-A KUBE-SEP-DKGQUZGBKLTPAR56 -m comment --comment ns1/svc1:p80 -m tcp -p tcp -j DNAT --to-destination 10.0.0.0:80", firstEndpoint)
	if numEndpoints != expectedEndpoints {
		t.Errorf("Found wrong number of endpoints: expected %d, got %d", expectedEndpoints, numEndpoints)
	}
	if numComments != numEndpoints {
		t.Errorf("numComments (%d) != numEndpoints (%d) when numEndpoints < threshold (%d)", numComments, numEndpoints, largeClusterEndpointsThreshold)
	}

	fp.OnEndpointSliceAdd(makeTestEndpointSlice("ns3", "svc3", 1, func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"203.0.113.4"},
		}, {
			Addresses: []string{"203.0.113.8"},
		}, {
			Addresses: []string{"203.0.113.12"},
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p8081"),
			Port:     ptr.To[int32](8081),
			Protocol: ptr.To(v1.ProtocolTCP),
		}}
	}))
	fp.syncProxyRules()

	firstEndpoint, numEndpoints, numComments = countEndpointsAndComments(fp.iptablesData.String(), "203.0.113.4")
	assert.Equal(t, "-A KUBE-SEP-RUVVH7YV3PHQBDOS -m tcp -p tcp -j DNAT --to-destination 203.0.113.4:8081", firstEndpoint)
	// 由于 svc1/svc2 没有变化，syncProxyRules 只会输出 svc3 的端点；
	// 跨过 largeClusterEndpointsThreshold 时不会自动触发全量同步。
	if numEndpoints != 3 {
		t.Errorf("Found wrong number of endpoints on partial resync: expected %d, got %d", 3, numEndpoints)
	}
	if numComments != 0 {
		t.Errorf("numComments (%d) != 0 after partial resync when numEndpoints (%d) > threshold (%d)", numComments, expectedEndpoints+3, largeClusterEndpointsThreshold)
	}

	// 即使 FullSyncPeriod 已到，只要没有显式变化需要全量同步，
	// 大集群模式下仍保持部分同步。
	if !fp.largeClusterMode {
		t.Fatalf("expected to be in large cluster mode")
	}
	expectedLastFullSync := time.Now().Add(-proxyutil.FullSyncPeriod).Add(-time.Second)
	fp.lastFullSync = expectedLastFullSync
	err := fp.syncProxyRules()
	if err != nil {
		t.Fatalf("syncProxyRules failed: %v", err)
	}
	if !fp.lastFullSync.Equal(expectedLastFullSync) {
		t.Fatalf("expected periodic sync in large cluster mode to skip full sync: lastFullSync changed from %v to %v", expectedLastFullSync, fp.lastFullSync)
	}

	// 强制全量同步，确认较早创建的、无注释的服务规则也会被重写。
	// 全量同步也会重写旧服务的规则，移除其中的注释。
	fp.forceSyncProxyRules()
	expectedEndpoints += 3

	firstEndpoint, numEndpoints, numComments = countEndpointsAndComments(fp.iptablesData.String(), "10.0.0.0")
	assert.Equal(t, "-A KUBE-SEP-DKGQUZGBKLTPAR56 -m tcp -p tcp -j DNAT --to-destination 10.0.0.0:80", firstEndpoint)
	if numEndpoints != expectedEndpoints {
		t.Errorf("Found wrong number of endpoints: expected %d, got %d", expectedEndpoints, numEndpoints)
	}
	if numComments != 0 {
		t.Errorf("numComments (%d) != 0 when numEndpoints (%d) > threshold (%d)", numComments, numEndpoints, largeClusterEndpointsThreshold)
	}

	// 测试删除服务：需要新建一个服务，因为一旦删除现有服务，
	// 端点数会跌破阈值，退出大集群模式。
	svc4 := makeTestService("ns4", "svc4", func(svc *v1.Service) {
		svc.Spec.Type = v1.ServiceTypeClusterIP
		svc.Spec.ClusterIP = "172.30.0.44"
		svc.Spec.Ports = []v1.ServicePort{{
			Name:     "p8082",
			Port:     8082,
			Protocol: v1.ProtocolTCP,
		}}
	})
	fp.OnServiceAdd(svc4)
	fp.OnEndpointSliceAdd(makeTestEndpointSlice("ns4", "svc4", 1, func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.4.0.1"},
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p8082"),
			Port:     ptr.To[int32](8082),
			Protocol: ptr.To(v1.ProtocolTCP),
		}}
	}))
	fp.syncProxyRules()

	svc4Endpoint, numEndpoints, _ := countEndpointsAndComments(fp.iptablesData.String(), "10.4.0.1")
	assert.Equal(t, "-A KUBE-SEP-SU5STNODRYEWJAUF -m tcp -p tcp -j DNAT --to-destination 10.4.0.1:8082", svc4Endpoint, "svc4 endpoint was not created")
	// 此时应只同步 svc4。
	if numEndpoints != 1 {
		t.Errorf("Found wrong number of endpoints after svc4 creation: expected %d, got %d", 1, numEndpoints)
	}

	// 大集群模式下删除服务时不会重写其链，
	// 但也不会立刻删除链。
	fp.lastIPTablesCleanup = time.Now()
	fp.OnServiceDelete(svc4)
	fp.syncProxyRules()

	svc4Endpoint, numEndpoints, _ = countEndpointsAndComments(fp.iptablesData.String(), "10.4.0.1")
	assert.Equal(t, "", svc4Endpoint, "svc4 endpoint was still created!")
	// 应只同步 svc4，且不输出它的端点。
	if numEndpoints != 0 {
		t.Errorf("Found wrong number of endpoints after service deletion: expected %d, got %d", 0, numEndpoints)
	}
	assert.NotContains(t, fp.iptablesData.String(), "-X ", "iptables data unexpectedly contains chain deletions")

	// 等待足够长时间后再同步，过时的链才会被删除。
	fp.lastIPTablesCleanup = time.Now().Add(-fp.syncPeriod).Add(-1)
	fp.syncProxyRules()

	svc4Endpoint, numEndpoints, _ = countEndpointsAndComments(fp.iptablesData.String(), "10.4.0.1")
	assert.Equal(t, "", svc4Endpoint, "svc4 endpoint was still created!")
	if numEndpoints != 0 {
		t.Errorf("Found wrong number of endpoints after delayed resync: expected %d, got %d", 0, numEndpoints)
	}
	assert.Contains(t, fp.iptablesData.String(), "-X KUBE-SVC-EBDQOQU5SJFXRIL3", "iptables data does not contain chain deletion")
	assert.Contains(t, fp.iptablesData.String(), "-X KUBE-SEP-SU5STNODRYEWJAUF", "iptables data does not contain endpoint deletions")

	// 强制全量同步并重新统计，确认链被彻底清理。
	fp.forceSyncProxyRules()
	_, numEndpoints, _ = countEndpointsAndComments(fp.iptablesData.String(), "10.0.0.0")
	if numEndpoints != expectedEndpoints {
		t.Errorf("Found wrong number of endpoints: expected %d, got %d", expectedEndpoints, numEndpoints)
	}
}

// 测试在各种变更下多次调用 syncProxyRules() 的行为。
// TestSyncProxyRulesRepeated 验证反复同步时规则稳定性和增量/全量同步的计数。
func TestSyncProxyRulesRepeated(t *testing.T) {
	logger, _ := klogtesting.NewTestContext(t)
	ipt := iptablestest.NewFake()
	fp := NewFakeProxier(ipt)
	metrics.RegisterMetrics(kubeproxyconfig.ProxyModeIPTables)
	defer legacyregistry.Reset()

	// 建立初始状态。
	var svc2 *v1.Service

	makeServiceMap(fp,
		makeTestService("ns1", "svc1", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = "172.30.0.41"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
			}}
		}),
		makeTestService("ns2", "svc2", func(svc *v1.Service) {
			svc2 = svc
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = "172.30.0.42"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p8080",
				Port:     8080,
				Protocol: v1.ProtocolTCP,
			}}
		}),
	)

	populateEndpointSlices(fp,
		makeTestEndpointSlice("ns1", "svc1", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.0.1.1"},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p80"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		makeTestEndpointSlice("ns2", "svc2", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.0.2.1"},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p8080"),
				Port:     ptr.To[int32](8080),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	fp.syncProxyRules()

	expected := dedent.Dedent(`
		*filter
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-EXTERNAL-SERVICES - [0:0]
		:KUBE-FIREWALL - [0:0]
		:KUBE-FORWARD - [0:0]
		:KUBE-PROXY-FIREWALL - [0:0]
		-A KUBE-FIREWALL -m comment --comment "block incoming localnet connections" -d 127.0.0.0/8 ! -s 127.0.0.0/8 -m conntrack ! --ctstate RELATED,ESTABLISHED,DNAT -j DROP
		-A KUBE-FORWARD -m conntrack --ctstate INVALID -m nfacct --nfacct-name ct_state_invalid_dropped_pkts -j DROP
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
		COMMIT
		*nat
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-MARK-MASQ - [0:0]
		:KUBE-POSTROUTING - [0:0]
		:KUBE-SEP-SNQ3ZNILQDEJNDQO - [0:0]
		:KUBE-SEP-UHEGFW77JX3KXTOV - [0:0]
		:KUBE-SVC-2VJB64SDSIJUP5T6 - [0:0]
		:KUBE-SVC-XPGD46QRK7WJZT7O - [0:0]
		-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 -j KUBE-SVC-XPGD46QRK7WJZT7O
		-A KUBE-SERVICES -m comment --comment "ns2/svc2:p8080 cluster IP" -m tcp -p tcp -d 172.30.0.42 --dport 8080 -j KUBE-SVC-2VJB64SDSIJUP5T6
		-A KUBE-SERVICES -m comment --comment "kubernetes service nodeports; NOTE: this must be the last rule in this chain" -m addrtype --dst-type LOCAL -j KUBE-NODEPORTS
		-A KUBE-MARK-MASQ -j MARK --or-mark 0x4000
		-A KUBE-POSTROUTING -m mark ! --mark 0x4000/0x4000 -j RETURN
		-A KUBE-POSTROUTING -j MARK --xor-mark 0x4000
		-A KUBE-POSTROUTING -m comment --comment "kubernetes service traffic requiring SNAT" -j MASQUERADE --random-fully
		-A KUBE-SEP-SNQ3ZNILQDEJNDQO -m comment --comment ns1/svc1:p80 -s 10.0.1.1 -j KUBE-MARK-MASQ
		-A KUBE-SEP-SNQ3ZNILQDEJNDQO -m comment --comment ns1/svc1:p80 -m tcp -p tcp -j DNAT --to-destination 10.0.1.1:80
		-A KUBE-SEP-UHEGFW77JX3KXTOV -m comment --comment ns2/svc2:p8080 -s 10.0.2.1 -j KUBE-MARK-MASQ
		-A KUBE-SEP-UHEGFW77JX3KXTOV -m comment --comment ns2/svc2:p8080 -m tcp -p tcp -j DNAT --to-destination 10.0.2.1:8080
		-A KUBE-SVC-2VJB64SDSIJUP5T6 -m comment --comment "ns2/svc2:p8080 cluster IP" -m tcp -p tcp -d 172.30.0.42 --dport 8080 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
		-A KUBE-SVC-2VJB64SDSIJUP5T6 -m comment --comment "ns2/svc2:p8080 -> 10.0.2.1:8080" -j KUBE-SEP-UHEGFW77JX3KXTOV
		-A KUBE-SVC-XPGD46QRK7WJZT7O -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
		-A KUBE-SVC-XPGD46QRK7WJZT7O -m comment --comment "ns1/svc1:p80 -> 10.0.1.1:80" -j KUBE-SEP-SNQ3ZNILQDEJNDQO
		COMMIT
		`)
	assertIPTablesRulesEqual(t, getLine(), true, expected, fp.iptablesData.String())

	rulesSynced := countRules(logger, utiliptables.TableNAT, expected)
	rulesSyncedMetric := countRulesFromLastSyncMetric(logger, utiliptables.TableNAT, string(fp.ipFamily))
	if rulesSyncedMetric != rulesSynced {
		t.Errorf("metric shows %d rules synced but iptables data shows %d", rulesSyncedMetric, rulesSynced)
	}

	rulesTotal := rulesSynced
	rulesTotalMetric := countRulesFromMetric(logger, utiliptables.TableNAT, string(fp.ipFamily))
	if rulesTotalMetric != rulesTotal {
		t.Errorf("metric shows %d rules total but expected %d", rulesTotalMetric, rulesTotal)
	}

	// 新增 Service 及其端点：同步只会重写新服务的 SVC 与 SEP 规则，
	// 已有服务的规则保持不变。
	makeServiceMap(fp,
		makeTestService("ns3", "svc3", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = "172.30.0.43"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
			}}
		}),
	)
	var eps3 *discovery.EndpointSlice
	populateEndpointSlices(fp,
		makeTestEndpointSlice("ns3", "svc3", 1, func(eps *discovery.EndpointSlice) {
			eps3 = eps
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.0.3.1"},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p80"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)
	fp.syncProxyRules()

	expected = dedent.Dedent(`
		*filter
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-EXTERNAL-SERVICES - [0:0]
		:KUBE-FIREWALL - [0:0]
		:KUBE-FORWARD - [0:0]
		:KUBE-PROXY-FIREWALL - [0:0]
		-A KUBE-FIREWALL -m comment --comment "block incoming localnet connections" -d 127.0.0.0/8 ! -s 127.0.0.0/8 -m conntrack ! --ctstate RELATED,ESTABLISHED,DNAT -j DROP
		-A KUBE-FORWARD -m conntrack --ctstate INVALID -m nfacct --nfacct-name ct_state_invalid_dropped_pkts -j DROP
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
		COMMIT
		*nat
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-MARK-MASQ - [0:0]
		:KUBE-POSTROUTING - [0:0]
		:KUBE-SEP-BSWRHOQ77KEXZLNL - [0:0]
		:KUBE-SVC-X27LE4BHSL4DOUIK - [0:0]
		-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 -j KUBE-SVC-XPGD46QRK7WJZT7O
		-A KUBE-SERVICES -m comment --comment "ns2/svc2:p8080 cluster IP" -m tcp -p tcp -d 172.30.0.42 --dport 8080 -j KUBE-SVC-2VJB64SDSIJUP5T6
		-A KUBE-SERVICES -m comment --comment "ns3/svc3:p80 cluster IP" -m tcp -p tcp -d 172.30.0.43 --dport 80 -j KUBE-SVC-X27LE4BHSL4DOUIK
		-A KUBE-SERVICES -m comment --comment "kubernetes service nodeports; NOTE: this must be the last rule in this chain" -m addrtype --dst-type LOCAL -j KUBE-NODEPORTS
		-A KUBE-MARK-MASQ -j MARK --or-mark 0x4000
		-A KUBE-POSTROUTING -m mark ! --mark 0x4000/0x4000 -j RETURN
		-A KUBE-POSTROUTING -j MARK --xor-mark 0x4000
		-A KUBE-POSTROUTING -m comment --comment "kubernetes service traffic requiring SNAT" -j MASQUERADE --random-fully
		-A KUBE-SEP-BSWRHOQ77KEXZLNL -m comment --comment ns3/svc3:p80 -s 10.0.3.1 -j KUBE-MARK-MASQ
		-A KUBE-SEP-BSWRHOQ77KEXZLNL -m comment --comment ns3/svc3:p80 -m tcp -p tcp -j DNAT --to-destination 10.0.3.1:80
		-A KUBE-SVC-X27LE4BHSL4DOUIK -m comment --comment "ns3/svc3:p80 cluster IP" -m tcp -p tcp -d 172.30.0.43 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
		-A KUBE-SVC-X27LE4BHSL4DOUIK -m comment --comment "ns3/svc3:p80 -> 10.0.3.1:80" -j KUBE-SEP-BSWRHOQ77KEXZLNL
		COMMIT
		`)
	assertIPTablesRulesEqual(t, getLine(), false, expected, fp.iptablesData.String())

	rulesSynced = countRules(logger, utiliptables.TableNAT, expected)
	rulesSyncedMetric = countRulesFromLastSyncMetric(logger, utiliptables.TableNAT, string(fp.ipFamily))
	if rulesSyncedMetric != rulesSynced {
		t.Errorf("metric shows %d rules synced but iptables data shows %d", rulesSyncedMetric, rulesSynced)
	}

	// 新增了 1 条 KUBE-SERVICES 规则、2 条 KUBE-SVC-X27LE4BHSL4DOUIK 规则，
	// 以及 2 条 KUBE-SEP-BSWRHOQ77KEXZLNL 规则。
	rulesTotal += 5
	rulesTotalMetric = countRulesFromMetric(logger, utiliptables.TableNAT, string(fp.ipFamily))
	if rulesTotalMetric != rulesTotal {
		t.Errorf("metric shows %d rules total but expected %d", rulesTotalMetric, rulesTotal)
	}

	// 删除 Service：其他 Service 的规则不受影响。
	fp.OnServiceDelete(svc2)
	fp.syncProxyRules()

	expected = dedent.Dedent(`
		*filter
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-EXTERNAL-SERVICES - [0:0]
		:KUBE-FIREWALL - [0:0]
		:KUBE-FORWARD - [0:0]
		:KUBE-PROXY-FIREWALL - [0:0]
		-A KUBE-FIREWALL -m comment --comment "block incoming localnet connections" -d 127.0.0.0/8 ! -s 127.0.0.0/8 -m conntrack ! --ctstate RELATED,ESTABLISHED,DNAT -j DROP
		-A KUBE-FORWARD -m conntrack --ctstate INVALID -m nfacct --nfacct-name ct_state_invalid_dropped_pkts -j DROP
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
		COMMIT
		*nat
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-MARK-MASQ - [0:0]
		:KUBE-POSTROUTING - [0:0]
		:KUBE-SEP-UHEGFW77JX3KXTOV - [0:0]
		:KUBE-SVC-2VJB64SDSIJUP5T6 - [0:0]
		-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 -j KUBE-SVC-XPGD46QRK7WJZT7O
		-A KUBE-SERVICES -m comment --comment "ns3/svc3:p80 cluster IP" -m tcp -p tcp -d 172.30.0.43 --dport 80 -j KUBE-SVC-X27LE4BHSL4DOUIK
		-A KUBE-SERVICES -m comment --comment "kubernetes service nodeports; NOTE: this must be the last rule in this chain" -m addrtype --dst-type LOCAL -j KUBE-NODEPORTS
		-A KUBE-MARK-MASQ -j MARK --or-mark 0x4000
		-A KUBE-POSTROUTING -m mark ! --mark 0x4000/0x4000 -j RETURN
		-A KUBE-POSTROUTING -j MARK --xor-mark 0x4000
		-A KUBE-POSTROUTING -m comment --comment "kubernetes service traffic requiring SNAT" -j MASQUERADE --random-fully
		-X KUBE-SEP-UHEGFW77JX3KXTOV
		-X KUBE-SVC-2VJB64SDSIJUP5T6
		COMMIT
		`)
	assertIPTablesRulesEqual(t, getLine(), false, expected, fp.iptablesData.String())

	rulesSynced = countRules(logger, utiliptables.TableNAT, expected)
	rulesSyncedMetric = countRulesFromLastSyncMetric(logger, utiliptables.TableNAT, string(fp.ipFamily))
	if rulesSyncedMetric != rulesSynced {
		t.Errorf("metric shows %d rules synced but iptables data shows %d", rulesSyncedMetric, rulesSynced)
	}

	// 删除了 1 条 KUBE-SERVICES 规则、2 条 KUBE-SVC-2VJB64SDSIJUP5T6 规则，
	// 以及 2 条 KUBE-SEP-UHEGFW77JX3KXTOV 规则。
	rulesTotal -= 5
	rulesTotalMetric = countRulesFromMetric(logger, utiliptables.TableNAT, string(fp.ipFamily))
	if rulesTotalMetric != rulesTotal {
		t.Errorf("metric shows %d rules total but expected %d", rulesTotalMetric, rulesTotal)
	}

	// 先新增 Service 并同步，再新增其端点：第一次同步除 REJECT 规则外是空操作，
	// 第二次同步才会创建新服务的规则。
	var svc4 *v1.Service
	makeServiceMap(fp,
		makeTestService("ns4", "svc4", func(svc *v1.Service) {
			svc4 = svc
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = "172.30.0.44"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
			}}
		}),
	)
	fp.syncProxyRules()
	expected = dedent.Dedent(`
		*filter
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-EXTERNAL-SERVICES - [0:0]
		:KUBE-FIREWALL - [0:0]
		:KUBE-FORWARD - [0:0]
		:KUBE-PROXY-FIREWALL - [0:0]
		-A KUBE-SERVICES -m comment --comment "ns4/svc4:p80 has no endpoints" -m tcp -p tcp -d 172.30.0.44 --dport 80 -j REJECT
		-A KUBE-FIREWALL -m comment --comment "block incoming localnet connections" -d 127.0.0.0/8 ! -s 127.0.0.0/8 -m conntrack ! --ctstate RELATED,ESTABLISHED,DNAT -j DROP
		-A KUBE-FORWARD -m conntrack --ctstate INVALID -m nfacct --nfacct-name ct_state_invalid_dropped_pkts -j DROP
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
		COMMIT
		*nat
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-MARK-MASQ - [0:0]
		:KUBE-POSTROUTING - [0:0]
		-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 -j KUBE-SVC-XPGD46QRK7WJZT7O
		-A KUBE-SERVICES -m comment --comment "ns3/svc3:p80 cluster IP" -m tcp -p tcp -d 172.30.0.43 --dport 80 -j KUBE-SVC-X27LE4BHSL4DOUIK
		-A KUBE-SERVICES -m comment --comment "kubernetes service nodeports; NOTE: this must be the last rule in this chain" -m addrtype --dst-type LOCAL -j KUBE-NODEPORTS
		-A KUBE-MARK-MASQ -j MARK --or-mark 0x4000
		-A KUBE-POSTROUTING -m mark ! --mark 0x4000/0x4000 -j RETURN
		-A KUBE-POSTROUTING -j MARK --xor-mark 0x4000
		-A KUBE-POSTROUTING -m comment --comment "kubernetes service traffic requiring SNAT" -j MASQUERADE --random-fully
		COMMIT
		`)
	assertIPTablesRulesEqual(t, getLine(), false, expected, fp.iptablesData.String())

	rulesSynced = countRules(logger, utiliptables.TableNAT, expected)
	rulesSyncedMetric = countRulesFromLastSyncMetric(logger, utiliptables.TableNAT, string(fp.ipFamily))
	if rulesSyncedMetric != rulesSynced {
		t.Errorf("metric shows %d rules synced but iptables data shows %d", rulesSyncedMetric, rulesSynced)
	}

	// REJECT 规则位于 filter 表而非 nat 表，因此 nat 规则数没有变化。
	rulesTotalMetric = countRulesFromMetric(logger, utiliptables.TableNAT, string(fp.ipFamily))
	if rulesTotalMetric != rulesTotal {
		t.Errorf("metric shows %d rules total but expected %d", rulesTotalMetric, rulesTotal)
	}

	populateEndpointSlices(fp,
		makeTestEndpointSlice("ns4", "svc4", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.0.4.1"},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p80"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)
	fp.syncProxyRules()
	expected = dedent.Dedent(`
		*filter
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-EXTERNAL-SERVICES - [0:0]
		:KUBE-FIREWALL - [0:0]
		:KUBE-FORWARD - [0:0]
		:KUBE-PROXY-FIREWALL - [0:0]
		-A KUBE-FIREWALL -m comment --comment "block incoming localnet connections" -d 127.0.0.0/8 ! -s 127.0.0.0/8 -m conntrack ! --ctstate RELATED,ESTABLISHED,DNAT -j DROP
		-A KUBE-FORWARD -m conntrack --ctstate INVALID -m nfacct --nfacct-name ct_state_invalid_dropped_pkts -j DROP
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
		COMMIT
		*nat
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-MARK-MASQ - [0:0]
		:KUBE-POSTROUTING - [0:0]
		:KUBE-SEP-AYCN5HPXMIRJNJXU - [0:0]
		:KUBE-SVC-4SW47YFZTEDKD3PK - [0:0]
		-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 -j KUBE-SVC-XPGD46QRK7WJZT7O
		-A KUBE-SERVICES -m comment --comment "ns3/svc3:p80 cluster IP" -m tcp -p tcp -d 172.30.0.43 --dport 80 -j KUBE-SVC-X27LE4BHSL4DOUIK
		-A KUBE-SERVICES -m comment --comment "ns4/svc4:p80 cluster IP" -m tcp -p tcp -d 172.30.0.44 --dport 80 -j KUBE-SVC-4SW47YFZTEDKD3PK
		-A KUBE-SERVICES -m comment --comment "kubernetes service nodeports; NOTE: this must be the last rule in this chain" -m addrtype --dst-type LOCAL -j KUBE-NODEPORTS
		-A KUBE-MARK-MASQ -j MARK --or-mark 0x4000
		-A KUBE-POSTROUTING -m mark ! --mark 0x4000/0x4000 -j RETURN
		-A KUBE-POSTROUTING -j MARK --xor-mark 0x4000
		-A KUBE-POSTROUTING -m comment --comment "kubernetes service traffic requiring SNAT" -j MASQUERADE --random-fully
		-A KUBE-SEP-AYCN5HPXMIRJNJXU -m comment --comment ns4/svc4:p80 -s 10.0.4.1 -j KUBE-MARK-MASQ
		-A KUBE-SEP-AYCN5HPXMIRJNJXU -m comment --comment ns4/svc4:p80 -m tcp -p tcp -j DNAT --to-destination 10.0.4.1:80
		-A KUBE-SVC-4SW47YFZTEDKD3PK -m comment --comment "ns4/svc4:p80 cluster IP" -m tcp -p tcp -d 172.30.0.44 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
		-A KUBE-SVC-4SW47YFZTEDKD3PK -m comment --comment "ns4/svc4:p80 -> 10.0.4.1:80" -j KUBE-SEP-AYCN5HPXMIRJNJXU
		COMMIT
		`)
	assertIPTablesRulesEqual(t, getLine(), false, expected, fp.iptablesData.String())

	rulesSynced = countRules(logger, utiliptables.TableNAT, expected)
	rulesSyncedMetric = countRulesFromLastSyncMetric(logger, utiliptables.TableNAT, string(fp.ipFamily))
	if rulesSyncedMetric != rulesSynced {
		t.Errorf("metric shows %d rules synced but iptables data shows %d", rulesSyncedMetric, rulesSynced)
	}

	// 新增了 1 条 KUBE-SERVICES 规则、2 条 KUBE-SVC-4SW47YFZTEDKD3PK 规则，
	// 以及 2 条 KUBE-SEP-AYCN5HPXMIRJNJXU 规则。
	rulesTotal += 5
	rulesTotalMetric = countRulesFromMetric(logger, utiliptables.TableNAT, string(fp.ipFamily))
	if rulesTotalMetric != rulesTotal {
		t.Errorf("metric shows %d rules total but expected %d", rulesTotalMetric, rulesTotal)
	}

	// 修改某 Service 的端点：其 SVC 与 SEP 链会被重写。
	eps3update := eps3.DeepCopy()
	eps3update.Endpoints[0].Addresses[0] = "10.0.3.2"
	fp.OnEndpointSliceUpdate(eps3, eps3update)
	fp.syncProxyRules()

	expected = dedent.Dedent(`
		*filter
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-EXTERNAL-SERVICES - [0:0]
		:KUBE-FIREWALL - [0:0]
		:KUBE-FORWARD - [0:0]
		:KUBE-PROXY-FIREWALL - [0:0]
		-A KUBE-FIREWALL -m comment --comment "block incoming localnet connections" -d 127.0.0.0/8 ! -s 127.0.0.0/8 -m conntrack ! --ctstate RELATED,ESTABLISHED,DNAT -j DROP
		-A KUBE-FORWARD -m conntrack --ctstate INVALID -m nfacct --nfacct-name ct_state_invalid_dropped_pkts -j DROP
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
		COMMIT
		*nat
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-MARK-MASQ - [0:0]
		:KUBE-POSTROUTING - [0:0]
		:KUBE-SEP-BSWRHOQ77KEXZLNL - [0:0]
		:KUBE-SEP-DKCFIS26GWF2WLWC - [0:0]
		:KUBE-SVC-X27LE4BHSL4DOUIK - [0:0]
		-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 -j KUBE-SVC-XPGD46QRK7WJZT7O
		-A KUBE-SERVICES -m comment --comment "ns3/svc3:p80 cluster IP" -m tcp -p tcp -d 172.30.0.43 --dport 80 -j KUBE-SVC-X27LE4BHSL4DOUIK
		-A KUBE-SERVICES -m comment --comment "ns4/svc4:p80 cluster IP" -m tcp -p tcp -d 172.30.0.44 --dport 80 -j KUBE-SVC-4SW47YFZTEDKD3PK
		-A KUBE-SERVICES -m comment --comment "kubernetes service nodeports; NOTE: this must be the last rule in this chain" -m addrtype --dst-type LOCAL -j KUBE-NODEPORTS
		-A KUBE-MARK-MASQ -j MARK --or-mark 0x4000
		-A KUBE-POSTROUTING -m mark ! --mark 0x4000/0x4000 -j RETURN
		-A KUBE-POSTROUTING -j MARK --xor-mark 0x4000
		-A KUBE-POSTROUTING -m comment --comment "kubernetes service traffic requiring SNAT" -j MASQUERADE --random-fully
		-A KUBE-SEP-DKCFIS26GWF2WLWC -m comment --comment ns3/svc3:p80 -s 10.0.3.2 -j KUBE-MARK-MASQ
		-A KUBE-SEP-DKCFIS26GWF2WLWC -m comment --comment ns3/svc3:p80 -m tcp -p tcp -j DNAT --to-destination 10.0.3.2:80
		-A KUBE-SVC-X27LE4BHSL4DOUIK -m comment --comment "ns3/svc3:p80 cluster IP" -m tcp -p tcp -d 172.30.0.43 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
		-A KUBE-SVC-X27LE4BHSL4DOUIK -m comment --comment "ns3/svc3:p80 -> 10.0.3.2:80" -j KUBE-SEP-DKCFIS26GWF2WLWC
		-X KUBE-SEP-BSWRHOQ77KEXZLNL
		COMMIT
		`)
	assertIPTablesRulesEqual(t, getLine(), false, expected, fp.iptablesData.String())

	rulesSynced = countRules(logger, utiliptables.TableNAT, expected)
	rulesSyncedMetric = countRulesFromLastSyncMetric(logger, utiliptables.TableNAT, string(fp.ipFamily))
	if rulesSyncedMetric != rulesSynced {
		t.Errorf("metric shows %d rules synced but iptables data shows %d", rulesSyncedMetric, rulesSynced)
	}

	// 规则被重写但总数不变，因此还需比对内容以确认更新生效。
	rulesTotalMetric = countRulesFromMetric(logger, utiliptables.TableNAT, string(fp.ipFamily))
	if rulesTotalMetric != rulesTotal {
		t.Errorf("metric shows %d rules total but expected %d", rulesTotalMetric, rulesTotal)
	}

	// 为 Service 增加一个端点：SVC 与 SEP 链会被重写。
	eps3update2 := eps3update.DeepCopy()
	eps3update2.Endpoints = append(eps3update2.Endpoints, discovery.Endpoint{Addresses: []string{"10.0.3.3"}})
	fp.OnEndpointSliceUpdate(eps3update, eps3update2)
	fp.syncProxyRules()

	expected = dedent.Dedent(`
		*filter
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-EXTERNAL-SERVICES - [0:0]
		:KUBE-FIREWALL - [0:0]
		:KUBE-FORWARD - [0:0]
		:KUBE-PROXY-FIREWALL - [0:0]
		-A KUBE-FIREWALL -m comment --comment "block incoming localnet connections" -d 127.0.0.0/8 ! -s 127.0.0.0/8 -m conntrack ! --ctstate RELATED,ESTABLISHED,DNAT -j DROP
		-A KUBE-FORWARD -m conntrack --ctstate INVALID -m nfacct --nfacct-name ct_state_invalid_dropped_pkts -j DROP
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
		COMMIT
		*nat
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-MARK-MASQ - [0:0]
		:KUBE-POSTROUTING - [0:0]
		:KUBE-SEP-DKCFIS26GWF2WLWC - [0:0]
		:KUBE-SEP-JVVZVJ7BSEPPRNBS - [0:0]
		:KUBE-SVC-X27LE4BHSL4DOUIK - [0:0]
		-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 -j KUBE-SVC-XPGD46QRK7WJZT7O
		-A KUBE-SERVICES -m comment --comment "ns3/svc3:p80 cluster IP" -m tcp -p tcp -d 172.30.0.43 --dport 80 -j KUBE-SVC-X27LE4BHSL4DOUIK
		-A KUBE-SERVICES -m comment --comment "ns4/svc4:p80 cluster IP" -m tcp -p tcp -d 172.30.0.44 --dport 80 -j KUBE-SVC-4SW47YFZTEDKD3PK
		-A KUBE-SERVICES -m comment --comment "kubernetes service nodeports; NOTE: this must be the last rule in this chain" -m addrtype --dst-type LOCAL -j KUBE-NODEPORTS
		-A KUBE-MARK-MASQ -j MARK --or-mark 0x4000
		-A KUBE-POSTROUTING -m mark ! --mark 0x4000/0x4000 -j RETURN
		-A KUBE-POSTROUTING -j MARK --xor-mark 0x4000
		-A KUBE-POSTROUTING -m comment --comment "kubernetes service traffic requiring SNAT" -j MASQUERADE --random-fully
		-A KUBE-SEP-DKCFIS26GWF2WLWC -m comment --comment ns3/svc3:p80 -s 10.0.3.2 -j KUBE-MARK-MASQ
		-A KUBE-SEP-DKCFIS26GWF2WLWC -m comment --comment ns3/svc3:p80 -m tcp -p tcp -j DNAT --to-destination 10.0.3.2:80
		-A KUBE-SEP-JVVZVJ7BSEPPRNBS -m comment --comment ns3/svc3:p80 -s 10.0.3.3 -j KUBE-MARK-MASQ
		-A KUBE-SEP-JVVZVJ7BSEPPRNBS -m comment --comment ns3/svc3:p80 -m tcp -p tcp -j DNAT --to-destination 10.0.3.3:80
		-A KUBE-SVC-X27LE4BHSL4DOUIK -m comment --comment "ns3/svc3:p80 cluster IP" -m tcp -p tcp -d 172.30.0.43 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
		-A KUBE-SVC-X27LE4BHSL4DOUIK -m comment --comment "ns3/svc3:p80 -> 10.0.3.2:80" -m statistic --mode random --probability 0.5000000000 -j KUBE-SEP-DKCFIS26GWF2WLWC
		-A KUBE-SVC-X27LE4BHSL4DOUIK -m comment --comment "ns3/svc3:p80 -> 10.0.3.3:80" -j KUBE-SEP-JVVZVJ7BSEPPRNBS
		COMMIT
		`)
	assertIPTablesRulesEqual(t, getLine(), false, expected, fp.iptablesData.String())

	rulesSynced = countRules(logger, utiliptables.TableNAT, expected)
	rulesSyncedMetric = countRulesFromLastSyncMetric(logger, utiliptables.TableNAT, string(fp.ipFamily))
	if rulesSyncedMetric != rulesSynced {
		t.Errorf("metric shows %d rules synced but iptables data shows %d", rulesSyncedMetric, rulesSynced)
	}

	// 新增了 2 条 KUBE-SEP-JVVZVJ7BSEPPRNBS 规则和 1 条跳转到新 SEP 链的
	// KUBE-SVC-X27LE4BHSL4DOUIK 规则；svc3 的其他规则也被重写，
	// 但规则总数不变。
	rulesTotal += 3
	rulesTotalMetric = countRulesFromMetric(logger, utiliptables.TableNAT, string(fp.ipFamily))
	if rulesTotalMetric != rulesTotal {
		t.Errorf("metric shows %d rules total but expected %d", rulesTotalMetric, rulesTotal)
	}

	// 无变更时再次同步：不应重写任何 SVC 或 SEP 链。
	fp.syncProxyRules()

	expected = dedent.Dedent(`
		*filter
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-EXTERNAL-SERVICES - [0:0]
		:KUBE-FIREWALL - [0:0]
		:KUBE-FORWARD - [0:0]
		:KUBE-PROXY-FIREWALL - [0:0]
		-A KUBE-FIREWALL -m comment --comment "block incoming localnet connections" -d 127.0.0.0/8 ! -s 127.0.0.0/8 -m conntrack ! --ctstate RELATED,ESTABLISHED,DNAT -j DROP
		-A KUBE-FORWARD -m conntrack --ctstate INVALID -m nfacct --nfacct-name ct_state_invalid_dropped_pkts -j DROP
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
		COMMIT
		*nat
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-MARK-MASQ - [0:0]
		:KUBE-POSTROUTING - [0:0]
		-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 -j KUBE-SVC-XPGD46QRK7WJZT7O
		-A KUBE-SERVICES -m comment --comment "ns3/svc3:p80 cluster IP" -m tcp -p tcp -d 172.30.0.43 --dport 80 -j KUBE-SVC-X27LE4BHSL4DOUIK
		-A KUBE-SERVICES -m comment --comment "ns4/svc4:p80 cluster IP" -m tcp -p tcp -d 172.30.0.44 --dport 80 -j KUBE-SVC-4SW47YFZTEDKD3PK
		-A KUBE-SERVICES -m comment --comment "kubernetes service nodeports; NOTE: this must be the last rule in this chain" -m addrtype --dst-type LOCAL -j KUBE-NODEPORTS
		-A KUBE-MARK-MASQ -j MARK --or-mark 0x4000
		-A KUBE-POSTROUTING -m mark ! --mark 0x4000/0x4000 -j RETURN
		-A KUBE-POSTROUTING -j MARK --xor-mark 0x4000
		-A KUBE-POSTROUTING -m comment --comment "kubernetes service traffic requiring SNAT" -j MASQUERADE --random-fully
		COMMIT
		`)
	assertIPTablesRulesEqual(t, getLine(), false, expected, fp.iptablesData.String())

	rulesSynced = countRules(logger, utiliptables.TableNAT, expected)
	rulesSyncedMetric = countRulesFromLastSyncMetric(logger, utiliptables.TableNAT, string(fp.ipFamily))
	if rulesSyncedMetric != rulesSynced {
		t.Errorf("metric shows %d rules synced but iptables data shows %d", rulesSyncedMetric, rulesSynced)
	}

	// 无变更。
	rulesTotalMetric = countRulesFromMetric(logger, utiliptables.TableNAT, string(fp.ipFamily))
	if rulesTotalMetric != rulesTotal {
		t.Errorf("metric shows %d rules total but expected %d", rulesTotalMetric, rulesTotal)
	}

	// 制造一次部分同步失败，并确认能够正确恢复。
	if fp.needFullSync {
		t.Fatalf("Proxier unexpectedly already needs a full sync?")
	}
	partialRestoreFailures, err := testutil.GetCounterMetricValue(metrics.IPTablesPartialRestoreFailuresTotal.WithLabelValues(string(fp.ipFamily)))
	if err != nil {
		t.Fatalf("Could not get partial restore failures metric: %v", err)
	}
	if partialRestoreFailures != 0.0 {
		t.Errorf("Already did a partial resync? Something failed earlier!")
	}

	// 添加一条从 svc3 服务链跳转到 svc4 端点的规则，然后尝试删除 svc4。
	// 部分同步不会重写 svc3 的规则，若按部分恢复执行会留下悬空跳转，
	// 因此该操作应失败；作为响应，proxier 会排队一次全量同步，
	// 全量同步会重写 svc3 的规则，从而成功删除 svc4。
	// 部分同步不会重写 svc3 的规则，若按部分恢复执行会留下悬空跳转，
	// 因此该操作应失败；全量同步重写 svc3 的规则后才会成功。
	//
	// 这个场景很荒谬，但也只能如此：部分同步失败本应不可能发生；
	// 若存在不荒谬的触发场景，那就是 bug，应予以修复。
	if _, err := fp.iptables.ChainExists(utiliptables.TableNAT, utiliptables.Chain("KUBE-SEP-AYCN5HPXMIRJNJXU")); err != nil {
		t.Fatalf("svc4's endpoint chain unexpected already does not exist!")
	}
	if _, err := fp.iptables.EnsureRule(utiliptables.Append, utiliptables.TableNAT, utiliptables.Chain("KUBE-SVC-X27LE4BHSL4DOUIK"), "-j", "KUBE-SEP-AYCN5HPXMIRJNJXU"); err != nil {
		t.Fatalf("Could not add bad iptables rule: %v", err)
	}

	fp.OnServiceDelete(svc4)
	fp.syncProxyRules()

	if _, err := fp.iptables.ChainExists(utiliptables.TableNAT, utiliptables.Chain("KUBE-SEP-AYCN5HPXMIRJNJXU")); err != nil {
		t.Errorf("svc4's endpoint chain was successfully deleted despite dangling references!")
	}
	if !fp.needFullSync {
		t.Errorf("Proxier did not fail on previous partial resync?")
	}
	updatedPartialRestoreFailures, err := testutil.GetCounterMetricValue(metrics.IPTablesPartialRestoreFailuresTotal.WithLabelValues(string(fp.ipFamily)))
	if err != nil {
		t.Errorf("Could not get partial restore failures metric: %v", err)
	}
	if updatedPartialRestoreFailures != partialRestoreFailures+1.0 {
		t.Errorf("Partial restore failures metric was not incremented after failed partial resync (expected %.02f, got %.02f)", partialRestoreFailures+1.0, updatedPartialRestoreFailures)
	}

	// 重试时应执行全量同步并成功删除 svc4。
	fp.syncProxyRules()

	expected = dedent.Dedent(`
		*filter
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-EXTERNAL-SERVICES - [0:0]
		:KUBE-FIREWALL - [0:0]
		:KUBE-FORWARD - [0:0]
		:KUBE-PROXY-FIREWALL - [0:0]
		-A KUBE-FIREWALL -m comment --comment "block incoming localnet connections" -d 127.0.0.0/8 ! -s 127.0.0.0/8 -m conntrack ! --ctstate RELATED,ESTABLISHED,DNAT -j DROP
		-A KUBE-FORWARD -m conntrack --ctstate INVALID -m nfacct --nfacct-name ct_state_invalid_dropped_pkts -j DROP
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
		COMMIT
		*nat
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-MARK-MASQ - [0:0]
		:KUBE-POSTROUTING - [0:0]
		:KUBE-SEP-AYCN5HPXMIRJNJXU - [0:0]
		:KUBE-SEP-DKCFIS26GWF2WLWC - [0:0]
		:KUBE-SEP-JVVZVJ7BSEPPRNBS - [0:0]
		:KUBE-SEP-SNQ3ZNILQDEJNDQO - [0:0]
		:KUBE-SVC-4SW47YFZTEDKD3PK - [0:0]
		:KUBE-SVC-X27LE4BHSL4DOUIK - [0:0]
		:KUBE-SVC-XPGD46QRK7WJZT7O - [0:0]
		-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 -j KUBE-SVC-XPGD46QRK7WJZT7O
		-A KUBE-SERVICES -m comment --comment "ns3/svc3:p80 cluster IP" -m tcp -p tcp -d 172.30.0.43 --dport 80 -j KUBE-SVC-X27LE4BHSL4DOUIK
		-A KUBE-SERVICES -m comment --comment "kubernetes service nodeports; NOTE: this must be the last rule in this chain" -m addrtype --dst-type LOCAL -j KUBE-NODEPORTS
		-A KUBE-MARK-MASQ -j MARK --or-mark 0x4000
		-A KUBE-POSTROUTING -m mark ! --mark 0x4000/0x4000 -j RETURN
		-A KUBE-POSTROUTING -j MARK --xor-mark 0x4000
		-A KUBE-POSTROUTING -m comment --comment "kubernetes service traffic requiring SNAT" -j MASQUERADE --random-fully
		-A KUBE-SEP-DKCFIS26GWF2WLWC -m comment --comment ns3/svc3:p80 -s 10.0.3.2 -j KUBE-MARK-MASQ
		-A KUBE-SEP-DKCFIS26GWF2WLWC -m comment --comment ns3/svc3:p80 -m tcp -p tcp -j DNAT --to-destination 10.0.3.2:80
		-A KUBE-SEP-JVVZVJ7BSEPPRNBS -m comment --comment ns3/svc3:p80 -s 10.0.3.3 -j KUBE-MARK-MASQ
		-A KUBE-SEP-JVVZVJ7BSEPPRNBS -m comment --comment ns3/svc3:p80 -m tcp -p tcp -j DNAT --to-destination 10.0.3.3:80
		-A KUBE-SEP-SNQ3ZNILQDEJNDQO -m comment --comment ns1/svc1:p80 -s 10.0.1.1 -j KUBE-MARK-MASQ
		-A KUBE-SEP-SNQ3ZNILQDEJNDQO -m comment --comment ns1/svc1:p80 -m tcp -p tcp -j DNAT --to-destination 10.0.1.1:80
		-A KUBE-SVC-X27LE4BHSL4DOUIK -m comment --comment "ns3/svc3:p80 cluster IP" -m tcp -p tcp -d 172.30.0.43 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
		-A KUBE-SVC-X27LE4BHSL4DOUIK -m comment --comment "ns3/svc3:p80 -> 10.0.3.2:80" -m statistic --mode random --probability 0.5000000000 -j KUBE-SEP-DKCFIS26GWF2WLWC
		-A KUBE-SVC-X27LE4BHSL4DOUIK -m comment --comment "ns3/svc3:p80 -> 10.0.3.3:80" -j KUBE-SEP-JVVZVJ7BSEPPRNBS
		-A KUBE-SVC-XPGD46QRK7WJZT7O -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
		-A KUBE-SVC-XPGD46QRK7WJZT7O -m comment --comment "ns1/svc1:p80 -> 10.0.1.1:80" -j KUBE-SEP-SNQ3ZNILQDEJNDQO
		-X KUBE-SEP-AYCN5HPXMIRJNJXU
		-X KUBE-SVC-4SW47YFZTEDKD3PK
		COMMIT
		`)
	assertIPTablesRulesEqual(t, getLine(), false, expected, fp.iptablesData.String())

	rulesSynced = countRules(logger, utiliptables.TableNAT, expected)
	rulesSyncedMetric = countRulesFromLastSyncMetric(logger, utiliptables.TableNAT, string(fp.ipFamily))
	if rulesSyncedMetric != rulesSynced {
		t.Errorf("metric shows %d rules synced but iptables data shows %d", rulesSyncedMetric, rulesSynced)
	}

	// 删除了 1 条 KUBE-SERVICES 规则、2 条 KUBE-SVC-4SW47YFZTEDKD3PK 规则，
	// 以及 2 条 KUBE-SEP-AYCN5HPXMIRJNJXU 规则。
	rulesTotal -= 5
	rulesTotalMetric = countRulesFromMetric(logger, utiliptables.TableNAT, string(fp.ipFamily))
	if rulesTotalMetric != rulesTotal {
		t.Errorf("metric shows %d rules total but expected %d", rulesTotalMetric, rulesTotal)
	}
}

// 分别构造有/无端点的服务，检查无端点计数指标按服务而非端口累加。
// TestNoEndpointsMetric 验证无端点服务的指标统计与 REJECT 规则一致。
func TestNoEndpointsMetric(t *testing.T) {
	type endpoint struct {
		ip       string
		nodeName string
	}

	metrics.RegisterMetrics(kubeproxyconfig.ProxyModeIPTables)
	testCases := []struct {
		name                                                string
		internalTrafficPolicy                               *v1.ServiceInternalTrafficPolicy
		externalTrafficPolicy                               v1.ServiceExternalTrafficPolicy
		endpoints                                           []endpoint
		expectedSyncProxyRulesNoLocalEndpointsTotalInternal int
		expectedSyncProxyRulesNoLocalEndpointsTotalExternal int
	}{
		{
			name:                  "internalTrafficPolicy is set and there are local endpoints",
			internalTrafficPolicy: ptr.To(v1.ServiceInternalTrafficPolicyLocal),
			endpoints: []endpoint{
				{"10.0.1.1", testNodeName},
				{"10.0.1.2", "node1"},
				{"10.0.1.3", "node2"},
				// 多个端口的服务只应计一次无端点，避免把端口数误当成服务数。
			},
		},
		{
			name:                  "externalTrafficPolicy is set and there are local endpoints",
			externalTrafficPolicy: v1.ServiceExternalTrafficPolicyLocal,
			endpoints: []endpoint{
				{"10.0.1.1", testNodeName},
				{"10.0.1.2", "node1"},
				{"10.0.1.3", "node2"},
				// 逐场景更新 EndpointSlice 并同步，再读取 gauge 指标进行核对。
			},
		},
		{
			name:                  "both policies are set and there are local endpoints",
			internalTrafficPolicy: ptr.To(v1.ServiceInternalTrafficPolicyLocal),
			externalTrafficPolicy: v1.ServiceExternalTrafficPolicyLocal,
			endpoints: []endpoint{
				{"10.0.1.1", testNodeName},
				{"10.0.1.2", "node1"},
				{"10.0.1.3", "node2"},
			},
		},
		{
			name:                  "internalTrafficPolicy is set and there are no local endpoints",
			internalTrafficPolicy: ptr.To(v1.ServiceInternalTrafficPolicyLocal),
			endpoints: []endpoint{
				{"10.0.1.1", "node0"},
				{"10.0.1.2", "node1"},
				{"10.0.1.3", "node2"},
			},
			expectedSyncProxyRulesNoLocalEndpointsTotalInternal: 1,
		},
		{
			name:                  "externalTrafficPolicy is set and there are no local endpoints",
			externalTrafficPolicy: v1.ServiceExternalTrafficPolicyLocal,
			endpoints: []endpoint{
				{"10.0.1.1", "node0"},
				{"10.0.1.2", "node1"},
				{"10.0.1.3", "node2"},
			},
			expectedSyncProxyRulesNoLocalEndpointsTotalExternal: 1,
		},
		{
			name:                  "both policies are set and there are no local endpoints",
			internalTrafficPolicy: ptr.To(v1.ServiceInternalTrafficPolicyLocal),
			externalTrafficPolicy: v1.ServiceExternalTrafficPolicyLocal,
			endpoints: []endpoint{
				{"10.0.1.1", "node0"},
				{"10.0.1.2", "node1"},
				{"10.0.1.3", "node2"},
			},
			expectedSyncProxyRulesNoLocalEndpointsTotalInternal: 1,
			expectedSyncProxyRulesNoLocalEndpointsTotalExternal: 1,
		},
		{
			name:                  "both policies are set and there are no endpoints at all",
			internalTrafficPolicy: ptr.To(v1.ServiceInternalTrafficPolicyLocal),
			externalTrafficPolicy: v1.ServiceExternalTrafficPolicyLocal,
			endpoints:             []endpoint{},
			expectedSyncProxyRulesNoLocalEndpointsTotalInternal: 0,
			expectedSyncProxyRulesNoLocalEndpointsTotalExternal: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ipt := iptablestest.NewFake()
			fp := NewFakeProxier(ipt)
			fp.OnServiceSynced()
			fp.OnEndpointSlicesSynced()

			serviceName := "svc1"
			namespaceName := "ns1"

			svc := &v1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: serviceName, Namespace: namespaceName},
				Spec: v1.ServiceSpec{
					ClusterIP: "172.30.1.1",
					Selector:  map[string]string{"foo": "bar"},
					Ports:     []v1.ServicePort{{Name: "", Port: 80, Protocol: v1.ProtocolTCP, NodePort: 123}},
				},
			}
			if tc.internalTrafficPolicy != nil {
				svc.Spec.InternalTrafficPolicy = tc.internalTrafficPolicy
			}
			if tc.externalTrafficPolicy != "" {
				svc.Spec.Type = v1.ServiceTypeNodePort
				svc.Spec.ExternalTrafficPolicy = tc.externalTrafficPolicy
			}

			fp.OnServiceAdd(svc)

			endpointSlice := &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-1", serviceName),
					Namespace: namespaceName,
					Labels:    map[string]string{discovery.LabelServiceName: serviceName},
				},
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}},
				AddressType: discovery.AddressTypeIPv4,
			}
			for _, ep := range tc.endpoints {
				endpointSlice.Endpoints = append(endpointSlice.Endpoints, discovery.Endpoint{
					Addresses:  []string{ep.ip},
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
					NodeName:   ptr.To(ep.nodeName),
				})
			}

			fp.OnEndpointSliceAdd(endpointSlice)
			fp.syncProxyRules()
			syncProxyRulesNoLocalEndpointsTotalInternal, err := testutil.GetGaugeMetricValue(metrics.SyncProxyRulesNoLocalEndpointsTotal.WithLabelValues("internal", string(fp.ipFamily)))
			if err != nil {
				t.Errorf("failed to get %s value, err: %v", metrics.SyncProxyRulesNoLocalEndpointsTotal.Name, err)
			}

			if tc.expectedSyncProxyRulesNoLocalEndpointsTotalInternal != int(syncProxyRulesNoLocalEndpointsTotalInternal) {
				t.Errorf("sync_proxy_rules_no_endpoints_total metric mismatch(internal): got=%d, expected %d", int(syncProxyRulesNoLocalEndpointsTotalInternal), tc.expectedSyncProxyRulesNoLocalEndpointsTotalInternal)
			}

			syncProxyRulesNoLocalEndpointsTotalExternal, err := testutil.GetGaugeMetricValue(metrics.SyncProxyRulesNoLocalEndpointsTotal.WithLabelValues("external", string(fp.ipFamily)))
			if err != nil {
				t.Errorf("failed to get %s value(external), err: %v", metrics.SyncProxyRulesNoLocalEndpointsTotal.Name, err)
			}

			if tc.expectedSyncProxyRulesNoLocalEndpointsTotalExternal != int(syncProxyRulesNoLocalEndpointsTotalExternal) {
				t.Errorf("sync_proxy_rules_no_endpoints_total metric mismatch(internal): got=%d, expected %d", int(syncProxyRulesNoLocalEndpointsTotalExternal), tc.expectedSyncProxyRulesNoLocalEndpointsTotalExternal)
			}
		})
	}
}

// 验证 Proxy 模式下仅 LB IP 自身的规则存在，不会为 ingress 地址生成额外规则。

// TestLoadBalancerIngressRouteTypeProxy 验证 ingress 路由模式为 Proxy 时 LoadBalancer 的规则生成。
func TestLoadBalancerIngressRouteTypeProxy(t *testing.T) {
	testCases := []struct {
		name         string
		svcIP        string
		svcLBIP      string
		ipMode       *v1.LoadBalancerIPMode
		expectedRule bool
	}{
		{
			name:         "ipMode Proxy",
			svcIP:        "10.20.30.41",
			svcLBIP:      "1.2.3.4",
			ipMode:       ptr.To(v1.LoadBalancerIPModeProxy),
			expectedRule: false,
		},
		{
			name:         "ipMode VIP",
			svcIP:        "10.20.30.42",
			svcLBIP:      "1.2.3.5",
			ipMode:       ptr.To(v1.LoadBalancerIPModeVIP),
			expectedRule: true,
		},
		{
			name:         "ipMode nil",
			svcIP:        "10.20.30.43",
			svcLBIP:      "1.2.3.6",
			ipMode:       nil,
			expectedRule: true,
			// 对每个 LB IP 单独建服务并同步，避免相互干扰规则检查。
		},
	}

	svcPort := 80
	svcNodePort := 3001
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ipt := iptablestest.NewFake()
			fp := NewFakeProxier(ipt)
			makeServiceMap(fp,
				makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
					svc.Spec.Type = "LoadBalancer"
					svc.Spec.ClusterIP = testCase.svcIP
					svc.Spec.Ports = []v1.ServicePort{{
						Name:     svcPortName.Port,
						Port:     int32(svcPort),
						Protocol: v1.ProtocolTCP,
						NodePort: int32(svcNodePort),
					}}
					svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
						IP:     testCase.svcLBIP,
						IPMode: testCase.ipMode,
					}}
				}),
			)

			populateEndpointSlices(fp,
				makeTestEndpointSlice("ns1", "svc1", 1, func(eps *discovery.EndpointSlice) {
					eps.AddressType = discovery.AddressTypeIPv4
					eps.Endpoints = []discovery.Endpoint{{
						Addresses: []string{"10.180.0.1"},
					}}
					eps.Ports = []discovery.EndpointPort{{
						Name:     ptr.To("p80"),
						Port:     ptr.To[int32](80),
						Protocol: ptr.To(v1.ProtocolTCP),
					}}
				}),
			)

			fp.syncProxyRules()

			c, _ := ipt.Dump.GetChain(utiliptables.TableNAT, kubeServicesChain)
			ruleExists := false
			for _, r := range c.Rules {
				if r.DestinationAddress != nil && r.DestinationAddress.Value == testCase.svcLBIP {
					ruleExists = true
				}
			}
			if ruleExists != testCase.expectedRule {
				t.Errorf("unexpected rule for %s", testCase.svcLBIP)
			}
		})
	}
}

// 用非法 IP/CIDR 构造 Service 与 EndpointSlice，检查生成的规则仍然合法。
// 会被修正为合法值。
// 用非法 IP/CIDR 构造 Service 与 EndpointSlice，检查生成的规则仍然合法。
// 不合法的 IP 会被规范化为合法地址，否则 iptables-restore 会拒绝整批规则。
func TestBadIPs(t *testing.T) {
	ipt := iptablestest.NewFake()
	fp := NewFakeProxier(ipt)
	metrics.RegisterMetrics(kubeproxyconfig.ProxyModeIPTables)

	makeServiceMap(fp,
		makeTestService("ns1", "svc1", func(svc *v1.Service) {
			svc.Spec.Type = "LoadBalancer"
			svc.Spec.ClusterIP = "172.30.0.041"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
				NodePort: 3001,
			}}
			svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
				IP: "1.2.3.004",
			}}
			svc.Spec.ExternalIPs = []string{"192.168.099.022"}
			svc.Spec.LoadBalancerSourceRanges = []string{"203.0.113.000/025"}
		}),
	)
	populateEndpointSlices(fp,
		makeTestEndpointSlice("ns1", "svc1", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.180.00.001"},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p80"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	fp.syncProxyRules()

	expected := dedent.Dedent(`
		*filter
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-EXTERNAL-SERVICES - [0:0]
		:KUBE-FIREWALL - [0:0]
		:KUBE-FORWARD - [0:0]
		:KUBE-PROXY-FIREWALL - [0:0]
		-A KUBE-FIREWALL -m comment --comment "block incoming localnet connections" -d 127.0.0.0/8 ! -s 127.0.0.0/8 -m conntrack ! --ctstate RELATED,ESTABLISHED,DNAT -j DROP
		-A KUBE-FORWARD -m conntrack --ctstate INVALID -m nfacct --nfacct-name ct_state_invalid_dropped_pkts -j DROP
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding rules" -m mark --mark 0x4000/0x4000 -j ACCEPT
		-A KUBE-FORWARD -m comment --comment "kubernetes forwarding conntrack rule" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
		-A KUBE-PROXY-FIREWALL -m comment --comment "ns1/svc1:p80 traffic not accepted by KUBE-FW-XPGD46QRK7WJZT7O" -m tcp -p tcp -d 1.2.3.4 --dport 80 -j DROP
		COMMIT
		*nat
		:KUBE-NODEPORTS - [0:0]
		:KUBE-SERVICES - [0:0]
		:KUBE-EXT-XPGD46QRK7WJZT7O - [0:0]
		:KUBE-FW-XPGD46QRK7WJZT7O - [0:0]
		:KUBE-MARK-MASQ - [0:0]
		:KUBE-POSTROUTING - [0:0]
		:KUBE-SEP-SXIVWICOYRO3J4NJ - [0:0]
		:KUBE-SVC-XPGD46QRK7WJZT7O - [0:0]
		-A KUBE-NODEPORTS -m comment --comment ns1/svc1:p80 -m tcp -p tcp -d 127.0.0.0/8 --dport 3001 -m nfacct --nfacct-name localhost_nps_accepted_pkts -j KUBE-EXT-XPGD46QRK7WJZT7O
		-A KUBE-NODEPORTS -m comment --comment ns1/svc1:p80 -m tcp -p tcp --dport 3001 -j KUBE-EXT-XPGD46QRK7WJZT7O
		-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 -j KUBE-SVC-XPGD46QRK7WJZT7O
		-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 external IP" -m tcp -p tcp -d 192.168.99.22 --dport 80 -j KUBE-EXT-XPGD46QRK7WJZT7O
		-A KUBE-SERVICES -m comment --comment "ns1/svc1:p80 loadbalancer IP" -m tcp -p tcp -d 1.2.3.4 --dport 80 -j KUBE-FW-XPGD46QRK7WJZT7O
		-A KUBE-SERVICES -m comment --comment "kubernetes service nodeports; NOTE: this must be the last rule in this chain" -m addrtype --dst-type LOCAL -j KUBE-NODEPORTS
		-A KUBE-EXT-XPGD46QRK7WJZT7O -m comment --comment "masquerade traffic for ns1/svc1:p80 external destinations" -j KUBE-MARK-MASQ
		-A KUBE-EXT-XPGD46QRK7WJZT7O -j KUBE-SVC-XPGD46QRK7WJZT7O
		-A KUBE-FW-XPGD46QRK7WJZT7O -m comment --comment "ns1/svc1:p80 loadbalancer IP" -s 203.0.113.0/25 -j KUBE-EXT-XPGD46QRK7WJZT7O
		-A KUBE-FW-XPGD46QRK7WJZT7O -m comment --comment "other traffic to ns1/svc1:p80 will be dropped by KUBE-PROXY-FIREWALL"
		-A KUBE-MARK-MASQ -j MARK --or-mark 0x4000
		-A KUBE-POSTROUTING -m mark ! --mark 0x4000/0x4000 -j RETURN
		-A KUBE-POSTROUTING -j MARK --xor-mark 0x4000
		-A KUBE-POSTROUTING -m comment --comment "kubernetes service traffic requiring SNAT" -j MASQUERADE --random-fully
		-A KUBE-SEP-SXIVWICOYRO3J4NJ -m comment --comment ns1/svc1:p80 -s 10.180.0.1 -j KUBE-MARK-MASQ
		-A KUBE-SEP-SXIVWICOYRO3J4NJ -m comment --comment ns1/svc1:p80 -m tcp -p tcp -j DNAT --to-destination 10.180.0.1:80
		-A KUBE-SVC-XPGD46QRK7WJZT7O -m comment --comment "ns1/svc1:p80 cluster IP" -m tcp -p tcp -d 172.30.0.41 --dport 80 ! -s 10.0.0.0/8 -j KUBE-MARK-MASQ
		-A KUBE-SVC-XPGD46QRK7WJZT7O -m comment --comment "ns1/svc1:p80 -> 10.180.0.1:80" -j KUBE-SEP-SXIVWICOYRO3J4NJ
		COMMIT
		`)

	assertIPTablesRulesEqual(t, getLine(), true, expected, fp.iptablesData.String())
}
