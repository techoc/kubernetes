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

package dns

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/record"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/test/utils/ktesting"
	netutils "k8s.io/utils/net"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	// testHostNameserver 与 testHostDomain 也被 dns_windows_test.go 复用。
	testHostNameserver = "8.8.8.8"
	testHostDomain     = "host.domain"
	// fetchEvent 以非阻塞方式取出 FakeRecorder 中的第一条事件，队列为空时返回空串。
	fetchEvent = func(recorder *record.FakeRecorder) string {
		select {
		case event := <-recorder.Events:
			return event
		default:
			return ""
		}
	}
)

// TestParseResolvConf 覆盖 resolv.conf 解析：忽略空行与注释，后出现的同名条目覆盖先前的，
// 空的 nameserver/search 条目视为错误。
func TestParseResolvConf(t *testing.T) {
	testCases := []struct {
		data        string
		nameservers []string
		searches    []string
		options     []string
		isErr       bool
	}{
		{"", []string{}, []string{}, []string{}, false},
		{" ", []string{}, []string{}, []string{}, false},
		{"\n", []string{}, []string{}, []string{}, false},
		{"\t\n\t", []string{}, []string{}, []string{}, false},
		{"#comment\n", []string{}, []string{}, []string{}, false},
		{" #comment\n", []string{}, []string{}, []string{}, false},
		{"#comment\n#comment", []string{}, []string{}, []string{}, false},
		{"#comment\nnameserver", []string{}, []string{}, []string{}, true},                           // nameserver 缺省值
		{"#comment\nnameserver\nsearch", []string{}, []string{}, []string{}, true},                   // nameserver 与 search 均为空
		{"#comment\nnameserver 1.2.3.4\nsearch", []string{"1.2.3.4"}, []string{}, []string{}, false}, // 指定了 nameserver，search 为空
		{"nameserver 1.2.3.4", []string{"1.2.3.4"}, []string{}, []string{}, false},
		{" nameserver 1.2.3.4", []string{"1.2.3.4"}, []string{}, []string{}, false},
		{"\tnameserver 1.2.3.4", []string{"1.2.3.4"}, []string{}, []string{}, false},
		{"nameserver\t1.2.3.4", []string{"1.2.3.4"}, []string{}, []string{}, false},
		{"nameserver \t 1.2.3.4", []string{"1.2.3.4"}, []string{}, []string{}, false},
		{"nameserver 1.2.3.4\nnameserver 5.6.7.8", []string{"1.2.3.4", "5.6.7.8"}, []string{}, []string{}, false},
		{"nameserver 1.2.3.4 #comment", []string{"1.2.3.4"}, []string{}, []string{}, false},
		{"search ", []string{}, []string{}, []string{}, false},  // search 为空
		{"search .", []string{}, []string{}, []string{}, false}, // 忽略单独的 "."，它表示根域
		{"search . foo", []string{}, []string{"foo"}, []string{}, false},
		{"search foo .", []string{}, []string{"foo"}, []string{}, false},
		{"search foo .  bar", []string{}, []string{"foo", "bar"}, []string{}, false},
		{"search foo", []string{}, []string{"foo"}, []string{}, false},
		{"search foo bar", []string{}, []string{"foo", "bar"}, []string{}, false},
		{"search foo. bar", []string{}, []string{"foo", "bar"}, []string{}, false},
		{"search foo bar bat\n", []string{}, []string{"foo", "bar", "bat"}, []string{}, false},
		{"search foo\nsearch bar", []string{}, []string{"bar"}, []string{}, false},
		{"nameserver 1.2.3.4\nsearch foo bar", []string{"1.2.3.4"}, []string{"foo", "bar"}, []string{}, false},
		{"nameserver 1.2.3.4\nsearch foo\nnameserver 5.6.7.8\nsearch bar", []string{"1.2.3.4", "5.6.7.8"}, []string{"bar"}, []string{}, false},
		{"#comment\nnameserver 1.2.3.4\n#comment\nsearch foo\ncomment", []string{"1.2.3.4"}, []string{"foo"}, []string{}, false},
		{"options ", []string{}, []string{}, []string{}, false},
		{"options ndots:5 attempts:2", []string{}, []string{}, []string{"ndots:5", "attempts:2"}, false},
		{"options ndots:1 ndots:5 attempts:2", []string{}, []string{}, []string{"ndots:5", "attempts:2"}, false},
		{"options ndots:1 edns0 attempts:2 single-request-reopen", []string{}, []string{}, []string{"edns0", "attempts:2", "single-request-reopen", "ndots:1"}, false},
		{"options ndots:1\noptions ndots:5 attempts:3", []string{}, []string{}, []string{"ndots:5", "attempts:3"}, false},
		{"options ndots:1 timeout:3 timeout:1 attempts:3\noptions ndots:5", []string{}, []string{}, []string{"ndots:5", "timeout:1", "attempts:3"}, false},
		{"options ndots:1 attempts:3\noptions ndots:1 attempts:3 ndots:5", []string{}, []string{}, []string{"ndots:5", "attempts:3"}, false},
		{"nameserver 1.2.3.4\nsearch foo\nnameserver 5.6.7.8\nsearch bar\noptions ndots:5 attempts:4", []string{"1.2.3.4", "5.6.7.8"}, []string{"bar"}, []string{"ndots:5", "attempts:4"}, false},
	}
	// 逐条解析用例，校验解析结果或预期错误。
	for i, tc := range testCases {
		ns, srch, opts, err := parseResolvConf(strings.NewReader(tc.data))
		if !tc.isErr {
			require.NoError(t, err)
			assert.EqualValues(t, tc.nameservers, ns, "test case [%d]: name servers", i)
			assert.EqualValues(t, tc.searches, srch, "test case [%d] searches", i)
			assert.EqualValues(t, sets.New[string](tc.options...), sets.New[string](opts...), "test case [%d] options", i)
		} else {
			require.Error(t, err, "tc.searches %v", tc.searches)
		}
	}
}

// TestFormDNSSearchFitsLimits 校验搜索路径裁剪：单条路径不超过 256 字符、总长度不超过 2048 字符、
// 路径数不超过 32；超限时裁剪并产生 Warning 事件。
func TestFormDNSSearchFitsLimits(t *testing.T) {
	searchPathList2048Chars := []string{
		// 2048 = 128 + 127*15 + 15，构造恰好达到长度上限的搜索路径列表
		strings.Repeat("A", 128),
		strings.Repeat("A", 127),
		strings.Repeat("A", 127),
		strings.Repeat("A", 127),
		strings.Repeat("A", 127),
		strings.Repeat("A", 127),
		strings.Repeat("A", 127),
		strings.Repeat("A", 127),
		strings.Repeat("A", 127),
		strings.Repeat("A", 127),
		strings.Repeat("A", 127),
		strings.Repeat("A", 127),
		strings.Repeat("A", 127),
		strings.Repeat("A", 127),
		strings.Repeat("A", 127),
		strings.Repeat("A", 127),
	}

	// 任务用例期望的搜索域顺序与产生的告警事件文案。
	recorder := record.NewFakeRecorder(20)
	nodeRef := &v1.ObjectReference{
		Kind:      "Node",
		Name:      string("testNode"),
		UID:       types.UID("testNode"),
		Namespace: "",
	}
	testClusterDNSDomain := "TEST"

	configurer := NewConfigurer(recorder, nodeRef, nil, nil, testClusterDNSDomain, "")

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:         "",
			Name:        "test_pod",
			Namespace:   "testNS",
			Annotations: map[string]string{},
		},
	}

	testCases := []struct {
		desc         string
		hostNames    []string
		resultSearch []string
		events       []string
	}{
		{
			desc:         "valid: 3 search paths",
			hostNames:    []string{"testNS.svc.TEST", "svc.TEST", "TEST"},
			resultSearch: []string{"testNS.svc.TEST", "svc.TEST", "TEST"},
			events:       []string{},
		},

		{
			desc:         "valid: 5 search paths",
			hostNames:    []string{"testNS.svc.TEST", "svc.TEST", "TEST", "AAA", "BBB"},
			resultSearch: []string{"testNS.svc.TEST", "svc.TEST", "TEST", "AAA", "BBB"},
			events:       []string{},
		},

		{
			desc:         "invalid: longer than 256 characters in search path list",
			hostNames:    []string{"testNS.svc.TEST", "svc.TEST", "TEST", "AAA", strings.Repeat("B", 256), "BBB"},
			resultSearch: []string{"testNS.svc.TEST", "svc.TEST", "TEST", "AAA", "BBB"},
			events:       []string{"Search Line limits were exceeded, some search paths have been omitted, the applied search line is: testNS.svc.TEST svc.TEST TEST AAA BBB"},
		},

		{
			desc:         "valid: 2048 characters in search path list",
			hostNames:    searchPathList2048Chars,
			resultSearch: searchPathList2048Chars,
			events:       []string{},
		},

		{
			desc:         "invalid: 2050 characters in search path list",
			hostNames:    append(searchPathList2048Chars, "B"),
			resultSearch: searchPathList2048Chars,
			events:       []string{fmt.Sprintf("Search Line limits were exceeded, some search paths have been omitted, the applied search line is: %s", strings.Join(searchPathList2048Chars, " "))},
		},

		{
			desc:         "invalid: 256 characters search path",
			hostNames:    []string{"testNS.svc.TEST", "svc.TEST", "TEST", "AAA", strings.Repeat("B", 256), "BBB"},
			resultSearch: []string{"testNS.svc.TEST", "svc.TEST", "TEST", "AAA", "BBB"},
			events:       []string{"Search Line limits were exceeded, some search paths have been omitted, the applied search line is: testNS.svc.TEST svc.TEST TEST AAA BBB"},
		},

		{
			desc:         "valid: 7 search paths",
			hostNames:    []string{"testNS.svc.TEST", "svc.TEST", "TEST", "AAA", "BBB", "CCC", "DDD"},
			resultSearch: []string{"testNS.svc.TEST", "svc.TEST", "TEST", "AAA", "BBB", "CCC", "DDD"},
			events:       []string{},
		},

		{
			desc:         "valid: 32 search paths",
			hostNames:    []string{"testNS.svc.TEST", "svc.TEST", "TEST", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32"},
			resultSearch: []string{"testNS.svc.TEST", "svc.TEST", "TEST", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32"},
			events:       []string{},
		},

		{
			desc:         "invalid: 33 search paths",
			hostNames:    []string{"testNS.svc.TEST", "svc.TEST", "TEST", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33"},
			resultSearch: []string{"testNS.svc.TEST", "svc.TEST", "TEST", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32"},
			events:       []string{"Search Line limits were exceeded, some search paths have been omitted, the applied search line is: testNS.svc.TEST svc.TEST TEST 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32"},
		},
	}

	for i, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			dnsSearch := configurer.formDNSSearchFitsLimits(logger, tc.hostNames, pod)
			assert.EqualValues(t, tc.resultSearch, dnsSearch, "test [%d]", i)
			// 事件格式为 "<type> <reason> <message>"，逐条比对。
			for _, expectedEvent := range tc.events {
				expected := fmt.Sprintf("%s %s %s", v1.EventTypeWarning, "DNSConfigForming", expectedEvent)
				event := fetchEvent(recorder)
				assert.Equal(t, expected, event, "test [%d]", i)
			}
		})
	}
}

// TestFormDNSNameserversFitsLimits 校验 nameserver 超过 3 个时被截断为前 3 个，且仅超限时才产生事件。
func TestFormDNSNameserversFitsLimits(t *testing.T) {
	recorder := record.NewFakeRecorder(20)
	nodeRef := &v1.ObjectReference{
		Kind:      "Node",
		Name:      string("testNode"),
		UID:       types.UID("testNode"),
		Namespace: "",
	}
	testClusterDNSDomain := "TEST"

	configurer := NewConfigurer(recorder, nodeRef, nil, nil, testClusterDNSDomain, "")

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:         "",
			Name:        "test_pod",
			Namespace:   "testNS",
			Annotations: map[string]string{},
		},
	}

	testCases := []struct {
		desc               string
		nameservers        []string
		expectedNameserver []string
		expectedEvent      bool
	}{
		{
			desc:               "valid: 1 nameserver",
			nameservers:        []string{"127.0.0.1"},
			expectedNameserver: []string{"127.0.0.1"},
			expectedEvent:      false,
		},
		{
			desc:               "valid: 3 nameservers",
			nameservers:        []string{"127.0.0.1", "10.0.0.10", "8.8.8.8"},
			expectedNameserver: []string{"127.0.0.1", "10.0.0.10", "8.8.8.8"},
			expectedEvent:      false,
		},
		{
			desc:               "invalid: 4 nameservers, trimmed to 3",
			nameservers:        []string{"127.0.0.1", "10.0.0.10", "8.8.8.8", "1.2.3.4"},
			expectedNameserver: []string{"127.0.0.1", "10.0.0.10", "8.8.8.8"},
			expectedEvent:      true,
		},
	}

	for _, tc := range testCases {
		logger, _ := ktesting.NewTestContext(t)
		// 截断结果写入实际配置，超限用例还应伴随一条事件。
		appliedNameservers := configurer.formDNSNameserversFitsLimits(logger, tc.nameservers, pod)
		assert.EqualValues(t, tc.expectedNameserver, appliedNameservers, tc.desc)
		event := fetchEvent(recorder)
		if tc.expectedEvent && len(event) == 0 {
			t.Errorf("%s: formDNSNameserversFitsLimits(%v) expected event, got no event.", tc.desc, tc.nameservers)
		} else if !tc.expectedEvent && len(event) > 0 {
			t.Errorf("%s: formDNSNameserversFitsLimits(%v) expected no event, got event: %v", tc.desc, tc.nameservers, event)
		}
	}
}

// TestMergeDNSOptions 校验 Pod DNSConfig 选项与现有 resolv.conf 选项的合并：同名选项由新值覆写。
func TestMergeDNSOptions(t *testing.T) {
	testOptionValue := "3"

	testCases := []struct {
		desc                     string
		existingDNSConfigOptions []string
		dnsConfigOptions         []v1.PodDNSConfigOption
		expectedOptions          []string
	}{
		{
			desc:                     "Empty dnsConfigOptions",
			existingDNSConfigOptions: []string{"ndots:5", "debug"},
			dnsConfigOptions:         nil,
			expectedOptions:          []string{"ndots:5", "debug"},
		},
		{
			desc:                     "No duplicated entries",
			existingDNSConfigOptions: []string{"ndots:5", "debug"},
			dnsConfigOptions: []v1.PodDNSConfigOption{
				{Name: "single-request"},
				{Name: "attempts", Value: &testOptionValue},
			},
			expectedOptions: []string{"ndots:5", "debug", "single-request", "attempts:3"},
		},
		{
			desc:                     "Overwrite duplicated entries",
			existingDNSConfigOptions: []string{"ndots:5", "debug"},
			dnsConfigOptions: []v1.PodDNSConfigOption{
				{Name: "ndots", Value: &testOptionValue},
				{Name: "debug"},
				{Name: "single-request"},
				{Name: "attempts", Value: &testOptionValue},
			},
			expectedOptions: []string{"ndots:3", "debug", "single-request", "attempts:3"},
		},
	}

	for _, tc := range testCases {
		options := mergeDNSOptions(tc.existingDNSConfigOptions, tc.dnsConfigOptions)
		// 转换后选项顺序可能变化，因此按集合比较。
		if !sets.New[string](options...).Equal(sets.New[string](tc.expectedOptions...)) {
			t.Errorf("%s: mergeDNSOptions(%v, %v)=%v, want %v", tc.desc, tc.existingDNSConfigOptions, tc.dnsConfigOptions, options, tc.expectedOptions)
		}
	}
}

// TestGetPodDNSType 覆盖 DNSPolicy、hostNetwork 与集群 DNS 可用性组合下选出的 DNS 类型，
// 以及非法策略返回错误的行为。
func TestGetPodDNSType(t *testing.T) {
	recorder := record.NewFakeRecorder(20)
	nodeRef := &v1.ObjectReference{
		Kind:      "Node",
		Name:      string("testNode"),
		UID:       types.UID("testNode"),
		Namespace: "",
	}
	testClusterDNSDomain := "TEST"
	clusterNS := "203.0.113.1"
	testClusterDNS := []net.IP{netutils.ParseIPSloppy(clusterNS)}

	configurer := NewConfigurer(recorder, nodeRef, nil, nil, testClusterDNSDomain, "")

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:         "",
			Name:        "test_pod",
			Namespace:   "testNS",
			Annotations: map[string]string{},
		},
	}

	testCases := []struct {
		desc            string
		hasClusterDNS   bool
		hostNetwork     bool
		dnsPolicy       v1.DNSPolicy
		expectedDNSType podDNSType
		expectedError   bool
	}{
		{
			desc:            "valid DNSClusterFirst without hostnetwork",
			hasClusterDNS:   true,
			dnsPolicy:       v1.DNSClusterFirst,
			expectedDNSType: podDNSCluster,
		},
		{
			desc:            "valid DNSClusterFirstWithHostNet with hostnetwork",
			hasClusterDNS:   true,
			hostNetwork:     true,
			dnsPolicy:       v1.DNSClusterFirstWithHostNet,
			expectedDNSType: podDNSCluster,
		},
		{
			desc:            "valid DNSClusterFirstWithHostNet without hostnetwork",
			hasClusterDNS:   true,
			dnsPolicy:       v1.DNSClusterFirstWithHostNet,
			expectedDNSType: podDNSCluster,
		},
		{
			desc:            "valid DNSDefault without hostnetwork",
			dnsPolicy:       v1.DNSDefault,
			expectedDNSType: podDNSHost,
		},
		{
			desc:            "valid DNSDefault with hostnetwork",
			hostNetwork:     true,
			dnsPolicy:       v1.DNSDefault,
			expectedDNSType: podDNSHost,
		},
		{
			desc:            "DNSClusterFirst with hostnetwork, fallback to DNSDefault",
			hasClusterDNS:   true,
			hostNetwork:     true,
			dnsPolicy:       v1.DNSClusterFirst,
			expectedDNSType: podDNSHost,
		},
		{
			desc:            "valid DNSNone",
			dnsPolicy:       v1.DNSNone,
			expectedDNSType: podDNSNone,
		},
		{
			desc:          "invalid DNS policy, should return error",
			dnsPolicy:     "invalidPolicy",
			expectedError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			if tc.hasClusterDNS {
				configurer.clusterDNS = testClusterDNS
			} else {
				configurer.clusterDNS = nil
			}
			pod.Spec.DNSPolicy = tc.dnsPolicy
			pod.Spec.HostNetwork = tc.hostNetwork

			// 先按策略与 hostNetwork 决定类型，再决定是否回退到主机 DNS。
			resType, err := getPodDNSType(pod)
			if tc.expectedError {
				if err == nil {
					t.Errorf("%s: GetPodDNSType(%v) got no error, want error", tc.desc, pod)
				}
				return
			}
			if resType != tc.expectedDNSType {
				t.Errorf("%s: GetPodDNSType(%v)=%v, want %v", tc.desc, pod, resType, tc.expectedDNSType)
			}
		})
	}
}

// TestGetPodDNS 校验各 DNSPolicy 生成的服务端与搜索域，并验证使用自定义 resolv.conf 时
// 搜索路径会与集群域合并且不超过 MaxDNSSearchPaths。
func TestGetPodDNS(t *testing.T) {
	ctx := ktesting.Init(t)
	recorder := record.NewFakeRecorder(20)
	nodeRef := &v1.ObjectReference{
		Kind:      "Node",
		Name:      string("testNode"),
		UID:       types.UID("testNode"),
		Namespace: "",
	}
	clusterNS := "203.0.113.1"
	testClusterDNSDomain := "kubernetes.io"
	testClusterDNS := []net.IP{netutils.ParseIPSloppy(clusterNS)}

	configurer := NewConfigurer(recorder, nodeRef, nil, testClusterDNS, testClusterDNSDomain, "")

	pods := newTestPods(4)
	pods[0].Spec.DNSPolicy = v1.DNSClusterFirstWithHostNet
	pods[1].Spec.DNSPolicy = v1.DNSClusterFirst
	pods[2].Spec.DNSPolicy = v1.DNSClusterFirst
	pods[2].Spec.HostNetwork = false
	pods[3].Spec.DNSPolicy = v1.DNSDefault

	options := make([]struct {
		DNS       []string
		DNSSearch []string
	}, 4)
	for i, pod := range pods {
		var err error
		dnsConfig, err := configurer.GetPodDNS(ctx, pod)
		if err != nil {
			t.Fatalf("failed to generate container options: %v", err)
		}
		options[i].DNS, options[i].DNSSearch = dnsConfig.Servers, dnsConfig.Searches
	}
	if len(options[0].DNS) != 1 || options[0].DNS[0] != clusterNS {
		t.Errorf("expected nameserver %s, got %+v", clusterNS, options[0].DNS)
	}
	if len(options[0].DNSSearch) == 0 || options[0].DNSSearch[0] != ".svc."+configurer.ClusterDomain {
		t.Errorf("expected search %s, got %+v", ".svc."+configurer.ClusterDomain, options[0].DNSSearch)
	}
	if len(options[1].DNS) != 1 || options[1].DNS[0] != "127.0.0.1" {
		t.Errorf("expected nameserver 127.0.0.1, got %+v", options[1].DNS)
	}
	if len(options[1].DNSSearch) != 1 || options[1].DNSSearch[0] != "." {
		t.Errorf("expected search \".\", got %+v", options[1].DNSSearch)
	}
	if len(options[2].DNS) != 1 || options[2].DNS[0] != clusterNS {
		t.Errorf("expected nameserver %s, got %+v", clusterNS, options[2].DNS)
	}
	if len(options[2].DNSSearch) == 0 || options[2].DNSSearch[0] != ".svc."+configurer.ClusterDomain {
		t.Errorf("expected search %s, got %+v", ".svc."+configurer.ClusterDomain, options[2].DNSSearch)
	}
	if len(options[3].DNS) != 1 || options[3].DNS[0] != "127.0.0.1" {
		t.Errorf("expected nameserver 127.0.0.1, got %+v", options[3].DNS)
	}
	if len(options[3].DNSSearch) != 1 || options[3].DNSSearch[0] != "." {
		t.Errorf("expected search \".\", got %+v", options[3].DNSSearch)
	}

	// 换成默认主机 resolv.conf 并桩掉主机配置读取，验证集群域与主机搜索域的合并且限制路径数。
	configurer = NewConfigurer(recorder, nodeRef, nil, testClusterDNS, testClusterDNSDomain, defaultResolvConf)
	configurer.getHostDNSConfig = fakeGetHostDNSConfigCustom

	for i, pod := range pods {
		var err error
		dnsConfig, err := configurer.GetPodDNS(ctx, pod)
		if err != nil {
			t.Fatalf("failed to generate container options: %v", err)
		}
		options[i].DNS, options[i].DNSSearch = dnsConfig.Servers, dnsConfig.Searches
	}
	t.Logf("nameservers %+v", options[1].DNS)
	if len(options[0].DNS) != 1 {
		t.Errorf("expected cluster nameserver only, got %+v", options[0].DNS)
	} else if options[0].DNS[0] != clusterNS {
		t.Errorf("expected nameserver %s, got %v", clusterNS, options[0].DNS[0])
	}
	// 集群域会追加 ".svc.<domain>"、".svc"、domain 三条，但总数受 MaxDNSSearchPaths 限制。
	expLength := len(options[1].DNSSearch) + 3

	maxDNSSearchPaths := validation.MaxDNSSearchPaths

	if expLength > maxDNSSearchPaths {
		expLength = maxDNSSearchPaths
	}
	if len(options[0].DNSSearch) != expLength {
		t.Errorf("expected prepend of cluster domain, got %+v", options[0].DNSSearch)
	} else if options[0].DNSSearch[0] != ".svc."+configurer.ClusterDomain {
		t.Errorf("expected domain %s, got %s", ".svc."+configurer.ClusterDomain, options[0].DNSSearch)
	}
	if len(options[2].DNS) != 1 {
		t.Errorf("expected cluster nameserver only, got %+v", options[2].DNS)
	} else if options[2].DNS[0] != clusterNS {
		t.Errorf("expected nameserver %s, got %v", clusterNS, options[2].DNS[0])
	}
	if len(options[2].DNSSearch) != expLength {
		t.Errorf("expected prepend of cluster domain, got %+v", options[2].DNSSearch)
	} else if options[2].DNSSearch[0] != ".svc."+configurer.ClusterDomain {
		t.Errorf("expected domain %s, got %s", ".svc."+configurer.ClusterDomain, options[0].DNSSearch)
	}
}

// TestGetPodDNSCustom 校验 Pod DNSConfig 与集群/主机 DNS 的合并顺序：不同 DNSPolicy 合并来源不同，
// 自定义 nameserver/search 追加，同名 option 覆盖。
func TestGetPodDNSCustom(t *testing.T) {
	recorder := record.NewFakeRecorder(20)
	nodeRef := &v1.ObjectReference{
		Kind:      "Node",
		Name:      string("testNode"),
		UID:       types.UID("testNode"),
		Namespace: "",
	}

	testPodNamespace := "testNS"
	testClusterNameserver := "10.0.0.10"
	testClusterDNSDomain := "kubernetes.io"
	testSvcDomain := fmt.Sprintf("svc.%s", testClusterDNSDomain)
	testNsSvcDomain := fmt.Sprintf("%s.svc.%s", testPodNamespace, testClusterDNSDomain)
	testNdotsOptionValue := "3"

	testPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test_pod",
			Namespace: testPodNamespace,
		},
	}

	// 写入临时 resolv.conf，作为主机 DNS 的上游来源。
	resolvConfContent := []byte(fmt.Sprintf("nameserver %s\nsearch %s\n", testHostNameserver, testHostDomain))
	tmpfile, err := os.CreateTemp("", "tmpResolvConf")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	if _, err := tmpfile.Write(resolvConfContent); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	configurer := NewConfigurer(recorder, nodeRef, nil, []net.IP{netutils.ParseIPSloppy(testClusterNameserver)}, testClusterDNSDomain, tmpfile.Name())

	testCases := []struct {
		desc              string
		hostnetwork       bool
		dnsPolicy         v1.DNSPolicy
		dnsConfig         *v1.PodDNSConfig
		expectedDNSConfig *runtimeapi.DNSConfig
	}{
		{
			desc:              "DNSNone without DNSConfig should have empty DNS settings",
			dnsPolicy:         v1.DNSNone,
			expectedDNSConfig: &runtimeapi.DNSConfig{},
		},
		{
			desc:      "DNSNone with DNSConfig should have a merged DNS settings",
			dnsPolicy: v1.DNSNone,
			dnsConfig: &v1.PodDNSConfig{
				Nameservers: []string{"203.0.113.1"},
				Searches:    []string{"my.domain", "second.domain"},
				Options: []v1.PodDNSConfigOption{
					{Name: "ndots", Value: &testNdotsOptionValue},
					{Name: "debug"},
				},
			},
			expectedDNSConfig: &runtimeapi.DNSConfig{
				Servers:  []string{"203.0.113.1"},
				Searches: []string{"my.domain", "second.domain"},
				Options:  []string{"ndots:3", "debug"},
			},
		},
		{
			desc:      "DNSClusterFirst with DNSConfig should have a merged DNS settings",
			dnsPolicy: v1.DNSClusterFirst,
			dnsConfig: &v1.PodDNSConfig{
				Nameservers: []string{"10.0.0.11"},
				Searches:    []string{"my.domain"},
				Options: []v1.PodDNSConfigOption{
					{Name: "ndots", Value: &testNdotsOptionValue},
					{Name: "debug"},
				},
			},
			expectedDNSConfig: &runtimeapi.DNSConfig{
				Servers:  []string{testClusterNameserver, "10.0.0.11"},
				Searches: []string{testNsSvcDomain, testSvcDomain, testClusterDNSDomain, testHostDomain, "my.domain"},
				Options:  []string{"ndots:3", "debug"},
			},
		},
		{
			desc:        "DNSClusterFirstWithHostNet with DNSConfig should have a merged DNS settings",
			hostnetwork: true,
			dnsPolicy:   v1.DNSClusterFirstWithHostNet,
			dnsConfig: &v1.PodDNSConfig{
				Nameservers: []string{"10.0.0.11"},
				Searches:    []string{"my.domain"},
				Options: []v1.PodDNSConfigOption{
					{Name: "ndots", Value: &testNdotsOptionValue},
					{Name: "debug"},
				},
			},
			expectedDNSConfig: &runtimeapi.DNSConfig{
				Servers:  []string{testClusterNameserver, "10.0.0.11"},
				Searches: []string{testNsSvcDomain, testSvcDomain, testClusterDNSDomain, testHostDomain, "my.domain"},
				Options:  []string{"ndots:3", "debug"},
			},
		},
		{
			desc:      "DNSDefault with DNSConfig should have a merged DNS settings",
			dnsPolicy: v1.DNSDefault,
			dnsConfig: &v1.PodDNSConfig{
				Nameservers: []string{"10.0.0.11"},
				Searches:    []string{"my.domain"},
				Options: []v1.PodDNSConfigOption{
					{Name: "ndots", Value: &testNdotsOptionValue},
					{Name: "debug"},
				},
			},
			expectedDNSConfig: &runtimeapi.DNSConfig{
				Servers:  []string{testHostNameserver, "10.0.0.11"},
				Searches: []string{testHostDomain, "my.domain"},
				Options:  []string{"ndots:3", "debug"},
			},
		},
	}

	// 每个用例独立设置 Pod 的 hostNetwork/DNSConfig/DNSPolicy 后再生成 DNS 配置。
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := ktesting.Init(t)
			testPod.Spec.HostNetwork = tc.hostnetwork
			testPod.Spec.DNSConfig = tc.dnsConfig
			testPod.Spec.DNSPolicy = tc.dnsPolicy

			resDNSConfig, err := configurer.GetPodDNS(ctx, testPod)
			if err != nil {
				t.Errorf("%s: GetPodDNS(%v), unexpected error: %v", tc.desc, testPod, err)
			}
			if !dnsConfigsAreEqual(resDNSConfig, tc.expectedDNSConfig) {
				t.Errorf("%s: GetPodDNS(%v)=%v, want %v", tc.desc, testPod, resDNSConfig, tc.expectedDNSConfig)
			}
		})
	}
}

// dnsConfigsAreEqual 按顺序比较服务器与搜索域，选项按集合比较（顺序无关）。
func dnsConfigsAreEqual(resConfig, expectedConfig *runtimeapi.DNSConfig) bool {
	if len(resConfig.Servers) != len(expectedConfig.Servers) ||
		len(resConfig.Searches) != len(expectedConfig.Searches) ||
		len(resConfig.Options) != len(expectedConfig.Options) {
		return false
	}
	for i, server := range resConfig.Servers {
		if expectedConfig.Servers[i] != server {
			return false
		}
	}
	for i, search := range resConfig.Searches {
		if expectedConfig.Searches[i] != search {
			return false
		}
	}
	// Options order may be changed after conversion.
	return sets.New[string](resConfig.Options...).Equal(sets.New[string](expectedConfig.Options...))
}

// newTestPods 构造 count 个 HostNetwork 测试 Pod；UID 与名称按序号递增以便区分。
func newTestPods(count int) []*v1.Pod {
	pods := make([]*v1.Pod, count)
	for i := 0; i < count; i++ {
		pods[i] = &v1.Pod{
			Spec: v1.PodSpec{
				HostNetwork: true,
			},
			ObjectMeta: metav1.ObjectMeta{
				UID:  types.UID(strconv.Itoa(10000 + i)),
				Name: fmt.Sprintf("pod%d", i),
			},
		}
	}
	return pods
}
