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

package netpol

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/intstr"

	v1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"

	"github.com/onsi/ginkgo/v2"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	"k8s.io/kubernetes/test/e2e/network/common"
	admissionapi "k8s.io/pod-security-admission/api"
	utilnet "k8s.io/utils/net"
)

const (
	isVerbose = true

	// See https://github.com/kubernetes/kubernetes/issues/95879
	// network policy 对 loopback 调用的影响语义可能没有定义：它们是否
	// 应始终 ALLOWED；Service 对此有何影响？
	// Calico、Cilium、Antrea 的做法似乎各不相同。
	// 不同 CNI 结果不同，导致包含 loopback 的测试在
	// 某些 CNI 上失败。因此在判断测试通过与否时，干脆忽略 loopback 调用。
	ignoreLoopback    = true
	namespaceLabelKey = "kubernetes.io/metadata.name"
)

var (
	protocolTCP  = v1.ProtocolTCP
	protocolUDP  = v1.ProtocolUDP
	protocolSCTP = v1.ProtocolSCTP
)

/*
你可能会疑惑，为什么每个测试用例要用多个 namespace？

这些测试基于"真值表"，比较每对 pod 的预期和实际连通性。
由于 network policy 存在于 namespace 中，且 peer 可按 namespace 选择，
展示其他 namespace 中 pod 的连通性是判断 network policy 是否按预期工作的关键信息。

每个测试指定它需要的确切 pod 拓扑（例如 x/a、x/b、y/a、z/a）。
*/

var _ = common.SIGDescribe("Netpol", func() {
	f := framework.NewDefaultFramework("netpol")
	f.SkipNamespaceCreation = true // we create our own 3 test namespaces, we don't need the default one
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	ginkgo.Context("NetworkPolicy between server and client", func() {
		var k8s *kubeManager

		f.It("should support a 'default-deny-ingress' policy", feature.NetworkPolicy, func(ctx context.Context) {

			// 只轮询 TCP
			protocols := []v1.Protocol{protocolTCP}

			// 只测试端口 80
			ports := []int32{80}

			// Namespace X 有 default-deny-ingress 策略，所以 X 中需要 2 个 pod 来
			// 验证 X 到 X 的 ingress 被阻断，Y 中需要 2 个 pod 来验证 Y 到 X 也被
			// 阻断，同时 X 到 Y 和 Y 到 Y 的流量仍被允许。
			// （后面的测试中，我们假设 X 中的策略不会影响 Y 到 Y 的流量。）
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a", "y/b")

			// 只在 namespace X 中制定策略
			nsX, _, _ := getK8sNamespaces(k8s)
			policy := GenNetworkPolicyWithNameAndPodSelector("deny-ingress", metav1.LabelSelector{}, SetSpecIngressRules())

			// 创建策略
			CreatePolicy(ctx, k8s, policy, nsX)

			// 为 x、y、z 中所有 pod 制作连通性真值表
			reachability := NewReachability(k8s.AllPodStrings(), true)
			// 把 nsX 设为 false，因为它有阻断流量的策略
			reachability.ExpectPeer(&Peer{}, &Peer{Namespace: nsX}, false)

			// 确认真实世界的连通性与我们的矩阵一致
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should support a 'default-deny-all' policy", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// Namespace X 对 ingress 和 egress 都 default-deny，所以需要 2 个
			// X 中的 pod 验证 X 到 X 流量被阻断，Y 中一个 pod 验证
			// X<->Y 流量双向都被阻断。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a")
			nsX, _, _ := getK8sNamespaces(k8s)

			policy := GenNetworkPolicyWithNameAndPodSelector("deny-all", metav1.LabelSelector{}, SetSpecIngressRules(), SetSpecEgressRules())
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectPeer(&Peer{}, &Peer{Namespace: nsX}, false)
			reachability.ExpectPeer(&Peer{Namespace: nsX}, &Peer{}, false)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should enforce policy to allow traffic from pods within server namespace based on PodSelector", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// 策略隔离 x/a 且只允许来自 x/b 的 ingress，因此需要 x/b 作为
			// 允许的同 namespace peer，x/c 作为同 namespace 不匹配的 pod，
			// y/a 作为不能访问 x/a 的跨 namespace peer。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "x/c", "y/a")
			nsX, _, _ := getK8sNamespaces(k8s)

			allowedPods := metav1.LabelSelector{
				MatchLabels: map[string]string{
					"pod": "b",
				},
			}
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{PodSelector: &allowedPods})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("x-a-allows-x-b", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectAllIngress(NewPodString(nsX, "a"), false)
			reachability.Expect(NewPodString(nsX, "b"), NewPodString(nsX, "a"), true)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should enforce policy to allow ingress traffic for a target", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// Namespace X 是 default-deny-ingress，但我们允许 ingress 到 x/a。
			// 需要 x/a 和 x/b 来展示针对特定目标的行为，并用 y/a
			// 验证跨 namespace 的 ingress。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a")
			nsX, _, _ := getK8sNamespaces(k8s)

			ginkgo.By("having a deny all ingress policy")
			// 对 nsX namespace 中的 pod 设置拒绝所有 Ingress 流量的策略
			policy := GenNetworkPolicyWithNameAndPodSelector("deny-all", metav1.LabelSelector{}, SetSpecIngressRules())
			CreatePolicy(ctx, k8s, policy, nsX)

			// 只允许来自任意 pod 的 Ingress 流量到 pod x/a
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{PodSelector: &metav1.LabelSelector{}, NamespaceSelector: &metav1.LabelSelector{}})
			allowPolicy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-all-to-a", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, allowPolicy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectAllIngress(NewPodString(nsX, "a"), true)
			reachability.ExpectAllIngress(NewPodString(nsX, "b"), false)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should enforce policy to allow ingress traffic from pods in all namespaces", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// x/a 上的策略应允许来自任意 namespace 的 ingress，包括自己的 namespace。
			// 我们用 x/b 作为同 namespace client，Y 和 Z 中各一个 pod 作为
			// 跨 namespace client。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a", "z/a")
			nsX, _, _ := getK8sNamespaces(k8s)

			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{}}})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-from-another-ns", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should enforce policy to allow traffic only from a different namespace, based on NamespaceSelector", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// x/a 上的策略只允许来自 namespace Y 的 ingress。需要 x/b 来展示
			// 同 namespace ingress 被拒绝，y/a 作为允许的来源，z/a 作为
			// 必须被拒绝的无关 namespace。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a", "z/a")
			nsX, nsY, nsZ := getK8sNamespaces(k8s)

			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{namespaceLabelKey: nsY}}})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-client-a-via-ns-selector", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			// 拒绝来自 x 或 z namespace 的所有流量
			reachability.ExpectPeer(&Peer{Namespace: nsX}, &Peer{Namespace: nsX, Pod: "a"}, false)
			reachability.ExpectPeer(&Peer{Namespace: nsZ}, &Peer{Namespace: nsX, Pod: "a"}, false)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should enforce policy based on PodSelector with MatchExpressions", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// 与基本 PodSelector 测试相同，但使用 MatchExpressions：x/b 必须
			// 能访问 x/a，而 y/a（其他 namespace）不能访问 x/a。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a")
			nsX, _, _ := getK8sNamespaces(k8s)

			allowedPods := metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      "pod",
					Operator: metav1.LabelSelectorOpIn,
					Values:   []string{"b"},
				}},
			}
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{PodSelector: &allowedPods})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("x-a-allows-x-b", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectAllIngress(NewPodString(nsX, "a"), false)
			reachability.Expect(NewPodString(nsX, "b"), NewPodString(nsX, "a"), true)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should enforce policy based on NamespaceSelector with MatchExpressions", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// 与基本 NamespaceSelector 测试相同，但使用 MatchExpressions：y/a 是
			// 允许的来源 namespace，而 x/b（同 namespace）和 z/a 连接 x/a 时
			// 必须被拒绝。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a", "z/a")
			nsX, nsY, nsZ := getK8sNamespaces(k8s)

			allowedNamespaces := &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      namespaceLabelKey,
					Operator: metav1.LabelSelectorOpIn,
					Values:   []string{nsY},
				}},
			}
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{NamespaceSelector: allowedNamespaces})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-ns-y-match-selector", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			// 拒绝来自 x 或 z namespace 的所有流量
			reachability.ExpectPeer(&Peer{Namespace: nsX}, &Peer{Namespace: nsX, Pod: "a"}, false)
			reachability.ExpectPeer(&Peer{Namespace: nsZ}, &Peer{Namespace: nsX, Pod: "a"}, false)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should enforce policy based on PodSelector or NamespaceSelector", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// x/a 上的策略允许来自 (a) 除 X 外的任意 namespace，或
			// (b) pod x/b 的 ingress。我们用 x/b 作为 PodSelector 允许的来源，x/c 作为
			// X 中不匹配、必须被拒绝的 pod，y/a 作为 NamespaceSelector
			// 允许的来源。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "x/c", "y/a")
			nsX, _, _ := getK8sNamespaces(k8s)

			allowedNamespaces := &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      namespaceLabelKey,
					Operator: metav1.LabelSelectorOpNotIn,
					Values:   []string{nsX},
				}},
			}
			podBAllowlisting := &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"pod": "b",
				},
			}
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{NamespaceSelector: allowedNamespaces}, networkingv1.NetworkPolicyPeer{PodSelector: podBAllowlisting})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-ns-y-match-selector", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.Expect(NewPodString(nsX, "a"), NewPodString(nsX, "a"), false)
			reachability.Expect(NewPodString(nsX, "c"), NewPodString(nsX, "a"), false)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should enforce policy based on PodSelector and NamespaceSelector", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// x/a 上的策略要求 namespace 匹配且 pod 匹配。我们用 y/b
			// 和 z/b 作为同时匹配两个 selector 的来源，x/b 作为反例
			// （pod 匹配但 namespace 不匹配），y/a 作为反例（namespace
			// 匹配但 pod 不匹配）。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a", "y/b", "z/b")
			nsX, nsY, nsZ := getK8sNamespaces(k8s)

			allowedNamespaces := &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      namespaceLabelKey,
					Operator: metav1.LabelSelectorOpNotIn,
					Values:   []string{nsX},
				}},
			}
			allowedPod := &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"pod": "b",
				},
			}
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{NamespaceSelector: allowedNamespaces, PodSelector: allowedPod})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-ns-y-podselector-and-nsselector", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectAllIngress(NewPodString(nsX, "a"), false)
			reachability.Expect(NewPodString(nsY, "b"), NewPodString(nsX, "a"), true)
			reachability.Expect(NewPodString(nsZ, "b"), NewPodString(nsX, "a"), true)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should enforce policy based on Multiple PodSelectors and NamespaceSelectors", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// x/a 上的策略只允许来自除 X 外 namespace 中 {b,c} 的 pod 的 ingress。
			// 需要 x/b 证明同 namespace 流量被拒绝，y/b 和 y/c 作为允许的
			// 来源，y/a 作为允许 namespace 中不匹配的 pod。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a", "y/b", "y/c")
			nsX, nsY, _ := getK8sNamespaces(k8s)

			allowedNamespaces := &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      namespaceLabelKey,
					Operator: metav1.LabelSelectorOpNotIn,
					Values:   []string{nsX},
				}},
			}
			allowedPod := &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      "pod",
					Operator: metav1.LabelSelectorOpIn,
					Values:   []string{"b", "c"},
				}},
			}
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{NamespaceSelector: allowedNamespaces, PodSelector: allowedPod})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-ns-y-z-pod-b-c", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectPeer(&Peer{Namespace: nsX}, &Peer{Namespace: nsX, Pod: "a"}, false)
			reachability.Expect(NewPodString(nsY, "a"), NewPodString(nsX, "a"), false)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should enforce policy based on any PodSelectors", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// x/a 上的策略使用多个 "from" 条目（pod=b 或 pod=c）。需要
			// x/b 和 x/c 作为允许的来源，x/a 作为被隔离的目标 pod。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "x/c")
			nsX, _, _ := getK8sNamespaces(k8s)

			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			for _, label := range []map[string]string{{"pod": "b"}, {"pod": "c"}} {
				ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{PodSelector: &metav1.LabelSelector{MatchLabels: label}})
			}
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-ns-x-pod-b-c", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectAllIngress(NewPodString(nsX, "a"), false)

			// 从 namespace nsX 把 Pod b 和 c 连到 pod a
			reachability.Expect(NewPodString(nsX, "b"), NewPodString(nsX, "a"), true)
			reachability.Expect(NewPodString(nsX, "c"), NewPodString(nsX, "a"), true)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should enforce policy to allow traffic only from a pod in a different namespace based on PodSelector and NamespaceSelector", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// x/a 上的策略只允许来自 y/a 的 ingress（namespace+pod selector）。保留
			// x/b 作为同 namespace/不匹配的 pod，y/b 和 z/a 作为不匹配的
			// 跨 namespace pod，确保它们无法访问 x/a。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a", "y/b", "z/a")
			nsX, nsY, _ := getK8sNamespaces(k8s)

			allowedNamespaces := &metav1.LabelSelector{
				MatchLabels: map[string]string{
					namespaceLabelKey: nsY,
				},
			}
			allowedPods := &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"pod": "a",
				},
			}
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{NamespaceSelector: allowedNamespaces, PodSelector: allowedPods})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-ns-y-pod-a-via-namespace-pod-selector", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectAllIngress(NewPodString(nsX, "a"), false)
			reachability.Expect(NewPodString(nsY, "a"), NewPodString(nsX, "a"), true)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should enforce policy based on Ports", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{81}
			// 这个测试是按端口的：namespace X 只允许来自 namespace Y 的
			// 端口 81 ingress 到 x/a。我们用 x/b 作为同 namespace 来源
			// （拒绝），y/a、y/b 作为 namespace Y 的来源（允许），z/a 作为被拒绝的
			// 第三 namespace 来源。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a", "y/b", "z/a")
			nsX, nsY, nsZ := getK8sNamespaces(k8s)

			ginkgo.By("Creating a network allowPort81Policy which only allows allow listed namespaces (y) to connect on exactly one port (81)")
			allowedLabels := &metav1.LabelSelector{
				MatchLabels: map[string]string{
					namespaceLabelKey: nsY,
				},
			}
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{NamespaceSelector: allowedLabels})
			ingressRule.Ports = append(ingressRule.Ports, networkingv1.NetworkPolicyPort{Port: &intstr.IntOrString{IntVal: 81}, Protocol: &protocolTCP})
			allowPort81Policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-client-a-via-ns-selector", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, allowPort81Policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectPeer(&Peer{Namespace: nsX}, &Peer{Namespace: nsX, Pod: "a"}, false)
			reachability.ExpectPeer(&Peer{Namespace: nsY}, &Peer{Namespace: nsX, Pod: "a"}, true)
			reachability.ExpectPeer(&Peer{Namespace: nsZ}, &Peer{Namespace: nsX, Pod: "a"}, false)

			ValidateOrFail(k8s, &TestCase{ToPort: 81, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should enforce multiple, stacked policies with overlapping podSelectors", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80, 81}
			// 我们叠加多个策略来验证按端口的行为和策略优先级。
			// 需要 x/a 作为目标，x/b 作为同 namespace 来源，y/a、y/b 作为
			// 允许的 namespace，z/a 作为必须保持拒绝的 namespace。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a", "y/b", "z/a")
			nsX, nsY, nsZ := getK8sNamespaces(k8s)

			ginkgo.By("Creating a network allowPort81Policy which only allows allow listed namespaces (y) to connect on exactly one port (81)")
			allowedLabels := &metav1.LabelSelector{
				MatchLabels: map[string]string{
					namespaceLabelKey: nsY,
				},
			}
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{NamespaceSelector: allowedLabels})
			ingressRule.Ports = append(ingressRule.Ports, networkingv1.NetworkPolicyPort{Port: &intstr.IntOrString{IntVal: 81}, Protocol: &protocolTCP})
			allowPort81Policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-client-a-via-ns-selector", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, allowPort81Policy, nsX)

			reachabilityALLOW := NewReachability(k8s.AllPodStrings(), true)
			reachabilityALLOW.ExpectPeer(&Peer{Namespace: nsX}, &Peer{Namespace: nsX, Pod: "a"}, false)
			reachabilityALLOW.ExpectPeer(&Peer{Namespace: nsY}, &Peer{Namespace: nsX, Pod: "a"}, true)
			reachabilityALLOW.ExpectPeer(&Peer{Namespace: nsZ}, &Peer{Namespace: nsX, Pod: "a"}, false)

			ginkgo.By("Verifying traffic on port 81.")
			ValidateOrFail(k8s, &TestCase{ToPort: 81, Protocol: v1.ProtocolTCP, Reachability: reachabilityALLOW})

			reachabilityDENY := NewReachability(k8s.AllPodStrings(), true)
			reachabilityDENY.ExpectAllIngress(NewPodString(nsX, "a"), false)

			ginkgo.By("Verifying traffic on port 80.")
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachabilityDENY})

			ingressRule = networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{NamespaceSelector: allowedLabels})
			ingressRule.Ports = append(ingressRule.Ports, networkingv1.NetworkPolicyPort{Port: &intstr.IntOrString{IntVal: 80}, Protocol: &protocolTCP})
			allowPort80Policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-client-a-via-ns-selector-80", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, allowPort80Policy, nsX)

			ginkgo.By("Verifying that we can add a policy to unblock port 80")
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachabilityALLOW})
		})

		f.It("should support allow-all policy", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80, 81}
			// allow-all 不应限制任何东西。我们在 X 中放两个 pod、Y 中放一个，
			// 以覆盖同 namespace（x/a<->x/b）和跨 namespace 流量。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a")
			nsX, _, _ := getK8sNamespaces(k8s)

			ginkgo.By("Creating a network policy which allows all traffic.")
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-all", map[string]string{}, SetSpecIngressRules(networkingv1.NetworkPolicyIngressRule{}))
			CreatePolicy(ctx, k8s, policy, nsX)

			ginkgo.By("Testing pods can connect to both ports when an 'allow-all' policy is present.")
			reachability := NewReachability(k8s.AllPodStrings(), true)
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
			ValidateOrFail(k8s, &TestCase{ToPort: 81, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should allow ingress access on one named port", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80, 81}
			// 策略作用于 X 中所有 pod，只允许命名端口 serve-81-tcp。
			// 需要 X 中两个 pod 验证 X 到 X 流量在端口 80 被阻断，
			// Y 中一个 pod 验证跨 namespace ingress 也按预期受限。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a")
			nsX, _, _ := getK8sNamespaces(k8s)

			ginkgo.By("Blocking all ports other then 81 in the entire namespace")
			IngressRules := networkingv1.NetworkPolicyIngressRule{}
			IngressRules.Ports = append(IngressRules.Ports, networkingv1.NetworkPolicyPort{Port: &intstr.IntOrString{Type: intstr.String, StrVal: "serve-81-tcp"}})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-all", map[string]string{}, SetSpecIngressRules(IngressRules))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachabilityPort81 := NewReachability(k8s.AllPodStrings(), true)
			ValidateOrFail(k8s, &TestCase{ToPort: 81, Protocol: v1.ProtocolTCP, Reachability: reachabilityPort81})

			// 拒绝所有到 x namespace 的流量
			reachabilityPort80 := NewReachability(k8s.AllPodStrings(), true)
			reachabilityPort80.ExpectPeer(&Peer{}, &Peer{Namespace: nsX}, false)
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachabilityPort80})
		})

		f.It("should allow ingress access from namespace on one named port", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80, 81}
			// 只有 namespace Y 应能通过命名端口 serve-80-tcp 访问 x/a，
			// 端口 81 应对所有人阻断。我们用 x/b 作为
			// 同 namespace peer（拒绝），y/a 作为允许的 namespace，z/a 作为
			// 被拒绝的 namespace。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a", "z/a")
			nsX, nsY, nsZ := getK8sNamespaces(k8s)

			allowedLabels := &metav1.LabelSelector{
				MatchLabels: map[string]string{
					namespaceLabelKey: nsY,
				},
			}
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{NamespaceSelector: allowedLabels})
			ingressRule.Ports = append(ingressRule.Ports, networkingv1.NetworkPolicyPort{Port: &intstr.IntOrString{Type: intstr.String, StrVal: "serve-80-tcp"}, Protocol: &protocolTCP})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-client-a-via-ns-selector-80", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			// 拒绝来自 x 或 z namespace 的所有流量
			reachability.ExpectPeer(&Peer{Namespace: nsX}, &Peer{Namespace: nsX, Pod: "a"}, false)
			reachability.ExpectPeer(&Peer{Namespace: nsZ}, &Peer{Namespace: nsX, Pod: "a"}, false)

			ginkgo.By("Verify that port 80 is allowed for namespace y")
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})

			ginkgo.By("Verify that port 81 is blocked for all namespaces including y")
			reachabilityFAIL := NewReachability(k8s.AllPodStrings(), true)
			reachabilityFAIL.ExpectAllIngress(NewPodString(nsX, "a"), false)
			ValidateOrFail(k8s, &TestCase{ToPort: 81, Protocol: v1.ProtocolTCP, Reachability: reachabilityFAIL})
		})

		f.It("should allow egress access on one named port", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80, 81}
			// 策略按命名端口限制 x/a 的 egress：端口 80 允许，端口 81
			// 拒绝。X 中一个 pod 和 Y 中一个 pod 足以验证 egress
			// 行为。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "y/a")
			nsX, _, _ := getK8sNamespaces(k8s)

			egressRule := networkingv1.NetworkPolicyEgressRule{}
			egressRule.Ports = append(egressRule.Ports, networkingv1.NetworkPolicyPort{Port: &intstr.IntOrString{Type: intstr.String, StrVal: "serve-80-tcp"}})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-egress", map[string]string{}, SetSpecEgressRules(egressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachabilityPort80 := NewReachability(k8s.AllPodStrings(), true)
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachabilityPort80})

			// 同时端口 81 上不应有任何流量，因为 egress 策略是在 80 上
			reachabilityPort81 := NewReachability(k8s.AllPodStrings(), true)
			reachabilityPort81.ExpectPeer(&Peer{Namespace: nsX}, &Peer{}, false)
			ValidateOrFail(k8s, &TestCase{ToPort: 81, Protocol: v1.ProtocolTCP, Reachability: reachabilityPort81})
		})

		// 这个测试*确实*适用于不实现命名端口的插件
		// （测试标题特意不含 "named port" 字符串，以免
		// 被跳过此类测试的插件意外跳过）。如果测试失败，说明存在
		// 安全漏洞，因为你放行了本应被阻断的流量。
		// because you are allowing traffic that is supposed to be blocked.
		f.It("should not allow all ports if it cannot limit to the requested port", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// 一个 server（x/a）和一个 client（y/a）就足够了
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "y/a")
			nsX, _, _ := getK8sNamespaces(k8s)

			ginkgo.By("Creating a network policy for the server which allows traffic only on a (non-existent) named port.")
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.Ports = append(ingressRule.Ports, networkingv1.NetworkPolicyPort{Port: &intstr.IntOrString{Type: intstr.String, StrVal: "no-such-port"}, Protocol: &protocolTCP})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-ingress-on-nonexistent-port", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			ginkgo.By("Trying to connect to TCP port 80, which should be blocked by implicit isolation.")
			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectAllIngress(NewPodString(nsX, "a"), false)
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should enforce updated policy", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{81}
			// 这个测试修改 namespace X 中的策略并验证连通性变化
			// 从允许变为拒绝。X 中一个 pod 和 Y 中一个足以观察
			// 修改前后的行为。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "y/a")
			nsX, _, _ := getK8sNamespaces(k8s)

			ginkgo.By("Using the simplest possible mutation: start with allow all, then switch to deny all")
			// 第 1 部分）全部允许
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-all-mutate-to-deny-all", map[string]string{}, SetSpecIngressRules(networkingv1.NetworkPolicyIngressRule{}))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			ValidateOrFail(k8s, &TestCase{ToPort: 81, Protocol: v1.ProtocolTCP, Reachability: reachability})

			// 第 2 部分）更新策略为全部拒绝
			policy.Spec.Ingress = []networkingv1.NetworkPolicyIngressRule{}
			UpdatePolicy(ctx, k8s, policy, nsX)

			reachabilityDeny := NewReachability(k8s.AllPodStrings(), true)
			reachabilityDeny.ExpectPeer(&Peer{}, &Peer{Namespace: nsX}, false)
			ValidateOrFail(k8s, &TestCase{ToPort: 81, Protocol: v1.ProtocolTCP, Reachability: reachabilityDeny})
		})

		f.It("should allow ingress access from updated namespace", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// 这里测试的是 namespace 标签变化。需要 x/a 作为
			// 目标，y/a 作为要更新 namespace 标签的外部 client。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "y/a")
			nsX, nsY, _ := getK8sNamespaces(k8s)
			ginkgo.DeferCleanup(DeleteNamespaceLabel, k8s, nsY, "ns2")

			allowedLabels := &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"ns2": "updated",
				},
			}
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{NamespaceSelector: allowedLabels})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-client-a-via-ns-selector", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectAllIngress(NewPodString(nsX, "a"), false)
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})

			// 添加新标签
			AddNamespaceLabel(ctx, k8s, nsY, "ns2", "updated")

			// 来自 namespace 'y' 的任何流量都应能访问 x/a
			reachabilityWithLabel := NewReachability(k8s.AllPodStrings(), true)
			reachabilityWithLabel.ExpectAllIngress(NewPodString(nsX, "a"), false)
			reachabilityWithLabel.ExpectPeer(&Peer{Namespace: nsY}, &Peer{}, true)
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachabilityWithLabel})
		})

		f.It("should allow ingress access from updated pod", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// 这里测试的是 pod 标签变化。需要 x/a 作为目标，
			// x/b 作为标签被更新以匹配 PodSelector 的 client。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b")
			nsX, _, _ := getK8sNamespaces(k8s)
			ginkgo.DeferCleanup(ResetPodLabels, k8s, nsX, "b")

			// 添加新标签
			matchLabels := map[string]string{"pod": "b", "pod2": "updated"}
			allowedLabels := &metav1.LabelSelector{MatchLabels: matchLabels}
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{PodSelector: allowedLabels})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-client-a-via-pod-selector", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectAllIngress(NewPodString(nsX, "a"), false)
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})

			AddPodLabels(ctx, k8s, nsX, "b", matchLabels)

			ginkgo.By("x/b is able to reach x/a when label is updated")

			reachabilityWithLabel := NewReachability(k8s.AllPodStrings(), true)
			reachabilityWithLabel.ExpectAllIngress(NewPodString(nsX, "a"), false)
			reachabilityWithLabel.Expect(NewPodString(nsX, "b"), NewPodString(nsX, "a"), true)
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachabilityWithLabel})
		})

		f.It("should deny ingress from pods on other namespaces", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// X 中的 Ingress 策略应只允许同 namespace 的 pod。需要 X 中两个 pod
			// 验证 X 到 X 被允许，Y 和 Z 中各一个 pod 验证
			// 进入 X 的跨 namespace ingress 被拒绝。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a", "z/a")
			nsX, nsY, nsZ := getK8sNamespaces(k8s)

			IngressRules := networkingv1.NetworkPolicyIngressRule{}
			IngressRules.From = append(IngressRules.From, networkingv1.NetworkPolicyPeer{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{}}})
			policy := GenNetworkPolicyWithNameAndPodSelector("deny-empty-policy", metav1.LabelSelector{}, SetSpecIngressRules(IngressRules))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectPeer(&Peer{Namespace: nsY}, &Peer{Namespace: nsX}, false)
			reachability.ExpectPeer(&Peer{Namespace: nsZ}, &Peer{Namespace: nsX}, false)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should deny ingress access to updated pod", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// 这个测试验证 x/a 的标签更新为匹配 deny-ingress 策略后变为
			// 被隔离。需要 x/a 作为目标，y/a 作为外部
			// 来源。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "y/a")
			nsX, _, _ := getK8sNamespaces(k8s)
			ginkgo.DeferCleanup(ResetPodLabels, k8s, nsX, "a")

			policy := GenNetworkPolicyWithNameAndPodSelector("deny-ingress-via-label-selector",
				metav1.LabelSelector{MatchLabels: map[string]string{"target": "isolated"}}, SetSpecIngressRules())
			CreatePolicy(ctx, k8s, policy, nsX)

			ginkgo.By("Verify that everything can reach x/a")
			reachability := NewReachability(k8s.AllPodStrings(), true)
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})

			AddPodLabels(ctx, k8s, nsX, "a", map[string]string{"target": "isolated"})

			reachabilityIsolated := NewReachability(k8s.AllPodStrings(), true)
			reachabilityIsolated.ExpectAllIngress(NewPodString(nsX, "a"), false)
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachabilityIsolated})
		})

		f.It("should deny egress from pods based on PodSelector", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// Egress 策略选中 x/a 并拒绝所有 egress。用 x/b 验证
			// 未被选中的 pod 不受影响，加上 y/a 作为跨 namespace 目的地。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a")
			nsX, _, _ := getK8sNamespaces(k8s)

			policy := GenNetworkPolicyWithNameAndPodSelector("deny-egress-pod-a", metav1.LabelSelector{MatchLabels: map[string]string{"pod": "a"}}, SetSpecEgressRules())
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectAllEgress(NewPodString(nsX, "a"), false)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should deny egress from all pods in a namespace", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// X 中 namespace 级别的 egress 拒绝：用 x/b 验证 x/a->x/b 被拒绝，
			// 用 y/a 验证从 X 出发的跨 namespace egress 也被拒绝。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a")
			nsX, _, _ := getK8sNamespaces(k8s)

			policy := GenNetworkPolicyWithNameAndPodSelector("deny-egress-ns-x", metav1.LabelSelector{}, SetSpecEgressRules())
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectPeer(&Peer{Namespace: nsX}, &Peer{}, false)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should work with Ingress, Egress specified together", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80, 81}
			// x/a 上的策略只允许来自 x/a 和 x/b 的 ingress，且只允许 egress 到
			// 端口 80。用 x/c 作为同 namespace 不允许的 ingress 来源，y/a
			// 作为 egress 检查的跨 namespace 目的地。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "x/c", "y/a")
			nsX, _, _ := getK8sNamespaces(k8s)

			allowedPodLabels := &metav1.LabelSelector{MatchLabels: map[string]string{"pod": "b"}}
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{PodSelector: allowedPodLabels})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-client-a-via-pod-selector", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))

			// 给它加一条 egress 规则...
			policy.Spec.Egress = []networkingv1.NetworkPolicyEgressRule{
				{
					Ports: []networkingv1.NetworkPolicyPort{
						{
							// 不使用命名端口
							Port: &intstr.IntOrString{Type: intstr.Int, IntVal: 80},
						},
					},
				},
			}
			policy.Spec.PolicyTypes = []networkingv1.PolicyType{networkingv1.PolicyTypeEgress, networkingv1.PolicyTypeIngress}
			CreatePolicy(ctx, k8s, policy, nsX)

			reachabilityPort80 := NewReachability(k8s.AllPodStrings(), true)
			reachabilityPort80.ExpectAllIngress(NewPodString(nsX, "a"), false)
			reachabilityPort80.Expect(NewPodString(nsX, "b"), NewPodString(nsX, "a"), true)
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachabilityPort80})

			ginkgo.By("validating that port 81 doesn't work")
			// 同时端口 81 上不应有 egress 流量，因为 egress 策略是在 80 上
			reachabilityPort81 := NewReachability(k8s.AllPodStrings(), true)
			reachabilityPort81.ExpectAllIngress(NewPodString(nsX, "a"), false)
			reachabilityPort81.ExpectAllEgress(NewPodString(nsX, "a"), false)
			reachabilityPort81.Expect(NewPodString(nsX, "b"), NewPodString(nsX, "a"), true)
			ValidateOrFail(k8s, &TestCase{ToPort: 81, Protocol: v1.ProtocolTCP, Reachability: reachabilityPort81})
		})

		f.It("should support denying of egress traffic on the client side (even if the server explicitly allows this traffic)", feature.NetworkPolicy, func(ctx context.Context) {
			// x/a --> y/a 和 y/b
			// 只允许 egress 到 y/a，到 y/b 的 egress 应被阻断
			// y/a 和 y/b 上的 Ingress 允许来自 x/a 的流量
			// 预期：x/a 到 y/a 的流量允许，x/a 到 y/b 的流量被 egress 策略拒绝

			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// 需要 x/a 作为被 egress 策略选中的 client，Y 中两个 pod 以便
			// 展示 y/a 是允许的目的地，而 y/b 被 egress 拒绝，即使
			// server 端的 ingress 允许两者。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "y/a", "y/b")
			nsX, nsY, _ := getK8sNamespaces(k8s)

			// 构建只允许 x/a 到 y/a 的 egress 策略
			allowedEgressNamespaces := &metav1.LabelSelector{
				MatchLabels: map[string]string{
					namespaceLabelKey: nsY,
				},
			}
			allowedEgressPods := &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"pod": "a",
				},
			}
			egressRule1 := networkingv1.NetworkPolicyEgressRule{}
			egressRule1.To = append(egressRule1.To, networkingv1.NetworkPolicyPeer{NamespaceSelector: allowedEgressNamespaces, PodSelector: allowedEgressPods})
			egressPolicy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-to-ns-y-pod-a", map[string]string{"pod": "a"}, SetSpecEgressRules(egressRule1))
			CreatePolicy(ctx, k8s, egressPolicy, nsX)

			// 创建 ingress 策略，允许从 x/a 到 y/a 和 y/b
			allowedIngressNamespaces := &metav1.LabelSelector{
				MatchLabels: map[string]string{
					namespaceLabelKey: nsX,
				},
			}
			allowedIngressPods := &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"pod": "a",
				},
			}
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{NamespaceSelector: allowedIngressNamespaces, PodSelector: allowedIngressPods})
			allowIngressPolicyPodA := GenNetworkPolicyWithNameAndPodMatchLabel("allow-from-xa-on-ya-match-selector", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			allowIngressPolicyPodB := GenNetworkPolicyWithNameAndPodMatchLabel("allow-from-xa-on-yb-match-selector", map[string]string{"pod": "b"}, SetSpecIngressRules(ingressRule))

			CreatePolicy(ctx, k8s, allowIngressPolicyPodA, nsY)
			CreatePolicy(ctx, k8s, allowIngressPolicyPodB, nsY)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectPeer(&Peer{Namespace: nsX, Pod: "a"}, &Peer{}, false)
			reachability.ExpectPeer(&Peer{}, &Peer{Namespace: nsY, Pod: "a"}, false)
			reachability.ExpectPeer(&Peer{Namespace: nsX, Pod: "a"}, &Peer{Namespace: nsY, Pod: "a"}, true)
			reachability.ExpectPeer(&Peer{}, &Peer{Namespace: nsY, Pod: "b"}, false)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should enforce egress policy allowing traffic to a server in a different namespace based on PodSelector and NamespaceSelector", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// x/a 上的 Egress 策略只允许到 y/a 的流量（namespace+pod selector）。
			// 用 y/b 作为不匹配的目的地，确保它被拒绝。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "y/a", "y/b")
			nsX, nsY, _ := getK8sNamespaces(k8s)

			allowedNamespaces := &metav1.LabelSelector{
				MatchLabels: map[string]string{
					namespaceLabelKey: nsY,
				},
			}
			allowedPods := &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"pod": "a",
				},
			}
			egressRule1 := networkingv1.NetworkPolicyEgressRule{}
			egressRule1.To = append(egressRule1.To, networkingv1.NetworkPolicyPeer{NamespaceSelector: allowedNamespaces, PodSelector: allowedPods})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-to-ns-y-pod-a", map[string]string{"pod": "a"}, SetSpecEgressRules(egressRule1))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectAllEgress(NewPodString(nsX, "a"), false)
			reachability.Expect(NewPodString(nsX, "a"), NewPodString(nsY, "a"), true)
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should enforce ingress policy allowing any port traffic to a server on a specific protocol", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP, protocolUDP}
			ports := []int32{80}
			// 仅按协议的 ingress allowlist：一个 server（x/a）和一个 client（y/a）
			// 足以验证 TCP 被允许而 UDP 被拒绝。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "y/a")
			nsX, _, _ := getK8sNamespaces(k8s)

			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.Ports = append(ingressRule.Ports, networkingv1.NetworkPolicyPort{Protocol: &protocolTCP})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-ingress-by-proto", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachabilityTCP := NewReachability(k8s.AllPodStrings(), true)
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachabilityTCP})

			reachabilityUDP := NewReachability(k8s.AllPodStrings(), true)
			reachabilityUDP.ExpectPeer(&Peer{}, &Peer{Namespace: nsX, Pod: "a"}, false)
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolUDP, Reachability: reachabilityUDP})
		})

		f.It("should enforce multiple ingress policies with ingress allow-all policy taking precedence", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{81}
			// 只需要 x/a 作为策略目标，y/a 作为 client，就能观察
			// allow-all ingress 策略覆盖了更严格的 ingress 策略。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "y/a")
			nsX, _, _ := getK8sNamespaces(k8s)

			IngressRules := networkingv1.NetworkPolicyIngressRule{}
			IngressRules.Ports = append(IngressRules.Ports, networkingv1.NetworkPolicyPort{Port: &intstr.IntOrString{Type: intstr.Int, IntVal: 80}})
			policyAllowOnlyPort80 := GenNetworkPolicyWithNameAndPodMatchLabel("allow-ingress-port-80", map[string]string{}, SetSpecIngressRules(IngressRules))
			CreatePolicy(ctx, k8s, policyAllowOnlyPort80, nsX)

			ginkgo.By("The policy targets port 80 -- so let's make sure traffic on port 81 is blocked")

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectPeer(&Peer{}, &Peer{Namespace: nsX}, false)
			ValidateOrFail(k8s, &TestCase{ToPort: 81, Protocol: v1.ProtocolTCP, Reachability: reachability})

			ginkgo.By("Allowing all ports")

			policyAllowAll := GenNetworkPolicyWithNameAndPodMatchLabel("allow-ingress", map[string]string{}, SetSpecIngressRules(networkingv1.NetworkPolicyIngressRule{}))
			CreatePolicy(ctx, k8s, policyAllowAll, nsX)

			reachabilityAll := NewReachability(k8s.AllPodStrings(), true)
			ValidateOrFail(k8s, &TestCase{ToPort: 81, Protocol: v1.ProtocolTCP, Reachability: reachabilityAll})
		})

		f.It("should enforce multiple egress policies with egress allow-all policy taking precedence", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{81}
			// 只需要 x/a 作为策略目标，y/a 作为目的地，就能观察
			// allow-all egress 策略覆盖了更严格的 egress 策略。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "y/a")
			nsX, _, _ := getK8sNamespaces(k8s)

			egressRule := networkingv1.NetworkPolicyEgressRule{}
			egressRule.Ports = append(egressRule.Ports, networkingv1.NetworkPolicyPort{Port: &intstr.IntOrString{Type: intstr.Int, IntVal: 80}})
			policyAllowPort80 := GenNetworkPolicyWithNameAndPodMatchLabel("allow-egress-port-80", map[string]string{}, SetSpecEgressRules(egressRule))
			CreatePolicy(ctx, k8s, policyAllowPort80, nsX)

			ginkgo.By("Making sure ingress doesn't work other than port 80")

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectPeer(&Peer{Namespace: nsX}, &Peer{}, false)
			ValidateOrFail(k8s, &TestCase{ToPort: 81, Protocol: v1.ProtocolTCP, Reachability: reachability})

			ginkgo.By("Allowing all ports")

			policyAllowAll := GenNetworkPolicyWithNameAndPodMatchLabel("allow-egress", map[string]string{}, SetSpecEgressRules(networkingv1.NetworkPolicyEgressRule{}))
			CreatePolicy(ctx, k8s, policyAllowAll, nsX)

			reachabilityAll := NewReachability(k8s.AllPodStrings(), true)
			ValidateOrFail(k8s, &TestCase{ToPort: 81, Protocol: v1.ProtocolTCP, Reachability: reachabilityAll})
		})

		f.It("should stop enforcing policies after they are deleted", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// 一对 client/server 足以验证策略存在时连通性被拒绝，
			// 策略删除后恢复允许。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "y/a")
			nsX, _, _ := getK8sNamespaces(k8s)

			ginkgo.By("Creating a network policy for the server which denies all traffic.")
			// 拒绝所有进出 "x" 的流量。
			policy := GenNetworkPolicyWithNameAndPodSelector("deny-all", metav1.LabelSelector{}, SetSpecIngressRules(), SetSpecEgressRules())
			CreatePolicy(ctx, k8s, policy, nsX)
			reachability := NewReachability(k8s.AllPodStrings(), true)

			// 预期所有进出 "x" 的流量都为 False。
			reachability.ExpectPeer(&Peer{Namespace: nsX}, &Peer{}, false)
			reachability.ExpectPeer(&Peer{}, &Peer{Namespace: nsX}, false)
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})

			err := k8s.cleanNetworkPolicies(ctx)
			time.Sleep(3 * time.Second) // TODO we can remove this eventually, its just a hack to keep CI stable.
			framework.ExpectNoError(err, "unable to clean network policies")

			// 现在策略已删除，预期所有连通性恢复。
			reachabilityAll := NewReachability(k8s.AllPodStrings(), true)
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachabilityAll})
		})

		// TODO: 弄清楚接下来 3 个测试在双栈下该如何工作：是否需要不同于单纯 "podIP" 的抽象？

		f.It("should allow egress access to server in CIDR block", feature.NetworkPolicy, func(ctx context.Context) {
			// 获取 podServer 的 status 以拿到它的 IP，用来构造 CIDR
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// 这个测试需要真实 pod 的 IP 来构造 IPBlock CIDR。创建 y/b
			// 作为 server（提供 IP），y/a 作为不匹配的目的地，x/a
			// 作为 egress 受限的 client。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "y/a", "y/b")
			nsX, nsY, _ := getK8sNamespaces(k8s)

			podList, err := f.ClientSet.CoreV1().Pods(nsY).List(ctx, metav1.ListOptions{LabelSelector: "pod=b"})
			framework.ExpectNoError(err, "Failing to list pods in namespace y")
			pod := podList.Items[0]

			hostMask := 32
			if utilnet.IsIPv6String(pod.Status.PodIP) {
				hostMask = 128
			}
			podServerCIDR := fmt.Sprintf("%s/%d", pod.Status.PodIP, hostMask)
			egressRule1 := networkingv1.NetworkPolicyEgressRule{}
			egressRule1.To = append(egressRule1.To, networkingv1.NetworkPolicyPeer{IPBlock: &networkingv1.IPBlock{CIDR: podServerCIDR}})
			policyAllowCIDR := GenNetworkPolicyWithNameAndPodMatchLabel("allow-client-a-via-cidr-egress-rule",
				map[string]string{"pod": "a"}, SetSpecEgressRules(egressRule1))
			CreatePolicy(ctx, k8s, policyAllowCIDR, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectAllEgress(NewPodString(nsX, "a"), false)
			reachability.Expect(NewPodString(nsX, "a"), NewPodString(nsY, "b"), true)
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should enforce except clause while egress access to server in CIDR block", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// 需要 x/b 提供 IPBlock 使用的 IP（并作为目的地），
			// x/a 作为 client，x/c 验证非例外流量仍然可用。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "x/c")
			nsX, _, _ := getK8sNamespaces(k8s)

			// 获取 podServer 的 status 以拿到它的 IP，用来构造带 except 子句的 CIDR
			podList, err := f.ClientSet.CoreV1().Pods(nsX).List(ctx, metav1.ListOptions{LabelSelector: "pod=b"})
			framework.ExpectNoError(err, "Failing to find pod x/b")
			podB := podList.Items[0]

			// 创建一条规则，允许 egress 到 podB 周围
			// 一大片 IP，但不包括 podB 本身。

			podServerAllowCIDR := makeLargeCIDRForIP(podB.Status.PodIP)
			hostMask := 32
			if utilnet.IsIPv6String(podB.Status.PodIP) {
				hostMask = 128
			}
			podServerExceptList := []string{fmt.Sprintf("%s/%d", podB.Status.PodIP, hostMask)}

			egressRule1 := networkingv1.NetworkPolicyEgressRule{}
			egressRule1.To = append(egressRule1.To, networkingv1.NetworkPolicyPeer{IPBlock: &networkingv1.IPBlock{CIDR: podServerAllowCIDR, Except: podServerExceptList}})
			policyAllowCIDR := GenNetworkPolicyWithNameAndPodMatchLabel("allow-client-a-via-cidr-egress-rule", map[string]string{"pod": "a"}, SetSpecEgressRules(egressRule1))

			CreatePolicy(ctx, k8s, policyAllowCIDR, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.Expect(NewPodString(nsX, "a"), NewPodString(nsX, "b"), false)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should ensure an IP overlapping both IPBlock.CIDR and IPBlock.Except is allowed", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// 需要 x/b 提供 IPBlock 使用的 IP（并作为目的地），
			// x/a 作为 client，x/c 验证非例外流量对
			// CIDR/Except 重叠行为仍然有效。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "x/c")
			nsX, _, _ := getK8sNamespaces(k8s)

			// 获取 podServer 的 status 以拿到它的 IP，用来构造带 except 子句的 CIDR
			podList, err := f.ClientSet.CoreV1().Pods(nsX).List(ctx, metav1.ListOptions{LabelSelector: "pod=b"})
			framework.ExpectNoError(err, "Failing to find pod x/b")
			podB := podList.Items[0]

			// 创建一条规则，允许 egress 到 podB 周围
			// 一大片 IP，但不包括 podB 本身。

			podServerAllowCIDR := makeLargeCIDRForIP(podB.Status.PodIP)
			hostMask := 32
			if utilnet.IsIPv6String(podB.Status.PodIP) {
				hostMask = 128
			}
			podServerExceptList := []string{fmt.Sprintf("%s/%d", podB.Status.PodIP, hostMask)}
			egressRule1 := networkingv1.NetworkPolicyEgressRule{}
			egressRule1.To = append(egressRule1.To, networkingv1.NetworkPolicyPeer{IPBlock: &networkingv1.IPBlock{CIDR: podServerAllowCIDR, Except: podServerExceptList}})
			policyAllowCIDR := GenNetworkPolicyWithNameAndPodMatchLabel("allow-client-a-via-cidr-egress-rule",
				map[string]string{"pod": "a"}, SetSpecEgressRules(egressRule1))
			CreatePolicy(ctx, k8s, policyAllowCIDR, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.Expect(NewPodString(nsX, "a"), NewPodString(nsX, "b"), false)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})

			// 创建第二个 NetworkPolicy 允许访问 podB
			podBIP := fmt.Sprintf("%s/%d", podB.Status.PodIP, hostMask)
			egressRule3 := networkingv1.NetworkPolicyEgressRule{}
			egressRule3.To = append(egressRule3.To, networkingv1.NetworkPolicyPeer{IPBlock: &networkingv1.IPBlock{CIDR: podBIP}})
			allowPolicy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-client-a-via-cidr-egress-rule",
				map[string]string{"pod": "a"}, SetSpecEgressRules(egressRule3))
			// 这该用 UPDATE 还是 CREATE？JAY TESTING 10/31
			UpdatePolicy(ctx, k8s, allowPolicy, nsX)

			reachabilityAllow := NewReachability(k8s.AllPodStrings(), true)
			reachabilityAllow.ExpectAllEgress(NewPodString(nsX, "a"), false)
			reachabilityAllow.Expect(NewPodString(nsX, "a"), NewPodString(nsX, "b"), true)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachabilityAllow})
		})

		f.It("should enforce policies to check ingress and egress policies can be controlled independently based on PodSelector", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// 策略目标是 x/a。需要 x/b 验证 a->b 允许而 b->a 可以
			// 被拒绝，y/a 作为外部 pod，使初始的"所有流量允许"步骤
			// 也覆盖跨 namespace 连通性。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a")
			nsX, _, _ := getK8sNamespaces(k8s)

			/*
					测试步骤：
					1. 验证每个 namespace 中的每个 pod 都能互相通信
				          - 包括 a -> b 和 b -> a
					2. 创建一条策略允许 egress a -> b（target = a）
				    3. 创建一条策略*拒绝* ingress b -> a（target = a）
					4. 验证 a -> b 允许；b -> a 被阻断
			*/
			targetLabels := map[string]string{"pod": "a"}

			ginkgo.By("Creating a network policy for pod-a which allows Egress traffic to pod-b.")

			allowEgressPolicy := GenNetworkPolicyWithNameAndPodSelector("allow-egress-for-target",
				metav1.LabelSelector{MatchLabels: targetLabels}, SetSpecEgressRules(networkingv1.NetworkPolicyEgressRule{}))
			CreatePolicy(ctx, k8s, allowEgressPolicy, nsX)

			allowEgressReachability := NewReachability(k8s.AllPodStrings(), true)
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: allowEgressReachability})

			ginkgo.By("Creating a network policy for pod-a that denies traffic from pod-b.")

			denyAllIngressPolicy := GenNetworkPolicyWithNameAndPodSelector("deny-ingress-via-label-selector", metav1.LabelSelector{MatchLabels: targetLabels}, SetSpecIngressRules())
			CreatePolicy(ctx, k8s, denyAllIngressPolicy, nsX)

			denyIngressToXReachability := NewReachability(k8s.AllPodStrings(), true)
			denyIngressToXReachability.ExpectAllIngress(NewPodString(nsX, "a"), false)
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: denyIngressToXReachability})
		})

		// 这个测试*确实*适用于不实现 SCTP 的插件。如果测试失败，说明存在
		// 安全漏洞，因为你放行了本应被阻断的 TCP
		// 流量。
		f.It("should not mistakenly treat 'protocol: SCTP' as 'protocol: TCP', even if the plugin doesn't support SCTP", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{81}
			// 一个 server（x/a）和一个 client（y/a）就足够：创建仅允许
			// SCTP 的策略，验证到同一端口的 TCP 仍被阻断。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "y/a")
			nsX, _, _ := getK8sNamespaces(k8s)

			ginkgo.By("Creating a default-deny ingress policy.")
			// 空的 podSelector 阻断整个 namespace
			policy := GenNetworkPolicyWithNameAndPodSelector("deny-ingress", metav1.LabelSelector{}, SetSpecIngressRules())
			CreatePolicy(ctx, k8s, policy, nsX)

			ginkgo.By("Creating a network policy for the server which allows traffic only via SCTP on port 81.")
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.Ports = append(ingressRule.Ports, networkingv1.NetworkPolicyPort{Port: &intstr.IntOrString{IntVal: 81}, Protocol: &protocolSCTP})
			policy = GenNetworkPolicyWithNameAndPodMatchLabel("allow-only-sctp-ingress-on-port-81", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			ginkgo.By("Trying to connect to TCP port 81, which should be blocked by the deny-ingress policy.")
			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectPeer(&Peer{}, &Peer{Namespace: nsX}, false)
			ValidateOrFail(k8s, &TestCase{ToPort: 81, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		// 这个测试*确实*适用于不实现 SCTP 的插件。如果测试失败，说明存在
		// 安全漏洞，因为你放行了本应被阻断的 TCP
		// 流量。
		f.It("should properly isolate pods that are selected by a policy allowing SCTP, even if the plugin doesn't support SCTP", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{81}
			// 一个 server（x/a）和一个 client（y/a）就足够：仅允许 SCTP 的规则
			// 仍应使 pod 在其他端口的 TCP 流量上保持隔离。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "y/a")
			nsX, _, _ := getK8sNamespaces(k8s)

			ginkgo.By("Creating a network policy for the server which allows traffic only via SCTP on port 80.")
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.Ports = append(ingressRule.Ports, networkingv1.NetworkPolicyPort{Port: &intstr.IntOrString{IntVal: 80}, Protocol: &protocolSCTP})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-only-sctp-ingress-on-port-80", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			ginkgo.By("Trying to connect to TCP port 81, which should be blocked by implicit isolation.")
			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectAllIngress(NewPodString(nsX, "a"), false)
			ValidateOrFail(k8s, &TestCase{ToPort: 81, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		f.It("should not allow access by TCP when a policy specifies only UDP", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{81}
			// 一个 server（x/a）和一个 client（y/a）就足够：仅允许 UDP 的规则
			// 不得意外放行 TCP 连通性。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "y/a")
			nsX, _, _ := getK8sNamespaces(k8s)

			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.Ports = append(ingressRule.Ports, networkingv1.NetworkPolicyPort{Port: &intstr.IntOrString{IntVal: 81}, Protocol: &protocolUDP})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-only-udp-ingress-on-port-81", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			ginkgo.By("Creating a network policy for the server which allows traffic only via UDP on port 81.")

			// 用 TCP 探测，因此所有流量都应被丢弃。
			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectAllIngress(NewPodString(nsX, "a"), false)
			ValidateOrFail(k8s, &TestCase{ToPort: 81, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		// 注意这个默认 namespace 功能由 APIMachinery 组维护，但我们仍在这里测试，因为它是重要特性。
		f.It("should enforce policy to allow traffic based on NamespaceSelector with MatchLabels using default ns label", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// x/a 上的策略只允许来自 namespace Y 的 ingress（使用默认 ns 名称
			// 标签）。需要 x/b（同 namespace）和 z/a（其他 namespace）作为
			// 反例。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a", "z/a")
			nsX, nsY, nsZ := getK8sNamespaces(k8s)

			allowedLabels := &metav1.LabelSelector{
				MatchLabels: map[string]string{
					v1.LabelMetadataName: nsY,
				},
			}
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{NamespaceSelector: allowedLabels})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-client-a-via-ns-selector-for-immutable-ns-label", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectPeer(&Peer{Namespace: nsX}, &Peer{Namespace: nsX, Pod: "a"}, false)
			reachability.ExpectPeer(&Peer{Namespace: nsZ}, &Peer{Namespace: nsX, Pod: "a"}, false)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})

		// 注意这个默认 namespace 功能由 APIMachinery 组维护，但我们仍在这里测试，因为它是重要特性。
		f.It("should enforce policy based on NamespaceSelector with MatchExpressions using default ns label", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolTCP}
			ports := []int32{80}
			// 这里对默认 namespace 名称标签使用 MatchExpressions，用于
			// 选中 x/a 的 egress 策略。selector 是 NotIn{Y}，所以 x/a -> Y 必须被拒绝，
			// 而 x/a -> X 和 x/a -> Z 保持允许。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a", "z/a")
			nsX, nsY, _ := getK8sNamespaces(k8s)

			allowedNamespaces := &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      v1.LabelMetadataName,
					Operator: metav1.LabelSelectorOpNotIn,
					Values:   []string{nsY},
				}},
			}
			egressRule := networkingv1.NetworkPolicyEgressRule{}
			egressRule.To = append(egressRule.To, networkingv1.NetworkPolicyPeer{NamespaceSelector: allowedNamespaces})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-ns-y-match-selector-for-immutable-ns-label", map[string]string{"pod": "a"}, SetSpecEgressRules(egressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectPeer(&Peer{Namespace: nsX, Pod: "a"}, &Peer{Namespace: nsY}, false)
			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolTCP, Reachability: reachability})
		})
	})
})

var _ = common.SIGDescribe("Netpol [LinuxOnly]", func() {
	f := framework.NewDefaultFramework("udp-network-policy")
	f.SkipNamespaceCreation = true
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline
	var k8s *kubeManager
	ginkgo.BeforeEach(func() {
		// Windows 不支持通过 agnhost 测试 UDP。
		e2eskipper.SkipIfNodeOSDistroIs("windows")
	})

	ginkgo.Context("NetworkPolicy between server and client using UDP", func() {

		f.It("should support a 'default-deny-ingress' policy", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolUDP}
			ports := []int32{80}
			// UDP：namespace X 有 default-deny-ingress 策略，所以 X 中需要 2 个 pod 来
			// 验证 X 到 X 的 ingress 被阻断，Y 中 2 个 pod 验证 Y 到 X 也被
			// 阻断，同时 X 到 Y 和 Y 到 Y 的流量仍被允许。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a", "y/b")
			nsX, _, _ := getK8sNamespaces(k8s)

			policy := GenNetworkPolicyWithNameAndPodSelector("deny-all", metav1.LabelSelector{}, SetSpecIngressRules())
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectPeer(&Peer{}, &Peer{Namespace: nsX}, false)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolUDP, Reachability: reachability})
		})

		f.It("should enforce policy based on Ports", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolUDP}
			ports := []int32{81}
			// UDP 端口测试：namespace X 应只允许来自 namespace Y 的
			// 端口 81 ingress 到 x/a。用 x/b 作为同 namespace 来源（拒绝），z/a 作为
			// 无关 namespace（拒绝）。Y 中一个 pod 就够，因为规则
			// 是基于 namespace 的。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a", "z/a")
			nsX, nsY, nsZ := getK8sNamespaces(k8s)

			ginkgo.By("Creating a network policy allowPort81Policy which only allows allow listed namespaces (y) to connect on exactly one port (81)")
			allowedLabels := &metav1.LabelSelector{
				MatchLabels: map[string]string{
					namespaceLabelKey: nsY,
				},
			}
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{NamespaceSelector: allowedLabels})
			ingressRule.Ports = append(ingressRule.Ports, networkingv1.NetworkPolicyPort{Port: &intstr.IntOrString{IntVal: 81}, Protocol: &protocolUDP})
			allowPort81Policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-ingress-on-port-81-ns-x", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, allowPort81Policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectPeer(&Peer{Namespace: nsX}, &Peer{Namespace: nsX, Pod: "a"}, false)
			reachability.ExpectPeer(&Peer{Namespace: nsZ}, &Peer{Namespace: nsX, Pod: "a"}, false)

			ValidateOrFail(k8s, &TestCase{ToPort: 81, Protocol: v1.ProtocolUDP, Reachability: reachability})
		})

		f.It("should enforce policy to allow traffic only from a pod in a different namespace based on PodSelector and NamespaceSelector", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolUDP}
			ports := []int32{80}
			// UDP：x/a 上的策略只允许来自 y/a 的 ingress（namespace+pod selector）。
			// 保留 x/b 作为同 namespace/不匹配的 pod，确保它无法访问
			// x/a。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a")
			nsX, nsY, _ := getK8sNamespaces(k8s)

			allowedNamespaces := &metav1.LabelSelector{
				MatchLabels: map[string]string{
					namespaceLabelKey: nsY,
				},
			}
			allowedPods := &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"pod": "a",
				},
			}
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{NamespaceSelector: allowedNamespaces, PodSelector: allowedPods})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-ns-y-pod-a-via-namespace-pod-selector", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectAllIngress(NewPodString(nsX, "a"), false)
			reachability.Expect(NewPodString(nsY, "a"), NewPodString(nsX, "a"), true)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolUDP, Reachability: reachability})
		})
	})
})

var _ = common.SIGDescribe("Netpol", feature.SCTPConnectivity, "[LinuxOnly]", func() {
	f := framework.NewDefaultFramework("sctp-network-policy")
	f.SkipNamespaceCreation = true
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline
	var k8s *kubeManager
	ginkgo.BeforeEach(func() {
		// Windows 不支持 SCTP
		e2eskipper.SkipIfNodeOSDistroIs("windows")
	})

	ginkgo.Context("NetworkPolicy between server and client using SCTP", func() {

		f.It("should support a 'default-deny-ingress' policy", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolSCTP}
			ports := []int32{80}
			// SCTP：namespace X 有 default-deny-ingress 策略，所以 X 中需要 2 个 pod
			// 验证 X 到 X 的 ingress 被阻断，Y 中 2 个 pod 验证 Y 到 X 也被
			// 阻断，同时 X 到 Y 和 Y 到 Y 的流量仍被允许。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a", "y/b")
			nsX, _, _ := getK8sNamespaces(k8s)

			policy := GenNetworkPolicyWithNameAndPodSelector("deny-all", metav1.LabelSelector{}, SetSpecIngressRules())
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectPeer(&Peer{}, &Peer{Namespace: nsX}, false)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolSCTP, Reachability: reachability})
		})

		f.It("should enforce policy based on Ports", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolSCTP}
			ports := []int32{81}
			// SCTP 端口测试：namespace X 应只允许来自 namespace Y 的
			// 端口 81 ingress 到 x/a。用 x/b 作为同 namespace 来源（拒绝），z/a 作为
			// 无关 namespace（拒绝）。Y 中一个 pod 就够，因为规则
			// 是基于 namespace 的。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a", "z/a")
			nsX, nsY, nsZ := getK8sNamespaces(k8s)

			ginkgo.By("Creating a network allowPort81Policy which only allows allow listed namespaces (y) to connect on exactly one port (81)")
			allowedLabels := &metav1.LabelSelector{
				MatchLabels: map[string]string{
					namespaceLabelKey: nsY,
				},
			}
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{NamespaceSelector: allowedLabels})
			ingressRule.Ports = append(ingressRule.Ports, networkingv1.NetworkPolicyPort{Port: &intstr.IntOrString{IntVal: 81}, Protocol: &protocolSCTP})
			allowPort81Policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-ingress-on-port-81-ns-x", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, allowPort81Policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectPeer(&Peer{Namespace: nsX}, &Peer{Namespace: nsX, Pod: "a"}, false)
			reachability.ExpectPeer(&Peer{Namespace: nsZ}, &Peer{Namespace: nsX, Pod: "a"}, false)

			ValidateOrFail(k8s, &TestCase{ToPort: 81, Protocol: v1.ProtocolSCTP, Reachability: reachability})
		})

		f.It("should enforce policy to allow traffic only from a pod in a different namespace based on PodSelector and NamespaceSelector", feature.NetworkPolicy, func(ctx context.Context) {
			protocols := []v1.Protocol{protocolSCTP}
			ports := []int32{80}
			// SCTP：x/a 上的策略只允许来自 y/a 的 ingress（namespace+pod selector）。
			// 保留 x/b 作为同 namespace/不匹配的 pod，确保它无法访问
			// x/a。
			k8s = initializeResources(ctx, f, protocols, ports, "x/a", "x/b", "y/a")
			nsX, nsY, _ := getK8sNamespaces(k8s)

			allowedNamespaces := &metav1.LabelSelector{
				MatchLabels: map[string]string{
					namespaceLabelKey: nsY,
				},
			}
			allowedPods := &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"pod": "a",
				},
			}
			ingressRule := networkingv1.NetworkPolicyIngressRule{}
			ingressRule.From = append(ingressRule.From, networkingv1.NetworkPolicyPeer{NamespaceSelector: allowedNamespaces, PodSelector: allowedPods})
			policy := GenNetworkPolicyWithNameAndPodMatchLabel("allow-ns-y-pod-a-via-namespace-pod-selector", map[string]string{"pod": "a"}, SetSpecIngressRules(ingressRule))
			CreatePolicy(ctx, k8s, policy, nsX)

			reachability := NewReachability(k8s.AllPodStrings(), true)
			reachability.ExpectAllIngress(NewPodString(nsX, "a"), false)
			reachability.Expect(NewPodString(nsY, "a"), NewPodString(nsX, "a"), true)

			ValidateOrFail(k8s, &TestCase{ToPort: 80, Protocol: v1.ProtocolSCTP, Reachability: reachability})
		})
	})
})

// getNamespaceNames 返回本测试使用的基础 namespace 名称集合，以根 ns 为输入。
// framework 创建 namespace 时还会追加唯一后缀。
// 这样测试就能并行运行。
func getNamespaceNames(rootNs string) []string {
	if rootNs != "" {
		rootNs += "-"
	}
	nsX := fmt.Sprintf("%sx", rootNs)
	nsY := fmt.Sprintf("%sy", rootNs)
	nsZ := fmt.Sprintf("%sz", rootNs)
	return []string{nsX, nsY, nsZ}
}

// modelFromPodStrings 把 "x/a" 这样的 pod 引用转换为使用
// 实际生成的 namespace 名称的 model。
func modelFromPodStrings(namespaces []string, modelPods []string, protocols []v1.Protocol, ports []int32) (*Model, error) {
	if len(modelPods) == 0 {
		return nil, fmt.Errorf("model pod list must not be empty")
	}

	namespaceBySuffix := map[string]string{
		"x": namespaces[0],
		"y": namespaces[1],
		"z": namespaces[2],
	}
	podNamesByNamespace := map[string]sets.Set[string]{
		namespaces[0]: sets.New[string](),
		namespaces[1]: sets.New[string](),
		namespaces[2]: sets.New[string](),
	}

	for _, modelPod := range modelPods {
		namespaceSuffix, podName, found := strings.Cut(modelPod, "/")
		if !found || namespaceSuffix == "" || podName == "" {
			return nil, fmt.Errorf("invalid model pod %q; expected <namespace>/<pod> like x/a", modelPod)
		}

		namespaceName, found := namespaceBySuffix[namespaceSuffix]
		if !found {
			return nil, fmt.Errorf("invalid model pod %q; namespace suffix %q must be one of x, y, z", modelPod, namespaceSuffix)
		}

		podNamesByNamespace[namespaceName].Insert(podName)
	}

	return newModelWithPerNamespacePodNames(namespaces, podNamesByNamespace, ports, protocols), nil
}

// getK8sNamespaces 返回 3 个实际的 namespace 名称。
func getK8sNamespaces(k8s *kubeManager) (string, string, string) {
	ns := k8s.NamespaceNames()
	return ns[0], ns[1], ns[2]
}

func initializeCluster(ctx context.Context, f *framework.Framework, protocols []v1.Protocol, ports []int32, modelPods ...string) (*kubeManager, error) {
	dnsDomain := framework.TestContext.ClusterDNSDomain
	framework.Logf("dns domain: %s", dnsDomain)

	k8s := newKubeManager(f, dnsDomain)
	rootNs := f.BaseName
	namespaceNames := getNamespaceNames(rootNs)

	model, err := modelFromPodStrings(namespaceNames, modelPods, protocols, ports)
	if err != nil {
		return nil, err
	}

	framework.Logf("initializing cluster: ensuring namespaces, pods and services exist and are ready")

	if err := k8s.initializeClusterFromModel(ctx, model); err != nil {
		return nil, err
	}

	framework.Logf("finished initializing cluster state")

	if err := waitForHTTPServers(k8s, model); err != nil {
		return nil, err
	}

	return k8s, nil
}

// initializeResources 使用 e2e framework，根据从 framework 推导出的 network policy
// model 创建所有必要的 namespace 资源，然后等待 model 描述的资源
// 启动并运行（即所有 pod 在自己的 namespace 中就绪并运行）。
func initializeResources(ctx context.Context, f *framework.Framework, protocols []v1.Protocol, ports []int32, modelPods ...string) *kubeManager {
	k8s, err := initializeCluster(ctx, f, protocols, ports, modelPods...)
	framework.ExpectNoError(err, "unable to initialize resources")
	return k8s
}

// makeLargeCIDRForIP 返回一个 CIDR，匹配给定 IP 以及非常非常多的其他
// IP。（具体说，返回包含该 IP 的 /4 网段。）
func makeLargeCIDRForIP(ip string) string {
	podIP := utilnet.ParseIPSloppy(ip)
	if ip4 := podIP.To4(); ip4 != nil {
		podIP = ip4
	}
	cidrBase := podIP.Mask(net.CIDRMask(4, 8*len(podIP)))
	return fmt.Sprintf("%s/4", cidrBase.String())
}
