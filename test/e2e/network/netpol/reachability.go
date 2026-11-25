/*
Copyright 2020 The Kubernetes Authors.

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
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/test/e2e/framework"
)

// TestCase 描述 netpol 测试的数据
type TestCase struct {
	ToPort       int
	Protocol     v1.Protocol
	Reachability *Reachability
}

// PodString 将命名空间 'x' 和 Pod 'a' 表示为 "x/a"。
type PodString string

// NewPodString 从给定的命名空间和名称实例化一个 PodString。
func NewPodString(namespace string, podName string) PodString {
	return PodString(fmt.Sprintf("%s/%s", namespace, podName))
}

// String 转换回字符串
func (pod PodString) String() string {
	return string(pod)
}

func (pod PodString) split() (string, string) {
	pieces := strings.Split(string(pod), "/")
	if len(pieces) != 2 {
		framework.Failf("expected ns/pod, found %+v", pieces)
	}
	return pieces[0], pieces[1]
}

// Namespace 提取命名空间
func (pod PodString) Namespace() string {
	ns, _ := pod.split()
	return ns
}

// PodName 提取 Pod 名称
func (pod PodString) PodName() string {
	_, podName := pod.split()
	return podName
}

// Peer 用于按 Pod 的命名空间和/或名称匹配 Pod。
type Peer struct {
	Namespace string
	Pod       string
}

// Matches 检查 Peer 是否匹配 PodString：
// - 空命名空间表示总是匹配
// - 否则命名空间必须与 PodString 的命名空间一致
// - Pod 同理：空匹配全部，否则必须完全匹配
func (p *Peer) Matches(pod PodString) bool {
	return (p.Namespace == "" || p.Namespace == pod.Namespace()) && (p.Pod == "" || p.Pod == pod.PodName())
}

// Reachability 封装集群范围连通性探测的数据
type Reachability struct {
	Expected   *TruthTable
	Observed   *TruthTable
	PodStrings []PodString
}

// NewReachability 实例化一个 reachability
func NewReachability(podStrings []PodString, defaultExpectation bool) *Reachability {
	var podNames []string
	for _, podString := range podStrings {
		podNames = append(podNames, podString.String())
	}
	r := &Reachability{
		Expected:   NewTruthTableFromItems(podNames, &defaultExpectation),
		Observed:   NewTruthTableFromItems(podNames, nil),
		PodStrings: podStrings,
	}
	return r
}

// AllowLoopback 期望从 Pod 到自身的所有通信都被允许。
// 一般应在设置其他规则后调用，因为 loopback 逻辑不受任何策略约束。
func (r *Reachability) AllowLoopback() {
	for _, podString := range r.PodStrings {
		podName := podString.String()
		r.Expected.Set(podName, podName, true)
	}
}

// Expect 设置单次观测的期望值
func (r *Reachability) Expect(from PodString, to PodString, isConnected bool) {
	r.Expected.Set(string(from), string(to), isConnected)
}

// ExpectAllIngress 定义进入 Pod 的任意流量是被允许还是拒绝（true/false）
func (r *Reachability) ExpectAllIngress(pod PodString, connected bool) {
	r.Expected.SetAllTo(string(pod), connected)
	if !connected {
		framework.Logf("Denying all traffic *to* %s", pod)
	}
}

// ExpectAllEgress 定义离开 Pod 的任意流量是被允许还是拒绝（true/false）
func (r *Reachability) ExpectAllEgress(pod PodString, connected bool) {
	r.Expected.SetAllFrom(string(pod), connected)
	if !connected {
		framework.Logf("Denying all traffic *from* %s", pod)
	}
}

// ExpectPeer 使用 Peer matcher 设置期望值
func (r *Reachability) ExpectPeer(from *Peer, to *Peer, connected bool) {
	for _, fromPod := range r.PodStrings {
		if from.Matches(fromPod) {
			for _, toPod := range r.PodStrings {
				if to.Matches(toPod) {
					r.Expected.Set(fromPod.String(), toPod.String(), connected)
				}
			}
		}
	}
}

// Observe 记录一次连通性观测
func (r *Reachability) Observe(fromPod PodString, toPod PodString, isConnected bool) {
	r.Observed.Set(fromPod.String(), toPod.String(), isConnected)
}

// Summary 生成期望数据与观测数据的有用摘要
func (r *Reachability) Summary(ignoreLoopback bool) (trueObs int, falseObs int, ignoredObs int, comparison *TruthTable) {
	comparison = r.Expected.Compare(r.Observed)
	if !comparison.IsComplete() {
		framework.Failf("observations not complete!")
	}
	falseObs, trueObs, ignoredObs = 0, 0, 0
	for from, dict := range comparison.Values {
		for to, val := range dict {
			if ignoreLoopback && from == to {
				// loopback 绝不算失败，因为尚未对其作出定义。
				ignoredObs++
			} else if val {
				trueObs++
			} else {
				falseObs++
			}
		}
	}
	return
}

// PrintSummary 打印摘要
func (r *Reachability) PrintSummary(printExpected bool, printObserved bool, printComparison bool) {
	right, wrong, ignored, comparison := r.Summary(ignoreLoopback)
	if ignored > 0 {
		framework.Logf("warning: this test doesn't take into consideration hairpin traffic, i.e. traffic whose source and destination is the same pod: %d cases ignored", ignored)
	}
	framework.Logf("reachability: correct:%v, incorrect:%v, result=%t\n\n", right, wrong, wrong == 0)
	if printExpected {
		framework.Logf("expected:\n\n%s\n\n\n", r.Expected.PrettyPrint(""))
	}
	if printObserved {
		framework.Logf("observed:\n\n%s\n\n\n", r.Observed.PrettyPrint(""))
	}
	if printComparison {
		framework.Logf("comparison:\n\n%s\n\n\n", comparison.PrettyPrint(""))
	}
}
