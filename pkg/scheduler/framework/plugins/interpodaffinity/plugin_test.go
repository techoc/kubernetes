/*
Copyright 2024 The Kubernetes Authors.

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

package interpodaffinity

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	plugintesting "k8s.io/kubernetes/pkg/scheduler/framework/plugins/testing"
	schedruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
)

// Test_isSchedulableAfterAssignedPodChange 验证已分配 Pod 发生新增/更新/删除时，
// isSchedulableAfterAssignedPodChange 返回的 QueueingHint 是否符合预期：
// 只有变更可能影响待调度 Pod 的（反）亲和性匹配时才入队，否则跳过。
func Test_isSchedulableAfterAssignedPodChange(t *testing.T) {
	// 表驱动覆盖：新增匹配/不匹配 Pod、未调度 Pod、标签从不匹配到匹配及反向、
	// 多 term 场景，以及反亲和性下删除 Pod 等；Queue 表示需重排，QueueSkip 表示可跳过。
	tests := []struct {
		name           string
		pod            *v1.Pod
		oldPod, newPod *v1.Pod
		expectedHint   fwk.QueueingHint
	}{
		{
			name:         "add a pod which matches the pod affinity",
			pod:          st.MakePod().UID("p").Name("p").PodAffinityIn("service", "region", []string{"securityscan", "value2"}, st.PodAffinityWithRequiredReq).Obj(),
			newPod:       st.MakePod().UID("other").Node("fake-node").Label("service", "securityscan").Obj(),
			expectedHint: fwk.Queue,
		},
		{
			name:         "add an un-scheduled pod",
			pod:          st.MakePod().UID("p").Name("p").PodAffinityIn("service", "region", []string{"securityscan", "value2"}, st.PodAffinityWithRequiredReq).Obj(),
			newPod:       st.MakePod().UID("other").Label("service", "securityscan").Obj(),
			expectedHint: fwk.QueueSkip,
		},
		{
			name:         "add a pod which doesn't match the pod affinity",
			pod:          st.MakePod().UID("p").Name("p").PodAffinityIn("service", "region", []string{"securityscan", "value2"}, st.PodAffinityWithRequiredReq).Obj(),
			newPod:       st.MakePod().UID("other").Node("fake-node").Label("aaa", "a").Obj(),
			expectedHint: fwk.QueueSkip,
		},
		{
			name:         "update a pod from non-match to match pod affinity",
			pod:          st.MakePod().UID("p").Name("p").PodAffinityIn("service", "region", []string{"securityscan", "value2"}, st.PodAffinityWithRequiredReq).Obj(),
			newPod:       st.MakePod().UID("other").Node("fake-node").Label("service", "securityscan").Obj(),
			oldPod:       st.MakePod().UID("other").Node("fake-node").Label("aaa", "a").Obj(),
			expectedHint: fwk.Queue,
		},
		{
			name:         "the updating pod matches target pod's affinity both before and after label changes",
			pod:          st.MakePod().UID("p").Name("p").PodAffinityIn("service", "region", []string{"securityscan", "value2"}, st.PodAffinityWithRequiredReq).Obj(),
			newPod:       st.MakePod().UID("other").Node("fake-node").Label("service", "securityscan").Obj(),
			oldPod:       st.MakePod().UID("other").Node("fake-node").Label("service", "value2").Obj(),
			expectedHint: fwk.QueueSkip,
		},
		{
			name:         "update an un-scheduled pod",
			pod:          st.MakePod().UID("p").Name("p").PodAffinityIn("service", "region", []string{"securityscan", "value2"}, st.PodAffinityWithRequiredReq).Obj(),
			newPod:       st.MakePod().UID("other").Label("service", "securityscan").Obj(),
			oldPod:       st.MakePod().UID("other").Label("aaa", "a").Obj(),
			expectedHint: fwk.QueueSkip,
		},
		{
			name:         "update a pod from match to non-match the pod affinity",
			pod:          st.MakePod().UID("p").Name("p").PodAffinityIn("service", "region", []string{"securityscan", "value2"}, st.PodAffinityWithRequiredReq).Obj(),
			newPod:       st.MakePod().UID("other").Node("fake-node").Label("aaa", "a").Obj(),
			oldPod:       st.MakePod().UID("other").Node("fake-node").Label("service", "securityscan").Obj(),
			expectedHint: fwk.QueueSkip,
		},
		{
			name: "update a pod from match to non-match pod's affinity - multiple terms case",
			pod: st.MakePod().UID("p").Name("p").PodAffinityExists("aaa", "hostname", st.PodAffinityWithRequiredReq).
				PodAffinityExists("bbb", "hostname", st.PodAffinityWithRequiredReq).Obj(),
			newPod:       st.MakePod().UID("other").Node("fake-node").Label("service", "securityscan").Obj(),
			oldPod:       st.MakePod().UID("other").Node("fake-node").Label("aaa", "a").Obj(),
			expectedHint: fwk.QueueSkip,
		},
		{
			name: "update a pod from non-match to match pod's affinity - multiple terms case",
			pod: st.MakePod().UID("p").Name("p").PodAffinityExists("aaa", "hostname", st.PodAffinityWithRequiredReq).
				PodAffinityExists("bbb", "hostname", st.PodAffinityWithRequiredReq).Obj(),
			newPod:       st.MakePod().UID("other").Node("fake-node").Label("aaa", "").Label("bbb", "").Obj(),
			oldPod:       st.MakePod().UID("other").Node("fake-node").Label("aaa", "a").Obj(),
			expectedHint: fwk.Queue,
		},
		{
			name:         "modify pod label to change it from satisfying pod anti-affinity to not satisfying anti-affinity",
			pod:          st.MakePod().UID("p").Name("p").PodAntiAffinityIn("service", "region", []string{"securityscan", "value2"}, st.PodAntiAffinityWithRequiredReq).Obj(),
			newPod:       st.MakePod().UID("other").Node("fake-node").Label("service", "aaaa").Obj(),
			oldPod:       st.MakePod().UID("other").Node("fake-node").Label("service", "securityscan").Obj(),
			expectedHint: fwk.Queue,
		},
		{
			name:         "modify pod label to change it from not satisfying pod anti-affinity to satisfying anti-affinity",
			pod:          st.MakePod().UID("p").Name("p").PodAntiAffinityIn("service", "region", []string{"securityscan", "value2"}, st.PodAntiAffinityWithRequiredReq).Obj(),
			newPod:       st.MakePod().UID("other").Node("fake-node").Label("service", "securityscan").Obj(),
			oldPod:       st.MakePod().UID("other").Node("fake-node").Label("service", "bbb").Obj(),
			expectedHint: fwk.QueueSkip,
		},
		{
			name:         "delete a pod which doesn't match pod's anti-affinity",
			pod:          st.MakePod().UID("p").Name("p").PodAntiAffinityIn("service", "region", []string{"securityscan", "value2"}, st.PodAntiAffinityWithRequiredReq).Obj(),
			oldPod:       st.MakePod().UID("other").Node("fake-node").Label("aaa", "a").Obj(),
			expectedHint: fwk.QueueSkip,
		},
		{
			name:         "delete a pod which matches pod's anti-affinity",
			pod:          st.MakePod().UID("p").Name("p").PodAntiAffinityIn("service", "region", []string{"securityscan", "value2"}, st.PodAntiAffinityWithRequiredReq).Obj(),
			oldPod:       st.MakePod().UID("other").Node("fake-node").Label("service", "securityscan").Obj(),
			expectedHint: fwk.Queue,
		},
		{
			name:         "delete a pod with anti-affinity that matches pending pod",
			pod:          st.MakePod().Name("p").Label("service", "securityscan").Obj(),
			oldPod:       st.MakePod().Node("fake-node").PodAntiAffinityIn("service", "region", []string{"securityscan", "value2"}, st.PodAntiAffinityWithRequiredReq).Obj(),
			expectedHint: fwk.Queue,
		},
		{
			name:         "delete a pod with anti-affinity that doesn't match pending pod",
			pod:          st.MakePod().Name("p").Label("service", "foo").Obj(),
			oldPod:       st.MakePod().Node("fake-node").PodAntiAffinityIn("service", "region", []string{"securityscan", "value2"}, st.PodAntiAffinityWithRequiredReq).Obj(),
			expectedHint: fwk.QueueSkip,
		},
		{
			name:         "delete a pod which doesn't match pending pod's anti-affinity and has anti-affinity that doesn't match pending pod",
			pod:          st.MakePod().Name("p").PodAntiAffinityIn("service", "region", []string{"securityscan", "value2"}, st.PodAntiAffinityWithRequiredReq).Label("service", "foo").Obj(),
			oldPod:       st.MakePod().Node("fake-node").PodAntiAffinityIn("service", "region", []string{"securityscan", "value2"}, st.PodAntiAffinityWithRequiredReq).Label("service", "foo").Obj(),
			expectedHint: fwk.QueueSkip,
		},
		{
			name:         "delete a nominated pod which doesn't match pod's anti-affinity",
			pod:          st.MakePod().Name("p").PodAntiAffinityIn("service", "region", []string{"securityscan", "value2"}, st.PodAntiAffinityWithRequiredReq).Obj(),
			oldPod:       st.MakePod().NominatedNodeName("fake-node").Label("aaa", "a").Obj(),
			expectedHint: fwk.QueueSkip,
		},
		{
			name:         "delete a nominated pod which matches pod's anti-affinity",
			pod:          st.MakePod().Name("p").PodAntiAffinityIn("service", "region", []string{"securityscan", "value2"}, st.PodAntiAffinityWithRequiredReq).Obj(),
			oldPod:       st.MakePod().NominatedNodeName("fake-node").Label("service", "securityscan").Obj(),
			expectedHint: fwk.Queue,
		},
		{
			name: "modify pod label to change it from matching only one of target pod's anti-affinity terms to matching none",
			pod: st.MakePod().UID("p").Name("p").
				PodAntiAffinityIn("service", "region", []string{"securityscan"}, st.PodAntiAffinityWithRequiredReq).
				PodAntiAffinityIn("app", "region", []string{"web"}, st.PodAntiAffinityWithRequiredReq).Obj(),
			newPod:       st.MakePod().UID("other").Node("fake-node").Label("service", "foo").Label("app", "bar").Obj(),
			oldPod:       st.MakePod().UID("other").Node("fake-node").Label("service", "securityscan").Label("app", "bar").Obj(),
			expectedHint: fwk.Queue,
		},
		{
			name: "modify pod label but it still matches one of target pod's anti-affinity terms",
			pod: st.MakePod().UID("p").Name("p").
				PodAntiAffinityIn("service", "region", []string{"securityscan"}, st.PodAntiAffinityWithRequiredReq).
				PodAntiAffinityIn("app", "region", []string{"web"}, st.PodAntiAffinityWithRequiredReq).Obj(),
			newPod:       st.MakePod().UID("other").Node("fake-node").Label("service", "foo").Label("app", "web").Obj(),
			oldPod:       st.MakePod().UID("other").Node("fake-node").Label("service", "securityscan").Label("app", "bar").Obj(),
			expectedHint: fwk.QueueSkip,
		},
		{
			name: "delete a pod which matches only one of target pod's anti-affinity terms",
			pod: st.MakePod().UID("p").Name("p").
				PodAntiAffinityIn("service", "region", []string{"securityscan"}, st.PodAntiAffinityWithRequiredReq).
				PodAntiAffinityIn("app", "region", []string{"web"}, st.PodAntiAffinityWithRequiredReq).Obj(),
			oldPod:       st.MakePod().UID("other").Node("fake-node").Label("service", "securityscan").Obj(),
			expectedHint: fwk.Queue,
		},
		{
			name: "delete a pod with anti-affinity that matches pending pod on only one term",
			pod:  st.MakePod().Name("p").Label("service", "securityscan").Obj(),
			oldPod: st.MakePod().Node("fake-node").UID("other").
				PodAntiAffinityIn("service", "region", []string{"securityscan"}, st.PodAntiAffinityWithRequiredReq).
				PodAntiAffinityIn("app", "region", []string{"web"}, st.PodAntiAffinityWithRequiredReq).Obj(),
			expectedHint: fwk.Queue,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 用测试上下文绑定 logger，避免日志泄漏到其他测试。
			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			// 空 snapshot + informers 足以驱动 QueueingHint 计算；
			// 这里只关心变更是否满足亲和性，不依赖真实的集群状态。
			snapshot := cache.NewSnapshot(nil, nil)
			pl := plugintesting.SetupPluginWithInformers(ctx, t, schedruntime.FactoryAdapter(feature.Features{}, New), &config.InterPodAffinityArgs{}, snapshot, namespaces)
			p := pl.(*InterPodAffinity)
			actualHint, err := p.isSchedulableAfterAssignedPodChange(logger, tc.pod, tc.oldPod, tc.newPod)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// 比较期望的 Hint，失败时输出 diff 便于定位具体是哪个用例。
			if diff := cmp.Diff(tc.expectedHint, actualHint); diff != "" {
				t.Errorf("unexpected QueueingHint (-want, +got):\n%s", diff)
			}
		})
	}
}

// Test_isSchedulableAfterTargetPodUpdateLabel 验证目标 Pod 标签更新时的 QueueingHint：
// 即便新旧标签相同（DeepCopy），只要涉及亲和性相关标签就会入队。
func Test_isSchedulableAfterTargetPodUpdateLabel(t *testing.T) {
	// 使用测试上下文绑定 logger，并显式取消 context 回收资源。
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pod := st.MakePod().UID("p").Name("p").Label("foo", "baz").Obj()

	snapshot := cache.NewSnapshot(nil, nil)
	pl := plugintesting.SetupPluginWithInformers(ctx, t, schedruntime.FactoryAdapter(feature.Features{}, New), &config.InterPodAffinityArgs{}, snapshot, namespaces)
	p := pl.(*InterPodAffinity)
	// 新旧均为 pod 的副本：验证即便标签实际未变，也保守地返回 Queue。
	actualHint, err := p.isSchedulableAfterTargetPodUpdateLabel(logger, pod, pod.DeepCopy(), pod.DeepCopy())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 期望 Queue：标签更新可能影响其他 Pod 的亲和性匹配，需重新入队。
	if diff := cmp.Diff(fwk.Queue, actualHint); diff != "" {
		t.Errorf("unexpected QueueingHint (-want, +got):\n%s", diff)
	}
}

// Test_isSchedulableAfterNodeChange 验证节点新增/更新时 isSchedulableAfterNodeChange
// 返回的 QueueingHint：只有拓扑域（topologyKey）相关变更才可能影响亲和性匹配并触发重排。
func Test_isSchedulableAfterNodeChange(t *testing.T) {
	// 表驱动：节点标签变化只有涉及亲和性 topologyKey 时才入队；无关标签跳过。
	testcases := []struct {
		name             string
		pod              *v1.Pod
		oldNode, newNode *v1.Node
		expectedHint     fwk.QueueingHint
	}{
		// affinity：Pod 亲和性场景。
		{
			name:         "add a new node with matched pod affinity topologyKey",
			pod:          st.MakePod().Name("p").PodAffinityIn("service", "zone", []string{"securityscan", "value2"}, st.PodAffinityWithRequiredReq).Obj(),
			newNode:      st.MakeNode().Name("node-a").Label("zone", "zone1").Obj(),
			expectedHint: fwk.Queue,
		},
		{
			name:         "add a new node without matched topologyKey",
			pod:          st.MakePod().Name("p").PodAffinityIn("service", "region", []string{"securityscan", "value2"}, st.PodAffinityWithRequiredReq).Obj(),
			newNode:      st.MakeNode().Name("node-a").Label("zone", "zone1").Obj(),
			expectedHint: fwk.QueueSkip,
		},
		{
			name:         "update node label but not topologyKey",
			pod:          st.MakePod().Name("p").PodAffinityIn("service", "zone", []string{"securityscan", "value2"}, st.PodAffinityWithRequiredReq).Obj(),
			oldNode:      st.MakeNode().Name("node-a").Label("aaa", "a").Obj(),
			newNode:      st.MakeNode().Name("node-a").Label("aaa", "b").Obj(),
			expectedHint: fwk.QueueSkip,
		},
		{
			name:         "update node label that isn't related to the pod affinity",
			pod:          st.MakePod().Name("p").PodAffinityIn("service", "zone", []string{"securityscan", "value2"}, st.PodAffinityWithRequiredReq).Obj(),
			oldNode:      st.MakeNode().Name("node-a").Label("zone", "zone1").Obj(),
			newNode:      st.MakeNode().Name("node-a").Label("zone", "zone1").Label("unrelated-label", "unrelated").Obj(),
			expectedHint: fwk.QueueSkip,
		},
		{
			name:         "update node with different affinity topologyKey value",
			pod:          st.MakePod().Name("p").PodAffinityIn("service", "zone", []string{"securityscan", "value2"}, st.PodAffinityWithRequiredReq).Obj(),
			oldNode:      st.MakeNode().Name("node-a").Label("zone", "zone1").Obj(),
			newNode:      st.MakeNode().Name("node-a").Label("zone", "zone2").Obj(),
			expectedHint: fwk.Queue,
		},
		{
			name:         "update node to have the affinity topology label",
			pod:          st.MakePod().Name("p").PodAffinityIn("service", "zone", []string{"securityscan", "value2"}, st.PodAffinityWithRequiredReq).Obj(),
			oldNode:      st.MakeNode().Name("node-a").Label("aaa", "a").Obj(),
			newNode:      st.MakeNode().Name("node-a").Label("zone", "zone1").Obj(),
			expectedHint: fwk.Queue,
		},
		// anti-affinity：Pod 反亲和性场景。
		{
			name:         "add a new node with matched pod anti-affinity topologyKey",
			pod:          st.MakePod().Name("p").PodAntiAffinity("zone", &metav1.LabelSelector{MatchLabels: map[string]string{"another": "label"}}, st.PodAntiAffinityWithRequiredReq).Obj(),
			newNode:      st.MakeNode().Name("node-a").Label("zone", "zone1").Obj(),
			expectedHint: fwk.Queue,
		},
		{
			name:         "add a new node without matched pod anti-affinity topologyKey",
			pod:          st.MakePod().Name("p").PodAntiAffinity("zone", &metav1.LabelSelector{MatchLabels: map[string]string{"another": "label"}}, st.PodAntiAffinityWithRequiredReq).Obj(),
			newNode:      st.MakeNode().Name("node-a").Obj(),
			expectedHint: fwk.Queue,
		},
		{
			name:         "update node label that isn't related to the pod anti-affinity",
			pod:          st.MakePod().Name("p").PodAntiAffinity("zone", &metav1.LabelSelector{MatchLabels: map[string]string{"another": "label"}}, st.PodAntiAffinityWithRequiredReq).Obj(),
			oldNode:      st.MakeNode().Name("node-a").Label("zone", "zone1").Obj(),
			newNode:      st.MakeNode().Name("node-a").Label("zone", "zone1").Label("unrelated-label", "unrelated").Obj(),
			expectedHint: fwk.QueueSkip,
		},
		{
			name:         "update node with different anti-affinity topologyKey value",
			pod:          st.MakePod().Name("p").PodAntiAffinity("zone", &metav1.LabelSelector{MatchLabels: map[string]string{"another": "label"}}, st.PodAntiAffinityWithRequiredReq).Obj(),
			oldNode:      st.MakeNode().Name("node-a").Label("zone", "zone1").Obj(),
			newNode:      st.MakeNode().Name("node-a").Label("zone", "zone2").Obj(),
			expectedHint: fwk.Queue,
		},
		{
			name:         "update node to have the anti-affinity topology label",
			pod:          st.MakePod().Name("p").PodAntiAffinity("zone", &metav1.LabelSelector{MatchLabels: map[string]string{"another": "label"}}, st.PodAntiAffinityWithRequiredReq).Obj(),
			oldNode:      st.MakeNode().Name("node-a").Label("aaa", "a").Obj(),
			newNode:      st.MakeNode().Name("node-a").Label("zone", "zone1").Obj(),
			expectedHint: fwk.QueueSkip,
		},
		{
			name:         "update node label not to have anti-affinity topology label",
			pod:          st.MakePod().Name("p").PodAntiAffinity("zone", &metav1.LabelSelector{MatchLabels: map[string]string{"another": "label"}}, st.PodAntiAffinityWithRequiredReq).Obj(),
			oldNode:      st.MakeNode().Name("node-a").Label("zone", "zone1").Obj(),
			newNode:      st.MakeNode().Name("node-a").Label("aaa", "a").Obj(),
			expectedHint: fwk.Queue,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// 使用测试上下文绑定 logger，取消 context 回收资源。
			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			// 空 snapshot 足以驱动节点变更的 Hint 计算，无需真实集群状态。
			snapshot := cache.NewSnapshot(nil, nil)
			pl := plugintesting.SetupPluginWithInformers(ctx, t, schedruntime.FactoryAdapter(feature.Features{}, New), &config.InterPodAffinityArgs{}, snapshot, namespaces)
			p := pl.(*InterPodAffinity)
			actualHint, err := p.isSchedulableAfterNodeChange(logger, tc.pod, tc.oldNode, tc.newNode)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			// 比较期望 Hint，diff 便于定位拓扑域相关的判定偏差。
			if diff := cmp.Diff(tc.expectedHint, actualHint); diff != "" {
				t.Errorf("expected QueuingHint doesn't match (-want,+got): \n %s", diff)
			}
		})
	}
}

// TestPodAffinitySignature 验证 SignPod 为 Pod 生成的签名：
// 默认按标签签名，配置忽略现有 Pod 的 preferred term 时签名应为空，带 affinity 的 Pod 不可调度。
func TestPodAffinitySignature(t *testing.T) {
	// 表驱动：默认标签签名 / 忽略 preferred term 配置 / 带 affinity 不可调度三种场景。
	tests := []struct {
		name              string
		pod               *v1.Pod
		expectedSignature []fwk.SignFragment
		schedulable       bool
		config            config.InterPodAffinityArgs
	}{
		{
			name: "no affinity, default settings",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"foo": "bar"},
				},
				Spec: v1.PodSpec{},
			},
			expectedSignature: []fwk.SignFragment{
				{
					Key:   fwk.LabelsSignerName,
					Value: map[string]string{"foo": "bar"},
				},
			},
			schedulable: true,
		},
		{
			name: "no affinity, ignore setting set",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"foo": "bar"},
				},
				Spec: v1.PodSpec{},
			},
			schedulable: true,
			config: config.InterPodAffinityArgs{
				IgnorePreferredTermsOfExistingPods: true,
			},
		},
		{
			name: "affinity set",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"foo": "bar"},
				},
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						PodAffinity: &v1.PodAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{},
							},
						},
					},
				},
			},
			schedulable: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 使用测试上下文，显式取消以回收资源。
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			// 按各用例的 config 建插件，签名行为随配置而变。
			snapshot := cache.NewSnapshot(nil, nil)
			pl := plugintesting.SetupPluginWithInformers(ctx, t, schedruntime.FactoryAdapter(feature.Features{}, New), &test.config, snapshot, namespaces)
			p := pl.(*InterPodAffinity)
			signature, status := p.SignPod(ctx, test.pod)

			// 带 affinity 的 Pod 不可调度，其余用例应成功。
			if !status.IsSuccess() && test.schedulable {
				t.Fatalf("Expected success, got %v", status)
			}

			// 校验签名片段与期望一致，diff 便于定位缺失或多出的签名项。
			if diff := cmp.Diff(test.expectedSignature, signature); diff != "" {
				t.Fatalf("Diff %s", diff)
			}
		})
	}
}
