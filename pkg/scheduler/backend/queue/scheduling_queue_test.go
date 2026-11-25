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

package queue

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	v1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	plfeature "k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/schedulinggates"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"k8s.io/kubernetes/test/utils/ktesting"
	testingclock "k8s.io/utils/clock/testing"
)

const queueMetricMetadata = `
		# HELP scheduler_queue_incoming_pods_total [STABLE] Number of pods added to scheduling queues by event and queue type.
		# TYPE scheduler_queue_incoming_pods_total counter
	`

const queueIncomingEntitiesMetricMetadata = `
	# HELP scheduler_queue_incoming_entities_total [ALPHA] Number of scheduling entities added to scheduling queues by event, queue type, and entity type. Entity types are either 'pod' (for individual pods that are not members of any podgroup) or 'podgroup'.
	# TYPE scheduler_queue_incoming_entities_total counter
`

const queuedEntitiesMetricMetadata = `
	# HELP scheduler_queued_entities [ALPHA] Number of queued scheduling entities ('pod' or 'podgroup'; 'pod' stands for individual pods that are not members of any podgroup) by the queue type. 'active' means number of entities in activeQ; 'backoff' means number of entities in backoffQ; 'unschedulable' means number of entities in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable entities that the scheduler never attempted to schedule because they are gated.
	# TYPE scheduler_queued_entities gauge
`

var (
	// nodeAdd 是集群中新增节点的事件。
	nodeAdd = fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add}
	// pvAdd 是集群中新增持久卷的事件。
	pvAdd = fwk.ClusterEvent{Resource: fwk.PersistentVolume, ActionType: fwk.Add}
	// pvUpdate 是集群中持久卷更新的事件。
	pvUpdate = fwk.ClusterEvent{Resource: fwk.PersistentVolume, ActionType: fwk.Update}
	// pvcAdd 是集群中新增持久卷声明的事件。
	pvcAdd = fwk.ClusterEvent{Resource: fwk.PersistentVolumeClaim, ActionType: fwk.Add}
	// csiNodeUpdate 是集群中 CSI 节点更新的事件。
	csiNodeUpdate = fwk.ClusterEvent{Resource: fwk.CSINode, ActionType: fwk.Update}

	lowPriority, midPriority, highPriority = int32(0), int32(100), int32(1000)
	mediumPriority                         = (lowPriority + highPriority) / 2

	highPriorityPodInfo = mustNewPodInfo(
		st.MakePod().Name("hpp").Namespace("ns1").UID("hppns1").Priority(highPriority).Obj(),
	)
	highPriNominatedPodInfo = mustNewPodInfo(
		st.MakePod().Name("hpp").Namespace("ns1").UID("hppns1").Priority(highPriority).NominatedNodeName("node1").Obj(),
	)
	medPriorityPodInfo = mustNewPodInfo(
		st.MakePod().Name("mpp").Namespace("ns2").UID("mppns2").Annotation("annot2", "val2").Priority(mediumPriority).NominatedNodeName("node1").Obj(),
	)
	unschedulablePodInfo = mustNewPodInfo(
		st.MakePod().Name("up").Namespace("ns1").UID("upns1").Annotation("annot2", "val2").Priority(lowPriority).NominatedNodeName("node1").Condition(v1.PodScheduled, v1.ConditionFalse, v1.PodReasonUnschedulable).Obj(),
	)
	nonExistentPodInfo = mustNewPodInfo(
		st.MakePod().Name("ne").Namespace("ns1").UID("nens1").Obj(),
	)
	scheduledPodInfo = mustNewPodInfo(
		st.MakePod().Name("sp").Namespace("ns1").UID("spns1").Node("foo").Obj(),
	)

	nominatorCmpOpts = []cmp.Option{
		cmp.AllowUnexported(nominator{}, podRef{}),
		cmpopts.IgnoreFields(nominator{}, "podLister", "nLock"),
	}

	queueHintReturnQueue = func(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
		return fwk.Queue, nil
	}
	queueHintReturnSkip = func(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
		return fwk.QueueSkip, nil
	}
)

func init() {
	metrics.Register()
}

func setQueuedPodInfoGated(queuedPodInfo *framework.QueuedPodInfo, gatingPlugin string, gatingPluginEvents []fwk.ClusterEvent) *framework.QueuedPodInfo {
	queuedPodInfo.GatingPlugin = gatingPlugin
	// GatingPlugin 也应注册到 UnschedulablePlugins。
	queuedPodInfo.UnschedulablePlugins = sets.New(gatingPlugin)
	queuedPodInfo.GatingPluginEvents = gatingPluginEvents
	return queuedPodInfo
}

func getUnschedulablePod(p *PriorityQueue, pod *v1.Pod) *v1.Pod {
	pInfo := p.unschedulableEntities.get(newQueuedPodInfoForLookup(pod))
	if pInfo != nil {
		return pInfo.(*framework.QueuedPodInfo).Pod
	}
	return nil
}

// makeEmptyQueueingHintMapPerProfile 为 "" profile 名初始化一个空的 QueueingHintMapPerProfile。
func makeEmptyQueueingHintMapPerProfile() QueueingHintMapPerProfile {
	m := make(QueueingHintMapPerProfile)
	m[""] = make(QueueingHintMap)
	return m
}

func withPodGroupName(pod *v1.Pod, podGroupName string) *v1.Pod {
	pod = pod.DeepCopy()
	pod.Spec.SchedulingGroup = &v1.PodSchedulingGroup{PodGroupName: &podGroupName}
	return pod
}

type podInfoOpt func(*st.PodWrapper)

func withPodLabel(key, value string) podInfoOpt {
	return func(pw *st.PodWrapper) {
		pw.Label(key, value)
	}
}

func withPodGroup(name string) podInfoOpt {
	return func(pw *st.PodWrapper) {
		pw.PodGroupName(name)
	}
}

func makeQueuedPodInfo(t *testing.T, name, ns string, params framework.QueueingParams, opts ...podInfoOpt) *framework.QueuedPodInfo {
	podBuilder := st.MakePod().Name(name).Namespace(ns).UID(name)
	for _, opt := range opts {
		opt(podBuilder)
	}
	return &framework.QueuedPodInfo{
		PodInfo:        mustNewTestPodInfo(t, podBuilder.Obj()),
		QueueingParams: params,
	}
}

// TestPriorityQueue_Add 验证 PriorityQueue.Add 把不同状态的 Pod 放入正确的队列。
func TestPriorityQueue_Add(t *testing.T) {
	tests := []struct {
		name                   string
		usePodGroups           bool
		genericWorkloadEnabled bool
	}{
		{
			name:                   "individual pod with GenericWorkload gate disabled",
			usePodGroups:           false,
			genericWorkloadEnabled: false,
		},
		{
			name:                   "individual pod with GenericWorkload gate enabled",
			usePodGroups:           false,
			genericWorkloadEnabled: true,
		},
		{
			name:                   "pod group member with GenericWorkload gate enabled",
			usePodGroups:           true,
			genericWorkloadEnabled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.GenericWorkload, tt.genericWorkloadEnabled)
			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			var medPod, unschedPod, highPod = medPriorityPodInfo.Pod, unschedulablePodInfo.Pod, highPriorityPodInfo.Pod
			if tt.usePodGroups {
				medPod = withPodGroupName(medPod, "pg-med")
				unschedPod = withPodGroupName(unschedPod, "pg-unsched")
				highPod = withPodGroupName(highPod, "pg-high")
			}

			objs := []runtime.Object{medPod, unschedPod, highPod}
			q := NewTestQueueWithObjects(ctx, newDefaultQueueSort(), objs)
			if tt.usePodGroups {
				podGroups := []*schedulingv1beta1.PodGroup{
					st.MakePodGroup().Name("pg-med").Namespace(medPod.Namespace).Priority(midPriority).Obj(),
					st.MakePodGroup().Name("pg-unsched").Namespace(unschedPod.Namespace).Priority(lowPriority).Obj(),
					st.MakePodGroup().Name("pg-high").Namespace(highPod.Namespace).Priority(highPriority).Obj(),
				}
				for _, podGroup := range podGroups {
					q.AddPodGroup(logger, podGroup)
				}
			}
			q.Add(ctx, medPod)
			q.Add(ctx, unschedPod)
			q.Add(ctx, highPod)
			expectedNominatedPods := &nominator{
				nominatedPodToNode: map[types.UID]string{
					medPod.UID:     "node1",
					unschedPod.UID: "node1",
				},
				nominatedPods: map[string][]podRef{
					"node1": {podToRef(medPod), podToRef(unschedPod)},
				},
			}
			if diff := cmp.Diff(q.nominator, expectedNominatedPods, nominatorCmpOpts...); diff != "" {
				t.Errorf("Unexpected diff after adding pods (-want, +got):\n%s", diff)
			}

			getPod := func(entity framework.QueuedEntityInfo) *v1.Pod {
				if tt.usePodGroups {
					var pod *v1.Pod
					entity.ForEachPodInfo(func(pInfo *framework.QueuedPodInfo) bool {
						pod = pInfo.Pod
						return false
					})
					return pod
				}
				return entity.(*framework.QueuedPodInfo).Pod
			}

			if entity, err := q.Pop(logger); err != nil {
				t.Errorf("Pop failed: %v", err)
			} else if diff := cmp.Diff(highPod, getPod(entity)); diff != "" {
				t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
			}
			if entity, err := q.Pop(logger); err != nil {
				t.Errorf("Pop failed: %v", err)
			} else if diff := cmp.Diff(medPod, getPod(entity)); diff != "" {
				t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
			}
			if entity, err := q.Pop(logger); err != nil {
				t.Errorf("Pop failed: %v", err)
			} else if diff := cmp.Diff(unschedPod, getPod(entity)); diff != "" {
				t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
			}
			if len(q.nominator.nominatedPods["node1"]) != 2 {
				t.Errorf("Expected medPod and unschedPod to be still present in nominatedPods: %v", q.nominator.nominatedPods["node1"])
			}
		})
	}
}

// TestPriorityQueue_AddNominatedGatedPod 验证 Add 对带 gate 的 nominated Pod 的处理。
func TestPriorityQueue_AddNominatedGatedPod(t *testing.T) {
	gatedPod := st.MakePod().Name("pod-gated").Namespace("ns1").UID("pod-gated").NominatedNodeName("node1").Obj()
	objs := []runtime.Object{gatedPod}
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	plugin := &preEnqueuePlugin{allowlists: []string{"allow"}}
	m := map[string]map[string]fwk.PreEnqueuePlugin{
		"": {
			"preEnqueuePlugin": plugin,
		},
	}
	q := NewTestQueueWithObjects(ctx, newDefaultQueueSort(), objs, WithPreEnqueuePluginMap(m))
	q.Add(ctx, gatedPod)

	// 验证 Pod 已被 gate
	pInfo := q.unschedulableEntities.get(newQueuedPodInfoForLookup(gatedPod))
	if pInfo == nil || !pInfo.Gated() {
		t.Fatalf("Expected pod to be gated in unschedulableEntities")
	}

	// 验证 Pod 已加入 nominator
	if len(q.nominator.nominatedPods["node1"]) != 1 {
		t.Errorf("Expected pod-gated in nominatedPods")
	}
	if q.nominator.nominatedPodToNode[gatedPod.UID] != "node1" {
		t.Errorf("Expected pod-gated in nominatedPodToNode")
	}
}

func newDefaultQueueSort() fwk.LessFunc {
	sort := &queuesort.PrioritySort{}
	return sort.Less
}

// TestPriorityQueue_AddWithReversePriorityLessFunc 验证使用反向优先级 Less 函数时 Add 与 Pop 的顺序。
func TestPriorityQueue_AddWithReversePriorityLessFunc(t *testing.T) {
	objs := []runtime.Object{medPriorityPodInfo.Pod, highPriorityPodInfo.Pod}
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	q := NewTestQueueWithObjects(ctx, newDefaultQueueSort(), objs)
	q.Add(ctx, medPriorityPodInfo.Pod)
	q.Add(ctx, highPriorityPodInfo.Pod)
	if p, err := q.Pop(logger); err != nil {
		t.Errorf("Pop failed: %v", err)
	} else if diff := cmp.Diff(highPriorityPodInfo.Pod, p.(*framework.QueuedPodInfo).Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}
	if p, err := q.Pop(logger); err != nil {
		t.Errorf("Pop failed: %v", err)
	} else if diff := cmp.Diff(medPriorityPodInfo.Pod, p.(*framework.QueuedPodInfo).Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}
}

// Test_InFlightPods 验证调度期间 in-flight Pod 的事件处理与重新入队行为。
func Test_InFlightPods(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	pod1 := st.MakePod().Name("targetpod").UID("pod1").Obj()
	pod2 := st.MakePod().Name("targetpod2").UID("pod2").Obj()
	pod3 := st.MakePod().Name("targetpod3").UID("pod3").Obj()

	pgName := "pg-test"
	pgPod1 := st.MakePod().Name("pgpod1").UID("pgpod1").PodGroupName(pgName).Obj()
	pgPod2 := st.MakePod().Name("pgpod2").UID("pgpod2").PodGroupName(pgName).Obj()
	pgTest := st.MakePodGroup().Name(pgName).Namespace(pgPod1.Namespace).Obj()

	var poppedPod, poppedPod2 *framework.QueuedPodInfo

	type action struct {
		// 以下字段只能设置其中一个。
		eventHappens *fwk.ClusterEvent
		podPopped    *v1.Pod
		// podCreated 是创建后插入 activeQ 的 Pod。
		podCreated *v1.Pod
		// podEnqueued 是重新入队到 activeQ 的 Pod。
		podEnqueued *framework.QueuedPodInfo
		// podGroupAttempted 是被尝试调度的 PodGroup。
		podGroupAttempted *framework.QueuedPodGroupInfo
		// podGroupAdded 是在队列中添加/更新的 PodGroup。
		podGroupAdded *schedulingv1beta1.PodGroup
		callback      func(t *testing.T, q *PriorityQueue)
	}

	tests := []struct {
		name            string
		queueingHintMap QueueingHintMapPerProfile
		// initialPods 是 activeQ 中的初始 Pod。
		initialPods                []*v1.Pod
		actions                    []action
		genericWorkloadEnabled     []bool
		wantInFlightPods           []*v1.Pod
		wantInFlightEvents         []interface{}
		wantActiveQPodNames        []string
		wantBackoffQPodNames       []string
		wantUnschedPodPoolPodNames []string
	}{
		{
			name:        "Pod and interested events are registered in inFlightPods/inFlightEvents",
			initialPods: []*v1.Pod{pod1},
			actions: []action{
				// 此时还没有 in-flight Pod，因此不会加入 inFlightEvents。
				{eventHappens: &pvcAdd},
				{podPopped: pod1},
				// 这个事件会因该 Pod 被加入。
				{eventHappens: &pvAdd},
				// 没有插件关注 PvUpdate，因此不会被加入。
				{eventHappens: &pvUpdate},
			},
			wantInFlightPods:   []*v1.Pod{pod1},
			wantInFlightEvents: []interface{}{pod1, pvAdd},
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					pvAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
				},
			},
		},
		{
			name:        "Pod, registered in inFlightPods, is enqueued back to backoffQ",
			initialPods: []*v1.Pod{pod1, pod2},
			actions: []action{
				// 此时还没有 in-flight Pod，因此不会加入 inFlightEvents。
				{eventHappens: &pvcAdd},
				{podPopped: pod1},
				{eventHappens: &pvAdd},
				{podPopped: pod2},
				{eventHappens: &nodeAdd},
				// 没有插件注册为 unschedulable 插件，该 Pod 会立即重新入队到 backoffQ，
				// 这意味着 Pod 遇到了意外错误（例如网络错误）。
				{podEnqueued: newQueuedPodInfoForLookup(pod1)},
			},
			wantBackoffQPodNames: []string{"targetpod"},
			wantInFlightPods:     []*v1.Pod{pod2}, // 仅 pod2 已登记，因为该 Pod 已重新入队。
			wantInFlightEvents:   []interface{}{pod2, nodeAdd},
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					pvAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
					nodeAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
					pvcAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
				},
			},
		},
		{
			name:        "All Pods registered in inFlightPods are enqueued back to activeQ",
			initialPods: []*v1.Pod{pod1, pod2},
			actions: []action{
				// 此时还没有 in-flight Pod，因此不会加入 inFlightEvents。
				{eventHappens: &pvcAdd},
				{podPopped: pod1},
				{eventHappens: &pvAdd},
				{podPopped: pod2},
				{eventHappens: &nodeAdd},
				// 没有插件注册为 unschedulable 插件，该 Pod 会立即重新入队到 backoffQ，
				// 这意味着 Pod 遇到了意外错误（例如网络错误）。
				{podEnqueued: newQueuedPodInfoForLookup(pod1)},
				{eventHappens: &csiNodeUpdate},
				// 没有插件注册为 unschedulable 插件，该 Pod 会立即重新入队到 backoffQ，
				// 这意味着 Pod 遇到了意外错误（例如网络错误）。
				{podEnqueued: newQueuedPodInfoForLookup(pod2)},
			},
			wantBackoffQPodNames: []string{"targetpod", "targetpod2"},
			wantInFlightPods:     nil, // 空
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					pvAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
					nodeAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
					pvcAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
					csiNodeUpdate: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
				},
			},
		},
		{
			name:        "One intermediate Pod registered in inFlightPods is enqueued back to activeQ",
			initialPods: []*v1.Pod{pod1, pod2, pod3},
			actions: []action{
				// 此时还没有 in-flight Pod，因此不会加入 inFlightEvents。
				{eventHappens: &pvcAdd},
				{podPopped: pod1},
				{eventHappens: &pvAdd},
				{podPopped: pod2},
				{eventHappens: &nodeAdd},
				// 该 Pod 不会再被重新入队。
				{podPopped: pod3},
				{eventHappens: &framework.EventAssignedPodAdd},
				// 没有插件注册为 unschedulable 插件，该 Pod 会立即重新入队到 backoffQ，
				// 这意味着 Pod 遇到了意外错误（例如网络错误）。
				{podEnqueued: newQueuedPodInfoForLookup(pod2)},
			},
			wantBackoffQPodNames: []string{"targetpod2"},
			wantInFlightPods:     []*v1.Pod{pod1, pod3},
			wantInFlightEvents:   []interface{}{pod1, pvAdd, nodeAdd, pod3, framework.EventAssignedPodAdd},
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					pvAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
					nodeAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
					framework.EventAssignedPodAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
				},
			},
		},
		{
			name:        "events before popping Pod are ignored when Pod is enqueued back to queue",
			initialPods: []*v1.Pod{pod1},
			actions: []action{
				{eventHappens: &framework.EventUnschedulableTimeout},
				{podPopped: pod1},
				{eventHappens: &framework.EventAssignedPodAdd},
				// fooPlugin1 返回 QueueSkip，该 Pod 不会重新进入 activeQ/backoffQ。
				{podEnqueued: newQueuedPodInfoForLookup(pod1, "fooPlugin1")},
			},
			wantUnschedPodPoolPodNames: []string{"targetpod"},
			wantInFlightPods:           nil,
			wantInFlightEvents:         nil,
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					// fooPlugin1 为 framework.AssignedPodAdd 注册了 queueing hint 函数，
					// 但 hint 函数认为此事件不会使 Pod 可调度。
					framework.EventAssignedPodAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnSkip,
						},
					},
				},
			},
		},
		{
			name:        "pod is enqueued to backoff if no failed plugin",
			initialPods: []*v1.Pod{pod1},
			actions: []action{
				{podPopped: pod1},
				{eventHappens: &framework.EventAssignedPodAdd},
				// 没有插件注册为 unschedulable 插件，该 Pod 会立即重新入队到 backoffQ，
				// 这意味着 Pod 遇到了意外错误（例如网络错误）。
				{podEnqueued: newQueuedPodInfoForLookup(pod1)},
			},
			wantBackoffQPodNames: []string{"targetpod"},
			wantInFlightPods:     nil,
			wantInFlightEvents:   nil,
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					// 没有失败插件，因此它会被忽略。
					framework.EventAssignedPodAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
				},
			},
		},
		{
			name:        "pod is enqueued to unschedulable pod pool if no events that can make the pod schedulable",
			initialPods: []*v1.Pod{pod1},
			actions: []action{
				{podPopped: pod1},
				{eventHappens: &nodeAdd},
				{podEnqueued: newQueuedPodInfoForLookup(pod1, "fooPlugin1")},
			},
			wantUnschedPodPoolPodNames: []string{"targetpod"},
			wantInFlightPods:           nil,
			wantInFlightEvents:         nil,
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					// fooPlugin1 没有为 NodeAdd 注册 queueing hint 函数。
					framework.EventAssignedPodAdd: {
						{
							// 事件不是 NodeAdd，因此它会被忽略。
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
				},
			},
		},
		{
			name:        "pod is enqueued to unschedulable pod pool because the failed plugin has a hint fn but it returns Skip",
			initialPods: []*v1.Pod{pod1},
			actions: []action{
				{podPopped: pod1},
				{eventHappens: &framework.EventAssignedPodAdd},
				{podEnqueued: newQueuedPodInfoForLookup(pod1, "fooPlugin1")},
			},
			wantUnschedPodPoolPodNames: []string{"targetpod"},
			wantInFlightPods:           nil,
			wantInFlightEvents:         nil,
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					// fooPlugin1 为 framework.AssignedPodAdd 注册了 queueing hint 函数，
					// 但 hint 函数认为此事件不会使 Pod 可调度。
					framework.EventAssignedPodAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnSkip,
						},
					},
				},
			},
		},
		{
			name:        "pod is enqueued to activeQ because the Pending plugins has a hint fn and it returns Queue",
			initialPods: []*v1.Pod{pod1},
			actions: []action{
				{podPopped: pod1},
				{eventHappens: &framework.EventAssignedPodAdd},
				{podEnqueued: &framework.QueuedPodInfo{
					PodInfo: mustNewPodInfo(pod1),
					QueueingParams: framework.QueueingParams{
						UnschedulablePlugins: sets.New("fooPlugin2", "fooPlugin3"),
						PendingPlugins:       sets.New("fooPlugin1"),
					},
				}},
			},
			wantActiveQPodNames: []string{"targetpod"},
			wantInFlightPods:    nil,
			wantInFlightEvents:  nil,
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					framework.EventAssignedPodAdd: {
						{
							PluginName:     "fooPlugin3",
							QueueingHintFn: queueHintReturnSkip,
						},
						{
							PluginName:     "fooPlugin2",
							QueueingHintFn: queueHintReturnQueue,
						},
						{
							// hint 函数认为此事件立即使 Pod 可调度。
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
				},
			},
		},
		{
			name:        "pod is enqueued to backoffQ because the failed plugin has a hint fn and it returns Queue",
			initialPods: []*v1.Pod{pod1},
			actions: []action{
				{podPopped: pod1},
				{eventHappens: &framework.EventAssignedPodAdd},
				{podEnqueued: newQueuedPodInfoForLookup(pod1, "fooPlugin1", "fooPlugin2")},
			},
			wantBackoffQPodNames: []string{"targetpod"},
			wantInFlightPods:     nil,
			wantInFlightEvents:   nil,
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					framework.EventAssignedPodAdd: {
						{
							// 它会被忽略：hint 函数返回 Skip，弱于 fooPlugin1 的 queueHintReturnQueue。
							PluginName:     "fooPlugin2",
							QueueingHintFn: queueHintReturnSkip,
						},
						{
							// hint 函数认为此事件使 Pod 可调度。
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
				},
			},
		},
		{
			name:        "pod is enqueued to activeQ because the pending plugin has a hint fn and it returns Queue for a concurrent event that was received while some other pod was in flight",
			initialPods: []*v1.Pod{pod1, pod2},
			actions: []action{
				{callback: func(t *testing.T, q *PriorityQueue) { poppedPod = popPod(t, logger, q, pod1) }},
				{eventHappens: &nodeAdd},
				{callback: func(t *testing.T, q *PriorityQueue) { poppedPod2 = popPod(t, logger, q, pod2) }},
				{eventHappens: &framework.EventAssignedPodAdd},
				{callback: func(t *testing.T, q *PriorityQueue) {
					logger, _ := ktesting.NewTestContext(t)
					// 没有插件注册为 unschedulable 插件，该 Pod 会立即重新入队到 backoffQ，
					// 这意味着 Pod 遇到了意外错误（例如网络错误）。
					err := q.AddUnschedulablePodIfNotPresent(logger, poppedPod, q.SchedulingCycle())
					if err != nil {
						t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
					}
				}},
				{callback: func(t *testing.T, q *PriorityQueue) {
					logger, _ := ktesting.NewTestContext(t)
					poppedPod2.UnschedulablePlugins = sets.New("fooPlugin2", "fooPlugin3")
					poppedPod2.PendingPlugins = sets.New("fooPlugin1")
					err := q.AddUnschedulablePodIfNotPresent(logger, poppedPod2, q.SchedulingCycle())
					if err != nil {
						t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
					}
				}},
			},
			wantActiveQPodNames:  []string{pod2.Name},
			wantBackoffQPodNames: []string{pod1.Name},
			wantInFlightPods:     nil,
			wantInFlightEvents:   nil,
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					framework.EventAssignedPodAdd: {
						{
							// 它会被忽略：hint 函数返回 QueueSkip，弱于 fooPlugin1 的 queueHintReturnQueueImmediately。
							PluginName:     "fooPlugin3",
							QueueingHintFn: queueHintReturnSkip,
						},
						{
							// 它会被忽略：fooPlugin2 注册在 UnschedulablePlugins 中，其 Queue 弱于 fooPlugin1 的 QueueImmediately。
							PluginName:     "fooPlugin2",
							QueueingHintFn: queueHintReturnQueue,
						},
						{
							// hint 函数认为此事件使 Pod 可调度。
							// 由于 fooPlugin1 注册在 Pendings 中，Queue 被解释为 queueImmediately。
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
				},
			},
		},
		{
			name:        "popped pod preserves UnschedulablePlugins and PendingPlugins",
			initialPods: []*v1.Pod{pod1},
			actions: []action{
				{callback: func(t *testing.T, q *PriorityQueue) { poppedPod = popPod(t, logger, q, pod1) }},
				{callback: func(t *testing.T, q *PriorityQueue) {
					logger, _ := ktesting.NewTestContext(t)
					// 因 PendingPlugins 而不可调度。
					poppedPod.PendingPlugins = sets.New("fooPlugin1")
					poppedPod.UnschedulablePlugins = sets.New("fooPlugin2")
					if err := q.AddUnschedulablePodIfNotPresent(logger, poppedPod, q.SchedulingCycle()); err != nil {
						t.Errorf("Unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
					}
				}},
				{eventHappens: &pvAdd}, // 再次活跃。
				{callback: func(t *testing.T, q *PriorityQueue) {
					poppedPod = popPod(t, logger, q, pod1)
					// 应保留 UnschedulablePlugins 以便记录日志/调试
					if !poppedPod.GetUnschedulablePlugins().Equal(sets.New("fooPlugin2")) {
						t.Errorf("QueuedPodInfo from Pop should preserve UnschedulablePlugins, expected fooPlugin2, got: %+v", poppedPod.GetUnschedulablePlugins())
					}
					// Pop() 后保留 PendingPlugins 以便记录日志
					if !poppedPod.PendingPlugins.Equal(sets.New("fooPlugin1")) {
						t.Errorf("QueuedPodInfo from Pop should preserve PendingPlugins, expected fooPlugin1, got: %+v", poppedPod.PendingPlugins)
					}
				}},
				{callback: func(t *testing.T, q *PriorityQueue) {
					logger, _ := ktesting.NewTestContext(t)
					// 失败（即没有 UnschedulablePlugins），应进入 backoff。
					if err := q.AddUnschedulablePodIfNotPresent(logger, poppedPod, q.SchedulingCycle()); err != nil {
						t.Errorf("Unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
					}
				}},
			},
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					pvAdd: {
						{
							// hint 函数认为此事件立即使 Pod 可调度。
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
				},
			},
		},
		{
			// 除非引入类似 https://github.com/kubernetes/kubernetes/issues/118226 的 bug，否则不应出现该场景。
			// 但该 bug 可能造成严重且难以发现的内存泄漏，
			// 应对同一 bug 加一道防护，至少可以避免内存泄漏。
			name:        "Pop is made twice for the same Pod, but the cleanup still happen correctly",
			initialPods: []*v1.Pod{pod1, pod2},
			actions: []action{
				// 此时还没有 in-flight Pod，因此不会加入 inFlightEvents。
				{eventHappens: &pvcAdd},
				{podPopped: pod1},
				{eventHappens: &pvAdd},
				{podPopped: pod2},
				// 模拟一个 bug：Pod 正在调度时又被放进 activeQ。
				{callback: func(t *testing.T, q *PriorityQueue) {
					q.activeQ.add(logger, newQueuedPodInfoForLookup(pod1), framework.EventUnscheduledPodAdd.Label(), nil)
				}},
				// 此时 activeQ 中依次是 pod1 和 pod3。
				{podCreated: pod3},
				// 弹出的是 pod3，不是 pod1。
				// 具体来说，本次 Pop() 先尝试弹出 pod1，但它正在调度中，因此被丢弃。
				// 然后弹出下一个 Pod，即 pod3。
				{podPopped: pod3},
				{callback: func(t *testing.T, q *PriorityQueue) {
					// 确认 pod1 被丢弃，activeQ 中已无 Pod。
					if len(q.activeQ.list()) != 0 {
						t.Fatalf("activeQ should be empty, but got: %v", q.activeQ.list())
					}
				}},
				{eventHappens: &nodeAdd},
				// 没有插件注册为 unschedulable 插件，该 Pod 会立即重新入队到 backoffQ，
				// 这意味着 Pod 遇到了意外错误（例如网络错误）。
				{podEnqueued: newQueuedPodInfoForLookup(pod1)},
				{eventHappens: &csiNodeUpdate},
				// 没有插件注册为 unschedulable 插件，该 Pod 会立即重新入队到 backoffQ，
				// 这意味着 Pod 遇到了意外错误（例如网络错误）。
				{podEnqueued: newQueuedPodInfoForLookup(pod2)},
				// 没有插件注册为 unschedulable 插件，该 Pod 会立即重新入队到 backoffQ，
				// 这意味着 Pod 遇到了意外错误（例如网络错误）。
				{podEnqueued: newQueuedPodInfoForLookup(pod3)},
			},
			wantBackoffQPodNames: []string{"targetpod", "targetpod2", "targetpod3"},
			wantInFlightPods:     nil, // 应为空
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					pvAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
					nodeAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
					pvcAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
					csiNodeUpdate: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
				},
			},
		},
		{
			name:                   "Pod group members and interested events are registered in inFlightPods/inFlightEvents",
			genericWorkloadEnabled: []bool{true},
			initialPods:            []*v1.Pod{pgPod1, pgPod2},
			actions: []action{
				{podPopped: pgPod1}, // 弹出整个组，因此 pgPod1 与 pgPod2 均在途
				{eventHappens: &pvAdd},
			},
			wantInFlightPods:   []*v1.Pod{pgPod1, pgPod2},
			wantInFlightEvents: []any{pgPod1, pgPod2, pvAdd},
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					pvAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
				},
			},
		},
		{
			name:                   "Pop is made twice for all pods in the group, but the cleanup still happens correctly",
			genericWorkloadEnabled: []bool{true},
			initialPods:            []*v1.Pod{pgPod1, pgPod2},
			actions: []action{
				// 此时还没有 in-flight Pod，因此不会加入 inFlightEvents。
				{eventHappens: &pvcAdd},
				// 弹出 PodGroup，因此 pgPod1 和 pgPod2 处于 in-flight。
				{podPopped: pgPod1},
				{eventHappens: &pvAdd},
				{podGroupAttempted: newQueuedPodGroupInfoForLookup(pgPod1.Namespace, pgName, fwk.PodGroupKeyType)},
				// 模拟一个 bug：PodGroup 处于 in-flight 时把 pgPod1、pgPod2 加回 activeQ。
				{podCreated: pgPod1},
				{podCreated: pgPod2},
				{podGroupAdded: pgTest},
				// 此时 activeQ 中依次是 PodGroup（含 pgPod1、pgPod2）和 pod3。
				{podCreated: pod3},
				// 弹出的是 pod3，不是 pgPod1。
				// 具体来说，本次 Pop() 先尝试弹出含 pgPod1、pgPod2 的 PodGroup，但它正在调度中，因此被丢弃。
				// 然后弹出下一个 Pod，即 pod3。
				{podPopped: pod3},
				{callback: func(t *testing.T, q *PriorityQueue) {
					// 确认 PodGroup 被丢弃，activeQ 中已无实体。
					if len(q.activeQ.list()) != 0 {
						t.Fatalf("activeQ should be empty, but got: %v", q.activeQ.list())
					}
				}},
				{eventHappens: &nodeAdd},
				// 该 Pod 是 PodGroup 成员，而队列中的 PodGroup 本身已丢失，因此会被重新入队到 activeQ。
				{podEnqueued: newQueuedPodInfoForLookup(pgPod1)},
				{eventHappens: &csiNodeUpdate},
				// 该 Pod 是 PodGroup 成员，而队列中的 PodGroup 本身已丢失，因此会被重新入队到 activeQ。
				{podEnqueued: newQueuedPodInfoForLookup(pgPod2)},
				{podGroupAdded: pgTest},
				// 没有插件注册为 unschedulable 插件，该 Pod 会立即重新入队到 backoffQ，
				// 这意味着 Pod 遇到了意外错误（例如网络错误）。
				{podEnqueued: newQueuedPodInfoForLookup(pod3)},
			},
			wantActiveQPodNames:  []string{"pgpod1", "pgpod2"},
			wantBackoffQPodNames: []string{"targetpod3"},
			wantInFlightPods:     nil, // 应为空
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					pvAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
					nodeAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
					pvcAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
					csiNodeUpdate: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
				},
			},
		},
		{
			name:                   "Pop is made twice for a subset (one) pod in the group, but the cleanup still happens correctly",
			genericWorkloadEnabled: []bool{true},
			initialPods:            []*v1.Pod{pgPod1},
			actions: []action{
				// 此时还没有 in-flight Pod，因此不会加入 inFlightEvents。
				{eventHappens: &pvcAdd},
				// 弹出 PodGroup，因此 pgPod1 处于 in-flight。
				{podPopped: pgPod1},
				{eventHappens: &pvAdd},
				{podGroupAttempted: newQueuedPodGroupInfoForLookup(pgPod1.Namespace, pgName, fwk.PodGroupKeyType)},
				// 模拟一个 bug：PodGroup 处于 in-flight 时把 pgPod1 加回 activeQ。
				{podCreated: pgPod1},
				// 向 activeQ 加入新的 pgPod2。
				{podCreated: pgPod2},
				{podGroupAdded: pgTest},
				// 此时 activeQ 中依次是 PodGroup（含 pgPod1、pgPod2）和 pod3。
				{podCreated: pod3},
				// pgPod2 被弹出，pgPod1 被丢弃。
				{podPopped: pgPod2},
				// 弹出的是 pod3。
				{podPopped: pod3},
				{callback: func(t *testing.T, q *PriorityQueue) {
					// 确认 pgPod1 被丢弃，activeQ 中已无实体。
					if len(q.activeQ.list()) != 0 {
						t.Fatalf("activeQ should be empty, but got: %v", q.activeQ.list())
					}
				}},
				{eventHappens: &nodeAdd},
				// 该 Pod 是 PodGroup 成员，而队列中的 PodGroup 本身已丢失，因此会被重新入队到 activeQ。
				{podEnqueued: newQueuedPodInfoForLookup(pgPod1)},
				{eventHappens: &csiNodeUpdate},
				// 该 Pod 是 PodGroup 成员，而队列中的 PodGroup 本身已丢失，因此会被重新入队到 activeQ。
				{podEnqueued: newQueuedPodInfoForLookup(pgPod2)},
				{podGroupAdded: pgTest},
				// 没有插件注册为 unschedulable 插件，该 Pod 会立即重新入队到 backoffQ，
				// 这意味着 Pod 遇到了意外错误（例如网络错误）。
				{podEnqueued: newQueuedPodInfoForLookup(pod3)},
			},
			wantActiveQPodNames: []string{"pgpod1", "pgpod2"},
			wantInFlightPods:    nil, // 应为空
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					pvAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
					nodeAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
					pvcAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
					csiNodeUpdate: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
				},
			},
		},
		{
			name:                   "pod group is enqueued to backoff if no failed plugin",
			genericWorkloadEnabled: []bool{true},
			initialPods:            []*v1.Pod{pgPod1, pgPod2},
			actions: []action{
				{podPopped: pgPod1}, // 弹出整个组，因此 pgPod1 与 pgPod2 均在途
				// 不带插件依次重新入队（意外错误）
				{podEnqueued: &framework.QueuedPodInfo{
					PodInfo: mustNewPodInfo(pgPod1),
				}},
				{podEnqueued: &framework.QueuedPodInfo{
					PodInfo: mustNewPodInfo(pgPod2),
				}},
				{podGroupAttempted: newQueuedPodGroupInfoForLookup(pgPod1.Namespace, pgName, fwk.PodGroupKeyType)},
			},
			wantBackoffQPodNames: []string{"pgpod1", "pgpod2"},
		},
		{
			name:                   "pod group is enqueued to backoff even if there were no events that can make the pod group schedulable",
			genericWorkloadEnabled: []bool{true},
			initialPods:            []*v1.Pod{pgPod1, pgPod2},
			actions: []action{
				{podPopped: pgPod1}, // 弹出整个组，因此 pgPod1 与 pgPod2 均在途
				// 带失败插件依次重新入队
				{podEnqueued: &framework.QueuedPodInfo{
					PodInfo: mustNewPodInfo(pgPod1),
					QueueingParams: framework.QueueingParams{
						UnschedulablePlugins: sets.New("fooPlugin1"),
					},
				}},
				{podEnqueued: &framework.QueuedPodInfo{
					PodInfo: mustNewPodInfo(pgPod2),
					QueueingParams: framework.QueueingParams{
						UnschedulablePlugins: sets.New("fooPlugin1"),
					},
				}},
				{podGroupAttempted: newQueuedPodGroupInfoForLookup(pgPod1.Namespace, pgName, fwk.PodGroupKeyType)},
			},
			wantBackoffQPodNames: []string{"pgpod1", "pgpod2"},
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					pvAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		for _, genericWorkloadEnabled := range test.genericWorkloadEnabled {
			t.Run(fmt.Sprintf("%s (genericWorkloadEnabled: %v)", test.name, genericWorkloadEnabled), func(t *testing.T) {
				featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
					features.GenericWorkload: genericWorkloadEnabled,
				})
				logger, ctx := ktesting.NewTestContext(t)
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				obj := make([]runtime.Object, 0, len(test.initialPods))
				pgNamesSeen := sets.New[string]()
				var podGroupsToAdd []*schedulingv1beta1.PodGroup
				for _, p := range test.initialPods {
					obj = append(obj, p)
					if p.Spec.SchedulingGroup != nil && p.Spec.SchedulingGroup.PodGroupName != nil {
						pgName := *p.Spec.SchedulingGroup.PodGroupName
						if !pgNamesSeen.Has(pgName) {
							pgNamesSeen.Insert(pgName)
							pg := st.MakePodGroup().Name(pgName).Namespace(p.Namespace).Obj()
							podGroupsToAdd = append(podGroupsToAdd, pg)
						}
					}
				}
				fakeClock := testingclock.NewFakeClock(time.Now())
				q := NewTestQueueWithObjects(ctx, newDefaultQueueSort(), obj, WithQueueingHintMapPerProfile(test.queueingHintMap), WithClock(fakeClock))
				sortOpt := cmpopts.SortSlices(func(a, b string) bool { return a < b })
				if genericWorkloadEnabled {
					for _, pg := range podGroupsToAdd {
						q.AddPodGroup(logger, pg)
					}
				}

				// Pod 加入队列时，QueuedPodInfo 会获得新时间戳。
				// Windows 上 time.Now() 精度较低，连续两次调用可能得到相同时间戳。
				// 因此所有 QueuedPodInfo 可能时间戳相同，这会带来问题：
				// 而我们期望它们按特定顺序弹出（Less 函数
				// 相同 Pod 优先级时会按时间戳排序）。
				// 给队列使用假时钟，并在每次加入 Pod 后递增，可以
				// 解决 Windows 单元测试中的这一问题。
				// Windows 时钟精度问题的详情见：https://github.com/golang/go/issues/8687
				for _, p := range test.initialPods {
					q.Add(ctx, p)
					fakeClock.Step(time.Second)
				}

				for _, action := range test.actions {
					switch {
					case action.podCreated != nil:
						q.Add(ctx, action.podCreated)
					case action.podPopped != nil:
						popPod(t, logger, q, action.podPopped)
					case action.eventHappens != nil:
						q.MoveAllToActiveOrBackoffQueue(logger, *action.eventHappens, nil, nil, nil)
					case action.podEnqueued != nil:
						err := q.AddUnschedulablePodIfNotPresent(logger, action.podEnqueued, q.SchedulingCycle())
						if err != nil {
							t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
						}
					case action.podGroupAttempted != nil:
						err := q.AddAttemptedPodGroupIfNeeded(logger, action.podGroupAttempted, q.SchedulingCycle(), fwk.NewStatus(fwk.Error))
						if err != nil {
							t.Fatalf("unexpected error from AddAttemptedPodGroupIfNeeded: %v", err)
						}
					case action.podGroupAdded != nil:
						q.AddPodGroup(logger, action.podGroupAdded)
					case action.callback != nil:
						action.callback(t, q)
					}
				}

				actualInFlightPods := make(map[types.UID]*v1.Pod)
				for _, pod := range q.activeQ.listInFlightPods() {
					actualInFlightPods[pod.UID] = pod
				}
				wantInFlightPods := make(map[types.UID]*v1.Pod)
				for _, pod := range test.wantInFlightPods {
					wantInFlightPods[pod.UID] = pod
				}
				if diff := cmp.Diff(wantInFlightPods, actualInFlightPods); diff != "" {
					t.Errorf("Unexpected diff in inFlightPods (-want, +got):\n%s", diff)
				}

				var wantInFlightEvents []interface{}
				for _, value := range test.wantInFlightEvents {
					if event, ok := value.(fwk.ClusterEvent); ok {
						value = &clusterEvent{event: event}
					}
					wantInFlightEvents = append(wantInFlightEvents, value)
				}
				if diff := cmp.Diff(wantInFlightEvents, q.activeQ.listInFlightEvents(), cmp.AllowUnexported(clusterEvent{}), cmpopts.EquateComparable(fwk.ClusterEvent{})); diff != "" {
					t.Errorf("Unexpected diff in inFlightEvents (-want, +got):\n%s", diff)
				}

				if test.wantActiveQPodNames != nil {
					pods := q.activeQ.list()
					var podNames []string
					for _, pod := range pods {
						podNames = append(podNames, pod.Name)
					}
					if diff := cmp.Diff(test.wantActiveQPodNames, podNames, sortOpt); diff != "" {
						t.Fatalf("Unexpected diff of activeQ pod names (-want, +got):\n%s", diff)
					}

					wantPodNames := sets.New(test.wantActiveQPodNames...)
					for _, pod := range pods {
						if !wantPodNames.Has(pod.Name) {
							t.Fatalf("Pod %v was not expected to be in the activeQ.", pod.Name)
						}
					}
				}

				if test.wantBackoffQPodNames != nil {
					pods := q.backoffQ.list()
					var podNames []string
					for _, pod := range pods {
						podNames = append(podNames, pod.Name)
					}
					if diff := cmp.Diff(test.wantBackoffQPodNames, podNames, sortOpt); diff != "" {
						t.Fatalf("Unexpected diff of backoffQ pod names (-want, +got):\n%s", diff)
					}

					wantPodNames := sets.New(test.wantBackoffQPodNames...)
					for _, podGotFromBackoffQ := range pods {
						if !wantPodNames.Has(podGotFromBackoffQ.Name) {
							t.Fatalf("Pod %v was not expected to be in the backoffQ.", podGotFromBackoffQ.Name)
						}
					}
				}

				for _, podName := range test.wantUnschedPodPoolPodNames {
					p := getUnschedulablePod(q, &st.MakePod().Name(podName).Pod)
					if p == nil {
						t.Fatalf("Pod %v was not found in the unschedulableEntities.", podName)
					}
				}
			})
		}
	}
}

func popPod(t *testing.T, logger klog.Logger, q *PriorityQueue, pod *v1.Pod) *framework.QueuedPodInfo {
	entity, err := q.Pop(logger)
	if err != nil {
		t.Fatalf("Pop failed: %v", err)
	}
	var pInfo *framework.QueuedPodInfo
	switch specificEntity := entity.(type) {
	case *framework.QueuedPodInfo:
		pInfo = specificEntity
	case *framework.QueuedPodGroupInfo:
		specificEntity.ForEachPodInfo(func(pi *framework.QueuedPodInfo) bool {
			if pi.Pod.UID == pod.UID {
				pInfo = pi
				return false
			}
			return true
		})
	default:
		t.Fatalf("unexpected popped entity type: %T", entity)
	}
	if pInfo == nil {
		t.Fatalf("Pod %s was not found in the popped entity", pod.UID)
	}
	if pInfo.Pod.UID != pod.UID {
		t.Errorf("Unexpected popped pod: expected %s, got %s", pod.UID, pInfo.Pod.UID)
	}
	return pInfo
}

// TestPop 验证 Pop 在队列为空、重复弹出等场景下的行为。
func TestPop(t *testing.T) {
	pod := st.MakePod().Name("targetpod").UID("pod1").Obj()
	queueingHintMap := QueueingHintMapPerProfile{
		"": {
			pvAdd: {
				{
					// hint 函数认为此事件使 Pod 可调度。
					PluginName:     "fooPlugin1",
					QueueingHintFn: queueHintReturnQueue,
				},
			},
		},
	}
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	q := NewTestQueueWithObjects(ctx, newDefaultQueueSort(), []runtime.Object{pod}, WithQueueingHintMapPerProfile(queueingHintMap))
	q.Add(ctx, pod)

	// 模拟使 Pod 变为不可调度的失败尝试。
	poppedPod := popPod(t, logger, q, pod)
	// 把插件注册到 PendingPlugins，这样会被解释为 queueImmediately 并跳过退避。
	poppedPod.PendingPlugins = sets.New("fooPlugin1")
	if err := q.AddUnschedulablePodIfNotPresent(logger, poppedPod, q.SchedulingCycle()); err != nil {
		t.Errorf("Unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
	}

	// 再次激活它。
	q.MoveAllToActiveOrBackoffQueue(logger, pvAdd, nil, nil, nil)

	// 现在检查 Pop 的结果。
	poppedPod = popPod(t, logger, q, pod)
	// Pop() 后保留 PendingPlugins，这样调度成功时可记录日志，
	// 调度成功时可记录日志，失败时在 handleSchedulingFailure() 中清除。
	if !poppedPod.PendingPlugins.Equal(sets.New("fooPlugin1")) {
		t.Errorf("QueuedPodInfo from Pop should preserve PendingPlugins, expected fooPlugin1, got instead: %+v", poppedPod)
	}
}

// TestPriorityQueue_AddUnschedulablePodIfNotPresent 验证 AddUnschedulablePodIfNotPresent 对 in-flight Pod 事件的处理。
func TestPriorityQueue_AddUnschedulablePodIfNotPresent(t *testing.T) {
	objs := []runtime.Object{highPriNominatedPodInfo.Pod, unschedulablePodInfo.Pod}
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	q := NewTestQueueWithObjects(ctx, newDefaultQueueSort(), objs)
	// 插入 unschedulablePodInfo 后立刻弹出
	// 因为调度队列把 unschedulablePod 记录为 in-flight Pod。
	q.Add(ctx, unschedulablePodInfo.Pod)
	if p, err := q.Pop(logger); err != nil {
		t.Errorf("Pop failed: %v", err)
	} else if diff := cmp.Diff(unschedulablePodInfo.Pod, p.(*framework.QueuedPodInfo).Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}

	q.Add(ctx, highPriNominatedPodInfo.Pod)
	err := q.AddUnschedulablePodIfNotPresent(logger, newQueuedPodInfoForLookup(unschedulablePodInfo.Pod, "plugin"), q.SchedulingCycle())
	if err != nil {
		t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
	}
	if p, err := q.Pop(logger); err != nil {
		t.Errorf("Pop failed: %v", err)
	} else if diff := cmp.Diff(highPriNominatedPodInfo.Pod, p.(*framework.QueuedPodInfo).Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}
	if len(q.nominator.nominatedPods) != 1 {
		t.Errorf("Expected nominatedPods to have one element: %v", q.nominator)
	}
	// 调度期间没有事件发生，unschedulablePodInfo 被插入不可调度 Pod 池。
	if diff := cmp.Diff(unschedulablePodInfo.Pod, getUnschedulablePod(q, unschedulablePodInfo.Pod)); diff != "" {
		t.Errorf("Unexpected pod in unschedulableEntities (-want, +got):\n%s", diff)
	}
}

// TestPriorityQueue_AddUnschedulablePodIfNotPresent_Backoff 测试以下场景：
// AddUnschedulablePodIfNotPresent 被异步调用。
// 当前及更早调度周期的 Pod 会被放回 activeQueue
// 如果收到 move 请求时正在尝试调度它们。
func TestPriorityQueue_AddUnschedulablePodIfNotPresent_Backoff(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	q := NewTestQueue(ctx, newDefaultQueueSort(), WithClock(testingclock.NewFakeClock(time.Now())))
	totalNum := 10
	expectedPods := make([]v1.Pod, 0, totalNum)
	for i := 0; i < totalNum; i++ {
		priority := int32(i)
		p := st.MakePod().Name(fmt.Sprintf("pod%d", i)).Namespace(fmt.Sprintf("ns%d", i)).UID(fmt.Sprintf("upns%d", i)).Priority(priority).Obj()
		expectedPods = append(expectedPods, *p)
		// 设置优先级使 Pod 在 PriorityQueue 中有序
		q.Add(ctx, p)
	}

	// 弹出除第一个外的所有 Pod
	for i := totalNum - 1; i > 0; i-- {
		p, _ := q.Pop(logger)
		if diff := cmp.Diff(&expectedPods[i], p.(*framework.QueuedPodInfo).Pod); diff != "" {
			t.Errorf("Unexpected pod (-want, +got):\n%s", diff)
		}
	}

	// 在尝试调度它们时把所有 Pod 移到 active queue
	q.MoveAllToActiveOrBackoffQueue(logger, framework.EventUnschedulableTimeout, nil, nil, nil)
	oldCycle := q.SchedulingCycle()

	item, _ := q.Pop(logger)
	firstPod := item.(*framework.QueuedPodInfo)
	if diff := cmp.Diff(&expectedPods[0], firstPod.Pod); diff != "" {
		t.Errorf("Unexpected pod (-want, +got):\n%s", diff)
	}

	// 把 pods[1] ~ pods[totalNum-1] 标记为不可调度并加回队列
	for i := 1; i < totalNum; i++ {
		unschedulablePod := expectedPods[i].DeepCopy()
		unschedulablePod.Status = v1.PodStatus{
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodScheduled,
					Status: v1.ConditionFalse,
					Reason: v1.PodReasonUnschedulable,
				},
			},
		}

		err := q.AddUnschedulablePodIfNotPresent(logger, newQueuedPodInfoForLookup(unschedulablePod, "plugin"), oldCycle)
		if err != nil {
			t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
		}
	}

	// 由于收到 move 请求时正处于 "oldCycle" 周期，这些 Pod
	// 应位于 backoff 队列。
	for i := 1; i < totalNum; i++ {
		if !q.backoffQ.has(newQueuedPodInfoForLookup(&expectedPods[i])) {
			t.Errorf("Expected %v to be added to backoffQ.", expectedPods[i].Name)
		}
	}
}

// tryPop 尝试从队列中弹出一个实体并返回。
// 最多等待 5 秒后超时，假定此时队列为空。
func tryPop(t *testing.T, logger klog.Logger, q *PriorityQueue) framework.QueuedEntityInfo {
	t.Helper()

	var gotEntity framework.QueuedEntityInfo
	popped := make(chan struct{}, 1)
	go func() {
		entity, err := q.Pop(logger)
		if err != nil {
			t.Errorf("Failed to pop entity from scheduling queue: %s", err)
		}
		if entity != nil {
			gotEntity = entity
		}
		popped <- struct{}{}
	}()

	timer := time.NewTimer(5 * time.Second)
	select {
	case <-timer.C:
		q.Close()
	case <-popped:
		timer.Stop()
	}
	return gotEntity
}

// TestPriorityQueue_Pop 验证 PriorityQueue.Pop 按优先级和退避状态弹出实体。
func TestPriorityQueue_Pop(t *testing.T) {
	highPriorityPodInfo2 := mustNewPodInfo(
		st.MakePod().Name("hpp2").Namespace("ns1").UID("hpp2ns1").Priority(highPriority).Obj(),
	)
	tests := []struct {
		name                   string
		popFromBackoffQEnabled bool
		genericWorkloadEnabled bool
		usePodGroups           bool
		wantPods               []string
	}{
		{
			name:                   "individual pod, GenericWorkload enabled, PopFromBackoffQ enabled, pops from both activeQ and backoffQ",
			popFromBackoffQEnabled: true,
			genericWorkloadEnabled: true,
			usePodGroups:           false,
			wantPods:               []string{medPriorityPodInfo.Pod.Name, highPriorityPodInfo.Pod.Name},
		},
		{
			name:                   "individual pod, GenericWorkload enabled, PopFromBackoffQ disabled, pops only from activeQ",
			popFromBackoffQEnabled: false,
			genericWorkloadEnabled: true,
			usePodGroups:           false,
			wantPods:               []string{medPriorityPodInfo.Pod.Name},
		},
		{
			name:                   "individual pod, GenericWorkload disabled, PopFromBackoffQ enabled, pops from both activeQ and backoffQ",
			popFromBackoffQEnabled: true,
			genericWorkloadEnabled: false,
			usePodGroups:           false,
			wantPods:               []string{medPriorityPodInfo.Pod.Name, highPriorityPodInfo.Pod.Name},
		},
		{
			name:                   "individual pod, GenericWorkload disabled, PopFromBackoffQ disabled, pops only from activeQ",
			popFromBackoffQEnabled: false,
			genericWorkloadEnabled: false,
			usePodGroups:           false,
			wantPods:               []string{medPriorityPodInfo.Pod.Name},
		},
		{
			name:                   "pod group, PopFromBackoffQ enabled, pops from both activeQ and backoffQ",
			popFromBackoffQEnabled: true,
			genericWorkloadEnabled: true,
			usePodGroups:           true,
			wantPods:               []string{medPriorityPodInfo.Pod.Name, highPriorityPodInfo.Pod.Name},
		},
		{
			name:                   "pod group, PopFromBackoffQ disabled, pops only from activeQ",
			popFromBackoffQEnabled: false,
			genericWorkloadEnabled: true,
			usePodGroups:           true,
			wantPods:               []string{medPriorityPodInfo.Pod.Name},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload:          tt.genericWorkloadEnabled,
				features.SchedulerPopFromBackoffQ: tt.popFromBackoffQEnabled,
			})

			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			medPod, backoffPod, errorBackoffPod, unschedPod := medPriorityPodInfo.Pod, highPriorityPodInfo.Pod, highPriorityPodInfo2.Pod, unschedulablePodInfo.Pod
			if tt.usePodGroups {
				medPod = withPodGroupName(medPriorityPodInfo.Pod, "pg-med")
				backoffPod = withPodGroupName(highPriorityPodInfo.Pod, "pg-backoff")
				errorBackoffPod = withPodGroupName(highPriorityPodInfo2.Pod, "pg-errbackoff")
				unschedPod = withPodGroupName(unschedulablePodInfo.Pod, "pg-unsched")
			}

			objs := []runtime.Object{medPod, backoffPod, errorBackoffPod, unschedPod}
			q := NewTestQueueWithObjects(ctx, newDefaultQueueSort(), objs)

			var medEntity, backoffEntity, errorBackoffEntity, unschedEntity framework.QueuedEntityInfo
			if tt.usePodGroups {
				medEntity = newSingleLevelPodGroupInfo(q.newQueuedPodInfo(ctx, medPod), nil)
				backoffPodGroup := newSingleLevelPodGroupInfo(q.newQueuedPodInfo(ctx, backoffPod, "plugin"), nil)
				backoffPodGroup.UnschedulablePlugins = sets.New("plugin")
				backoffEntity = backoffPodGroup
				errorBackoffEntity = newSingleLevelPodGroupInfo(q.newQueuedPodInfo(ctx, errorBackoffPod), nil)
				unschedPodGroup := newSingleLevelPodGroupInfo(q.newQueuedPodInfo(ctx, unschedPod, "plugin"), nil)
				unschedPodGroup.UnschedulablePlugins = sets.New("plugin")
				unschedEntity = unschedPodGroup
			} else {
				medEntity = q.newQueuedPodInfo(ctx, medPod)
				backoffEntity = q.newQueuedPodInfo(ctx, backoffPod, "plugin")
				errorBackoffEntity = q.newQueuedPodInfo(ctx, errorBackoffPod)
				unschedEntity = q.newQueuedPodInfo(ctx, unschedPod, "plugin")
			}

			// 把中优先级实体加入 activeQ
			q.activeQ.add(logger, medEntity, framework.EventUnscheduledPodAdd.Label(), nil)
			// 把高优先级实体加入 backoffQ
			q.backoffQ.add(logger, backoffEntity, framework.EventUnscheduledPodAdd.Label(), nil)
			// 把高优先级实体加入 errorBackoffQ
			q.backoffQ.add(logger, errorBackoffEntity, framework.EventUnscheduledPodAdd.Label(), nil)
			// 把实体加入 unschedulableEntities
			q.unschedulableEntities.addOrUpdate(unschedEntity, false, framework.EventUnscheduledPodAdd.Label(), nil)

			var gotPods []string
			for i := 0; i < len(tt.wantPods)+1; i++ {
				gotEntity := tryPop(t, logger, q)
				if gotEntity == nil {
					break
				}
				if _, isPodGroup := gotEntity.(*framework.QueuedPodGroupInfo); isPodGroup != tt.usePodGroups {
					t.Errorf("Expected queued pod group: %v, got: %v", tt.usePodGroups, isPodGroup)
				}
				gotEntity.ForEachPodInfo(func(pInfo *framework.QueuedPodInfo) bool {
					gotPods = append(gotPods, pInfo.Pod.Name)
					return true
				})
			}

			if diff := cmp.Diff(tt.wantPods, gotPods); diff != "" {
				t.Errorf("Unexpected popped pods (-want, +got): %s", diff)
			}
		})
	}
}

// TestPriorityQueue_Update 验证 Update 对队列中及 in-flight Pod 的处理。
func TestPriorityQueue_Update(t *testing.T) {
	c := testingclock.NewFakeClock(time.Now())

	queuePlugin := "queuePlugin"
	skipPlugin := "skipPlugin"
	queueingHintMap := QueueingHintMapPerProfile{
		"": {
			framework.EventTargetPodUpdate: {
				{
					PluginName:     queuePlugin,
					QueueingHintFn: queueHintReturnQueue,
				},
				{
					PluginName:     skipPlugin,
					QueueingHintFn: queueHintReturnSkip,
				},
			},
		},
	}

	withGate := func(p *v1.Pod) *v1.Pod {
		newPod := p.DeepCopy()
		newPod.Labels = map[string]string{"deny": "true"}
		return newPod
	}

	notInAnyQueue := "NotInAnyQueue"
	tests := []struct {
		name  string
		wantQ string
		// wantAddedToNominated 表示测试用例中的 Pod 是否应注册为 nominator 中的 nominated Pod。
		wantAddedToNominated bool
		// prepareFunc 在测试用例调用 Update() 前准备队列中的 Pod。
		// 该函数返回三个值；
		// - oldPod/newPod：各测试用例会以它们调用 Update()。
		prepareFunc func(tCtx ktesting.TContext, q *PriorityQueue) (oldPod, newPod *v1.Pod)
	}{
		{
			name:  "Update pod that didn't exist in the queue",
			wantQ: activeQ,
			prepareFunc: func(tCtx ktesting.TContext, q *PriorityQueue) (oldPod, newPod *v1.Pod) {
				updatedPod := medPriorityPodInfo.Pod.DeepCopy()
				updatedPod.Annotations["foo"] = "test"
				return medPriorityPodInfo.Pod, updatedPod
			},
		},
		{
			name:  "Update gated pod that didn't exist in the queue",
			wantQ: unschedulableQ,
			prepareFunc: func(tCtx ktesting.TContext, q *PriorityQueue) (oldPod, newPod *v1.Pod) {
				updatedPod := withGate(medPriorityPodInfo.Pod)
				updatedPod.Annotations["foo"] = "test"
				return withGate(medPriorityPodInfo.Pod), updatedPod
			},
		},
		{
			name:                 "Update non-existent highPriorityPodInfo and add a nominatedNodeName to it",
			wantQ:                activeQ,
			wantAddedToNominated: true,
			prepareFunc: func(tCtx ktesting.TContext, q *PriorityQueue) (oldPod, newPod *v1.Pod) {
				return highPriorityPodInfo.Pod, highPriNominatedPodInfo.Pod
			},
		},
		{
			name:                 "Update non-existent gated highPriorityPodInfo and add a nominatedNodeName to it",
			wantQ:                unschedulableQ,
			wantAddedToNominated: true,
			prepareFunc: func(tCtx ktesting.TContext, q *PriorityQueue) (oldPod, newPod *v1.Pod) {
				return withGate(highPriorityPodInfo.Pod), withGate(highPriNominatedPodInfo.Pod)
			},
		},
		{
			name:  "When updating a pod that is already in activeQ, the pod should remain in activeQ after Update()",
			wantQ: activeQ,
			prepareFunc: func(tCtx ktesting.TContext, q *PriorityQueue) (oldPod, newPod *v1.Pod) {
				q.Add(tCtx, highPriorityPodInfo.Pod)
				return highPriorityPodInfo.Pod, highPriorityPodInfo.Pod
			},
		},
		{
			name:  "When updating a pod that is in backoff queue and is still backing off, it will be updated in backoff queue",
			wantQ: backoffQ,
			prepareFunc: func(tCtx ktesting.TContext, q *PriorityQueue) (oldPod, newPod *v1.Pod) {
				podInfo := q.newQueuedPodInfo(tCtx, medPriorityPodInfo.Pod)
				q.backoffQ.add(klog.FromContext(tCtx), podInfo, framework.EventUnscheduledPodAdd.Label(), nil)
				return podInfo.Pod, podInfo.Pod
			},
		},
		{
			name:  "when updating a pod in unschedulableEntities, if its backoff timer has not yet expired, it moves to backoffQ",
			wantQ: backoffQ,
			prepareFunc: func(tCtx ktesting.TContext, q *PriorityQueue) (oldPod, newPod *v1.Pod) {
				pInfo := q.newQueuedPodInfo(tCtx, medPriorityPodInfo.Pod, queuePlugin)
				// 需要递增才能使 Pod 进入退避
				pInfo.UnschedulableCount++
				q.unschedulableEntities.addOrUpdate(pInfo, false, framework.EventUnscheduledPodAdd.Label(), nil)
				updatedPod := medPriorityPodInfo.Pod.DeepCopy()
				updatedPod.Annotations["foo"] = "test"
				return medPriorityPodInfo.Pod, updatedPod
			},
		},
		{
			name:  "when updating a pod in unschedulableEntities, if its backoff timer has expired, it moves to activeQ",
			wantQ: activeQ,
			prepareFunc: func(tCtx ktesting.TContext, q *PriorityQueue) (oldPod, newPod *v1.Pod) {
				pInfo := q.newQueuedPodInfo(tCtx, medPriorityPodInfo.Pod, queuePlugin)
				// 需要递增才能使 Pod 进入退避
				pInfo.UnschedulableCount++
				q.unschedulableEntities.addOrUpdate(pInfo, false, framework.EventUnscheduledPodAdd.Label(), nil)
				updatedPod := medPriorityPodInfo.Pod.DeepCopy()
				updatedPod.Annotations["foo"] = "test1"
				// 把时钟推进 podMaxBackoffDuration，使 unschedulableEntities 中的 Pod 完成退避，
				// Pod 会被移入 activeQ。
				c.Step(q.backoffQ.podMaxBackoffDuration())
				return medPriorityPodInfo.Pod, updatedPod
			},
		},
		{
			name:  "when updating a pod in unschedulableEntities, if the scheduling hint returns QueueSkip, it remains in unschedulableEntities",
			wantQ: unschedulableQ,
			prepareFunc: func(tCtx ktesting.TContext, q *PriorityQueue) (oldPod, newPod *v1.Pod) {
				q.unschedulableEntities.addOrUpdate(q.newQueuedPodInfo(tCtx, medPriorityPodInfo.Pod, skipPlugin), false, framework.EventUnscheduledPodAdd.Label(), nil)
				updatedPod := medPriorityPodInfo.Pod.DeepCopy()
				updatedPod.Annotations["foo"] = "test1"
				return medPriorityPodInfo.Pod, updatedPod
			},
		},
		{
			name:  "when updating a pod which is in flightPods, the pod will not be added to any queue",
			wantQ: notInAnyQueue,
			prepareFunc: func(tCtx ktesting.TContext, q *PriorityQueue) (oldPod, newPod *v1.Pod) {
				// 需要先把该 Pod 加入 activeQ 并 Pop 一次，才能正确注册到 inFlightPods。
				q.Add(tCtx, medPriorityPodInfo.Pod)
				if p, err := q.Pop(klog.FromContext(tCtx)); err != nil {
					t.Errorf("Pop failed: %v", err)
				} else if diff := cmp.Diff(medPriorityPodInfo.Pod, p.(*framework.QueuedPodInfo).Pod); diff != "" {
					t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
				}
				updatedPod := medPriorityPodInfo.Pod.DeepCopy()
				updatedPod.Annotations["foo"] = "bar"
				return medPriorityPodInfo.Pod, updatedPod
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			objs := []runtime.Object{highPriorityPodInfo.Pod, unschedulablePodInfo.Pod, medPriorityPodInfo.Pod}
			plugin := &denyingPreEnqueuePlugin{denylists: []string{"deny"}}
			m := map[string]map[string]fwk.PreEnqueuePlugin{
				"": {
					"denyingPreEnqueuePlugin": plugin,
				},
			}
			q := NewTestQueueWithObjects(tCtx, newDefaultQueueSort(), objs, WithClock(c), WithQueueingHintMapPerProfile(queueingHintMap), WithPreEnqueuePluginMap(m))

			oldPod, newPod := tt.prepareFunc(tCtx, q)

			q.Update(tCtx, oldPod, newPod)

			var pInfo *framework.QueuedPodInfo

			// 校验期望队列
			if pInfoFromBackoff, exists := q.backoffQ.get(newQueuedPodInfoForLookup(newPod)); exists {
				if tt.wantQ != backoffQ {
					t.Errorf("expected pod %s not to be queued to backoffQ, but it was", newPod.Name)
				}
				pInfo = pInfoFromBackoff.(*framework.QueuedPodInfo)
			}

			if pInfoFromActive, exists := q.activeQ.get(newQueuedPodInfoForLookup(newPod)); exists {
				if tt.wantQ != activeQ {
					t.Errorf("expected pod %s not to be queued to activeQ, but it was", newPod.Name)
				}
				pInfo = pInfoFromActive.(*framework.QueuedPodInfo)
			}

			if pInfoFromUnsched := q.unschedulableEntities.get(newQueuedPodInfoForLookup(newPod)); pInfoFromUnsched != nil {
				if tt.wantQ != unschedulableQ {
					t.Errorf("expected pod %s to not be queued to unschedulableEntities, but it was", newPod.Name)
				}
				pInfo = pInfoFromUnsched.(*framework.QueuedPodInfo)
			}

			if tt.wantQ == notInAnyQueue {
				// 如果预期 Pod 不在任何队列中，跳过后续测试。
				return
			}

			if diff := cmp.Diff(newPod, pInfo.PodInfo.Pod); diff != "" {
				t.Errorf("Unexpected updated pod diff (-want, +got): %s", diff)
			}

			if tt.wantAddedToNominated && len(q.nominator.nominatedPods) != 1 {
				t.Errorf("Expected one item in nominatedPods map: %v", q.nominator.nominatedPods)
			}

		})
	}
}

// TestPriorityQueue_UpdateWhenInflight 确保 Pod 在调度期间
// 如果它在调度期间确实收到了可能使其可调度的更新。
// 更多背景见 https://github.com/kubernetes/kubernetes/pull/125578#discussion_r1648338033。
func TestPriorityQueue_UpdateWhenInflight(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	m := makeEmptyQueueingHintMapPerProfile()
	// fakePlugin 会随 Pod 的任意更新改变调度结果。
	m[""][framework.EventTargetPodUpdate] = []*QueueingHintFunction{
		{
			PluginName:     "fakePlugin",
			QueueingHintFn: queueHintReturnQueue,
		},
	}
	c := testingclock.NewFakeClock(time.Now())
	q := NewTestQueue(ctx, newDefaultQueueSort(), WithQueueingHintMapPerProfile(m), WithClock(c))

	// test-pod 被创建并从队列中弹出
	testPod := st.MakePod().Name("test-pod").Namespace("test-ns").UID("test-uid").Obj()
	q.Add(ctx, testPod)
	if p, err := q.Pop(logger); err != nil {
		t.Errorf("Pop failed: %v", err)
	} else if diff := cmp.Diff(testPod, p.(*framework.QueuedPodInfo).Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}

	// testPod 在调度期间被更新。
	updatedPod := testPod.DeepCopy()
	updatedPod.Spec.Tolerations = []v1.Toleration{
		{
			Key:    "foo",
			Effect: v1.TaintEffectNoSchedule,
		},
	}

	q.Update(ctx, testPod, updatedPod)
	// test-pod 被 fakePlugin 拒绝，
	// 但刚收到的更新事件可能改变这一调度结果，
	// 因此应把这个 Pod 放回 activeQ/backoffQ。
	err := q.AddUnschedulablePodIfNotPresent(logger, newQueuedPodInfoForLookup(updatedPod, "fakePlugin"), q.SchedulingCycle())
	if err != nil {
		t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
	}

	item, exists := q.backoffQ.get(newQueuedPodInfoForLookup(updatedPod))
	if !exists {
		t.Fatalf("expected pod %s to be queued to backoffQ, but it wasn't.", updatedPod.Name)
	}
	pInfo := item.(*framework.QueuedPodInfo)
	if diff := cmp.Diff(updatedPod, pInfo.PodInfo.Pod); diff != "" {
		t.Errorf("Unexpected updated pod diff (-want, +got): %s", diff)
	}
}

// TestPriorityQueue_Delete 验证 Delete 对队列中及 nominated Pod 的处理。
func TestPriorityQueue_Delete(t *testing.T) {
	metrics.Register()
	timestamp := time.Now()

	pod1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").Obj()
	pInfo1 := &framework.QueuedPodInfo{
		PodInfo: mustNewTestPodInfo(t, pod1),
		QueueingParams: framework.QueueingParams{
			Timestamp:            timestamp,
			UnschedulablePlugins: sets.New("fakePlugin"),
			PendingPlugins:       sets.New("fakePendingPlugin"),
		},
	}
	pod2 := st.MakePod().Name("pod2").Namespace("ns1").UID("pod2").Obj()
	pInfo2 := &framework.QueuedPodInfo{
		PodInfo: mustNewTestPodInfo(t, pod2),
		QueueingParams: framework.QueueingParams{
			Timestamp:            timestamp,
			UnschedulablePlugins: sets.New("fakePlugin2"),
		},
	}

	tests := []struct {
		name                string
		operations          []operation
		operands            []*framework.QueuedPodInfo
		podToDelete         *v1.Pod
		expectedAbsentPods  []*v1.Pod
		expectedPresentPods []*v1.Pod
		expectedMetrics     map[string]int
	}{
		{
			name: "Delete pod from activeQ",
			operations: []operation{
				add,
				add,
			},
			operands:            []*framework.QueuedPodInfo{pInfo1, pInfo2},
			podToDelete:         pod1,
			expectedAbsentPods:  []*v1.Pod{pod1},
			expectedPresentPods: []*v1.Pod{pod2},
			expectedMetrics: map[string]int{
				"fakePlugin":        0,
				"fakePendingPlugin": 0,
				"fakePlugin2":       0,
			},
		},
		{
			name: "Delete pod from backoffQ",
			operations: []operation{
				popAndRequeueAsBackoff,
				add,
			},
			operands:            []*framework.QueuedPodInfo{pInfo1, pInfo2},
			podToDelete:         pod1,
			expectedAbsentPods:  []*v1.Pod{pod1},
			expectedPresentPods: []*v1.Pod{pod2},
			expectedMetrics: map[string]int{
				"fakePlugin":        0,
				"fakePendingPlugin": 0,
				"fakePlugin2":       0,
			},
		},
		{
			name: "Delete pod from unschedulablePods",
			operations: []operation{
				popAndRequeueAsUnschedulable,
				popAndRequeueAsUnschedulable,
			},
			operands:            []*framework.QueuedPodInfo{pInfo1, pInfo2},
			podToDelete:         pod1,
			expectedAbsentPods:  []*v1.Pod{pod1},
			expectedPresentPods: []*v1.Pod{pod2},
			expectedMetrics: map[string]int{
				"fakePlugin":        0,
				"fakePendingPlugin": 0,
				"fakePlugin2":       1,
			},
		},
		{
			name: "Delete nominated pod from activeQ",
			operations: []operation{
				add,
			},
			operands:           []*framework.QueuedPodInfo{{PodInfo: highPriNominatedPodInfo}},
			podToDelete:        highPriNominatedPodInfo.Pod,
			expectedAbsentPods: []*v1.Pod{highPriNominatedPodInfo.Pod},
			expectedMetrics: map[string]int{
				"fakePlugin":        0,
				"fakePendingPlugin": 0,
				"fakePlugin2":       0,
			},
		},
		{
			name: "Delete non-existing pod",
			operations: []operation{
				popAndRequeueAsUnschedulable,
			},
			operands:            []*framework.QueuedPodInfo{pInfo1},
			podToDelete:         pod2,
			expectedAbsentPods:  []*v1.Pod{pod2},
			expectedPresentPods: []*v1.Pod{pod1},
			expectedMetrics: map[string]int{
				"fakePlugin":        1,
				"fakePendingPlugin": 1,
				"fakePlugin2":       0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			q := NewTestQueue(tCtx, newDefaultQueueSort(), WithClock(testingclock.NewFakeClock(timestamp)))

			// 重置测试中用到的插件指标
			allPlugins := sets.New("fakePlugin", "fakePendingPlugin", "fakePlugin2")
			for plugin := range allPlugins {
				metrics.UnschedulableReason(plugin, pod1.Spec.SchedulerName).Set(0)
			}

			// 执行操作
			for i, op := range tt.operations {
				op(tCtx, q, tt.operands[i])
			}

			q.Delete(tCtx.Logger(), tt.podToDelete)

			// 验证
			for _, pod := range tt.expectedAbsentPods {
				pInfoLookup := newQueuedPodInfoForLookup(pod)
				if q.activeQ.has(pInfoLookup) || q.backoffQ.has(pInfoLookup) || q.unschedulableEntities.get(pInfoLookup) != nil {
					t.Errorf("Expected pod %v to be absent, but it is present", pod.Name)
				}
			}

			for _, pod := range tt.expectedPresentPods {
				pInfoLookup := newQueuedPodInfoForLookup(pod)
				if !q.activeQ.has(pInfoLookup) && !q.backoffQ.has(pInfoLookup) && q.unschedulableEntities.get(pInfoLookup) == nil {
					t.Errorf("Expected pod %v to be present, but it is absent", pod.Name)
				}
			}

			for plugin, expectedVal := range tt.expectedMetrics {
				val, _ := testutil.GetGaugeMetricValue(metrics.UnschedulableReason(plugin, tt.podToDelete.Spec.SchedulerName))
				if diff := cmp.Diff(float64(expectedVal), val); diff != "" {
					t.Errorf("Unexpected metric value for plugin %v after delete (-want, +got):\n%s", plugin, diff)
				}
			}

			if len(q.nominator.nominatedPods) != 0 || len(q.nominator.nominatedPodToNode) != 0 {
				t.Errorf("Expected nominatedPods and nominatedPodToNode to be empty, but got %v and %v", q.nominator.nominatedPods, q.nominator.nominatedPodToNode)
			}
		})
	}
}

// TestPriorityQueue_Activate 验证 Activate 把指定 Pod 从不可调度状态激活。
func TestPriorityQueue_Activate(t *testing.T) {
	metrics.Register()
	tests := []struct {
		name                            string
		qPodInfoInUnschedulableEntities []*framework.QueuedPodInfo
		qPodInfoInBackoffQ              []*framework.QueuedPodInfo
		qPodInActiveQ                   []*v1.Pod
		qPodInfoToActivate              *framework.QueuedPodInfo
		qPodInInFlightPod               *v1.Pod
		expectedInFlightEvent           *clusterEvent
		want                            []*framework.QueuedPodInfo
	}{
		{
			name:               "pod already in activeQ",
			qPodInActiveQ:      []*v1.Pod{highPriNominatedPodInfo.Pod},
			qPodInfoToActivate: &framework.QueuedPodInfo{PodInfo: highPriNominatedPodInfo},
			want:               []*framework.QueuedPodInfo{{PodInfo: highPriNominatedPodInfo}}, // 1 已活跃
		},
		{
			name:               "pod not in unschedulableEntities/backoffQ",
			qPodInfoToActivate: &framework.QueuedPodInfo{PodInfo: highPriNominatedPodInfo},
			want:               []*framework.QueuedPodInfo{},
		},
		{
			name:                  "pod not in unschedulableEntities/backoffQ but in-flight",
			qPodInfoToActivate:    &framework.QueuedPodInfo{PodInfo: highPriNominatedPodInfo},
			qPodInInFlightPod:     highPriNominatedPodInfo.Pod,
			expectedInFlightEvent: &clusterEvent{oldObj: (*v1.Pod)(nil), newObj: highPriNominatedPodInfo.Pod, event: framework.EventForceActivate},
			want:                  []*framework.QueuedPodInfo{},
		},
		{
			name:               "pod not in unschedulableEntities/backoffQ and not in-flight",
			qPodInfoToActivate: &framework.QueuedPodInfo{PodInfo: highPriNominatedPodInfo},
			qPodInInFlightPod:  medPriorityPodInfo.Pod, // 另一个 Pod 在途
			want:               []*framework.QueuedPodInfo{},
		},
		{
			name:                            "pod in unschedulableEntities",
			qPodInfoInUnschedulableEntities: []*framework.QueuedPodInfo{{PodInfo: highPriNominatedPodInfo}},
			qPodInfoToActivate:              &framework.QueuedPodInfo{PodInfo: highPriNominatedPodInfo},
			want:                            []*framework.QueuedPodInfo{{PodInfo: highPriNominatedPodInfo}},
		},
		{
			name:               "pod in backoffQ",
			qPodInfoInBackoffQ: []*framework.QueuedPodInfo{{PodInfo: highPriNominatedPodInfo}},
			qPodInfoToActivate: &framework.QueuedPodInfo{PodInfo: highPriNominatedPodInfo},
			want:               []*framework.QueuedPodInfo{{PodInfo: highPriNominatedPodInfo}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			var objs []runtime.Object
			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			q := NewTestQueueWithObjects(ctx, newDefaultQueueSort(), objs)

			if tt.qPodInInFlightPod != nil {
				// 先 Put 再 Pop，使 Pod 注册进 inFlightPods。
				q.activeQ.add(logger, newQueuedPodInfoForLookup(tt.qPodInInFlightPod), framework.EventUnscheduledPodAdd.Label(), nil)
				p, err := q.activeQ.pop(logger)
				if err != nil {
					t.Fatalf("Pop failed: %v", err)
				}
				if p.(*framework.QueuedPodInfo).Pod.Name != tt.qPodInInFlightPod.Name {
					t.Errorf("Unexpected popped pod: %v", p.(*framework.QueuedPodInfo).Pod.Name)
				}
				if len(q.activeQ.listInFlightEvents()) != 1 {
					t.Fatal("Expected the pod to be recorded in in-flight events, but it doesn't")
				}
			}

			// 按表格准备 activeQ/unschedulableEntities/backoffQ
			for _, qPod := range tt.qPodInActiveQ {
				q.Add(ctx, qPod)
			}

			for _, qPodInfo := range tt.qPodInfoInUnschedulableEntities {
				q.unschedulableEntities.addOrUpdate(qPodInfo, false, framework.EventUnscheduledPodAdd.Label(), nil)
			}

			for _, qPodInfo := range tt.qPodInfoInBackoffQ {
				q.backoffQ.add(logger, qPodInfo, framework.EventUnscheduledPodAdd.Label(), nil)
			}

			// 按表格激活指定 Pod
			q.Activate(logger, map[string]*v1.Pod{"test_pod": tt.qPodInfoToActivate.PodInfo.Pod})

			// 通过 activeQ 长度检查激活后的结果
			if wantLen := len(tt.want); q.activeQ.len() != wantLen {
				t.Fatalf("length compare: want %v, got %v", wantLen, q.activeQ.len())
			}

			if tt.expectedInFlightEvent != nil {
				if len(q.activeQ.listInFlightEvents()) != 2 {
					t.Fatalf("Expected two in-flight event to be recorded, but got %v events", len(q.activeQ.listInFlightEvents()))
				}
				found := false
				for _, e := range q.activeQ.listInFlightEvents() {
					event, ok := e.(*clusterEvent)
					if !ok {
						continue
					}

					if d := cmp.Diff(tt.expectedInFlightEvent, event, cmpopts.EquateComparable(clusterEvent{})); d != "" {
						t.Fatalf("Unexpected in-flight event (-want, +got):\n%s", d)
					}
					found = true
				}

				if !found {
					t.Fatalf("Expected in-flight event to be recorded, but it wasn't.")
				}
			}

			// 检查指定 Pod 是否在 activeQ 中
			for _, want := range tt.want {
				if !q.activeQ.has(newQueuedPodInfoForLookup(want.PodInfo.Pod)) {
					t.Errorf("podInfo not exist in activeQ: want %v", want.PodInfo.Pod.Name)
				}
			}
		})
	}
}

type preEnqueuePlugin struct {
	allowlists []string
	name       string
}

func (pl *preEnqueuePlugin) Name() string {
	if pl.name != "" {
		return pl.name
	}
	return "preEnqueuePlugin"
}

func (pl *preEnqueuePlugin) PreEnqueue(ctx context.Context, p *v1.Pod) *fwk.Status {
	for _, allowed := range pl.allowlists {
		for label := range p.Labels {
			if label == allowed {
				return nil
			}
		}
	}
	return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "pod label not in allowlists")
}

type denyingPreEnqueuePlugin struct {
	denylists []string
}

func (pl *denyingPreEnqueuePlugin) Name() string {
	return "denyingPreEnqueuePlugin"
}

func (pl *denyingPreEnqueuePlugin) PreEnqueue(ctx context.Context, p *v1.Pod) *fwk.Status {
	for _, denied := range pl.denylists {
		for label := range p.Labels {
			if label == denied {
				return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "pod label in denylists")
			}
		}
	}
	return nil
}

// TestPriorityQueue_moveToActiveQ 验证 moveToActiveQ 把实体移入 activeQ。
func TestPriorityQueue_moveToActiveQ(t *testing.T) {
	tests := []struct {
		name                   string
		plugins                []fwk.PreEnqueuePlugin
		pod                    *v1.Pod
		event                  string
		movesFromBackoffQ      bool
		popFromBackoffQEnabled []bool
		wantUnschedulablePods  int
		wantSuccess            bool
	}{
		{
			name:                  "no plugins registered",
			pod:                   st.MakePod().Name("p").Label("p", "").Obj(),
			event:                 framework.EventUnscheduledPodAdd.Label(),
			wantUnschedulablePods: 0,
			wantSuccess:           true,
		},
		{
			name:                  "preEnqueue plugin registered, pod name not in allowlists",
			plugins:               []fwk.PreEnqueuePlugin{&preEnqueuePlugin{}, &preEnqueuePlugin{}},
			pod:                   st.MakePod().Name("p").Label("p", "").Obj(),
			event:                 framework.EventUnscheduledPodAdd.Label(),
			wantUnschedulablePods: 1,
			wantSuccess:           false,
		},
		{
			name: "preEnqueue plugin registered, pod failed one preEnqueue plugin",
			plugins: []fwk.PreEnqueuePlugin{
				&preEnqueuePlugin{allowlists: []string{"foo", "bar"}},
				&preEnqueuePlugin{allowlists: []string{"foo"}},
			},
			pod:                   st.MakePod().Name("bar").Label("bar", "").Obj(),
			event:                 framework.EventUnscheduledPodAdd.Label(),
			wantUnschedulablePods: 1,
			wantSuccess:           false,
		},
		{
			name: "preEnqueue plugin registered, preEnqueue rejects the pod, even if it is after backoff",
			plugins: []fwk.PreEnqueuePlugin{
				&preEnqueuePlugin{allowlists: []string{"foo", "bar"}},
				&preEnqueuePlugin{allowlists: []string{"foo"}},
			},
			pod:                    st.MakePod().Name("bar").Label("bar", "").Obj(),
			event:                  framework.BackoffComplete,
			popFromBackoffQEnabled: []bool{false},
			wantUnschedulablePods:  1,
			wantSuccess:            false,
		},
		{
			// 启用 SchedulerPopFromBackoffQ 后，队列假定 Pod 已通过 PreEnqueue，
			// 也不会再次运行 PreEnqueue，总是把 Pod 放入 activeQ。
			name: "preEnqueue plugin registered, pod would fail one preEnqueue plugin, but it is moved from backoffQ after completing backoff, so preEnqueue is not executed",
			plugins: []fwk.PreEnqueuePlugin{
				&preEnqueuePlugin{allowlists: []string{"foo", "bar"}},
				&preEnqueuePlugin{allowlists: []string{"foo"}},
			},
			pod:                    st.MakePod().Name("bar").Label("bar", "").Obj(),
			event:                  framework.BackoffComplete,
			movesFromBackoffQ:      true,
			popFromBackoffQEnabled: []bool{true},
			wantUnschedulablePods:  0,
			wantSuccess:            true,
		},
		{
			name: "preEnqueue plugin registered, pod failed one preEnqueue plugin when activated from unschedulableEntities",
			plugins: []fwk.PreEnqueuePlugin{
				&preEnqueuePlugin{allowlists: []string{"foo", "bar"}},
				&preEnqueuePlugin{allowlists: []string{"foo"}},
			},
			pod:                    st.MakePod().Name("bar").Label("bar", "").Obj(),
			event:                  framework.ForceActivate,
			movesFromBackoffQ:      false,
			popFromBackoffQEnabled: []bool{true},
			wantUnschedulablePods:  1,
			wantSuccess:            false,
		},
		{
			name: "preEnqueue plugin registered, pod would fail one preEnqueue plugin, but was activated from backoffQ, so preEnqueue is not executed",
			plugins: []fwk.PreEnqueuePlugin{
				&preEnqueuePlugin{allowlists: []string{"foo", "bar"}},
				&preEnqueuePlugin{allowlists: []string{"foo"}},
			},
			pod:                    st.MakePod().Name("bar").Label("bar", "").Obj(),
			event:                  framework.ForceActivate,
			movesFromBackoffQ:      true,
			popFromBackoffQEnabled: []bool{true},
			wantUnschedulablePods:  0,
			wantSuccess:            true,
		},
		{
			name: "preEnqueue plugin registered, pod passed all preEnqueue plugins",
			plugins: []fwk.PreEnqueuePlugin{
				&preEnqueuePlugin{allowlists: []string{"foo", "bar"}},
				&preEnqueuePlugin{allowlists: []string{"bar"}},
			},
			pod:                   st.MakePod().Name("bar").Label("bar", "").Obj(),
			event:                 framework.EventUnscheduledPodAdd.Label(),
			wantUnschedulablePods: 0,
			wantSuccess:           true,
		},
	}

	for _, tt := range tests {
		if tt.popFromBackoffQEnabled == nil {
			tt.popFromBackoffQEnabled = []bool{true, false}
		}
		for _, popFromBackoffQEnabled := range tt.popFromBackoffQEnabled {
			t.Run(fmt.Sprintf("%s popFromBackoffQEnabled(%v)", tt.name, popFromBackoffQEnabled), func(t *testing.T) {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SchedulerPopFromBackoffQ, popFromBackoffQEnabled)
				logger, ctx := ktesting.NewTestContext(t)
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()

				m := map[string]map[string]fwk.PreEnqueuePlugin{"": make(map[string]fwk.PreEnqueuePlugin, len(tt.plugins))}
				for _, plugin := range tt.plugins {
					m[""][plugin.Name()] = plugin
				}
				q := NewTestQueueWithObjects(ctx, newDefaultQueueSort(), []runtime.Object{tt.pod}, WithPreEnqueuePluginMap(m),
					WithPodInitialBackoffDuration(time.Second*30), WithPodMaxBackoffDuration(time.Second*60))
				got := q.moveToActiveQ(logger, q.newQueuedPodInfo(ctx, tt.pod), tt.event, tt.movesFromBackoffQ, nil)
				if got != tt.wantSuccess {
					t.Errorf("Unexpected result: want %v, but got %v", tt.wantSuccess, got)
				}
				if tt.wantUnschedulablePods != len(q.unschedulableEntities.entityInfoMap) {
					t.Errorf("Unexpected unschedulableEntities: want %v, but got %v", tt.wantUnschedulablePods, len(q.unschedulableEntities.entityInfoMap))
				}

				// 模拟一次更新事件。
				clone := tt.pod.DeepCopy()
				metav1.SetMetaDataAnnotation(&clone.ObjectMeta, "foo", "")
				q.Update(ctx, tt.pod, clone)
				// 确认 Pod 仍在 unschedulableEntities 中。
				if tt.wantUnschedulablePods != len(q.unschedulableEntities.entityInfoMap) {
					t.Errorf("Unexpected unschedulableEntities: want %v, but got %v", tt.wantUnschedulablePods, len(q.unschedulableEntities.entityInfoMap))
				}
			})
		}
	}
}

// TestPriorityQueue_moveToBackoffQ 验证 moveToBackoffQ 把实体移入 backoffQ。
func TestPriorityQueue_moveToBackoffQ(t *testing.T) {
	tests := []struct {
		name                   string
		plugins                []fwk.PreEnqueuePlugin
		pod                    *v1.Pod
		popFromBackoffQEnabled []bool
		wantSuccess            bool
	}{
		{
			name:        "no plugins registered",
			pod:         st.MakePod().Name("p").Label("p", "").Obj(),
			wantSuccess: true,
		},
		{
			name:                   "preEnqueue plugin registered, pod name would not be in allowlists",
			plugins:                []fwk.PreEnqueuePlugin{&preEnqueuePlugin{}, &preEnqueuePlugin{}},
			pod:                    st.MakePod().Name("p").Label("p", "").Obj(),
			popFromBackoffQEnabled: []bool{false},
			wantSuccess:            true,
		},
		{
			name:                   "preEnqueue plugin registered, pod name not in allowlists",
			plugins:                []fwk.PreEnqueuePlugin{&preEnqueuePlugin{}, &preEnqueuePlugin{}},
			pod:                    st.MakePod().Name("p").Label("p", "").Obj(),
			popFromBackoffQEnabled: []bool{true},
			wantSuccess:            false,
		},
		{
			name: "preEnqueue plugin registered, preEnqueue plugin would reject the pod, but isn't run",
			plugins: []fwk.PreEnqueuePlugin{
				&preEnqueuePlugin{allowlists: []string{"foo", "bar"}},
				&preEnqueuePlugin{allowlists: []string{"foo"}},
			},
			pod:                    st.MakePod().Name("bar").Label("bar", "").Obj(),
			popFromBackoffQEnabled: []bool{false},
			wantSuccess:            true,
		},
		{
			name: "preEnqueue plugin registered, pod failed one preEnqueue plugin",
			plugins: []fwk.PreEnqueuePlugin{
				&preEnqueuePlugin{allowlists: []string{"foo", "bar"}},
				&preEnqueuePlugin{allowlists: []string{"foo"}},
			},
			pod:                    st.MakePod().Name("bar").Label("bar", "").Obj(),
			popFromBackoffQEnabled: []bool{true},
			wantSuccess:            false,
		},
		{
			name: "preEnqueue plugin registered, pod passed all preEnqueue plugins",
			plugins: []fwk.PreEnqueuePlugin{
				&preEnqueuePlugin{allowlists: []string{"foo", "bar"}},
				&preEnqueuePlugin{allowlists: []string{"bar"}},
			},
			pod:         st.MakePod().Name("bar").Label("bar", "").Obj(),
			wantSuccess: true,
		},
	}

	for _, tt := range tests {
		if tt.popFromBackoffQEnabled == nil {
			tt.popFromBackoffQEnabled = []bool{true, false}
		}
		for _, popFromBackoffQEnabled := range tt.popFromBackoffQEnabled {
			t.Run(fmt.Sprintf("%s popFromBackoffQEnabled(%v)", tt.name, popFromBackoffQEnabled), func(t *testing.T) {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SchedulerPopFromBackoffQ, popFromBackoffQEnabled)
				logger, ctx := ktesting.NewTestContext(t)
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()

				m := map[string]map[string]fwk.PreEnqueuePlugin{"": make(map[string]fwk.PreEnqueuePlugin, len(tt.plugins))}
				for _, plugin := range tt.plugins {
					m[""][plugin.Name()] = plugin
				}
				q := NewTestQueueWithObjects(ctx, newDefaultQueueSort(), []runtime.Object{tt.pod}, WithPreEnqueuePluginMap(m),
					WithPodInitialBackoffDuration(time.Second*30), WithPodMaxBackoffDuration(time.Second*60))
				pInfo := q.newQueuedPodInfo(ctx, tt.pod)
				got := q.moveToBackoffQ(logger, pInfo, framework.EventUnscheduledPodAdd.Label(), nil)
				if got != tt.wantSuccess {
					t.Errorf("Unexpected result: want %v, but got %v", tt.wantSuccess, got)
				}
				if tt.wantSuccess {
					if !q.backoffQ.has(pInfo) {
						t.Errorf("Expected pod to be in backoffQ, but it isn't")
					}
					if q.unschedulableEntities.get(pInfo) != nil {
						t.Errorf("Expected pod not to be in unschedulableEntities, but it is")
					}
				} else {
					if q.backoffQ.has(pInfo) {
						t.Errorf("Expected pod not to be in backoffQ, but it is")
					}
					if q.unschedulableEntities.get(pInfo) == nil {
						t.Errorf("Expected pod to be in unschedulableEntities, but it isn't")
					}
				}
			})
		}
	}
}

// BenchmarkMoveAllToActiveOrBackoffQueue 基准测试 MoveAllToActiveOrBackoffQueue 在不同插件规模下的性能。
func BenchmarkMoveAllToActiveOrBackoffQueue(b *testing.B) {
	tests := []struct {
		name      string
		moveEvent fwk.ClusterEvent
	}{
		{
			name:      "baseline",
			moveEvent: framework.EventUnschedulableTimeout,
		},
		{
			name:      "worst",
			moveEvent: nodeAdd,
		},
		{
			name: "random",
			// 不指定 "moveEvent"
		},
	}

	podTemplates := []*v1.Pod{
		highPriorityPodInfo.Pod, highPriNominatedPodInfo.Pod,
		medPriorityPodInfo.Pod, unschedulablePodInfo.Pod,
	}

	events := []fwk.ClusterEvent{
		{Resource: fwk.Node, ActionType: fwk.Add},
		{Resource: fwk.Node, ActionType: fwk.UpdateNodeTaint},
		{Resource: fwk.Node, ActionType: fwk.UpdateNodeAllocatable},
		{Resource: fwk.Node, ActionType: fwk.UpdateNodeCondition},
		{Resource: fwk.Node, ActionType: fwk.UpdateNodeLabel},
		{Resource: fwk.Node, ActionType: fwk.UpdateNodeAnnotation},
		{Resource: fwk.PersistentVolumeClaim, ActionType: fwk.Add},
		{Resource: fwk.PersistentVolumeClaim, ActionType: fwk.Update},
		{Resource: fwk.PersistentVolume, ActionType: fwk.Add},
		{Resource: fwk.PersistentVolume, ActionType: fwk.Update},
		{Resource: fwk.StorageClass, ActionType: fwk.Add},
		{Resource: fwk.StorageClass, ActionType: fwk.Update},
		{Resource: fwk.CSINode, ActionType: fwk.Add},
		{Resource: fwk.CSINode, ActionType: fwk.Update},
		{Resource: fwk.CSIDriver, ActionType: fwk.Add},
		{Resource: fwk.CSIDriver, ActionType: fwk.Update},
		{Resource: fwk.CSIStorageCapacity, ActionType: fwk.Add},
		{Resource: fwk.CSIStorageCapacity, ActionType: fwk.Update},
	}

	pluginNum := 20
	var plugins []string
	// 模拟运行时加载了 20 个插件。
	for i := 0; i < pluginNum; i++ {
		plugins = append(plugins, fmt.Sprintf("fake-plugin-%v", i))
	}

	for _, tt := range tests {
		for _, podsInUnschedulablePods := range []int{1000, 5000} {
			b.Run(fmt.Sprintf("%v-%v", tt.name, podsInUnschedulablePods), func(b *testing.B) {
				logger, ctx := ktesting.NewTestContext(b)
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					c := testingclock.NewFakeClock(time.Now())

					m := makeEmptyQueueingHintMapPerProfile()
					// - events[0]（即 NodeAdd）注册全部插件。
					// - events[1] 注册一半插件
					// - events[2] 注册三分之一插件
					// - ...
					for j := 0; j < len(events); j++ {
						for k := 0; k < len(plugins); k++ {
							if (k+1)%(j+1) == 0 {
								m[""][events[j]] = append(m[""][events[j]], &QueueingHintFunction{
									PluginName:     plugins[k],
									QueueingHintFn: queueHintReturnQueue,
								})
							}
						}
					}

					ctx, cancel := context.WithCancel(ctx)
					defer cancel()
					q := NewTestQueue(ctx, newDefaultQueueSort(), WithClock(c), WithQueueingHintMapPerProfile(m))

					// 初始化 unschedulableEntities 中的 Pod。
					for j := 0; j < podsInUnschedulablePods; j++ {
						p := podTemplates[j%len(podTemplates)].DeepCopy()
						p.Name, p.UID = fmt.Sprintf("%v-%v", p.Name, j), types.UID(fmt.Sprintf("%v-%v", p.UID, j))
						var podInfo *framework.QueuedPodInfo
						// 构造各 PodInfo 的终极目标是覆盖以下交集路径：
						// （unschedulable）插件名与注册了 moveEvent 的插件的交集，
						// 理由如下：
						// - baseline 场景不注入 unschedulable 插件名，podMatchesEvent()
						//   永远不会执行。
						// - worst 场景让交集两端都很大，即，
						//   M 与 N 相交，而非 M 与 1（或 1 与 N）相交
						// - random 场景每个 Pod 由随机插件判失败，moveEvent 也
						//   是随机的。
						if tt.name == "baseline" {
							podInfo = q.newQueuedPodInfo(ctx, p)
						} else if tt.name == "worst" {
							// 每个 Pod 都被所有插件判失败。
							podInfo = q.newQueuedPodInfo(ctx, p, plugins...)
						} else {
							// 随机场景。
							podInfo = q.newQueuedPodInfo(ctx, p, plugins[j%len(plugins)])
						}
						err := q.AddUnschedulablePodIfNotPresent(logger, podInfo, q.SchedulingCycle())
						if err != nil {
							b.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
						}
					}

					b.StartTimer()
					if tt.moveEvent.Resource != "" {
						q.MoveAllToActiveOrBackoffQueue(logger, tt.moveEvent, nil, nil, nil)
					} else {
						// 随机场景。
						q.MoveAllToActiveOrBackoffQueue(logger, events[i%len(events)], nil, nil, nil)
					}
				}
			})
		}
	}
}

// TestPriorityQueue_MoveAllToActiveOrBackoffQueueWithQueueingHint 验证 MoveAllToActiveOrBackoffQueue 结合 QueueingHint 的实体筛选与状态转换。
func TestPriorityQueue_MoveAllToActiveOrBackoffQueueWithQueueingHint(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	p := st.MakePod().Name("pod1").Namespace("ns1").UID("1").Label("foo", "bar").Obj()
	gatedPod := st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj()
	tests := []struct {
		name    string
		podInfo *framework.QueuedPodInfo
		hint    fwk.QueueingHintFn
		// duration 是 Pod 在不可调度队列中停留的时长。
		duration time.Duration
		// triggerEvent 是触发移动的事件。未设置时默认为 nodeAdd。
		triggerEvent *fwk.ClusterEvent
		// expectedQ 是该 Pod 应进入的队列名（activeQ、backoffQ 或 unschedulableEntities）。
		expectedQ string
	}{
		{
			name: "Queue queues pod to activeQ",
			// 该 Pod 带有 PendingPlugins，因此会被直接推入 activeQ
			podInfo: &framework.QueuedPodInfo{
				PodInfo: mustNewPodInfo(p),
				QueueingParams: framework.QueueingParams{
					PendingPlugins: sets.New("foo"),
				},
			},
			hint:      queueHintReturnQueue,
			expectedQ: activeQ,
		},
		{
			name: "Queue queues pod to backoffQ if Pod is backing off",
			// 需要 UnschedulableCount 才会进入退避。
			podInfo: &framework.QueuedPodInfo{
				PodInfo: mustNewPodInfo(p),
				QueueingParams: framework.QueueingParams{
					UnschedulablePlugins: sets.New("foo"),
				},
			},
			hint:      queueHintReturnQueue,
			expectedQ: backoffQ,
		},
		{
			name: "Queue queues pod to activeQ if Pod is not backing off",
			podInfo: &framework.QueuedPodInfo{
				PodInfo: mustNewPodInfo(p),
				QueueingParams: framework.QueueingParams{
					UnschedulablePlugins: sets.New("foo"),
				},
			},
			hint: queueHintReturnQueue,
			// 假设 Pod 已调度失败一次，惩罚为 DefaultPodInitialBackoffDuration。
			// 要完成退避，仅等待 DefaultPodInitialBackoffDuration 不够，需再等 +1，
			// 因为当 `{退避到期时间} == trancate({当前时间})` 时，Pod 仍被视为处于退避中
			duration:  DefaultPodInitialBackoffDuration + time.Second,
			expectedQ: activeQ,
		},
		{
			name: "Skip queues pod to unschedulableEntities",
			podInfo: &framework.QueuedPodInfo{
				PodInfo: mustNewPodInfo(p),
				QueueingParams: framework.QueueingParams{
					UnschedulablePlugins: sets.New("foo"),
				},
			},
			hint:      queueHintReturnSkip,
			expectedQ: unschedulableQ,
		},
		{
			name: "Queue queues pod to backoffQ if Pod is not gated and the event is wildcard",
			podInfo: &framework.QueuedPodInfo{
				PodInfo: mustNewPodInfo(p),
				QueueingParams: framework.QueueingParams{
					UnschedulablePlugins: sets.New("foo"),
				},
			},
			triggerEvent: &framework.EventUnschedulableTimeout,
			hint: func(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
				return fwk.Queue, fmt.Errorf("QueueingHintFn should not be called as trigger event is wildcard")
			},
			expectedQ: backoffQ,
		},
		{
			name:         "Queue queues pod to backoffQ when Pod is no longer gated and the event is wildcard",
			podInfo:      setQueuedPodInfoGated(&framework.QueuedPodInfo{PodInfo: mustNewPodInfo(p)}, "foo", []fwk.ClusterEvent{framework.EventUnscheduledPodUpdate}),
			triggerEvent: &framework.EventUnschedulableTimeout,
			hint: func(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
				return fwk.Queue, fmt.Errorf("QueueingHintFn should not be called as trigger event is wildcard")
			},
			expectedQ: backoffQ,
		},
		{
			name:         "Queue queues pod to unschedulableQ when Pod is gated and the event is wildcard",
			podInfo:      setQueuedPodInfoGated(&framework.QueuedPodInfo{PodInfo: mustNewPodInfo(gatedPod)}, "foo", []fwk.ClusterEvent{framework.EventUnscheduledPodUpdate}),
			triggerEvent: &framework.EventUnschedulableTimeout,
			hint: func(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
				return fwk.Queue, fmt.Errorf("QueueingHintFn should not be called as trigger event is wildcard")
			},
			expectedQ: unschedulableQ,
		},
		{
			name:    "QueueHintFunction is not called when Pod is gated by the plugin that isn't interested in the event",
			podInfo: setQueuedPodInfoGated(&framework.QueuedPodInfo{PodInfo: mustNewPodInfo(p)}, names.SchedulingGates, []fwk.ClusterEvent{framework.EventUnscheduledPodUpdate}),
			// Pod 被 SchedulingGates 插件加了 gate，不应调用 hintFn，
			// 调度 gate 不关注 node add 事件，
			// 队列应把这个 Pod 留在 unschedQ，不调用 hintFn。
			hint: func(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
				return fwk.Queue, fmt.Errorf("QueueingHintFn should not be called as pod is gated")
			},
			expectedQ: unschedulableQ,
		},
		{
			name:    "QueueHintFunction is called when Pod is gated by the plugin that is interested in the event",
			podInfo: setQueuedPodInfoGated(&framework.QueuedPodInfo{PodInfo: mustNewPodInfo(p)}, "foo", []fwk.ClusterEvent{nodeAdd}),
			// 此时应调用 hintFn，因为 Pod 被关注 NodeAdd 事件的 foo 插件加了 gate。
			hint: queueHintReturnQueue,
			// 因此，该 Pod 应被排入 backoffQ。
			expectedQ: backoffQ,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger, ctx := ktesting.NewTestContext(t)
			m := makeEmptyQueueingHintMapPerProfile()
			m[""][nodeAdd] = []*QueueingHintFunction{
				{
					PluginName:     "foo",
					QueueingHintFn: test.hint,
				},
			}
			m[""][framework.EventUnscheduledPodUpdate] = []*QueueingHintFunction{
				{
					PluginName:     names.SchedulingGates,
					QueueingHintFn: queueHintReturnQueue,
				},
			}
			cl := testingclock.NewFakeClock(now)
			plugin, _ := schedulinggates.New(ctx, nil, nil, plfeature.Features{})
			preEnqM := map[string]map[string]fwk.PreEnqueuePlugin{"": {
				names.SchedulingGates: plugin.(fwk.PreEnqueuePlugin),
				"foo":                 &preEnqueuePlugin{allowlists: []string{"foo"}},
			}}
			q := NewTestQueue(ctx, newDefaultQueueSort(), WithQueueingHintMapPerProfile(m), WithClock(cl), WithPreEnqueuePluginMap(preEnqM))
			q.Add(ctx, test.podInfo.Pod)
			if q.activeQ.len() > 0 {
				if p, err := q.Pop(logger); err != nil {
					t.Errorf("Pop failed: %v", err)
				} else if diff := cmp.Diff(test.podInfo.Pod, p.(*framework.QueuedPodInfo).Pod); diff != "" {
					t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
				}
				// 加入不可调度 Pod 池
				err := q.AddUnschedulablePodIfNotPresent(logger, test.podInfo, q.SchedulingCycle())
				if err != nil {
					t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
				}
			} else {
				// Pod 因带 gate 已被移入 unschedulableEntities。
				// 用测试配置的 podInfo 更新它，确保自定义字段生效。
				q.unschedulableEntities.addOrUpdate(test.podInfo, test.podInfo.Gated(), "test-setup", nil)
			}
			cl.Step(test.duration)

			event := nodeAdd
			if test.triggerEvent != nil {
				event = *test.triggerEvent
			}
			q.MoveAllToActiveOrBackoffQueue(logger, event, nil, nil, nil)

			if q.backoffQ.len() == 0 && test.expectedQ == backoffQ {
				t.Fatalf("expected pod to be queued to backoffQ, but it was not")
			}

			if q.activeQ.len() == 0 && test.expectedQ == activeQ {
				t.Fatalf("expected pod to be queued to activeQ, but it was not")
			}

			if q.unschedulableEntities.get(test.podInfo) == nil && test.expectedQ == unschedulableQ {
				t.Fatalf("expected pod to be queued to unschedulableEntities, but it was not")
			}
		})
	}
}

// TestPriorityQueue_MoveAllToActiveOrBackoffQueue 验证 MoveAllToActiveOrBackoffQueue 在不同队列状态下的行为。
func TestPriorityQueue_MoveAllToActiveOrBackoffQueue(t *testing.T) {
	c := testingclock.NewFakeClock(time.Now())
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	m := makeEmptyQueueingHintMapPerProfile()

	m[""][nodeAdd] = []*QueueingHintFunction{
		{
			PluginName:     "fooPlugin",
			QueueingHintFn: queueHintReturnQueue,
		},
	}
	q := NewTestQueue(ctx, newDefaultQueueSort(), WithClock(c), WithQueueingHintMapPerProfile(m))
	// 为模拟真实世界中 Pod 调度失败，在下面的 AddUnschedulablePodIfNotPresent 之前先从 activeQ 弹出该 Pod。
	q.Add(ctx, unschedulablePodInfo.Pod)
	if p, err := q.Pop(logger); err != nil {
		t.Errorf("Pop failed: %v", err)
	} else if diff := cmp.Diff(unschedulablePodInfo.Pod, p.(*framework.QueuedPodInfo).Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}
	expectInFlightPods(t, q, unschedulablePodInfo.Pod.UID)
	q.Add(ctx, highPriorityPodInfo.Pod)
	if p, err := q.Pop(logger); err != nil {
		t.Errorf("Pop failed: %v", err)
	} else if diff := cmp.Diff(highPriorityPodInfo.Pod, p.(*framework.QueuedPodInfo).Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}
	expectInFlightPods(t, q, unschedulablePodInfo.Pod.UID, highPriorityPodInfo.Pod.UID)
	err := q.AddUnschedulablePodIfNotPresent(logger, q.newQueuedPodInfo(ctx, unschedulablePodInfo.Pod, "fooPlugin"), q.SchedulingCycle())
	if err != nil {
		t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
	}
	err = q.AddUnschedulablePodIfNotPresent(logger, q.newQueuedPodInfo(ctx, highPriorityPodInfo.Pod, "fooPlugin"), q.SchedulingCycle())
	if err != nil {
		t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
	}
	expectInFlightPods(t, q)
	// 构造一个 Pod，但不把调度失败关联到任何插件
	hpp1 := clonePod(highPriorityPodInfo.Pod, "hpp1")
	q.Add(ctx, hpp1)
	if p, err := q.Pop(logger); err != nil {
		t.Errorf("Pop failed: %v", err)
	} else if diff := cmp.Diff(hpp1, p.(*framework.QueuedPodInfo).Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}
	expectInFlightPods(t, q, hpp1.UID)
	// 没有关联失败插件，该 Pod 会进入 backoffQ。
	hpp1PodInfo := q.newQueuedPodInfo(ctx, hpp1)
	hpp1PodInfo.UnschedulableCount++
	err = q.AddUnschedulablePodIfNotPresent(logger, hpp1PodInfo, q.SchedulingCycle())
	if err != nil {
		t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
	}
	expectInFlightPods(t, q)
	// 构造另一个 Pod，把调度失败关联到插件 "barPlugin"。
	hpp2 := clonePod(highPriorityPodInfo.Pod, "hpp2")
	q.Add(ctx, hpp2)
	if p, err := q.Pop(logger); err != nil {
		t.Errorf("Pop failed: %v", err)
	} else if diff := cmp.Diff(hpp2, p.(*framework.QueuedPodInfo).Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}
	expectInFlightPods(t, q, hpp2.UID)
	// 该 Pod 会进入不可调度 Pod 池。
	err = q.AddUnschedulablePodIfNotPresent(logger, q.newQueuedPodInfo(ctx, hpp2, "barPlugin"), q.SchedulingCycle())
	if err != nil {
		t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
	}
	expectInFlightPods(t, q)
	// 这个 NodeAdd 事件把 unschedulablePodInfo 和 highPriorityPodInfo 移到 backoffQ，
	// 因为为 NodeAdd/fooPlugin 注册了 queueing hint 函数。
	q.MoveAllToActiveOrBackoffQueue(logger, nodeAdd, nil, nil, nil)
	q.Add(ctx, medPriorityPodInfo.Pod)
	if q.activeQ.len() != 1 {
		t.Errorf("Expected 1 item to be in activeQ, but got: %v", q.activeQ.len())
	}
	// 从 activeQ 中弹出 medPriorityPodInfo。
	if p, err := q.Pop(logger); err != nil {
		t.Errorf("Pop failed: %v", err)
	} else if diff := cmp.Diff(medPriorityPodInfo.Pod, p.(*framework.QueuedPodInfo).Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}
	expectInFlightPods(t, q, medPriorityPodInfo.Pod.UID)
	// hpp2 不会被移动。
	if q.backoffQ.len() != 3 {
		t.Fatalf("Expected 3 items to be in backoffQ, but got: %v", q.backoffQ.len())
	}

	// 弹出 backoffQ 中的 Pod。
	// 这不会使它们成为 in-flight Pod。
	c.Step(q.backoffQ.podMaxBackoffDuration())
	_ = q.backoffQ.popAllBackoffCompleted(logger)
	expectInFlightPods(t, q, medPriorityPodInfo.Pod.UID)

	q.Add(ctx, unschedulablePodInfo.Pod)
	if p, err := q.Pop(logger); err != nil {
		t.Errorf("Pop failed: %v", err)
	} else if diff := cmp.Diff(unschedulablePodInfo.Pod, p.(*framework.QueuedPodInfo).Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}
	expectInFlightPods(t, q, medPriorityPodInfo.Pod.UID, unschedulablePodInfo.Pod.UID)
	q.Add(ctx, highPriorityPodInfo.Pod)
	if p, err := q.Pop(logger); err != nil {
		t.Errorf("Pop failed: %v", err)
	} else if diff := cmp.Diff(highPriorityPodInfo.Pod, p.(*framework.QueuedPodInfo).Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}
	expectInFlightPods(t, q, medPriorityPodInfo.Pod.UID, unschedulablePodInfo.Pod.UID, highPriorityPodInfo.Pod.UID)
	q.Add(ctx, hpp1)
	if p, err := q.Pop(logger); err != nil {
		t.Errorf("Pop failed: %v", err)
	} else if diff := cmp.Diff(hpp1, p.(*framework.QueuedPodInfo).Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}
	unschedulableQueuedPodInfo := q.newQueuedPodInfo(ctx, unschedulablePodInfo.Pod, "fooPlugin")
	highPriorityQueuedPodInfo := q.newQueuedPodInfo(ctx, highPriorityPodInfo.Pod, "fooPlugin")
	hpp1QueuedPodInfo := q.newQueuedPodInfo(ctx, hpp1)
	expectInFlightPods(t, q, medPriorityPodInfo.Pod.UID, unschedulablePodInfo.Pod.UID, highPriorityPodInfo.Pod.UID, hpp1.UID)
	err = q.AddUnschedulablePodIfNotPresent(logger, unschedulableQueuedPodInfo, q.SchedulingCycle())
	if err != nil {
		t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
	}
	expectInFlightPods(t, q, medPriorityPodInfo.Pod.UID, highPriorityPodInfo.Pod.UID, hpp1.UID)
	err = q.AddUnschedulablePodIfNotPresent(logger, highPriorityQueuedPodInfo, q.SchedulingCycle())
	if err != nil {
		t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
	}
	expectInFlightPods(t, q, medPriorityPodInfo.Pod.UID, hpp1.UID)
	err = q.AddUnschedulablePodIfNotPresent(logger, hpp1QueuedPodInfo, q.SchedulingCycle())
	if err != nil {
		t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
	}
	expectInFlightPods(t, q, medPriorityPodInfo.Pod.UID)
	q.Add(ctx, medPriorityPodInfo.Pod)
	// hpp1 没有关联失败插件，会进入 backoffQ。
	// 除 hpp1 外的所有插件都被记入不可调度 Pod 池。
	for _, pod := range []*v1.Pod{unschedulablePodInfo.Pod, highPriorityPodInfo.Pod, hpp2} {
		if q.unschedulableEntities.get(newQueuedPodInfoForLookup(pod)) == nil {
			t.Errorf("Expected %v in the unschedulableEntities", pod.Name)
		}
	}
	if !q.backoffQ.has(hpp1QueuedPodInfo) {
		t.Errorf("Expected %v in the backoffQ", hpp1.Name)
	}
	// 其余所有 Pod 都应位于 activeQ。
	if q.activeQ.len() != 1 {
		t.Errorf("Expected %v in the activeQ", medPriorityPodInfo.Pod.Name)
	}

	// 把时钟推进 podMaxBackoffDuration，使 unschedulableEntities 中的 Pod 完成退避，
	// Pod 会被移入 activeQ。
	c.Step(q.backoffQ.podMaxBackoffDuration())
	q.flushBackoffQCompleted(logger) // 冲刷已完成的 backoffQ，使 hpp1 移入 activeQ。
	q.MoveAllToActiveOrBackoffQueue(logger, nodeAdd, nil, nil, nil)
	if q.activeQ.len() != 4 {
		t.Errorf("Expected 4 items to be in activeQ, but got: %v", q.activeQ.len())
	}
	if q.backoffQ.len() != 0 {
		t.Errorf("Expected 0 item to be in backoffQ, but got: %v", q.backoffQ.len())
	}
	expectInFlightPods(t, q, medPriorityPodInfo.Pod.UID)
	if len(q.unschedulableEntities.entityInfoMap) != 1 {
		// 无论退避计时器如何，hpp2 都不会被移动。
		t.Errorf("Expected 1 item to be in unschedulableEntities, but got: %v", len(q.unschedulableEntities.entityInfoMap))
	}
}

func clonePod(pod *v1.Pod, newName string) *v1.Pod {
	pod = pod.DeepCopy()
	pod.Name = newName
	pod.UID = types.UID(pod.Name + pod.Namespace)
	return pod
}

func expectInFlightPods(t *testing.T, q *PriorityQueue, uids ...types.UID) {
	t.Helper()
	var actualUIDs []types.UID
	for _, pod := range q.activeQ.listInFlightPods() {
		actualUIDs = append(actualUIDs, pod.UID)
	}
	sortUIDs := cmpopts.SortSlices(func(a, b types.UID) bool { return a < b })
	if diff := cmp.Diff(uids, actualUIDs, sortUIDs); diff != "" {
		t.Fatalf("Unexpected content of inFlightPods (-want, +have):\n%s", diff)
	}
	actualUIDs = nil
	events := q.activeQ.listInFlightEvents()
	for _, e := range events {
		if pod, ok := e.(*v1.Pod); ok {
			actualUIDs = append(actualUIDs, pod.UID)
		}
	}
	if diff := cmp.Diff(uids, actualUIDs, sortUIDs); diff != "" {
		t.Fatalf("Unexpected pods in inFlightEvents (-want, +have):\n%s", diff)
	}
}

// TestPriorityQueue_NominatedPodsForNode 验证 NominatedPodsForNode 返回节点上的 nominated Pod。
func TestPriorityQueue_NominatedPodsForNode(t *testing.T) {
	objs := []runtime.Object{medPriorityPodInfo.Pod, unschedulablePodInfo.Pod, highPriorityPodInfo.Pod}
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	q := NewTestQueueWithObjects(ctx, newDefaultQueueSort(), objs)
	q.Add(ctx, medPriorityPodInfo.Pod)
	q.Add(ctx, unschedulablePodInfo.Pod)
	q.Add(ctx, highPriorityPodInfo.Pod)
	if p, err := q.Pop(logger); err != nil {
		t.Errorf("Pop failed: %v", err)
	} else if diff := cmp.Diff(highPriorityPodInfo.Pod, p.(*framework.QueuedPodInfo).Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}
	expectedList := []fwk.PodInfo{medPriorityPodInfo, unschedulablePodInfo}
	podInfos := q.NominatedPodsForNode("node1")
	if diff := cmp.Diff(expectedList, podInfos, cmpopts.IgnoreUnexported(framework.PodInfo{})); diff != "" {
		t.Errorf("Unexpected list of nominated Pods for node: (-want, +got):\n%s", diff)
	}
	podInfos[0].GetPod().Name = "not mpp"
	if diff := cmp.Diff(podInfos, q.NominatedPodsForNode("node1"), cmpopts.IgnoreUnexported(framework.PodInfo{})); diff == "" {
		t.Error("Expected list of nominated Pods for node2 is different from podInfos")
	}
	if len(q.NominatedPodsForNode("node2")) != 0 {
		t.Error("Expected list of nominated Pods for node2 to be empty.")
	}
}

// TestPriorityQueue_NominatedPodDeleted 验证 nominated Pod 被删除后的提名信息清理。
func TestPriorityQueue_NominatedPodDeleted(t *testing.T) {
	tests := []struct {
		name      string
		podInfo   *framework.PodInfo
		deletePod bool
		wantLen   int
	}{
		{
			name:    "alive pod gets added into PodNominator",
			podInfo: medPriorityPodInfo,
			wantLen: 1,
		},
		{
			name:      "deleted pod shouldn't be added into PodNominator",
			podInfo:   highPriNominatedPodInfo,
			deletePod: true,
			wantLen:   0,
		},
		{
			name:    "pod without .status.nominatedPodName specified shouldn't be added into PodNominator",
			podInfo: highPriorityPodInfo,
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, ctx := ktesting.NewTestContext(t)
			cs := fake.NewClientset(tt.podInfo.Pod)
			informerFactory := informers.NewSharedInformerFactory(cs, 0)
			podLister := informerFactory.Core().V1().Pods().Lister()

			// 构造一个 PriorityQueue。
			q := NewPriorityQueue(newDefaultQueueSort(), informerFactory, WithPodLister(podLister))
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			informerFactory.Start(ctx.Done())
			informerFactory.WaitForCacheSync(ctx.Done())

			if tt.deletePod {
				// 模拟测试 Pod 被物理删除。
				informerFactory.Core().V1().Pods().Informer().GetStore().Delete(tt.podInfo.Pod)
			}

			q.AddNominatedPod(logger, tt.podInfo, nil)

			if got := len(q.NominatedPodsForNode(tt.podInfo.Pod.Status.NominatedNodeName)); got != tt.wantLen {
				t.Errorf("Expected %v nominated pods for node, but got %v", tt.wantLen, got)
			}
		})
	}
}

// TestPriorityQueue_NominatedNodeNameEmptyNodeKey 确保空的 NominatedNodeName 不会
// 把 Pod 存到 nominatedPods[""] 下（#138267），包括 ModeNoop 行为。
func TestPriorityQueue_NominatedNodeNameEmptyNodeKey(t *testing.T) {
	tests := []struct {
		name                 string
		initialNominatedNode string // 若设置，则在测试调用前提名该 Pod 到此节点。
		podNominatedNode     string // pod.Status.NominatedNodeName
		nominatingMode       fwk.NominatingMode
		nominatingNodeName   string // nominatingInfo 中的 NominatedNodeName
		wantNominatedNode    string // 调用后的期望节点；为空表示不提名。
	}{
		{
			name:           "ModeOverride empty without prior nomination",
			nominatingMode: fwk.ModeOverride,
		},
		{
			name:                 "nominated then cleared with ModeOverride empty",
			initialNominatedNode: "node1",
			nominatingMode:       fwk.ModeOverride,
		},
		{
			name:           "ModeNoop empty without prior nomination",
			nominatingMode: fwk.ModeNoop,
		},
		{
			name:                 "nominated then cleared with ModeNoop empty",
			initialNominatedNode: "node1",
			nominatingMode:       fwk.ModeNoop,
		},
		{
			name:               "ModeNoop with non-empty nominatingInfo NNN ignored when pod NNN empty",
			nominatingMode:     fwk.ModeNoop,
			nominatingNodeName: "node1",
		},
		{
			name:               "ModeNoop with non-empty nominatingInfo NNN uses pod NNN",
			nominatingMode:     fwk.ModeNoop,
			podNominatedNode:   "node2",
			nominatingNodeName: "node1",
			wantNominatedNode:  "node2",
		},
		{
			name:              "ModeNoop empty nominatingInfo NNN with non-empty pod NNN",
			nominatingMode:    fwk.ModeNoop,
			podNominatedNode:  "node2",
			wantNominatedNode: "node2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			podBuilder := st.MakePod().Name("hpp").Namespace("ns1").UID("hppns1").Priority(highPriority)
			if tt.podNominatedNode != "" {
				podBuilder = podBuilder.NominatedNodeName(tt.podNominatedNode)
			}
			podInfo := mustNewTestPodInfo(t, podBuilder.Obj())

			logger, ctx := ktesting.NewTestContext(t)
			cs := fake.NewClientset(podInfo.Pod)
			informerFactory := informers.NewSharedInformerFactory(cs, 0)
			q := NewPriorityQueue(newDefaultQueueSort(), informerFactory, WithPodLister(informerFactory.Core().V1().Pods().Lister()))
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			informerFactory.Start(ctx.Done())
			informerFactory.WaitForCacheSync(ctx.Done())

			if tt.initialNominatedNode != "" {
				q.AddNominatedPod(logger, podInfo, &fwk.NominatingInfo{
					NominatingMode:    fwk.ModeOverride,
					NominatedNodeName: tt.initialNominatedNode,
				})
				if got := len(q.nominator.nominatedPods[tt.initialNominatedNode]); got != 1 {
					t.Fatalf("expected 1 nominated pod on %s before clear, got %d", tt.initialNominatedNode, got)
				}
			}

			q.AddNominatedPod(logger, podInfo, &fwk.NominatingInfo{
				NominatingMode:    tt.nominatingMode,
				NominatedNodeName: tt.nominatingNodeName,
			})

			if len(q.nominator.nominatedPods[""]) != 0 {
				t.Errorf("expected no pods under nominatedPods[\"\"], got %v", q.nominator.nominatedPods[""])
			}
			if tt.wantNominatedNode == "" {
				if len(q.nominator.nominatedPods) != 0 {
					t.Errorf("expected nominatedPods empty, got %v", q.nominator.nominatedPods)
				}
				if len(q.nominator.nominatedPodToNode) != 0 {
					t.Errorf("expected nominatedPodToNode empty, got %v", q.nominator.nominatedPodToNode)
				}
			} else {
				if got := len(q.nominator.nominatedPods[tt.wantNominatedNode]); got != 1 {
					t.Errorf("expected 1 nominated pod on %s, got %d", tt.wantNominatedNode, got)
				}
				if got := q.nominator.nominatedPodToNode[podInfo.Pod.UID]; got != tt.wantNominatedNode {
					t.Errorf("expected nominatedPodToNode[%s]=%s, got %s", podInfo.Pod.UID, tt.wantNominatedNode, got)
				}
			}
		})
	}
}

// TestPriorityQueue_PendingPods 验证 pending Pod 的添加与激活行为。
func TestPriorityQueue_PendingPods(t *testing.T) {
	makeSet := func(pods []*v1.Pod) map[*v1.Pod]struct{} {
		pendingSet := map[*v1.Pod]struct{}{}
		for _, p := range pods {
			pendingSet[p] = struct{}{}
		}
		return pendingSet
	}

	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	q := NewTestQueue(ctx, newDefaultQueueSort())
	// 为模拟真实世界中 Pod 调度失败，在下面的 AddUnschedulablePodIfNotPresent 之前先从 activeQ 弹出该 Pod。
	q.Add(ctx, unschedulablePodInfo.Pod)
	if p, err := q.Pop(logger); err != nil {
		t.Errorf("Pop failed: %v", err)
	} else if diff := cmp.Diff(unschedulablePodInfo.Pod, p.(*framework.QueuedPodInfo).Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}
	q.Add(ctx, highPriorityPodInfo.Pod)
	if p, err := q.Pop(logger); err != nil {
		t.Errorf("Pop failed: %v", err)
	} else if diff := cmp.Diff(highPriorityPodInfo.Pod, p.(*framework.QueuedPodInfo).Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}
	q.Add(ctx, medPriorityPodInfo.Pod)
	err := q.AddUnschedulablePodIfNotPresent(logger, q.newQueuedPodInfo(ctx, unschedulablePodInfo.Pod, "plugin"), q.SchedulingCycle())
	if err != nil {
		t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
	}
	err = q.AddUnschedulablePodIfNotPresent(logger, q.newQueuedPodInfo(ctx, highPriorityPodInfo.Pod, "plugin"), q.SchedulingCycle())
	if err != nil {
		t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
	}

	expectedSet := makeSet([]*v1.Pod{medPriorityPodInfo.Pod, unschedulablePodInfo.Pod, highPriorityPodInfo.Pod})
	gotPods, gotSummary := q.PendingPods()
	if diff := cmp.Diff(expectedSet, makeSet(gotPods)); diff != "" {
		t.Errorf("Unexpected list of pending Pods (-want, +got):\n%s", diff)
	}
	if wantSummary := fmt.Sprintf(pendingPodsSummary, 1, 0, 2); wantSummary != gotSummary {
		t.Errorf("Unexpected pending pods summary: want %v, but got %v.", wantSummary, gotSummary)
	}
	// 把所有 Pod 移到 active queue。看到的 Pod 集合应不变。
	q.MoveAllToActiveOrBackoffQueue(logger, framework.EventUnschedulableTimeout, nil, nil, nil)
	gotPods, gotSummary = q.PendingPods()
	if diff := cmp.Diff(expectedSet, makeSet(gotPods)); diff != "" {
		t.Errorf("Unexpected list of pending Pods (-want, +got):\n%s", diff)
	}
	if wantSummary := fmt.Sprintf(pendingPodsSummary, 1, 2, 0); wantSummary != gotSummary {
		t.Errorf("Unexpected pending pods summary: want %v, but got %v.", wantSummary, gotSummary)
	}
}

// TestPriorityQueue_UpdateNominatedPodForNode 验证 UpdateNominatedPodForNode 对提名信息的更新。
func TestPriorityQueue_UpdateNominatedPodForNode(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	objs := []runtime.Object{medPriorityPodInfo.Pod, unschedulablePodInfo.Pod, highPriorityPodInfo.Pod, scheduledPodInfo.Pod}
	q := NewTestQueueWithObjects(ctx, newDefaultQueueSort(), objs)
	q.Add(ctx, medPriorityPodInfo.Pod)
	// 更新 unschedulablePodInfo，使用与 Pod 中不同的节点。
	q.AddNominatedPod(logger, mustNewTestPodInfo(t, unschedulablePodInfo.Pod),
		&fwk.NominatingInfo{NominatingMode: fwk.ModeOverride, NominatedNodeName: "node5"})

	// 更新一个 Pod 的 nominated node name，指定 Pod 对象中未写明的节点。
	q.AddNominatedPod(logger, mustNewTestPodInfo(t, highPriorityPodInfo.Pod),
		&fwk.NominatingInfo{NominatingMode: fwk.ModeOverride, NominatedNodeName: "node2"})
	expectedNominatedPods := &nominator{
		nominatedPodToNode: map[types.UID]string{
			medPriorityPodInfo.Pod.UID:   "node1",
			highPriorityPodInfo.Pod.UID:  "node2",
			unschedulablePodInfo.Pod.UID: "node5",
		},
		nominatedPods: map[string][]podRef{
			"node1": {podToRef(medPriorityPodInfo.Pod)},
			"node2": {podToRef(highPriorityPodInfo.Pod)},
			"node5": {podToRef(unschedulablePodInfo.Pod)},
		},
	}
	if diff := cmp.Diff(q.nominator, expectedNominatedPods, nominatorCmpOpts...); diff != "" {
		t.Errorf("Unexpected diff after adding pods (-want, +got):\n%s", diff)
	}
	if p, err := q.Pop(logger); err != nil {
		t.Errorf("Pop failed: %v", err)
	} else if diff := cmp.Diff(medPriorityPodInfo.Pod, p.(*framework.QueuedPodInfo).Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}
	// 从队列弹出后，nominated Pod 列表不应变化。
	if diff := cmp.Diff(q.nominator, expectedNominatedPods, nominatorCmpOpts...); diff != "" {
		t.Errorf("Unexpected diff after popping pods (-want, +got):\n%s", diff)
	}
	// 更新其中一个 nominated Pod，它在 Pod 对象中没有 nominatedNodeName，
	// Pod 对象，它应被正确更新。
	q.AddNominatedPod(logger, highPriorityPodInfo, &fwk.NominatingInfo{NominatingMode: fwk.ModeOverride, NominatedNodeName: "node4"})
	expectedNominatedPods = &nominator{
		nominatedPodToNode: map[types.UID]string{
			medPriorityPodInfo.Pod.UID:   "node1",
			highPriorityPodInfo.Pod.UID:  "node4",
			unschedulablePodInfo.Pod.UID: "node5",
		},
		nominatedPods: map[string][]podRef{
			"node1": {podToRef(medPriorityPodInfo.Pod)},
			"node4": {podToRef(highPriorityPodInfo.Pod)},
			"node5": {podToRef(unschedulablePodInfo.Pod)},
		},
	}
	if diff := cmp.Diff(q.nominator, expectedNominatedPods, nominatorCmpOpts...); diff != "" {
		t.Errorf("Unexpected diff after updating pods (-want, +got):\n%s", diff)
	}

	// 尝试提名一个已从 informer 缓存中删除的 Pod。
	// 不应有任何变化。
	q.AddNominatedPod(logger, nonExistentPodInfo, &fwk.NominatingInfo{NominatingMode: fwk.ModeOverride, NominatedNodeName: "node1"})
	if diff := cmp.Diff(q.nominator, expectedNominatedPods, nominatorCmpOpts...); diff != "" {
		t.Errorf("Unexpected diff after nominating a deleted pod (-want, +got):\n%s", diff)
	}
	// 尝试提名一个在 informer 缓存中已处于已调度状态的 Pod。
	// 不应有任何变化。
	scheduledPodCopy := scheduledPodInfo.Pod.DeepCopy()
	scheduledPodInfo.Pod.Spec.NodeName = ""
	q.AddNominatedPod(logger, mustNewTestPodInfo(t, scheduledPodCopy), &fwk.NominatingInfo{NominatingMode: fwk.ModeOverride, NominatedNodeName: "node1"})
	if diff := cmp.Diff(q.nominator, expectedNominatedPods, nominatorCmpOpts...); diff != "" {
		t.Errorf("Unexpected diff after nominating a scheduled pod (-want, +got):\n%s", diff)
	}

	// 删除一个 Pod 中没有 nominatedNodeName 的 nominated Pod
	// 对象，它应被删除。
	q.DeleteNominatedPodIfExists(highPriorityPodInfo.Pod)
	expectedNominatedPods = &nominator{
		nominatedPodToNode: map[types.UID]string{
			medPriorityPodInfo.Pod.UID:   "node1",
			unschedulablePodInfo.Pod.UID: "node5",
		},
		nominatedPods: map[string][]podRef{
			"node1": {podToRef(medPriorityPodInfo.Pod)},
			"node5": {podToRef(unschedulablePodInfo.Pod)},
		},
	}
	if diff := cmp.Diff(q.nominator, expectedNominatedPods, nominatorCmpOpts...); diff != "" {
		t.Errorf("Unexpected diff after deleting pods (-want, +got):\n%s", diff)
	}
}

// TestPriorityQueue_NewWithOptions 验证 NewWithOptions 用自定义参数构造队列。
func TestPriorityQueue_NewWithOptions(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	q := NewTestQueue(ctx,
		newDefaultQueueSort(),
		WithPodInitialBackoffDuration(2*time.Second),
		WithPodMaxBackoffDuration(20*time.Second),
	)

	if q.backoffQ.podInitialBackoffDuration() != 2*time.Second {
		t.Errorf("Unexpected pod backoff initial duration. Expected: %v, got: %v", 2*time.Second, q.backoffQ.podInitialBackoffDuration())
	}

	if q.backoffQ.podMaxBackoffDuration() != 20*time.Second {
		t.Errorf("Unexpected pod backoff max duration. Expected: %v, got: %v", 2*time.Second, q.backoffQ.podMaxBackoffDuration())
	}
}

// TestSchedulingQueue_Close 验证 Close 能安全关闭队列并中断阻塞的 Pop。
func TestSchedulingQueue_Close(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	q := NewTestQueue(ctx, newDefaultQueueSort())
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		entity, err := q.Pop(logger)
		if err != nil {
			t.Errorf("Expected nil err from Pop() if queue is closed, but got %q", err.Error())
		}
		if entity != nil {
			t.Errorf("Expected nil item from Pop() if queue is closed, but got: %v", entity)
		}
	}()
	q.Close()
	wg.Wait()
}

// TestRecentlyTriedPodsGoBack 验证最近尝试过且不可调度的 Pod
// 不可调度的 Pod 会排在同优先级其他 Pod 之后。这一行为
// 保证在频繁有事件把 Pod 移入 active queue 时，
// 频繁有事件把 Pod 移入 active queue。
func TestRecentlyTriedPodsGoBack(t *testing.T) {
	c := testingclock.NewFakeClock(time.Now())
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	q := NewTestQueue(ctx, newDefaultQueueSort(), WithClock(c))
	// 向优先级队列加入几个 Pod。
	for i := 0; i < 5; i++ {
		p := st.MakePod().Name(fmt.Sprintf("test-pod-%v", i)).Namespace("ns1").UID(fmt.Sprintf("tp00%v", i)).Priority(highPriority).Node("node1").NominatedNodeName("node1").Obj()
		q.Add(ctx, p)
	}
	c.Step(time.Microsecond)
	// 模拟 Pod 被调度器弹出、判定为不可调度，然后
	// 随后被移回 active queue。
	entity, err := q.Pop(logger)
	if err != nil {
		t.Errorf("Error while popping the head of the queue: %v", err)
	}
	p1 := entity.(*framework.QueuedPodInfo)
	// 更新 Pod 条件为 unschedulable。
	podutil.UpdatePodCondition(&p1.PodInfo.Pod.Status, &v1.PodCondition{
		Type:          v1.PodScheduled,
		Status:        v1.ConditionFalse,
		Reason:        v1.PodReasonUnschedulable,
		Message:       "fake scheduling failure",
		LastProbeTime: metav1.Now(),
	})
	p1.UnschedulablePlugins = sets.New("plugin")
	// 放入不可调度队列。
	err = q.AddUnschedulablePodIfNotPresent(logger, p1, q.SchedulingCycle())
	if err != nil {
		t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
	}
	c.Step(q.backoffQ.podMaxBackoffDuration())
	// 把所有不可调度 Pod 移到 active queue。
	q.MoveAllToActiveOrBackoffQueue(logger, framework.EventUnschedulableTimeout, nil, nil, nil)
	// 模拟结束。现在弹出所有 Pod，最先弹出的应当是
	// 这里最后弹出的那个。
	for i := 0; i < 5; i++ {
		entity, err := q.Pop(logger)
		if err != nil {
			t.Errorf("Error while popping pods from the queue: %v", err)
		}
		p := entity.(*framework.QueuedPodInfo)
		if (i == 4) != (p1 == p) {
			t.Errorf("A pod tried before is not the last pod popped: i: %v, pod name: %v", i, p.PodInfo.Pod.Name)
		}
	}
}

// TestPodFailedSchedulingMultipleTimesDoesNotBlockNewerPod 验证
// 多次被判定为不可调度的 Pod 不会阻塞任何更新的 Pod。
// 这一行为保证在频繁有事件把 Pod 移入 active queue 时，
// 频繁有事件把 Pod 移入 active queue。
func TestPodFailedSchedulingMultipleTimesDoesNotBlockNewerPod(t *testing.T) {
	c := testingclock.NewFakeClock(time.Now())
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	q := NewTestQueue(ctx, newDefaultQueueSort(), WithClock(c))

	// 向优先级队列加入一个不可调度 Pod。
	// 这样就构造出 Pod 曾被尝试调度、
	// 且到目前为止已被判定为不可调度
	unschedulablePod := st.MakePod().Name("test-pod-unscheduled").Namespace("ns1").UID("tp001").Priority(highPriority).NominatedNodeName("node1").Obj()

	// 更新 Pod 条件为 unschedulable。
	podutil.UpdatePodCondition(&unschedulablePod.Status, &v1.PodCondition{
		Type:    v1.PodScheduled,
		Status:  v1.ConditionFalse,
		Reason:  v1.PodReasonUnschedulable,
		Message: "fake scheduling failure",
	})

	// 为模拟真实世界中 Pod 调度失败，在下面的 AddUnschedulablePodIfNotPresent() 之前先从 activeQ 弹出该 Pod。
	q.Add(ctx, unschedulablePod)
	if p, err := q.Pop(logger); err != nil {
		t.Errorf("Pop failed: %v", err)
	} else if diff := cmp.Diff(unschedulablePod, p.(*framework.QueuedPodInfo).Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}
	// 放入不可调度队列
	err := q.AddUnschedulablePodIfNotPresent(logger, newQueuedPodInfoForLookup(unschedulablePod, "plugin"), q.SchedulingCycle())
	if err != nil {
		t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
	}
	// 推进时钟，让不可调度 Pod 完成退避。
	c.Step(DefaultPodInitialBackoffDuration + time.Second)
	// 把所有不可调度 Pod 移到 active queue。
	q.MoveAllToActiveOrBackoffQueue(logger, framework.EventUnschedulableTimeout, nil, nil, nil)

	// 模拟 Pod 被调度器弹出、
	// 此时应弹出不可调度 Pod。
	entity, err := q.Pop(logger)
	if err != nil {
		t.Errorf("Error while popping the head of the queue: %v", err)
	}
	p1 := entity.(*framework.QueuedPodInfo)
	if diff := cmp.Diff(unschedulablePod, p1.Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}

	// 假设在不可调度 Pod 之后立刻加入了新 Pod
	// 从队列弹出后、重新入队前
	newerPod := st.MakePod().Name("test-newer-pod").Namespace("ns1").UID("tp002").CreationTimestamp(metav1.Now()).Priority(highPriority).NominatedNodeName("node1").Obj()
	q.Add(ctx, newerPod)

	// 随后 unschedulablePodInfo 又被判定为不可调度。
	podutil.UpdatePodCondition(&unschedulablePod.Status, &v1.PodCondition{
		Type:    v1.PodScheduled,
		Status:  v1.ConditionFalse,
		Reason:  v1.PodReasonUnschedulable,
		Message: "fake scheduling failure",
	})

	// 然后，把不可调度 Pod 放入不可调度队列
	err = q.AddUnschedulablePodIfNotPresent(logger, newQueuedPodInfoForLookup(unschedulablePod, "plugin"), q.SchedulingCycle())
	if err != nil {
		t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
	}
	// 推进时钟，让不可调度 Pod 完成退避。
	c.Step(DefaultPodInitialBackoffDuration + time.Second)
	// 把所有不可调度 Pod 移到 active queue。
	q.MoveAllToActiveOrBackoffQueue(logger, framework.EventUnschedulableTimeout, nil, nil, nil)

	// 此时应弹出 newerPod
	// 因为它是最早尝试过的 Pod。
	item2, err2 := q.Pop(logger)
	if err2 != nil {
		t.Errorf("Error while popping the head of the queue: %v", err2)
	} else {
		p2 := item2.(*framework.QueuedPodInfo)
		if diff := cmp.Diff(newerPod, p2.Pod); diff != "" {
			t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
		}
	}
}

// TestHighPriorityBackoff 验证高优先级 Pod 不可调度时
// 其他 Pod（如果它不可调度）
func TestHighPriorityBackoff(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	q := NewTestQueue(ctx, newDefaultQueueSort())

	midPod := st.MakePod().Name("test-midpod").Namespace("ns1").UID("tp-mid").Priority(midPriority).NominatedNodeName("node1").Obj()
	highPod := st.MakePod().Name("test-highpod").Namespace("ns1").UID("tp-high").Priority(highPriority).NominatedNodeName("node1").Obj()
	q.Add(ctx, midPod)
	q.Add(ctx, highPod)
	// 模拟 Pod 被调度器弹出、判定为不可调度，然后
	// 随后被移回 active queue。
	entity, err := q.Pop(logger)
	if err != nil {
		t.Errorf("Error while popping the head of the queue: %v", err)
	}
	p := entity.(*framework.QueuedPodInfo)
	if diff := cmp.Diff(highPod, p.Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}
	// 更新 Pod 条件为 unschedulable。
	podutil.UpdatePodCondition(&p.Pod.Status, &v1.PodCondition{
		Type:    v1.PodScheduled,
		Status:  v1.ConditionFalse,
		Reason:  v1.PodReasonUnschedulable,
		Message: "fake scheduling failure",
	})
	// 放入不可调度队列。
	err = q.AddUnschedulablePodIfNotPresent(logger, newQueuedPodInfoForLookup(p.Pod, "fooPlugin"), q.SchedulingCycle())
	if err != nil {
		t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
	}
	// 把所有不可调度 Pod 移到 activeQ/backoffQ。
	q.MoveAllToActiveOrBackoffQueue(logger, framework.EventUnschedulableTimeout, nil, nil, nil)

	entity, err = q.Pop(logger)
	p = entity.(*framework.QueuedPodInfo)
	if err != nil {
		t.Errorf("Error while popping the head of the queue: %v", err)
	} else if diff := cmp.Diff(midPod, p.Pod); diff != "" {
		// 高优先级 Pod 应在 backoffQ 中
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}
}

// TestHighPriorityFlushUnschedulableEntitiesLeftover 验证实体在 unschedulableEntities 中
// 一分钟后移入 activeQ（如果它位于 unschedulableEntities）。
func TestHighPriorityFlushUnschedulableEntitiesLeftover(t *testing.T) {
	c := testingclock.NewFakeClock(time.Now())
	m := makeEmptyQueueingHintMapPerProfile()
	m[""][nodeAdd] = []*QueueingHintFunction{
		{
			PluginName:     "fakePlugin",
			QueueingHintFn: queueHintReturnQueue,
		},
	}
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	q := NewTestQueue(ctx, newDefaultQueueSort(), WithClock(c), WithQueueingHintMapPerProfile(m))
	midPod := st.MakePod().Name("test-midpod").Namespace("ns1").UID("tp-mid").Priority(midPriority).NominatedNodeName("node1").Obj()
	highPod := st.MakePod().Name("test-highpod").Namespace("ns1").UID("tp-high").Priority(highPriority).NominatedNodeName("node1").Obj()

	// 更新 highPod 的 Pod 条件。
	podutil.UpdatePodCondition(&highPod.Status, &v1.PodCondition{
		Type:    v1.PodScheduled,
		Status:  v1.ConditionFalse,
		Reason:  v1.PodReasonUnschedulable,
		Message: "fake scheduling failure",
	})

	// 更新 midPod 的 Pod 条件。
	podutil.UpdatePodCondition(&midPod.Status, &v1.PodCondition{
		Type:    v1.PodScheduled,
		Status:  v1.ConditionFalse,
		Reason:  v1.PodReasonUnschedulable,
		Message: "fake scheduling failure",
	})

	// 为模拟真实世界中 Pod 调度失败，在下面的 AddUnschedulablePodIfNotPresent 之前先从 activeQ 弹出该 Pod。
	q.Add(ctx, highPod)
	if p, err := q.Pop(logger); err != nil {
		t.Errorf("Pop failed: %v", err)
	} else if diff := cmp.Diff(highPod, p.(*framework.QueuedPodInfo).Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}
	q.Add(ctx, midPod)
	if p, err := q.Pop(logger); err != nil {
		t.Errorf("Pop failed: %v", err)
	} else if diff := cmp.Diff(midPod, p.(*framework.QueuedPodInfo).Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}
	err := q.AddUnschedulablePodIfNotPresent(logger, q.newQueuedPodInfo(ctx, highPod, "fakePlugin"), q.SchedulingCycle())
	if err != nil {
		t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
	}
	err = q.AddUnschedulablePodIfNotPresent(logger, q.newQueuedPodInfo(ctx, midPod, "fakePlugin"), q.SchedulingCycle())
	if err != nil {
		t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
	}
	c.Step(DefaultPodMaxInUnschedulablePodsDuration + time.Second)
	q.flushUnschedulableEntitiesLeftover(logger)

	if p, err := q.Pop(logger); err != nil {
		t.Errorf("Pop failed: %v", err)
	} else if diff := cmp.Diff(highPod, p.(*framework.QueuedPodInfo).Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}
	if p, err := q.Pop(logger); err != nil {
		t.Errorf("Pop failed: %v", err)
	} else if diff := cmp.Diff(midPod, p.(*framework.QueuedPodInfo).Pod); diff != "" {
		t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
	}
}

// TestFlushUnschedulableEntitiesLeftoverSetsFlag 验证 WasFlushedFromUnschedulable
// 实体被 flush 时标志正确置位，回到队列时正确清除。
func TestFlushUnschedulableEntitiesLeftoverSetsFlag(t *testing.T) {
	c := testingclock.NewFakeClock(time.Now())
	m := makeEmptyQueueingHintMapPerProfile()
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	q := NewTestQueue(ctx, newDefaultQueueSort(), WithClock(c), WithQueueingHintMapPerProfile(m))

	pod := st.MakePod().Name("test-pod").Namespace("ns1").UID("tp-1").Priority(midPriority).NominatedNodeName("node1").Obj()

	// 把 Pod 加入 activeQ 再 Pop，模拟一次调度尝试
	q.Add(ctx, pod)
	entity, err := q.Pop(logger)
	if err != nil {
		t.Fatalf("Unexpected error from Pop: %v", err)
	}
	pInfo := entity.(*framework.QueuedPodInfo)

	// 验证标志初始为 false
	if pInfo.WasFlushedFromUnschedulable {
		t.Errorf("Expected WasFlushedFromUnschedulable to be false initially, but got true")
	}

	// 把 Pod 加入 unschedulableEntities（模拟调度失败）
	err = q.AddUnschedulablePodIfNotPresent(logger, q.newQueuedPodInfo(ctx, pod, "fakePlugin"), q.SchedulingCycle())
	if err != nil {
		t.Fatalf("Unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
	}

	// 把时间推进超过 flush 周期并触发 flush
	c.Step(DefaultPodMaxInUnschedulablePodsDuration + time.Second)
	q.flushUnschedulableEntitiesLeftover(logger)

	// 弹出 Pod 并验证标志现在为 true
	entity, err = q.Pop(logger)
	pInfo = entity.(*framework.QueuedPodInfo)
	if err != nil {
		t.Fatalf("Unexpected error from Pop after flush: %v", err)
	}
	if !pInfo.WasFlushedFromUnschedulable {
		t.Errorf("Expected WasFlushedFromUnschedulable to be true after flush, but got false")
	}

	// 模拟 Pod 再次调度失败并返回队列
	err = q.AddUnschedulablePodIfNotPresent(logger, q.newQueuedPodInfo(ctx, pInfo.Pod, "fakePlugin"), q.SchedulingCycle())
	if err != nil {
		t.Fatalf("Unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
	}

	// 验证 Pod 回到队列后标志被清除
	internalPInfo := q.unschedulableEntities.get(newQueuedPodInfoForLookup(pod))
	if internalPInfo == nil {
		t.Fatalf("pod should be in unschedulableEntities")
	}
	if internalPInfo.(*framework.QueuedPodInfo).WasFlushedFromUnschedulable {
		t.Errorf("Expected WasFlushedFromUnschedulable to be cleared (false) after returning to queue, but got true")
	}
}

// TestFlushUnschedulablePodsLeftoverSetsFlag_GatedPod 验证带 gate 的 Pod 被 flush 后标志正确置位与清除。
func TestFlushUnschedulablePodsLeftoverSetsFlag_GatedPod(t *testing.T) {
	tests := []struct {
		name                            string
		gatedBeforeFlush                bool
		gatedAfterFlush                 bool
		backingOff                      bool
		wantWasFlushedFromUnschedulable bool
		wantQ                           string
	}{
		{
			name:                            "flag is set when pod is not gated",
			wantWasFlushedFromUnschedulable: true,
			wantQ:                           activeQ,
		},
		{
			name:                            "flag is set when pod is no longer gated",
			gatedBeforeFlush:                true,
			wantWasFlushedFromUnschedulable: true,
			wantQ:                           activeQ,
		},
		{
			name:                            "flag is unset when pod is newly gated",
			gatedAfterFlush:                 true,
			wantWasFlushedFromUnschedulable: false,
			wantQ:                           unschedulableQ,
		},
		{
			name:                            "flag is unset when pod is still gated",
			gatedBeforeFlush:                true,
			gatedAfterFlush:                 true,
			wantWasFlushedFromUnschedulable: false,
			wantQ:                           unschedulableQ,
		},
		{
			name:                            "flag is set when pod is not gated and backoff is not complete",
			backingOff:                      true,
			wantWasFlushedFromUnschedulable: true,
			wantQ:                           backoffQ,
		},
		{
			name:                            "flag is set when pod is no longer gated and backoff is not complete",
			gatedBeforeFlush:                true,
			backingOff:                      true,
			wantWasFlushedFromUnschedulable: true,
			wantQ:                           backoffQ,
		},
		{
			name:                            "flag is unset when pod is newly gated and backoff is not complete",
			gatedAfterFlush:                 true,
			backingOff:                      true,
			wantWasFlushedFromUnschedulable: false,
			wantQ:                           unschedulableQ,
		},
		{
			name:                            "flag is unset when pod is still gated and backoff is not complete",
			gatedBeforeFlush:                true,
			gatedAfterFlush:                 true,
			backingOff:                      true,
			wantWasFlushedFromUnschedulable: false,
			wantQ:                           unschedulableQ,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const allowedLabel = "allow"
			const preEnqueuePluginName = "preEnqueuePlugin"

			var backoffDuration time.Duration
			if tt.backingOff {
				backoffDuration = DefaultPodMaxInUnschedulablePodsDuration + time.Minute
			}

			c := testingclock.NewFakeClock(time.Now())
			m := makeEmptyQueueingHintMapPerProfile()
			preEnqM := map[string]map[string]fwk.PreEnqueuePlugin{
				"": {
					preEnqueuePluginName: &preEnqueuePlugin{allowlists: []string{allowedLabel}},
				},
			}
			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			q := NewTestQueue(ctx, newDefaultQueueSort(), WithClock(c), WithQueueingHintMapPerProfile(m), WithPreEnqueuePluginMap(preEnqM),
				WithPodInitialBackoffDuration(backoffDuration), WithPodMaxBackoffDuration(backoffDuration))

			pod := st.MakePod().Name("pod1").Namespace("ns1").UID("1").Label(allowedLabel, "").Obj()
			podInfo := &framework.QueuedPodInfo{PodInfo: mustNewPodInfo(pod), QueueingParams: framework.QueueingParams{UnschedulablePlugins: sets.New("foo")}}
			if tt.gatedBeforeFlush {
				podInfo = setQueuedPodInfoGated(podInfo, preEnqueuePluginName, []fwk.ClusterEvent{})
			}

			q.Add(ctx, podInfo.Pod)
			_, err := q.Pop(logger)
			if err != nil {
				t.Fatalf("Failed to pop from active queue: %v", err)
			}

			if tt.gatedAfterFlush {
				delete(pod.Labels, allowedLabel)
			}

			err = q.AddUnschedulablePodIfNotPresent(logger, podInfo, q.SchedulingCycle())
			if err != nil {
				t.Fatalf("Failed to add pod to unschedulable: %v", err)
			}

			// 把时间推进超过 flush 周期并触发 flush
			c.Step(DefaultPodMaxInUnschedulablePodsDuration + time.Second)
			q.flushUnschedulableEntitiesLeftover(logger)

			queueSizes := map[string]int{
				unschedulableQ: len(q.UnschedulablePods()),
				backoffQ:       len(q.PodsInBackoffQ()),
				activeQ:        len(q.PodsInActiveQ()),
			}

			if queueSizes[tt.wantQ] == 0 {
				t.Errorf("Pod not found in %s", tt.wantQ)
			}
			actualPod, ok := q.GetPod(podInfo.Pod.Name, podInfo.Pod.Namespace, nil)
			if !ok {
				t.Fatalf("Pod not found in scheduling queue")
			}
			if actualPod.WasFlushedFromUnschedulable != tt.wantWasFlushedFromUnschedulable {
				t.Errorf("Unexpected WasFlushedFromUnschedulable value: %v", actualPod.WasFlushedFromUnschedulable)
			}
		})
	}
}

// TestGatedPodFlushFrequency 验证带 gate 的 Pod 仅每隔
// podMaxInUnschedulablePodsDuration，而不是每次周期性 flush 都执行。
func TestGatedPodFlushFrequency(t *testing.T) {
	gatedPod := mustNewPodInfo(st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj())

	tests := []struct {
		name       string
		entityInfo framework.QueuedEntityInfo
	}{
		{
			name: "queued pod",
			entityInfo: &framework.QueuedPodInfo{
				PodInfo:        gatedPod,
				QueueingParams: framework.QueueingParams{UnschedulablePlugins: sets.New("foo")},
			},
		},
		{
			name: "queued pod group",
			entityInfo: &framework.QueuedPodGroupInfo{
				PodGroupInfo: &framework.PodGroupInfo{Namespace: gatedPod.GetNamespace(), Name: "pg", UnscheduledPods: []*v1.Pod{gatedPod.Pod}},
				QueuedPodInfos: map[fwk.EntityKey][]*framework.QueuedPodInfo{
					fwk.PodGroupKey("test", "pg"): {{PodInfo: gatedPod, QueueingParams: framework.QueueingParams{UnschedulablePlugins: sets.New("foo")}}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := testingclock.NewFakeClock(time.Now())
			m := makeEmptyQueueingHintMapPerProfile()
			preEnqueuePluginName := "preEnqueuePlugin"
			preEnqM := map[string]map[string]fwk.PreEnqueuePlugin{
				"": {
					preEnqueuePluginName: &preEnqueuePlugin{}, // 允许列表为空，阻塞所有 Pod。
				},
			}
			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			// 为清晰起见，使用用户自定义的 5 分钟时长
			flushDuration := 5 * time.Minute
			q := NewTestQueue(ctx, newDefaultQueueSort(), WithClock(c), WithQueueingHintMapPerProfile(m), WithPreEnqueuePluginMap(preEnqM),
				WithPodMaxInUnschedulablePodsDuration(flushDuration))

			getEntityFromAnyQueue := func() framework.QueuedEntityInfo {
				var entity framework.QueuedEntityInfo
				q.activeQ.underRLock(func(unlockedActiveQ unlockedActiveQueueReader) {
					entity = q.getEntityFromAnyQueue(unlockedActiveQ, tt.entityInfo)
				})
				if entity == nil {
					t.Fatalf("Failed to find entity in any queue")
				}
				return entity
			}

			// 直接把带 gate 的 Pod 加入 unschedulableEntities
			q.unschedulableEntities.addOrUpdate(tt.entityInfo, false, "test-setup", nil)

			// 推进时钟超过 flush 周期并触发 flush
			// T=5:01
			c.Step(flushDuration + time.Second)

			q.flushUnschedulableEntitiesLeftover(logger)

			actualEntity := getEntityFromAnyQueue()
			// 验证发生了 flush
			firstFlushTime := actualEntity.GetFlushTimestamp()
			if firstFlushTime.IsZero() {
				t.Errorf("Expected FlushTimestamp to be set after the first leftover flush")
			}

			// 推进时钟不足 flush 周期并触发 flush
			// T=9:01
			c.Step(4 * time.Minute)
			q.flushUnschedulableEntitiesLeftover(logger)

			actualEntity = getEntityFromAnyQueue()
			// 验证没有发生 flush
			if actualEntity.GetFlushTimestamp() != firstFlushTime {
				t.Errorf("Expected FlushTimestamp to remain %v, but was updated to %v (pod was flushed prematurely)", firstFlushTime, actualEntity.GetFlushTimestamp())
			}

			// 推进时钟超过距上次 flush 的时长
			// T=10:02
			c.Step(time.Minute + time.Second)
			q.flushUnschedulableEntitiesLeftover(logger)

			actualEntity = getEntityFromAnyQueue()
			// 验证发生了 flush
			if !actualEntity.GetFlushTimestamp().After(firstFlushTime) {
				t.Errorf("Expected FlushTimestamp to be updated to a newer time after 5 minutes elapsed, but remained %v", actualEntity.GetFlushTimestamp())
			}
		})
	}
}

// TestAddAttemptedPodGroupIfNeeded 验证 AddAttemptedPodGroupIfNeeded
// 能正确处理带或不带失败插件的 PodGroup。
func TestAddAttemptedPodGroupIfNeeded(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.GenericWorkload, true)

	pgName := "test-pg"
	pod1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").Priority(midPriority).PodGroupName(pgName).Obj()
	pod2 := st.MakePod().Name("pod2").Namespace("ns1").UID("pod2").Priority(midPriority).PodGroupName(pgName).Obj()

	tests := []struct {
		name                            string
		setup                           func(t ktesting.TContext, q *PriorityQueue, pgInfo *framework.QueuedPodGroupInfo)
		disableBackoff                  bool
		blockOnPreEnqueue               bool
		skipAddPodGroup                 bool
		status                          *fwk.Status
		expectedUnschedulableCount      int
		expectedConsecutiveErrorsCount  int
		expectedInActiveQ               bool
		expectedInBackoffQ              bool
		expectedInUnschedulableEntities bool
		expectedInIncomplete            []*v1.Pod
		expectedPodsInGroup             int
		expectPreservedTimestamp        bool
	}{
		{
			name: "No pending pods, pod group is not enqueued",
			setup: func(tCtx ktesting.TContext, q *PriorityQueue, pgInfo *framework.QueuedPodGroupInfo) {
				// 无需准备，pendingPodGroupPods 为空
			},
			expectedPodsInGroup: 1,
		},
		{
			name: "Pods present, pod group scheduling had an error, and pod group not backing off",
			setup: func(tCtx ktesting.TContext, q *PriorityQueue, pgInfo *framework.QueuedPodGroupInfo) {
				pInfo1 := q.newQueuedPodInfo(tCtx, pod1)
				pInfo2 := q.newQueuedPodInfo(tCtx, pod2)
				q.pendingPodGroupPods.add(pInfo1)
				q.pendingPodGroupPods.add(pInfo2)
			},
			status:                         fwk.NewStatus(fwk.Error),
			disableBackoff:                 true,
			expectedConsecutiveErrorsCount: 1,
			expectedInActiveQ:              true,
			expectedPodsInGroup:            2,
		},
		{
			name: "Pods present, pod group scheduling result is unschedulable, and pod group not backing off",
			setup: func(tCtx ktesting.TContext, q *PriorityQueue, pgInfo *framework.QueuedPodGroupInfo) {
				pInfo1 := q.newQueuedPodInfo(tCtx, pod1, "fakePlugin")
				pInfo2 := q.newQueuedPodInfo(tCtx, pod2)
				q.pendingPodGroupPods.add(pInfo1)
				q.pendingPodGroupPods.add(pInfo2)
				pgInfo.ConsecutiveErrorsCount = 5 // 设为非零值，验证重置行为。
			},
			status:                     fwk.NewStatus(fwk.Unschedulable),
			disableBackoff:             true,
			expectedUnschedulableCount: 1,
			expectedInActiveQ:          true,
			expectedPodsInGroup:        2,
		},
		{
			name: "Pods present, pod group scheduling result is unschedulable, and pod group not backing off, but blocked on PreEnqueue",
			setup: func(tCtx ktesting.TContext, q *PriorityQueue, pgInfo *framework.QueuedPodGroupInfo) {
				pInfo1 := q.newQueuedPodInfo(tCtx, pod1, "fakePlugin")
				pInfo2 := q.newQueuedPodInfo(tCtx, pod2)
				q.pendingPodGroupPods.add(pInfo1)
				q.pendingPodGroupPods.add(pInfo2)
			},
			status:                          fwk.NewStatus(fwk.Unschedulable),
			disableBackoff:                  true,
			blockOnPreEnqueue:               true,
			expectedUnschedulableCount:      1,
			expectedInUnschedulableEntities: true,
			expectedPodsInGroup:             2,
		},
		{
			name: "Pods present, pod group scheduling result is Error, and pod group backing off",
			setup: func(tCtx ktesting.TContext, q *PriorityQueue, pgInfo *framework.QueuedPodGroupInfo) {
				pInfo1 := q.newQueuedPodInfo(tCtx, pod1)
				pInfo2 := q.newQueuedPodInfo(tCtx, pod2)
				q.pendingPodGroupPods.add(pInfo1)
				q.pendingPodGroupPods.add(pInfo2)
			},
			status:                         fwk.NewStatus(fwk.Error),
			expectedConsecutiveErrorsCount: 1,
			expectedInBackoffQ:             true,
			expectedPodsInGroup:            2,
		},
		{
			name: "Pods present, pod group scheduling result is Unschedulable, and pod group backing off",
			setup: func(tCtx ktesting.TContext, q *PriorityQueue, pgInfo *framework.QueuedPodGroupInfo) {
				pInfo1 := q.newQueuedPodInfo(tCtx, pod1, "fakePlugin")
				pInfo2 := q.newQueuedPodInfo(tCtx, pod2)
				q.pendingPodGroupPods.add(pInfo1)
				q.pendingPodGroupPods.add(pInfo2)
				pgInfo.ConsecutiveErrorsCount = 5 // 设为非零值，验证重置行为。
			},
			status:                     fwk.NewStatus(fwk.Unschedulable),
			expectedUnschedulableCount: 1,
			expectedInBackoffQ:         true,
			expectedPodsInGroup:        2,
		},
		{
			name: "Pods present, pod group scheduling result is Error, and pod group backing off, but blocked on PreEnqueue",
			setup: func(tCtx ktesting.TContext, q *PriorityQueue, pgInfo *framework.QueuedPodGroupInfo) {
				pInfo1 := q.newQueuedPodInfo(tCtx, pod1)
				pInfo2 := q.newQueuedPodInfo(tCtx, pod2)
				q.pendingPodGroupPods.add(pInfo1)
				q.pendingPodGroupPods.add(pInfo2)
			},
			status:                          fwk.NewStatus(fwk.Error),
			blockOnPreEnqueue:               true,
			expectedConsecutiveErrorsCount:  1,
			expectedInUnschedulableEntities: true,
			expectedPodsInGroup:             2,
		},
		{
			name: "Unschedulable pods are present but pod group scheduling was successful, requeue to active queue directly and preserve timestamp",
			setup: func(tCtx ktesting.TContext, q *PriorityQueue, pgInfo *framework.QueuedPodGroupInfo) {
				pInfo1 := q.newQueuedPodInfo(tCtx, pod1, "fakePlugin")
				pInfo2 := q.newQueuedPodInfo(tCtx, pod2)
				q.pendingPodGroupPods.add(pInfo1)
				q.pendingPodGroupPods.add(pInfo2)
			},
			expectedInActiveQ:        true,
			expectedPodsInGroup:      2,
			expectPreservedTimestamp: true,
		},
		{
			name: "New pods only are present and pod group scheduling was successful, requeue to active queue directly with new timestamp",
			setup: func(tCtx ktesting.TContext, q *PriorityQueue, pgInfo *framework.QueuedPodGroupInfo) {
				pInfo2 := q.newQueuedPodInfo(tCtx, pod2)
				q.pendingPodGroupPods.add(pInfo2)
			},
			expectedInActiveQ:        true,
			expectedPodsInGroup:      1,
			expectPreservedTimestamp: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)

			c := testingclock.NewFakeClock(time.Now())
			m := makeEmptyQueueingHintMapPerProfile()
			preEnqueueMap := make(map[string]map[string]fwk.PreEnqueuePlugin)
			if test.blockOnPreEnqueue {
				preEnqueueMap[""] = map[string]fwk.PreEnqueuePlugin{
					"testPlugin": &preEnqueuePlugin{},
				}
			}

			opts := []Option{WithClock(c), WithQueueingHintMapPerProfile(m), WithPreEnqueuePluginMap(preEnqueueMap)}
			if test.disableBackoff {
				opts = append(opts, WithPodInitialBackoffDuration(0), WithPodMaxBackoffDuration(0))
			}
			q := NewTestQueue(tCtx, newDefaultQueueSort(), opts...)
			podGroup := st.MakePodGroup().Name(pgName).Namespace("ns1").Obj()
			if !test.skipAddPodGroup {
				q.AddPodGroup(tCtx.Logger(), podGroup)
			}

			pgInfo := newSingleLevelPodGroupInfo(q.newQueuedPodInfo(tCtx, pod1), podGroup)
			oldTimestamp := pgInfo.Timestamp

			test.setup(tCtx, q, pgInfo)
			c.Step(time.Second)

			err := q.AddAttemptedPodGroupIfNeeded(tCtx.Logger(), pgInfo, q.SchedulingCycle(), test.status)
			if err != nil {
				tCtx.Fatalf("Unexpected error from AddAttemptedPodGroupIfNeeded: %v", err)
			}
			if !test.skipAddPodGroup {
				if queuedInfo, ok := q.GetPodGroup(podGroup.Name, podGroup.Namespace, fwk.PodGroupKeyType); ok {
					pgInfo = queuedInfo
				}
			}

			if test.expectPreservedTimestamp && !pgInfo.Timestamp.Equal(oldTimestamp) {
				tCtx.Errorf("Expected timestamp to be preserved (%v), but got %v", oldTimestamp, pgInfo.Timestamp)
			}
			if !test.expectPreservedTimestamp && (test.expectedInActiveQ || test.expectedInBackoffQ || test.expectedInUnschedulableEntities) && pgInfo.Timestamp != c.Now() {
				tCtx.Errorf("Expected timestamp to be %v, but got %v", c.Now(), pgInfo.Timestamp)
			}
			if pgInfo.UnschedulableCount != test.expectedUnschedulableCount {
				tCtx.Errorf("Expected UnschedulableCount to be %v, got %v", test.expectedUnschedulableCount, pgInfo.UnschedulableCount)
			}
			if pgInfo.ConsecutiveErrorsCount != test.expectedConsecutiveErrorsCount {
				tCtx.Errorf("Expected ConsecutiveErrorsCount to be %v, got %v", test.expectedConsecutiveErrorsCount, pgInfo.ConsecutiveErrorsCount)
			}
			if isInActiveQ := q.activeQ.has(pgInfo); isInActiveQ != test.expectedInActiveQ {
				tCtx.Errorf("Expected pod group to be in activeQ: %v, got %v", test.expectedInActiveQ, isInActiveQ)
			}
			if isInBackoffQ := q.backoffQ.has(pgInfo); isInBackoffQ != test.expectedInBackoffQ {
				tCtx.Errorf("Expected pod group to be in backoffQ: %v, got %v", test.expectedInBackoffQ, isInBackoffQ)
			}
			if isInUnschedulable := q.unschedulableEntities.get(pgInfo) != nil; isInUnschedulable != test.expectedInUnschedulableEntities {
				tCtx.Errorf("Expected pod group to be in unschedulableEntities: %v, got %v", test.expectedInUnschedulableEntities, isInUnschedulable)
			}
			if q.pendingPodGroupPods.len() != 0 {
				tCtx.Errorf("Expected pendingPodGroupPods to be cleared")
			}
			for _, pod := range test.expectedInIncomplete {
				if !q.incompletePodGroupPods.has(pod) {
					tCtx.Errorf("Expected pod %v to be in incompletePodGroupPods", pod.Name)
				}
			}
			if pgInfo.Size() != test.expectedPodsInGroup {
				tCtx.Errorf("Expected QueuedPodInfos to have %v elements, got %v", test.expectedPodsInGroup, pgInfo.Size())
			}
		})
	}
}

// TestPriorityQueue_initPodMaxInUnschedulablePodsDuration 验证 initPodMaxInUnschedulablePodsDuration 按配置初始化时长。
func TestPriorityQueue_initPodMaxInUnschedulablePodsDuration(t *testing.T) {
	pod1 := st.MakePod().Name("test-pod-1").Namespace("ns1").UID("tp-1").NominatedNodeName("node1").Obj()
	pod2 := st.MakePod().Name("test-pod-2").Namespace("ns2").UID("tp-2").NominatedNodeName("node2").Obj()

	var timestamp = time.Now()
	pInfo1 := &framework.QueuedPodInfo{
		PodInfo: mustNewTestPodInfo(t, pod1),
		QueueingParams: framework.QueueingParams{
			Timestamp: timestamp.Add(-time.Second),
		},
	}
	pInfo2 := &framework.QueuedPodInfo{
		PodInfo: mustNewTestPodInfo(t, pod2),
		QueueingParams: framework.QueueingParams{
			Timestamp: timestamp.Add(-2 * time.Second),
		},
	}

	tests := []struct {
		name                              string
		podMaxInUnschedulablePodsDuration time.Duration
		operations                        []operation
		operands                          []*framework.QueuedPodInfo
		expected                          []*framework.QueuedPodInfo
	}{
		{
			name: "New priority queue by the default value of podMaxInUnschedulablePodsDuration",
			operations: []operation{
				addPodUnschedulablePods,
				addPodUnschedulablePods,
				flushUnscheduledQ,
			},
			operands: []*framework.QueuedPodInfo{pInfo1, pInfo2, nil},
			expected: []*framework.QueuedPodInfo{pInfo2, pInfo1},
		},
		{
			name:                              "New priority queue by user-defined value of podMaxInUnschedulablePodsDuration",
			podMaxInUnschedulablePodsDuration: 30 * time.Second,
			operations: []operation{
				addPodUnschedulablePods,
				addPodUnschedulablePods,
				flushUnscheduledQ,
			},
			operands: []*framework.QueuedPodInfo{pInfo1, pInfo2, nil},
			expected: []*framework.QueuedPodInfo{pInfo2, pInfo1},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pInfo1.FlushTimestamp = time.Time{}
			pInfo2.FlushTimestamp = time.Time{}
			tCtx := ktesting.Init(t)
			logger := klog.FromContext(tCtx)
			var queue *PriorityQueue
			if test.podMaxInUnschedulablePodsDuration > 0 {
				queue = NewTestQueue(tCtx, newDefaultQueueSort(),
					WithClock(testingclock.NewFakeClock(timestamp)),
					WithPodMaxInUnschedulablePodsDuration(test.podMaxInUnschedulablePodsDuration))
			} else {
				queue = NewTestQueue(tCtx, newDefaultQueueSort(),
					WithClock(testingclock.NewFakeClock(timestamp)))
			}

			var podInfoList []*framework.QueuedPodInfo

			for i, op := range test.operations {
				op(tCtx, queue, test.operands[i])
			}

			expectedLen := len(test.expected)
			if queue.activeQ.len() != expectedLen {
				t.Fatalf("Expected %v items to be in activeQ, but got: %v", expectedLen, queue.activeQ.len())
			}

			for i := 0; i < expectedLen; i++ {
				entity, err := queue.activeQ.pop(logger)
				if err != nil {
					t.Errorf("Error while popping the head of the queue: %v", err)
				} else {
					pInfo := entity.(*framework.QueuedPodInfo)
					podInfoList = append(podInfoList, pInfo)
					// 清理 activeQ.pop() 中递增的尝试计数
					pInfo.Attempts = 0
				}
			}

			if diff := cmp.Diff(test.expected, podInfoList, cmpopts.IgnoreUnexported(framework.PodInfo{})); diff != "" {
				t.Errorf("Unexpected QueuedPodInfo list (-want, +got):\n%s", diff)
			}
		})
	}
}

type operation func(tCtx ktesting.TContext, queue *PriorityQueue, pInfo *framework.QueuedPodInfo)

var (
	add = func(tCtx ktesting.TContext, queue *PriorityQueue, pInfo *framework.QueuedPodInfo) {
		queue.Add(tCtx, pInfo.Pod)
	}
	pop = func(tCtx ktesting.TContext, queue *PriorityQueue, _ *framework.QueuedPodInfo) {
		_, err := queue.Pop(klog.FromContext(tCtx))
		if err != nil {
			tCtx.Fatalf("Unexpected error during Pop: %v", err)
		}
	}
	popAndRequeueAsUnschedulable = func(tCtx ktesting.TContext, queue *PriorityQueue, pInfo *framework.QueuedPodInfo) {
		// 为模拟真实世界中 Pod 调度失败，在下面的 AddUnschedulablePodIfNotPresent() 之前先从 activeQ 弹出该 Pod。
		// Pop 会清空 UnschedulablePlugins 和 PendingPlugins，所以先复制一份。
		logger := klog.FromContext(tCtx)
		unschedulablePlugins := pInfo.UnschedulablePlugins.Clone()
		pendingPlugins := pInfo.PendingPlugins.Clone()
		queue.Add(tCtx, pInfo.Pod)
		entity, err := queue.Pop(logger)
		p := entity.(*framework.QueuedPodInfo)
		if err != nil {
			tCtx.Fatalf("Unexpected error during Pop: %v", err)
		} else if diff := cmp.Diff(pInfo.Pod, p.Pod); diff != "" {
			tCtx.Fatalf("Unexpected pod after Pop (-want, +got):\n%s", diff)
		}
		// 模拟等待某些事件的插件。
		p.UnschedulablePlugins = unschedulablePlugins
		p.PendingPlugins = pendingPlugins
		if err := queue.AddUnschedulablePodIfNotPresent(logger, p, 1); err != nil {
			tCtx.Fatalf("Unexpected error during AddUnschedulablePodIfNotPresent: %v", err)
		}
	}
	popAndRequeueAsBackoff = func(tCtx ktesting.TContext, queue *PriorityQueue, pInfo *framework.QueuedPodInfo) {
		// 为模拟真实世界中 Pod 调度失败，在下面的 AddUnschedulablePodIfNotPresent() 之前先从 activeQ 弹出该 Pod。
		logger := klog.FromContext(tCtx)
		queue.Add(tCtx, pInfo.Pod)
		entity, err := queue.Pop(logger)
		p := entity.(*framework.QueuedPodInfo)
		if err != nil {
			tCtx.Fatalf("Unexpected error during Pop: %v", err)
		} else if diff := cmp.Diff(pInfo.Pod, p.Pod); diff != "" {
			tCtx.Fatalf("Unexpected pod after Pop (-want, +got):\n%s", diff)
		}
		// 需要递增它才能进入退避
		p.UnschedulableCount++
		// 没有已知的 unschedulable 插件时，Pod 总是进入 backoff 队列。
		if err := queue.AddUnschedulablePodIfNotPresent(logger, p, 1); err != nil {
			tCtx.Fatalf("Unexpected error during AddUnschedulablePodIfNotPresent: %v", err)
		}
	}
	addPodActiveQ = func(tCtx ktesting.TContext, queue *PriorityQueue, pInfo *framework.QueuedPodInfo) {
		queue.Add(tCtx, pInfo.Pod)
	}
	addPodActiveQDirectly = func(tCtx ktesting.TContext, queue *PriorityQueue, pInfo *framework.QueuedPodInfo) {
		queue.activeQ.add(klog.FromContext(tCtx), pInfo, framework.EventUnscheduledPodAdd.Label(), nil)
	}
	addPodUnschedulablePods = func(tCtx ktesting.TContext, queue *PriorityQueue, pInfo *framework.QueuedPodInfo) {
		if !pInfo.Gated() {
			// 更新 Pod 条件为 unschedulable。
			podutil.UpdatePodCondition(&pInfo.Pod.Status, &v1.PodCondition{
				Type:    v1.PodScheduled,
				Status:  v1.ConditionFalse,
				Reason:  v1.PodReasonUnschedulable,
				Message: "fake scheduling failure",
			})
			// 需要递增它才能进入退避
			pInfo.UnschedulableCount++
		}
		queue.unschedulableEntities.addOrUpdate(pInfo, false, framework.EventUnscheduledPodAdd.Label(), nil)
	}
	deletePod = func(tCtx ktesting.TContext, queue *PriorityQueue, pInfo *framework.QueuedPodInfo) {
		queue.Delete(tCtx.Logger(), pInfo.Pod)
	}
	updatePodQueueable = func(tCtx ktesting.TContext, queue *PriorityQueue, pInfo *framework.QueuedPodInfo) {
		newPod := pInfo.Pod.DeepCopy()
		newPod.Labels = map[string]string{"queueable": ""}
		queue.Update(tCtx, pInfo.Pod, newPod)
	}
	addPodBackoffQ = func(tCtx ktesting.TContext, queue *PriorityQueue, pInfo *framework.QueuedPodInfo) {
		queue.backoffQ.add(klog.FromContext(tCtx), pInfo, framework.EventUnscheduledPodAdd.Label(), nil)
	}
	moveAllToActiveOrBackoffQ = func(tCtx ktesting.TContext, queue *PriorityQueue, _ *framework.QueuedPodInfo) {
		queue.MoveAllToActiveOrBackoffQueue(klog.FromContext(tCtx), framework.EventUnschedulableTimeout, nil, nil, nil)
	}
	flushBackoffQ = func(tCtx ktesting.TContext, queue *PriorityQueue, _ *framework.QueuedPodInfo) {
		queue.clock.(*testingclock.FakeClock).Step(3 * time.Second)
		queue.flushBackoffQCompleted(klog.FromContext(tCtx))
	}
	moveClockForward = func(tCtx ktesting.TContext, queue *PriorityQueue, _ *framework.QueuedPodInfo) {
		queue.clock.(*testingclock.FakeClock).Step(3 * time.Second)
	}
	flushUnscheduledQ = func(tCtx ktesting.TContext, queue *PriorityQueue, _ *framework.QueuedPodInfo) {
		queue.clock.(*testingclock.FakeClock).Step(queue.podMaxInUnschedulablePodsDuration)
		queue.flushUnschedulableEntitiesLeftover(klog.FromContext(tCtx))
	}
	updatePluginToGateAllPods = func(tCtx ktesting.TContext, queue *PriorityQueue, _ *framework.QueuedPodInfo) {
		queue.preEnqueuePluginMap[""]["preEnqueuePlugin"] = &preEnqueuePlugin{allowlists: []string{""}}
	}
	updatePluginToUngateAllPods = func(tCtx ktesting.TContext, queue *PriorityQueue, _ *framework.QueuedPodInfo) {
		queue.preEnqueuePluginMap[""]["preEnqueuePlugin"] = &preEnqueuePlugin{allowlists: []string{"queueable"}}
	}
	addPodGroupForPod = func(tCtx ktesting.TContext, queue *PriorityQueue, pInfo *framework.QueuedPodInfo) {
		pgName := *pInfo.Pod.Spec.SchedulingGroup.PodGroupName
		pg := st.MakePodGroup().Name(pgName).Namespace(pInfo.Pod.Namespace).Obj()
		queue.AddPodGroup(klog.FromContext(tCtx), pg)
	}
)

// TestPodTimestamp 测试与 QueuedPodInfo 相关的操作。
func TestPodTimestamp(t *testing.T) {
	pod1 := st.MakePod().Name("test-pod-1").Namespace("ns1").UID("tp-1").NominatedNodeName("node1").Obj()
	pod2 := st.MakePod().Name("test-pod-2").Namespace("ns2").UID("tp-2").NominatedNodeName("node2").Obj()

	var timestamp = time.Now()
	pInfo1 := &framework.QueuedPodInfo{
		PodInfo: mustNewTestPodInfo(t, pod1),
		QueueingParams: framework.QueueingParams{
			Timestamp: timestamp,
		},
	}
	pInfo2 := &framework.QueuedPodInfo{
		PodInfo: mustNewTestPodInfo(t, pod2),
		QueueingParams: framework.QueueingParams{
			Timestamp: timestamp.Add(time.Second),
		},
	}

	tests := []struct {
		name       string
		operations []operation
		operands   []*framework.QueuedPodInfo
		expected   []*framework.QueuedPodInfo
	}{
		{
			name: "add two pod to activeQ and sort them by the timestamp",
			operations: []operation{
				// 需要直接把 Pod 加入 activeQ 以覆盖时间戳。
				addPodActiveQDirectly,
				addPodActiveQDirectly,
			},
			operands: []*framework.QueuedPodInfo{pInfo2, pInfo1},
			expected: []*framework.QueuedPodInfo{pInfo1, pInfo2},
		},
		{
			name: "add two pod to unschedulableEntities then move them to activeQ and sort them by the timestamp",
			operations: []operation{
				addPodUnschedulablePods,
				addPodUnschedulablePods,
				moveClockForward,
				moveAllToActiveOrBackoffQ,
			},
			operands: []*framework.QueuedPodInfo{pInfo2, pInfo1, nil, nil},
			expected: []*framework.QueuedPodInfo{pInfo1, pInfo2},
		},
		{
			name: "add one pod to BackoffQ and move it to activeQ",
			operations: []operation{
				// 需要直接把 Pod 加入 activeQ 以覆盖时间戳。
				addPodActiveQDirectly,
				addPodBackoffQ,
				flushBackoffQ,
				moveAllToActiveOrBackoffQ,
			},
			operands: []*framework.QueuedPodInfo{pInfo2, pInfo1, nil, nil},
			expected: []*framework.QueuedPodInfo{pInfo1, pInfo2},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			logger := klog.FromContext(tCtx)
			queue := NewTestQueue(tCtx, newDefaultQueueSort(), WithClock(testingclock.NewFakeClock(timestamp)))
			var podInfoList []*framework.QueuedPodInfo

			for i, op := range test.operations {
				op(tCtx, queue, test.operands[i])
			}

			expectedLen := len(test.expected)
			if queue.activeQ.len() != expectedLen {
				t.Fatalf("Expected %v items to be in activeQ, but got: %v", expectedLen, queue.activeQ.len())
			}

			for i := 0; i < expectedLen; i++ {
				entity, err := queue.activeQ.pop(logger)
				if err != nil {
					t.Errorf("Error while popping the head of the queue: %v", err)
				} else {
					pInfo := entity.(*framework.QueuedPodInfo)
					podInfoList = append(podInfoList, pInfo)
					// 清理 activeQ.pop() 中递增的尝试计数
					pInfo.Attempts = 0
				}
			}

			if diff := cmp.Diff(test.expected, podInfoList, cmpopts.IgnoreUnexported(framework.PodInfo{})); diff != "" {
				t.Errorf("Unexpected QueuedPodInfo list (-want, +got):\n%s", diff)
			}
		})
	}
}

// TestSchedulerPodsMetric 测试 Prometheus 指标
func TestSchedulerPodsMetric(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.GenericWorkload, true)
	timestamp := time.Now()
	preenqueuePluginName := "preEnqueuePlugin"
	metrics.Register()
	total := 60
	queueableNum := 50
	queueable, failme := "queueable", "failme"
	// 前 50 个 Pod 可入队。
	pInfos := makeQueuedPodInfos(queueableNum, "x", queueable, timestamp)
	// 最后 10 个 Pod 不可入队。
	gated := makeQueuedPodInfos(total-queueableNum, "y", failme, timestamp)
	// 手动把它们标记为 gated=true。
	for _, pInfo := range gated {
		setQueuedPodInfoGated(pInfo, preenqueuePluginName, []fwk.ClusterEvent{framework.EventUnscheduledPodUpdate})
	}
	pInfos = append(pInfos, gated...)
	totalWithDelay := 20
	pInfosWithDelay := makeQueuedPodInfos(totalWithDelay, "z", queueable, timestamp.Add(2*time.Second))

	pInfo1 := makeQueuedPodInfo(t, "pod1", "ns1", framework.QueueingParams{}, withPodLabel("queueable", ""), withPodGroup("pg"))
	pInfo2 := makeQueuedPodInfo(t, "pod2", "ns1", framework.QueueingParams{}, withPodLabel("queueable", ""), withPodGroup("pg"))
	pInfoWithPgIncomplete := makeQueuedPodInfo(t, "pod3", "ns1", framework.QueueingParams{}, withPodLabel("queueable", ""), withPodGroup("pg-incomplete"))

	resetPodInfos := func() {
		// 重置 PodInfo 的 Attempts，因为它们影响退避时间计算。
		for i := range pInfos {
			pInfos[i].Attempts = 0
			pInfos[i].UnschedulableCount = 0
		}
		for i := range pInfosWithDelay {
			pInfosWithDelay[i].Attempts = 0
			pInfosWithDelay[i].UnschedulableCount = 0
		}
	}

	tests := []struct {
		name                       string
		operations                 []operation
		operands                   [][]*framework.QueuedPodInfo
		metricsName                string
		pluginMetricsSamplePercent int
		disablePopFromBackoffQ     bool
		wants                      string
	}{
		{
			name: "add pods to activeQ and unschedulableEntities",
			operations: []operation{
				addPodActiveQ,
				addPodUnschedulablePods,
			},
			operands: [][]*framework.QueuedPodInfo{
				pInfos[:30],
				pInfos[30:],
			},
			metricsName: "scheduler_pending_pods",
			wants: `
# HELP scheduler_pending_pods [STABLE] Number of pending pods, by the queue type. 'active' means number of pods in activeQ; 'backoff' means number of pods in backoffQ; 'unschedulable' means number of pods in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable pods that the scheduler never attempted to schedule because they are gated; 'incomplete' means number of pods in incompletePodGroupPods; 'pending' means number of pods in pendingPodGroupPods.
# TYPE scheduler_pending_pods gauge
scheduler_pending_pods{queue="active"} 30
scheduler_pending_pods{queue="backoff"} 0
scheduler_pending_pods{queue="gated"} 10
scheduler_pending_pods{queue="incomplete"} 0
scheduler_pending_pods{queue="pending"} 0
scheduler_pending_pods{queue="unschedulable"} 20
`,
		},
		{
			name: "add pods to all kinds of queues",
			operations: []operation{
				addPodActiveQ,
				addPodBackoffQ,
				addPodUnschedulablePods,
			},
			operands: [][]*framework.QueuedPodInfo{
				pInfos[:15],
				pInfos[15:40],
				pInfos[40:],
			},
			metricsName: "scheduler_pending_pods",
			wants: `
# HELP scheduler_pending_pods [STABLE] Number of pending pods, by the queue type. 'active' means number of pods in activeQ; 'backoff' means number of pods in backoffQ; 'unschedulable' means number of pods in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable pods that the scheduler never attempted to schedule because they are gated; 'incomplete' means number of pods in incompletePodGroupPods; 'pending' means number of pods in pendingPodGroupPods.
# TYPE scheduler_pending_pods gauge
scheduler_pending_pods{queue="active"} 15
scheduler_pending_pods{queue="backoff"} 25
scheduler_pending_pods{queue="gated"} 10
scheduler_pending_pods{queue="incomplete"} 0
scheduler_pending_pods{queue="pending"} 0
scheduler_pending_pods{queue="unschedulable"} 10
`,
		},
		{
			name: "add pods to unschedulableEntities and then move all to activeQ",
			operations: []operation{
				addPodUnschedulablePods,
				moveClockForward,
				moveAllToActiveOrBackoffQ,
			},
			operands: [][]*framework.QueuedPodInfo{
				pInfos[:total],
				{nil},
				{nil},
			},
			metricsName: "scheduler_pending_pods",
			wants: `
# HELP scheduler_pending_pods [STABLE] Number of pending pods, by the queue type. 'active' means number of pods in activeQ; 'backoff' means number of pods in backoffQ; 'unschedulable' means number of pods in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable pods that the scheduler never attempted to schedule because they are gated; 'incomplete' means number of pods in incompletePodGroupPods; 'pending' means number of pods in pendingPodGroupPods.
# TYPE scheduler_pending_pods gauge
scheduler_pending_pods{queue="active"} 50
scheduler_pending_pods{queue="backoff"} 0
scheduler_pending_pods{queue="gated"} 10
scheduler_pending_pods{queue="incomplete"} 0
scheduler_pending_pods{queue="pending"} 0
scheduler_pending_pods{queue="unschedulable"} 0
`,
		},
		{
			name: "make some pods subject to backoff, add pods to unschedulableEntities, and then move all to activeQ",
			operations: []operation{
				addPodUnschedulablePods,
				moveClockForward,
				addPodUnschedulablePods,
				moveAllToActiveOrBackoffQ,
			},
			operands: [][]*framework.QueuedPodInfo{
				pInfos[20:total],
				{nil},
				pInfosWithDelay[:20],
				{nil},
			},
			metricsName: "scheduler_pending_pods",
			wants: `
# HELP scheduler_pending_pods [STABLE] Number of pending pods, by the queue type. 'active' means number of pods in activeQ; 'backoff' means number of pods in backoffQ; 'unschedulable' means number of pods in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable pods that the scheduler never attempted to schedule because they are gated; 'incomplete' means number of pods in incompletePodGroupPods; 'pending' means number of pods in pendingPodGroupPods.
# TYPE scheduler_pending_pods gauge
scheduler_pending_pods{queue="active"} 30
scheduler_pending_pods{queue="backoff"} 20
scheduler_pending_pods{queue="gated"} 10
scheduler_pending_pods{queue="incomplete"} 0
scheduler_pending_pods{queue="pending"} 0
scheduler_pending_pods{queue="unschedulable"} 0
`,
		},
		{
			name: "make some pods subject to backoff, add pods to unschedulableEntities/activeQ, move all to activeQ, and finally flush backoffQ",
			operations: []operation{
				addPodUnschedulablePods,
				addPodActiveQ,
				moveAllToActiveOrBackoffQ,
				flushBackoffQ,
			},
			operands: [][]*framework.QueuedPodInfo{
				pInfos[:40],
				pInfos[40:50],
				{nil},
				{nil},
			},
			metricsName: "scheduler_pending_pods",
			wants: `
# HELP scheduler_pending_pods [STABLE] Number of pending pods, by the queue type. 'active' means number of pods in activeQ; 'backoff' means number of pods in backoffQ; 'unschedulable' means number of pods in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable pods that the scheduler never attempted to schedule because they are gated; 'incomplete' means number of pods in incompletePodGroupPods; 'pending' means number of pods in pendingPodGroupPods.
# TYPE scheduler_pending_pods gauge
scheduler_pending_pods{queue="active"} 50
scheduler_pending_pods{queue="backoff"} 0
scheduler_pending_pods{queue="gated"} 0
scheduler_pending_pods{queue="incomplete"} 0
scheduler_pending_pods{queue="pending"} 0
scheduler_pending_pods{queue="unschedulable"} 0
`,
		},
		{
			name: "add pods to activeQ/unschedulableEntities and then delete some Pods",
			operations: []operation{
				addPodActiveQ,
				addPodUnschedulablePods,
				deletePod,
				deletePod,
				deletePod,
			},
			operands: [][]*framework.QueuedPodInfo{
				pInfos[:30],
				pInfos[30:],
				pInfos[:2],
				pInfos[30:33],
				pInfos[50:54],
			},
			metricsName: "scheduler_pending_pods",
			wants: `
# HELP scheduler_pending_pods [STABLE] Number of pending pods, by the queue type. 'active' means number of pods in activeQ; 'backoff' means number of pods in backoffQ; 'unschedulable' means number of pods in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable pods that the scheduler never attempted to schedule because they are gated; 'incomplete' means number of pods in incompletePodGroupPods; 'pending' means number of pods in pendingPodGroupPods.
# TYPE scheduler_pending_pods gauge
scheduler_pending_pods{queue="active"} 28
scheduler_pending_pods{queue="backoff"} 0
scheduler_pending_pods{queue="gated"} 6
scheduler_pending_pods{queue="incomplete"} 0
scheduler_pending_pods{queue="pending"} 0
scheduler_pending_pods{queue="unschedulable"} 17
`,
		},
		{
			name: "add pods to activeQ/unschedulableEntities and then update some Pods as queueable",
			operations: []operation{
				addPodActiveQ,
				addPodUnschedulablePods,
				updatePodQueueable,
			},
			operands: [][]*framework.QueuedPodInfo{
				pInfos[:30],
				pInfos[30:],
				pInfos[50:55],
			},
			metricsName: "scheduler_pending_pods",
			wants: `
# HELP scheduler_pending_pods [STABLE] Number of pending pods, by the queue type. 'active' means number of pods in activeQ; 'backoff' means number of pods in backoffQ; 'unschedulable' means number of pods in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable pods that the scheduler never attempted to schedule because they are gated; 'incomplete' means number of pods in incompletePodGroupPods; 'pending' means number of pods in pendingPodGroupPods.
# TYPE scheduler_pending_pods gauge
scheduler_pending_pods{queue="active"} 35
scheduler_pending_pods{queue="backoff"} 0
scheduler_pending_pods{queue="gated"} 5
scheduler_pending_pods{queue="incomplete"} 0
scheduler_pending_pods{queue="pending"} 0
scheduler_pending_pods{queue="unschedulable"} 20
`,
		},
		{
			name: "the metrics should not be recorded (pluginMetricsSamplePercent=0)",
			operations: []operation{
				add,
			},
			operands: [][]*framework.QueuedPodInfo{
				pInfos[:1],
			},
			metricsName:                "scheduler_plugin_execution_duration_seconds",
			pluginMetricsSamplePercent: 0,
			wants: `
# HELP scheduler_plugin_execution_duration_seconds [BETA] Duration for running a plugin at a specific extension point.
# TYPE scheduler_plugin_execution_duration_seconds histogram
`, // 观测值始终为 0，因为未推进 fake clock。
		},
		{
			name: "the metrics should be recorded (pluginMetricsSamplePercent=100)",
			operations: []operation{
				add,
			},
			operands: [][]*framework.QueuedPodInfo{
				pInfos[:1],
			},
			metricsName:                "scheduler_plugin_execution_duration_seconds",
			pluginMetricsSamplePercent: 100,
			wants: `
# HELP scheduler_plugin_execution_duration_seconds [BETA] Duration for running a plugin at a specific extension point.
# TYPE scheduler_plugin_execution_duration_seconds histogram
scheduler_plugin_execution_duration_seconds_bucket{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success",le="1e-05"} 1
scheduler_plugin_execution_duration_seconds_bucket{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success",le="1.5000000000000002e-05"} 1
scheduler_plugin_execution_duration_seconds_bucket{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success",le="2.2500000000000005e-05"} 1
scheduler_plugin_execution_duration_seconds_bucket{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success",le="3.375000000000001e-05"} 1
scheduler_plugin_execution_duration_seconds_bucket{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success",le="5.062500000000001e-05"} 1
scheduler_plugin_execution_duration_seconds_bucket{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success",le="7.593750000000002e-05"} 1
scheduler_plugin_execution_duration_seconds_bucket{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success",le="0.00011390625000000003"} 1
scheduler_plugin_execution_duration_seconds_bucket{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success",le="0.00017085937500000006"} 1
scheduler_plugin_execution_duration_seconds_bucket{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success",le="0.0002562890625000001"} 1
scheduler_plugin_execution_duration_seconds_bucket{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success",le="0.00038443359375000017"} 1
scheduler_plugin_execution_duration_seconds_bucket{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success",le="0.0005766503906250003"} 1
scheduler_plugin_execution_duration_seconds_bucket{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success",le="0.0008649755859375004"} 1
scheduler_plugin_execution_duration_seconds_bucket{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success",le="0.0012974633789062506"} 1
scheduler_plugin_execution_duration_seconds_bucket{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success",le="0.0019461950683593758"} 1
scheduler_plugin_execution_duration_seconds_bucket{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success",le="0.0029192926025390638"} 1
scheduler_plugin_execution_duration_seconds_bucket{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success",le="0.004378938903808595"} 1
scheduler_plugin_execution_duration_seconds_bucket{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success",le="0.006568408355712893"} 1
scheduler_plugin_execution_duration_seconds_bucket{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success",le="0.009852612533569338"} 1
scheduler_plugin_execution_duration_seconds_bucket{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success",le="0.014778918800354007"} 1
scheduler_plugin_execution_duration_seconds_bucket{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success",le="0.02216837820053101"} 1
scheduler_plugin_execution_duration_seconds_bucket{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success",le="+Inf"} 1
scheduler_plugin_execution_duration_seconds_sum{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success"} 0
scheduler_plugin_execution_duration_seconds_count{extension_point="PreEnqueue",plugin="preEnqueuePlugin",status="Success"} 1
`, // 观测值始终为 0，因为未推进 fake clock。
		},
		{
			name: "Gated metric should be 1 when Ungated to Gated transition into moveToActiveQ",
			operations: []operation{
				addPodUnschedulablePods,
				moveClockForward,
				updatePluginToGateAllPods,
				updatePodQueueable,
			},
			operands: [][]*framework.QueuedPodInfo{
				pInfos[:1],
				{nil},
				{nil},
				pInfos[:1],
			},
			metricsName:                "scheduler_pending_pods",
			pluginMetricsSamplePercent: 100,
			wants: `
# HELP scheduler_pending_pods [STABLE] Number of pending pods, by the queue type. 'active' means number of pods in activeQ; 'backoff' means number of pods in backoffQ; 'unschedulable' means number of pods in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable pods that the scheduler never attempted to schedule because they are gated; 'incomplete' means number of pods in incompletePodGroupPods; 'pending' means number of pods in pendingPodGroupPods.
# TYPE scheduler_pending_pods gauge
scheduler_pending_pods{queue="active"} 0
scheduler_pending_pods{queue="backoff"} 0
scheduler_pending_pods{queue="gated"} 1
scheduler_pending_pods{queue="incomplete"} 0
scheduler_pending_pods{queue="pending"} 0
scheduler_pending_pods{queue="unschedulable"} 0
`,
		},
		{
			name: "Gated metric should be 1 when Ungated to Gated transition into moveToBackoffQ",
			operations: []operation{
				addPodUnschedulablePods,
				updatePluginToGateAllPods,
				updatePodQueueable,
			},
			operands: [][]*framework.QueuedPodInfo{
				pInfos[:1],
				{nil},
				pInfos[:1],
			},
			metricsName:                "scheduler_pending_pods",
			pluginMetricsSamplePercent: 100,
			wants: `
# HELP scheduler_pending_pods [STABLE] Number of pending pods, by the queue type. 'active' means number of pods in activeQ; 'backoff' means number of pods in backoffQ; 'unschedulable' means number of pods in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable pods that the scheduler never attempted to schedule because they are gated; 'incomplete' means number of pods in incompletePodGroupPods; 'pending' means number of pods in pendingPodGroupPods.
# TYPE scheduler_pending_pods gauge
scheduler_pending_pods{queue="active"} 0
scheduler_pending_pods{queue="backoff"} 0
scheduler_pending_pods{queue="gated"} 1
scheduler_pending_pods{queue="incomplete"} 0
scheduler_pending_pods{queue="pending"} 0
scheduler_pending_pods{queue="unschedulable"} 0
`,
		},
		{
			name: "Gated metric should be 1 when Ungated to Gated transition when popFromBackoffQ is disabled",
			operations: []operation{
				addPodUnschedulablePods,
				moveClockForward,
				updatePluginToGateAllPods,
				updatePodQueueable,
			},
			operands: [][]*framework.QueuedPodInfo{
				pInfos[:1],
				{nil},
				{nil},
				pInfos[:1],
			},
			metricsName:                "scheduler_pending_pods",
			pluginMetricsSamplePercent: 100,
			disablePopFromBackoffQ:     true,
			wants: `
# HELP scheduler_pending_pods [STABLE] Number of pending pods, by the queue type. 'active' means number of pods in activeQ; 'backoff' means number of pods in backoffQ; 'unschedulable' means number of pods in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable pods that the scheduler never attempted to schedule because they are gated; 'incomplete' means number of pods in incompletePodGroupPods; 'pending' means number of pods in pendingPodGroupPods.
# TYPE scheduler_pending_pods gauge
scheduler_pending_pods{queue="active"} 0
scheduler_pending_pods{queue="backoff"} 0
scheduler_pending_pods{queue="gated"} 1
scheduler_pending_pods{queue="incomplete"} 0
scheduler_pending_pods{queue="pending"} 0
scheduler_pending_pods{queue="unschedulable"} 0
`,
		},
		{
			name: "Gated metric should be 0 when Ungated -> Gated -> Ungated (ActiveQ) transition",
			operations: []operation{
				addPodUnschedulablePods,
				moveClockForward,
				updatePluginToGateAllPods,
				updatePodQueueable,
				updatePluginToUngateAllPods,
				updatePodQueueable,
			},
			operands: [][]*framework.QueuedPodInfo{
				pInfos[:1],
				{nil},
				{nil},
				pInfos[:1],
				{nil},
				pInfos[:1],
			},
			pluginMetricsSamplePercent: 100,
			metricsName:                "scheduler_pending_pods",
			wants: `
# HELP scheduler_pending_pods [STABLE] Number of pending pods, by the queue type. 'active' means number of pods in activeQ; 'backoff' means number of pods in backoffQ; 'unschedulable' means number of pods in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable pods that the scheduler never attempted to schedule because they are gated; 'incomplete' means number of pods in incompletePodGroupPods; 'pending' means number of pods in pendingPodGroupPods.
# TYPE scheduler_pending_pods gauge
scheduler_pending_pods{queue="active"} 1
scheduler_pending_pods{queue="backoff"} 0
scheduler_pending_pods{queue="gated"} 0
scheduler_pending_pods{queue="incomplete"} 0
scheduler_pending_pods{queue="pending"} 0
scheduler_pending_pods{queue="unschedulable"} 0
`,
		},
		{
			name: "Gated metric should be 0 when Ungated -> Gated -> Ungated (BackoffQ) transition",
			operations: []operation{
				addPodUnschedulablePods,
				updatePluginToGateAllPods,
				updatePodQueueable,
				updatePluginToUngateAllPods,
				updatePodQueueable,
			},
			operands: [][]*framework.QueuedPodInfo{
				pInfos[:1],
				{nil},
				pInfos[:1],
				{nil},
				pInfos[:1],
			},
			pluginMetricsSamplePercent: 100,
			metricsName:                "scheduler_pending_pods",
			wants: `
# HELP scheduler_pending_pods [STABLE] Number of pending pods, by the queue type. 'active' means number of pods in activeQ; 'backoff' means number of pods in backoffQ; 'unschedulable' means number of pods in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable pods that the scheduler never attempted to schedule because they are gated; 'incomplete' means number of pods in incompletePodGroupPods; 'pending' means number of pods in pendingPodGroupPods.
# TYPE scheduler_pending_pods gauge
scheduler_pending_pods{queue="active"} 0
scheduler_pending_pods{queue="backoff"} 1
scheduler_pending_pods{queue="gated"} 0
scheduler_pending_pods{queue="incomplete"} 0
scheduler_pending_pods{queue="pending"} 0
scheduler_pending_pods{queue="unschedulable"} 0
`,
		},
		{
			name: "Gated metric should be 0 when Ungated -> Gated -> Ungated transition, when popFromBackoffQ is disabled",
			operations: []operation{
				addPodUnschedulablePods,
				moveClockForward,
				updatePluginToGateAllPods,
				updatePodQueueable,
				updatePluginToUngateAllPods,
				updatePodQueueable,
			},
			operands: [][]*framework.QueuedPodInfo{
				pInfos[:1],
				{nil},
				{nil},
				pInfos[:1],
				{nil},
				pInfos[:1],
			},
			pluginMetricsSamplePercent: 100,
			disablePopFromBackoffQ:     true,
			wants: `
# HELP scheduler_pending_pods [STABLE] Number of pending pods, by the queue type. 'active' means number of pods in activeQ; 'backoff' means number of pods in backoffQ; 'unschedulable' means number of pods in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable pods that the scheduler never attempted to schedule because they are gated; 'incomplete' means number of pods in incompletePodGroupPods; 'pending' means number of pods in pendingPodGroupPods.
# TYPE scheduler_pending_pods gauge
scheduler_pending_pods{queue="active"} 1
scheduler_pending_pods{queue="backoff"} 0
scheduler_pending_pods{queue="gated"} 0
scheduler_pending_pods{queue="incomplete"} 0
scheduler_pending_pods{queue="pending"} 0
scheduler_pending_pods{queue="unschedulable"} 0
`,
		},
		{
			name: "adding a pod that belongs to a podgroup not yet observed by scheduler ends up in incompletePodGroupPods",
			operations: []operation{
				addPodActiveQ,
			},
			operands: [][]*framework.QueuedPodInfo{
				{pInfoWithPgIncomplete},
			},
			wants: `
# HELP scheduler_pending_pods [STABLE] Number of pending pods, by the queue type. 'active' means number of pods in activeQ; 'backoff' means number of pods in backoffQ; 'unschedulable' means number of pods in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable pods that the scheduler never attempted to schedule because they are gated; 'incomplete' means number of pods in incompletePodGroupPods; 'pending' means number of pods in pendingPodGroupPods.
# TYPE scheduler_pending_pods gauge
scheduler_pending_pods{queue="active"} 0
scheduler_pending_pods{queue="backoff"} 0
scheduler_pending_pods{queue="gated"} 0
scheduler_pending_pods{queue="incomplete"} 1
scheduler_pending_pods{queue="pending"} 0
scheduler_pending_pods{queue="unschedulable"} 0
`,
		},
		{
			name: "adding a pod that belongs to a popped podgroup ends up in pendingPodGroupPods",
			operations: []operation{
				addPodActiveQ,
				addPodGroupForPod,
				pop,
				addPodActiveQ,
			},
			operands: [][]*framework.QueuedPodInfo{
				{pInfo1},
				{pInfo1},
				{nil},
				{pInfo2},
			},
			wants: `
# HELP scheduler_pending_pods [STABLE] Number of pending pods, by the queue type. 'active' means number of pods in activeQ; 'backoff' means number of pods in backoffQ; 'unschedulable' means number of pods in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable pods that the scheduler never attempted to schedule because they are gated; 'incomplete' means number of pods in incompletePodGroupPods; 'pending' means number of pods in pendingPodGroupPods.
# TYPE scheduler_pending_pods gauge
scheduler_pending_pods{queue="active"} 0
scheduler_pending_pods{queue="backoff"} 0
scheduler_pending_pods{queue="gated"} 0
scheduler_pending_pods{queue="incomplete"} 0
scheduler_pending_pods{queue="pending"} 1
scheduler_pending_pods{queue="unschedulable"} 0
`,
		},
	}

	resetMetrics := func() {
		metrics.ActivePods().Set(0)
		metrics.BackoffPods().Set(0)
		metrics.UnschedulablePods().Set(0)
		metrics.GatedPods().Set(0)
		metrics.IncompletePodGroupPods().Set(0)
		metrics.PendingPodGroupPods().Set(0)
		metrics.PluginExecutionDuration.Reset()
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			resetMetrics()
			resetPodInfos()

			m := makeEmptyQueueingHintMapPerProfile()
			m[""][framework.EventTargetPodUpdate] = []*QueueingHintFunction{
				{
					PluginName:     preenqueuePluginName,
					QueueingHintFn: queueHintReturnQueue,
				},
			}
			preenq := map[string]map[string]fwk.PreEnqueuePlugin{"": {(&preEnqueuePlugin{}).Name(): &preEnqueuePlugin{allowlists: []string{queueable}}}}
			recorderCtx, stopRecorder := context.WithCancel(tCtx)
			recorder := metrics.NewMetricsAsyncRecorder(3, 20*time.Microsecond, recorderCtx.Done())
			queue := NewTestQueue(tCtx, newDefaultQueueSort(), WithClock(testingclock.NewFakeClock(timestamp)), WithPreEnqueuePluginMap(preenq), WithPluginMetricsSamplePercent(test.pluginMetricsSamplePercent), WithMetricsRecorder(recorder), WithQueueingHintMapPerProfile(m))
			queue.isPopFromBackoffQEnabled = !test.disablePopFromBackoffQ
			for i, op := range test.operations {
				for _, pInfo := range test.operands[i] {
					op(tCtx, queue, pInfo)
				}
			}

			stopRecorder()
			<-recorder.IsStoppedCh
			recorder.FlushMetrics()

			if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(test.wants), test.metricsName); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestPerPodSchedulingMetrics 确保多次 Add/Pop 时 Pod 调度尝试次数正确更新，
// 多次 Add/Pop 期间 initialAttemptTimestamp 保持不变。
func TestPerPodSchedulingMetrics(t *testing.T) {
	timestamp := time.Now()

	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tests := []struct {
		name                            string
		perPodSchedulingMetricsScenario func(*testingclock.FakeClock, *PriorityQueue, *v1.Pod)
		wantAttempts                    int
		wantInitialAttemptTs            time.Time
	}{
		{
			// 队列操作依次为 Add -> Pop。
			name: "pod is created and scheduled after 1 attempt",
			perPodSchedulingMetricsScenario: func(c *testingclock.FakeClock, queue *PriorityQueue, pod *v1.Pod) {
				queue.Add(ctx, pod)
			},
			wantAttempts:         1,
			wantInitialAttemptTs: timestamp,
		},
		{
			// 队列操作依次为 Add -> Pop -> AddUnschedulablePodIfNotPresent -> flushUnschedulableEntitiesLeftover -> Pop。
			name: "pod is created and scheduled after 2 attempts",
			perPodSchedulingMetricsScenario: func(c *testingclock.FakeClock, queue *PriorityQueue, pod *v1.Pod) {
				queue.Add(ctx, pod)
				entity, err := queue.Pop(logger)
				pInfo := entity.(*framework.QueuedPodInfo)
				if err != nil {
					t.Fatalf("Failed to pop a pod %v", err)
				}

				pInfo.UnschedulablePlugins = sets.New("plugin")
				err = queue.AddUnschedulablePodIfNotPresent(logger, pInfo, 1)
				if err != nil {
					t.Fatalf("Failed to add unschedulable pod %v", err)
				}
				// 把时钟推进超过 DefaultPodMaxInUnschedulablePodsDuration，使不可调度 Pod
				// 将被移入 activeQ
				c.SetTime(timestamp.Add(DefaultPodMaxInUnschedulablePodsDuration + 1))
				queue.flushUnschedulableEntitiesLeftover(logger)
			},
			wantAttempts:         2,
			wantInitialAttemptTs: timestamp,
		},
		{
			// 队列操作依次为 Add -> Pop -> AddUnschedulablePodIfNotPresent -> flushUnschedulableEntitiesLeftover -> Update -> Pop。
			name: "pod is created and scheduled after 2 attempts but before the second pop, call update",
			perPodSchedulingMetricsScenario: func(c *testingclock.FakeClock, queue *PriorityQueue, pod *v1.Pod) {
				queue.Add(ctx, pod)
				entity, err := queue.Pop(logger)
				pInfo := entity.(*framework.QueuedPodInfo)
				if err != nil {
					t.Fatalf("Failed to pop a pod %v", err)
				}

				pInfo.UnschedulablePlugins = sets.New("plugin")
				err = queue.AddUnschedulablePodIfNotPresent(logger, pInfo, 1)
				if err != nil {
					t.Fatalf("Failed to add unschedulable pod %v", err)
				}
				// 把时钟推进超过 DefaultPodMaxInUnschedulablePodsDuration，使不可调度 Pod
				// 将被移入 activeQ
				updatedTimestamp := timestamp
				c.SetTime(updatedTimestamp.Add(DefaultPodMaxInUnschedulablePodsDuration + 1))
				queue.flushUnschedulableEntitiesLeftover(logger)
				newPod := pod.DeepCopy()
				newPod.Generation = 1
				queue.Update(ctx, pod, newPod)
			},
			wantAttempts:         2,
			wantInitialAttemptTs: timestamp,
		},
		{
			// 队列操作依次为 Add 带 gate 的 Pod -> 检查 unschedulableEntities -> 解除 gate 并更新 Pod -> Pop。
			name: "A gated pod is created and scheduled after lifting gate",
			perPodSchedulingMetricsScenario: func(c *testingclock.FakeClock, queue *PriorityQueue, pod *v1.Pod) {
				// 创建一个带 PreEnqueuePlugin 的队列
				queue.preEnqueuePluginMap = map[string]map[string]fwk.PreEnqueuePlugin{"": {(&preEnqueuePlugin{}).Name(): &preEnqueuePlugin{allowlists: []string{"foo"}}}}
				queue.pluginMetricsSamplePercent = 0
				queue.Add(ctx, pod)
				// 检查 Pod 是否已加入 unschedulableEntities 队列。
				if diff := cmp.Diff(pod, getUnschedulablePod(queue, pod)); diff != "" {
					t.Errorf("Unexpected pod in unschedulableEntities (-want, +got):\n%s", diff)
				}
				// 覆盖时钟以获得不同的 InitialAttemptTimestamp
				c.Step(1 * time.Minute)

				// 用所需的标签更新 Pod，使其离开 unschedulableEntities 队列。
				updateGatedPod := pod.DeepCopy()
				updateGatedPod.Labels = map[string]string{"foo": ""}
				queue.Update(ctx, pod, updateGatedPod)
			},
			wantAttempts:         1,
			wantInitialAttemptTs: timestamp.Add(1 * time.Minute),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			c := testingclock.NewFakeClock(timestamp)
			pod := st.MakePod().Name("test-pod").Namespace("test-ns").UID("test-uid").Obj()
			queue := NewTestQueue(ctx, newDefaultQueueSort(), WithClock(c))

			test.perPodSchedulingMetricsScenario(c, queue, pod)
			entity, err := queue.Pop(logger)
			if err != nil {
				t.Fatal(err)
			}
			if entity.GetAttempts() != test.wantAttempts {
				t.Errorf("Pod schedule attempt unexpected, got %v, want %v", entity.GetAttempts(), test.wantAttempts)
			}
			if *entity.GetInitialAttemptTimestamp() != test.wantInitialAttemptTs {
				t.Errorf("Pod initial schedule attempt timestamp unexpected, got %v, want %v", *entity.GetInitialAttemptTimestamp(), test.wantInitialAttemptTs)
			}
		})
	}
}

// TestIncomingPodsMetrics 验证新到达 Pod 相关的 Prometheus 指标。
func TestIncomingPodsMetrics(t *testing.T) {
	timestamp := time.Now()
	unschedulablePlg := "unschedulable_plugin"
	var pInfos = make([]*framework.QueuedPodInfo, 0, 3)

	for i := 1; i <= 3; i++ {
		p := &framework.QueuedPodInfo{
			PodInfo: mustNewTestPodInfo(t,
				st.MakePod().Name(fmt.Sprintf("test-pod-%d", i)).Namespace(fmt.Sprintf("ns%d", i)).UID(fmt.Sprintf("tp-%d", i)).Obj()),
			QueueingParams: framework.QueueingParams{
				Timestamp:            timestamp,
				UnschedulablePlugins: sets.New(unschedulablePlg),
			},
		}
		pInfos = append(pInfos, p)
	}
	tests := []struct {
		name       string
		operations []operation
		want       string
	}{
		{
			name: "add pods to activeQ",
			operations: []operation{
				add,
			},
			want: `
            scheduler_queue_incoming_pods_total{event="UnscheduledPodAdd",queue="active"} 3
`,
		},
		{
			name: "add unscheduled pods then make them unschedulable",
			operations: []operation{
				popAndRequeueAsUnschedulable,
			},
			want: `scheduler_queue_incoming_pods_total{event="UnscheduledPodAdd",queue="active"} 3
             scheduler_queue_incoming_pods_total{event="ScheduleAttemptFailure",queue="unschedulable"} 3
`,
		},
		{
			name: "add unscheduled pods, make them unschedulable, and move them to backoffQ",
			operations: []operation{
				popAndRequeueAsUnschedulable,
				moveAllToActiveOrBackoffQ,
			},
			want: `scheduler_queue_incoming_pods_total{event="UnscheduledPodAdd",queue="active"} 3
			scheduler_queue_incoming_pods_total{event="ScheduleAttemptFailure",queue="unschedulable"} 3
            scheduler_queue_incoming_pods_total{event="UnschedulableTimeout",queue="backoff"} 3
`,
		},
		{
			name: "add unscheduled pods, make them unschedulable, and move them to activeQ",
			operations: []operation{
				popAndRequeueAsUnschedulable,
				moveClockForward,
				moveAllToActiveOrBackoffQ,
			},
			want: `scheduler_queue_incoming_pods_total{event="UnscheduledPodAdd",queue="active"} 3
			scheduler_queue_incoming_pods_total{event="ScheduleAttemptFailure",queue="unschedulable"} 3
            scheduler_queue_incoming_pods_total{event="UnschedulableTimeout",queue="active"} 3
`,
		},
		{
			name: "make some pods subject to backoff and add them to backoffQ, then flush backoffQ",
			operations: []operation{
				popAndRequeueAsBackoff,
				moveClockForward,
				flushBackoffQ,
			},
			want: `scheduler_queue_incoming_pods_total{event="UnscheduledPodAdd",queue="active"} 3
			scheduler_queue_incoming_pods_total{event="BackoffComplete",queue="active"} 3
            scheduler_queue_incoming_pods_total{event="ScheduleAttemptFailure",queue="backoff"} 3
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			metrics.SchedulerQueueIncomingPods.Reset()
			queue := NewTestQueue(tCtx, newDefaultQueueSort(), WithClock(testingclock.NewFakeClock(timestamp)))
			for _, op := range test.operations {
				for _, pInfo := range pInfos {
					op(tCtx, queue, pInfo)
				}
			}
			metricName := metrics.SchedulerSubsystem + "_" + metrics.SchedulerQueueIncomingPods.Name
			if err := testutil.CollectAndCompare(metrics.SchedulerQueueIncomingPods, strings.NewReader(queueMetricMetadata+test.want), metricName); err != nil {
				t.Errorf("unexpected collecting result:\n%s", err)
			}

		})
	}
}

// TestIncomingEntitiesMetrics 验证新到达实体相关的 Prometheus 指标。
func TestIncomingEntitiesMetrics(t *testing.T) {
	logger := klog.Background()
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.GenericWorkload, true)
	timestamp := time.Now()

	unschedulablePlugin := "unschedulable_plugin"
	queuingParams := framework.QueueingParams{
		Timestamp:            timestamp,
		UnschedulablePlugins: sets.New(unschedulablePlugin),
	}

	pInfos := []*framework.QueuedPodInfo{
		makeQueuedPodInfo(t, "pod1", "ns-pg", queuingParams),
		makeQueuedPodInfo(t, "pod2", "ns-pg", queuingParams, withPodGroup("pg-1")),
		makeQueuedPodInfo(t, "pod3", "ns-pg", queuingParams, withPodGroup("pg-1")),
		makeQueuedPodInfo(t, "pod4", "ns-pg", queuingParams, withPodGroup("pg-1")),
	}

	metricName := metrics.SchedulerSubsystem + "_" + metrics.SchedulerQueueIncomingEntities.Name

	tests := []struct {
		name string
		run  func(tCtx ktesting.TContext, queue *PriorityQueue)
		want string
	}{
		{
			name: "add all pods to active queue",
			run: func(tCtx ktesting.TContext, queue *PriorityQueue) {
				for _, pInfo := range pInfos {
					queue.Add(tCtx, pInfo.Pod)
				}
			},
			want: `
				scheduler_queue_incoming_entities_total{event="UnscheduledPodAdd",queue="active",type="pod"} 1
				scheduler_queue_incoming_entities_total{event="UnscheduledPodAdd",queue="active",type="podgroup"} 1
			`,
		},
		{
			name: "add pods of a podgroup to active queue, simulate scheduling failure attempt, and requeue podgroup to backoff queue",
			run: func(tCtx ktesting.TContext, queue *PriorityQueue) {
				queue.Add(tCtx, pInfos[1].Pod)
				queue.Add(tCtx, pInfos[2].Pod)
				queue.Add(tCtx, pInfos[3].Pod)
				entityGroup, err := queue.Pop(logger)
				if err != nil {
					tCtx.Fatalf("Unexpected error popping from queue: %v", err)
				}
				pgInfo := entityGroup.(*framework.QueuedPodGroupInfo)
				queue.pendingPodGroupPods.add(pInfos[2])
				queue.pendingPodGroupPods.add(pInfos[3])
				if err := queue.AddAttemptedPodGroupIfNeeded(logger, pgInfo, 1, fwk.NewStatus(fwk.Unschedulable)); err != nil {
					tCtx.Fatalf("Unexpected error adding attempted pod group: %v", err)
				}
			},
			want: `
				scheduler_queue_incoming_entities_total{event="UnscheduledPodAdd",queue="active",type="podgroup"} 1
				scheduler_queue_incoming_entities_total{event="ScheduleAttemptFailure",queue="backoff",type="podgroup"} 1
			`,
		},
		{
			name: "add individual pod to active queue",
			run: func(tCtx ktesting.TContext, queue *PriorityQueue) {
				queue.Add(tCtx, pInfos[0].Pod)
				entity, err := queue.Pop(logger)
				if err != nil {
					tCtx.Fatalf("Unexpected error popping from queue: %v", err)
				}
				pod := entity.(*framework.QueuedPodInfo)
				pod.UnschedulablePlugins = sets.New(unschedulablePlugin)
				if err := queue.AddUnschedulablePodIfNotPresent(logger, pod, 1); err != nil {
					tCtx.Fatalf("Unexpected error adding unschedulable pod: %v", err)
				}
			},
			want: `
				scheduler_queue_incoming_entities_total{event="UnscheduledPodAdd",queue="active",type="pod"} 1
			`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			metrics.SchedulerQueueIncomingEntities.Reset()
			queue := NewTestQueue(tCtx, newDefaultQueueSort(), WithClock(testingclock.NewFakeClock(timestamp)))
			queue.AddPodGroup(logger, st.MakePodGroup().Name("pg-1").Namespace("ns-pg").Obj())
			test.run(tCtx, queue)
			if err := testutil.CollectAndCompare(metrics.SchedulerQueueIncomingEntities, strings.NewReader(queueIncomingEntitiesMetricMetadata+test.want), metricName); err != nil {
				t.Errorf("unexpected collecting result:\n%s", err)
			}
		})
	}
}

// TestQueuedEntitiesMetrics 验证各队列中实体数量的 Prometheus 指标。
func TestQueuedEntitiesMetrics(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.GenericWorkload, true)
	timestamp := time.Now()
	unschedulablePlugin := "unschedulable_plugin"
	logger := klog.Background()
	queuingParams := framework.QueueingParams{
		Timestamp:            timestamp,
		UnschedulablePlugins: sets.New(unschedulablePlugin),
	}

	pInfos := []*framework.QueuedPodInfo{
		makeQueuedPodInfo(t, "pod1", "ns-pg", queuingParams),
		makeQueuedPodInfo(t, "pod2", "ns-pg", queuingParams, withPodGroup("pg-1")),
		makeQueuedPodInfo(t, "pod3", "ns-pg", queuingParams, withPodGroup("pg-1")),
		makeQueuedPodInfo(t, "pod4", "ns-pg", queuingParams, withPodGroup("pg-1")),
	}

	metricName := metrics.SchedulerSubsystem + "_" + metrics.QueuedEntities.Name

	tests := []struct {
		name string
		run  func(tCtx ktesting.TContext, queue *PriorityQueue)
		want string
	}{
		{
			name: "add all pods to active queue",
			run: func(tCtx ktesting.TContext, queue *PriorityQueue) {
				for _, pInfo := range pInfos {
					queue.Add(tCtx, pInfo.Pod)
				}
			},
			want: `
				scheduler_queued_entities{queue="active",type="pod"} 1
				scheduler_queued_entities{queue="active",type="podgroup"} 1
			`,
		},
		{
			name: "add pods of a podgroup to active queue, simulate scheduling failure attempt, and requeue podgroup to backoff queue",
			run: func(tCtx ktesting.TContext, queue *PriorityQueue) {
				queue.Add(tCtx, pInfos[1].Pod)
				queue.Add(tCtx, pInfos[2].Pod)
				queue.Add(tCtx, pInfos[3].Pod)
				entityGroup, err := queue.Pop(logger)
				if err != nil {
					tCtx.Fatalf("Unexpected error popping from queue: %v", err)
				}
				pgInfo := entityGroup.(*framework.QueuedPodGroupInfo)
				queue.pendingPodGroupPods.add(pInfos[2])
				queue.pendingPodGroupPods.add(pInfos[3])
				if err := queue.AddAttemptedPodGroupIfNeeded(logger, pgInfo, 1, fwk.NewStatus(fwk.Unschedulable)); err != nil {
					tCtx.Fatalf("Unexpected error adding attempted pod group: %v", err)
				}
			},
			want: `
				scheduler_queued_entities{queue="active",type="podgroup"} 0
				scheduler_queued_entities{queue="backoff",type="podgroup"} 1
			`,
		},
		{
			name: "Add individual pod to active queue, then mark it as unschedulable and add it unschedulable queue",
			run: func(tCtx ktesting.TContext, queue *PriorityQueue) {
				queue.Add(tCtx, pInfos[0].Pod)
				entity, err := queue.Pop(logger)
				if err != nil {
					tCtx.Fatalf("Unexpected error popping from queue: %v", err)
				}
				pod := entity.(*framework.QueuedPodInfo)
				pod.UnschedulablePlugins = sets.New(unschedulablePlugin)
				if err := queue.AddUnschedulablePodIfNotPresent(logger, pod, 1); err != nil {
					tCtx.Fatalf("Unexpected error adding unschedulable pod: %v", err)
				}
			},
			want: `
				scheduler_queued_entities{queue="active",type="pod"} 0
				scheduler_queued_entities{queue="unschedulable",type="pod"} 1
			`,
		},
		{
			name: "Add individual pod, mark it as unschedulable, and move it to unschedulable queue then backoff queue",
			run: func(tCtx ktesting.TContext, queue *PriorityQueue) {
				queue.Add(tCtx, pInfos[0].Pod)
				entity, err := queue.Pop(logger)
				if err != nil {
					tCtx.Fatalf("Unexpected error popping from queue: %v", err)
				}
				pod := entity.(*framework.QueuedPodInfo)
				pod.UnschedulablePlugins = sets.New(unschedulablePlugin)
				if err := queue.AddUnschedulablePodIfNotPresent(logger, pod, 1); err != nil {
					tCtx.Fatalf("Unexpected error adding unschedulable pod: %v", err)
				}

				queue.MoveAllToActiveOrBackoffQueue(logger, framework.EventUnschedulableTimeout, nil, nil, nil)
			},
			want: `
				scheduler_queued_entities{queue="active",type="pod"} 0
				scheduler_queued_entities{queue="backoff",type="pod"} 1
				scheduler_queued_entities{queue="unschedulable",type="pod"} 0
			`,
		},
		{
			name: "Add podgroup, mark as unschedulable and move it to unschedulable queue, then to backoff queue",
			run: func(tCtx ktesting.TContext, queue *PriorityQueue) {
				queue.Add(tCtx, pInfos[1].Pod)
				queue.Add(tCtx, pInfos[2].Pod)
				queue.Add(tCtx, pInfos[3].Pod)
				entityGroup, err := queue.Pop(logger)
				if err != nil {
					tCtx.Fatalf("Unexpected error popping from queue: %v", err)
				}
				pgInfo := entityGroup.(*framework.QueuedPodGroupInfo)
				queue.pendingPodGroupPods.add(pInfos[2])
				queue.pendingPodGroupPods.add(pInfos[3])
				pgInfo.UnschedulablePlugins = sets.New(unschedulablePlugin)
				pgInfo.UnschedulableCount = 1
				queue.unschedulableEntities.addOrUpdate(pgInfo, false, framework.ScheduleAttemptFailure, nil)

				queue.MoveAllToActiveOrBackoffQueue(logger, framework.EventUnschedulableTimeout, nil, nil, nil)
			},
			want: `
				scheduler_queued_entities{queue="active",type="podgroup"} 0
				scheduler_queued_entities{queue="backoff",type="podgroup"} 1
				scheduler_queued_entities{queue="unschedulable",type="podgroup"} 0
			`,
		},
		{
			name: "Move a podgroup from backoff queue to active queue",
			run: func(tCtx ktesting.TContext, queue *PriorityQueue) {
				queue.Add(tCtx, pInfos[1].Pod)
				queue.Add(tCtx, pInfos[2].Pod)
				queue.Add(tCtx, pInfos[3].Pod)
				entityGroup, err := queue.Pop(logger)
				if err != nil {
					tCtx.Fatalf("Unexpected error popping from queue: %v", err)
				}
				pgInfo := entityGroup.(*framework.QueuedPodGroupInfo)
				queue.pendingPodGroupPods.add(pInfos[2])
				queue.pendingPodGroupPods.add(pInfos[3])
				pgInfo.UnschedulablePlugins = sets.New(unschedulablePlugin)
				pgInfo.UnschedulableCount = 1
				queue.unschedulableEntities.addOrUpdate(pgInfo, false, framework.ScheduleAttemptFailure, nil)

				queue.clock.(*testingclock.FakeClock).Step(3 * time.Second)
				queue.MoveAllToActiveOrBackoffQueue(logger, framework.EventUnschedulableTimeout, nil, nil, nil)
			},
			want: `
				scheduler_queued_entities{queue="active",type="podgroup"} 1
				scheduler_queued_entities{queue="unschedulable",type="podgroup"} 0
			`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			metrics.QueuedEntities.Reset()
			queue := NewTestQueue(tCtx, newDefaultQueueSort(), WithClock(testingclock.NewFakeClock(timestamp)))
			queue.AddPodGroup(logger, st.MakePodGroup().Name("pg-1").Namespace("ns-pg").Obj())

			test.run(tCtx, queue)

			if err := testutil.CollectAndCompare(metrics.QueuedEntities, strings.NewReader(queuedEntitiesMetricMetadata+test.want), metricName); err != nil {
				t.Errorf("unexpected collecting result:\n%s", err)
			}
		})
	}
}

// TestBackOffFlow 验证 Pod 在 backoff 队列中的退避与移动流程。
func TestBackOffFlow(t *testing.T) {
	cl := testingclock.NewFakeClock(time.Now())
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	steps := []struct {
		wantBackoff time.Duration
	}{
		{wantBackoff: time.Second},
		{wantBackoff: 2 * time.Second},
		{wantBackoff: 4 * time.Second},
		{wantBackoff: 8 * time.Second},
		{wantBackoff: 10 * time.Second},
		{wantBackoff: 10 * time.Second},
		{wantBackoff: 10 * time.Second},
	}
	for _, popFromBackoffQEnabled := range []bool{true, false} {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SchedulerPopFromBackoffQ, popFromBackoffQEnabled)

		q := NewTestQueue(ctx, newDefaultQueueSort(), WithClock(cl))

		pod := st.MakePod().Name("test-pod").Namespace("test-ns").UID("test-uid").Obj()
		podID := types.NamespacedName{
			Namespace: pod.Namespace,
			Name:      pod.Name,
		}
		q.Add(ctx, pod)

		for i, step := range steps {
			t.Run(fmt.Sprintf("step %d popFromBackoffQEnabled(%v)", i, popFromBackoffQEnabled), func(t *testing.T) {
				timestamp := cl.Now()
				// 模拟一次调度尝试。
				entity, err := q.Pop(logger)
				if err != nil {
					t.Fatal(err)
				}
				podInfo := entity.(*framework.QueuedPodInfo)
				if podInfo.GetAttempts() != i+1 {
					t.Errorf("got attempts %d, want %d", podInfo.GetAttempts(), i+1)
				}
				podInfo.GetUnschedulablePlugins().Insert("unsched-plugin")
				err = q.AddUnschedulablePodIfNotPresent(logger, podInfo, int64(i))
				if err != nil {
					t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
				}

				// 事件发生。
				q.MoveAllToActiveOrBackoffQueue(logger, framework.EventUnschedulableTimeout, nil, nil, nil)

				if !q.backoffQ.has(podInfo) {
					t.Errorf("pod %v is not in the backoff queue", podID)
				}

				// 检查退避时长。
				deadline := podInfo.BackoffExpiration
				backoff := deadline.Sub(timestamp)
				if popFromBackoffQEnabled {
					// 启用 popFromBackoffQ 时，实际退避时间按排序窗口时长向上取整计算。
					backoff = backoff.Truncate(backoffQOrderingWindowDuration) + backoffQOrderingWindowDuration
				}
				if backoff != step.wantBackoff {
					t.Errorf("got backoff %s, want %s", backoff, step.wantBackoff)
				}

				// 模拟持续 flush backoff 队列的例程。
				cl.Step(backoffQOrderingWindowDuration)
				q.flushBackoffQCompleted(logger)
				// 提前 flush 后仍在 backoff 队列中。
				if !q.backoffQ.has(podInfo) {
					t.Errorf("pod %v is not in the backoff queue", podID)
				}
				// 超时后已移出 backoff 队列。
				cl.Step(backoff)
				q.flushBackoffQCompleted(logger)
				if q.backoffQ.has(podInfo) {
					t.Errorf("pod %v is still in the backoff queue", podID)
				}
			})
		}
	}
}

// TestMoveAllToActiveOrBackoffQueue_PreEnqueueChecks 验证 MoveAllToActiveOrBackoffQueue 中 PreEnqueue 检查的行为。
func TestMoveAllToActiveOrBackoffQueue_PreEnqueueChecks(t *testing.T) {
	var podInfos []*framework.QueuedPodInfo
	for i := 0; i < 5; i++ {
		pInfo := newQueuedPodInfoForLookup(
			st.MakePod().Name(fmt.Sprintf("p%d", i)).Priority(int32(i)).Obj(),
		)
		podInfos = append(podInfos, pInfo)
	}

	tests := []struct {
		name            string
		preEnqueueCheck PreEnqueueCheck
		podInfos        []*framework.QueuedPodInfo
		event           fwk.ClusterEvent
		want            sets.Set[string]
	}{
		{
			name:     "nil PreEnqueueCheck",
			podInfos: podInfos,
			event:    framework.EventUnschedulableTimeout,
			want:     sets.New("p0", "p1", "p2", "p3", "p4"),
		},
		{
			name:            "move Pods with priority greater than 2",
			podInfos:        podInfos,
			event:           framework.EventUnschedulableTimeout,
			preEnqueueCheck: func(pod *v1.Pod) bool { return *pod.Spec.Priority >= 2 },
			want:            sets.New("p2", "p3", "p4"),
		},
		{
			name:     "move Pods with even priority and greater than 2",
			podInfos: podInfos,
			event:    framework.EventUnschedulableTimeout,
			preEnqueueCheck: func(pod *v1.Pod) bool {
				return *pod.Spec.Priority%2 == 0 && *pod.Spec.Priority >= 2
			},
			want: sets.New("p2", "p4"),
		},
		{
			name:     "move Pods with even and negative priority",
			podInfos: podInfos,
			event:    framework.EventUnschedulableTimeout,
			preEnqueueCheck: func(pod *v1.Pod) bool {
				return *pod.Spec.Priority%2 == 0 && *pod.Spec.Priority < 0
			},
		},
		{
			name:     "preCheck isn't called if the event is not interested by any plugins",
			podInfos: podInfos,
			event:    pvAdd, // 没有插件关注此事件。
			preEnqueueCheck: func(pod *v1.Pod) bool {
				panic("preCheck shouldn't be called")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := testingclock.NewFakeClock(time.Now().Truncate(backoffQOrderingWindowDuration))
			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			q := NewTestQueue(ctx, newDefaultQueueSort(), WithClock(c))
			for _, podInfo := range tt.podInfos {
				// 为模拟真实世界中 Pod 调度失败，在下面的 AddUnschedulablePodIfNotPresent() 之前先从 activeQ 弹出该 Pod。
				q.Add(ctx, podInfo.Pod)
				if p, err := q.Pop(logger); err != nil {
					t.Errorf("Pop failed: %v", err)
				} else if diff := cmp.Diff(podInfo.Pod, p.(*framework.QueuedPodInfo).Pod); diff != "" {
					t.Errorf("Unexpected pod after Pop (-want, +got):\n%s", diff)
				}
				podInfo.UnschedulablePlugins = sets.New("plugin")
				err := q.AddUnschedulablePodIfNotPresent(logger, podInfo, q.activeQ.schedulingCycle())
				if err != nil {
					t.Fatalf("unexpected error from AddUnschedulablePodIfNotPresent: %v", err)
				}
			}
			q.MoveAllToActiveOrBackoffQueue(logger, tt.event, nil, nil, tt.preEnqueueCheck)
			got := sets.New[string]()
			c.Step(2 * q.backoffQ.podMaxBackoffDuration())
			gotPodInfos := q.backoffQ.popAllBackoffCompleted(logger)
			for _, pInfo := range gotPodInfos {
				got.Insert(pInfo.(*framework.QueuedPodInfo).Pod.Name)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Unexpected diff (-want, +got):\n%s", diff)
			}
		})
	}
}

func makeQueuedPodInfos(num int, namePrefix, label string, timestamp time.Time) []*framework.QueuedPodInfo {
	var pInfos = make([]*framework.QueuedPodInfo, 0, num)
	for i := 1; i <= num; i++ {
		p := &framework.QueuedPodInfo{
			PodInfo: mustNewPodInfo(
				st.MakePod().Name(fmt.Sprintf("%v-%d", namePrefix, i)).Namespace(fmt.Sprintf("ns%d", i)).Label(label, "").UID(fmt.Sprintf("tp-%d", i)).Obj()),
			QueueingParams: framework.QueueingParams{
				Timestamp:            timestamp,
				UnschedulablePlugins: sets.New[string](),
			},
		}
		pInfos = append(pInfos, p)
	}
	return pInfos
}

func mustNewTestPodInfo(t *testing.T, pod *v1.Pod) *framework.PodInfo {
	podInfo, err := framework.NewPodInfo(pod)
	if err != nil {
		t.Fatal(err)
	}
	return podInfo
}

func mustNewPodInfo(pod *v1.Pod) *framework.PodInfo {
	podInfo, err := framework.NewPodInfo(pod)
	if err != nil {
		panic(err)
	}
	return podInfo
}

// Test_isPodWorthRequeuing 测试 isPodWorthRequeuing 函数。
func Test_isPodWorthRequeuing(t *testing.T) {
	metrics.Register()
	count := 0
	queueHintReturnQueue := func(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
		count++
		return fwk.Queue, nil
	}
	queueHintReturnSkip := func(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
		count++
		return fwk.QueueSkip, nil
	}
	queueHintReturnErr := func(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
		count++
		return fwk.QueueSkip, fmt.Errorf("unexpected error")
	}

	tests := []struct {
		name                   string
		podInfo                *framework.QueuedPodInfo
		event                  fwk.ClusterEvent
		oldObj                 interface{}
		newObj                 interface{}
		expected               queueingStrategy
		expectedExecutionCount int // queueing hint 函数的期望总执行次数
		queueingHintMap        QueueingHintMapPerProfile
	}{
		{
			name: "return Queue when no queueing hint function is registered for the event",
			podInfo: &framework.QueuedPodInfo{
				QueueingParams: framework.QueueingParams{
					UnschedulablePlugins: sets.New("fooPlugin1"),
				},
				PodInfo: mustNewPodInfo(st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj()),
			},
			event:                  nodeAdd,
			oldObj:                 nil,
			newObj:                 st.MakeNode().Obj(),
			expected:               queueSkip,
			expectedExecutionCount: 0,
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					// 没有为 NodeAdd 注册 queueing hint 函数。
					framework.EventAssignedPodAdd: {
						{
							// 事件不是 NodeAdd，因此它会被忽略。
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
				},
			},
		},
		{
			name: "Treat the event as Queue when QueueHintFn returns error",
			podInfo: &framework.QueuedPodInfo{
				QueueingParams: framework.QueueingParams{
					UnschedulablePlugins: sets.New("fooPlugin1"),
				},
				PodInfo: mustNewPodInfo(st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj()),
			},
			event:                  nodeAdd,
			oldObj:                 nil,
			newObj:                 st.MakeNode().Obj(),
			expected:               queueAfterBackoff,
			expectedExecutionCount: 1,
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					nodeAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnErr,
						},
					},
				},
			},
		},
		{
			name: "return Queue when the event is wildcard",
			podInfo: &framework.QueuedPodInfo{
				QueueingParams: framework.QueueingParams{
					UnschedulablePlugins: sets.New("fooPlugin1"),
				},
				PodInfo: mustNewPodInfo(st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj()),
			},
			event:                  framework.EventUnschedulableTimeout,
			oldObj:                 nil,
			newObj:                 nil,
			expected:               queueAfterBackoff,
			expectedExecutionCount: 0,
			queueingHintMap:        QueueingHintMapPerProfile{},
		},
		{
			name: "return Queue when the event is wildcard and the wildcard targets the pod to be requeued right now",
			podInfo: &framework.QueuedPodInfo{
				QueueingParams: framework.QueueingParams{
					UnschedulablePlugins: sets.New("fooPlugin1"),
				},
				PodInfo: mustNewPodInfo(st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj()),
			},
			event:                  framework.EventForceActivate,
			oldObj:                 nil,
			newObj:                 st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj(),
			expected:               queueAfterBackoff,
			expectedExecutionCount: 0,
			queueingHintMap:        QueueingHintMapPerProfile{},
		},
		{
			name: "return Skip when the event is wildcard, but the wildcard targets a different pod",
			podInfo: &framework.QueuedPodInfo{
				QueueingParams: framework.QueueingParams{
					UnschedulablePlugins: sets.New("fooPlugin1"),
				},
				PodInfo: mustNewPodInfo(st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj()),
			},
			event:                  framework.EventForceActivate,
			oldObj:                 nil,
			newObj:                 st.MakePod().Name("pod-different").Namespace("ns2").UID("2").Obj(),
			expected:               queueSkip,
			expectedExecutionCount: 0,
			queueingHintMap:        QueueingHintMapPerProfile{},
		},
		{
			name: "interprets Queue from the Pending plugin as queueImmediately",
			podInfo: &framework.QueuedPodInfo{
				QueueingParams: framework.QueueingParams{
					UnschedulablePlugins: sets.New("fooPlugin1", "fooPlugin3"),
					PendingPlugins:       sets.New("fooPlugin2"),
				},
				PodInfo: mustNewPodInfo(st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj()),
			},
			event:                  nodeAdd,
			oldObj:                 nil,
			newObj:                 st.MakeNode().Node,
			expected:               queueImmediately,
			expectedExecutionCount: 2,
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					nodeAdd: {
						{
							PluginName: "fooPlugin1",
							// 它返回 Queue，被解释为 queueAfterBackoff。
							// 但因为 Pod 有 PendingPlugins，函数会继续执行其他 hint，可能得到 queueImmediately。
							QueueingHintFn: queueHintReturnQueue,
						},
						{
							PluginName: "fooPlugin2",
							// 它被解释为 queueImmediately。
							// queueImmediately 优先级最高，因此不会再运行其他 hint。
							QueueingHintFn: queueHintReturnQueue,
						},
						{
							PluginName:     "fooPlugin3",
							QueueingHintFn: queueHintReturnQueue,
						},
						{
							PluginName:     "fooPlugin4",
							QueueingHintFn: queueHintReturnErr,
						},
					},
				},
			},
		},
		{
			name: "interprets Queue from the Unschedulable plugin as queueAfterBackoff",
			podInfo: &framework.QueuedPodInfo{
				QueueingParams: framework.QueueingParams{
					UnschedulablePlugins: sets.New("fooPlugin1", "fooPlugin2"),
				},
				PodInfo: mustNewPodInfo(st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj()),
			},
			event:                  nodeAdd,
			oldObj:                 nil,
			newObj:                 st.MakeNode().Obj(),
			expected:               queueAfterBackoff,
			expectedExecutionCount: 2,
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					nodeAdd: {
						{
							// Skip 会被忽略
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnSkip,
						},
						{
							// Skip 会被忽略
							PluginName:     "fooPlugin2",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
				},
			},
		},
		{
			name: "Queueing hint function that isn't from the plugin in UnschedulablePlugins/PendingPlugins is ignored",
			podInfo: &framework.QueuedPodInfo{
				QueueingParams: framework.QueueingParams{
					UnschedulablePlugins: sets.New("fooPlugin1", "fooPlugin2"),
				},
				PodInfo: mustNewPodInfo(st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj()),
			},
			event:                  nodeAdd,
			oldObj:                 nil,
			newObj:                 st.MakeNode().Node,
			expected:               queueSkip,
			expectedExecutionCount: 2,
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					nodeAdd: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnSkip,
						},
						{
							PluginName:     "fooPlugin2",
							QueueingHintFn: queueHintReturnSkip,
						},
						{
							PluginName:     "fooPlugin3",
							QueueingHintFn: queueHintReturnQueue, // 会被忽略。
						},
					},
				},
			},
		},
		{
			name: "If event is specific Node update event, queueing hint function for NodeUpdate/UpdateNodeLabel is also executed",
			podInfo: &framework.QueuedPodInfo{
				QueueingParams: framework.QueueingParams{
					UnschedulablePlugins: sets.New("fooPlugin1", "fooPlugin2"),
				},
				PodInfo: mustNewPodInfo(st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj()),
			},
			event:                  fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.UpdateNodeLabel},
			oldObj:                 nil,
			newObj:                 st.MakeNode().Obj(),
			expected:               queueAfterBackoff,
			expectedExecutionCount: 1,
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.UpdateNodeLabel}: {
						{
							PluginName: "fooPlugin1",
							// 它只会被执行并解释为 queueAfterBackoff。
							// 该 Pod 没有 PendingPlugins，因此不会再运行其他 hint。
							QueueingHintFn: queueHintReturnQueue,
						},
						{
							PluginName:     "fooPlugin2",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
					fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Update}: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
					nodeAdd: { // 不执行，因为 NodeAdd 无关。
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
				},
			},
		},
		{
			name: "If event with '*' Resource, queueing hint function for specified Resource is also executed",
			podInfo: &framework.QueuedPodInfo{
				QueueingParams: framework.QueueingParams{
					UnschedulablePlugins: sets.New("fooPlugin1"),
				},
				PodInfo: mustNewPodInfo(st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj()),
			},
			event:                  fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add},
			oldObj:                 nil,
			newObj:                 st.MakeNode().Obj(),
			expected:               queueAfterBackoff,
			expectedExecutionCount: 1,
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					fwk.ClusterEvent{Resource: fwk.WildCard, ActionType: fwk.Add}: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
				},
			},
		},
		{
			name: "If event is a wildcard one, queueing hint function for all kinds of events is executed",
			podInfo: &framework.QueuedPodInfo{
				QueueingParams: framework.QueueingParams{
					UnschedulablePlugins: sets.New("fooPlugin1"),
				},
				PodInfo: mustNewPodInfo(st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj()),
			},
			event:                  fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.UpdateNodeLabel | fwk.UpdateNodeTaint},
			oldObj:                 nil,
			newObj:                 st.MakeNode().Obj(),
			expected:               queueAfterBackoff,
			expectedExecutionCount: 1,
			queueingHintMap: QueueingHintMapPerProfile{
				"": {
					fwk.ClusterEvent{Resource: fwk.WildCard, ActionType: fwk.All}: {
						{
							PluginName:     "fooPlugin1",
							QueueingHintFn: queueHintReturnQueue,
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			count = 0 // 每次重置计数
			logger, ctx := ktesting.NewTestContext(t)
			q := NewTestQueue(ctx, newDefaultQueueSort(), WithQueueingHintMapPerProfile(test.queueingHintMap))
			actual := q.isPodWorthRequeuing(logger, test.podInfo, test.event, test.oldObj, test.newObj, nil)
			if actual != test.expected {
				t.Errorf("isPodWorthRequeuing() = %v, want %v", actual, test.expected)
			}
			if count != test.expectedExecutionCount {
				t.Errorf("isPodWorthRequeuing() executed queueing hint functions %v times, expected: %v", count, test.expectedExecutionCount)
			}
		})
	}
}

// Test_queuedPodInfo_gatedSetUponCreationAndUnsetUponUpdate 验证 queuedPodInfo 的 gate 标志在创建时置位、更新时清除。
func Test_queuedPodInfo_gatedSetUponCreationAndUnsetUponUpdate(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	plugin, _ := schedulinggates.New(ctx, nil, nil, plfeature.Features{})
	m := map[string]map[string]fwk.PreEnqueuePlugin{"": {names.SchedulingGates: plugin.(fwk.PreEnqueuePlugin)}}
	q := NewTestQueue(ctx, newDefaultQueueSort(), WithPreEnqueuePluginMap(m))

	gatedPod := st.MakePod().SchedulingGates([]string{"hello world"}).Obj()
	q.Add(ctx, gatedPod)

	if !q.unschedulableEntities.get(newQueuedPodInfoForLookup(gatedPod)).Gated() {
		t.Error("Expected pod to be gated")
	}

	ungatedPod := gatedPod.DeepCopy()
	ungatedPod.Spec.SchedulingGates = nil
	q.Update(ctx, gatedPod, ungatedPod)

	ungatedPodInfo, _ := q.Pop(logger)
	if ungatedPodInfo.Gated() {
		t.Error("Expected pod to be ungated")
	}
}

// TestPriorityQueue_GetPod 验证 GetPod 能从各队列中按名称取回 Pod。
func TestPriorityQueue_GetPod(t *testing.T) {
	activeQPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1",
			Namespace: "default",
		},
	}
	backoffQPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod2",
			Namespace: "default",
		},
	}
	unschedPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod3",
			Namespace: "default",
		},
	}

	logger, ctx := ktesting.NewTestContext(t)
	q := NewTestQueue(ctx, newDefaultQueueSort())
	q.activeQ.add(logger, newQueuedPodInfoForLookup(activeQPod), framework.EventUnscheduledPodAdd.Label(), nil)
	q.backoffQ.add(logger, newQueuedPodInfoForLookup(backoffQPod), framework.EventUnscheduledPodAdd.Label(), nil)
	q.unschedulableEntities.addOrUpdate(newQueuedPodInfoForLookup(unschedPod), false, framework.EventUnscheduledPodAdd.Label(), nil)

	tests := []struct {
		name        string
		podName     string
		namespace   string
		expectedPod *v1.Pod
		expectedOK  bool
	}{
		{
			name:        "pod is found in activeQ",
			podName:     "pod1",
			namespace:   "default",
			expectedPod: activeQPod,
			expectedOK:  true,
		},
		{
			name:        "pod is found in backoffQ",
			podName:     "pod2",
			namespace:   "default",
			expectedPod: backoffQPod,
			expectedOK:  true,
		},
		{
			name:        "pod is found in unschedulableEntities",
			podName:     "pod3",
			namespace:   "default",
			expectedPod: unschedPod,
			expectedOK:  true,
		},
		{
			name:        "pod is not found",
			podName:     "pod4",
			namespace:   "default",
			expectedPod: nil,
			expectedOK:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pInfo, ok := q.GetPod(tt.podName, tt.namespace, nil)
			if ok != tt.expectedOK {
				t.Errorf("Expected ok=%v, but got ok=%v", tt.expectedOK, ok)
			}

			if tt.expectedPod == nil {
				if pInfo == nil {
					return
				}
				t.Fatalf("Expected pod is empty, but got pod=%v", pInfo.Pod)
			}

			if !cmp.Equal(pInfo.Pod, tt.expectedPod) {
				t.Errorf("Expected pod=%v, but got pod=%v", tt.expectedPod, pInfo.Pod)
			}
		})
	}
}

// TestUnschedulablePodsMetric 验证不可调度 Pod 数量的 Prometheus 指标。
func TestUnschedulablePodsMetric(t *testing.T) {
	type step func(tCtx ktesting.TContext, q *PriorityQueue)

	addPod := func(pInfo *framework.QueuedPodInfo) step {
		return func(tCtx ktesting.TContext, q *PriorityQueue) {
			add(tCtx, q, pInfo)
		}
	}
	deletePod := func(pInfo *framework.QueuedPodInfo) step {
		return func(tCtx ktesting.TContext, q *PriorityQueue) {
			deletePod(tCtx, q, pInfo)
		}
	}
	popPod := func() step {
		return func(tCtx ktesting.TContext, q *PriorityQueue) {
			pop(tCtx, q, nil)
		}
	}
	moveAllToActiveOrBackoffQ := func() step {
		return func(tCtx ktesting.TContext, q *PriorityQueue) {
			moveAllToActiveOrBackoffQ(tCtx, q, nil)
		}
	}
	updatePluginAllowList := func(pluginName string, list []string) step {
		return func(tCtx ktesting.TContext, q *PriorityQueue) {
			q.preEnqueuePluginMap[""][pluginName].(*preEnqueuePlugin).allowlists = list
		}
	}

	pluginName1 := "plugin1"
	pluginName2 := "plugin2"
	queueable := "queueable"
	timestamp := time.Now()
	pod := &framework.QueuedPodInfo{
		PodInfo: mustNewPodInfo(
			st.MakePod().Name("podA").Namespace("namespaceA").Label(queueable, "").UID("someUid").Obj()),
		QueueingParams: framework.QueueingParams{
			Timestamp:            timestamp,
			UnschedulablePlugins: sets.New[string](),
		},
	}

	resetMetrics := func() {
		metrics.UnschedulableReason(pluginName1, "").Set(0)
		metrics.UnschedulableReason(pluginName2, "").Set(0)
	}

	makeGated := func(pInfo *framework.QueuedPodInfo) *framework.QueuedPodInfo {
		return setQueuedPodInfoGated(pInfo.DeepCopy(), pluginName1, []fwk.ClusterEvent{framework.EventUnschedulableTimeout})
	}

	tests := []struct {
		name            string
		steps           []step
		expectedMetrics []int
	}{
		{
			name: "Unschedulable pods metric must be 0 after a pod is gated, ungated, re-queued, and eventually popped from the scheduling queue",
			steps: []step{
				updatePluginAllowList(pluginName1, []string{}),
				addPod(pod),
				moveAllToActiveOrBackoffQ(),
				updatePluginAllowList(pluginName1, []string{queueable}),
				moveAllToActiveOrBackoffQ(),
				popPod(),
			},
			expectedMetrics: []int{0, 0},
		},
		{
			name: "Unschedulable pods metric must be 0 after pod is gated and then deleted",
			steps: []step{
				updatePluginAllowList(pluginName1, []string{}),
				addPod(pod),
				moveAllToActiveOrBackoffQ(),
				deletePod(pod),
			},
			expectedMetrics: []int{0, 0},
		},
		{
			name: "Unschedulable pods metric must be 1 after pod is gated multiple time by the same plugin",
			steps: []step{
				updatePluginAllowList(pluginName1, []string{}),
				addPod(pod),
				moveAllToActiveOrBackoffQ(),
			},
			expectedMetrics: []int{1, 0},
		},
		{
			name: "Unschedulable pods metric must be 0 after non gated pods is added and then deleted",
			steps: []step{
				addPod(pod),
				deletePod(pod),
			},
			expectedMetrics: []int{0, 0},
		},
		{
			name: "Unschedulable pods metric should not be duplicate if gated pods added and then gated with the same plugin again",
			steps: []step{
				updatePluginAllowList(pluginName1, []string{}),
				addPod(makeGated(pod)),
				moveAllToActiveOrBackoffQ(),
			},
			expectedMetrics: []int{1, 0},
		},
		{
			name: "Unschedulable pods metric should be 0 if pod was gated by two plugins sequentially and then ungated and popped",
			steps: []step{
				updatePluginAllowList(pluginName1, []string{}),
				addPod(pod),
				updatePluginAllowList(pluginName1, []string{queueable}),
				updatePluginAllowList(pluginName2, []string{}),
				moveAllToActiveOrBackoffQ(),
				updatePluginAllowList(pluginName2, []string{queueable}),
				moveAllToActiveOrBackoffQ(),
				popPod(),
			},
			expectedMetrics: []int{0, 0},
		},
		{
			name: "Unschedulable pods metric should be 0 if pod was gated by two plugins sequentially and then deleted",
			steps: []step{
				updatePluginAllowList(pluginName1, []string{}),
				addPod(pod),
				updatePluginAllowList(pluginName1, []string{queueable}),
				updatePluginAllowList(pluginName2, []string{}),
				moveAllToActiveOrBackoffQ(),
				deletePod(pod),
			},
			expectedMetrics: []int{0, 0},
		},
		{
			name: "Unschedulable pods metric should be 1 for both plugins if pod was gated by two plugins sequentially",
			steps: []step{
				updatePluginAllowList(pluginName1, []string{}),
				addPod(pod),
				updatePluginAllowList(pluginName1, []string{queueable}),
				updatePluginAllowList(pluginName2, []string{}),
				moveAllToActiveOrBackoffQ(),
			},
			expectedMetrics: []int{1, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			resetMetrics()

			m := makeEmptyQueueingHintMapPerProfile()
			m[""][framework.EventUnschedulableTimeout] = []*QueueingHintFunction{
				{
					PluginName:     pluginName1,
					QueueingHintFn: queueHintReturnQueue,
				},
				{
					PluginName:     pluginName2,
					QueueingHintFn: queueHintReturnQueue,
				},
			}

			plugin1 := preEnqueuePlugin{name: pluginName1, allowlists: []string{queueable}}
			plugin2 := preEnqueuePlugin{name: pluginName2, allowlists: []string{queueable}}

			preenq := map[string]map[string]fwk.PreEnqueuePlugin{"": {pluginName1: &plugin1, pluginName2: &plugin2}}
			recorder := metrics.NewMetricsAsyncRecorder(3, 20*time.Microsecond, tCtx.Done())
			q := NewTestQueue(tCtx, newDefaultQueueSort(), WithClock(testingclock.NewFakeClock(timestamp)), WithPreEnqueuePluginMap(preenq), WithMetricsRecorder(recorder), WithQueueingHintMapPerProfile(m))

			for _, step := range tt.steps {
				step(tCtx, q)
			}

			for i, pluginName := range []string{pluginName1, pluginName2} {
				val, err := testutil.GetGaugeMetricValue(metrics.UnschedulableReason(pluginName, ""))

				if err != nil {
					t.Errorf("Error while collection metric value:\n%s", err)
				}
				if int(val) != tt.expectedMetrics[i] {
					t.Errorf("Unexpected metric for plugin %s result expected %d, actual %d", pluginName, tt.expectedMetrics[i], int(val))
				}
			}

		})
	}
}

// TestPriorityQueue_signPod 验证 signPod 生成的签名稳定且覆盖相关字段。
func TestPriorityQueue_signPod(t *testing.T) {
	tests := []struct {
		name              string
		enableFeatureGate bool
		signers           map[string]PodSigner
		pod               *v1.Pod
		expectedSignature fwk.PodSignature
	}{
		{
			name:              "Feature gate disabled",
			enableFeatureGate: false,
			signers: map[string]PodSigner{
				"default-scheduler": func(ctx context.Context, pod *v1.Pod) fwk.PodSignature {
					return fwk.PodSignature("sig-1")
				},
			},
			pod: st.MakePod().Name("pod1").SchedulerName("default-scheduler").Obj(),
		},
		{
			name:              "No signers configured",
			enableFeatureGate: true,
			signers:           nil,
			pod:               st.MakePod().Name("pod1").SchedulerName("default-scheduler").Obj(),
		},
		{
			name:              "Signer not found for scheduler",
			enableFeatureGate: true,
			signers: map[string]PodSigner{
				"default-scheduler": func(ctx context.Context, pod *v1.Pod) fwk.PodSignature {
					return fwk.PodSignature("sig-1")
				},
			},
			pod: st.MakePod().Name("pod1").SchedulerName("custom-scheduler").Obj(),
		},
		{
			name:              "Successful signature computation",
			enableFeatureGate: true,
			signers: map[string]PodSigner{
				"default-scheduler": func(ctx context.Context, pod *v1.Pod) fwk.PodSignature {
					return fwk.PodSignature("sig-1")
				},
			},
			pod:               st.MakePod().Name("pod1").SchedulerName("default-scheduler").Obj(),
			expectedSignature: fwk.PodSignature("sig-1"),
		},
		{
			name:              "Signer returns nil (unsignable pod)",
			enableFeatureGate: true,
			signers: map[string]PodSigner{
				"default-scheduler": func(ctx context.Context, pod *v1.Pod) fwk.PodSignature {
					return nil
				},
			},
			pod: st.MakePod().Name("pod1").SchedulerName("default-scheduler").Obj(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.OpportunisticBatching, tt.enableFeatureGate)

			tCtx := ktesting.Init(t)
			q := NewTestQueue(tCtx, newDefaultQueueSort(), WithPodSigners(tt.signers))

			signature := q.signPod(tCtx, tt.pod)

			if !bytes.Equal(signature, tt.expectedSignature) {
				t.Errorf("Expected signature '%s', got '%s'", string(tt.expectedSignature), string(signature))
			}
		})
	}
}

// TestPriorityQueue_AddComputesSignature 验证 Add 会计算并保存 Pod 签名。
func TestPriorityQueue_AddComputesSignature(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.OpportunisticBatching, true)

	signers := map[string]PodSigner{
		"default-scheduler": func(ctx context.Context, pod *v1.Pod) fwk.PodSignature {
			if val, ok := pod.Labels["key"]; ok {
				return fwk.PodSignature(fmt.Sprintf("sig-%s", val))
			}
			return fwk.PodSignature("sig-default")
		},
	}

	tCtx := ktesting.Init(t)
	q := NewTestQueue(tCtx, newDefaultQueueSort(), WithPodSigners(signers))

	pod := st.MakePod().Name("pod1").SchedulerName("default-scheduler").Label("key", "value1").Obj()
	q.Add(tCtx, pod)

	pInfo, exists := q.GetPod(pod.Name, pod.Namespace, nil)
	if !exists {
		t.Fatal("Pod not found in queue after Add")
	}
	if !bytes.Equal(pInfo.PodSignature, fwk.PodSignature("sig-value1")) {
		t.Errorf("Expected signature 'sig-value1', got '%s'", string(pInfo.PodSignature))
	}
}

// TestPriorityQueue_UpdateRecomputesSignature 验证 Update 会重新计算 Pod 签名。
func TestPriorityQueue_UpdateRecomputesSignature(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.OpportunisticBatching, true)

	signers := map[string]PodSigner{
		"default-scheduler": func(ctx context.Context, pod *v1.Pod) fwk.PodSignature {
			if val, ok := pod.Labels["key"]; ok {
				return fwk.PodSignature(fmt.Sprintf("sig-%s", val))
			}
			return fwk.PodSignature("sig-default")
		},
	}

	pod1 := st.MakePod().Name("pod1").SchedulerName("default-scheduler").Label("key", "value1").Obj()
	pod2 := pod1.DeepCopy()
	pod2.Labels["key"] = "value2"

	tests := []struct {
		name        string
		prepareFunc func(tCtx ktesting.TContext, q *PriorityQueue)
	}{
		{
			name: "pod in activeQ",
			prepareFunc: func(tCtx ktesting.TContext, q *PriorityQueue) {
				q.Add(tCtx, pod1)
			},
		},
		{
			name: "pod in backoffQ",
			prepareFunc: func(tCtx ktesting.TContext, q *PriorityQueue) {
				pInfo := q.newQueuedPodInfo(tCtx, pod1)
				q.backoffQ.add(klog.FromContext(tCtx), pInfo, framework.EventUnscheduledPodAdd.Label(), nil)
			},
		},
		{
			name: "pod in unschedulableEntities",
			prepareFunc: func(tCtx ktesting.TContext, q *PriorityQueue) {
				pInfo := q.newQueuedPodInfo(tCtx, pod1)
				q.unschedulableEntities.addOrUpdate(pInfo, false, framework.EventUnscheduledPodAdd.Label(), nil)
			},
		},
		{
			name:        "pod not in any queue",
			prepareFunc: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			q := NewTestQueue(tCtx, newDefaultQueueSort(), WithPodSigners(signers))

			if tt.prepareFunc != nil {
				tt.prepareFunc(tCtx, q)
			}

			// 用不同的标签更新 Pod
			q.Update(tCtx, pod1, pod2)

			// 检查签名是否被重新计算
			pInfo, exists := q.GetPod(pod2.Name, pod2.Namespace, nil)
			if !exists {
				t.Fatal("Pod not found in queue after update")
			}
			if !bytes.Equal(pInfo.PodSignature, fwk.PodSignature("sig-value2")) {
				t.Errorf("Expected signature 'sig-value2', got '%s'", string(pInfo.PodSignature))
			}
		})
	}
}

// TestPriorityQueue_MultipleProfiles 验证多 profile 场景下队列按 profile 隔离状态。
func TestPriorityQueue_MultipleProfiles(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.OpportunisticBatching, true)

	_, ctx := ktesting.NewTestContext(t)

	signers := map[string]PodSigner{
		"scheduler-1": func(ctx context.Context, pod *v1.Pod) fwk.PodSignature {
			return fwk.PodSignature("sig-scheduler-1")
		},
		"scheduler-2": func(ctx context.Context, pod *v1.Pod) fwk.PodSignature {
			return fwk.PodSignature("sig-scheduler-2")
		},
	}

	q := NewTestQueue(ctx, newDefaultQueueSort(), WithPodSigners(signers))

	pod1 := st.MakePod().Name("pod1").SchedulerName("scheduler-1").Obj()
	pod2 := st.MakePod().Name("pod2").SchedulerName("scheduler-2").Obj()
	pod3 := st.MakePod().Name("pod3").SchedulerName("scheduler-3").Obj() // 无签名器

	q.Add(ctx, pod1)
	q.Add(ctx, pod2)
	q.Add(ctx, pod3)

	pInfo1, _ := q.GetPod(pod1.Name, pod1.Namespace, nil)
	pInfo2, _ := q.GetPod(pod2.Name, pod2.Namespace, nil)
	pInfo3, _ := q.GetPod(pod3.Name, pod3.Namespace, nil)

	if !bytes.Equal(pInfo1.PodSignature, fwk.PodSignature("sig-scheduler-1")) {
		t.Errorf("Pod1: expected 'sig-scheduler-1', got '%s'", string(pInfo1.PodSignature))
	}
	if !bytes.Equal(pInfo2.PodSignature, fwk.PodSignature("sig-scheduler-2")) {
		t.Errorf("Pod2: expected 'sig-scheduler-2', got '%s'", string(pInfo2.PodSignature))
	}
	if pInfo3.PodSignature != nil {
		t.Errorf("Pod3: expected nil signature (no signer), got '%s'", string(pInfo3.PodSignature))
	}

}

// TestConcurrentUpdateAndPop 验证并发 Update 与 Pop 不会死锁或丢失 Pod。
func TestConcurrentUpdateAndPop(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)

	q := NewTestQueue(ctx, newDefaultQueueSort())

	podName := "test-pod"
	// 创建一个高优先级 Pod，确保它排在队首
	pod := st.MakePod().Name(podName).Namespace("default").UID("uid-1").Priority(100).Obj()
	q.Add(ctx, pod)

	var wg sync.WaitGroup
	start := time.Now()
	testDuration := 3 * time.Second

	// Goroutine 1：不断 Pop 并重新 Add
	wg.Go(func() {
		for time.Since(start) < testDuration {
			// 队列为空时 Pop 会阻塞，这里验证它不会永久阻塞或 panic
			entity, err := q.Pop(logger)
			if err != nil {
				t.Errorf("Unexpected error during Pop: %v", err)
				return
			}
			if entity == nil {
				t.Errorf("Unexpected nil QueuedEntityInfo during Pop")
				return
			}
			pInfo := entity.(*framework.QueuedPodInfo)
			if pInfo.Pod.UID != pod.UID {
				t.Errorf("Expected pod UID %v, got %v", pod.UID, pInfo.Pod.UID)
			}
			// 模拟一些工作以放大竞态窗口
			time.Sleep(100 * time.Microsecond)
			q.Done(pInfo.Pod.UID)
			// 重新加入队列以维持循环
			q.Add(ctx, pInfo.Pod)
		}
	})

	// Goroutine 2：不断 Update Pod
	wg.Go(func() {
		iter := 0
		currentPod := pod
		for time.Since(start) < testDuration {
			iter++
			newPod := currentPod.DeepCopy()
			newPod.Annotations = map[string]string{"ver": fmt.Sprintf("%d", iter)}
			// 更新是原子的
			q.Update(ctx, currentPod, newPod)
			currentPod = newPod
			time.Sleep(50 * time.Microsecond)
		}
	})

	wg.Wait()
	q.Close()
}

type initialQueueState int

const (
	stateActive initialQueueState = iota
	statePopped
	stateBackoff
	stateUnschedulable
	stateGated
	stateIncomplete
)

func setupInitialPodGroupState(t *testing.T, ctx context.Context, q *PriorityQueue, initialPods []*v1.Pod, initialState initialQueueState, initialPodGroup *schedulingv1beta1.PodGroup) {
	t.Helper()

	if initialState != stateIncomplete {
		logger := klog.FromContext(ctx)
		q.AddPodGroup(logger, initialPodGroup)
	}

	if len(initialPods) == 0 {
		return
	}
	logger := klog.FromContext(ctx)

	for _, pod := range initialPods {
		q.Add(ctx, pod)
	}

	pgLookup := newQueuedPodGroupInfoForLookup(initialPodGroup.Namespace, initialPodGroup.Name, fwk.PodGroupKeyType)
	switch initialState {
	case statePopped:
		if _, err := q.Pop(logger); err != nil {
			t.Fatalf("Unexpected error from Pop: %v", err)
		}
	case stateBackoff:
		entity := q.activeQ.delete(pgLookup)
		if entity != nil {
			if pgInfo, ok := entity.(*framework.QueuedPodGroupInfo); ok {
				pgInfo.UnschedulableCount = 1
				pgInfo.Timestamp = q.clock.Now()
				pgInfo.UnschedulablePlugins = sets.New("fooPlugin")
			} else if pInfo, ok := entity.(*framework.QueuedPodInfo); ok {
				pInfo.UnschedulableCount = 1
				pInfo.Timestamp = q.clock.Now()
				pInfo.UnschedulablePlugins = sets.New("fooPlugin")
			}
			q.backoffQ.add(logger, entity, framework.EventUnscheduledPodAdd.Label(), nil)
		}
	case stateUnschedulable:
		entity := q.activeQ.delete(pgLookup)
		if entity != nil {
			q.unschedulableEntities.addOrUpdate(entity, false, framework.EventUnscheduledPodAdd.Label(), nil)
		}
	case stateGated:
		entity := q.activeQ.delete(pgLookup)
		if entity != nil {
			entity.SetGatingPlugin("preEnqueuePlugin", []fwk.ClusterEvent{pvAdd})
			q.unschedulableEntities.addOrUpdate(entity, false, framework.EventUnscheduledPodAdd.Label(), nil)
		}
	}
}

// TestAddPodGroupMember 验证添加 PodGroup 成员时实体在队列中的组织方式。
func TestAddPodGroupMember(t *testing.T) {
	pgName := "pg-test"
	p1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").Label("allow", "").PodGroupName(pgName).Obj()
	p2 := st.MakePod().Name("pod2").Namespace("ns1").UID("pod2").Label("allow", "").PodGroupName(pgName).Obj()
	gatedP1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").PodGroupName(pgName).Obj()
	gatedP2 := st.MakePod().Name("pod2").Namespace("ns1").UID("pod2").PodGroupName(pgName).Obj()

	tests := []struct {
		name                          string
		initialPod                    *v1.Pod
		initialState                  initialQueueState
		incomingPod                   *v1.Pod
		expectedInActiveQ             bool
		expectedInUnschedulable       bool
		expectedInBackoffQ            bool
		expectedInPendingPodGroupPods bool
		expectedInIncomplete          []*v1.Pod
		expectedGated                 bool
		expectedGroupSize             int
	}{
		{
			name:              "first member added (ungated), creates new pod group in activeQ",
			incomingPod:       p1,
			expectedInActiveQ: true,
			expectedGroupSize: 1,
		},
		{
			name:                    "first member added (gated), creates new pod group directly in unschedulableEntities",
			incomingPod:             gatedP1,
			expectedInUnschedulable: true,
			expectedGated:           true,
			expectedGroupSize:       1,
		},
		{
			name:                          "matching pod group is currently in flight (last popped), pod (ungated) goes to pendingPodGroupPods",
			initialPod:                    p1,
			initialState:                  statePopped,
			incomingPod:                   p2,
			expectedInPendingPodGroupPods: true,
		},
		{
			name:                          "matching pod group is currently in flight (last popped), pod (gated) goes to pendingPodGroupPods",
			initialPod:                    p1,
			initialState:                  statePopped,
			incomingPod:                   gatedP2,
			expectedInPendingPodGroupPods: true,
		},
		{
			name:              "existing pod group in activeQ (ungated addition), stays in activeQ",
			initialPod:        p1,
			initialState:      stateActive,
			incomingPod:       p2,
			expectedInActiveQ: true,
			expectedGroupSize: 2,
		},
		{
			name:                    "existing pod group in activeQ (gated addition), becomes gated and moves to unschedulableEntities",
			initialPod:              p1,
			initialState:            stateActive,
			incomingPod:             gatedP2,
			expectedInUnschedulable: true,
			expectedGated:           true,
			expectedGroupSize:       2,
		},
		{
			name:               "existing pod group in backoffQ (ungated addition), stays in backoffQ",
			initialPod:         p1,
			initialState:       stateBackoff,
			incomingPod:        p2,
			expectedInBackoffQ: true,
			expectedGroupSize:  2,
		},
		{
			name:                    "existing pod group in backoffQ (gated addition), becomes gated and moves to unschedulableEntities",
			initialPod:              p1,
			initialState:            stateBackoff,
			incomingPod:             gatedP2,
			expectedInUnschedulable: true,
			expectedGated:           true,
			expectedGroupSize:       2,
		},
		{
			name:                    "existing pod group in unschedulableEntities (ungated addition), stays in unschedulableEntities as ungated",
			initialPod:              p1,
			initialState:            stateUnschedulable,
			incomingPod:             p2,
			expectedInUnschedulable: true,
			expectedGated:           false,
			expectedGroupSize:       2,
		},
		{
			name:                    "existing pod group in unschedulableEntities (gated addition), stays in unschedulableEntities as ungated",
			initialPod:              p1,
			initialState:            stateUnschedulable,
			incomingPod:             gatedP2,
			expectedInUnschedulable: true,
			expectedGated:           false,
			expectedGroupSize:       2,
		},
		{
			name:                    "existing gated pod group in unschedulableEntities (ungated addition), stays in unschedulableEntities as gated",
			initialPod:              gatedP1,
			initialState:            stateGated,
			incomingPod:             p2,
			expectedInUnschedulable: true,
			expectedGated:           true,
			expectedGroupSize:       2,
		},
		{
			name:                 "first member added before pod group exists, goes to incompletePodGroupPods",
			initialState:         stateIncomplete,
			incomingPod:          p1,
			expectedInIncomplete: []*v1.Pod{p1},
		},
		{
			name:                 "existing pod in incompletePodGroupPods, stays in incompletePodGroupPods",
			initialPod:           p1,
			initialState:         stateIncomplete,
			incomingPod:          p2,
			expectedInIncomplete: []*v1.Pod{p1, p2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload: true,
			})

			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			// 配置一个 PreEnqueue 插件，只放行带 "allow" 标签的 Pod，其余都加 gate。
			preEnqueueMap := map[string]map[string]fwk.PreEnqueuePlugin{
				"": {
					"preEnqueuePlugin": &preEnqueuePlugin{allowlists: []string{"allow"}},
				},
			}

			q := NewTestQueue(ctx, newDefaultQueueSort(), WithPreEnqueuePluginMap(preEnqueueMap))
			podGroup := st.MakePodGroup().Name(pgName).Namespace("ns1").Obj()

			var initialPods []*v1.Pod
			if tt.initialPod != nil {
				initialPods = []*v1.Pod{tt.initialPod}
			}
			setupInitialPodGroupState(t, ctx, q, initialPods, tt.initialState, podGroup)

			// 加入新到达的 Pod
			q.Add(ctx, tt.incomingPod)

			// 验证条件
			pgLookup := newQueuedPodGroupInfoForLookup(podGroup.Namespace, podGroup.Name, fwk.PodGroupKeyType)

			if inActive := q.activeQ.has(pgLookup); inActive != tt.expectedInActiveQ {
				t.Errorf("Expected incoming pod in activeQ: %v, got %v", tt.expectedInActiveQ, inActive)
			}
			var entity framework.QueuedEntityInfo
			if tt.expectedInActiveQ {
				entity, _ = q.activeQ.get(pgLookup)
			}

			if inBackoff := q.backoffQ.has(pgLookup); inBackoff != tt.expectedInBackoffQ {
				t.Errorf("Expected incoming pod in backoffQ: %v, got %v", tt.expectedInBackoffQ, inBackoff)
			}
			if tt.expectedInBackoffQ {
				entity, _ = q.backoffQ.get(pgLookup)
			}

			unschedulableEntity := q.unschedulableEntities.get(pgLookup)
			inUnschedulable := unschedulableEntity != nil
			if inUnschedulable != tt.expectedInUnschedulable {
				t.Errorf("Expected incoming pod in unschedulableEntities: %v, got %v", tt.expectedInUnschedulable, inUnschedulable)
			}
			if tt.expectedInUnschedulable {
				entity = unschedulableEntity
			}

			if entity != nil {
				if isGated := entity.Gated(); isGated != tt.expectedGated {
					t.Errorf("Expected pod group to be gated: %v, got %v", tt.expectedGated, isGated)
				}
				if size := entity.Size(); size != tt.expectedGroupSize {
					t.Errorf("Expected pod group to be of size: %d, got %d", tt.expectedGroupSize, size)
				}

				// 验证新到达 Pod 确实被加入
				foundMember := false
				entity.(*framework.QueuedPodGroupInfo).ForEachPodInfo(func(pInfo *framework.QueuedPodInfo) bool {
					if pInfo.Pod.Name == tt.incomingPod.Name {
						foundMember = true
						return false
					}
					return true
				})
				if !foundMember {
					t.Errorf("Incoming pod %s was not found in the pod group members", tt.incomingPod.Name)
				}
			}

			inPending := q.pendingPodGroupPods.has(tt.incomingPod)
			if inPending != tt.expectedInPendingPodGroupPods {
				t.Errorf("Expected incoming pod in pendingPodGroupPods: %v, got %v", tt.expectedInPendingPodGroupPods, inPending)
			}

			for _, pod := range tt.expectedInIncomplete {
				if !q.incompletePodGroupPods.has(pod) {
					t.Errorf("Expected pod %v in incompletePodGroupPods", pod.Name)
				}
			}
			if q.incompletePodGroupPods.len() != len(tt.expectedInIncomplete) {
				t.Errorf("Expected incompletePodGroupPods size to be %v, got %v", len(tt.expectedInIncomplete), q.incompletePodGroupPods.len())
			}
		})
	}
}

// TestDeletePodGroupMember 验证删除 PodGroup 成员时实体在队列中的清理方式。
func TestDeletePodGroupMember(t *testing.T) {
	pgName := "pg-test"
	p1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").Label("allow", "").PodGroupName(pgName).Obj()
	p2 := st.MakePod().Name("pod2").Namespace("ns1").UID("pod2").Label("allow", "").PodGroupName(pgName).Obj()
	p3 := st.MakePod().Name("pod3").Namespace("ns1").UID("pod3").Label("allow", "").PodGroupName(pgName).Obj()
	gatedP2 := st.MakePod().Name("pod2").Namespace("ns1").UID("pod2").PodGroupName(pgName).Obj()
	gatedP3 := st.MakePod().Name("pod3").Namespace("ns1").UID("pod3").PodGroupName(pgName).Obj()
	notFoundPod := st.MakePod().Name("pod-not-found").Namespace("ns1").UID("pod-not-found").Label("allow", "").PodGroupName(pgName).Obj()

	tests := []struct {
		name                    string
		initialPods             []*v1.Pod
		initialState            initialQueueState
		pendingPods             []*v1.Pod
		podToDelete             *v1.Pod
		expectedInActiveQ       bool
		expectedInUnschedulable bool
		expectedInBackoffQ      bool
		expectedPodsInPending   int
		expectedInIncomplete    []*v1.Pod
		expectedGated           bool
		expectedGroupSize       int
	}{
		{
			name:                  "delete from pendingPodGroupPods (group in flight)",
			initialPods:           []*v1.Pod{p1},
			initialState:          statePopped,
			pendingPods:           []*v1.Pod{p2},
			podToDelete:           p2,
			expectedPodsInPending: 0,
		},
		{
			name:                  "delete one pending pod where there are more pending pods for a group in flight",
			initialPods:           []*v1.Pod{p1, p2, p3},
			initialState:          statePopped,
			pendingPods:           []*v1.Pod{p2, p3},
			podToDelete:           p2,
			expectedPodsInPending: 1,
		},
		{
			name:              "delete the only pod of a group in activeQ, completely removes group",
			initialPods:       []*v1.Pod{p1},
			initialState:      stateActive,
			podToDelete:       p1,
			expectedInActiveQ: false,
		},
		{
			name:              "delete one pod of a size-2 group in activeQ, decreases size and keeps in activeQ",
			initialPods:       []*v1.Pod{p1, p2},
			initialState:      stateActive,
			podToDelete:       p2,
			expectedInActiveQ: true,
			expectedGroupSize: 1,
		},
		{
			name:               "delete the only pod of a group in backoffQ, completely removes group",
			initialPods:        []*v1.Pod{p1},
			initialState:       stateBackoff,
			podToDelete:        p1,
			expectedInBackoffQ: false,
		},
		{
			name:               "delete one pod of a size-2 group in backoffQ, keeps group in backoffQ",
			initialPods:        []*v1.Pod{p1, p2},
			initialState:       stateBackoff,
			podToDelete:        p2,
			expectedInBackoffQ: true,
			expectedGroupSize:  1,
		},
		{
			name:                    "delete the only pod of a group in unschedulableEntities, completely removes group",
			initialPods:             []*v1.Pod{p1},
			initialState:            stateUnschedulable,
			podToDelete:             p1,
			expectedInUnschedulable: false,
		},
		{
			name:                    "delete one pod of a size-2 group in unschedulableEntities, decreases size and keeps in unschedulableEntities",
			initialPods:             []*v1.Pod{p1, p2},
			initialState:            stateUnschedulable,
			podToDelete:             p2,
			expectedInUnschedulable: true,
			expectedGroupSize:       1,
		},
		{
			name:              "delete a pod that doesn't exist in any queue or pending map",
			initialPods:       []*v1.Pod{p1},
			initialState:      stateActive,
			podToDelete:       notFoundPod,
			expectedInActiveQ: true,
			expectedGroupSize: 1,
		},
		{
			name:                    "delete the only gated pod from a group, ungates the pod group",
			initialPods:             []*v1.Pod{p1, gatedP2},
			initialState:            stateGated,
			podToDelete:             gatedP2,
			expectedInUnschedulable: true,
			expectedGated:           false,
			expectedGroupSize:       1,
		},
		{
			name:                    "delete one gated pod from a group where another gated pod remains, group stays gated",
			initialPods:             []*v1.Pod{p1, gatedP2, gatedP3},
			initialState:            stateGated,
			podToDelete:             gatedP2,
			expectedInUnschedulable: true,
			expectedGated:           true,
			expectedGroupSize:       2,
		},
		{
			name:                 "delete from incompletePodGroupPods",
			initialPods:          []*v1.Pod{p1},
			initialState:         stateIncomplete,
			podToDelete:          p1,
			expectedInIncomplete: nil,
		},
		{
			name:                 "delete one pod from incompletePodGroupPods with multiple pods",
			initialPods:          []*v1.Pod{p1, p2},
			initialState:         stateIncomplete,
			podToDelete:          p1,
			expectedInIncomplete: []*v1.Pod{p2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload: true,
			})

			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			preEnqueueMap := map[string]map[string]fwk.PreEnqueuePlugin{
				"": {
					"preEnqueuePlugin": &preEnqueuePlugin{allowlists: []string{"allow"}},
				},
			}
			q := NewTestQueue(ctx, newDefaultQueueSort(), WithPreEnqueuePluginMap(preEnqueueMap))
			podGroup := st.MakePodGroup().Name(pgName).Namespace("ns1").Obj()

			setupInitialPodGroupState(t, ctx, q, tt.initialPods, tt.initialState, podGroup)
			// 加入 pending Pod
			for _, pod := range tt.pendingPods {
				q.Add(ctx, pod)
			}

			// 删除目标 Pod
			q.Delete(logger, tt.podToDelete)

			// 验证条件
			pgLookup := newQueuedPodGroupInfoForLookup(podGroup.Namespace, podGroup.Name, fwk.PodGroupKeyType)

			if inActive := q.activeQ.has(pgLookup); inActive != tt.expectedInActiveQ {
				t.Errorf("Expected target pod group in activeQ: %v, got %v", tt.expectedInActiveQ, inActive)
			}
			var entity framework.QueuedEntityInfo
			if tt.expectedInActiveQ {
				entity, _ = q.activeQ.get(pgLookup)
			}

			if inBackoff := q.backoffQ.has(pgLookup); inBackoff != tt.expectedInBackoffQ {
				t.Errorf("Expected target pod group in backoffQ: %v, got %v", tt.expectedInBackoffQ, inBackoff)
			}
			if tt.expectedInBackoffQ {
				entity, _ = q.backoffQ.get(pgLookup)
			}

			unschedulableEntity := q.unschedulableEntities.get(pgLookup)
			inUnschedulable := unschedulableEntity != nil
			if inUnschedulable != tt.expectedInUnschedulable {
				t.Errorf("Expected target pod group in unschedulableEntities: %v, got %v", tt.expectedInUnschedulable, inUnschedulable)
			}
			if tt.expectedInUnschedulable {
				entity = unschedulableEntity
			}

			if entity != nil {
				if size := entity.Size(); size != tt.expectedGroupSize {
					t.Errorf("Expected pod group to be of size: %d, got %d", tt.expectedGroupSize, size)
				}

				// 验证被删除 Pod 确实被移除
				entity.(*framework.QueuedPodGroupInfo).ForEachPodInfo(func(pInfo *framework.QueuedPodInfo) bool {
					if pInfo.Pod.Name == tt.podToDelete.Name {
						t.Errorf("Deleted pod %s is still present in the pod group members", tt.podToDelete.Name)
					}
					return true
				})
			}

			if pendingLen := q.pendingPodGroupPods.len(); pendingLen != tt.expectedPodsInPending {
				t.Errorf("Expected pod group to have %d pods in pendingPodGroupPods, got %d", tt.expectedPodsInPending, pendingLen)
			}
			if q.pendingPodGroupPods.has(tt.podToDelete) {
				t.Errorf("Deleted pod %s is still present in pendingPodGroupPods map", tt.podToDelete.Name)
			}

			for _, pod := range tt.expectedInIncomplete {
				if !q.incompletePodGroupPods.has(pod) {
					t.Errorf("Expected pod %s in incompletePodGroupPods", pod.Name)
				}
			}
			if q.incompletePodGroupPods.len() != len(tt.expectedInIncomplete) {
				t.Errorf("Expected incompletePodGroupPods size to be %d, got %d", len(tt.expectedInIncomplete), q.incompletePodGroupPods.len())
			}
		})
	}
}

// TestUpdatePodGroupMember 验证更新 PodGroup 成员时实体在队列中的变化。
func TestUpdatePodGroupMember(t *testing.T) {
	pgName := "pg-test"
	p1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").Label("allow", "").PodGroupName(pgName).Obj()
	p2 := st.MakePod().Name("pod2").Namespace("ns1").UID("pod2").Label("allow", "").PodGroupName(pgName).Obj()
	updatedP2 := st.MakePod().Name("pod2").Namespace("ns1").UID("pod2").Label("allow", "").Label("update", "true").PodGroupName(pgName).Obj()
	nominatedP2 := st.MakePod().Name("pod2").Namespace("ns1").UID("pod2").Label("allow", "").Label("update", "true").NominatedNodeName("node1").PodGroupName(pgName).Obj()
	gatedP1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").PodGroupName(pgName).Obj()
	ungatedP1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").Label("allow", "").PodGroupName(pgName).Obj()
	updatedGatedP1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").Label("updated", "true").PodGroupName(pgName).Obj()
	gatedP2 := st.MakePod().Name("pod2").Namespace("ns1").UID("pod2").PodGroupName(pgName).Obj()
	notFoundPod := st.MakePod().Name("pod-not-found").Namespace("ns1").UID("pod-not-found").Label("allow", "").PodGroupName(pgName).Obj()
	updatedNotFoundPod := st.MakePod().Name("pod-not-found").Namespace("ns1").UID("pod-not-found").Label("allow", "").Label("updated", "true").PodGroupName(pgName).Obj()

	tests := []struct {
		name                    string
		initialPods             []*v1.Pod
		initialState            initialQueueState
		pendingPods             []*v1.Pod
		oldPod                  *v1.Pod
		newPod                  *v1.Pod
		expectedInActiveQ       bool
		expectedInUnschedulable bool
		expectedInBackoffQ      bool
		expectedPodsInPending   int
		expectedInIncomplete    *v1.Pod
		expectedGated           bool
		expectedGroupSize       int
		expectedNominatedPods   map[types.UID]string
	}{
		{
			name:              "update pod in activeQ, keeps pod group in activeQ",
			initialPods:       []*v1.Pod{p1, p2},
			initialState:      stateActive,
			oldPod:            p2,
			newPod:            updatedP2,
			expectedInActiveQ: true,
			expectedGroupSize: 2,
		},
		{
			name:               "update pod in backoffQ, keeps pod group in backoffQ",
			initialPods:        []*v1.Pod{p1, p2},
			initialState:       stateBackoff,
			oldPod:             p2,
			newPod:             updatedP2,
			expectedInBackoffQ: true,
			expectedGroupSize:  2,
		},
		{
			name:              "update pod in unschedulableEntities moves it to activeQ (ungated pod group)",
			initialPods:       []*v1.Pod{p1, p2},
			initialState:      stateUnschedulable,
			oldPod:            p2,
			newPod:            updatedP2,
			expectedInActiveQ: true,
			expectedGroupSize: 2,
		},
		{
			name:                  "update pod in pendingPodGroupPods (group in flight), updates pod in pending buffer",
			initialPods:           []*v1.Pod{p1},
			initialState:          statePopped,
			pendingPods:           []*v1.Pod{p2},
			oldPod:                p2,
			newPod:                updatedP2,
			expectedPodsInPending: 1,
		},
		{
			name:              "gated pod update (removal of gate), ungates and moves pod group to activeQ",
			initialPods:       []*v1.Pod{gatedP1},
			initialState:      stateGated,
			oldPod:            gatedP1,
			newPod:            ungatedP1,
			expectedInActiveQ: true,
			expectedGated:     false,
			expectedGroupSize: 1,
		},
		{
			name:                    "gated pod update without removal of gate, keeps pod group in unschedulableEntities as gated",
			initialPods:             []*v1.Pod{gatedP1},
			initialState:            stateGated,
			oldPod:                  gatedP1,
			newPod:                  updatedGatedP1,
			expectedInUnschedulable: true,
			expectedGated:           true,
			expectedGroupSize:       1,
		},
		{
			name:                    "ungate one pod but another remains gated, keeps pod group in unschedulableEntities as gated",
			initialPods:             []*v1.Pod{gatedP1, gatedP2},
			initialState:            stateGated,
			oldPod:                  gatedP1,
			newPod:                  ungatedP1,
			expectedInUnschedulable: true,
			expectedGated:           true,
			expectedGroupSize:       2,
		},
		{
			name:              "updating pod that doesn't exist in any queue, creates new pod group in activeQ",
			initialPods:       nil,
			initialState:      stateActive,
			oldPod:            notFoundPod,
			newPod:            updatedNotFoundPod,
			expectedInActiveQ: true,
			expectedGated:     false,
			expectedGroupSize: 1,
		},
		{
			name:              "update pod to nominated in activeQ, keeps pod group in activeQ",
			initialPods:       []*v1.Pod{p1, p2},
			initialState:      stateActive,
			oldPod:            p2,
			newPod:            nominatedP2,
			expectedInActiveQ: true,
			expectedGroupSize: 2,
			expectedNominatedPods: map[types.UID]string{
				"pod2": "node1",
			},
		},
		{
			name:               "update pod to nominated in backoffQ, keeps pod group in backoffQ",
			initialPods:        []*v1.Pod{p1, p2},
			initialState:       stateBackoff,
			oldPod:             p2,
			newPod:             nominatedP2,
			expectedInBackoffQ: true,
			expectedGroupSize:  2,
			expectedNominatedPods: map[types.UID]string{
				"pod2": "node1",
			},
		},
		{
			name:              "update pod to nominated in unschedulableEntities moves it to activeQ (ungated pod group)",
			initialPods:       []*v1.Pod{p1, p2},
			initialState:      stateUnschedulable,
			oldPod:            p2,
			newPod:            nominatedP2,
			expectedInActiveQ: true,
			expectedGroupSize: 2,
			expectedNominatedPods: map[types.UID]string{
				"pod2": "node1",
			},
		},
		{
			name:                  "update pod to nominated in pendingPodGroupPods (group in flight), updates pod in pending buffer",
			initialPods:           []*v1.Pod{p1},
			initialState:          statePopped,
			pendingPods:           []*v1.Pod{p2},
			oldPod:                p2,
			newPod:                nominatedP2,
			expectedPodsInPending: 1,
			expectedNominatedPods: map[types.UID]string{
				"pod2": "node1",
			},
		},
		{
			name:                 "update pod in incompletePodGroupPods",
			initialPods:          []*v1.Pod{p2},
			initialState:         stateIncomplete,
			oldPod:               p2,
			newPod:               updatedP2,
			expectedInIncomplete: updatedP2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload: true,
			})

			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			preEnqueueMap := map[string]map[string]fwk.PreEnqueuePlugin{
				"": {
					"preEnqueuePlugin": &preEnqueuePlugin{allowlists: []string{"allow"}},
				},
			}
			q := NewTestQueueWithObjects(ctx, newDefaultQueueSort(), []runtime.Object{tt.newPod}, WithPreEnqueuePluginMap(preEnqueueMap))
			podGroup := st.MakePodGroup().Name(pgName).Namespace("ns1").Obj()

			setupInitialPodGroupState(t, ctx, q, tt.initialPods, tt.initialState, podGroup)
			// 加入 pending Pod
			for _, pod := range tt.pendingPods {
				q.Add(ctx, pod)
			}

			// 执行更新
			q.Update(ctx, tt.oldPod, tt.newPod)

			// 验证条件
			pgLookup := newQueuedPodGroupInfoForLookup(podGroup.Namespace, podGroup.Name, fwk.PodGroupKeyType)

			if inActive := q.activeQ.has(pgLookup); inActive != tt.expectedInActiveQ {
				t.Errorf("Expected target pod group in activeQ: %v, got %v", tt.expectedInActiveQ, inActive)
			}
			var entity framework.QueuedEntityInfo
			if tt.expectedInActiveQ {
				entity, _ = q.activeQ.get(pgLookup)
			}

			if inBackoff := q.backoffQ.has(pgLookup); inBackoff != tt.expectedInBackoffQ {
				t.Errorf("Expected target pod group in backoffQ: %v, got %v", tt.expectedInBackoffQ, inBackoff)
			}
			if tt.expectedInBackoffQ {
				entity, _ = q.backoffQ.get(pgLookup)
			}

			unschedulableEntity := q.unschedulableEntities.get(pgLookup)
			inUnschedulable := unschedulableEntity != nil
			if inUnschedulable != tt.expectedInUnschedulable {
				t.Errorf("Expected target pod group in unschedulableEntities: %v, got %v", tt.expectedInUnschedulable, inUnschedulable)
			}
			if tt.expectedInUnschedulable {
				entity = unschedulableEntity
			}

			if entity != nil {
				if isGated := entity.Gated(); isGated != tt.expectedGated {
					t.Errorf("Expected pod group to be gated: %v, got %v", tt.expectedGated, isGated)
				}
				if size := entity.Size(); size != tt.expectedGroupSize {
					t.Errorf("Expected pod group to be of size: %d, got %d", tt.expectedGroupSize, size)
				}

				// 验证被更新 Pod 确实被更新
				foundUpdated := false
				entity.(*framework.QueuedPodGroupInfo).ForEachPodInfo(func(pInfo *framework.QueuedPodInfo) bool {
					if pInfo.Pod.Name == tt.newPod.Name {
						foundUpdated = true
						if diff := cmp.Diff(tt.newPod, pInfo.Pod); diff != "" {
							t.Errorf("Queued member pod differs from newPod (-want +got):\n%s", diff)
						}
						return false
					}
					return true
				})
				if !foundUpdated {
					t.Errorf("Updated pod %s was not found in the pod group members", tt.newPod.Name)
				}
			}

			if pendingLen := q.pendingPodGroupPods.len(); pendingLen != tt.expectedPodsInPending {
				t.Errorf("Expected pod group to have %d pods in pendingPodGroupPods, got %d", tt.expectedPodsInPending, pendingLen)
			}
			if tt.expectedPodsInPending > 0 {
				pInfo := q.pendingPodGroupPods.get(tt.newPod)
				if diff := cmp.Diff(tt.newPod, pInfo.Pod); diff != "" {
					t.Errorf("Pending member pod differs from newPod (-want +got):\n%s", diff)
				}
			}

			if tt.expectedInIncomplete != nil {
				pInfo := q.incompletePodGroupPods.get(tt.expectedInIncomplete)
				if diff := cmp.Diff(tt.expectedInIncomplete, pInfo.Pod); diff != "" {
					t.Errorf("Unexpected pod in incompletePodGroupPods (-want +got):\n%s", diff)
				}
			} else if incompleteLen := q.incompletePodGroupPods.len(); incompleteLen != 0 {
				t.Errorf("Expected no pods in incompletePodGroupPods, got %v", incompleteLen)
			}

			if diff := cmp.Diff(tt.expectedNominatedPods, q.nominatedPodToNode, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Unexpected nominated pods (-want +got):\n%s", diff)
			}
		})
	}
}

// TestActivatePodGroupMember 验证激活 PodGroup 成员及整组实体的行为。
func TestActivatePodGroupMember(t *testing.T) {
	pgName := "pg-test"
	p1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").Label("allow", "").PodGroupName(pgName).Obj()
	p2 := st.MakePod().Name("pod2").Namespace("ns1").UID("pod2").Label("allow", "").PodGroupName(pgName).Obj()
	gatedP1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").PodGroupName(pgName).Obj()
	notFoundPod := st.MakePod().Name("pod-not-found").Namespace("ns1").UID("pod-not-found").Label("allow", "").PodGroupName(pgName).Obj()

	tests := []struct {
		name                       string
		initialPods                []*v1.Pod
		initialState               initialQueueState
		pendingPods                []*v1.Pod
		podToActivate              *v1.Pod
		expectedInActiveQ          bool
		expectedInUnschedulable    bool
		expectedPodsInPending      int
		expectedInIncomplete       []*v1.Pod
		expectedGated              bool
		expectedForceActivateEvent bool
	}{
		{
			name:              "activate pod group in unschedulableEntities (ungated), moves pod group to activeQ",
			initialPods:       []*v1.Pod{p1, p2},
			initialState:      stateUnschedulable,
			podToActivate:     p1,
			expectedInActiveQ: true,
		},
		{
			name:                    "activate pod group in unschedulableEntities (gated), keeps pod group in unschedulableEntities as gated",
			initialPods:             []*v1.Pod{gatedP1},
			initialState:            stateGated,
			podToActivate:           gatedP1,
			expectedInUnschedulable: true,
			expectedGated:           true,
		},
		{
			name:              "activate pod group in backoffQ, moves pod group to activeQ",
			initialPods:       []*v1.Pod{p1, p2},
			initialState:      stateBackoff,
			podToActivate:     p1,
			expectedInActiveQ: true,
		},
		{
			name:                       "activate pod group in flight (popped), pod group remains in flight but tracks event",
			initialPods:                []*v1.Pod{p1, p2},
			initialState:               statePopped,
			podToActivate:              p1,
			expectedForceActivateEvent: true,
		},
		{
			name:              "activating pod that does not exist in any queue or in flight is ignored",
			podToActivate:     notFoundPod,
			expectedInActiveQ: false,
		},
		// {
		// 	name:                           "activate pending pod when pod group is in flight, pods remain pending in pendingPodGroupPods map, but track event",
		// 	initialPods:                    []*v1.Pod{p1},
		// 	initialState:                   statePopped,
		// 	pendingPods:                    []*v1.Pod{p2},
		// 	podToActivate:                  p2,
		// 	expectedForceActivateEvent:     true,
		// 	expectedPodsInPending:          1,
		// },
		{
			name:                 "activate pod in incompletePodGroupPods is a no-op",
			initialPods:          []*v1.Pod{p1, p2},
			initialState:         stateIncomplete,
			podToActivate:        p1,
			expectedInIncomplete: []*v1.Pod{p1, p2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload: true,
			})

			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			preEnqueueMap := map[string]map[string]fwk.PreEnqueuePlugin{
				"": {
					"preEnqueuePlugin": &preEnqueuePlugin{allowlists: []string{"allow"}},
				},
			}
			q := NewTestQueue(ctx, newDefaultQueueSort(), WithPreEnqueuePluginMap(preEnqueueMap))
			podGroup := st.MakePodGroup().Name(pgName).Namespace("ns1").Obj()

			setupInitialPodGroupState(t, ctx, q, tt.initialPods, tt.initialState, podGroup)
			// 加入 pending Pod
			for _, pod := range tt.pendingPods {
				q.Add(ctx, pod)
			}

			// 激活 Pod
			q.Activate(logger, map[string]*v1.Pod{string(tt.podToActivate.UID): tt.podToActivate})

			// 验证条件
			pgLookup := newQueuedPodGroupInfoForLookup(podGroup.Namespace, podGroup.Name, fwk.PodGroupKeyType)

			if inActive := q.activeQ.has(pgLookup); inActive != tt.expectedInActiveQ {
				t.Errorf("Expected target pod group in activeQ: %v, got %v", tt.expectedInActiveQ, inActive)
			}
			var entity framework.QueuedEntityInfo
			if tt.expectedInActiveQ {
				entity, _ = q.activeQ.get(pgLookup)
			}

			if q.backoffQ.has(pgLookup) {
				t.Errorf("Expected target pod group not to be present in backoffQ")
			}

			unschedulableEntity := q.unschedulableEntities.get(pgLookup)
			inUnschedulable := unschedulableEntity != nil
			if inUnschedulable != tt.expectedInUnschedulable {
				t.Errorf("Expected target pod group in unschedulableEntities: %v, got %v", tt.expectedInUnschedulable, inUnschedulable)
			}
			if tt.expectedInUnschedulable {
				entity = unschedulableEntity
			}

			if entity != nil {
				if isGated := entity.Gated(); isGated != tt.expectedGated {
					t.Errorf("Expected pod group to be gated: %v, got %v", tt.expectedGated, isGated)
				}
			}

			if pendingLen := q.pendingPodGroupPods.len(); pendingLen != tt.expectedPodsInPending {
				t.Errorf("Expected pod group to have %d pods in pendingPodGroupPods, got %d", tt.expectedPodsInPending, pendingLen)
			}
			if tt.expectedPodsInPending > 0 && !q.pendingPodGroupPods.has(tt.podToActivate) {
				t.Errorf("Pod %s was not found in pendingPodGroupPods map", tt.podToActivate.Name)
			}

			for _, pod := range tt.expectedInIncomplete {
				if !q.incompletePodGroupPods.has(pod) {
					t.Errorf("Expected pod %s in incompletePodGroupPods", pod.Name)
				}
			}
			if q.incompletePodGroupPods.len() != len(tt.expectedInIncomplete) {
				t.Errorf("Expected incompletePodGroupPods size to be %d, got %d", len(tt.expectedInIncomplete), q.incompletePodGroupPods.len())
			}

			if tt.expectedForceActivateEvent {
				foundEvent := false
				for _, ev := range q.activeQ.listInFlightEvents() {
					clusterEvent, ok := ev.(*clusterEvent)
					if !ok {
						continue
					}
					if clusterEvent.event.Label() == framework.EventForceActivate.Label() {
						foundEvent = true
						break
					}
				}
				if !foundEvent {
					t.Errorf("Expected ForceActivate in-flight event to be tracked, but it wasn't")
				}
			}
		})
	}
}

// TestMoveAllToActiveOrBackoffQueuePodGroupMember 验证 move 事件下 PodGroup 成员实体在各队列间的移动。
func TestMoveAllToActiveOrBackoffQueuePodGroupMember(t *testing.T) {
	pgName := "pg-test"
	p1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").Label("allow", "").PodGroupName(pgName).Obj()
	p2 := st.MakePod().Name("pod2").Namespace("ns1").UID("pod2").Label("allow", "").PodGroupName(pgName).Obj()
	gatedP1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").PodGroupName(pgName).Obj()

	tests := []struct {
		name                    string
		initialPods             []*v1.Pod
		initialState            initialQueueState
		event                   fwk.ClusterEvent
		preCheck                PreEnqueueCheck
		expectedInActiveQ       bool
		expectedInUnschedulable bool
		expectedGated           bool
		expectedGroupSize       int
		expectedInFlightEvent   bool
	}{
		{
			name:              "event of interest moves ungated pod group from unschedulableEntities to activeQ",
			initialPods:       []*v1.Pod{p1, p2},
			initialState:      stateUnschedulable,
			event:             fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add},
			expectedInActiveQ: true,
			expectedGroupSize: 2,
		},
		{
			name:                    "event not of interest keeps pod group in unschedulableEntities",
			initialPods:             []*v1.Pod{p1, p2},
			initialState:            stateUnschedulable,
			event:                   fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Delete},
			expectedInUnschedulable: true,
			expectedGroupSize:       2,
		},
		// {
		// 	name:              "gated pod group matching gating event moves the pod group to activeQ",
		// 	initialPods:       []*v1.Pod{gatedP1, p2},
		// 	initialState:      stateGated,
		// 	event:             fwk.ClusterEvent{Resource: fwk.Pod, ActionType: fwk.Add},
		// 	expectedInActiveQ: true,
		// 	expectedGroupSize: 2,
		// },
		{
			name:                    "gated pod group not matching gating event stays in unschedulableEntities as gated",
			initialPods:             []*v1.Pod{gatedP1, p2},
			initialState:            stateGated,
			event:                   fwk.ClusterEvent{Resource: fwk.Pod, ActionType: fwk.Delete},
			expectedInUnschedulable: true,
			expectedGated:           true,
			expectedGroupSize:       2,
		},
		{
			name:                    "gated pod group matching event for ungated pod stays in unschedulableEntities as gated",
			initialPods:             []*v1.Pod{gatedP1, p2},
			initialState:            stateGated,
			event:                   fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add},
			expectedInUnschedulable: true,
			expectedGated:           true,
			expectedGroupSize:       2,
		},
		{
			name:                  "event received while pod group is in flight is tracked in inFlightEvents",
			initialPods:           []*v1.Pod{p1, p2},
			initialState:          statePopped,
			event:                 fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add},
			expectedInFlightEvent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload: true,
			})

			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			preEnqueueMap := map[string]map[string]fwk.PreEnqueuePlugin{
				"": {
					"preEnqueuePlugin": &preEnqueuePlugin{allowlists: []string{"allow"}},
				},
			}

			m := makeEmptyQueueingHintMapPerProfile()
			m[""] = map[fwk.ClusterEvent][]*QueueingHintFunction{
				{Resource: fwk.Pod, ActionType: fwk.Add}: {
					{
						PluginName:     "preEnqueuePlugin",
						QueueingHintFn: queueHintReturnQueue,
					},
				},
				{Resource: fwk.Node, ActionType: fwk.Add}: {
					{
						PluginName: "otherPlugin",
						QueueingHintFn: func(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
							if pod.Name == "pod1" {
								return fwk.Queue, nil
							}
							return fwk.QueueSkip, nil
						},
					},
				},
			}

			q := NewTestQueue(ctx, newDefaultQueueSort(), WithPreEnqueuePluginMap(preEnqueueMap), WithQueueingHintMapPerProfile(m))
			podGroup := st.MakePodGroup().Name(pgName).Namespace("ns1").Obj()

			setupInitialPodGroupState(t, ctx, q, tt.initialPods, tt.initialState, podGroup)

			// 用给定事件调用 MoveAllToActiveOrBackoffQueue
			q.MoveAllToActiveOrBackoffQueue(logger, tt.event, nil, nil, tt.preCheck)

			// 验证条件
			pgLookup := newQueuedPodGroupInfoForLookup(podGroup.Namespace, podGroup.Name, fwk.PodGroupKeyType)

			if inActive := q.activeQ.has(pgLookup); inActive != tt.expectedInActiveQ {
				t.Errorf("Expected target pod group in activeQ: %v, got %v", tt.expectedInActiveQ, inActive)
			}
			var entity framework.QueuedEntityInfo
			if tt.expectedInActiveQ {
				entity, _ = q.activeQ.get(pgLookup)
			}

			if q.backoffQ.has(pgLookup) {
				t.Errorf("Expected target pod group not to be present in backoffQ")
			}

			unschedulableEntity := q.unschedulableEntities.get(pgLookup)
			inUnschedulable := unschedulableEntity != nil
			if inUnschedulable != tt.expectedInUnschedulable {
				t.Errorf("Expected target pod group in unschedulableEntities: %v, got %v", tt.expectedInUnschedulable, inUnschedulable)
			}
			if tt.expectedInUnschedulable {
				entity = unschedulableEntity
			}

			if entity != nil {
				if isGated := entity.Gated(); isGated != tt.expectedGated {
					t.Errorf("Expected pod group to be gated: %v, got %v", tt.expectedGated, isGated)
				}
				if size := entity.Size(); size != tt.expectedGroupSize {
					t.Errorf("Expected pod group to be of size: %d, got %d", tt.expectedGroupSize, size)
				}

				if tt.expectedGroupSize > 0 {
					foundPod := false
					entity.(*framework.QueuedPodGroupInfo).ForEachPodInfo(func(pInfo *framework.QueuedPodInfo) bool {
						if pInfo.Pod.Name == tt.initialPods[0].Name {
							foundPod = true
							return false
						}
						return true
					})
					if !foundPod {
						t.Errorf("Pod %s was not found in the pod group members", tt.initialPods[0].Name)
					}
				}
			}

			if tt.expectedInFlightEvent {
				foundEvent := false
				for _, ev := range q.activeQ.listInFlightEvents() {
					clusterEvent, ok := ev.(*clusterEvent)
					if !ok {
						continue
					}
					if clusterEvent.event.Label() == tt.event.Label() {
						foundEvent = true
						break
					}
				}
				if !foundEvent {
					t.Errorf("Expected in-flight event %s to be tracked, but it wasn't", tt.event.Label())
				}
				return
			}
		})
	}
}

// TestFlushBackoffQCompletedPodGroupMember 验证 flush backoffQ 对 PodGroup 成员的影响。
func TestFlushBackoffQCompletedPodGroupMember(t *testing.T) {
	pgName := "pg-test"
	p1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").Label("allow", "").PodGroupName(pgName).Obj()
	p2 := st.MakePod().Name("pod2").Namespace("ns1").UID("pod2").Label("allow", "").PodGroupName(pgName).Obj()

	tests := []struct {
		name               string
		initialPods        []*v1.Pod
		initialState       initialQueueState
		advanceClock       time.Duration
		expectedInActiveQ  bool
		expectedInBackoffQ bool
		expectedGroupSize  int
	}{
		{
			name:              "flushing backoffQ after backoff completes moves pod group to activeQ",
			initialPods:       []*v1.Pod{p1, p2},
			initialState:      stateBackoff,
			advanceClock:      10 * time.Second,
			expectedInActiveQ: true,
			expectedGroupSize: 2,
		},
		{
			name:               "flushing backoffQ before backoff completes keeps pod group in backoffQ",
			initialPods:        []*v1.Pod{p1, p2},
			initialState:       stateBackoff,
			advanceClock:       0,
			expectedInBackoffQ: true,
			expectedGroupSize:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload: true,
			})

			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			preEnqueueMap := map[string]map[string]fwk.PreEnqueuePlugin{
				"": {
					"preEnqueuePlugin": &preEnqueuePlugin{allowlists: []string{"allow"}},
				},
			}

			fakeClock := testingclock.NewFakeClock(time.Now())
			q := NewTestQueue(ctx, newDefaultQueueSort(), WithPreEnqueuePluginMap(preEnqueueMap), WithClock(fakeClock))
			podGroup := st.MakePodGroup().Name(pgName).Namespace("ns1").Obj()

			setupInitialPodGroupState(t, ctx, q, tt.initialPods, tt.initialState, podGroup)

			if tt.advanceClock > 0 {
				fakeClock.Step(tt.advanceClock)
			}

			// flush backoff 队列
			q.flushBackoffQCompleted(logger)

			// 验证条件
			pgLookup := newQueuedPodGroupInfoForLookup(podGroup.Namespace, podGroup.Name, fwk.PodGroupKeyType)

			if inActive := q.activeQ.has(pgLookup); inActive != tt.expectedInActiveQ {
				t.Errorf("Expected target pod group in activeQ: %v, got %v", tt.expectedInActiveQ, inActive)
			}
			var entity framework.QueuedEntityInfo
			if tt.expectedInActiveQ {
				entity, _ = q.activeQ.get(pgLookup)
			}

			if inBackoff := q.backoffQ.has(pgLookup); inBackoff != tt.expectedInBackoffQ {
				t.Errorf("Expected target pod group in backoffQ: %v, got %v", tt.expectedInBackoffQ, inBackoff)
			}
			if tt.expectedInBackoffQ {
				entity, _ = q.backoffQ.get(pgLookup)
			}

			if q.unschedulableEntities.get(pgLookup) != nil {
				t.Errorf("Expected target pod group not to be present in unschedulableEntities")
			}

			if entity != nil {
				if size := entity.Size(); size != tt.expectedGroupSize {
					t.Errorf("Expected pod group to be of size: %d, got %d", tt.expectedGroupSize, size)
				}
			}
		})
	}
}

// TestFlushUnschedulableEntitiesLeftoverPodGroupMember 验证 flush unschedulable 对 PodGroup 成员的影响。
func TestFlushUnschedulableEntitiesLeftoverPodGroupMember(t *testing.T) {
	pgName := "pg-test"
	p1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").Label("allow", "").PodGroupName(pgName).Obj()
	p2 := st.MakePod().Name("pod2").Namespace("ns1").UID("pod2").Label("allow", "").PodGroupName(pgName).Obj()
	gatedP1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").PodGroupName(pgName).Obj()

	tests := []struct {
		name                    string
		initialPods             []*v1.Pod
		initialState            initialQueueState
		advanceClock            time.Duration
		expectedInActiveQ       bool
		expectedInUnschedulable bool
		expectedGated           bool
		expectedGroupSize       int
	}{
		{
			name:              "flushing unschedulable leftover after max duration moves ungated pod group to activeQ",
			initialPods:       []*v1.Pod{p1, p2},
			initialState:      stateUnschedulable,
			advanceClock:      DefaultPodMaxInUnschedulablePodsDuration + time.Second,
			expectedInActiveQ: true,
			expectedGroupSize: 2,
		},
		{
			name:                    "flushing unschedulable leftover before max duration keeps pod group in unschedulableEntities",
			initialPods:             []*v1.Pod{p1, p2},
			initialState:            stateUnschedulable,
			advanceClock:            0,
			expectedInUnschedulable: true,
			expectedGroupSize:       2,
		},
		{
			name:                    "flushing unschedulable leftover for gated pod group after max duration keeps it in unschedulableEntities as gated",
			initialPods:             []*v1.Pod{gatedP1, p2},
			initialState:            stateGated,
			advanceClock:            DefaultPodMaxInUnschedulablePodsDuration + time.Second,
			expectedInUnschedulable: true,
			expectedGated:           true,
			expectedGroupSize:       2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload: true,
			})

			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			preEnqueueMap := map[string]map[string]fwk.PreEnqueuePlugin{
				"": {
					"preEnqueuePlugin": &preEnqueuePlugin{allowlists: []string{"allow"}},
				},
			}

			fakeClock := testingclock.NewFakeClock(time.Now())
			q := NewTestQueue(ctx, newDefaultQueueSort(), WithPreEnqueuePluginMap(preEnqueueMap), WithClock(fakeClock))
			podGroup := st.MakePodGroup().Name(pgName).Namespace("ns1").Obj()

			setupInitialPodGroupState(t, ctx, q, tt.initialPods, tt.initialState, podGroup)

			if tt.advanceClock > 0 {
				fakeClock.Step(tt.advanceClock)
			}

			// flush unschedulable 实体
			q.flushUnschedulableEntitiesLeftover(logger)

			// 验证条件
			pgLookup := newQueuedPodGroupInfoForLookup(podGroup.Namespace, podGroup.Name, fwk.PodGroupKeyType)

			if inActive := q.activeQ.has(pgLookup); inActive != tt.expectedInActiveQ {
				t.Errorf("Expected target pod group in activeQ: %v, got %v", tt.expectedInActiveQ, inActive)
			}
			var entity framework.QueuedEntityInfo
			if tt.expectedInActiveQ {
				entity, _ = q.activeQ.get(pgLookup)
			}

			if q.backoffQ.has(pgLookup) {
				t.Errorf("Expected target pod group not to be present in backoffQ")
			}

			unschedulableEntity := q.unschedulableEntities.get(pgLookup)
			inUnschedulable := unschedulableEntity != nil
			if inUnschedulable != tt.expectedInUnschedulable {
				t.Errorf("Expected target pod group in unschedulableEntities: %v, got %v", tt.expectedInUnschedulable, inUnschedulable)
			}
			if tt.expectedInUnschedulable {
				entity = unschedulableEntity
			}

			if entity != nil {
				if isGated := entity.Gated(); isGated != tt.expectedGated {
					t.Errorf("Expected pod group to be gated: %v, got %v", tt.expectedGated, isGated)
				}
				if size := entity.Size(); size != tt.expectedGroupSize {
					t.Errorf("Expected pod group to be of size: %d, got %d", tt.expectedGroupSize, size)
				}
			}
		})
	}
}

// TestAddUnschedulablePodIfNotPresentPodGroupMember 验证 AddUnschedulablePodIfNotPresent 对 PodGroup 成员的处理。
func TestAddUnschedulablePodIfNotPresentPodGroupMember(t *testing.T) {
	pgName := "pg-test"
	p1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").Label("allow", "").PodGroupName(pgName).Obj()
	p2 := st.MakePod().Name("pod2").Namespace("ns1").UID("pod2").Label("allow", "").PodGroupName(pgName).Obj()
	p3 := st.MakePod().Name("pod3").Namespace("ns1").UID("pod3").Label("allow", "").PodGroupName(pgName).Obj()
	gatedP3 := st.MakePod().Name("pod3").Namespace("ns1").UID("pod3").PodGroupName(pgName).Obj()

	pInfo1 := &framework.QueuedPodInfo{
		PodInfo: mustNewPodInfo(p1),
		QueueingParams: framework.QueueingParams{
			UnschedulablePlugins: sets.New("fooPlugin"),
		},
	}
	pInfo1NoPlugins := &framework.QueuedPodInfo{
		PodInfo: mustNewPodInfo(p1),
	}
	pInfo2 := &framework.QueuedPodInfo{
		PodInfo: mustNewPodInfo(p2),
		QueueingParams: framework.QueueingParams{
			UnschedulablePlugins: sets.New("barPlugin"),
		},
	}
	gatedPInfo3 := &framework.QueuedPodInfo{
		PodInfo: mustNewPodInfo(gatedP3),
		QueueingParams: framework.QueueingParams{
			UnschedulablePlugins: sets.New("bazPlugin"),
		},
	}

	tests := []struct {
		name                    string
		initialPods             []*v1.Pod
		initialState            initialQueueState
		clearLastPopped         bool
		deletePodGroup          bool
		podsToAdd               []*framework.QueuedPodInfo
		expectedPodsInPending   int
		expectedInActiveQ       bool
		expectedInUnschedulable bool
		expectedInBackoffQ      bool
		expectedInIncomplete    []*v1.Pod
		expectedGated           bool
		expectedGroupSize       int
	}{
		{
			name:                  "single pod group member added with plugin to pending when group is in flight",
			initialPods:           []*v1.Pod{p1, p2},
			initialState:          statePopped,
			podsToAdd:             []*framework.QueuedPodInfo{pInfo1},
			expectedPodsInPending: 1,
		},
		{
			name:                  "single pod group member added with no plugins (consecutive error) to pending",
			initialPods:           []*v1.Pod{p1, p2},
			initialState:          statePopped,
			podsToAdd:             []*framework.QueuedPodInfo{pInfo1NoPlugins},
			expectedPodsInPending: 1,
		},
		{
			name:                  "multiple pod group members, including gated added with plugins sequentially to pending when group is in flight",
			initialPods:           []*v1.Pod{p1, p2, p3},
			initialState:          statePopped,
			podsToAdd:             []*framework.QueuedPodInfo{pInfo1, pInfo2, gatedPInfo3},
			expectedPodsInPending: 3,
		},
		{
			name:              "pod group is not last popped entity and no group exists, a new group is created and added to activeQ",
			initialPods:       nil,
			podsToAdd:         []*framework.QueuedPodInfo{pInfo1},
			expectedInActiveQ: true,
			expectedGroupSize: 1,
		},
		{
			name:                    "pod group exists in unschedulableEntities, pod is added and group remains in unschedulableEntities",
			initialPods:             []*v1.Pod{p2},
			initialState:            stateUnschedulable,
			clearLastPopped:         true,
			podsToAdd:               []*framework.QueuedPodInfo{pInfo1},
			expectedInUnschedulable: true,
			expectedGroupSize:       2,
		},
		{
			name:              "pod group exists in activeQ, pod is added and group remains in activeQ",
			initialPods:       []*v1.Pod{p2},
			initialState:      stateActive,
			clearLastPopped:   true,
			podsToAdd:         []*framework.QueuedPodInfo{pInfo1},
			expectedInActiveQ: true,
			expectedGroupSize: 2,
		},
		{
			name:               "pod group exists in backoffQ, pod is added and group remains in backoffQ",
			initialPods:        []*v1.Pod{p2},
			initialState:       stateBackoff,
			clearLastPopped:    true,
			podsToAdd:          []*framework.QueuedPodInfo{pInfo1},
			expectedInBackoffQ: true,
			expectedGroupSize:  2,
		},
		{
			name:                    "pod group exists in unschedulableEntities (gated), ungated pod is added and group remains gated in unschedulableEntities",
			initialPods:             []*v1.Pod{gatedP3},
			initialState:            stateGated,
			clearLastPopped:         true,
			podsToAdd:               []*framework.QueuedPodInfo{pInfo1},
			expectedInUnschedulable: true,
			expectedGated:           true,
			expectedGroupSize:       2,
		},
		{
			name:                 "requeuing a pod group members after PodGroup was deleted moves them to incompletePodGroupPods",
			initialPods:          []*v1.Pod{p1, p2},
			initialState:         statePopped,
			deletePodGroup:       true,
			podsToAdd:            []*framework.QueuedPodInfo{pInfo1, pInfo2},
			expectedInIncomplete: []*v1.Pod{p1, p2},
		},
		{
			name:                 "requeuing a pod group members after PodGroup was deleted and is not last popped entity moves them to incompletePodGroupPods",
			initialPods:          []*v1.Pod{p1, p2},
			initialState:         statePopped,
			clearLastPopped:      true,
			deletePodGroup:       true,
			podsToAdd:            []*framework.QueuedPodInfo{pInfo1, pInfo2},
			expectedInIncomplete: []*v1.Pod{p1, p2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload: true,
			})

			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			preEnqueueMap := map[string]map[string]fwk.PreEnqueuePlugin{
				"": {
					"preEnqueuePlugin": &preEnqueuePlugin{allowlists: []string{"allow"}},
				},
			}

			q := NewTestQueue(ctx, newDefaultQueueSort(), WithPreEnqueuePluginMap(preEnqueueMap))
			podGroup := st.MakePodGroup().Name(pgName).Namespace("ns1").Obj()

			setupInitialPodGroupState(t, ctx, q, tt.initialPods, tt.initialState, podGroup)

			if tt.clearLastPopped {
				q.activeQ.clearPoppedEntity()
			}

			if tt.deletePodGroup {
				q.DeletePodGroup(logger, podGroup)
			}

			// 加入不可调度 Pod
			for _, pInfo := range tt.podsToAdd {
				pInfoCloned := pInfo.DeepCopy()
				if err := q.AddUnschedulablePodIfNotPresent(logger, pInfoCloned, q.SchedulingCycle()); err != nil {
					t.Errorf("Failed to add unschedulable pods %s: %v", pInfoCloned.Pod.Name, err)
				}
			}

			// 验证条件
			pgLookup := newQueuedPodGroupInfoForLookup(podGroup.Namespace, podGroup.Name, fwk.PodGroupKeyType)

			if inActive := q.activeQ.has(pgLookup); inActive != tt.expectedInActiveQ {
				t.Errorf("Expected pod group in activeQ: %v, got %v", tt.expectedInActiveQ, inActive)
			}
			var entity framework.QueuedEntityInfo
			if tt.expectedInActiveQ {
				entity, _ = q.activeQ.get(pgLookup)
			}

			if inBackoff := q.backoffQ.has(pgLookup); inBackoff != tt.expectedInBackoffQ {
				t.Errorf("Expected pod group in backoffQ: %v, got %v", tt.expectedInBackoffQ, inBackoff)
			}
			if tt.expectedInBackoffQ {
				entity, _ = q.backoffQ.get(pgLookup)
			}

			unschedulableEntity := q.unschedulableEntities.get(pgLookup)
			inUnschedulable := unschedulableEntity != nil
			if inUnschedulable != tt.expectedInUnschedulable {
				t.Errorf("Expected pod group in unschedulableEntities: %v, got %v", tt.expectedInUnschedulable, inUnschedulable)
			}
			if tt.expectedInUnschedulable {
				entity = unschedulableEntity
			}

			if entity != nil {
				if isGated := entity.Gated(); isGated != tt.expectedGated {
					t.Errorf("Expected pod group to be gated: %v, got %v", tt.expectedGated, isGated)
				}
				if size := entity.Size(); size != tt.expectedGroupSize {
					t.Errorf("Expected pod group to be of size: %d, got %d", tt.expectedGroupSize, size)
				}
			}

			if pendingLen := q.pendingPodGroupPods.len(); pendingLen != tt.expectedPodsInPending {
				t.Errorf("Expected pod group to have %d pods in pendingPodGroupPods, got %d", tt.expectedPodsInPending, pendingLen)
			}

			for _, pod := range tt.expectedInIncomplete {
				if !q.incompletePodGroupPods.has(pod) {
					t.Errorf("Expected pod %s in incompletePodGroupPods", pod.Name)
				}
			}
			if q.incompletePodGroupPods.len() != len(tt.expectedInIncomplete) {
				t.Errorf("Expected incompletePodGroupPods size to be %d, got %d", len(tt.expectedInIncomplete), q.incompletePodGroupPods.len())
			}
		})
	}
}

// TestAddPodGroup 验证添加 PodGroup 实体时队列的状态维护。
func TestAddPodGroup(t *testing.T) {
	pgName := "pg-test"
	p1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").Label("allow", "").PodGroupName(pgName).Obj()
	p2 := st.MakePod().Name("pod2").Namespace("ns1").UID("pod2").Label("allow", "").PodGroupName(pgName).Obj()
	gatedP1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").PodGroupName(pgName).Obj()
	podGroup := st.MakePodGroup().Name(pgName).Namespace("ns1").Obj()

	tests := []struct {
		name                    string
		initialPods             []*v1.Pod
		initialState            initialQueueState
		expectedInActiveQ       bool
		expectedInUnschedulable bool
		expectedGated           bool
		expectedGroupSize       int
	}{
		{
			name:              "add pod group without waiting pods",
			initialState:      stateIncomplete,
			expectedInActiveQ: false,
		},
		{
			name:              "add pod group moves pods from incompletePodGroupPods to activeQ",
			initialPods:       []*v1.Pod{p1, p2},
			initialState:      stateIncomplete,
			expectedInActiveQ: true,
			expectedGroupSize: 2,
		},
		{
			name:                    "add pod group moves gated pods from incompletePodGroupPods to unschedulableEntities",
			initialPods:             []*v1.Pod{gatedP1, p2},
			initialState:            stateIncomplete,
			expectedInUnschedulable: true,
			expectedGated:           true,
			expectedGroupSize:       2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload: true,
			})

			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			// 配置一个 PreEnqueue 插件，只放行带 "allow" 标签的 Pod，其余都加 gate。
			preEnqueueMap := map[string]map[string]fwk.PreEnqueuePlugin{
				"": {
					"preEnqueuePlugin": &preEnqueuePlugin{allowlists: []string{"allow"}},
				},
			}
			q := NewTestQueue(ctx, newDefaultQueueSort(), WithPreEnqueuePluginMap(preEnqueueMap))

			setupInitialPodGroupState(t, ctx, q, tt.initialPods, tt.initialState, podGroup)

			q.AddPodGroup(logger, podGroup)

			pgLookup := &framework.QueuedPodGroupInfo{
				PodGroupInfo: &framework.PodGroupInfo{
					Namespace: podGroup.Namespace,
					Name:      podGroup.Name,
					Type:      fwk.PodGroupKeyType,
				},
			}
			gotPodGroup, ok := q.workloadForest.getPodGroup(podGroup)
			if !ok {
				t.Fatalf("Expected pod group to be in workloadForest")
			}
			if diff := cmp.Diff(podGroup, gotPodGroup); diff != "" {
				t.Errorf("Unexpected pod group object (-want +got):\n%s", diff)
			}

			if inActive := q.activeQ.has(pgLookup); inActive != tt.expectedInActiveQ {
				t.Errorf("Expected incoming pod in activeQ: %v, got %v", tt.expectedInActiveQ, inActive)
			}
			var entity framework.QueuedEntityInfo
			if tt.expectedInActiveQ {
				entity, _ = q.activeQ.get(pgLookup)
			}

			unschedulableEntity := q.unschedulableEntities.get(pgLookup)
			inUnschedulable := unschedulableEntity != nil
			if inUnschedulable != tt.expectedInUnschedulable {
				t.Errorf("Expected incoming pod in unschedulableEntities: %v, got %v", tt.expectedInUnschedulable, inUnschedulable)
			}
			if tt.expectedInUnschedulable {
				entity = unschedulableEntity
			}

			if entity != nil {
				if isGated := entity.Gated(); isGated != tt.expectedGated {
					t.Errorf("Expected pod group to be gated: %v, got %v", tt.expectedGated, isGated)
				}
				if size := entity.Size(); size != tt.expectedGroupSize {
					t.Errorf("Expected pod group to be of size: %d, got %d", tt.expectedGroupSize, size)
				}
				pgInfo := entity.(*framework.QueuedPodGroupInfo)
				if diff := cmp.Diff(podGroup, pgInfo.PodGroup); diff != "" {
					t.Errorf("Unexpected pod group object in QueuedPodGroupInfo (-want +got):\n%s", diff)
				}
			}

			for _, pod := range tt.initialPods {
				if q.incompletePodGroupPods.has(pod) {
					t.Errorf("Expected pod %s not to be present in incompletePodGroupPods", pod.Name)
				}
			}
		})
	}
}

// TestUpdatePodGroup 验证更新 PodGroup 实体时队列的状态维护。
func TestUpdatePodGroup(t *testing.T) {
	pgName := "pg-test"
	p1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").Label("allow", "").PodGroupName(pgName).Obj()
	p2 := st.MakePod().Name("pod2").Namespace("ns1").UID("pod2").Label("allow", "").PodGroupName(pgName).Obj()
	gatedP1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").PodGroupName(pgName).Obj()
	podGroup := st.MakePodGroup().Name(pgName).Namespace("ns1").MinCount(1).Obj()
	updatedPodGroup := st.MakePodGroup().Name(pgName).Namespace("ns1").MinCount(2).Obj()

	tests := []struct {
		name                          string
		initialPods                   []*v1.Pod
		initialState                  initialQueueState
		expectedInActiveQ             bool
		expectedInBackoffQ            bool
		expectedInUnschedulable       bool
		expectedInPendingPodGroupPods bool
		expectedGated                 bool
	}{
		{
			name:         "update pod group without pods",
			initialState: stateActive, // 只添加 PodGroup
		},
		{
			name:              "update active pod group",
			initialPods:       []*v1.Pod{p1, p2},
			initialState:      stateActive,
			expectedInActiveQ: true,
		},
		{
			name:               "update backoff pod group",
			initialPods:        []*v1.Pod{p1, p2},
			initialState:       stateBackoff,
			expectedInBackoffQ: true,
		},
		{
			name:                    "update unschedulable pod group",
			initialPods:             []*v1.Pod{p1, p2},
			initialState:            stateUnschedulable,
			expectedInUnschedulable: true,
		},
		{
			name:                    "update gated pod group",
			initialPods:             []*v1.Pod{gatedP1, p2},
			initialState:            stateGated,
			expectedInUnschedulable: true,
			expectedGated:           true,
		},
		{
			name:                          "update in-flight pod group",
			initialPods:                   []*v1.Pod{p1, p2},
			initialState:                  statePopped,
			expectedInPendingPodGroupPods: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload: true,
			})

			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			preEnqueueMap := map[string]map[string]fwk.PreEnqueuePlugin{
				"": {
					"preEnqueuePlugin": &preEnqueuePlugin{allowlists: []string{"allow"}},
				},
			}
			q := NewTestQueue(ctx, newDefaultQueueSort(), WithPreEnqueuePluginMap(preEnqueueMap))

			setupInitialPodGroupState(t, ctx, q, tt.initialPods, tt.initialState, podGroup)

			q.UpdatePodGroup(logger, updatedPodGroup)

			gotPodGroup, ok := q.workloadForest.getPodGroup(podGroup)
			if !ok {
				t.Fatalf("Expected pod group to be in workloadForest")
			}
			if diff := cmp.Diff(updatedPodGroup, gotPodGroup); diff != "" {
				t.Errorf("Unexpected pod group object (-want +got):\n%s", diff)
			}

			pgLookup := &framework.QueuedPodGroupInfo{
				PodGroupInfo: &framework.PodGroupInfo{
					Namespace: podGroup.Namespace,
					Name:      podGroup.Name,
					Type:      fwk.PodGroupKeyType,
				},
			}
			if inActive := q.activeQ.has(pgLookup); inActive != tt.expectedInActiveQ {
				t.Errorf("Expected target pod group in activeQ: %v, got %v", tt.expectedInActiveQ, inActive)
			}
			var entity framework.QueuedEntityInfo
			if tt.expectedInActiveQ {
				entity, _ = q.activeQ.get(pgLookup)
			}

			if inBackoff := q.backoffQ.has(pgLookup); inBackoff != tt.expectedInBackoffQ {
				t.Errorf("Expected target pod group in backoffQ: %v, got %v", tt.expectedInBackoffQ, inBackoff)
			}
			if tt.expectedInBackoffQ {
				entity, _ = q.backoffQ.get(pgLookup)
			}

			unschedulableEntity := q.unschedulableEntities.get(pgLookup)
			inUnschedulable := unschedulableEntity != nil
			if inUnschedulable != tt.expectedInUnschedulable {
				t.Errorf("Expected target pod group in unschedulableEntities: %v, got %v", tt.expectedInUnschedulable, inUnschedulable)
			}
			if tt.expectedInUnschedulable {
				entity = unschedulableEntity
			}

			if entity != nil {
				if isGated := entity.Gated(); isGated != tt.expectedGated {
					t.Errorf("Expected pod group to be gated: %v, got %v", tt.expectedGated, isGated)
				}
				pgInfo := entity.(*framework.QueuedPodGroupInfo)
				if diff := cmp.Diff(updatedPodGroup, pgInfo.PodGroup); diff != "" {
					t.Errorf("Unexpected pod group object in QueuedPodGroupInfo (-want +got):\n%s", diff)
				}
			}
		})
	}
}

// TestDeletePodGroup 验证删除 PodGroup 实体时队列的状态维护。
func TestDeletePodGroup(t *testing.T) {
	pgName := "pg-test"
	p1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").Label("allow", "").PodGroupName(pgName).Obj()
	p2 := st.MakePod().Name("pod2").Namespace("ns1").UID("pod2").Label("allow", "").PodGroupName(pgName).Obj()
	gatedP1 := st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").PodGroupName(pgName).Obj()
	podGroup := st.MakePodGroup().Name(pgName).Namespace("ns1").Obj()

	tests := []struct {
		name                 string
		initialPods          []*v1.Pod
		initialState         initialQueueState
		pendingPods          []*v1.Pod
		expectedInIncomplete []*v1.Pod
	}{
		{
			name:                 "delete active pod group moves pods to incompletePodGroupPods",
			initialPods:          []*v1.Pod{p1, p2},
			initialState:         stateActive,
			expectedInIncomplete: []*v1.Pod{p1, p2},
		},
		{
			name:                 "delete backoff pod group moves pods to incompletePodGroupPods",
			initialPods:          []*v1.Pod{p1, p2},
			initialState:         stateBackoff,
			expectedInIncomplete: []*v1.Pod{p1, p2},
		},
		{
			name:                 "delete unschedulable pod group moves pods to incompletePodGroupPods",
			initialPods:          []*v1.Pod{p1, p2},
			initialState:         stateUnschedulable,
			expectedInIncomplete: []*v1.Pod{p1, p2},
		},
		{
			name:                 "delete gated pod group moves pods to incompletePodGroupPods",
			initialPods:          []*v1.Pod{gatedP1, p2},
			initialState:         stateGated,
			expectedInIncomplete: []*v1.Pod{gatedP1, p2},
		},
		{
			name:                 "delete in-flight pod group moves pending pods to incompletePodGroupPods",
			initialPods:          []*v1.Pod{p1, p2},
			initialState:         statePopped,
			pendingPods:          []*v1.Pod{p1, p2},
			expectedInIncomplete: []*v1.Pod{p1, p2},
		},
		{
			name:                 "delete in-flight pod group with no pending pods",
			initialPods:          []*v1.Pod{p1, p2},
			initialState:         statePopped,
			expectedInIncomplete: []*v1.Pod{},
		},
		{
			name:                 "delete pod group with no pods",
			expectedInIncomplete: []*v1.Pod{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload: true,
			})

			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			preEnqueueMap := map[string]map[string]fwk.PreEnqueuePlugin{
				"": {
					"preEnqueuePlugin": &preEnqueuePlugin{allowlists: []string{"allow"}},
				},
			}
			q := NewTestQueue(ctx, newDefaultQueueSort(), WithPreEnqueuePluginMap(preEnqueueMap))

			setupInitialPodGroupState(t, ctx, q, tt.initialPods, tt.initialState, podGroup)

			for _, pod := range tt.pendingPods {
				q.Add(ctx, pod)
			}

			q.DeletePodGroup(logger, podGroup)

			_, ok := q.workloadForest.getPodGroup(podGroup)
			if ok {
				t.Errorf("Expected pod group not to be present in workloadForest")
			}

			pgLookup := newQueuedPodGroupInfoForLookup(podGroup.Namespace, podGroup.Name, fwk.PodGroupKeyType)
			if q.activeQ.has(pgLookup) {
				t.Errorf("Expected pod group not to be in activeQ")
			}
			if q.backoffQ.has(pgLookup) {
				t.Errorf("Expected pod group not to be in backoffQ")
			}
			if q.unschedulableEntities.get(pgLookup) != nil {
				t.Errorf("Expected pod group not to be in unschedulableEntities")
			}

			for _, pod := range tt.pendingPods {
				if q.pendingPodGroupPods.has(pod) {
					t.Errorf("Expected pod %s not to be in pendingPodGroupPods", pod.Name)
				}
			}

			for _, pod := range tt.expectedInIncomplete {
				if !q.incompletePodGroupPods.has(pod) {
					t.Errorf("Expected pod %s in incompletePodGroupPods", pod.Name)
				}
			}
			if q.incompletePodGroupPods.len() != len(tt.expectedInIncomplete) {
				t.Errorf("Expected incompletePodGroupPods size to be %d, got %d", len(tt.expectedInIncomplete), q.incompletePodGroupPods.len())
			}
		})
	}
}

// TestPriorityQueue_AddCompositePodGroup 验证 Add 对 CompositePodGroup（CPG）实体的处理。
func TestPriorityQueue_AddCompositePodGroup(t *testing.T) {
	tests := []struct {
		name                   string
		initialPodGroups       []*schedulingv1beta1.PodGroup
		initialCPGs            []*schedulingv1alpha3.CompositePodGroup
		initialPods            []*v1.Pod
		beforeAdd              func(ctx context.Context, q *PriorityQueue)
		cpgToAdd               *schedulingv1alpha3.CompositePodGroup
		expectedActiveQ        map[string][]string
		expectedIncompletePods []string
		expectedPendingPods    []string
	}{
		{
			name: "Root with pods",
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			cpgToAdd:               st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
			expectedActiveQ:        map[string][]string{"compositepodgroup/root-cpg": {"pod1"}},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{},
		},
		{
			name: "Root without pods",
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			cpgToAdd:               st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
			expectedActiveQ:        map[string][]string{},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{},
		},
		{
			name: "Root when another root exists",
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
				st.MakePodGroup().Name("pg2").Namespace("ns1").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod2").Namespace("ns1").UID("uid2").PodGroupName("pg2").Obj(),
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			cpgToAdd: st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
			expectedActiveQ: map[string][]string{
				"compositepodgroup/root-cpg": {"pod1"},
				"podgroup/pg2":               {"pod2"},
			},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{},
		},
		{
			name: "Non-root when parent is missing",
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("child-cpg").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			cpgToAdd:               st.MakeCompositePodGroup().Name("child-cpg").Namespace("ns1").ParentCompositePodGroup("parent-cpg").Obj(),
			expectedActiveQ:        map[string][]string{},
			expectedIncompletePods: []string{"pod1"},
			expectedPendingPods:    []string{},
		},
		{
			name: "Non-root when parent is root",
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("child-cpg").Obj(),
			},
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			cpgToAdd:               st.MakeCompositePodGroup().Name("child-cpg").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			expectedActiveQ:        map[string][]string{"compositepodgroup/root-cpg": {"pod1"}},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{},
		},
		{
			name: "Non-root when parent is root and is already in active queue with another pod",
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("child-cpg").Obj(),
				st.MakePodGroup().Name("pg2").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod2").Namespace("ns1").UID("uid2").PodGroupName("pg2").Obj(),
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			cpgToAdd:               st.MakeCompositePodGroup().Name("child-cpg").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			expectedActiveQ:        map[string][]string{"compositepodgroup/root-cpg": {"pod1", "pod2"}},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{},
		},
		{
			name: "Non-root when parent exists but root is missing",
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("child-cpg").Obj(),
			},
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("parent-cpg").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			cpgToAdd:               st.MakeCompositePodGroup().Name("child-cpg").Namespace("ns1").ParentCompositePodGroup("parent-cpg").Obj(),
			expectedActiveQ:        map[string][]string{},
			expectedIncompletePods: []string{"pod1"},
			expectedPendingPods:    []string{},
		},
		{
			name: "Non-root when parent is root and is currently in-flight (popped)",
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("child-cpg").Obj(),
				st.MakePodGroup().Name("pg2").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod2").Namespace("ns1").UID("uid2").PodGroupName("pg2").Obj(),
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			beforeAdd: func(ctx context.Context, q *PriorityQueue) {
				_, _ = q.Pop(klog.FromContext(ctx)) // 弹出 root-cpg，使其进入在途状态
			},
			cpgToAdd:               st.MakeCompositePodGroup().Name("child-cpg").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			expectedActiveQ:        map[string][]string{},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{"pod1"},
		},
		{
			name: "Root when another root is currently in-flight (popped)",
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
				st.MakePodGroup().Name("pg2").Namespace("ns1").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod2").Namespace("ns1").UID("uid2").PodGroupName("pg2").Obj(),
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			beforeAdd: func(ctx context.Context, q *PriorityQueue) {
				_, _ = q.Pop(klog.FromContext(ctx))
			},
			cpgToAdd: st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
			expectedActiveQ: map[string][]string{
				"compositepodgroup/root-cpg": {"pod1"},
			},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload:                 true,
				features.TopologyAwareWorkloadScheduling: true,
				features.CompositePodGroup:               true,
			})
			logger, ctx := ktesting.NewTestContext(t)

			q := NewTestQueue(ctx, newDefaultQueueSort())
			defer q.Close()

			for _, pg := range tt.initialPodGroups {
				q.AddPodGroup(logger, pg)
			}
			for _, cpg := range tt.initialCPGs {
				q.AddCompositePodGroup(logger, cpg)
			}
			for _, pod := range tt.initialPods {
				q.Add(ctx, pod)
			}
			if tt.beforeAdd != nil {
				tt.beforeAdd(ctx, q)
			}

			q.AddCompositePodGroup(logger, tt.cpgToAdd)

			cmpOpts := []cmp.Option{cmpopts.EquateEmpty(), cmpopts.SortSlices(func(a, b string) bool { return a < b })}
			if diff := cmp.Diff(tt.expectedActiveQ, getActivePodGroups(q), cmpOpts...); diff != "" {
				t.Errorf("Active queue pod groups mismatch (-want,+got):\n%s", diff)
			}

			incomplete := q.IncompletePodGroupPodsPods()
			var gotIncomplete []string
			for _, p := range incomplete {
				gotIncomplete = append(gotIncomplete, p.Name)
			}
			if diff := cmp.Diff(tt.expectedIncompletePods, gotIncomplete, cmpOpts...); diff != "" {
				t.Errorf("Incomplete pods mismatch (-want,+got):\n%s", diff)
			}

			pending := q.PendingPodGroupPods()
			var gotPending []string
			for _, p := range pending {
				gotPending = append(gotPending, p.Name)
			}
			if diff := cmp.Diff(tt.expectedPendingPods, gotPending, cmpOpts...); diff != "" {
				t.Errorf("Pending pods mismatch (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestPriorityQueue_UpdateCompositePodGroup 验证 Update 对 CompositePodGroup（CPG）实体的处理。
func TestPriorityQueue_UpdateCompositePodGroup(t *testing.T) {
	tests := []struct {
		name                   string
		initialCPGs            []*schedulingv1alpha3.CompositePodGroup
		initialPodGroups       []*schedulingv1beta1.PodGroup
		initialPods            []*v1.Pod
		cpgToUpdate            *schedulingv1alpha3.CompositePodGroup
		expectedActiveQ        map[string][]string
		expectedIncompletePods []string
		expectedPendingPods    []string
	}{
		{
			name: "Root when minCount is updated",
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").MinGroupCount(3).Obj(),
			},
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			cpgToUpdate:            st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").MinGroupCount(5).Obj(),
			expectedActiveQ:        map[string][]string{"compositepodgroup/root-cpg": {"pod1"}},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{},
		},
		{
			name: "Non-root when minCount is updated",
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
				st.MakeCompositePodGroup().Name("child-cpg").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("child-cpg").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			cpgToUpdate:            st.MakeCompositePodGroup().Name("child-cpg").Namespace("ns1").ParentCompositePodGroup("root-cpg").MinGroupCount(2).Obj(),
			expectedActiveQ:        map[string][]string{"compositepodgroup/root-cpg": {"pod1"}},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload:                 true,
				features.TopologyAwareWorkloadScheduling: true,
				features.CompositePodGroup:               true,
			})
			logger, ctx := ktesting.NewTestContext(t)

			q := NewTestQueue(ctx, newDefaultQueueSort())
			defer q.Close()

			for _, cpg := range tt.initialCPGs {
				q.AddCompositePodGroup(logger, cpg)
			}
			for _, pg := range tt.initialPodGroups {
				q.AddPodGroup(logger, pg)
			}
			for _, pod := range tt.initialPods {
				q.Add(ctx, pod)
			}

			q.UpdateCompositePodGroup(logger, tt.cpgToUpdate)

			cmpOpts := []cmp.Option{cmpopts.EquateEmpty(), cmpopts.SortSlices(func(a, b string) bool { return a < b })}
			if diff := cmp.Diff(tt.expectedActiveQ, getActivePodGroups(q), cmpOpts...); diff != "" {
				t.Errorf("Active queue pod groups mismatch (-want,+got):\n%s", diff)
			}

			incomplete := q.IncompletePodGroupPodsPods()
			var gotIncomplete []string
			for _, p := range incomplete {
				gotIncomplete = append(gotIncomplete, p.Name)
			}
			if diff := cmp.Diff(tt.expectedIncompletePods, gotIncomplete, cmpOpts...); diff != "" {
				t.Errorf("Incomplete pods mismatch (-want,+got):\n%s", diff)
			}

			pending := q.PendingPodGroupPods()
			var gotPending []string
			for _, p := range pending {
				gotPending = append(gotPending, p.Name)
			}
			if diff := cmp.Diff(tt.expectedPendingPods, gotPending, cmpOpts...); diff != "" {
				t.Errorf("Pending pods mismatch (-want,+got):\n%s", diff)
			}

			entity := findPodGroupInfoInActiveQ(q, tt.cpgToUpdate.Namespace, tt.cpgToUpdate.Name, fwk.CompositePodGroupKeyType)
			if entity == nil || entity.CompositePodGroup == nil {
				t.Errorf("Expected CPG %s/%s in active queue hierarchy, but not found", tt.cpgToUpdate.Namespace, tt.cpgToUpdate.Name)
			} else if diff := cmp.Diff(tt.cpgToUpdate, entity.CompositePodGroup); diff != "" {
				t.Errorf("Updated CompositePodGroup mismatch (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestPriorityQueue_DeleteCompositePodGroup 验证 Delete 对 CompositePodGroup（CPG）实体的处理。
func TestPriorityQueue_DeleteCompositePodGroup(t *testing.T) {
	tests := []struct {
		name                   string
		initialCPGs            []*schedulingv1alpha3.CompositePodGroup
		initialPodGroups       []*schedulingv1beta1.PodGroup
		initialPods            []*v1.Pod
		beforeDelete           func(ctx context.Context, q *PriorityQueue)
		cpgToDelete            *schedulingv1alpha3.CompositePodGroup
		expectedActiveQ        map[string][]string
		expectedIncompletePods []string
		expectedPendingPods    []string
	}{
		{
			name: "Non-root when parent is root and is already in activeQ with another pod",
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
				st.MakeCompositePodGroup().Name("child-cpg").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("child-cpg").Obj(),
				st.MakePodGroup().Name("pg2").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
				st.MakePod().Name("pod2").Namespace("ns1").UID("uid2").PodGroupName("pg2").Obj(),
			},
			cpgToDelete:            st.MakeCompositePodGroup().Name("child-cpg").Namespace("ns1").Obj(),
			expectedActiveQ:        map[string][]string{"compositepodgroup/root-cpg": {"pod2"}},
			expectedIncompletePods: []string{"pod1"},
			expectedPendingPods:    []string{},
		},
		{
			name: "Root with pods",
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
				st.MakeCompositePodGroup().Name("child-cpg").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("child-cpg").Obj(),
				st.MakePodGroup().Name("pg2").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
				st.MakePod().Name("pod2").Namespace("ns1").UID("uid2").PodGroupName("pg2").Obj(),
			},
			cpgToDelete:            st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
			expectedActiveQ:        map[string][]string{},
			expectedIncompletePods: []string{"pod1", "pod2"},
			expectedPendingPods:    []string{},
		},
		{
			name: "Non-root when parent does not exist",
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("child-cpg").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("child-cpg").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			cpgToDelete:            st.MakeCompositePodGroup().Name("child-cpg").Namespace("ns1").Obj(),
			expectedActiveQ:        map[string][]string{},
			expectedIncompletePods: []string{"pod1"},
			expectedPendingPods:    []string{},
		},
		{
			name: "Non-root when parent is root and is in-flight (popped)",
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
				st.MakeCompositePodGroup().Name("child-cpg").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("child-cpg").Obj(),
				st.MakePodGroup().Name("pg2").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
				st.MakePod().Name("pod2").Namespace("ns1").UID("uid2").PodGroupName("pg2").Obj(),
			},
			beforeDelete: func(ctx context.Context, q *PriorityQueue) {
				_, _ = q.Pop(klog.FromContext(ctx)) // 弹出 root-cpg，使其进入在途状态
			},
			cpgToDelete:            st.MakeCompositePodGroup().Name("child-cpg").Namespace("ns1").Obj(),
			expectedActiveQ:        map[string][]string{},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{},
		},
		{
			name: "Non-root with pending pods when parent is root and is in-flight (popped)",
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
				st.MakeCompositePodGroup().Name("child-cpg").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("child-cpg").Obj(),
				st.MakePodGroup().Name("pg2").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
				st.MakePod().Name("pod2").Namespace("ns1").UID("uid2").PodGroupName("pg2").Obj(),
			},
			beforeDelete: func(ctx context.Context, q *PriorityQueue) {
				_, _ = q.Pop(klog.FromContext(ctx)) // 弹出 root-cpg
				pod3 := st.MakePod().Name("pod3").Namespace("ns1").UID("uid3").PodGroupName("pg1").Obj()
				q.Add(ctx, pod3) // root-cpg 在途期间到达，进入 child-cpg 子树的 pendingPodGroupPods
			},
			cpgToDelete:            st.MakeCompositePodGroup().Name("child-cpg").Namespace("ns1").Obj(),
			expectedActiveQ:        map[string][]string{},
			expectedIncompletePods: []string{"pod3"},
			expectedPendingPods:    []string{},
		},
		{
			name: "Root with pending pods when in-flight (popped)",
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
			},
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			beforeDelete: func(ctx context.Context, q *PriorityQueue) {
				_, _ = q.Pop(klog.FromContext(ctx)) // 弹出 root-cpg
				pod2 := st.MakePod().Name("pod2").Namespace("ns1").UID("uid2").PodGroupName("pg1").Obj()
				q.Add(ctx, pod2) // root-cpg 在途期间到达，进入 pendingPodGroupPods
			},
			cpgToDelete:            st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
			expectedActiveQ:        map[string][]string{},
			expectedIncompletePods: []string{"pod2"},
			expectedPendingPods:    []string{},
		},
		{
			name: "Non-root with pods when root ancestor exists",
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
				st.MakeCompositePodGroup().Name("mid-cpg").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("mid-cpg").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			cpgToDelete:            st.MakeCompositePodGroup().Name("mid-cpg").Namespace("ns1").Obj(),
			expectedActiveQ:        map[string][]string{},
			expectedIncompletePods: []string{"pod1"},
			expectedPendingPods:    []string{},
		},
		{
			name: "Non-root when root ancestor is already in activeQ with another pod",
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
				st.MakeCompositePodGroup().Name("mid-cpg").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("mid-cpg").Obj(),
				st.MakePodGroup().Name("pg2").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
				st.MakePod().Name("pod2").Namespace("ns1").UID("uid2").PodGroupName("pg2").Obj(),
			},
			cpgToDelete:            st.MakeCompositePodGroup().Name("mid-cpg").Namespace("ns1").Obj(),
			expectedActiveQ:        map[string][]string{"compositepodgroup/root-cpg": {"pod2"}},
			expectedIncompletePods: []string{"pod1"},
			expectedPendingPods:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload:                 true,
				features.TopologyAwareWorkloadScheduling: true,
				features.CompositePodGroup:               true,
			})
			logger, ctx := ktesting.NewTestContext(t)

			q := NewTestQueue(ctx, newDefaultQueueSort())
			defer q.Close()

			for _, cpg := range tt.initialCPGs {
				q.AddCompositePodGroup(logger, cpg)
			}
			for _, pg := range tt.initialPodGroups {
				q.AddPodGroup(logger, pg)
			}
			for _, pod := range tt.initialPods {
				q.Add(ctx, pod)
			}
			if tt.beforeDelete != nil {
				tt.beforeDelete(ctx, q)
			}

			q.DeleteCompositePodGroup(logger, tt.cpgToDelete)

			cmpOpts := []cmp.Option{cmpopts.EquateEmpty(), cmpopts.SortSlices(func(a, b string) bool { return a < b })}
			if diff := cmp.Diff(tt.expectedActiveQ, getActivePodGroups(q), cmpOpts...); diff != "" {
				t.Errorf("Active queue pod groups mismatch (-want,+got):\n%s", diff)
			}

			incomplete := q.IncompletePodGroupPodsPods()
			var gotIncomplete []string
			for _, p := range incomplete {
				gotIncomplete = append(gotIncomplete, p.Name)
			}
			if diff := cmp.Diff(tt.expectedIncompletePods, gotIncomplete, cmpOpts...); diff != "" {
				t.Errorf("Incomplete pods mismatch (-want,+got):\n%s", diff)
			}

			pending := q.PendingPodGroupPods()
			var gotPending []string
			for _, p := range pending {
				gotPending = append(gotPending, p.Name)
			}
			if diff := cmp.Diff(tt.expectedPendingPods, gotPending, cmpOpts...); diff != "" {
				t.Errorf("Pending pods mismatch (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestPriorityQueue_AddPodGroup_Hierarchical 验证 Add 对层级 PodGroup 的处理。
func TestPriorityQueue_AddPodGroup_Hierarchical(t *testing.T) {
	tests := []struct {
		name                   string
		disableCPGFeature      bool
		initialCPGs            []*schedulingv1alpha3.CompositePodGroup
		initialPodGroups       []*schedulingv1beta1.PodGroup
		initialPods            []*v1.Pod
		beforeAdd              func(ctx context.Context, q *PriorityQueue)
		pgToAdd                *schedulingv1beta1.PodGroup
		expectedActiveQ        map[string][]string
		expectedIncompletePods []string
		expectedPendingPods    []string
	}{
		{
			name: "Root with pods",
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			pgToAdd:                st.MakePodGroup().Name("pg1").Namespace("ns1").Obj(),
			expectedActiveQ:        map[string][]string{"podgroup/pg1": {"pod1"}},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{},
		},
		{
			name:                   "Root without pods",
			pgToAdd:                st.MakePodGroup().Name("pg1").Namespace("ns1").Obj(),
			expectedActiveQ:        map[string][]string{},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{},
		},
		{
			name: "Root when another root exists",
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg2").Namespace("ns1").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod2").Namespace("ns1").UID("uid2").PodGroupName("pg2").Obj(),
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			pgToAdd: st.MakePodGroup().Name("pg1").Namespace("ns1").Obj(),
			expectedActiveQ: map[string][]string{
				"podgroup/pg1": {"pod1"},
				"podgroup/pg2": {"pod2"},
			},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{},
		},
		{
			name: "Non-root when parent is missing",
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			pgToAdd:                st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("missing-cpg").Obj(),
			expectedActiveQ:        map[string][]string{},
			expectedIncompletePods: []string{"pod1"},
			expectedPendingPods:    []string{},
		},
		{
			name: "Non-root when parent is root",
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			pgToAdd:                st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			expectedActiveQ:        map[string][]string{"compositepodgroup/root-cpg": {"pod1"}},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{},
		},
		{
			name: "Non-root when parent is root and is already in active queue with another pod",
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
			},
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg2").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod2").Namespace("ns1").UID("uid2").PodGroupName("pg2").Obj(),
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			pgToAdd:                st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			expectedActiveQ:        map[string][]string{"compositepodgroup/root-cpg": {"pod1", "pod2"}},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{},
		},
		{
			name: "Non-root when parent exists but root is missing",
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("parent-cpg").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			pgToAdd:                st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("parent-cpg").Obj(),
			expectedActiveQ:        map[string][]string{},
			expectedIncompletePods: []string{"pod1"},
			expectedPendingPods:    []string{},
		},
		{
			name: "Non-root with pending pods when parent is root and is currently in-flight (popped)",
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
			},
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg2").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg2").Obj(),
			},
			beforeAdd: func(ctx context.Context, q *PriorityQueue) {
				_, _ = q.Pop(klog.FromContext(ctx)) // 弹出 root-cpg，使其进入在途状态
				pod2 := st.MakePod().Name("pod2").Namespace("ns1").UID("uid2").PodGroupName("pg1").Obj()
				q.Add(ctx, pod2) // root-cpg 在途期间到达
			},
			pgToAdd:                st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			expectedActiveQ:        map[string][]string{},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{"pod2"},
		},
		{
			name: "Non-root when parent is root and is currently in-flight (popped)",
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
			},
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg2").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg2").Obj(),
				st.MakePod().Name("pod2").Namespace("ns1").UID("uid2").PodGroupName("pg1").Obj(),
			},
			beforeAdd: func(ctx context.Context, q *PriorityQueue) {
				_, _ = q.Pop(klog.FromContext(ctx)) // 弹出 root-cpg，使其进入在途状态
			},
			pgToAdd:                st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			expectedActiveQ:        map[string][]string{},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{"pod2"},
		},
		{
			name: "Root when another root is currently in-flight (popped)",
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg2").Namespace("ns1").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod2").Namespace("ns1").UID("uid2").PodGroupName("pg2").Obj(),
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			beforeAdd: func(ctx context.Context, q *PriorityQueue) {
				_, _ = q.Pop(klog.FromContext(ctx))
			},
			pgToAdd: st.MakePodGroup().Name("pg1").Namespace("ns1").Obj(),
			expectedActiveQ: map[string][]string{
				"podgroup/pg1": {"pod1"},
			},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{},
		},
		{
			name:              "Non-root when CPG feature is disabled",
			disableCPGFeature: true,
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			pgToAdd:                st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			expectedActiveQ:        map[string][]string{"podgroup/pg1": {"pod1"}},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload:                 true,
				features.TopologyAwareWorkloadScheduling: true,
				features.CompositePodGroup:               !tt.disableCPGFeature,
			})
			logger, ctx := ktesting.NewTestContext(t)

			q := NewTestQueue(ctx, newDefaultQueueSort())
			defer q.Close()

			for _, cpg := range tt.initialCPGs {
				q.AddCompositePodGroup(logger, cpg)
			}
			for _, pg := range tt.initialPodGroups {
				q.AddPodGroup(logger, pg)
			}
			for _, pod := range tt.initialPods {
				q.Add(ctx, pod)
			}
			if tt.beforeAdd != nil {
				tt.beforeAdd(ctx, q)
			}

			q.AddPodGroup(logger, tt.pgToAdd)

			cmpOpts := []cmp.Option{cmpopts.EquateEmpty(), cmpopts.SortSlices(func(a, b string) bool { return a < b })}
			if diff := cmp.Diff(tt.expectedActiveQ, getActivePodGroups(q), cmpOpts...); diff != "" {
				t.Errorf("Active queue pod groups mismatch (-want,+got):\n%s", diff)
			}

			incomplete := q.IncompletePodGroupPodsPods()
			var gotIncomplete []string
			for _, p := range incomplete {
				gotIncomplete = append(gotIncomplete, p.Name)
			}
			if diff := cmp.Diff(tt.expectedIncompletePods, gotIncomplete, cmpOpts...); diff != "" {
				t.Errorf("Incomplete pods mismatch (-want,+got):\n%s", diff)
			}

			pending := q.PendingPodGroupPods()
			var gotPending []string
			for _, p := range pending {
				gotPending = append(gotPending, p.Name)
			}
			if diff := cmp.Diff(tt.expectedPendingPods, gotPending, cmpOpts...); diff != "" {
				t.Errorf("Pending pods mismatch (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestPriorityQueue_UpdatePodGroup_Hierarchical 验证 Update 对层级 PodGroup 的处理。
func TestPriorityQueue_UpdatePodGroup_Hierarchical(t *testing.T) {
	tests := []struct {
		name                   string
		disableCPGFeature      bool
		initialCPGs            []*schedulingv1alpha3.CompositePodGroup
		initialPodGroups       []*schedulingv1beta1.PodGroup
		initialPods            []*v1.Pod
		pgToUpdate             *schedulingv1beta1.PodGroup
		expectedActiveQ        map[string][]string
		expectedIncompletePods []string
		expectedPendingPods    []string
	}{
		{
			name: "Root when minCount is updated",
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").MinCount(2).Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			pgToUpdate:             st.MakePodGroup().Name("pg1").Namespace("ns1").MinCount(3).Obj(),
			expectedActiveQ:        map[string][]string{"podgroup/pg1": {"pod1"}},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{},
		},
		{
			name: "Non-root when minCount is updated",
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
			},
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			pgToUpdate:             st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("root-cpg").MinCount(3).Obj(),
			expectedActiveQ:        map[string][]string{"compositepodgroup/root-cpg": {"pod1"}},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{},
		},
		{
			name:              "Non-root when CPG feature is disabled",
			disableCPGFeature: true,
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("root-cpg").MinCount(2).Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			pgToUpdate:             st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("root-cpg").MinCount(3).Obj(),
			expectedActiveQ:        map[string][]string{"podgroup/pg1": {"pod1"}},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload:                 true,
				features.TopologyAwareWorkloadScheduling: true,
				features.CompositePodGroup:               !tt.disableCPGFeature,
			})
			logger, ctx := ktesting.NewTestContext(t)

			q := NewTestQueue(ctx, newDefaultQueueSort())
			defer q.Close()

			for _, cpg := range tt.initialCPGs {
				q.AddCompositePodGroup(logger, cpg)
			}
			for _, pg := range tt.initialPodGroups {
				q.AddPodGroup(logger, pg)
			}
			for _, pod := range tt.initialPods {
				q.Add(ctx, pod)
			}

			q.UpdatePodGroup(logger, tt.pgToUpdate)

			cmpOpts := []cmp.Option{cmpopts.EquateEmpty(), cmpopts.SortSlices(func(a, b string) bool { return a < b })}
			if diff := cmp.Diff(tt.expectedActiveQ, getActivePodGroups(q), cmpOpts...); diff != "" {
				t.Errorf("Active queue pod groups mismatch (-want,+got):\n%s", diff)
			}

			incomplete := q.IncompletePodGroupPodsPods()
			var gotIncomplete []string
			for _, p := range incomplete {
				gotIncomplete = append(gotIncomplete, p.Name)
			}
			if diff := cmp.Diff(tt.expectedIncompletePods, gotIncomplete, cmpOpts...); diff != "" {
				t.Errorf("Incomplete pods mismatch (-want,+got):\n%s", diff)
			}

			pending := q.PendingPodGroupPods()
			var gotPending []string
			for _, p := range pending {
				gotPending = append(gotPending, p.Name)
			}
			if diff := cmp.Diff(tt.expectedPendingPods, gotPending, cmpOpts...); diff != "" {
				t.Errorf("Pending pods mismatch (-want,+got):\n%s", diff)
			}

			entity := findPodGroupInfoInActiveQ(q, tt.pgToUpdate.Namespace, tt.pgToUpdate.Name, fwk.PodGroupKeyType)
			if entity == nil || entity.PodGroup == nil {
				t.Errorf("Expected PodGroup %s/%s in active queue hierarchy, but not found", tt.pgToUpdate.Namespace, tt.pgToUpdate.Name)
			} else if diff := cmp.Diff(tt.pgToUpdate, entity.PodGroup); diff != "" {
				t.Errorf("Updated PodGroup mismatch (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestPriorityQueue_DeletePodGroup_Hierarchical 验证 Delete 对层级 PodGroup 的处理。
func TestPriorityQueue_DeletePodGroup_Hierarchical(t *testing.T) {
	tests := []struct {
		name                   string
		disableCPGFeature      bool
		initialCPGs            []*schedulingv1alpha3.CompositePodGroup
		initialPodGroups       []*schedulingv1beta1.PodGroup
		initialPods            []*v1.Pod
		beforeDelete           func(ctx context.Context, q *PriorityQueue)
		pgToDelete             *schedulingv1beta1.PodGroup
		expectedActiveQ        map[string][]string
		expectedIncompletePods []string
		expectedPendingPods    []string
	}{
		{
			name: "Non-root when parent is root and is already in activeQ with another pod",
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
			},
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
				st.MakePodGroup().Name("pg2").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
				st.MakePod().Name("pod2").Namespace("ns1").UID("uid2").PodGroupName("pg2").Obj(),
			},
			pgToDelete:             st.MakePodGroup().Name("pg1").Namespace("ns1").Obj(),
			expectedActiveQ:        map[string][]string{"compositepodgroup/root-cpg": {"pod2"}},
			expectedIncompletePods: []string{"pod1"},
			expectedPendingPods:    []string{},
		},
		{
			name: "Root with pods",
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			pgToDelete:             st.MakePodGroup().Name("pg1").Namespace("ns1").Obj(),
			expectedActiveQ:        map[string][]string{},
			expectedIncompletePods: []string{"pod1"},
			expectedPendingPods:    []string{},
		},
		{
			name: "Non-root when parent is root and is in-flight (popped)",
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
			},
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
				st.MakePodGroup().Name("pg2").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
				st.MakePod().Name("pod2").Namespace("ns1").UID("uid2").PodGroupName("pg2").Obj(),
			},
			beforeDelete: func(ctx context.Context, q *PriorityQueue) {
				_, _ = q.Pop(klog.FromContext(ctx)) // 弹出 root-cpg，使其进入在途状态
			},
			pgToDelete:             st.MakePodGroup().Name("pg1").Namespace("ns1").Obj(),
			expectedActiveQ:        map[string][]string{},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{},
		},
		{
			name: "Non-root with pending pods when parent is root and is in-flight (popped)",
			initialCPGs: []*schedulingv1alpha3.CompositePodGroup{
				st.MakeCompositePodGroup().Name("root-cpg").Namespace("ns1").Obj(),
			},
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
				st.MakePodGroup().Name("pg2").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
				st.MakePod().Name("pod2").Namespace("ns1").UID("uid2").PodGroupName("pg2").Obj(),
			},
			beforeDelete: func(ctx context.Context, q *PriorityQueue) {
				_, _ = q.Pop(klog.FromContext(ctx)) // 弹出 root-cpg
				pod3 := st.MakePod().Name("pod3").Namespace("ns1").UID("uid3").PodGroupName("pg1").Obj()
				q.Add(ctx, pod3) // root-cpg 在途期间到达，进入 pendingPodGroupPods
			},
			pgToDelete:             st.MakePodGroup().Name("pg1").Namespace("ns1").Obj(),
			expectedActiveQ:        map[string][]string{},
			expectedIncompletePods: []string{"pod3"},
			expectedPendingPods:    []string{},
		},
		{
			name: "Root when in-flight (popped)",
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			beforeDelete: func(ctx context.Context, q *PriorityQueue) {
				_, _ = q.Pop(klog.FromContext(ctx)) // 弹出 pg1，使其进入在途状态
			},
			pgToDelete:             st.MakePodGroup().Name("pg1").Namespace("ns1").Obj(),
			expectedActiveQ:        map[string][]string{},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{},
		},
		{
			name: "Root with pending pods when in-flight (popped)",
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			beforeDelete: func(ctx context.Context, q *PriorityQueue) {
				_, _ = q.Pop(klog.FromContext(ctx)) // 弹出 pg1，使其进入在途状态
				pod2 := st.MakePod().Name("pod2").Namespace("ns1").UID("uid2").PodGroupName("pg1").Obj()
				q.Add(ctx, pod2) // 在途期间到达，进入 pendingPodGroupPods
			},
			pgToDelete:             st.MakePodGroup().Name("pg1").Namespace("ns1").Obj(),
			expectedActiveQ:        map[string][]string{},
			expectedIncompletePods: []string{"pod2"},
			expectedPendingPods:    []string{},
		},
		{
			name: "Root without pods",
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").Obj(),
			},
			pgToDelete:             st.MakePodGroup().Name("pg1").Namespace("ns1").Obj(),
			expectedActiveQ:        map[string][]string{},
			expectedIncompletePods: []string{},
			expectedPendingPods:    []string{},
		},
		{
			name:              "Non-root when CPG feature is disabled",
			disableCPGFeature: true,
			initialPodGroups: []*schedulingv1beta1.PodGroup{
				st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			},
			initialPods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("uid1").PodGroupName("pg1").Obj(),
			},
			pgToDelete:             st.MakePodGroup().Name("pg1").Namespace("ns1").ParentCompositePodGroup("root-cpg").Obj(),
			expectedActiveQ:        map[string][]string{},
			expectedIncompletePods: []string{"pod1"},
			expectedPendingPods:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload:                 true,
				features.TopologyAwareWorkloadScheduling: true,
				features.CompositePodGroup:               !tt.disableCPGFeature,
			})
			logger, ctx := ktesting.NewTestContext(t)

			q := NewTestQueue(ctx, newDefaultQueueSort())
			defer q.Close()

			for _, cpg := range tt.initialCPGs {
				q.AddCompositePodGroup(logger, cpg)
			}
			for _, pg := range tt.initialPodGroups {
				q.AddPodGroup(logger, pg)
			}
			for _, pod := range tt.initialPods {
				q.Add(ctx, pod)
			}
			if tt.beforeDelete != nil {
				tt.beforeDelete(ctx, q)
			}

			q.DeletePodGroup(logger, tt.pgToDelete)

			cmpOpts := []cmp.Option{cmpopts.EquateEmpty(), cmpopts.SortSlices(func(a, b string) bool { return a < b })}
			if diff := cmp.Diff(tt.expectedActiveQ, getActivePodGroups(q), cmpOpts...); diff != "" {
				t.Errorf("Active queue pod groups mismatch (-want,+got):\n%s", diff)
			}

			incomplete := q.IncompletePodGroupPodsPods()
			var gotIncomplete []string
			for _, p := range incomplete {
				gotIncomplete = append(gotIncomplete, p.Name)
			}
			if diff := cmp.Diff(tt.expectedIncompletePods, gotIncomplete, cmpOpts...); diff != "" {
				t.Errorf("Incomplete pods mismatch (-want,+got):\n%s", diff)
			}

			pending := q.PendingPodGroupPods()
			var gotPending []string
			for _, p := range pending {
				gotPending = append(gotPending, p.Name)
			}
			if diff := cmp.Diff(tt.expectedPendingPods, gotPending, cmpOpts...); diff != "" {
				t.Errorf("Pending pods mismatch (-want,+got):\n%s", diff)
			}
		})
	}
}

// getActivePodGroupInfos 返回当前 activeQ 中的所有根 QueuedPodGroupInfo 对象。
func getActivePodGroupInfos(q *PriorityQueue) []*framework.QueuedPodGroupInfo {
	var result []*framework.QueuedPodGroupInfo
	aq, ok := q.activeQ.(*activeQueue)
	if !ok {
		return result
	}
	aq.lock.RLock()
	defer aq.lock.RUnlock()
	for _, entity := range aq.queue.List() {
		if pgInfo, ok := entity.(*framework.QueuedPodGroupInfo); ok {
			result = append(result, pgInfo)
		}
	}
	return result
}

// getActivePodGroups 返回从实体键（type/name）到 activeQ 内 Pod 名称切片的映射。
func getActivePodGroups(q *PriorityQueue) map[string][]string {
	result := make(map[string][]string)
	for _, pgInfo := range getActivePodGroupInfos(q) {
		key := fmt.Sprintf("%s/%s", pgInfo.GetType(), pgInfo.GetName())
		var pods []string
		pgInfo.ForEachPodInfo(func(pi *framework.QueuedPodInfo) bool {
			pods = append(pods, pi.Pod.Name)
			return true
		})
		result[key] = pods
	}
	return result
}

// findPodGroupInfoInActiveQ 遍历 activeQ 中的根层级，查找 namespace、name 和类型匹配的 PodGroupInfo。
func findPodGroupInfoInActiveQ(q *PriorityQueue, namespace, name string, entityType fwk.EntityKeyType) *framework.PodGroupInfo {
	var search func(pgi *framework.PodGroupInfo) *framework.PodGroupInfo
	search = func(pgi *framework.PodGroupInfo) *framework.PodGroupInfo {
		if pgi == nil {
			return nil
		}
		if pgi.Namespace == namespace && pgi.Name == name && pgi.GetType() == entityType {
			return pgi
		}
		for _, child := range pgi.Children {
			if found := search(child); found != nil {
				return found
			}
		}
		return nil
	}
	for _, root := range getActivePodGroupInfos(q) {
		if root.PodGroupInfo != nil {
			if found := search(root.PodGroupInfo); found != nil {
				return found
			}
		}
	}
	return nil
}

// newQueuedPodGroupInfoForLookup 构造一个仅用于队列查找的 QueuedPodGroupInfo 对象。
func newQueuedPodGroupInfoForLookup(namespace, name string, entityType fwk.EntityKeyType) *framework.QueuedPodGroupInfo {
	// 这里只用于队列查找，因此只需设置 PodGroupInfo 的 namespace 和 name，
	// 这样避免创建完整的 QueuedPodGroupInfo，频繁实例化它开销较大。
	return &framework.QueuedPodGroupInfo{
		PodGroupInfo: &framework.PodGroupInfo{
			Namespace: namespace,
			Name:      name,
			Type:      entityType,
		},
	}
}

func newSingleLevelPodGroupInfo(podInfo *framework.QueuedPodInfo, podGroup *schedulingv1beta1.PodGroup) *framework.QueuedPodGroupInfo {
	pgName := *podInfo.Pod.Spec.SchedulingGroup.PodGroupName
	key := fwk.PodGroupKey(podInfo.Pod.Namespace, pgName)
	return &framework.QueuedPodGroupInfo{
		PodGroupInfo: &framework.PodGroupInfo{
			Namespace:       podInfo.Pod.Namespace,
			Name:            pgName,
			Type:            fwk.PodGroupKeyType,
			UnscheduledPods: []*v1.Pod{podInfo.Pod},
			PodGroup:        podGroup,
		},
		QueuedPodInfos: map[fwk.EntityKey][]*framework.QueuedPodInfo{key: {podInfo}},
		QueueingParams: framework.QueueingParams{
			Timestamp:               podInfo.Timestamp,
			InitialAttemptTimestamp: podInfo.InitialAttemptTimestamp,
		},
	}
}

// TestPriorityQueue_DeferredPodGroupCompatibility 验证延迟构造的 PodGroup 信息在队列查找中的兼容性。
func TestPriorityQueue_DeferredPodGroupCompatibility(t *testing.T) {
	tests := []struct {
		name                string
		pod                 *v1.Pod
		expectIsGroupMember bool
	}{
		{
			name: "deferred pod with scheduling group is queued individually",
			pod: st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").PodGroupName("pg1").
				Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj(),
			expectIsGroupMember: false,
		},
		{
			name:                "not-deferred pod with scheduling group is queued as group member",
			pod:                 st.MakePod().Name("pod1").Namespace("ns1").UID("pod1").PodGroupName("pg1").Obj(),
			expectIsGroupMember: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScalingSchedulerPreemption, true)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.GenericWorkload, true)

			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			q := NewTestQueue(ctx, nil)
			defer q.Close()

			if tt.pod.Spec.SchedulingGroup != nil && tt.pod.Spec.SchedulingGroup.PodGroupName != nil {
				pg := st.MakePodGroup().Name(*tt.pod.Spec.SchedulingGroup.PodGroupName).Namespace(tt.pod.Namespace).Obj()
				q.AddPodGroup(logger, pg)
			}

			q.Add(ctx, tt.pod)

			// 验证 PodGroup 是否被创建/加入
			pgLookup := newQueuedPodGroupInfoForLookup(tt.pod.Namespace, *tt.pod.Spec.SchedulingGroup.PodGroupName, fwk.PodGroupKeyType)
			hasGroup := q.activeQ.has(pgLookup)
			if hasGroup != tt.expectIsGroupMember {
				t.Errorf("Unexpected group enqueuing: got hasGroup=%v, want=%v", hasGroup, tt.expectIsGroupMember)
			}

			// 验证 Pod 是否被单独入队
			pInfo, _ := framework.NewPodInfo(tt.pod)
			podLookup := &framework.QueuedPodInfo{PodInfo: pInfo}
			hasIndividual := q.activeQ.has(podLookup)
			if hasIndividual == tt.expectIsGroupMember {
				t.Errorf("Unexpected individual enqueuing: got hasIndividual=%v, want=%v", hasIndividual, !tt.expectIsGroupMember)
			}

			// 验证它未被记录在 pending pod group pods 中
			if q.pendingPodGroupPods.has(tt.pod) {
				t.Errorf("Expected pod to not be in pendingPodGroupPods")
			}
		})
	}
}

// TestPriorityQueue_PreQueueingHint 验证 PreQueueingHint 在各类事件下的收窄与回退行为。
func TestPriorityQueue_PreQueueingHint(t *testing.T) {
	tests := []struct {
		name           string
		featureEnabled bool
		pods           []*v1.Pod
		hints          []*QueueingHintFunction
		wantMoved      []string
	}{
		{
			name:           "gate enabled, hint narrows to one pod",
			featureEnabled: true,
			pods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj(),
				st.MakePod().Name("pod2").Namespace("ns1").UID("2").Obj(),
				st.MakePod().Name("pod3").Namespace("ns1").UID("3").Obj(),
			},
			hints: []*QueueingHintFunction{{
				PluginName:     "foo",
				QueueingHintFn: queueHintReturnQueue,
				PreQueueingHintFn: func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
					return fwk.PreQueueingHintResult{Pods: []types.NamespacedName{{Name: "pod1", Namespace: "ns1"}}}, nil
				},
			}},
			wantMoved: []string{"pod1_ns1"},
		},
		{
			name:           "gate enabled, hint AllPods evaluates all",
			featureEnabled: true,
			pods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj(),
				st.MakePod().Name("pod2").Namespace("ns1").UID("2").Obj(),
			},
			hints: []*QueueingHintFunction{{
				PluginName:     "foo",
				QueueingHintFn: queueHintReturnQueue,
			}},
			wantMoved: []string{"pod1_ns1", "pod2_ns1"},
		},
		{
			name:           "gate enabled, one plugin AllPods means all pods evaluated",
			featureEnabled: true,
			pods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj(),
				st.MakePod().Name("pod2").Namespace("ns1").UID("2").Obj(),
				st.MakePod().Name("pod3").Namespace("ns1").UID("3").Obj(),
			},
			hints: []*QueueingHintFunction{
				{
					PluginName:     "foo",
					QueueingHintFn: queueHintReturnQueue,
					PreQueueingHintFn: func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
						return fwk.PreQueueingHintResult{Pods: []types.NamespacedName{{Name: "pod1", Namespace: "ns1"}}}, nil
					},
				},
				{
					PluginName:     "bar",
					QueueingHintFn: queueHintReturnQueue,
					PreQueueingHintFn: func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
						return fwk.PreQueueingHintResult{AllPods: true}, nil
					},
				},
			},
			wantMoved: []string{"pod1_ns1", "pod2_ns1", "pod3_ns1"},
		},
		{
			name:           "gate disabled, hint ignored, all pods evaluated",
			featureEnabled: false,
			pods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj(),
				st.MakePod().Name("pod2").Namespace("ns1").UID("2").Obj(),
				st.MakePod().Name("pod3").Namespace("ns1").UID("3").Obj(),
			},
			hints: []*QueueingHintFunction{{
				PluginName:     "foo",
				QueueingHintFn: queueHintReturnQueue,
				PreQueueingHintFn: func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
					return fwk.PreQueueingHintResult{Pods: []types.NamespacedName{{Name: "pod1", Namespace: "ns1"}}}, nil
				},
			}},
			wantMoved: []string{"pod1_ns1", "pod2_ns1", "pod3_ns1"},
		},
		{
			name:           "single plugin narrows to subset (2 of 3)",
			featureEnabled: true,
			pods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj(),
				st.MakePod().Name("pod2").Namespace("ns1").UID("2").Obj(),
				st.MakePod().Name("pod3").Namespace("ns1").UID("3").Obj(),
			},
			hints: []*QueueingHintFunction{{
				PluginName:     "foo",
				QueueingHintFn: queueHintReturnQueue,
				PreQueueingHintFn: func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
					return fwk.PreQueueingHintResult{Pods: []types.NamespacedName{
						{Name: "pod1", Namespace: "ns1"},
						{Name: "pod3", Namespace: "ns1"},
					}}, nil
				},
			}},
			wantMoved: []string{"pod1_ns1", "pod3_ns1"},
		},
		{
			name:           "hint narrows but QueueingHint rejects",
			featureEnabled: true,
			pods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj(),
				st.MakePod().Name("pod2").Namespace("ns1").UID("2").Obj(),
			},
			hints: []*QueueingHintFunction{{
				PluginName:     "foo",
				QueueingHintFn: queueHintReturnSkip,
				PreQueueingHintFn: func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
					return fwk.PreQueueingHintResult{Pods: []types.NamespacedName{{Name: "pod1", Namespace: "ns1"}}}, nil
				},
			}},
			wantMoved: []string{},
		},
		{
			name:           "two plugins narrow to same pod",
			featureEnabled: true,
			pods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj(),
				st.MakePod().Name("pod2").Namespace("ns1").UID("2").Obj(),
			},
			hints: []*QueueingHintFunction{
				{
					PluginName:     "foo",
					QueueingHintFn: queueHintReturnQueue,
					PreQueueingHintFn: func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
						return fwk.PreQueueingHintResult{Pods: []types.NamespacedName{{Name: "pod1", Namespace: "ns1"}}}, nil
					},
				},
				{
					PluginName:     "bar",
					QueueingHintFn: queueHintReturnQueue,
					PreQueueingHintFn: func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
						return fwk.PreQueueingHintResult{Pods: []types.NamespacedName{{Name: "pod1", Namespace: "ns1"}}}, nil
					},
				},
			},
			wantMoved: []string{"pod1_ns1"},
		},
		{
			name:           "two plugins narrow to same two pods",
			featureEnabled: true,
			pods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj(),
				st.MakePod().Name("pod2").Namespace("ns1").UID("2").Obj(),
				st.MakePod().Name("pod3").Namespace("ns1").UID("3").Obj(),
			},
			hints: []*QueueingHintFunction{
				{
					PluginName:     "foo",
					QueueingHintFn: queueHintReturnQueue,
					PreQueueingHintFn: func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
						return fwk.PreQueueingHintResult{Pods: []types.NamespacedName{
							{Name: "pod1", Namespace: "ns1"},
							{Name: "pod2", Namespace: "ns1"},
						}}, nil
					},
				},
				{
					PluginName:     "bar",
					QueueingHintFn: queueHintReturnQueue,
					PreQueueingHintFn: func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
						return fwk.PreQueueingHintResult{Pods: []types.NamespacedName{
							{Name: "pod1", Namespace: "ns1"},
							{Name: "pod2", Namespace: "ns1"},
						}}, nil
					},
				},
			},
			wantMoved: []string{"pod1_ns1", "pod2_ns1"},
		},
		{
			name:           "two plugins disjoint pods (union evaluated)",
			featureEnabled: true,
			pods: []*v1.Pod{
				st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj(),
				st.MakePod().Name("pod2").Namespace("ns1").UID("2").Obj(),
				st.MakePod().Name("pod3").Namespace("ns1").UID("3").Obj(),
			},
			hints: []*QueueingHintFunction{
				{
					PluginName:     "foo",
					QueueingHintFn: queueHintReturnQueue,
					PreQueueingHintFn: func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
						return fwk.PreQueueingHintResult{Pods: []types.NamespacedName{{Name: "pod1", Namespace: "ns1"}}}, nil
					},
				},
				{
					PluginName:     "bar",
					QueueingHintFn: queueHintReturnQueue,
					PreQueueingHintFn: func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
						return fwk.PreQueueingHintResult{Pods: []types.NamespacedName{{Name: "pod2", Namespace: "ns1"}}}, nil
					},
				},
			},
			wantMoved: []string{"pod1_ns1", "pod2_ns1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SchedulerPreQueueingHints, tt.featureEnabled)
			logger, ctx := ktesting.NewTestContext(t)

			m := makeEmptyQueueingHintMapPerProfile()
			m[""][nodeAdd] = tt.hints

			q := NewTestQueue(ctx, newDefaultQueueSort(), WithQueueingHintMapPerProfile(m))

			for _, pod := range tt.pods {
				q.Add(ctx, pod)
				entity, err := q.Pop(logger)
				if err != nil {
					t.Fatalf("Pop failed: %v", err)
				}
				pInfo := entity.(*framework.QueuedPodInfo)
				pInfo.UnschedulablePlugins = sets.New[string]("foo", "bar")
				if err := q.AddUnschedulablePodIfNotPresent(logger, pInfo, q.SchedulingCycle()); err != nil {
					t.Fatalf("AddUnschedulablePodIfNotPresent failed: %v", err)
				}
			}

			q.MoveAllToActiveOrBackoffQueue(logger, nodeAdd, nil, st.MakeNode().Name("node1").Obj(), nil)

			moved := sets.New[string]()
			unschedPods := q.UnschedulablePods()
			for _, pod := range tt.pods {
				key := pod.Name + "_" + pod.Namespace
				found := false
				for _, up := range unschedPods {
					if up.Name == pod.Name && up.Namespace == pod.Namespace {
						found = true
						break
					}
				}
				if !found {
					moved.Insert(key)
				}
			}

			wantMoved := sets.New(tt.wantMoved...)
			if !moved.Equal(wantMoved) {
				t.Errorf("moved pods = %v, want %v", moved, wantMoved)
			}
		})
	}
}

// TestPreQueueingHint_FlushRescue 验证被有问题的 PreQueueingHintFn 漏掉的 Pod
// （未纳入收窄集合）最终会被周期性 flush 救回
// 在 podMaxInUnschedulablePodsDuration 到期后被救回。
func TestPreQueueingHint_FlushRescue(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SchedulerPreQueueingHints, true)
	logger, ctx := ktesting.NewTestContext(t)

	c := testingclock.NewFakeClock(time.Now())

	// 只返回 pod1 的 PreQueueingHintFn —— pod2 会被“漏掉”
	preQueueingHintFn := func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
		return fwk.PreQueueingHintResult{Pods: []types.NamespacedName{{Name: "pod1", Namespace: "ns1"}}}, nil
	}

	m := makeEmptyQueueingHintMapPerProfile()
	m[""][nodeAdd] = []*QueueingHintFunction{
		{
			PluginName:        "fakePlugin",
			QueueingHintFn:    queueHintReturnQueue,
			PreQueueingHintFn: preQueueingHintFn,
		},
	}

	q := NewTestQueue(ctx, newDefaultQueueSort(), WithClock(c), WithQueueingHintMapPerProfile(m))

	pod1 := st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj()
	pod2 := st.MakePod().Name("pod2").Namespace("ns1").UID("2").Obj()

	// 把两个 Pod 都加入 unschedulable（模拟调度失败）
	for _, pod := range []*v1.Pod{pod1, pod2} {
		q.Add(ctx, pod)
		entity, err := q.Pop(logger)
		if err != nil {
			t.Fatalf("Pop failed: %v", err)
		}
		pInfo := entity.(*framework.QueuedPodInfo)
		pInfo.UnschedulablePlugins = sets.New[string]("fakePlugin")
		if err := q.AddUnschedulablePodIfNotPresent(logger, pInfo, q.SchedulingCycle()); err != nil {
			t.Fatalf("AddUnschedulablePodIfNotPresent failed: %v", err)
		}
	}

	// 触发事件 —— PreQueueingHint 只识别出 pod1
	q.MoveAllToActiveOrBackoffQueue(logger, nodeAdd, nil, st.MakeNode().Name("node1").Obj(), nil)

	// 验证 pod1 已被移动，pod2 仍不可调度
	unschedPods := q.UnschedulablePods()
	pod2Found := false
	for _, p := range unschedPods {
		if p.Name == "pod2" && p.Namespace == "ns1" {
			pod2Found = true
		}
		if p.Name == "pod1" && p.Namespace == "ns1" {
			t.Errorf("pod1 should have been moved out of unschedulable, but was still there")
		}
	}
	if !pod2Found {
		t.Fatalf("pod2 should still be in unschedulable after event (missed by PreQueueingHint)")
	}

	// 从 activeQ 弹出 pod1，确认它已被重新入队
	entity, err := q.Pop(logger)
	if err != nil {
		t.Fatalf("Pop failed after event: %v", err)
	}
	pInfo := entity.(*framework.QueuedPodInfo)
	if pInfo.Pod.Name != "pod1" {
		t.Errorf("expected pod1 to be popped, got %s", pInfo.Pod.Name)
	}

	// 把时钟推进超过 flush 周期 —— pod2 应被救回
	c.Step(DefaultPodMaxInUnschedulablePodsDuration + time.Second)
	q.flushUnschedulableEntitiesLeftover(logger)

	// 验证 pod2 现在位于 activeQ（被 flush 救回）
	unschedPods = q.UnschedulablePods()
	for _, p := range unschedPods {
		if p.Name == "pod2" && p.Namespace == "ns1" {
			t.Errorf("pod2 should have been flushed to activeQ, but is still in unschedulable")
		}
	}

	// 弹出 pod2 并验证它已被救回
	entity, err = q.Pop(logger)
	if err != nil {
		t.Fatalf("Pop failed after flush: %v", err)
	}
	pInfo = entity.(*framework.QueuedPodInfo)
	if pInfo.Pod.Name != "pod2" {
		t.Errorf("expected pod2 to be popped after flush rescue, got %s", pInfo.Pod.Name)
	}
	if !pInfo.WasFlushedFromUnschedulable {
		t.Errorf("expected WasFlushedFromUnschedulable to be true for rescued pod2")
	}
}

// TestPreQueueingHint_Metrics 验证调用 PreQueueingHintFn 时
// 调用 PreQueueingHintFn 时指标以正确的标签递增。
func TestPreQueueingHint_Metrics(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SchedulerPreQueueingHints, true)
	logger, ctx := ktesting.NewTestContext(t)

	metrics.Register()
	metrics.PreQueueingHintEvaluations.Reset()

	// 把范围收窄到一个 Pod 的插件
	preQueueingHintFn := func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
		return fwk.PreQueueingHintResult{Pods: []types.NamespacedName{{Name: "pod1", Namespace: "ns1"}}}, nil
	}

	m := makeEmptyQueueingHintMapPerProfile()
	m[""][nodeAdd] = []*QueueingHintFunction{
		{
			PluginName:        "testPlugin",
			QueueingHintFn:    queueHintReturnQueue,
			PreQueueingHintFn: preQueueingHintFn,
		},
	}

	recorderCtx, stopRecorder := context.WithCancel(ctx)
	recorder := metrics.NewMetricsAsyncRecorder(10, 20*time.Microsecond, recorderCtx.Done())
	q := NewTestQueue(ctx, newDefaultQueueSort(), WithQueueingHintMapPerProfile(m), WithMetricsRecorder(recorder))

	pod := st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj()
	q.Add(ctx, pod)
	entity, _ := q.Pop(logger)
	pInfo := entity.(*framework.QueuedPodInfo)
	pInfo.UnschedulablePlugins = sets.New[string]("testPlugin")
	if err := q.AddUnschedulablePodIfNotPresent(logger, pInfo, q.SchedulingCycle()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	q.MoveAllToActiveOrBackoffQueue(logger, nodeAdd, nil, st.MakeNode().Name("node1").Obj(), nil)

	// 停止后台 flush goroutine 并等待其退出，然后
	// 调用 FlushMetrics，因此不会出现并发排空 channel 的情况。
	stopRecorder()
	<-recorder.IsStoppedCh
	recorder.FlushMetrics()

	// 验证指标以 "narrowed" 标签递增
	narrowedCount, err := testutil.GetCounterMetricValue(metrics.PreQueueingHintEvaluations.WithLabelValues("testPlugin", "narrowed"))
	if err != nil {
		t.Fatalf("Failed to get metric: %v", err)
	}
	if narrowedCount != 1 {
		t.Errorf("Expected narrowed count = 1, got %v", narrowedCount)
	}
}

// TestPreQueueingHint_PodGroupLookup 验证 PreQueueingHint 中 PodGroup 成员的 O(1) 实体查找。
func TestPreQueueingHint_PodGroupLookup(t *testing.T) {
	// 验证 PreQueueingHint 的 O(1) 查找能正确解析 Pod 的
	// Pod 属于 PodGroup 时，通过 schedulingGroup 取实体键。
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.SchedulerPreQueueingHints: true,
		features.GenericWorkload:           true,
	})
	logger, ctx := ktesting.NewTestContext(t)

	pgName := "test-pg"
	pod1 := st.MakePod().Name("pgpod1").Namespace("ns1").UID("pgpod1").Label("allow", "").PodGroupName(pgName).Obj()
	pod2 := st.MakePod().Name("pgpod2").Namespace("ns1").UID("pgpod2").Label("allow", "").PodGroupName(pgName).Obj()
	// 不属于该组的独立 Pod。
	pod3 := st.MakePod().Name("standalone").Namespace("ns1").UID("standalone").Label("allow", "").Obj()

	preQueueingHintFn := func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
		// hint 返回 pgpod1（PodGroup 成员）。
		return fwk.PreQueueingHintResult{
			Pods: []types.NamespacedName{{Name: "pgpod1", Namespace: "ns1"}},
		}, nil
	}

	m := makeEmptyQueueingHintMapPerProfile()
	m[""][nodeAdd] = []*QueueingHintFunction{
		{
			PluginName:        "testPlugin",
			QueueingHintFn:    queueHintReturnQueue,
			PreQueueingHintFn: preQueueingHintFn,
		},
	}

	podGroup := st.MakePodGroup().Name(pgName).Namespace("ns1").Obj()

	// 在 informer 中放入 Pod 对象再建队列，这样 podLister.Get 才能取到。
	q := NewTestQueueWithObjects(ctx, newDefaultQueueSort(), []runtime.Object{pod1, pod2, pod3},
		WithQueueingHintMapPerProfile(m))

	// 准备一个处于 unschedulable 状态的 PodGroup。
	setupInitialPodGroupState(t, ctx, q, []*v1.Pod{pod1, pod2}, stateUnschedulable, podGroup)

	// 同时在实体上标记 UnschedulablePlugins，好让 QueueingHint 能移动它。
	pgLookup := newQueuedPodGroupInfoForLookup("ns1", "test-pg", fwk.PodGroupKeyType)
	entity := q.unschedulableEntities.get(pgLookup)
	if entity == nil {
		t.Fatal("pod group not found in unschedulable entities")
	}
	entity.(*framework.QueuedPodGroupInfo).UnschedulablePlugins = sets.New[string]("testPlugin")

	// 把独立 Pod 加入不可调度队列。
	q.Add(ctx, pod3)
	standaloneEntity := q.activeQ.delete(newQueuedPodInfoForLookup(pod3))
	if standaloneEntity == nil {
		t.Fatal("standalone pod not found in activeQ")
	}
	standaloneEntity.(*framework.QueuedPodInfo).UnschedulablePlugins = sets.New[string]("testPlugin")
	q.unschedulableEntities.addOrUpdate(standaloneEntity, false, framework.EventUnscheduledPodAdd.Label(), nil)

	// 触发事件 —— hint 只返回 pgpod1。O(1) 查找应当：
	// 1. 找不到 "pod/ns1/pgpod1"（它以 PodGroup 形式存储）
	// 2. 通过 podLister 查找 Pod，找到 schedulingGroup
	// 3. 构造 "podgroup/ns1/test-pg" 并找到实体
	q.MoveAllToActiveOrBackoffQueue(logger, nodeAdd, nil, st.MakeNode().Name("node1").Obj(), nil)

	// PodGroup 应已移出 unschedulable。
	unschedEntity := q.unschedulableEntities.get(pgLookup)
	if unschedEntity != nil {
		t.Errorf("pod group should have been moved out of unschedulable queue")
	}

	// 独立 Pod 应仍在 unschedulable 中（hint 没包含它）。
	standaloneLookup := newQueuedPodInfoForLookup(pod3)
	if q.unschedulableEntities.get(standaloneLookup) == nil {
		t.Errorf("standalone pod should remain in unschedulable queue")
	}
}

// TestPreQueueingHint_NilObjects 验证 PreQueueingHint 对 nil 新旧对象的处理。
func TestPreQueueingHint_NilObjects(t *testing.T) {
	// 验证 oldObj、newObj 都为 nil 时 getPreQueueingHintPodKeys 返回 nil，
	// 因为没有对象可供检查，PreQueueingHintFn 无法收窄。
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.SchedulerPreQueueingHints: true,
	})
	logger, ctx := ktesting.NewTestContext(t)

	preQueueingHintFn := func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
		// 两个对象都为 nil 时绝不应调用它。
		t.Fatal("PreQueueingHintFn should not be called with nil objects")
		return fwk.PreQueueingHintResult{}, nil
	}

	m := makeEmptyQueueingHintMapPerProfile()
	m[""][nodeAdd] = []*QueueingHintFunction{
		{
			PluginName:        "testPlugin",
			QueueingHintFn:    queueHintReturnQueue,
			PreQueueingHintFn: preQueueingHintFn,
		},
	}

	q := NewTestQueue(ctx, newDefaultQueueSort(), WithQueueingHintMapPerProfile(m))

	// 两个参数都传 nil —— 应直接返回 nil，不调用 PreQueueingHintFn。
	q.MoveAllToActiveOrBackoffQueue(logger, nodeAdd, nil, nil, nil)
}

// TestPreQueueingHint_ErrorFallback 验证 PreQueueingHint 返回错误时回退到评估所有 Pod。
func TestPreQueueingHint_ErrorFallback(t *testing.T) {
	// 验证 PreQueueingHintFn 返回错误时会评估所有 Pod
	// （回退到 AllPods 行为）。
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.SchedulerPreQueueingHints: true,
	})
	logger, ctx := ktesting.NewTestContext(t)

	preQueueingHintFn := func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
		return fwk.PreQueueingHintResult{}, fmt.Errorf("intentional error")
	}

	m := makeEmptyQueueingHintMapPerProfile()
	m[""][nodeAdd] = []*QueueingHintFunction{
		{
			PluginName:        "fakePlugin",
			QueueingHintFn:    queueHintReturnQueue,
			PreQueueingHintFn: preQueueingHintFn,
		},
	}

	q := NewTestQueue(ctx, newDefaultQueueSort(), WithQueueingHintMapPerProfile(m))

	pod1 := st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj()
	pod2 := st.MakePod().Name("pod2").Namespace("ns1").UID("2").Obj()

	// 把两个 Pod 都加入 unschedulable。
	for _, pod := range []*v1.Pod{pod1, pod2} {
		q.Add(ctx, pod)
		entity, err := q.Pop(logger)
		if err != nil {
			t.Fatalf("Pop failed: %v", err)
		}
		pInfo := entity.(*framework.QueuedPodInfo)
		pInfo.UnschedulablePlugins = sets.New[string]("fakePlugin")
		if err := q.AddUnschedulablePodIfNotPresent(logger, pInfo, q.SchedulingCycle()); err != nil {
			t.Fatalf("AddUnschedulablePodIfNotPresent failed: %v", err)
		}
	}

	// 触发事件 —— PreQueueingHint 返回错误，因此应评估所有 Pod。
	q.MoveAllToActiveOrBackoffQueue(logger, nodeAdd, nil, st.MakeNode().Name("node1").Obj(), nil)

	// 两个 Pod 都应移出 unschedulable（回退到 AllPods）。
	unschedPods := q.UnschedulablePods()
	if len(unschedPods) != 0 {
		t.Errorf("expected 0 pods in unschedulable after error fallback (all should be moved), got %d", len(unschedPods))
	}
}

// TestPreQueueingHint_PodGroupPreCheck 验证收窄路径下 preCheck 对 PodGroup 实体的作用。
func TestPreQueueingHint_PodGroupPreCheck(t *testing.T) {
	// 验证收窄路径下 preCheck 也作用于 PodGroup 实体。
	// preCheck 拒绝所有成员 Pod 时，PodGroup 实体不应被移动。
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.SchedulerPreQueueingHints: true,
		features.GenericWorkload:           true,
	})
	logger, ctx := ktesting.NewTestContext(t)

	pgName := "test-pg"
	pod1 := st.MakePod().Name("pgpod1").Namespace("ns1").UID("pgpod1").Label("block", "").PodGroupName(pgName).Obj()
	pod2 := st.MakePod().Name("pgpod2").Namespace("ns1").UID("pgpod2").Label("block", "").PodGroupName(pgName).Obj()
	podGroup := &schedulingv1beta1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: pgName, Namespace: "ns1", UID: "pg-uid"},
		Spec:       schedulingv1beta1.PodGroupSpec{},
	}

	preQueueingHintFn := func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
		return fwk.PreQueueingHintResult{
			Pods: []types.NamespacedName{{Name: "pgpod1", Namespace: "ns1"}},
		}, nil
	}

	m := makeEmptyQueueingHintMapPerProfile()
	m[""][nodeAdd] = []*QueueingHintFunction{
		{
			PluginName:        "testPlugin",
			QueueingHintFn:    queueHintReturnQueue,
			PreQueueingHintFn: preQueueingHintFn,
		},
	}

	q := NewTestQueue(ctx, newDefaultQueueSort(), WithQueueingHintMapPerProfile(m))

	// 准备一个处于 unschedulable 状态的 PodGroup。
	setupInitialPodGroupState(t, ctx, q, []*v1.Pod{pod1, pod2}, stateUnschedulable, podGroup)

	pgLookup := newQueuedPodGroupInfoForLookup("ns1", pgName, fwk.PodGroupKeyType)
	entity := q.unschedulableEntities.get(pgLookup)
	if entity == nil {
		t.Fatal("pod group not found in unschedulable entities")
	}
	entity.(*framework.QueuedPodGroupInfo).UnschedulablePlugins = sets.New[string]("testPlugin")

	// preCheck 拒绝带 "block" 标签的 Pod
	preCheck := func(pod *v1.Pod) bool {
		_, hasBlock := pod.Labels["block"]
		return !hasBlock
	}

	// 触发事件，preCheck 阻塞所有 PodGroup 成员 Pod。
	q.MoveAllToActiveOrBackoffQueue(logger, nodeAdd, nil, st.MakeNode().Name("node1").Obj(), preCheck)

	// preCheck 拒绝了所有成员，PodGroup 应继续留在 unschedulable。
	if q.unschedulableEntities.get(pgLookup) == nil {
		t.Errorf("pod group should remain in unschedulable when preCheck rejects all members")
	}
}

// TestPreQueueingHint_CPGDisablesNarrowing 验证启用 CPG 时 PreQueueingHint 不做收窄。
func TestPreQueueingHint_CPGDisablesNarrowing(t *testing.T) {
	// 启用 CompositePodGroup 特性开关时，PreQueueingHint
	// 禁用收窄以免漏掉 CPG 实体。
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.SchedulerPreQueueingHints:       true,
		features.GenericWorkload:                 true,
		features.CompositePodGroup:               true,
		features.TopologyAwareWorkloadScheduling: true,
	})
	logger, ctx := ktesting.NewTestContext(t)

	preQueueingHintFn := func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
		// 启用 CPG 时不应调用它。
		// collectEntitiesToEvaluate 中有 CPG 保护：hint 函数会被调用，但结果被忽略。
		return fwk.PreQueueingHintResult{Pods: []types.NamespacedName{{Name: "pod1", Namespace: "ns1"}}}, nil
	}

	m := makeEmptyQueueingHintMapPerProfile()
	m[""][nodeAdd] = []*QueueingHintFunction{
		{
			PluginName:        "testPlugin",
			QueueingHintFn:    queueHintReturnQueue,
			PreQueueingHintFn: preQueueingHintFn,
		},
	}

	q := NewTestQueue(ctx, newDefaultQueueSort(), WithQueueingHintMapPerProfile(m))

	pod1 := st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj()
	pod2 := st.MakePod().Name("pod2").Namespace("ns1").UID("2").Obj()

	for _, pod := range []*v1.Pod{pod1, pod2} {
		q.Add(ctx, pod)
		entity, err := q.Pop(logger)
		if err != nil {
			t.Fatalf("Pop failed: %v", err)
		}
		pInfo := entity.(*framework.QueuedPodInfo)
		pInfo.UnschedulablePlugins = sets.New[string]("testPlugin")
		if err := q.AddUnschedulablePodIfNotPresent(logger, pInfo, q.SchedulingCycle()); err != nil {
			t.Fatalf("AddUnschedulablePodIfNotPresent failed: %v", err)
		}
	}

	// 触发事件 —— 启用 CPG 后应评估所有 Pod（不收窄）。
	q.MoveAllToActiveOrBackoffQueue(logger, nodeAdd, nil, st.MakeNode().Name("node1").Obj(), nil)

	// 两个 Pod 都应被移动（未启用收窄）。
	unschedPods := q.UnschedulablePods()
	if len(unschedPods) != 0 {
		t.Errorf("expected 0 pods in unschedulable (all should be moved), got %d", len(unschedPods))
	}
}

// TestPreQueueingHint_WildcardSkipsNarrowing 验证通配事件下 PreQueueingHint 跳过收窄。
func TestPreQueueingHint_WildcardSkipsNarrowing(t *testing.T) {
	// 通配事件触发时，跳过 PreQueueingHint 收窄
	// 并评估所有 Pod。
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.SchedulerPreQueueingHints: true,
	})
	logger, ctx := ktesting.NewTestContext(t)

	preQueueingHintFn := func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
		// 通配事件不应调用它。
		t.Fatal("PreQueueingHintFn should not be called for wildcard events")
		return fwk.PreQueueingHintResult{}, nil
	}

	m := makeEmptyQueueingHintMapPerProfile()
	m[""][nodeAdd] = []*QueueingHintFunction{
		{
			PluginName:        "testPlugin",
			QueueingHintFn:    queueHintReturnQueue,
			PreQueueingHintFn: preQueueingHintFn,
		},
	}

	q := NewTestQueue(ctx, newDefaultQueueSort(), WithQueueingHintMapPerProfile(m))

	pod1 := st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj()
	pod2 := st.MakePod().Name("pod2").Namespace("ns1").UID("2").Obj()

	for _, pod := range []*v1.Pod{pod1, pod2} {
		q.Add(ctx, pod)
		entity, err := q.Pop(logger)
		if err != nil {
			t.Fatalf("Pop failed: %v", err)
		}
		pInfo := entity.(*framework.QueuedPodInfo)
		pInfo.UnschedulablePlugins = sets.New[string]("testPlugin")
		if err := q.AddUnschedulablePodIfNotPresent(logger, pInfo, q.SchedulingCycle()); err != nil {
			t.Fatalf("AddUnschedulablePodIfNotPresent failed: %v", err)
		}
	}

	// 触发通配事件 —— 应跳过收窄，评估所有 Pod。
	q.MoveAllToActiveOrBackoffQueue(logger, framework.EventUnschedulableTimeout, nil, nil, nil)

	// 两个 Pod 都应被移动。
	unschedPods := q.UnschedulablePods()
	if len(unschedPods) != 0 {
		t.Errorf("expected 0 pods in unschedulable after wildcard event, got %d", len(unschedPods))
	}
}

// TestPreQueueingHint_PerPluginNarrowing 验证按插件收窄时只有对应插件的 hint 被调用。
func TestPreQueueingHint_PerPluginNarrowing(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.SchedulerPreQueueingHints: true,
	})
	logger, ctx := ktesting.NewTestContext(t)

	var pluginAQueueingHintCalled []string
	var pluginBQueueingHintCalled []string

	m := makeEmptyQueueingHintMapPerProfile()
	m[""][nodeAdd] = []*QueueingHintFunction{
		{
			PluginName: "pluginA",
			QueueingHintFn: func(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
				pluginAQueueingHintCalled = append(pluginAQueueingHintCalled, pod.Name)
				return fwk.Queue, nil
			},
			PreQueueingHintFn: func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
				return fwk.PreQueueingHintResult{Pods: []types.NamespacedName{{Name: "pod1", Namespace: "ns1"}}}, nil
			},
		},
		{
			PluginName: "pluginB",
			QueueingHintFn: func(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
				pluginBQueueingHintCalled = append(pluginBQueueingHintCalled, pod.Name)
				return fwk.Queue, nil
			},
			PreQueueingHintFn: func(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
				return fwk.PreQueueingHintResult{Pods: []types.NamespacedName{{Name: "pod2", Namespace: "ns1"}}}, nil
			},
		},
	}

	q := NewTestQueue(ctx, newDefaultQueueSort(), WithQueueingHintMapPerProfile(m))

	pod1 := st.MakePod().Name("pod1").Namespace("ns1").UID("1").Obj()
	pod2 := st.MakePod().Name("pod2").Namespace("ns1").UID("2").Obj()

	for _, pod := range []*v1.Pod{pod1, pod2} {
		q.Add(ctx, pod)
		entity, err := q.Pop(logger)
		if err != nil {
			t.Fatalf("Pop failed: %v", err)
		}
		pInfo := entity.(*framework.QueuedPodInfo)
		pInfo.UnschedulablePlugins = sets.New[string]("pluginA", "pluginB")
		if err := q.AddUnschedulablePodIfNotPresent(logger, pInfo, q.SchedulingCycle()); err != nil {
			t.Fatalf("AddUnschedulablePodIfNotPresent failed: %v", err)
		}
	}

	q.MoveAllToActiveOrBackoffQueue(logger, nodeAdd, nil, st.MakeNode().Name("node1").Obj(), nil)

	// pluginA 的 QueueingHintFn 只应针对 pod1 调用。
	if slices.Contains(pluginAQueueingHintCalled, "pod2") {
		t.Errorf("pluginA QueueingHintFn should NOT be called for pod2, called for: %v", pluginAQueueingHintCalled)
	}
	if !slices.Contains(pluginAQueueingHintCalled, "pod1") {
		t.Errorf("pluginA QueueingHintFn should be called for pod1, called for: %v", pluginAQueueingHintCalled)
	}

	// pluginB 的 QueueingHintFn 只应针对 pod2 调用。
	if slices.Contains(pluginBQueueingHintCalled, "pod1") {
		t.Errorf("pluginB QueueingHintFn should NOT be called for pod1, called for: %v", pluginBQueueingHintCalled)
	}
	if !slices.Contains(pluginBQueueingHintCalled, "pod2") {
		t.Errorf("pluginB QueueingHintFn should be called for pod2, called for: %v", pluginBQueueingHintCalled)
	}
}

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
