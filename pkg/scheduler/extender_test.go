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

package scheduler

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/klog/v2/ktesting"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	fwk "k8s.io/kube-scheduler/framework"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/backend/queue"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	"k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	tf "k8s.io/kubernetes/pkg/scheduler/testing/framework"
)

// scheduleResultCmpOpts 允许 cmp 比较 ScheduleResult 的非导出字段。
var scheduleResultCmpOpts = []cmp.Option{
	cmp.AllowUnexported(ScheduleResult{}),
}

// TestSchedulerWithExtenders 验证调度器与 HTTP 扩展器的集成：过滤、打分、绑定，
// 以及扩展器报错/不可达/不感兴趣时的处理。
func TestSchedulerWithExtenders(t *testing.T) {
	tests := []struct {
		name            string
		registerPlugins []tf.RegisterPluginFunc
		extenders       []tf.FakeExtender
		nodes           []string
		expectedResult  ScheduleResult
		expectsErr      bool
	}{
		{
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: []tf.FakeExtender{
				{
					ExtenderName: "FakeExtender1",
					Predicates:   []tf.FitPredicate{tf.TruePredicateExtender},
				},
				{
					ExtenderName: "FakeExtender2",
					Predicates:   []tf.FitPredicate{tf.ErrorPredicateExtender},
				},
			},
			nodes:      []string{"node1", "node2"},
			expectsErr: true,
			// 扩展器 2 的 predicate 报错：过滤阶段的错误必须终止调度。
			name: "test 1",
		},
		{
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: []tf.FakeExtender{
				{
					ExtenderName: "FakeExtender1",
					Predicates:   []tf.FitPredicate{tf.TruePredicateExtender},
				},
				{
					ExtenderName: "FakeExtender2",
					Predicates:   []tf.FitPredicate{tf.FalsePredicateExtender},
				},
			},
			nodes:      []string{"node1", "node2"},
			expectsErr: true,
			// 扩展器 2 拒绝所有节点：无可行节点，调度应失败。
			name: "test 2",
		},
		{
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterScorePlugin("EqualPrioritizerPlugin", tf.NewEqualPrioritizerPlugin(), 1),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: []tf.FakeExtender{
				{
					ExtenderName: "FakeExtender1",
					Predicates:   []tf.FitPredicate{tf.TruePredicateExtender},
				},
				{
					ExtenderName: "FakeExtender2",
					Predicates:   []tf.FitPredicate{tf.Node1PredicateExtender},
				},
			},
			nodes: []string{"node1", "node2"},
			expectedResult: ScheduleResult{
				SuggestedHost:  "node1",
				EvaluatedNodes: 2,
				FeasibleNodes:  1,
			},
			// 扩展器 2 只接受 node1：验证扩展器过滤结果影响最终选节点。
			name: "test 3",
		},
		{
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: []tf.FakeExtender{
				{
					ExtenderName: "FakeExtender1",
					Predicates:   []tf.FitPredicate{tf.Node2PredicateExtender},
				},
				{
					ExtenderName: "FakeExtender2",
					Predicates:   []tf.FitPredicate{tf.Node1PredicateExtender},
				},
			},
			nodes:      []string{"node1", "node2"},
			expectsErr: true,
			// 两个扩展器的过滤条件互斥（只接受 node2 / 只接受 node1）：无节点能同时通过。
			name: "test 4",
		},
		{
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: []tf.FakeExtender{
				{
					ExtenderName: "FakeExtender1",
					Predicates:   []tf.FitPredicate{tf.TruePredicateExtender},
					Prioritizers: []tf.PriorityConfig{{Function: tf.ErrorPrioritizerExtender, Weight: 10}},
					Weight:       1,
				},
			},
			nodes: []string{"node1"},
			expectedResult: ScheduleResult{
				SuggestedHost:  "node1",
				EvaluatedNodes: 1,
				FeasibleNodes:  1,
			},
			// 扩展器的 prioritizing 报错：忽略单个打分错误，期望仍调度成功。
			name: "test 5",
		},
		{
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: []tf.FakeExtender{
				{
					ExtenderName: "FakeExtender1",
					Predicates:   []tf.FitPredicate{tf.TruePredicateExtender},
					Prioritizers: []tf.PriorityConfig{{Function: tf.Node1PrioritizerExtender, Weight: 10}},
					Weight:       1,
				},
				{
					ExtenderName: "FakeExtender2",
					Predicates:   []tf.FitPredicate{tf.TruePredicateExtender},
					Prioritizers: []tf.PriorityConfig{{Function: tf.Node2PrioritizerExtender, Weight: 10}},
					Weight:       5,
				},
			},
			nodes: []string{"node1", "node2"},
			expectedResult: ScheduleResult{
				SuggestedHost:  "node2",
				EvaluatedNodes: 2,
				FeasibleNodes:  2,
			},
			// 两个扩展器分别偏好 node1/node2，权重为 5 的扩展器 2 胜出。
			name: "test 6",
		},
		{
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
				tf.RegisterScorePlugin("Node2Prioritizer", tf.NewNode2PrioritizerPlugin(), 20),
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: []tf.FakeExtender{
				{
					ExtenderName: "FakeExtender1",
					Predicates:   []tf.FitPredicate{tf.TruePredicateExtender},
					Prioritizers: []tf.PriorityConfig{{Function: tf.Node1PrioritizerExtender, Weight: 10}},
					Weight:       1,
				},
			},
			nodes: []string{"node1", "node2"},
			expectedResult: ScheduleResult{
				SuggestedHost:  "node2",
				EvaluatedNodes: 2,
				FeasibleNodes:  2,
			}, // node2 得分更高
			// 扩展器偏好 node1，但打分插件权重 20 使 node2 得分更高：验证两者分数合并。
			name: "test 7",
		},
		{
			// 期望调度器在 Filter/Prioritize 阶段不向对该 pod 不感兴趣的扩展器发请求。
			// 若误发，errorPredicateExtender / errorPrioritizerExtender 的错误会让测试失败。
			//
			// 因此只要调度成功，就证明 UnInterested 的扩展器被正确跳过。
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
				tf.RegisterScorePlugin("Node2Prioritizer", tf.NewNode2PrioritizerPlugin(), 1),
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: []tf.FakeExtender{
				{
					ExtenderName: "FakeExtender1",
					Predicates:   []tf.FitPredicate{tf.ErrorPredicateExtender},
					Prioritizers: []tf.PriorityConfig{{Function: tf.ErrorPrioritizerExtender, Weight: 10}},
					UnInterested: true,
				},
			},
			nodes:      []string{"node1", "node2"},
			expectsErr: false,
			expectedResult: ScheduleResult{
				SuggestedHost:  "node2",
				EvaluatedNodes: 2,
				FeasibleNodes:  2,
			}, // node2 得分更高
			// 不感兴趣的扩展器配置了必失败的函数：验证调度器完全跳过它。
			name: "test 8",
		},
		{
			// 扩展器不可达但被标记为可忽略（Ignorable）时，调度不应在
			// Filter/Prioritize 阶段失败。
			//
			// 若未忽略该扩展器，errorPredicateExtender 的错误会导致测试失败。
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
				tf.RegisterScorePlugin("EqualPrioritizerPlugin", tf.NewEqualPrioritizerPlugin(), 1),
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: []tf.FakeExtender{
				{
					ExtenderName: "FakeExtender1",
					Predicates:   []tf.FitPredicate{tf.ErrorPredicateExtender},
					Ignorable:    true,
				},
				{
					ExtenderName: "FakeExtender2",
					Predicates:   []tf.FitPredicate{tf.Node1PredicateExtender},
				},
			},
			nodes:      []string{"node1", "node2"},
			expectsErr: false,
			expectedResult: ScheduleResult{
				SuggestedHost:  "node1",
				EvaluatedNodes: 2,
				FeasibleNodes:  1,
			},
			// 可忽略的扩展器报错，另一个扩展器正常过滤：期望仍选出 node1。
			name: "test 9",
		},
		{
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterFilterPlugin("TrueFilter", tf.NewTrueFilterPlugin),
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: []tf.FakeExtender{
				{
					ExtenderName: "FakeExtender1",
					Predicates:   []tf.FitPredicate{tf.TruePredicateExtender},
				},
				{
					ExtenderName: "FakeExtender2",
					Predicates:   []tf.FitPredicate{tf.Node1PredicateExtender},
				},
			},
			nodes: []string{"node1", "node2"},
			expectedResult: ScheduleResult{
				SuggestedHost:  "node1",
				EvaluatedNodes: 2,
				FeasibleNodes:  1,
			},
			// 无打分插件、扩展器过滤后有多个可行节点：评估全部两个节点。
			name: "test 10 - no scoring, extender filters configured, multiple feasible nodes are evaluated",
		},
		{
			registerPlugins: []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			},
			extenders: []tf.FakeExtender{
				{
					ExtenderName: "FakeExtender1",
					Binder:       func() error { return nil },
				},
			},
			nodes: []string{"node1", "node2"},
			expectedResult: ScheduleResult{
				SuggestedHost:  "node1",
				EvaluatedNodes: 1,
				FeasibleNodes:  1,
			},
			// 无打分、无预过滤、扩展器只做绑定：找到第一个可行节点即停止评估。
			name: "test 11 - no scoring, no prefilters or  extender filters configured, a single feasible node is evaluated",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 使用 fake 客户端和 informer，避免依赖真实 API Server。
			client := clientsetfake.NewClientset()
			informerFactory := informers.NewSharedInformerFactory(client, 0)

			// 将表驱动的 fake 扩展器转成框架所需的 Extender 接口切片。
			var extenders []fwk.Extender
			for ii := range test.extenders {
				extenders = append(extenders, &test.extenders[ii])
			}
			// 用测试上下文记录日志，并绑定 cancel 保证测试结束时释放资源。
			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			cache := internalcache.New(ctx, nil, false, false)
			for _, name := range test.nodes {
				// 将表中给出的节点加入调度缓存，构造调度场景。
				cache.AddNode(logger, createNode(name))
			}
			// 注册测试用插件，构造框架；依赖注入 fake 客户端、队列和空快照。
			fwk, err := tf.NewFramework(
				ctx,
				test.registerPlugins, "",
				runtime.WithClientSet(client),
				runtime.WithInformerFactory(informerFactory),
				runtime.WithPodNominator(internalqueue.NewSchedulingQueue(nil, informerFactory)),
				runtime.WithLogger(logger),
				runtime.WithSnapshotSharedLister(emptySnapshot),
			)
			if err != nil {
				t.Fatal(err)
			}

			// 直接组装 Scheduler：注入缓存、扩展器和默认处理链。
			sched := &Scheduler{
				Cache:                    cache,
				nodeInfoSnapshot:         emptySnapshot,
				percentageOfNodesToScore: schedulerapi.DefaultPercentageOfNodesToScore,
				Extenders:                extenders,
				logger:                   logger,
			}
			// 装配默认 handler，否则过滤/打分/绑定阶段不会执行。
			sched.applyDefaultHandlers()

			// 调度依赖快照中的节点信息，必须先刷新。
			if err := sched.Cache.UpdateSnapshot(logger, sched.nodeInfoSnapshot); err != nil {
				t.Fatalf("Unexpected error updating snapshot: %v", err)
			}

			// 用一个空 pod 触发调度；扩展器为 fake，不关心 pod 具体内容。
			podInfoIgnored := queuedPodInfoForPod(&v1.Pod{})
			result, err := sched.SchedulePod(ctx, fwk, framework.NewCycleState(), podInfoIgnored)
			// 按预期分别校验“必须失败”和“必须成功且结果一致”。
			if test.expectsErr {
				if err == nil {
					t.Errorf("Unexpected non-error, result %+v", result)
				}
			} else {
				// 期待调度成功时，先确认无错误再比对结果。
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
					return
				}

				// 比较完整调度结果：选中的节点、评估数、可行数都要一致。
				if diff := cmp.Diff(test.expectedResult, result, scheduleResultCmpOpts...); diff != "" {
					t.Errorf("Unexpected result: (-want, +got):\n%s", diff)
				}
			}
		})
	}
}

// createNode 构造仅含名称的 Node，测试中不关心其他字段。
func createNode(name string) *v1.Node {
	return &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

// TestIsInterested 验证扩展器只关心请求了其所管理资源的 pod。
func TestIsInterested(t *testing.T) {
	// 管理 memory 资源的扩展器，用于覆盖各资源声明场景。
	mem := &HTTPExtender{
		managedResources: sets.New[string](),
	}
	// 插入 "memory"，构造非空管理资源集合。
	mem.managedResources.Insert("memory")

	for _, tc := range []struct {
		label    string
		extender *HTTPExtender
		pod      *v1.Pod
		want     bool
	}{
		{
			// 未管理任何资源时，对任意 pod 都感兴趣。
			label: "Empty managed resources",
			extender: &HTTPExtender{
				managedResources: sets.New[string](),
			},
			pod:  &v1.Pod{},
			want: true,
		},
		{
			// pod 未申请 memory：不感兴趣。
			label:    "Managed memory, empty resources",
			extender: mem,
			pod:      st.MakePod().Container("app").Obj(),
			want:     false,
		},
		{
			// 容器 requests 中是零值也算显式声明：感兴趣。
			label:    "Managed memory, container memory with Requests",
			extender: mem,
			pod: st.MakePod().Req(map[v1.ResourceName]string{
				"memory": "0",
			}).Obj(),
			want: true,
		},
		{
			// limits 同样算声明，覆盖只设置 limits 的场景。
			label:    "Managed memory, container memory with Limits",
			extender: mem,
			pod: st.MakePod().Lim(map[v1.ResourceName]string{
				"memory": "0",
			}).Obj(),
			want: true,
		},
		{
			// init container 的请求也需要被识别，否则会漏调度。
			label:    "Managed memory, init container memory",
			extender: mem,
			pod: st.MakePod().Container("app").InitReq(map[v1.ResourceName]string{
				"memory": "0",
			}).Obj(),
			want: true,
		},
	} {
		t.Run(tc.label, func(t *testing.T) {
			// 逐场景校验 IsInterested 的判定与预期一致。
			if got := tc.extender.IsInterested(tc.pod); got != tc.want {
				t.Fatalf("IsInterested(%v) = %v, wanted %v", tc.pod, got, tc.want)
			}
		})
	}
}

// TestConvertToMetaVictims 验证转换只保留 pod UID 和 NumPDBViolations。
func TestConvertToMetaVictims(t *testing.T) {
	tests := []struct {
		name              string
		nodeNameToVictims map[string]*extenderv1.Victims
		want              map[string]*extenderv1.MetaVictims
	}{
		{
			// 两个节点各有不同违规次数：验证字段随节点一起被转换，没有串位。
			name: "test NumPDBViolations is transferred from nodeNameToVictims to nodeNameToMetaVictims",
			nodeNameToVictims: map[string]*extenderv1.Victims{
				"node1": {
					Pods: []*v1.Pod{
						st.MakePod().Name("pod1").UID("uid1").Obj(),
						st.MakePod().Name("pod3").UID("uid3").Obj(),
					},
					NumPDBViolations: 1,
				},
				"node2": {
					Pods: []*v1.Pod{
						st.MakePod().Name("pod2").UID("uid2").Obj(),
						st.MakePod().Name("pod4").UID("uid4").Obj(),
					},
					NumPDBViolations: 2,
				},
			},
			want: map[string]*extenderv1.MetaVictims{
				"node1": {
					Pods: []*extenderv1.MetaPod{
						{UID: "uid1"},
						{UID: "uid3"},
					},
					NumPDBViolations: 1,
				},
				"node2": {
					Pods: []*extenderv1.MetaPod{
						{UID: "uid2"},
						{UID: "uid4"},
					},
					NumPDBViolations: 2,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 比对整个映射，确保 pod 顺序和违规次数都正确传递。
			got := convertToMetaVictims(tt.nodeNameToVictims)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Unexpected convertToMetaVictims(): (-want, +got):\n%s", diff)
			}
		})
	}
}

// TestConvertToVictims 验证反向转换：按 UID 找回 pod 对象并保留违规次数。
func TestConvertToVictims(t *testing.T) {
	tests := []struct {
		name                  string
		httpExtender          *HTTPExtender
		nodeNameToMetaVictims map[string]*extenderv1.MetaVictims
		nodeNames             []string
		podsInNodeList        []*v1.Pod
		nodeInfos             fwk.NodeInfoLister
		want                  map[string]*extenderv1.Victims
		wantErr               bool
	}{
		{
			// 覆盖多节点、每节点多个 pod：验证 UID 到 pod 的映射正确。
			name:         "test NumPDBViolations is transferred from NodeNameToMetaVictims to newNodeNameToVictims",
			httpExtender: &HTTPExtender{},
			nodeNameToMetaVictims: map[string]*extenderv1.MetaVictims{
				"node1": {
					Pods: []*extenderv1.MetaPod{
						{UID: "uid1"},
						{UID: "uid3"},
					},
					NumPDBViolations: 1,
				},
				"node2": {
					Pods: []*extenderv1.MetaPod{
						{UID: "uid2"},
						{UID: "uid4"},
					},
					NumPDBViolations: 2,
				},
			},
			nodeNames: []string{"node1", "node2"},
			podsInNodeList: []*v1.Pod{
				st.MakePod().Name("pod1").UID("uid1").Obj(),
				st.MakePod().Name("pod2").UID("uid2").Obj(),
				st.MakePod().Name("pod3").UID("uid3").Obj(),
				st.MakePod().Name("pod4").UID("uid4").Obj(),
			},
			nodeInfos: nil,
			want: map[string]*extenderv1.Victims{
				"node1": {
					Pods: []*v1.Pod{
						st.MakePod().Name("pod1").UID("uid1").Obj(),
						st.MakePod().Name("pod3").UID("uid3").Obj(),
					},
					NumPDBViolations: 1,
				},
				"node2": {
					Pods: []*v1.Pod{
						st.MakePod().Name("pod2").UID("uid2").Obj(),
						st.MakePod().Name("pod4").UID("uid4").Obj(),
					},
					NumPDBViolations: 2,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 从节点名和 pod 列表构造 NodeInfoLister：下标 i 与 i+2 分别对应
			// 该节点上的两个 pod，模拟两个节点各承载两个 victim。
			nodeInfoList := make([]fwk.NodeInfo, 0, len(tt.nodeNames))
			for i, nm := range tt.nodeNames {
				nodeInfo := framework.NewNodeInfo()
				node := createNode(nm)
				nodeInfo.SetNode(node)
				nodeInfo.AddPod(tt.podsInNodeList[i])
				nodeInfo.AddPod(tt.podsInNodeList[i+2])
				nodeInfoList = append(nodeInfoList, nodeInfo)
			}
			// 把构造好的 lister 注入用例，供转换函数按节点查找 pod。
			tt.nodeInfos = tf.NodeInfoLister(nodeInfoList)

			// 先校验错误预期，再比对转换结果，避免错误分支掩盖结果差异。
			got, err := tt.httpExtender.convertToVictims(tt.nodeNameToMetaVictims, tt.nodeInfos)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertToVictims() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Unexpected convertToVictims(): (-want, +got):\n%s", diff)
			}
		})
	}
}
