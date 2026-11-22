/*
Copyright 2014 The Kubernetes Authors.

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
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/events"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-base/metrics/legacyregistry"
	fifometrics "k8s.io/component-base/metrics/prometheus/clientgo/fifo"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/testing/defaults"
	apicache "k8s.io/kubernetes/pkg/scheduler/backend/api_cache"
	apidispatcher "k8s.io/kubernetes/pkg/scheduler/backend/api_dispatcher"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/backend/queue"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	apicalls "k8s.io/kubernetes/pkg/scheduler/framework/api_calls"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/profile"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	tf "k8s.io/kubernetes/pkg/scheduler/testing/framework"
	utiltesting "k8s.io/kubernetes/test/utils/ktesting"
	testingclock "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
)

// init 注册调度器指标，避免各测试用例各自重复注册。
func init() {
	metrics.Register()
}

// TestSchedulerCreation 验证 New 在不同选项组合下的行为：
// 覆盖合法/冲突的 out-of-tree 插件注册表、多 profile、重复 profile、
// extender 注入以及自定义 nodeInfoSnapshot。
// 同时确认构造失败时返回的错误信息可被调用方识别。
func TestSchedulerCreation(t *testing.T) {
	// invalidRegistry 复用了内置插件名 DefaultBinder，用于触发注册表冲突。
	invalidRegistry := map[string]frameworkruntime.PluginFactory{
		defaultbinder.Name: defaultbinder.New,
	}
	// validRegistry 使用全新插件名，用于验证 out-of-tree 插件可正常注册。
	validRegistry := map[string]frameworkruntime.PluginFactory{
		"Foo": defaultbinder.New,
	}
	// customSnapshot 用于验证 WithNodeInfoSnapshot 能把快照注入到调度器。
	customSnapshot := internalcache.NewEmptySnapshot()
	cases := []struct {
		name                 string
		opts                 []Option
		wantErr              string
		wantProfiles         []string
		wantExtenders        []string
		wantNodeInfoSnapshot *internalcache.Snapshot
	}{
		{
			name: "valid out-of-tree registry",
			opts: []Option{
				WithFrameworkOutOfTreeRegistry(validRegistry),
				WithProfiles(
					schedulerapi.KubeSchedulerProfile{
						SchedulerName: "default-scheduler",
						Plugins: &schedulerapi.Plugins{
							QueueSort: schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "PrioritySort"}}},
							Bind:      schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "DefaultBinder"}}},
						},
					},
				)},
			wantProfiles: []string{"default-scheduler"},
		},
		{
			name: "repeated plugin name in out-of-tree plugin",
			opts: []Option{
				WithFrameworkOutOfTreeRegistry(invalidRegistry),
				WithProfiles(
					schedulerapi.KubeSchedulerProfile{
						SchedulerName: "default-scheduler",
						Plugins: &schedulerapi.Plugins{
							QueueSort: schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "PrioritySort"}}},
							Bind:      schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "DefaultBinder"}}},
						},
					},
				)},
			wantProfiles: []string{"default-scheduler"},
			wantErr:      "a plugin named DefaultBinder already exists",
		},
		{
			name: "multiple profiles",
			opts: []Option{
				WithProfiles(
					schedulerapi.KubeSchedulerProfile{
						SchedulerName: "foo",
						Plugins: &schedulerapi.Plugins{
							QueueSort: schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "PrioritySort"}}},
							Bind:      schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "DefaultBinder"}}},
						},
					},
					schedulerapi.KubeSchedulerProfile{
						SchedulerName: "bar",
						Plugins: &schedulerapi.Plugins{
							QueueSort: schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "PrioritySort"}}},
							Bind:      schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "DefaultBinder"}}},
						},
					},
				)},
			wantProfiles: []string{"bar", "foo"},
		},
		{
			name: "Repeated profiles",
			opts: []Option{
				WithProfiles(
					schedulerapi.KubeSchedulerProfile{
						SchedulerName: "foo",
						Plugins: &schedulerapi.Plugins{
							QueueSort: schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "PrioritySort"}}},
							Bind:      schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "DefaultBinder"}}},
						},
					},
					schedulerapi.KubeSchedulerProfile{
						SchedulerName: "bar",
						Plugins: &schedulerapi.Plugins{
							QueueSort: schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "PrioritySort"}}},
							Bind:      schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "DefaultBinder"}}},
						},
					},
					schedulerapi.KubeSchedulerProfile{
						SchedulerName: "foo",
						Plugins: &schedulerapi.Plugins{
							QueueSort: schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "PrioritySort"}}},
							Bind:      schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "DefaultBinder"}}},
						},
					},
				)},
			wantErr: "duplicate profile with scheduler name \"foo\"",
		},
		{
			name: "With extenders",
			opts: []Option{
				WithProfiles(
					schedulerapi.KubeSchedulerProfile{
						SchedulerName: "default-scheduler",
						Plugins: &schedulerapi.Plugins{
							QueueSort: schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "PrioritySort"}}},
							Bind:      schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "DefaultBinder"}}},
						},
					},
				),
				WithExtenders(
					schedulerapi.Extender{
						URLPrefix: "http://extender.kube-system/",
					},
				),
			},
			wantProfiles:  []string{"default-scheduler"},
			wantExtenders: []string{"http://extender.kube-system/"},
		},
		{
			name: "With custom nodeInfoSnapshot",
			opts: []Option{
				WithNodeInfoSnapshot(customSnapshot),
			},
			wantProfiles:         []string{"default-scheduler"},
			wantNodeInfoSnapshot: customSnapshot,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewClientset()
			informerFactory := informers.NewSharedInformerFactory(client, 0)

			eventBroadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: client.EventsV1()})

			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			s, err := New(
				ctx,
				client,
				informerFactory,
				nil,
				profile.NewRecorderFactory(eventBroadcaster),
				tc.opts...,
			)

			// 错误分支：期望 New 失败，且错误信息包含 wantErr。
			if len(tc.wantErr) != 0 {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("got error %q, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Failed to create scheduler: %v", err)
			}

			// 校验 profile 名称集合；map 遍历无序，需先排序再比较。
			profiles := make([]string, 0, len(s.Profiles))
			for name := range s.Profiles {
				profiles = append(profiles, name)
			}
			sort.Strings(profiles)
			if diff := cmp.Diff(tc.wantProfiles, profiles); diff != "" {
				t.Errorf("unexpected profiles (-want, +got):\n%s", diff)
			}

			// 校验 extender：调度器本身与每个 profile 的 framework 都必须持有相同的 extender。
			if len(tc.wantExtenders) != 0 {
				// Scheduler.Extenders 上的 extender。
				extenders := make([]string, 0, len(s.Extenders))
				for _, e := range s.Extenders {
					extenders = append(extenders, e.Name())
				}
				if diff := cmp.Diff(tc.wantExtenders, extenders); diff != "" {
					t.Errorf("unexpected extenders (-want, +got):\n%s", diff)
				}

				// fwk.Handle.Extenders() 上的 extender，确认注入也传播到了框架层。
				for _, p := range s.Profiles {
					extenders := make([]string, 0, len(p.Extenders()))
					for _, e := range p.Extenders() {
						extenders = append(extenders, e.Name())
					}
					if diff := cmp.Diff(tc.wantExtenders, extenders); diff != "" {
						t.Errorf("unexpected extenders (-want, +got):\n%s", diff)
					}
				}
			}

			// 校验自定义快照是否被原样注入（比较指针，要求同一实例）。
			if tc.wantNodeInfoSnapshot != nil && s.nodeInfoSnapshot != tc.wantNodeInfoSnapshot {
				t.Errorf("unexpected nodeInfoSnapshot: got %p, want %p", s.nodeInfoSnapshot, tc.wantNodeInfoSnapshot)
			}
		})
	}
}

// TestFailureHandler 验证失败处理：调度周期内发生 pod 更新/删除时，
// FailureHandler 应把队列里的 pod 恢复成最新状态（更新后的 pod），或删除已不存在的 pod。
// 同时覆盖同步与异步 API 调用两种模式（后者经 APIDispatcher 处理）。
func TestFailureHandler(t *testing.T) {
	metrics.Register()
	// testPodUpdated 模拟调度周期内 pod 被加上标签更新。
	testPod := st.MakePod().Name("test-pod").Namespace(v1.NamespaceDefault).Obj()
	testPodUpdated := testPod.DeepCopy()
	testPodUpdated.Labels = map[string]string{"foo": ""}

	tests := []struct {
		name                       string
		podUpdatedDuringScheduling bool // 调度周期内 pod 被更新
		podDeletedDuringScheduling bool // 调度周期内 pod 被删除
		expect                     *v1.Pod
	}{
		{
			name:                       "pod is updated during a scheduling cycle",
			podUpdatedDuringScheduling: true,
			expect:                     testPodUpdated,
		},
		{
			name:   "pod is not updated during a scheduling cycle",
			expect: testPod,
		},
		{
			name:                       "pod is deleted during a scheduling cycle",
			podDeletedDuringScheduling: true,
			expect:                     nil,
		},
	}

	for _, asyncAPICallsEnabled := range []bool{true, false} {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%s (Async API calls enabled: %v)", tt.name, asyncAPICallsEnabled), func(t *testing.T) {
				logger, ctx := ktesting.NewTestContext(t)
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()

				client := fake.NewClientset(&v1.PodList{Items: []v1.Pod{*testPod}})
				informerFactory := informers.NewSharedInformerFactory(client, 0)
				podInformer := informerFactory.Core().V1().Pods()
				// FailureHandler 通过 informer store 读取 pod 最新状态，
				// 因此必须先手动把 testPod 放进 store。
				if err := podInformer.Informer().GetStore().Add(testPod); err != nil {
					t.Fatal(err)
				}

				var apiDispatcher *apidispatcher.APIDispatcher
				if asyncAPICallsEnabled {
					apiDispatcher = apidispatcher.New(client, 16, apicalls.Relevances)
					apiDispatcher.Run(logger)
					defer apiDispatcher.Close()
				}

				recorder := metrics.NewMetricsAsyncRecorder(3, 20*time.Microsecond, ctx.Done())
				queue := internalqueue.NewPriorityQueue(nil, informerFactory, internalqueue.WithClock(testingclock.NewFakeClock(time.Now())), internalqueue.WithMetricsRecorder(recorder), internalqueue.WithAPIDispatcher(apiDispatcher))
				schedulerCache := internalcache.New(ctx, apiDispatcher, false, false)

				queue.Add(ctx, testPod)

				if _, err := queue.Pop(logger); err != nil {
					t.Fatalf("Pop failed: %v", err)
				}

				if tt.podUpdatedDuringScheduling {
					if err := podInformer.Informer().GetStore().Update(testPodUpdated); err != nil {
						t.Fatal(err)
					}
					queue.Update(ctx, testPod, testPodUpdated)
				}
				if tt.podDeletedDuringScheduling {
					if err := podInformer.Informer().GetStore().Delete(testPod); err != nil {
						t.Fatal(err)
					}
					queue.Delete(logger, testPod)
				}

				s, schedFramework, err := initScheduler(ctx, schedulerCache, queue, apiDispatcher, client, informerFactory)
				if err != nil {
					t.Fatal(err)
				}

				testPodInfo := &framework.QueuedPodInfo{PodInfo: mustNewPodInfo(t, testPod)}
				s.FailureHandler(ctx, schedFramework, testPodInfo, fwk.NewStatus(fwk.Unschedulable), nil, time.Now())

				var got *v1.Pod
				if tt.podUpdatedDuringScheduling {
					pInfo, ok := queue.GetPod(testPod.Name, testPod.Namespace, testPod.Spec.SchedulingGroup)
					if !ok {
						t.Fatalf("Failed to get pod %s/%s from queue", testPod.Namespace, testPod.Name)
					}
					got = pInfo.Pod
				} else {
					got = getPodFromPriorityQueue(queue, testPod)
				}

				if diff := cmp.Diff(tt.expect, got); diff != "" {
					t.Errorf("Unexpected pod (-want, +got): %s", diff)
				}
			})
		}
	}
}

// TestFailureHandler_PodAlreadyBound 验证当 pod 已经绑定到节点（NodeName 非空）时，
// FailureHandler 不应把它重新放回优先级队列，避免对已调度的 pod 重复调度。
func TestFailureHandler_PodAlreadyBound(t *testing.T) {
	for _, asyncAPICallsEnabled := range []bool{true, false} {
		t.Run(fmt.Sprintf("Async API calls enabled: %v", asyncAPICallsEnabled), func(t *testing.T) {
			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			nodeFoo := v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "foo"}}
			testPod := st.MakePod().Name("test-pod").Namespace(v1.NamespaceDefault).Node("foo").Obj()

			client := fake.NewClientset(&v1.PodList{Items: []v1.Pod{*testPod}}, &v1.NodeList{Items: []v1.Node{nodeFoo}})
			informerFactory := informers.NewSharedInformerFactory(client, 0)
			podInformer := informerFactory.Core().V1().Pods()
			// 先手动把 testPod 放进 store，供 FailureHandler 读取其最新状态。
			if err := podInformer.Informer().GetStore().Add(testPod); err != nil {
				t.Fatal(err)
			}

			var apiDispatcher *apidispatcher.APIDispatcher
			if asyncAPICallsEnabled {
				apiDispatcher = apidispatcher.New(client, 16, apicalls.Relevances)
				apiDispatcher.Run(logger)
				defer apiDispatcher.Close()
			}

			queue := internalqueue.NewPriorityQueue(nil, informerFactory, internalqueue.WithClock(testingclock.NewFakeClock(time.Now())), internalqueue.WithAPIDispatcher(apiDispatcher))
			schedulerCache := internalcache.New(ctx, apiDispatcher, false, false)

			// 无论该节点在 API server 中是否被删除，都把它加入调度器缓存，
			// 让 FailureHandler 在判断 pod 状态时能找到对应节点。
			schedulerCache.AddNode(logger, &nodeFoo)

			s, schedFramework, err := initScheduler(ctx, schedulerCache, queue, apiDispatcher, client, informerFactory)
			if err != nil {
				t.Fatal(err)
			}

			testPodInfo := &framework.QueuedPodInfo{PodInfo: mustNewPodInfo(t, testPod)}
			s.FailureHandler(ctx, schedFramework, testPodInfo, fwk.NewStatus(fwk.Unschedulable).WithError(fmt.Errorf("binding rejected: timeout")), nil, time.Now())

			pod := getPodFromPriorityQueue(queue, testPod)
			if pod != nil {
				t.Fatalf("Unexpected pod: %v should not be in PriorityQueue when the NodeName of pod is not empty", pod.Name)
			}
		})
	}
}

// TestWithPercentageOfNodesToScore 验证 WithPercentageOfNodesToScore 选项：
// 未配置时应回退到默认值 DefaultPercentageOfNodesToScore，显式配置时按配置值生效。
func TestWithPercentageOfNodesToScore(t *testing.T) {
	tests := []struct {
		name                           string
		percentageOfNodesToScoreConfig *int32
		wantedPercentageOfNodesToScore int32
	}{
		{
			name:                           "percentageOfNodesScore is nil",
			percentageOfNodesToScoreConfig: nil,
			wantedPercentageOfNodesToScore: schedulerapi.DefaultPercentageOfNodesToScore,
		},
		{
			name:                           "percentageOfNodesScore is not nil",
			percentageOfNodesToScoreConfig: ptr.To[int32](10),
			wantedPercentageOfNodesToScore: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := fake.NewClientset()
			informerFactory := informers.NewSharedInformerFactory(client, 0)
			eventBroadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: client.EventsV1()})
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			sched, err := New(
				ctx,
				client,
				informerFactory,
				nil,
				profile.NewRecorderFactory(eventBroadcaster),
				WithPercentageOfNodesToScore(tt.percentageOfNodesToScoreConfig),
			)
			if err != nil {
				t.Fatalf("Failed to create scheduler: %v", err)
			}
			if sched.percentageOfNodesToScore != tt.wantedPercentageOfNodesToScore {
				t.Errorf("scheduler.percercentageOfNodesToScore = %v, want %v", sched.percentageOfNodesToScore, tt.wantedPercentageOfNodesToScore)
			}
		})
	}
}

// getPodFromPriorityQueue 从优先级队列的 pending pods 中按 namespace/name 查找指定 pod。
// 用 MetaNamespaceKeyFunc 生成 key 比较，避免指针比较；找不到返回 nil。
func getPodFromPriorityQueue(queue *internalqueue.PriorityQueue, pod *v1.Pod) *v1.Pod {
	podList, _ := queue.PendingPods()
	if len(podList) == 0 {
		return nil
	}

	queryPodKey, err := cache.MetaNamespaceKeyFunc(pod)
	if err != nil {
		return nil
	}

	for _, foundPod := range podList {
		foundPodKey, err := cache.MetaNamespaceKeyFunc(foundPod)
		if err != nil {
			return nil
		}

		if foundPodKey == queryPodKey {
			return foundPod
		}
	}

	return nil
}

// initScheduler 构造一个测试用 Scheduler 及其 Framework：
// 注册 no-op 的 QueueSort/Bind 插件，按需接入 APIDispatcher 与 APICacher，
// 并调用 applyDefaultHandlers 装上默认事件处理器，方便测试直接调用 FailureHandler 等方法。
func initScheduler(ctx context.Context, cache internalcache.Cache, queue internalqueue.SchedulingQueue, apiDispatcher *apidispatcher.APIDispatcher,
	client kubernetes.Interface, informerFactory informers.SharedInformerFactory) (*Scheduler, framework.Framework, error) {
	logger := klog.FromContext(ctx)
	registerPluginFuncs := []tf.RegisterPluginFunc{
		tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
		tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
	}
	eventBroadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: client.EventsV1()})
	waitingPods := frameworkruntime.NewWaitingPodsMap()
	fwk, err := tf.NewFramework(ctx,
		registerPluginFuncs,
		testSchedulerName,
		frameworkruntime.WithClientSet(client),
		frameworkruntime.WithAPIDispatcher(apiDispatcher),
		frameworkruntime.WithInformerFactory(informerFactory),
		frameworkruntime.WithEventRecorder(eventBroadcaster.NewRecorder(scheme.Scheme, testSchedulerName)),
		frameworkruntime.WithWaitingPods(waitingPods),
	)
	if err != nil {
		return nil, nil, err
	}
	if apiDispatcher != nil {
		fwk.SetAPICacher(apicache.New(queue, cache))
	}

	s := &Scheduler{
		Cache:           cache,
		client:          client,
		StopEverything:  ctx.Done(),
		SchedulingQueue: queue,
		APIDispatcher:   apiDispatcher,
		Profiles:        profile.Map{testSchedulerName: fwk},
		logger:          logger,
	}
	s.applyDefaultHandlers()

	return s, fwk, nil
}

// TestInitPluginsWithIndexers 验证框架初始化时对 informer indexer 冲突的处理：
// 同名 indexer 注册两次会报错，不同名字的 indexer 即使实现相同也不冲突。
func TestInitPluginsWithIndexers(t *testing.T) {
	tests := []struct {
		name string
		// 用 map 存储插件工厂，使注册顺序随机，借此验证结果与顺序无关。
		entrypoints map[string]frameworkruntime.PluginFactory
		wantErr     string
	}{
		{
			name: "register indexer, no conflicts",
			entrypoints: map[string]frameworkruntime.PluginFactory{
				"AddIndexer": func(ctx context.Context, obj runtime.Object, handle fwk.Handle) (fwk.Plugin, error) {
					podInformer := handle.SharedInformerFactory().Core().V1().Pods()
					err := podInformer.Informer().AddIndexers(cache.Indexers{
						"nodeName": indexByPodSpecNodeName,
					})
					return &TestPlugin{name: "AddIndexer"}, err
				},
			},
		},
		{
			name: "register the same indexer name multiple times, conflict",
			// 顺序无关：无论哪个工厂先执行，同名 "nodeName" 的 indexer 必然冲突。
			entrypoints: map[string]frameworkruntime.PluginFactory{
				"AddIndexer1": func(ctx context.Context, obj runtime.Object, handle fwk.Handle) (fwk.Plugin, error) {
					podInformer := handle.SharedInformerFactory().Core().V1().Pods()
					err := podInformer.Informer().AddIndexers(cache.Indexers{
						"nodeName": indexByPodSpecNodeName,
					})
					return &TestPlugin{name: "AddIndexer1"}, err
				},
				"AddIndexer2": func(ctx context.Context, obj runtime.Object, handle fwk.Handle) (fwk.Plugin, error) {
					podInformer := handle.SharedInformerFactory().Core().V1().Pods()
					err := podInformer.Informer().AddIndexers(cache.Indexers{
						"nodeName": indexByPodAnnotationNodeName,
					})
					return &TestPlugin{name: "AddIndexer1"}, err
				},
			},
			wantErr: "indexer conflict",
		},
		{
			name: "register the same indexer body with different names, no conflicts",
			// 顺序无关：indexer 名不同（"nodeName1"/"nodeName2"），实现相同也不冲突。
			entrypoints: map[string]frameworkruntime.PluginFactory{
				"AddIndexer1": func(ctx context.Context, obj runtime.Object, handle fwk.Handle) (fwk.Plugin, error) {
					podInformer := handle.SharedInformerFactory().Core().V1().Pods()
					err := podInformer.Informer().AddIndexers(cache.Indexers{
						"nodeName1": indexByPodSpecNodeName,
					})
					return &TestPlugin{name: "AddIndexer1"}, err
				},
				"AddIndexer2": func(ctx context.Context, obj runtime.Object, handle fwk.Handle) (fwk.Plugin, error) {
					podInformer := handle.SharedInformerFactory().Core().V1().Pods()
					err := podInformer.Informer().AddIndexers(cache.Indexers{
						"nodeName2": indexByPodAnnotationNodeName,
					})
					return &TestPlugin{name: "AddIndexer2"}, err
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeInformerFactory := NewInformerFactory(&fake.Clientset{}, 0*time.Second, nil)

			var registerPluginFuncs []tf.RegisterPluginFunc
			for name, entrypoint := range tt.entrypoints {
				registerPluginFuncs = append(registerPluginFuncs,
					// 这里只关心 indexer 注册，用哪个扩展点注册插件都行。
					tf.RegisterFilterPlugin(name, entrypoint),
				)
			}
			// 框架初始化必须有 QueueSort 和 Bind 插件，否则无法建框架。
			registerPluginFuncs = append(registerPluginFuncs,
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			)
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			_, err := tf.NewFramework(ctx, registerPluginFuncs, "test", frameworkruntime.WithInformerFactory(fakeInformerFactory))

			if len(tt.wantErr) > 0 {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("got error %q, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Failed to create scheduler: %v", err)
			}
		})
	}
}

// indexByPodSpecNodeName 按 pod.Spec.NodeName 建索引，供已分配 pod 相关查询使用；
// 非 Pod 对象或 NodeName 为空时返回空索引。
func indexByPodSpecNodeName(obj interface{}) ([]string, error) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return []string{}, nil
	}
	if len(pod.Spec.NodeName) == 0 {
		return []string{}, nil
	}
	return []string{pod.Spec.NodeName}, nil
}

// indexByPodAnnotationNodeName 按 pod 的 "node-name" 注解建索引；
// 非 Pod、无注解或无该注解时返回空索引。
func indexByPodAnnotationNodeName(obj interface{}) ([]string, error) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return []string{}, nil
	}
	if len(pod.Annotations) == 0 {
		return []string{}, nil
	}
	nodeName, ok := pod.Annotations["node-name"]
	if !ok {
		return []string{}, nil
	}
	return []string{nodeName}, nil
}

// 以下常量是各 mock 插件的名字，既作为注册表 key，也出现在断言的期望值里。
const (
	filterWithoutEnqueueExtensions = "filterWithoutEnqueueExtensions"
	fakeNode                       = "fakeNode"
	fakePod                        = "fakePod"
	emptyEventsToRegister          = "emptyEventsToRegister"
	errorEventsToRegister          = "errorEventsToRegister"
	queueSort                      = "no-op-queue-sort-plugin"
	fakeBind                       = "bind-plugin"
	emptyEventExtensions           = "emptyEventExtensions"
	fakePermit                     = "fakePermit"
	fakeAssignedPod                = "fakeAssignedPodPlugin"
)

// Test_buildQueueingHintMap 验证 buildQueueingHintMap 把各插件 EnqueueExtensions
// 聚合为 cluster event 到 QueueingHintFunction 的映射：
// 未实现 EnqueueExtensions 的插件为所有事件注册默认 hint；
// 同名事件多次注册时 hint 函数按插件名排序追加；EventsToRegister 返回错误时向上传播。
func Test_buildQueueingHintMap(t *testing.T) {
	tests := []struct {
		name    string
		plugins []fwk.Plugin
		want    map[fwk.ClusterEvent][]*internalqueue.QueueingHintFunction
		wantErr error
	}{
		{
			name:    "filter without EnqueueExtensions plugin",
			plugins: []fwk.Plugin{&filterWithoutEnqueueExtensionsPlugin{}},
			want: map[fwk.ClusterEvent][]*internalqueue.QueueingHintFunction{
				{Resource: fwk.AssignedPod, ActionType: fwk.All}: {
					{PluginName: filterWithoutEnqueueExtensions, QueueingHintFn: defaultQueueingHintFn},
				},
				{Resource: fwk.UnscheduledPod, ActionType: fwk.All}: {
					{PluginName: filterWithoutEnqueueExtensions, QueueingHintFn: defaultQueueingHintFn},
				},
				{Resource: fwk.TargetPod, ActionType: fwk.All}: {
					{PluginName: filterWithoutEnqueueExtensions, QueueingHintFn: defaultQueueingHintFn},
				},
				{Resource: fwk.Node, ActionType: fwk.All}: {
					{PluginName: filterWithoutEnqueueExtensions, QueueingHintFn: defaultQueueingHintFn},
				},
				{Resource: fwk.CSINode, ActionType: fwk.All}: {
					{PluginName: filterWithoutEnqueueExtensions, QueueingHintFn: defaultQueueingHintFn},
				},
				{Resource: fwk.CSIDriver, ActionType: fwk.All}: {
					{PluginName: filterWithoutEnqueueExtensions, QueueingHintFn: defaultQueueingHintFn},
				},
				{Resource: fwk.CSIStorageCapacity, ActionType: fwk.All}: {
					{PluginName: filterWithoutEnqueueExtensions, QueueingHintFn: defaultQueueingHintFn},
				},
				{Resource: fwk.PersistentVolume, ActionType: fwk.All}: {
					{PluginName: filterWithoutEnqueueExtensions, QueueingHintFn: defaultQueueingHintFn},
				},
				{Resource: fwk.StorageClass, ActionType: fwk.All}: {
					{PluginName: filterWithoutEnqueueExtensions, QueueingHintFn: defaultQueueingHintFn},
				},
				{Resource: fwk.PersistentVolumeClaim, ActionType: fwk.All}: {
					{PluginName: filterWithoutEnqueueExtensions, QueueingHintFn: defaultQueueingHintFn},
				},
				{Resource: fwk.ResourceClaim, ActionType: fwk.All}: {
					{PluginName: filterWithoutEnqueueExtensions, QueueingHintFn: defaultQueueingHintFn},
				},
				{Resource: fwk.DeviceClass, ActionType: fwk.All}: {
					{PluginName: filterWithoutEnqueueExtensions, QueueingHintFn: defaultQueueingHintFn},
				},
				{Resource: fwk.PodGroup, ActionType: fwk.All}: {
					{PluginName: filterWithoutEnqueueExtensions, QueueingHintFn: defaultQueueingHintFn},
				},
			},
		},
		{
			name:    "node and pod plugin",
			plugins: []fwk.Plugin{&fakeNodePlugin{}, &fakePodPlugin{}},
			want: map[fwk.ClusterEvent][]*internalqueue.QueueingHintFunction{
				{Resource: fwk.AssignedPod, ActionType: fwk.Add}: {
					{PluginName: fakePod, QueueingHintFn: fakePodPluginQueueingFn},
				},
				{Resource: fwk.UnscheduledPod, ActionType: fwk.Add}: {
					{PluginName: fakePod, QueueingHintFn: fakePodPluginQueueingFn},
				},
				{Resource: fwk.TargetPod, ActionType: fwk.Add}: {
					{PluginName: fakePod, QueueingHintFn: fakePodPluginQueueingFn},
				},
				{Resource: fwk.Node, ActionType: fwk.Add}: {
					{PluginName: fakeNode, QueueingHintFn: fakeNodePluginQueueingFn},
				},
				{Resource: fwk.Node, ActionType: fwk.UpdateNodeTaint}: {
					// 注册 Node/Add 时框架会自动补上 Node/UpdateNodeTaint。
					{PluginName: fakeNode, QueueingHintFn: defaultQueueingHintFn},
				},
			},
		},
		{
			name:    "register plugin with empty event",
			plugins: []fwk.Plugin{&emptyEventPlugin{}},
			want:    map[fwk.ClusterEvent][]*internalqueue.QueueingHintFunction{},
		},
		{
			name:    "register plugins including emptyEventPlugin",
			plugins: []fwk.Plugin{&emptyEventPlugin{}, &fakeNodePlugin{}},
			want: map[fwk.ClusterEvent][]*internalqueue.QueueingHintFunction{
				{Resource: fwk.AssignedPod, ActionType: fwk.Add}: {
					{PluginName: fakePod, QueueingHintFn: fakePodPluginQueueingFn},
				},
				{Resource: fwk.UnscheduledPod, ActionType: fwk.Add}: {
					{PluginName: fakePod, QueueingHintFn: fakePodPluginQueueingFn},
				},
				{Resource: fwk.TargetPod, ActionType: fwk.Add}: {
					{PluginName: fakePod, QueueingHintFn: fakePodPluginQueueingFn},
				},
				{Resource: fwk.Node, ActionType: fwk.Add}: {
					{PluginName: fakeNode, QueueingHintFn: fakeNodePluginQueueingFn},
				},
				{Resource: fwk.Node, ActionType: fwk.UpdateNodeTaint}: {
					// 注册 Node/Add 时框架会自动补上 Node/UpdateNodeTaint。
					{PluginName: fakeNode, QueueingHintFn: defaultQueueingHintFn},
				},
			},
		},
		{
			name:    "one EventsToRegister returns an error",
			plugins: []fwk.Plugin{&errorEventsToRegisterPlugin{}},
			want:    map[fwk.ClusterEvent][]*internalqueue.QueueingHintFunction{},
			wantErr: errors.New("mock error"),
		},
		{
			name:    "plugin registering to a specific pod resource 'AssignedPod'",
			plugins: []fwk.Plugin{&fakeAssignedPodPlugin{}},
			want: map[fwk.ClusterEvent][]*internalqueue.QueueingHintFunction{
				{Resource: fwk.AssignedPod, ActionType: fwk.Update}: {
					{PluginName: fakeAssignedPod, QueueingHintFn: fakeAssignedPodPluginQueueingFn},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			registry := frameworkruntime.Registry{}
			cfgPls := &schedulerapi.Plugins{}
			plugins := append(tt.plugins, &fakebindPlugin{}, &fakeQueueSortPlugin{})
			for _, pl := range plugins {
				if err := registry.Register(pl.Name(), func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
					return pl, nil
				}); err != nil {
					t.Fatalf("fail to register filter plugin (%s)", pl.Name())
				}
				cfgPls.MultiPoint.Enabled = append(cfgPls.MultiPoint.Enabled, schedulerapi.Plugin{Name: pl.Name()})
			}

			profile := schedulerapi.KubeSchedulerProfile{Plugins: cfgPls}
			fwk, err := newFramework(ctx, registry, profile)
			if err != nil {
				t.Fatal(err)
			}

			exts := fwk.EnqueueExtensions()
			// hint 函数按插件名顺序追加，先排序保证断言结果稳定。
			sort.Slice(exts, func(i, j int) bool {
				return exts[i].Name() < exts[j].Name()
			})

			got, err := buildQueueingHintMap(ctx, exts)
			if err != nil {
				if tt.wantErr != nil && tt.wantErr.Error() != err.Error() {
					t.Fatalf("unexpected error from buildQueueingHintMap: expected: %v, actual: %v", tt.wantErr, err)
				}

				if tt.wantErr == nil {
					t.Fatalf("unexpected error from buildQueueingHintMap: %v", err)
				}
			}

			for e, fns := range got {
				wantfns, ok := tt.want[e]
				if !ok {
					t.Errorf("got unexpected event %v", e)
					continue
				}
				if len(fns) != len(wantfns) {
					t.Errorf("got %v queueing hint functions, want %v", len(fns), len(wantfns))
					continue
				}
				for i, fn := range fns {
					if fn.PluginName != wantfns[i].PluginName {
						t.Errorf("got plugin name %v, want %v", fn.PluginName, wantfns[i].PluginName)
						continue
					}
					got, gotErr := fn.QueueingHintFn(logger, nil, nil, nil)
					want, wantErr := wantfns[i].QueueingHintFn(logger, nil, nil, nil)
					if got != want || gotErr != wantErr {
						t.Errorf("got queueing hint function (%v) returning (%v, %v), expect it to return (%v, %v)", fn.PluginName, got, gotErr, want, wantErr)
						continue
					}
				}
			}
		})
	}
}

// Test_UnionedGVKs 验证 buildQueueingHintMap 与 UnionedGVKs 的联动：
// 给定一组已启用的插件（含默认 profile 与不同 feature gate 组合），
// 检查最终聚合出的 cluster event 到 ActionType 的映射是否正确。
func Test_UnionedGVKs(t *testing.T) {
	tests := []struct {
		name                            string
		plugins                         schedulerapi.PluginSet
		want                            map[fwk.EventResource]fwk.ActionType
		enableInPlacePodVerticalScaling bool
		enableDynamicResourceAllocation bool
		enableGenericWorkload           bool
	}{
		{
			name: "filter without EnqueueExtensions plugin",
			plugins: schedulerapi.PluginSet{
				Enabled: []schedulerapi.Plugin{
					{Name: filterWithoutEnqueueExtensions},
					{Name: queueSort},
					{Name: fakeBind},
				},
				Disabled: []schedulerapi.Plugin{{Name: "*"}}, // 禁用默认插件，只保留 Enabled 中列出的 mock 插件
			},
			want: map[fwk.EventResource]fwk.ActionType{
				fwk.AssignedPod:           fwk.All,
				fwk.UnscheduledPod:        fwk.All,
				fwk.TargetPod:             fwk.All,
				fwk.Node:                  fwk.All,
				fwk.CSINode:               fwk.All,
				fwk.CSIDriver:             fwk.All,
				fwk.CSIStorageCapacity:    fwk.All,
				fwk.PersistentVolume:      fwk.All,
				fwk.PersistentVolumeClaim: fwk.All,
				fwk.StorageClass:          fwk.All,
				fwk.ResourceClaim:         fwk.All,
				fwk.DeviceClass:           fwk.All,
			},
		},
		{
			name: "filter without EnqueueExtensions plugin (GenericWorkload enabled)",
			plugins: schedulerapi.PluginSet{
				Enabled: []schedulerapi.Plugin{
					{Name: filterWithoutEnqueueExtensions},
					{Name: queueSort},
					{Name: fakeBind},
				},
				Disabled: []schedulerapi.Plugin{{Name: "*"}}, // 禁用默认插件，只保留 Enabled 中列出的 mock 插件
			},
			want: map[fwk.EventResource]fwk.ActionType{
				fwk.AssignedPod:           fwk.All,
				fwk.UnscheduledPod:        fwk.All,
				fwk.TargetPod:             fwk.All,
				fwk.Node:                  fwk.All,
				fwk.CSINode:               fwk.All,
				fwk.CSIDriver:             fwk.All,
				fwk.CSIStorageCapacity:    fwk.All,
				fwk.PersistentVolume:      fwk.All,
				fwk.PersistentVolumeClaim: fwk.All,
				fwk.StorageClass:          fwk.All,
				fwk.ResourceClaim:         fwk.All,
				fwk.DeviceClass:           fwk.All,
				fwk.PodGroup:              fwk.All,
			},
			enableGenericWorkload:           true,
			enableInPlacePodVerticalScaling: true,
			enableDynamicResourceAllocation: true,
		},
		{
			name: "node plugin",
			plugins: schedulerapi.PluginSet{
				Enabled: []schedulerapi.Plugin{
					{Name: fakeNode},
					{Name: queueSort},
					{Name: fakeBind},
				},
				Disabled: []schedulerapi.Plugin{{Name: "*"}}, // 禁用默认插件，只保留 Enabled 中列出的 mock 插件
			},
			want: map[fwk.EventResource]fwk.ActionType{
				fwk.Node: fwk.Add,
			},
		},
		{
			name: "pod plugin",
			plugins: schedulerapi.PluginSet{
				Enabled: []schedulerapi.Plugin{
					{Name: fakePod},
					{Name: queueSort},
					{Name: fakeBind},
				},
				Disabled: []schedulerapi.Plugin{{Name: "*"}}, // 禁用默认插件，只保留 Enabled 中列出的 mock 插件
			},
			want: map[fwk.EventResource]fwk.ActionType{
				fwk.AssignedPod:    fwk.Add,
				fwk.UnscheduledPod: fwk.Add,
				fwk.TargetPod:      fwk.Add,
			},
		},
		{
			name: "node and pod plugin",
			plugins: schedulerapi.PluginSet{
				Enabled: []schedulerapi.Plugin{
					{Name: fakePod},
					{Name: fakeNode},
					{Name: queueSort},
					{Name: fakeBind},
				},
				Disabled: []schedulerapi.Plugin{{Name: "*"}}, // 禁用默认插件，只保留 Enabled 中列出的 mock 插件
			},
			want: map[fwk.EventResource]fwk.ActionType{
				fwk.AssignedPod:    fwk.Add,
				fwk.UnscheduledPod: fwk.Add,
				fwk.TargetPod:      fwk.Add,
				fwk.Node:           fwk.Add,
			},
		},
		{
			name: "empty EventsToRegister plugin",
			plugins: schedulerapi.PluginSet{
				Enabled: []schedulerapi.Plugin{
					{Name: emptyEventsToRegister},
					{Name: queueSort},
					{Name: fakeBind},
				},
				Disabled: []schedulerapi.Plugin{{Name: "*"}}, // 禁用默认插件，只保留 Enabled 中列出的 mock 插件
			},
			want: map[fwk.EventResource]fwk.ActionType{},
		},
		{
			name:    "plugins with default profile (queueingHint/InPlacePodVerticalScaling: enabled)",
			plugins: schedulerapi.PluginSet{Enabled: defaults.PluginsV1.MultiPoint.Enabled},
			want: map[fwk.EventResource]fwk.ActionType{
				fwk.AssignedPod:           fwk.Add | fwk.UpdatePodLabel | fwk.UpdatePodScaleDown | fwk.Delete,
				fwk.TargetPod:             fwk.UpdatePodLabel | fwk.UpdatePodToleration | fwk.UpdatePodSchedulingGatesEliminated | fwk.UpdatePodScaleDown,
				fwk.Node:                  fwk.Add | fwk.UpdateNodeAllocatable | fwk.UpdateNodeLabel | fwk.UpdateNodeTaint | fwk.Delete,
				fwk.CSINode:               fwk.All - fwk.Delete,
				fwk.CSIDriver:             fwk.Update,
				fwk.CSIStorageCapacity:    fwk.All - fwk.Delete,
				fwk.PersistentVolume:      fwk.All - fwk.Delete,
				fwk.PersistentVolumeClaim: fwk.All - fwk.Delete,
				fwk.StorageClass:          fwk.All - fwk.Delete,
				fwk.VolumeAttachment:      fwk.Delete,
			},
			enableInPlacePodVerticalScaling: true,
		},
		{
			name:    "plugins with default profile (queueingHint/DynamicResourceAllocation: enabled)",
			plugins: schedulerapi.PluginSet{Enabled: defaults.PluginsV1.MultiPoint.Enabled},
			want: map[fwk.EventResource]fwk.ActionType{
				fwk.AssignedPod:           fwk.Add | fwk.UpdatePodLabel | fwk.Delete,
				fwk.TargetPod:             fwk.UpdatePodLabel | fwk.UpdatePodGeneratedResourceClaim | fwk.UpdatePodToleration | fwk.UpdatePodSchedulingGatesEliminated,
				fwk.Node:                  fwk.Add | fwk.UpdateNodeAllocatable | fwk.UpdateNodeLabel | fwk.UpdateNodeTaint | fwk.Delete,
				fwk.CSINode:               fwk.All - fwk.Delete,
				fwk.CSIDriver:             fwk.Update,
				fwk.CSIStorageCapacity:    fwk.All - fwk.Delete,
				fwk.PersistentVolume:      fwk.All - fwk.Delete,
				fwk.PersistentVolumeClaim: fwk.All - fwk.Delete,
				fwk.StorageClass:          fwk.All - fwk.Delete,
				fwk.VolumeAttachment:      fwk.Delete,
				fwk.DeviceClass:           fwk.All - fwk.Delete,
				fwk.ResourceClaim:         fwk.All,
				fwk.ResourceSlice:         fwk.All - fwk.Delete,
			},
			enableDynamicResourceAllocation: true,
		},
		{
			name:    "plugins with default profile",
			plugins: defaults.PluginsV1.MultiPoint,
			want: map[fwk.EventResource]fwk.ActionType{
				// NodeDeclaredFeatures 插件额外注册了 fwk.Update。
				fwk.AssignedPod: fwk.Add | fwk.UpdatePodLabel | fwk.UpdatePodScaleDown | fwk.Delete,
				fwk.TargetPod:   fwk.UpdatePodGeneratedResourceClaim | fwk.UpdatePodToleration | fwk.UpdatePodSchedulingGatesEliminated | fwk.Update,
				// NodeDeclaredFeatures 插件额外注册了 fwk.UpdateNodeDeclaredFeature。
				fwk.Node:                  fwk.Add | fwk.UpdateNodeAllocatable | fwk.UpdateNodeLabel | fwk.UpdateNodeTaint | fwk.Delete | fwk.UpdateNodeDeclaredFeature,
				fwk.CSINode:               fwk.All - fwk.Delete,
				fwk.CSIDriver:             fwk.Update,
				fwk.CSIStorageCapacity:    fwk.All - fwk.Delete,
				fwk.PersistentVolume:      fwk.All - fwk.Delete,
				fwk.PersistentVolumeClaim: fwk.All - fwk.Delete,
				fwk.StorageClass:          fwk.All - fwk.Delete,
				fwk.VolumeAttachment:      fwk.Delete,
				fwk.DeviceClass:           fwk.All - fwk.Delete,
				fwk.ResourceClaim:         fwk.All,
				fwk.ResourceSlice:         fwk.All - fwk.Delete,
			},
			enableDynamicResourceAllocation: true,
			enableInPlacePodVerticalScaling: true,
		},
		{
			name: "plugin registering to a specific pod resource 'AssignedPod'",
			plugins: schedulerapi.PluginSet{
				Enabled: []schedulerapi.Plugin{
					{Name: fakeAssignedPod},
					{Name: queueSort},
					{Name: fakeBind},
				},
				Disabled: []schedulerapi.Plugin{{Name: "*"}}, // 禁用默认插件，只保留 Enabled 中列出的 mock 插件
			},
			want: map[fwk.EventResource]fwk.ActionType{
				fwk.AssignedPod: fwk.Update,
			},
		},
		{
			name:    "plugins with default profile and GangScheduling",
			plugins: schedulerapi.PluginSet{Enabled: append(defaults.PluginsV1.MultiPoint.Enabled, schedulerapi.Plugin{Name: names.GangScheduling})},
			want: map[fwk.EventResource]fwk.ActionType{
				fwk.AssignedPod:           fwk.Add | fwk.UpdatePodLabel | fwk.UpdatePodScaleDown | fwk.Delete,
				fwk.TargetPod:             fwk.UpdatePodLabel | fwk.UpdatePodGeneratedResourceClaim | fwk.UpdatePodScaleDown | fwk.UpdatePodToleration | fwk.UpdatePodSchedulingGatesEliminated | fwk.Update,
				fwk.UnscheduledPod:        fwk.Add,
				fwk.Node:                  fwk.Add | fwk.UpdateNodeAllocatable | fwk.UpdateNodeLabel | fwk.UpdateNodeTaint | fwk.Delete | fwk.UpdateNodeDeclaredFeature,
				fwk.CSINode:               fwk.All - fwk.Delete,
				fwk.CSIDriver:             fwk.Update,
				fwk.CSIStorageCapacity:    fwk.All - fwk.Delete,
				fwk.PersistentVolume:      fwk.All - fwk.Delete,
				fwk.PersistentVolumeClaim: fwk.All - fwk.Delete,
				fwk.StorageClass:          fwk.All - fwk.Delete,
				fwk.VolumeAttachment:      fwk.Delete,
				fwk.DeviceClass:           fwk.All - fwk.Delete,
				fwk.ResourceClaim:         fwk.All,
				fwk.ResourceSlice:         fwk.All - fwk.Delete,
				fwk.PodGroup:              fwk.Add | fwk.Update,
			},
			enableGenericWorkload:           true,
			enableInPlacePodVerticalScaling: true,
			enableDynamicResourceAllocation: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pluginConfig := defaults.PluginConfigsV1

			if !tt.enableDynamicResourceAllocation {
				// 必须先设置模拟版本再设置其他 feature gate，因为它会影响 gate 之间的依赖关系校验。
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, feature.DefaultFeatureGate, version.MustParse("1.33"))
				// 1.33 中 StorageCapacityScoring 为 alpha（默认关闭）。
				// 去掉 VolumeBinding 参数里的 Shape，避免校验失败。
				// StorageCapacityScoring 关闭时 VolumeBindingArgs 的 BindTimeoutSeconds 默认值为 600。
				pluginConfig = slices.Clone(pluginConfig)
				for i := range pluginConfig {
					if pluginConfig[i].Name == "VolumeBinding" {
						pluginConfig[i].Args = &schedulerapi.VolumeBindingArgs{BindTimeoutSeconds: 600}
						break
					}
				}
			} else if !tt.enableInPlacePodVerticalScaling {
				// 原地 pod 扩缩容在 1.35 GA；未显式开启该 flag 的用例把模拟版本设为 1.34。
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, feature.DefaultFeatureGate, version.MustParse("1.34"))
				// 1.34 中 DRADeviceBindingConditions 为 alpha（默认关闭）。
				// 去掉 DynamicResources 参数里的 BindingTimeout，避免校验失败。
				// 1.34 中 StorageCapacityScoring 为 alpha（默认关闭）。
				// 去掉 VolumeBinding 参数里的 Shape，避免校验失败。
				// StorageCapacityScoring 关闭时 VolumeBindingArgs 的 BindTimeoutSeconds 默认值为 600。
				pluginConfig = slices.Clone(pluginConfig)
				for i := range pluginConfig {
					switch pluginConfig[i].Name {
					case "DynamicResources":
						pluginConfig[i].Args = &schedulerapi.DynamicResourcesArgs{}
					case "VolumeBinding":
						pluginConfig[i].Args = &schedulerapi.VolumeBindingArgs{BindTimeoutSeconds: 600}
					}
				}
			} else {
				featuregatetesting.SetFeatureGatesDuringTest(t, feature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
					features.GenericWorkload: tt.enableGenericWorkload,
				})
			}
			featuregatetesting.SetFeatureGatesDuringTest(t, feature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.InPlacePodVerticalScaling: tt.enableInPlacePodVerticalScaling,
				features.DynamicResourceAllocation: tt.enableDynamicResourceAllocation,
			})

			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			registry := plugins.NewInTreeRegistry()

			cfgPls := &schedulerapi.Plugins{MultiPoint: tt.plugins}
			plugins := []fwk.Plugin{&fakeNodePlugin{}, &fakePodPlugin{}, &fakeAssignedPodPlugin{}, &filterWithoutEnqueueExtensionsPlugin{}, &emptyEventsToRegisterPlugin{}, &fakeQueueSortPlugin{}, &fakebindPlugin{}}
			for _, pl := range plugins {
				if err := registry.Register(pl.Name(), func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
					return pl, nil
				}); err != nil {
					t.Fatalf("fail to register filter plugin (%s)", pl.Name())
				}
			}

			profile := schedulerapi.KubeSchedulerProfile{Plugins: cfgPls, PluginConfig: pluginConfig}
			fwk, err := newFramework(ctx, registry, profile)
			if err != nil {
				t.Fatal(err)
			}
			queueingHintMap, err := buildQueueingHintMap(ctx, fwk.EnqueueExtensions())
			if err != nil {
				t.Fatal(err)
			}
			queueingHintsPerProfile := internalqueue.QueueingHintMapPerProfile{
				"default": queueingHintMap,
			}
			got := unionedGVKs(queueingHintsPerProfile)

			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Unexpected eventToPlugin map (-want,+got):%s", diff)
			}
		})
	}
}

// newFramework 用给定的插件注册表与 profile 构造 Framework，
// 注入空快照、informer factory 和 PodGroupManager，供队列提示相关测试复用。
func newFramework(ctx context.Context, r frameworkruntime.Registry, profile schedulerapi.KubeSchedulerProfile) (framework.Framework, error) {
	snapshot := internalcache.NewSnapshot(nil, nil)
	return frameworkruntime.NewFramework(ctx, r, &profile,
		frameworkruntime.WithSnapshotSharedLister(snapshot),
		frameworkruntime.WithMutableSnapshotLister(snapshot),
		frameworkruntime.WithInformerFactory(informers.NewSharedInformerFactory(fake.NewClientset(), 0)),
		frameworkruntime.WithPodGroupManager(internalcache.New(ctx, nil, false, false /* 不启用 CompositePodGroup */)),
	)
}

// TestFrameworkHandler_IterateOverWaitingPods 验证 IterateOverWaitingPods：
// 无论等待中的 pod 属于同一个 profile 还是分布在多个 profile，
// 遍历各 profile 的 framework 都能拿到全部 waiting pods。
func TestFrameworkHandler_IterateOverWaitingPods(t *testing.T) {
	// 三个 profile 名，分别对应不同 schedulerName 的 pod。
	const (
		testSchedulerProfile1 = "test-scheduler-profile-1"
		testSchedulerProfile2 = "test-scheduler-profile-2"
		testSchedulerProfile3 = "test-scheduler-profile-3"
	)

	nodes := []runtime.Object{
		st.MakeNode().Name("node1").UID("node1").Obj(),
		st.MakeNode().Name("node2").UID("node2").Obj(),
		st.MakeNode().Name("node3").UID("node3").Obj(),
	}

	cases := []struct {
		name                        string
		profiles                    []schedulerapi.KubeSchedulerProfile
		waitSchedulingPods          []*v1.Pod
		expectPodNamesInWaitingPods []string
	}{
		{
			name: "pods with same profile are waiting on permit stage",
			profiles: []schedulerapi.KubeSchedulerProfile{
				{
					SchedulerName: testSchedulerProfile1,
					Plugins: &schedulerapi.Plugins{
						QueueSort: schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "PrioritySort"}}},
						Permit:    schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: fakePermit}}},
						Bind:      schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "DefaultBinder"}}},
					},
				},
			},
			waitSchedulingPods: []*v1.Pod{
				st.MakePod().Name("pod1").UID("pod1").SchedulerName(testSchedulerProfile1).Obj(),
				st.MakePod().Name("pod2").UID("pod2").SchedulerName(testSchedulerProfile1).Obj(),
				st.MakePod().Name("pod3").UID("pod3").SchedulerName(testSchedulerProfile1).Obj(),
			},
			expectPodNamesInWaitingPods: []string{"pod1", "pod2", "pod3"},
		},
		{
			name: "pods with different profiles are waiting on permit stage",
			profiles: []schedulerapi.KubeSchedulerProfile{
				{
					SchedulerName: testSchedulerProfile1,
					Plugins: &schedulerapi.Plugins{
						QueueSort: schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "PrioritySort"}}},
						Permit:    schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: fakePermit}}},
						Bind:      schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "DefaultBinder"}}},
					},
				},
				{
					SchedulerName: testSchedulerProfile2,
					Plugins: &schedulerapi.Plugins{
						QueueSort: schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "PrioritySort"}}},
						Permit:    schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: fakePermit}}},
						Bind:      schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "DefaultBinder"}}},
					},
				},
				{
					SchedulerName: testSchedulerProfile3,
					Plugins: &schedulerapi.Plugins{
						QueueSort: schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "PrioritySort"}}},
						Permit:    schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: fakePermit}}},
						Bind:      schedulerapi.PluginSet{Enabled: []schedulerapi.Plugin{{Name: "DefaultBinder"}}},
					},
				},
			},
			waitSchedulingPods: []*v1.Pod{
				st.MakePod().Name("pod1").UID("pod1").SchedulerName(testSchedulerProfile1).Obj(),
				st.MakePod().Name("pod2").UID("pod2").SchedulerName(testSchedulerProfile1).Obj(),
				st.MakePod().Name("pod3").UID("pod3").SchedulerName(testSchedulerProfile2).Obj(),
				st.MakePod().Name("pod4").UID("pod4").SchedulerName(testSchedulerProfile3).Obj(),
			},
			expectPodNamesInWaitingPods: []string{"pod1", "pod2", "pod3", "pod4"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// 为 3 个节点准备调度器：额外放一个空 Namespace，
			// 避免 store 中缺少默认命名空间导致事件记录失败。
			objs := append([]runtime.Object{&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ""}}}, nodes...)
			fakeClient := fake.NewClientset(objs...)
			informerFactory := informers.NewSharedInformerFactory(fakeClient, 0)
			eventBroadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: fakeClient.EventsV1()})
			defer eventBroadcaster.Shutdown()

			eventRecorder := eventBroadcaster.NewRecorder(scheme.Scheme, fakePermit)

			outOfTreeRegistry := frameworkruntime.Registry{
				fakePermit: newFakePermitPlugin(eventRecorder),
			}

			tCtx := utiltesting.Init(t)

			scheduler, err := New(
				tCtx,
				fakeClient,
				informerFactory,
				nil,
				profile.NewRecorderFactory(eventBroadcaster),
				WithProfiles(tc.profiles...),
				WithFrameworkOutOfTreeRegistry(outOfTreeRegistry),
			)

			if err != nil {
				t.Fatalf("Failed to create scheduler: %v", err)
			}

			var wg sync.WaitGroup
			waitSchedulingPodNumber := len(tc.waitSchedulingPods)
			wg.Add(waitSchedulingPodNumber)
			// 用 fakePermitPlugin 发出的 "podWaiting" 事件作为同步点：
			// 收到每个 pod 的事件即表示它已进入 permit 阶段。
			stopFn, err := eventBroadcaster.StartEventWatcher(func(obj runtime.Object) {
				e, ok := obj.(*eventsv1.Event)
				if !ok || (e.Reason != podWaitingReason) {
					return
				}
				wg.Done()
			})
			if err != nil {
				t.Fatal(err)
			}
			defer stopFn()

			// 启动 informer 并等待缓存同步后运行调度器，确保创建 pod 时事件能被及时观察。
			informerFactory.Start(tCtx.Done())
			informerFactory.WaitForCacheSync(tCtx.Done())
			go scheduler.Run(tCtx)

			// 创建待调度的 pod，触发调度周期。
			for _, p := range tc.waitSchedulingPods {
				_, err = fakeClient.CoreV1().Pods("").Create(tCtx, p, metav1.CreateOptions{})
				if err != nil {
					t.Fatal(err)
				}
			}

			// 等到所有 pod 都进入 permit 阶段并发出等待事件。
			wg.Wait()

			tCtx.Eventually(func(utiltesting.TContext) sets.Set[string] {
				// 跨所有 profile 遍历 waiting pods，确认无论 pod 归属哪个 profile 都不会漏。
				actualPodNamesInWaitingPods := sets.New[string]()
				for _, schedFramework := range scheduler.Profiles {
					schedFramework.IterateOverWaitingPods(func(pod fwk.WaitingPod) {
						actualPodNamesInWaitingPods.Insert(pod.GetPod().Name)
					})
				}
				return actualPodNamesInWaitingPods
			}).WithTimeout(permitTimeout).Should(gomega.Equal(sets.New(tc.expectPodNamesInWaitingPods...)), "unexpected waitingPods in scheduler profile")
		})
	}
}

// 编译期断言：确保 fakeQueueSortPlugin 满足 QueueSortPlugin 接口。
var _ fwk.QueueSortPlugin = &fakeQueueSortPlugin{}

// fakeQueueSortPlugin 是 QueueSort 扩展点的空实现，仅用于让框架能构造成功。
type fakeQueueSortPlugin struct{}

// Name 返回插件名 queueSort。
func (pl *fakeQueueSortPlugin) Name() string {
	return queueSort
}

// Less 恒为 false，测试不关心队列顺序。
func (pl *fakeQueueSortPlugin) Less(_, _ fwk.QueuedEntityInfo) bool {
	return false
}

// 编译期断言：确保 fakebindPlugin 满足 BindPlugin 接口。
var _ fwk.BindPlugin = &fakebindPlugin{}

// fakebindPlugin 是 Bind 扩展点的空实现，避免测试真的去绑定节点。
type fakebindPlugin struct{}

// Name 返回插件名 fakeBind。
func (t *fakebindPlugin) Name() string {
	return fakeBind
}

// Bind 空实现，直接返回 nil 表示成功。
func (t *fakebindPlugin) Bind(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodeName string) *fwk.Status {
	return nil
}

// filterWithoutEnqueueExtensionsPlugin 实现了 Filter，但未实现 EnqueueExtensions，
// 用于测试 buildQueueingHintMap 对这类插件回退到默认提示的行为。
type filterWithoutEnqueueExtensionsPlugin struct{}

// Name 返回插件名 filterWithoutEnqueueExtensions。
func (*filterWithoutEnqueueExtensionsPlugin) Name() string { return filterWithoutEnqueueExtensions }

// Filter 空实现，恒返回 nil。
func (*filterWithoutEnqueueExtensionsPlugin) Filter(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ fwk.NodeInfo) *fwk.Status {
	return nil
}

// 各 fake 插件返回不同的 hint 值，便于在断言中区分是哪一个插件的函数被调用。
var hintFromFakeNode = fwk.QueueingHint(100)

// fakeNodePlugin 注册 Node/Add 事件，用于验证按资源类型聚合提示。
type fakeNodePlugin struct{}

// fakeNodePluginQueueingFn 返回固定的 hintFromFakeNode。
var fakeNodePluginQueueingFn = func(_ klog.Logger, _ *v1.Pod, _, _ interface{}) (fwk.QueueingHint, error) {
	return hintFromFakeNode, nil
}

// Name 返回插件名 fakeNode。
func (*fakeNodePlugin) Name() string { return fakeNode }

// Filter 空实现，恒返回 nil。
func (*fakeNodePlugin) Filter(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ fwk.NodeInfo) *fwk.Status {
	return nil
}

// EventsToRegister 注册 Node/Add 事件及其提示函数。
func (pl *fakeNodePlugin) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add}, QueueingHintFn: fakeNodePluginQueueingFn},
	}, nil
}

// hintFromFakePod 是 fakePodPlugin 返回的固定提示值。
var hintFromFakePod = fwk.QueueingHint(101)

// fakePodPlugin 注册 Pod/Add 事件，用于验证 pod 相关事件的聚合。
type fakePodPlugin struct{}

// fakePodPluginQueueingFn 返回固定的 hintFromFakePod。
var fakePodPluginQueueingFn = func(_ klog.Logger, _ *v1.Pod, _, _ interface{}) (fwk.QueueingHint, error) {
	return hintFromFakePod, nil
}

// Name 返回插件名 fakePod。
func (*fakePodPlugin) Name() string { return fakePod }

// Filter 空实现，恒返回 nil。
func (*fakePodPlugin) Filter(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ fwk.NodeInfo) *fwk.Status {
	return nil
}

// EventsToRegister 注册 Pod/Add 事件及其提示函数。
func (pl *fakePodPlugin) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: fwk.Pod, ActionType: fwk.Add}, QueueingHintFn: fakePodPluginQueueingFn},
	}, nil
}

// hintFromFakeAssignedPod 是 fakeAssignedPodPlugin 返回的固定提示值。
var hintFromFakeAssignedPod = fwk.QueueingHint(102)

// fakeAssignedPodPlugin 只注册 AssignedPod/Update 事件，
// 用于验证插件可精确注册到某一种 pod 资源。
type fakeAssignedPodPlugin struct{}

// fakeAssignedPodPluginQueueingFn 返回固定的 hintFromFakeAssignedPod。
var fakeAssignedPodPluginQueueingFn = func(_ klog.Logger, _ *v1.Pod, _, _ interface{}) (fwk.QueueingHint, error) {
	return hintFromFakeAssignedPod, nil
}

// Name 返回插件名 fakeAssignedPod。
func (*fakeAssignedPodPlugin) Name() string { return fakeAssignedPod }

// Filter 空实现，恒返回 nil。
func (*fakeAssignedPodPlugin) Filter(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ fwk.NodeInfo) *fwk.Status {
	return nil
}

// EventsToRegister 注册 AssignedPod/Update 事件及其提示函数。
func (pl *fakeAssignedPodPlugin) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.Update}, QueueingHintFn: fakeAssignedPodPluginQueueingFn},
	}, nil
}

// emptyEventPlugin 实现了 EnqueueExtensions，但不注册任何事件，用于验证空注册场景。
type emptyEventPlugin struct{}

// Name 返回插件名 emptyEventExtensions。
func (*emptyEventPlugin) Name() string { return emptyEventExtensions }

// Filter 空实现，恒返回 nil。
func (*emptyEventPlugin) Filter(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ fwk.NodeInfo) *fwk.Status {
	return nil
}

// EventsToRegister 返回 nil，表示不注册任何事件。
func (pl *emptyEventPlugin) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return nil, nil
}

// errorEventsToRegisterPlugin 在 EventsToRegister 中返回错误，
// 用于验证错误会沿 buildQueueingHintMap 向上传播。
type errorEventsToRegisterPlugin struct{}

// Name 返回插件名 errorEventsToRegister。
func (*errorEventsToRegisterPlugin) Name() string { return errorEventsToRegister }

// Filter 空实现，恒返回 nil。
func (*errorEventsToRegisterPlugin) Filter(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ fwk.NodeInfo) *fwk.Status {
	return nil
}

// EventsToRegister 返回 "mock error"，模拟插件注册事件失败。
func (*errorEventsToRegisterPlugin) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return nil, errors.New("mock error")
}

// emptyEventsToRegisterPlugin 实现了 fwk.EnqueueExtensions，但 EventsToRegister 返回 nil，
// 用于模拟插件虽在调度器启动时注册、却因某个 feature gate 关闭而不做任何事的情形。
type emptyEventsToRegisterPlugin struct{}

// Name 返回插件名 emptyEventsToRegister。
func (*emptyEventsToRegisterPlugin) Name() string { return emptyEventsToRegister }

// Filter 空实现，恒返回 nil。
func (*emptyEventsToRegisterPlugin) Filter(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ fwk.NodeInfo) *fwk.Status {
	return nil
}

// EventsToRegister 返回 nil，表示不注册任何事件。
func (*emptyEventsToRegisterPlugin) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return nil, nil
}

// fakePermitPlugin 只实现 PermitPlugin 接口：让 pod 在 permit 阶段等待，
// 并发出事件通知测试同步。
type fakePermitPlugin struct {
	eventRecorder events.EventRecorderLogger
}

// newFakePermitPlugin 返回插件工厂，把事件记录器注入 fakePermitPlugin。
func newFakePermitPlugin(eventRecorder events.EventRecorderLogger) frameworkruntime.PluginFactory {
	return func(ctx context.Context, configuration runtime.Object, f fwk.Handle) (fwk.Plugin, error) {
		pl := &fakePermitPlugin{
			eventRecorder: eventRecorder,
		}
		return pl, nil
	}
}

// Name 返回插件名 fakePermit。
func (f fakePermitPlugin) Name() string {
	return fakePermit
}

// 事件原因与等待超时常量；测试用事件作为 pod 进入 permit 阶段的同步信号。
const (
	podWaitingReason = "podWaiting"
	permitTimeout    = 10 * time.Second
)

// Permit 让所有 pod 等待 permitTimeout，并在返回前发出 "podWaiting" 事件，
// 告知测试该 pod 已进入 permit 阶段。
func (f fakePermitPlugin) Permit(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodeName string) (*fwk.Status, time.Duration) {
	defer func() {
		// 发出 podWaiting 事件，广播该 pod 正在 permit 阶段等待。
		f.eventRecorder.WithLogger(klog.FromContext(ctx)).Eventf(p, nil, v1.EventTypeWarning, podWaitingReason, "", "")
	}()

	return fwk.NewStatus(fwk.Wait), permitTimeout
}

// 编译期断言：确保 fakePermitPlugin 满足 PermitPlugin 接口。
var _ fwk.PermitPlugin = &fakePermitPlugin{}

// TestNewInformerFactoryTrim 验证 NewInformerFactory 注册的 transform：
// 从 API 读取 pod 时会裁掉 managedFields，减少调度器内存占用。
func TestNewInformerFactoryTrim(t *testing.T) {
	cs := fake.NewClientset()

	// 构造一个带 managedFields 的 pod，供 transform 裁剪。
	pd := &v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "test",
		Namespace: "default",
		ManagedFields: []metav1.ManagedFieldsEntry{
			{
				Manager:    "update",
				Operation:  metav1.ManagedFieldsOperationApply,
				APIVersion: "apps/v1",
			},
		},
	}}
	require.NoError(t, cs.Tracker().Add(pd))

	informerFactory := NewInformerFactory(cs, 0, nil)
	lister := informerFactory.Core().V1().Pods().Lister()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	informerFactory.Start(ctx.Done())
	informerFactory.WaitForCacheSync(ctx.Done())

	p, err := lister.Pods("default").Get("test")
	require.NoError(t, err)
	require.Empty(t, p.GetManagedFields(), "expected managedFields to be trimmed by the transform")
}

// TestNewInformerFactoryMetrics 验证 informer 工厂把身份标识（kube-scheduler）
// 传递给各 informer，使队列指标带上正确的 name 标签。
func TestNewInformerFactoryMetrics(t *testing.T) {
	// 重置 informer 名称注册表，避免测试间相互干扰。
	cache.ResetInformerNamesForTesting()
	fifometrics.Register()

	informerName, err := cache.NewInformerName("kube-scheduler")
	require.NoError(t, err)
	defer informerName.Release()

	cs := fake.NewClientset()
	informerFactory := NewInformerFactory(cs, 0, informerName)
	// pod informer 由调度器自身创建，而非工厂默认创建，
	// 这里显式构建它，确认同样带有身份标识。
	informerFactory.Core().V1().Pods().Informer()
	informerFactory.Core().V1().Nodes().Informer()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	informerFactory.Start(ctx.Done())
	informerFactory.WaitForCacheSync(ctx.Done())

	want := `
# HELP informer_queued_items [ALPHA] Number of items currently queued in the FIFO.
# TYPE informer_queued_items gauge
informer_queued_items{group="",name="kube-scheduler",resource="nodes",version="v1"} 0
informer_queued_items{group="",name="kube-scheduler",resource="pods",version="v1"} 0
`
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(want), "informer_queued_items"); err != nil {
		t.Error(err)
	}
}
