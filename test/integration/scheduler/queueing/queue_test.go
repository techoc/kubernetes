/*
Copyright 2021 The Kubernetes Authors.

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

package queueing

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	configv1 "k8s.io/kube-scheduler/config/v1"
	fwk "k8s.io/kube-scheduler/framework"
	apiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/pkg/scheduler"
	configtesting "k8s.io/kubernetes/pkg/scheduler/apis/config/testing"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	testfwk "k8s.io/kubernetes/test/integration/framework"
	testutils "k8s.io/kubernetes/test/integration/util"
	imageutils "k8s.io/kubernetes/test/utils/image"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/ptr"
)

func TestSchedulingGates(t *testing.T) {
	tests := []struct {
		name     string
		pods     []*v1.Pod
		schedule []string
		delete   []string
		rmGates  []string
	}{
		{
			name: "regular pods",
			pods: []*v1.Pod{
				st.MakePod().Name("p1").Container("pause").Obj(),
				st.MakePod().Name("p2").Container("pause").Obj(),
			},
			schedule: []string{"p1", "p2"},
		},
		{
			name: "one pod carrying scheduling gates",
			pods: []*v1.Pod{
				st.MakePod().Name("p1").SchedulingGates([]string{"foo"}).Container("pause").Obj(),
				st.MakePod().Name("p2").Container("pause").Obj(),
			},
			schedule: []string{"p2"},
		},
		{
			name: "two pod carrying scheduling gates, and remove gates of one pod",
			pods: []*v1.Pod{
				st.MakePod().Name("p1").SchedulingGates([]string{"foo"}).Container("pause").Obj(),
				st.MakePod().Name("p2").SchedulingGates([]string{"bar"}).Container("pause").Obj(),
				st.MakePod().Name("p3").Container("pause").Obj(),
			},
			schedule: []string{"p3"},
			rmGates:  []string{"p2"},
		},
		{
			name: "gated pod schedulable after deleting the scheduled pod and removing gate",
			pods: []*v1.Pod{
				st.MakePod().Name("p1").SchedulingGates([]string{"foo"}).Container("pause").Obj(),
				st.MakePod().Name("p2").Container("pause").Obj(),
			},
			schedule: []string{"p2"},
			delete:   []string{"p2"},
			rmGates:  []string{"p1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 使用零退避秒数以绕过 backoffQ。
			// 目的是不启动调度器的队列，从而不启动任何 flush 逻辑。稍后我们将手动弹出并调度 Pod。
			testCtx := testutils.InitTestSchedulerWithOptions(
				t,
				testutils.InitTestAPIServer(t, "pod-scheduling-gates", nil),
				0,
				scheduler.WithPodInitialBackoffSeconds(0),
				scheduler.WithPodMaxBackoffSeconds(0),
			)
			testutils.SyncSchedulerInformerFactory(testCtx)

			if testCtx.Scheduler.APIDispatcher != nil {
				logger, _ := ktesting.NewTestContext(t)
				testCtx.Scheduler.APIDispatcher.Run(logger)
				defer testCtx.Scheduler.APIDispatcher.Close()
			}

			cs, ns, ctx := testCtx.ClientSet, testCtx.NS.Name, testCtx.Ctx

			// 创建节点，以便调度 Pod。
			node := st.MakeNode().Name("node").Obj()
			if _, err := cs.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
				t.Fatal("Failed to create node")

			}

			// 创建 Pod。
			for _, p := range tt.pods {
				p.Namespace = ns
				if _, err := cs.CoreV1().Pods(ns).Create(ctx, p, metav1.CreateOptions{}); err != nil {
					t.Fatalf("Failed to create Pod %q: %v", p.Name, err)
				}
			}

			// 等待 Pod 出现在调度队列中。
			if err := wait.PollUntilContextTimeout(ctx, time.Millisecond*200, wait.ForeverTestTimeout, false, func(ctx context.Context) (bool, error) {
				pendingPods, _ := testCtx.Scheduler.SchedulingQueue.PendingPods()
				return len(pendingPods) == len(tt.pods), nil
			}); err != nil {
				t.Fatal(err)
			}

			// 调度 Pod。
			for _, podName := range tt.schedule {
				testCtx.Scheduler.ScheduleOne(testCtx.Ctx)
				if err := wait.PollUntilContextTimeout(ctx, time.Millisecond*200, wait.ForeverTestTimeout, false, testutils.PodScheduled(cs, ns, podName)); err != nil {
					t.Fatalf("Failed to schedule %s", podName)
				}
			}

			// 删除 Pod，这会在调度队列中触发 AssignedPodDelete 事件。
			for _, podName := range tt.delete {
				if err := cs.CoreV1().Pods(ns).Delete(ctx, podName, metav1.DeleteOptions{}); err != nil {
					t.Fatalf("Error calling Delete on %s", podName)
				}
				if err := wait.PollUntilContextTimeout(ctx, time.Millisecond*200, wait.ForeverTestTimeout, false, testutils.PodDeleted(ctx, cs, ns, podName)); err != nil {
					t.Fatalf("Failed to delete %s", podName)
				}
			}

			// 确保带有 gate 的 Pod 不在 ActiveQ 中
			if len(testCtx.Scheduler.SchedulingQueue.PodsInActiveQ()) > 0 {
				t.Fatal("Expected no schedulable pods")
			}

			// 从 Pod spec 中移除调度 gate。
			for _, podName := range tt.rmGates {
				patch := `{"spec": {"schedulingGates": null}}`
				if _, err := cs.CoreV1().Pods(ns).Patch(ctx, podName, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
					t.Fatalf("Failed to patch pod %v: %v", podName, err)
				}
			}

			// 调度已不再带有 gate 的 Pod。
			for _, podName := range tt.rmGates {
				testCtx.Scheduler.ScheduleOne(testCtx.Ctx)
				if err := wait.PollUntilContextTimeout(ctx, time.Millisecond*200, wait.ForeverTestTimeout, false, testutils.PodScheduled(cs, ns, podName)); err != nil {
					t.Fatalf("Failed to schedule %s", podName)
				}
			}
		})
	}
}

var _ fwk.FilterPlugin = &fakeCRPlugin{}
var _ fwk.EnqueueExtensions = &fakeCRPlugin{}

type fakeCRPlugin struct{}

func (f *fakeCRPlugin) Name() string {
	return "fakeCRPlugin"
}

func (f *fakeCRPlugin) Filter(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ fwk.NodeInfo) *fwk.Status {
	return fwk.NewStatus(fwk.Unschedulable, "always fail")
}

// EventsToRegister 返回可能使被该插件判定为失败的 Pod
// 变为可调度的事件。
func (f *fakeCRPlugin) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: "foos.v1.example.com", ActionType: fwk.All}},
	}, nil
}

// TestCustomResourceEnqueue 构造一个注册自定义资源的假插件，
// 以验证被该插件判定为失败的 Pod 能在 CR 事件发生时被正确移动。
func TestCustomResourceEnqueue(t *testing.T) {
	// 启动支持 apiextensions 的 API Server。
	server := apiservertesting.StartTestServerOrDie(
		t, apiservertesting.NewDefaultTestServerOptions(),
		[]string{"--disable-admission-plugins=ServiceAccount,TaintNodesByCondition", "--runtime-config=api/all=true"},
		testfwk.SharedEtcd(),
	)
	testCtx := &testutils.TestContext{}
	ctx, cancel := context.WithCancel(context.Background())
	testCtx.Ctx = ctx
	testCtx.CloseFn = func() {
		cancel()
		server.TearDownFn()
	}

	apiExtensionClient := apiextensionsclient.NewForConfigOrDie(server.ClientConfig)
	dynamicClient := dynamic.NewForConfigOrDie(server.ClientConfig)

	// 创建一个 Foo CRD。
	fooCRD := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foos.example.com",
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "example.com",
			Scope: apiextensionsv1.NamespaceScoped,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural: "foos",
				Kind:   "Foo",
			},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name:    "v1",
					Served:  true,
					Storage: true,
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
							Type: "object",
							Properties: map[string]apiextensionsv1.JSONSchemaProps{
								"field": {Type: "string"},
							},
						},
					},
				},
			},
		},
	}
	var err error
	fooCRD, err = apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Create(testCtx.Ctx, fooCRD, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	registry := frameworkruntime.Registry{
		"fakeCRPlugin": func(_ context.Context, _ runtime.Object, fh fwk.Handle) (fwk.Plugin, error) {
			return &fakeCRPlugin{}, nil
		},
	}
	cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
		Profiles: []configv1.KubeSchedulerProfile{{
			SchedulerName: ptr.To(v1.DefaultSchedulerName),
			Plugins: &configv1.Plugins{
				Filter: configv1.PluginSet{
					Enabled: []configv1.Plugin{
						{Name: "fakeCRPlugin"},
					},
				},
			},
		}}})

	testCtx.KubeConfig = server.ClientConfig
	testCtx.ClientSet = kubernetes.NewForConfigOrDie(server.ClientConfig)
	testCtx.NS, err = testCtx.ClientSet.CoreV1().Namespaces().Create(testCtx.Ctx, &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("cr-enqueue-%v", string(uuid.NewUUID()))}}, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		t.Fatalf("Failed to integration test ns: %v", err)
	}

	// 使用零退避秒数以绕过 backoffQ。
	// 目的是不启动调度器的队列，从而不启动任何 flush 逻辑。稍后我们将手动弹出并调度 Pod。
	testCtx = testutils.InitTestSchedulerWithOptions(
		t,
		testCtx,
		0,
		scheduler.WithProfiles(cfg.Profiles...),
		scheduler.WithFrameworkOutOfTreeRegistry(registry),
		scheduler.WithPodInitialBackoffSeconds(0),
		scheduler.WithPodMaxBackoffSeconds(0),
	)
	testutils.SyncSchedulerInformerFactory(testCtx)

	defer testutils.CleanupTest(t, testCtx)

	cs, ns, ctx := testCtx.ClientSet, testCtx.NS.Name, testCtx.Ctx
	// 创建一个 Node。
	node := st.MakeNode().Name("fake-node").Obj()
	if _, err := cs.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create Node %q: %v", node.Name, err)
	}

	// 创建一个测试 Pod。
	pause := imageutils.GetPauseImageName()
	pod := st.MakePod().Namespace(ns).Name("fake-pod").Container(pause).Obj()
	if _, err := cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create Pod %q: %v", pod.Name, err)
	}

	// 等待测试 Pod 出现在调度队列中。
	if err := wait.PollUntilContextTimeout(ctx, time.Millisecond*200, wait.ForeverTestTimeout, false, func(ctx context.Context) (bool, error) {
		pendingPods, _ := testCtx.Scheduler.SchedulingQueue.PendingPods()
		return len(pendingPods) == 1, nil
	}); err != nil {
		t.Fatal(err)
	}

	// 弹出 fake-pod。它应是不可调度的。
	entity := testutils.NextEntityOrDie(t, testCtx)
	podInfo := entity.(*framework.QueuedPodInfo)
	schedFramework, ok := testCtx.Scheduler.Profiles[podInfo.Pod.Spec.SchedulerName]
	if !ok {
		t.Fatalf("Cannot find the profile for Pod %v", podInfo.Pod.Name)
	}
	// 手动调度该 Pod。
	_, fitError := testCtx.Scheduler.SchedulePod(ctx, schedFramework, framework.NewCycleState(), podInfo)
	// fitError 预期非 nil，因为它在 fakeCRPlugin 插件处失败。
	if fitError == nil {
		t.Fatalf("Expect Pod %v to fail at scheduling.", podInfo.Pod.Name)
	}
	testCtx.Scheduler.FailureHandler(ctx, schedFramework, podInfo, fwk.NewStatus(fwk.Unschedulable).WithError(fitError), nil, time.Now())

	// 触发 Custom Resource 事件。
	// 期望该事件触发将测试 Pod 从 unschedulableEntities 移到 activeQ。
	crdGVR := schema.GroupVersionResource{Group: fooCRD.Spec.Group, Version: fooCRD.Spec.Versions[0].Name, Resource: "foos"}
	crClient := dynamicClient.Resource(crdGVR).Namespace(ns)
	if _, err := crClient.Create(ctx, &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "example.com/v1",
			"kind":       "Foo",
			"metadata":   map[string]interface{}{"name": "foo1"},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Unable to create cr: %v", err)
	}

	// 现在我们应该能够再次从 activeQ 中弹出该 Pod。
	entity = testutils.NextEntityOrDie(t, testCtx)
	if attempts := entity.GetAttempts(); attempts != 2 {
		t.Errorf("Expected the Pod to be attempted 2 times, but got %v", attempts)
	}
}

// TestRequeueByBindFailure 验证因绑定插件而失败的 Pod
// 无论是否发生事件都会被放回队列。
func TestRequeueByBindFailure(t *testing.T) {
	fakeBind := &firstFailBindPlugin{}
	registry := frameworkruntime.Registry{
		"firstFailBindPlugin": func(ctx context.Context, o runtime.Object, fh fwk.Handle) (fwk.Plugin, error) {
			binder, err := defaultbinder.New(ctx, nil, fh)
			if err != nil {
				return nil, err
			}

			fakeBind.defaultBinderPlugin = binder.(fwk.BindPlugin)
			return fakeBind, nil
		},
	}

	cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
		Profiles: []configv1.KubeSchedulerProfile{{
			SchedulerName: ptr.To(v1.DefaultSchedulerName),
			Plugins: &configv1.Plugins{
				MultiPoint: configv1.PluginSet{
					Enabled: []configv1.Plugin{
						{Name: "firstFailBindPlugin"},
					},
					Disabled: []configv1.Plugin{
						{Name: names.DefaultBinder},
					},
				},
			},
		}}})

	// 使用零退避秒数以绕过 backoffQ。
	testCtx := testutils.InitTestSchedulerWithOptions(
		t,
		testutils.InitTestAPIServer(t, "core-res-enqueue", nil),
		0,
		scheduler.WithPodInitialBackoffSeconds(0),
		scheduler.WithPodMaxBackoffSeconds(0),
		scheduler.WithProfiles(cfg.Profiles...),
		scheduler.WithFrameworkOutOfTreeRegistry(registry),
	)
	testutils.SyncSchedulerInformerFactory(testCtx)

	go testCtx.Scheduler.Run(testCtx.Ctx)

	cs, ns, ctx := testCtx.ClientSet, testCtx.NS.Name, testCtx.Ctx
	node := st.MakeNode().Name("fake-node").Obj()
	if _, err := cs.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create Node %q: %v", node.Name, err)
	}
	// 创建一个 Pod。
	pod := st.MakePod().Namespace(ns).Name("pod-1").Container(imageutils.GetPauseImageName()).Obj()
	if _, err := cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create Pod %q: %v", pod.Name, err)
	}

	// 1. 第一次绑定尝试应失败。
	// 2. Pod 应在没有任何事件的情况下被重新入队到 activeQ/backoffQ。
	// 3. Pod 应在第二次绑定尝试时被调度。
	// 这里等待直到 (3) 发生。
	err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, wait.ForeverTestTimeout, false, testutils.PodScheduled(cs, ns, pod.Name))
	if err != nil {
		t.Fatalf("Expect pod-1 to be scheduled by the bind plugin: %v", err)
	}

	// 确保第一次绑定尝试失败，且该 Pod 在第二次尝试时被调度。
	if fakeBind.counter != 1 {
		t.Fatalf("Expect pod-1 to be scheduled by the bind plugin in the second binding try: %v", err)
	}
}

// firstFailBindPlugin 在第一次 Bind 调用时拒绝 Pod。
type firstFailBindPlugin struct {
	counter             int
	defaultBinderPlugin fwk.BindPlugin
}

func (*firstFailBindPlugin) Name() string {
	return "firstFailBindPlugin"
}

func (p *firstFailBindPlugin) Bind(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodename string) *fwk.Status {
	if p.counter == 0 {
		// 在第一次 Bind 调用时失败。
		p.counter++
		return fwk.NewStatus(fwk.Error, "firstFailBindPlugin rejects the Pod")
	}

	return p.defaultBinderPlugin.Bind(ctx, state, pod, nodename)
}

// TestRequeueByPermitRejection 验证在绑定周期中因 permit 插件而失败的 Pod
// 会根据正确的调度周期编号被放回队列。
func TestRequeueByPermitRejection(t *testing.T) {
	queueingHintCalledCounter := 0
	fakePermit := &fakePermitPlugin{}
	registry := frameworkruntime.Registry{
		fakePermitPluginName: func(ctx context.Context, o runtime.Object, fh fwk.Handle) (fwk.Plugin, error) {
			fakePermit = &fakePermitPlugin{
				frameworkHandler: fh,
				schedulingHint: func(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
					queueingHintCalledCounter++
					return fwk.Queue, nil
				},
			}
			return fakePermit, nil
		},
	}
	cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
		Profiles: []configv1.KubeSchedulerProfile{{
			SchedulerName: ptr.To(v1.DefaultSchedulerName),
			Plugins: &configv1.Plugins{
				MultiPoint: configv1.PluginSet{
					Enabled: []configv1.Plugin{
						{Name: fakePermitPluginName},
					},
				},
			},
		}}})

	// 使用零退避秒数以绕过 backoffQ。
	testCtx := testutils.InitTestSchedulerWithOptions(
		t,
		testutils.InitTestAPIServer(t, "core-res-enqueue", nil),
		0,
		scheduler.WithPodInitialBackoffSeconds(0),
		scheduler.WithPodMaxBackoffSeconds(0),
		scheduler.WithProfiles(cfg.Profiles...),
		scheduler.WithFrameworkOutOfTreeRegistry(registry),
	)
	testutils.SyncSchedulerInformerFactory(testCtx)

	go testCtx.Scheduler.Run(testCtx.Ctx)

	cs, ns, ctx := testCtx.ClientSet, testCtx.NS.Name, testCtx.Ctx
	node := st.MakeNode().Name("fake-node").Obj()
	if _, err := cs.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create Node %q: %v", node.Name, err)
	}
	// 创建一个 Pod。
	pod := st.MakePod().Namespace(ns).Name("pod-1").Container(imageutils.GetPauseImageName()).Obj()
	if _, err := cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create Pod %q: %v", pod.Name, err)
	}

	// 更新节点标签（引发 NodeUpdate 事件）
	node.Labels = map[string]string{"updated": ""}
	if _, err := cs.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Failed to add labels to the node: %v", err)
	}

	// 创建一个 Pod，以增加调度队列中的调度周期编号。
	// 这样可以确保在上一个调度周期发生的 NodeUpdate 事件能通过调度队列使 Pod 被重新入队到 activeQ。
	pod = st.MakePod().Namespace(ns).Name("pod-2").Container(imageutils.GetPauseImageName()).Obj()
	if _, err := cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create Pod %q: %v", pod.Name, err)
	}

	// 拒绝 pod-1 以模拟 Permit 插件中的失败。
	// 由于 NodeUpdate 事件已经发生，pod-1 应被重新入队到 activeQ。
	fakePermit.frameworkHandler.IterateOverWaitingPods(func(wp fwk.WaitingPod) {
		if wp.GetPod().Name == "pod-1" {
			wp.Reject(fakePermitPluginName, "fakePermitPlugin rejects the Pod")
			return
		}
	})

	// 等待 pod-2 被调度。
	err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, wait.ForeverTestTimeout, false, func(ctx context.Context) (done bool, err error) {
		fakePermit.frameworkHandler.IterateOverWaitingPods(func(wp fwk.WaitingPod) {
			if wp.GetPod().Name == "pod-2" {
				wp.Allow(fakePermitPluginName)
			}
		})

		return testutils.PodScheduled(cs, ns, "pod-2")(ctx)
	})
	if err != nil {
		t.Fatalf("Expect pod-2 to be scheduled")
	}

	err = wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, wait.ForeverTestTimeout, false, func(ctx context.Context) (done bool, err error) {
		pod1Found := false
		fakePermit.frameworkHandler.IterateOverWaitingPods(func(wp fwk.WaitingPod) {
			if wp.GetPod().Name == "pod-1" {
				pod1Found = true
				wp.Allow(fakePermitPluginName)
			}
		})
		return pod1Found, nil
	})
	if err != nil {
		t.Fatal("Expect pod-1 to be scheduled again")
	}

	if queueingHintCalledCounter != 1 {
		t.Fatalf("Expected the scheduling hint to be called 1 time, but %v", queueingHintCalledCounter)
	}
}

type fakePermitPlugin struct {
	frameworkHandler fwk.Handle
	schedulingHint   fwk.QueueingHintFn
}

const fakePermitPluginName = "fakePermitPlugin"

func (p *fakePermitPlugin) Name() string {
	return fakePermitPluginName
}

func (p *fakePermitPlugin) Permit(ctx context.Context, state fwk.CycleState, _ *v1.Pod, _ string) (*fwk.Status, time.Duration) {
	return fwk.NewStatus(fwk.Wait), wait.ForeverTestTimeout
}

func (p *fakePermitPlugin) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.UpdateNodeLabel}, QueueingHintFn: p.schedulingHint},
	}, nil
}

func TestPopFromBackoffQWhenActiveQEmpty(t *testing.T) {
	// 将初始退避设为 1000 秒，以确保 Pod 重新入队后不会进入 activeQ。
	testCtx := testutils.InitTestSchedulerWithNS(t, "pop-from-backoffq", scheduler.WithPodInitialBackoffSeconds(1000), scheduler.WithPodMaxBackoffSeconds(1000))
	cs, ns, ctx := testCtx.ClientSet, testCtx.NS.Name, testCtx.Ctx

	// 创建节点，以便调度 Pod。
	node := st.MakeNode().Name("node").Obj()
	if _, err := cs.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
		t.Fatal("Failed to create node")
	}

	// 创建一个不可调度的 Pod。
	pod := st.MakePod().Namespace(ns).Name("pod").NodeAffinityIn("foo", []string{"bar"}, st.NodeSelectorTypeMatchExpressions).Container(imageutils.GetPauseImageName()).Obj()
	if _, err := cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, wait.ForeverTestTimeout, false, testutils.PodUnschedulable(cs, ns, pod.Name))
	if err != nil {
		t.Fatalf("Expected pod to be unschedulable: %v", err)
	}

	// 创建带有标签的节点，使 Pod 可调度。
	node2 := st.MakeNode().Name("node-schedulable").Label("foo", "bar").Obj()
	if _, err := cs.CoreV1().Nodes().Create(ctx, node2, metav1.CreateOptions{}); err != nil {
		t.Fatal("Failed to create node-schedulable")
	}

	// 即使 Pod 在 backoffQ 中，也应被调度，因为 PopFromBackoffQ 特性已启用。
	err = wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, wait.ForeverTestTimeout, false, testutils.PodScheduled(cs, ns, pod.Name))
	if err != nil {
		t.Fatalf("Expected pod to be scheduled: %v", err)
	}
}

// TestCoreResourceEnqueue 验证因内置默认插件而失败的 Pod
// 能在其注册的事件发生时被正确移动。
func TestCoreResourceEnqueue(t *testing.T) {
	for _, tt := range CoreResourceEnqueueTestCases {
		t.Run(strings.Join(append(tt.EnablePlugins, tt.Name), "/"), func(t *testing.T) {
			RunTestCoreResourceEnqueue(t, tt)
		})
	}
}
