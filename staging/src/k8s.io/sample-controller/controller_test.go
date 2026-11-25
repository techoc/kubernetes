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

// 本文件用 fake client 驱动 syncHandler，校验控制器的写入动作而不依赖 API server。
// fixture 把“初始对象、期望动作、期望 API 动作”集中声明，让每个测试只描述差异部分。

package main

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	apps "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/diff"
	kubeinformers "k8s.io/client-go/informers"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/utils/ptr"

	samplecontroller "k8s.io/sample-controller/pkg/apis/samplecontroller/v1alpha1"
	"k8s.io/sample-controller/pkg/generated/clientset/versioned/fake"
	informers "k8s.io/sample-controller/pkg/generated/informers/externalversions"
)

var (
	// 两个测试替身都服务于“直接调 syncHandler 不跑缓存同步”：
	// 一个让等待条件永远通过，一个关掉周期性 resync 避免重复入队干扰断言。
	alwaysReady        = func() bool { return true }
	noResyncPeriodFunc = func() time.Duration { return 0 }
)

// fixture 把一次同步测试所需的上下文打包：
// lister 决定“缓存里有什么”，objects 决定“API 里有什么”，actions 决定“应该写什么”。
type fixture struct {
	t *testing.T

	// 两套 fake client 对应自定义资源和内置资源，和 Controller 的字段一一对应。
	client     *fake.Clientset
	kubeclient *k8sfake.Clientset
	// lister 与 client 分开填充，才能模拟“缓存落后于 API”或“API 有而缓存没有”的情况。
	fooLister        []*samplecontroller.Foo
	deploymentLister []*apps.Deployment
	// 只记录会产生写入的动作；列表和监听由 filterInformerActions 过滤掉。
	kubeactions []core.Action
	actions     []core.Action
	// 预置到 fake client 的对象，供 Get/Create/Update 命中。
	kubeobjects []runtime.Object
	objects     []runtime.Object
}

func newFixture(t *testing.T) *fixture {
	f := &fixture{}
	f.t = t
	// 显式初始化为空切片，后续 append 时语义更清楚。
	f.objects = []runtime.Object{}
	f.kubeobjects = []runtime.Object{}
	return f
}

// newFoo 构造一个标准化的 Foo；只有名字和副本数真正影响测试预期。
func newFoo(name string, replicas *int32) *samplecontroller.Foo {
	return &samplecontroller.Foo{
		TypeMeta: metav1.TypeMeta{APIVersion: samplecontroller.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
		},
		Spec: samplecontroller.FooSpec{
			DeploymentName: fmt.Sprintf("%s-deployment", name),
			Replicas:       replicas,
		},
	}
}

// newController 复用生产构造函数，但把同步条件和事件记录器换成测试替身，
// 这样测试走的代码路径与真实运行尽量一致，又不用真的等缓存或写 Event。
func (f *fixture) newController(ctx context.Context) (*Controller, informers.SharedInformerFactory, kubeinformers.SharedInformerFactory) {
	f.client = fake.NewClientset(f.objects...)
	f.kubeclient = k8sfake.NewClientset(f.kubeobjects...)

	i := informers.NewSharedInformerFactory(f.client, noResyncPeriodFunc())
	k8sI := kubeinformers.NewSharedInformerFactory(f.kubeclient, noResyncPeriodFunc())

	c := NewController(ctx, f.kubeclient, f.client,
		k8sI.Apps().V1().Deployments(), i.Samplecontroller().V1alpha1().Foos())

	// 直接调 syncHandler 不经过 Run，所以缓存不会自动同步完成，这里手工放行。
	c.foosSynced = alwaysReady
	c.deploymentsSynced = alwaysReady
	// 事件内容不是本组测试的重点，用 FakeRecorder 吞掉即可，避免依赖 API。
	c.recorder = &record.FakeRecorder{}

	// 手工把对象塞进 informer 的索引器，构造出“缓存里已有这些对象”的场景。
	for _, f := range f.fooLister {
		i.Samplecontroller().V1alpha1().Foos().Informer().GetIndexer().Add(f)
	}

	for _, d := range f.deploymentLister {
		k8sI.Apps().V1().Deployments().Informer().GetIndexer().Add(d)
	}

	return c, i, k8sI
}

// run 执行一次应当成功的同步。
func (f *fixture) run(ctx context.Context, fooRef cache.ObjectName) {
	f.runController(ctx, fooRef, true, false)
}

// runExpectError 执行一次应当失败的同步，用于“拒绝接管非本控制器对象”之类场景。
func (f *fixture) runExpectError(ctx context.Context, fooRef cache.ObjectName) {
	f.runController(ctx, fooRef, true, true)
}

// runController 是唯一的测试驱动器：启动 informer、调用 syncHandler、再比对实际动作。
// 先断言“有没有多做事”，再断言“有没有少做事”，两个方向都要检查才算覆盖完整。
func (f *fixture) runController(ctx context.Context, fooRef cache.ObjectName, startInformers bool, expectError bool) {
	c, i, k8sI := f.newController(ctx)
	if startInformers {
		i.Start(ctx.Done())
		k8sI.Start(ctx.Done())
	}

	err := c.syncHandler(ctx, fooRef)
	// 错误预期本身就是断言的一部分：期望失败却成功，说明归属校验可能失效。
	if !expectError && err != nil {
		f.t.Errorf("error syncing foo: %v", err)
	} else if expectError && err == nil {
		f.t.Error("expected error syncing foo, got nil")
	}

	// fake client 会记录 informer 的 list/watch，过滤掉它们才能只比较业务写入。
	actions := filterInformerActions(f.client.Actions())
	for i, action := range actions {
		if len(f.actions) < i+1 {
			f.t.Errorf("%d unexpected actions: %+v", len(actions)-len(f.actions), actions[i:])
			break
		}

		expectedAction := f.actions[i]
		checkAction(expectedAction, action, f.t)
	}

	if len(f.actions) > len(actions) {
		f.t.Errorf("%d additional expected actions:%+v", len(f.actions)-len(actions), f.actions[len(actions):])
	}

	k8sActions := filterInformerActions(f.kubeclient.Actions())
	for i, action := range k8sActions {
		if len(f.kubeactions) < i+1 {
			f.t.Errorf("%d unexpected actions: %+v", len(k8sActions)-len(f.kubeactions), k8sActions[i:])
			break
		}

		expectedAction := f.kubeactions[i]
		checkAction(expectedAction, action, f.t)
	}

	if len(f.kubeactions) > len(k8sActions) {
		f.t.Errorf("%d additional expected actions:%+v", len(f.kubeactions)-len(k8sActions), f.kubeactions[len(k8sActions):])
	}
}

// checkAction 校验预期与实际动作及其附带资源是否一致。
// 分层比对：先比动词/资源/子资源，再比具体 payload；
// 这样失败信息能直接指出是“做了错动作”还是“动作对但内容错”。
func checkAction(expected, actual core.Action, t *testing.T) {
	if !(expected.Matches(actual.GetVerb(), actual.GetResource().Resource) && actual.GetSubresource() == expected.GetSubresource()) {
		t.Errorf("Expected\n\t%#v\ngot\n\t%#v", expected, actual)
		return
	}

	if reflect.TypeOf(actual) != reflect.TypeOf(expected) {
		t.Errorf("Action has wrong type. Expected: %t. Got: %t", expected, actual)
		return
	}

	// 按动作类型分别取值比对：Create/Update 看对象，Patch 看补丁字节。
	switch a := actual.(type) {
	case core.CreateActionImpl:
		e, _ := expected.(core.CreateActionImpl)
		expObject := e.GetObject()
		object := a.GetObject()

		if !reflect.DeepEqual(expObject, object) {
			// 并排打印差异，直接看是标签、副本数还是 ownerReference 写错。
			t.Errorf("Action %s %s has wrong object\nDiff:\n %s",
				a.GetVerb(), a.GetResource().Resource, diff.ObjectGoPrintSideBySide(expObject, object))
		}
	case core.UpdateActionImpl:
		e, _ := expected.(core.UpdateActionImpl)
		expObject := e.GetObject()
		object := a.GetObject()

		if !reflect.DeepEqual(expObject, object) {
			t.Errorf("Action %s %s has wrong object\nDiff:\n %s",
				a.GetVerb(), a.GetResource().Resource, diff.ObjectGoPrintSideBySide(expObject, object))
		}
	case core.PatchActionImpl:
		e, _ := expected.(core.PatchActionImpl)
		expPatch := e.GetPatch()
		patch := a.GetPatch()

		if !reflect.DeepEqual(expPatch, patch) {
			t.Errorf("Action %s %s has wrong patch\nDiff:\n %s",
				a.GetVerb(), a.GetResource().Resource, diff.ObjectGoPrintSideBySide(expPatch, patch))
		}
	default:
		// 刻意报错而不是静默放过：控制器新增动作类型时，测试必须跟着补断言。
		t.Errorf("Uncaptured Action %s %s, you should explicitly add a case to capture it",
			actual.GetVerb(), actual.GetResource().Resource)
	}
}

// filterInformerActions 过滤测试资源的后台 list/watch 动作，降低断言噪声。
// 这些动作是 informer 后台自发产生的，和 syncHandler 的业务决策无关；
// 留着它们会让期望动作列表随启动时序漂移。
func filterInformerActions(actions []core.Action) []core.Action {
	ret := []core.Action{}
	for _, action := range actions {
		// 只过滤集群级别的 list/watch；带 namespace 的同类动作仍可能是业务读取，不能误删。
		if len(action.GetNamespace()) == 0 &&
			(action.Matches("list", "foos") ||
				action.Matches("watch", "foos") ||
				action.Matches("list", "deployments") ||
				action.Matches("watch", "deployments")) {
			continue
		}
		ret = append(ret, action)
	}

	return ret
}

// expectCreateDeploymentAction 记录“应向 kube client 创建 Deployment”的预期；
// 用 newDeployment 的产物作为期望值，等于把构造逻辑也纳入回归范围。
func (f *fixture) expectCreateDeploymentAction(d *apps.Deployment) {
	f.kubeactions = append(f.kubeactions, core.NewCreateAction(schema.GroupVersionResource{Resource: "deployments"}, d.Namespace, d))
}

// expectUpdateDeploymentAction 记录“应更新已有 Deployment”的预期。
func (f *fixture) expectUpdateDeploymentAction(d *apps.Deployment) {
	f.kubeactions = append(f.kubeactions, core.NewUpdateAction(schema.GroupVersionResource{Resource: "deployments"}, d.Namespace, d))
}

// expectUpdateFooStatusAction 指向 status 子资源，确保控制器没有越过 spec 去写整个 Foo。
func (f *fixture) expectUpdateFooStatusAction(foo *samplecontroller.Foo) {
	action := core.NewUpdateSubresourceAction(schema.GroupVersionResource{Resource: "foos"}, "status", foo.Namespace, foo)
	f.actions = append(f.actions, action)
}

// getRef 复用 informer 的元数据转换，避免测试自己拼字符串与生产代码产生偏差。
func getRef(foo *samplecontroller.Foo, t *testing.T) cache.ObjectName {
	ref := cache.MetaObjectToName(foo)
	return ref
}

// TestCreatesDeployment 覆盖“Foo 存在但 Deployment 缺失”：应创建 Deployment 并回填状态。
func TestCreatesDeployment(t *testing.T) {
	f := newFixture(t)
	foo := newFoo("test", ptr.To[int32](1))
	_, ctx := ktesting.NewTestContext(t)

	f.fooLister = append(f.fooLister, foo)
	f.objects = append(f.objects, foo)

	expDeployment := newDeployment(foo)
	f.expectCreateDeploymentAction(expDeployment)
	f.expectUpdateFooStatusAction(foo)

	f.run(ctx, getRef(foo, t))
}

// TestDoNothing 覆盖“期望状态已经一致”：不创建也不更新 Deployment，只更新一次状态。
func TestDoNothing(t *testing.T) {
	f := newFixture(t)
	foo := newFoo("test", ptr.To[int32](1))
	_, ctx := ktesting.NewTestContext(t)

	d := newDeployment(foo)

	f.fooLister = append(f.fooLister, foo)
	f.objects = append(f.objects, foo)
	f.deploymentLister = append(f.deploymentLister, d)
	f.kubeobjects = append(f.kubeobjects, d)

	f.expectUpdateFooStatusAction(foo)
	f.run(ctx, getRef(foo, t))
}

// TestUpdateDeployment 覆盖“用户改了副本数”：应把差异同步到 Deployment，再回填状态。
func TestUpdateDeployment(t *testing.T) {
	f := newFixture(t)
	foo := newFoo("test", ptr.To[int32](1))
	_, ctx := ktesting.NewTestContext(t)

	d := newDeployment(foo)

	// 修改期望副本数，触发 Deployment 更新。
	foo.Spec.Replicas = ptr.To[int32](2)
	expDeployment := newDeployment(foo)

	f.fooLister = append(f.fooLister, foo)
	f.objects = append(f.objects, foo)
	f.deploymentLister = append(f.deploymentLister, d)
	f.kubeobjects = append(f.kubeobjects, d)

	f.expectUpdateFooStatusAction(foo)
	f.expectUpdateDeploymentAction(expDeployment)
	f.run(ctx, getRef(foo, t))
}

// TestNotControlledByUs 覆盖“同名 Deployment 不属于本 Foo”：
// 必须报错且不做任何写入，否则会抢走别人的资源。
func TestNotControlledByUs(t *testing.T) {
	f := newFixture(t)
	foo := newFoo("test", ptr.To[int32](1))
	_, ctx := ktesting.NewTestContext(t)

	d := newDeployment(foo)

	d.ObjectMeta.OwnerReferences = []metav1.OwnerReference{}

	f.fooLister = append(f.fooLister, foo)
	f.objects = append(f.objects, foo)
	f.deploymentLister = append(f.deploymentLister, d)
	f.kubeobjects = append(f.kubeobjects, d)

	f.runExpectError(ctx, getRef(foo, t))
}
