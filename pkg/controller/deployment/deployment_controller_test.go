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

package deployment

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	_ "k8s.io/kubernetes/pkg/apis/apps/install"
	_ "k8s.io/kubernetes/pkg/apis/authentication/install"
	_ "k8s.io/kubernetes/pkg/apis/authorization/install"
	_ "k8s.io/kubernetes/pkg/apis/autoscaling/install"
	_ "k8s.io/kubernetes/pkg/apis/batch/install"
	_ "k8s.io/kubernetes/pkg/apis/certificates/install"
	_ "k8s.io/kubernetes/pkg/apis/core/install"
	_ "k8s.io/kubernetes/pkg/apis/policy/install"
	_ "k8s.io/kubernetes/pkg/apis/rbac/install"
	_ "k8s.io/kubernetes/pkg/apis/storage/install"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/deployment/util"
	"k8s.io/kubernetes/pkg/controller/testutil"
	"k8s.io/utils/ptr"
)

var (
	// alwaysReady 作为同步检查的桩，让测试跳过 informer 缓存同步等待。
	alwaysReady = func() bool { return true }
	// noTimestamp 表示未设置创建时间，避免影响按时间排序的断言。
	noTimestamp = metav1.Time{}
)

// rs 构造一个指定名称、副本数、选择器和创建时间的 ReplicaSet。
func rs(name string, replicas int32, selector map[string]string, timestamp metav1.Time) *apps.ReplicaSet {
	return &apps.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: timestamp,
			Namespace:         metav1.NamespaceDefault,
		},
		Spec: apps.ReplicaSetSpec{
			Replicas: ptr.To(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: selector},
			Template: v1.PodTemplateSpec{},
		},
	}
}

// newRSWithStatus 构造一个同时设置了 spec 与 status 副本数的 ReplicaSet，
// 用于制造期望副本数与就绪副本数不一致的场景。
func newRSWithStatus(name string, specReplicas, statusReplicas int32, selector map[string]string) *apps.ReplicaSet {
	rs := rs(name, specReplicas, selector, noTimestamp)
	rs.Status = apps.ReplicaSetStatus{
		Replicas: statusReplicas,
	}
	return rs
}

// newDeployment 构造一个使用 RollingUpdate 策略的测试 Deployment；
// maxSurge 与 maxUnavailable 为 nil 时保留默认的 0 值。
func newDeployment(name string, replicas int32, revisionHistoryLimit *int32, maxSurge, maxUnavailable *intstr.IntOrString, selector map[string]string) *apps.Deployment {
	d := apps.Deployment{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{
			UID:         uuid.NewUUID(),
			Name:        name,
			Namespace:   metav1.NamespaceDefault,
			Annotations: make(map[string]string),
		},
		Spec: apps.DeploymentSpec{
			Strategy: apps.DeploymentStrategy{
				Type: apps.RollingUpdateDeploymentStrategyType,
				RollingUpdate: &apps.RollingUpdateDeployment{
					MaxUnavailable: ptr.To(intstr.FromInt32(0)),
					MaxSurge:       ptr.To(intstr.FromInt32(0)),
				},
			},
			Replicas: ptr.To(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: selector},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: selector,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Image: "foo/bar",
						},
					},
				},
			},
			RevisionHistoryLimit: revisionHistoryLimit,
		},
	}
	if maxSurge != nil {
		d.Spec.Strategy.RollingUpdate.MaxSurge = maxSurge
	}
	if maxUnavailable != nil {
		d.Spec.Strategy.RollingUpdate.MaxUnavailable = maxUnavailable
	}
	return &d
}

// newReplicaSet 基于 Deployment 的选择器与模板构造一个归属该 Deployment 的 ReplicaSet。
func newReplicaSet(d *apps.Deployment, name string, replicas int32) *apps.ReplicaSet {
	return &apps.ReplicaSet{
		TypeMeta: metav1.TypeMeta{Kind: "ReplicaSet"},
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			UID:             uuid.NewUUID(),
			Namespace:       metav1.NamespaceDefault,
			Labels:          d.Spec.Selector.MatchLabels,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(d, controllerKind)},
		},
		Spec: apps.ReplicaSetSpec{
			Selector: d.Spec.Selector,
			Replicas: ptr.To(replicas),
			Template: d.Spec.Template,
		},
	}
}

// fixture 封装单元测试所需的假客户端、informer 存储数据与期望的客户端动作。
type fixture struct {
	t testing.TB

	client *fake.Clientset
	// 放入 informer 存储（lister）中的对象。
	dLister   []*apps.Deployment
	rsLister  []*apps.ReplicaSet
	podLister []*v1.Pod

	// 期望客户端产生的动作；objects 中的对象会预加载到 NewSimpleFake。
	actions []core.Action
	objects []runtime.Object
}

// expectGetDeploymentAction 记录一次对指定 Deployment 的 get 动作期望。
func (f *fixture) expectGetDeploymentAction(d *apps.Deployment) {
	action := core.NewGetAction(schema.GroupVersionResource{Resource: "deployments"}, d.Namespace, d.Name)
	f.actions = append(f.actions, action)
}

// expectUpdateDeploymentStatusAction 记录一次对 Deployment status 子资源的 update 动作期望。
func (f *fixture) expectUpdateDeploymentStatusAction(d *apps.Deployment) {
	action := core.NewUpdateAction(schema.GroupVersionResource{Resource: "deployments"}, d.Namespace, d)
	action.Subresource = "status"
	f.actions = append(f.actions, action)
}

// expectUpdateDeploymentAction 记录一次对 Deployment 本体的 update 动作期望。
func (f *fixture) expectUpdateDeploymentAction(d *apps.Deployment) {
	action := core.NewUpdateAction(schema.GroupVersionResource{Resource: "deployments"}, d.Namespace, d)
	f.actions = append(f.actions, action)
}

// expectCreateRSAction 记录一次创建 ReplicaSet 的动作期望。
func (f *fixture) expectCreateRSAction(rs *apps.ReplicaSet) {
	f.actions = append(f.actions, core.NewCreateAction(schema.GroupVersionResource{Resource: "replicasets"}, rs.Namespace, rs))
}

// newFixture 创建一个空的测试夹具。
func newFixture(t testing.TB) *fixture {
	f := &fixture{}
	f.t = t
	f.objects = []runtime.Object{}
	return f
}

// newController 构造 DeploymentController 并把夹具中的对象灌入各 informer 索引。
// 同步检查被替换为 alwaysReady，以便测试无需真正等待缓存同步。
func (f *fixture) newController(ctx context.Context) (*DeploymentController, informers.SharedInformerFactory, error) {
	f.client = fake.NewSimpleClientset(f.objects...)
	informers := informers.NewSharedInformerFactory(f.client, controller.NoResyncPeriodFunc())
	c, err := NewDeploymentController(ctx, informers.Apps().V1().Deployments(), informers.Apps().V1().ReplicaSets(), informers.Core().V1().Pods(), f.client)
	if err != nil {
		return nil, nil, err
	}
	c.eventRecorder = &record.FakeRecorder{}
	c.dListerSynced = alwaysReady
	c.rsListerSynced = alwaysReady
	c.podListerSynced = alwaysReady
	for _, d := range f.dLister {
		informers.Apps().V1().Deployments().Informer().GetIndexer().Add(d)
	}
	for _, rs := range f.rsLister {
		informers.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(rs)
	}
	for _, pod := range f.podLister {
		informers.Core().V1().Pods().Informer().GetIndexer().Add(pod)
	}
	return c, informers, nil
}

// runExpectError 运行同步并断言返回错误。
func (f *fixture) runExpectError(ctx context.Context, deploymentName string, startInformers bool) {
	f.run_(ctx, deploymentName, startInformers, true)
}

// run 启动 informer 后同步指定 Deployment，并断言无错误、动作符合预期。
func (f *fixture) run(ctx context.Context, deploymentName string) {
	f.run_(ctx, deploymentName, true, false)
}

// run_ 是 run/runExpectError 的公共实现，可选择是否启动 informer，
// 并逐条比对客户端实际动作与期望动作。
func (f *fixture) run_(ctx context.Context, deploymentName string, startInformers bool, expectError bool) {
	c, informers, err := f.newController(ctx)
	if err != nil {
		f.t.Fatalf("error creating Deployment controller: %v", err)
	}
	if startInformers {
		stopCh := make(chan struct{})
		defer close(stopCh)
		informers.Start(stopCh)
	}

	err = c.syncDeployment(ctx, deploymentName)
	if !expectError && err != nil {
		f.t.Errorf("error syncing deployment: %v", err)
	} else if expectError && err == nil {
		f.t.Error("expected error syncing deployment, got nil")
	}

	actions := filterInformerActions(f.client.Actions())
	for i, action := range actions {
		if len(f.actions) < i+1 {
			f.t.Errorf("%d unexpected actions: %+v", len(actions)-len(f.actions), actions[i:])
			break
		}

		expectedAction := f.actions[i]
		if !(expectedAction.Matches(action.GetVerb(), action.GetResource().Resource) && action.GetSubresource() == expectedAction.GetSubresource()) {
			f.t.Errorf("Expected\n\t%#v\ngot\n\t%#v", expectedAction, action)
			continue
		}
	}

	if len(f.actions) > len(actions) {
		f.t.Errorf("%d additional expected actions:%+v", len(f.actions)-len(actions), f.actions[len(actions):])
	}
}

// filterInformerActions 过滤掉 informer 启动时的 list/watch 动作，
// 只保留业务同步产生的动作供断言使用。
func filterInformerActions(actions []core.Action) []core.Action {
	ret := []core.Action{}
	for _, action := range actions {
		if len(action.GetNamespace()) == 0 &&
			(action.Matches("list", "pods") ||
				action.Matches("list", "deployments") ||
				action.Matches("list", "replicasets") ||
				action.Matches("watch", "pods") ||
				action.Matches("watch", "deployments") ||
				action.Matches("watch", "replicasets")) {
			continue
		}
		ret = append(ret, action)
	}

	return ret
}

// TestSyncDeploymentCreatesReplicaSet 验证首次同步 Deployment 时会创建 ReplicaSet 并更新状态。
func TestSyncDeploymentCreatesReplicaSet(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	f := newFixture(t)

	d := newDeployment("foo", 1, nil, nil, nil, map[string]string{"foo": "bar"})
	f.dLister = append(f.dLister, d)
	f.objects = append(f.objects, d)

	rs := newReplicaSet(d, "deploymentrs-4186632231", 1)

	f.expectCreateRSAction(rs)
	f.expectUpdateDeploymentStatusAction(d)
	f.expectUpdateDeploymentStatusAction(d)

	f.run(ctx, testutil.GetKey(d, t))
}

// TestSyncDeploymentDontDoAnythingDuringDeletion 验证处于删除中的 Deployment 只更新状态，
// 不会创建新的 ReplicaSet。
func TestSyncDeploymentDontDoAnythingDuringDeletion(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	f := newFixture(t)

	d := newDeployment("foo", 1, nil, nil, nil, map[string]string{"foo": "bar"})
	now := metav1.Now()
	d.DeletionTimestamp = &now
	f.dLister = append(f.dLister, d)
	f.objects = append(f.objects, d)

	f.expectUpdateDeploymentStatusAction(d)
	f.run(ctx, testutil.GetKey(d, t))
}

// TestSyncDeploymentDeletionRace 验证缓存与直连读结果不一致时的竞态：
// 直连读显示对象已删除时必须重新检查，并让同步失败以等待缓存追上。
func TestSyncDeploymentDeletionRace(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	f := newFixture(t)

	d := newDeployment("foo", 1, nil, nil, nil, map[string]string{"foo": "bar"})
	d2 := *d
	// lister（缓存）认为对象未被删除。
	f.dLister = append(f.dLister, d)
	// 直连客户端认为对象已删除，应视为更新的状态。
	now := metav1.Now()
	d2.DeletionTimestamp = &now
	f.objects = append(f.objects, &d2)

	// 只有存在匹配的孤立 ReplicaSet 时才会触发删除时间戳的重新检查。
	rs := newReplicaSet(d, "rs1", 1)
	rs.OwnerReferences = nil
	f.objects = append(f.objects, rs)
	f.rsLister = append(f.rsLister, rs)

	// 期望只发生一次 get，用于重新检查 DeletionTimestamp。
	f.expectGetDeploymentAction(d)
	// 同步应失败并重入队，让缓存有机会追上。
	// 不启动 informer，避免缓存在本测试中追上直连读。
	f.runExpectError(ctx, testutil.GetKey(d, t), false)
}

// 问题：https://github.com/kubernetes/kubernetes/issues/23218
// TestDontSyncDeploymentsWithEmptyPodSelector 验证 Pod 选择器为空的 Deployment 不会被同步，
// 以免误选集群中所有 Pod。
func TestDontSyncDeploymentsWithEmptyPodSelector(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	f := newFixture(t)

	d := newDeployment("foo", 1, nil, nil, nil, map[string]string{"foo": "bar"})
	d.Spec.Selector = &metav1.LabelSelector{}
	f.dLister = append(f.dLister, d)
	f.objects = append(f.objects, d)

	// 正常应有一次状态更新以同步 observedGeneration，但这里的假 Deployment
	// 未设置 generation，因此预期不产生任何动作。
	f.run(ctx, testutil.GetKey(d, t))
}

// TestReentrantRollback 验证重复回滚（可重入）场景下只更新 Deployment，
// 不会反复更新 ReplicaSet。
func TestReentrantRollback(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	f := newFixture(t)

	d := newDeployment("foo", 1, nil, nil, nil, map[string]string{"foo": "bar"})
	d.Annotations = map[string]string{util.RevisionAnnotation: "2"}
	setRollbackTo(d, new(int64))
	f.dLister = append(f.dLister, d)

	rs1 := newReplicaSet(d, "deploymentrs-old", 0)
	rs1.Annotations = map[string]string{util.RevisionAnnotation: "1"}
	one := int64(1)
	rs1.Spec.Template.Spec.TerminationGracePeriodSeconds = &one
	rs1.Spec.Selector.MatchLabels[apps.DefaultDeploymentUniqueLabelKey] = "hash"

	rs2 := newReplicaSet(d, "deploymentrs-new", 1)
	rs2.Annotations = map[string]string{util.RevisionAnnotation: "2"}
	rs2.Spec.Selector.MatchLabels[apps.DefaultDeploymentUniqueLabelKey] = "hash"

	f.rsLister = append(f.rsLister, rs1, rs2)
	f.objects = append(f.objects, d, rs1, rs2)

	// 此处完成回滚，预期更新 Deployment。
	f.expectUpdateDeploymentAction(d)
	// 但不应更新任何 ReplicaSet。
	f.run(ctx, testutil.GetKey(d, t))
}

// TestPodDeletionEnqueuesRecreateDeployment 确保无论是否还存在其他 Pod，
// Pod 删除事件都会让 Recreate 策略的 Deployment 重新入队。
func TestPodDeletionEnqueuesRecreateDeployment(t *testing.T) {
	tests := []struct {
		name      string // 用例名称
		otherPods bool   // 是否还有其他同类 Pod 存活
	}{
		{
			name:      "last pod deleted",
			otherPods: false,
		},
		{
			name:      "pod with siblings deleted",
			otherPods: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, ctx := ktesting.NewTestContext(t)
			f := newFixture(t)

			foo := newDeployment("foo", 1, nil, nil, nil, map[string]string{"foo": "bar"})
			foo.Spec.Strategy.Type = apps.RecreateDeploymentStrategyType
			rs := newReplicaSet(foo, "foo-1", 1)
			pod := generatePodFromRS(rs)

			f.dLister = append(f.dLister, foo)
			f.rsLister = append(f.rsLister, rs)
			f.objects = append(f.objects, foo, rs)
			f.podLister = append(f.podLister, pod)

			if tc.otherPods {
				// 添加一个同属该 ReplicaSet 的兄弟 Pod。
				pod2 := generatePodFromRS(rs)
				pod2.Name = "foo-2"
				f.podLister = append(f.podLister, pod2)
			}

			c, _, err := f.newController(ctx)
			if err != nil {
				t.Fatalf("error creating Deployment controller: %v", err)
			}
			enqueued := false
			c.enqueueDeployment = func(d *apps.Deployment) {
				if d.Name == "foo" {
					enqueued = true
				}
			}

			c.deletePod(logger, pod)

			if !enqueued {
				t.Errorf("expected deployment %q to be queued after pod deletion", foo.Name)
			}
		})
	}
}

// TestPodDeletionPartialReplicaSetOwnershipEnqueueRecreateDeployment 验证在
// Deployment 拥有多个 ReplicaSet（部分 ReplicaSet 没有 OwnerReference）时，
// 只有当客户端再无其他 Pod 返回时，Pod 删除才会让 Recreate Deployment 入队。
func TestPodDeletionPartialReplicaSetOwnershipEnqueueRecreateDeployment(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)

	f := newFixture(t)

	foo := newDeployment("foo", 1, nil, nil, nil, map[string]string{"foo": "bar"})
	foo.Spec.Strategy.Type = apps.RecreateDeploymentStrategyType
	rs1 := newReplicaSet(foo, "foo-1", 1)
	rs2 := newReplicaSet(foo, "foo-2", 2)
	rs2.OwnerReferences = nil
	pod := generatePodFromRS(rs1)

	f.dLister = append(f.dLister, foo)
	f.rsLister = append(f.rsLister, rs1, rs2)
	f.objects = append(f.objects, foo, rs1, rs2)

	c, _, err := f.newController(ctx)
	if err != nil {
		t.Fatalf("error creating Deployment controller: %v", err)
	}
	enqueued := false
	c.enqueueDeployment = func(d *apps.Deployment) {
		if d.Name == "foo" {
			enqueued = true
		}
	}

	c.deletePod(logger, pod)

	if !enqueued {
		t.Errorf("expected deployment %q to be queued after pod deletion", foo.Name)
	}
}

// TestGetReplicaSetsForDeployment 验证通过 ControllerRef 能正确区分标签相同但
// 归属不同 Deployment 的 ReplicaSet。
func TestGetReplicaSetsForDeployment(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	f := newFixture(t)

	// 两个标签完全相同的 Deployment。
	d1 := newDeployment("foo", 1, nil, nil, nil, map[string]string{"foo": "bar"})
	d2 := newDeployment("bar", 1, nil, nil, nil, map[string]string{"foo": "bar"})

	// 两个 ReplicaSet 的标签同时匹配两个 Deployment，
	// 但通过 ControllerRef 明确区分归属。
	rs1 := newReplicaSet(d1, "rs1", 1)
	rs2 := newReplicaSet(d2, "rs2", 1)

	f.dLister = append(f.dLister, d1, d2)
	f.rsLister = append(f.rsLister, rs1, rs2)
	f.objects = append(f.objects, d1, d2, rs1, rs2)

	// 启动夹具。
	c, informers, err := f.newController(ctx)
	if err != nil {
		t.Fatalf("error creating Deployment controller: %v", err)
	}
	stopCh := make(chan struct{})
	defer close(stopCh)
	informers.Start(stopCh)

	rsList, err := c.getReplicaSetsForDeployment(ctx, d1)
	if err != nil {
		t.Fatalf("getReplicaSetsForDeployment() error: %v", err)
	}
	rsNames := []string{}
	for _, rs := range rsList {
		rsNames = append(rsNames, rs.Name)
	}
	if len(rsNames) != 1 || rsNames[0] != rs1.Name {
		t.Errorf("getReplicaSetsForDeployment() = %v, want [%v]", rsNames, rs1.Name)
	}

	rsList, err = c.getReplicaSetsForDeployment(ctx, d2)
	if err != nil {
		t.Fatalf("getReplicaSetsForDeployment() error: %v", err)
	}
	rsNames = []string{}
	for _, rs := range rsList {
		rsNames = append(rsNames, rs.Name)
	}
	if len(rsNames) != 1 || rsNames[0] != rs2.Name {
		t.Errorf("getReplicaSetsForDeployment() = %v, want [%v]", rsNames, rs2.Name)
	}
}

// TestGetReplicaSetsForDeploymentAdoptRelease 验证标签匹配但无主的 ReplicaSet 会被认领，
// 而 ControllerRef 匹配但标签不符的 ReplicaSet 会被释放。
func TestGetReplicaSetsForDeploymentAdoptRelease(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	f := newFixture(t)

	d := newDeployment("foo", 1, nil, nil, nil, map[string]string{"foo": "bar"})

	// 标签匹配但无主的 ReplicaSet，应被认领并返回。
	rsAdopt := newReplicaSet(d, "rsAdopt", 1)
	rsAdopt.OwnerReferences = nil
	// ControllerRef 匹配但标签不符的 ReplicaSet，应被释放（不再管理）。
	rsRelease := newReplicaSet(d, "rsRelease", 1)
	rsRelease.Labels = map[string]string{"foo": "notbar"}

	f.dLister = append(f.dLister, d)
	f.rsLister = append(f.rsLister, rsAdopt, rsRelease)
	f.objects = append(f.objects, d, rsAdopt, rsRelease)

	// 启动夹具。
	c, informers, err := f.newController(ctx)
	if err != nil {
		t.Fatalf("error creating Deployment controller: %v", err)
	}
	stopCh := make(chan struct{})
	defer close(stopCh)
	informers.Start(stopCh)

	rsList, err := c.getReplicaSetsForDeployment(ctx, d)
	if err != nil {
		t.Fatalf("getReplicaSetsForDeployment() error: %v", err)
	}
	rsNames := []string{}
	for _, rs := range rsList {
		rsNames = append(rsNames, rs.Name)
	}
	if len(rsNames) != 1 || rsNames[0] != rsAdopt.Name {
		t.Errorf("getReplicaSetsForDeployment() = %v, want [%v]", rsNames, rsAdopt.Name)
	}
}

// TestGetPodMapForReplicaSets 验证 Pod 按 ControllerRef 正确归入各 ReplicaSet，
// 无主 Pod 不会被计入。
func TestGetPodMapForReplicaSets(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	f := newFixture(t)

	d := newDeployment("foo", 1, nil, nil, nil, map[string]string{"foo": "bar"})

	rs1 := newReplicaSet(d, "rs1", 1)
	rs2 := newReplicaSet(d, "rs2", 1)

	// 为每个 ReplicaSet 添加一个 Pod。
	pod1 := generatePodFromRS(rs1)
	pod2 := generatePodFromRS(rs2)
	// 标签匹配但没有 ControllerRef 的 Pod，应被忽略。
	pod3 := generatePodFromRS(rs1)
	pod3.Name = "pod3"
	pod3.OwnerReferences = nil
	// 标签与 ControllerRef 都匹配、但已处于失败状态的 Pod，仍会被索引纳入。
	pod4 := generatePodFromRS(rs1)
	pod4.Name = "pod4"
	pod4.Status.Phase = v1.PodFailed

	f.dLister = append(f.dLister, d)
	f.rsLister = append(f.rsLister, rs1, rs2)
	f.podLister = append(f.podLister, pod1, pod2, pod3, pod4)
	f.objects = append(f.objects, d, rs1, rs2, pod1, pod2, pod3, pod4)

	// 启动夹具。
	c, informers, err := f.newController(ctx)
	if err != nil {
		t.Fatalf("error creating Deployment controller: %v", err)
	}
	stopCh := make(chan struct{})
	defer close(stopCh)
	informers.Start(stopCh)

	podMap, err := c.getPodMapForDeployment(d, f.rsLister)
	if err != nil {
		t.Fatalf("getPodMapForDeployment() error: %v", err)
	}
	podCount := 0
	for _, podList := range podMap {
		podCount += len(podList)
	}
	if got, want := podCount, 3; got != want {
		t.Errorf("podCount = %v, want %v", got, want)
	}

	if got, want := len(podMap), 2; got != want {
		t.Errorf("len(podMap) = %v, want %v", got, want)
	}
	if got, want := len(podMap[rs1.UID]), 2; got != want {
		t.Errorf("len(podMap[rs1]) = %v, want %v", got, want)
	}
	expect := map[string]struct{}{"rs1-pod": {}, "pod4": {}}
	for _, pod := range podMap[rs1.UID] {
		if _, ok := expect[pod.Name]; !ok {
			t.Errorf("unexpected pod name for rs1: %s", pod.Name)
		}
	}
	if got, want := len(podMap[rs2.UID]), 1; got != want {
		t.Errorf("len(podMap[rs2]) = %v, want %v", got, want)
	}
	if got, want := podMap[rs2.UID][0].Name, "rs2-pod"; got != want {
		t.Errorf("podMap[rs2] = [%v], want [%v]", got, want)
	}
}

// TestAddReplicaSet 验证 ReplicaSet 创建事件会把其所属 Deployment 加入队列。
func TestAddReplicaSet(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)

	f := newFixture(t)

	d1 := newDeployment("d1", 1, nil, nil, nil, map[string]string{"foo": "bar"})
	d2 := newDeployment("d2", 1, nil, nil, nil, map[string]string{"foo": "bar"})

	// 两个 ReplicaSet 的标签同时匹配两个 Deployment，
	// 但通过 ControllerRef 明确区分归属。
	rs1 := newReplicaSet(d1, "rs1", 1)
	rs2 := newReplicaSet(d2, "rs2", 1)

	f.dLister = append(f.dLister, d1, d2)
	f.objects = append(f.objects, d1, d2, rs1, rs2)

	// 只创建夹具而不启动，避免后台产生额外动作。
	dc, _, err := f.newController(ctx)
	if err != nil {
		t.Fatalf("error creating Deployment controller: %v", err)
	}

	dc.addReplicaSet(klog.FromContext(ctx), rs1)
	if got, want := dc.queue.Len(), 1; got != want {
		t.Fatalf("queue.Len() = %v, want %v", got, want)
	}
	key, done := dc.queue.Get()
	if key == "" || done {
		t.Fatalf("failed to enqueue controller for rs %v", rs1.Name)
	}
	expectedKey, _ := controller.KeyFunc(d1)
	if got, want := key, expectedKey; got != want {
		t.Errorf("queue.Get() = %v, want %v", got, want)
	}

	dc.addReplicaSet(logger, rs2)
	if got, want := dc.queue.Len(), 1; got != want {
		t.Fatalf("queue.Len() = %v, want %v", got, want)
	}
	key, done = dc.queue.Get()
	if key == "" || done {
		t.Fatalf("failed to enqueue controller for rs %v", rs2.Name)
	}
	expectedKey, _ = controller.KeyFunc(d2)
	if got, want := key, expectedKey; got != want {
		t.Errorf("queue.Get() = %v, want %v", got, want)
	}
}

// TestAddReplicaSetOrphan 验证孤儿 ReplicaSet 创建时所有标签匹配的 Deployment 都会入队。
func TestAddReplicaSetOrphan(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)

	f := newFixture(t)

	// 其中两个 Deployment 会匹配该 RS，一个不会。
	d1 := newDeployment("d1", 1, nil, nil, nil, map[string]string{"foo": "bar"})
	d2 := newDeployment("d2", 1, nil, nil, nil, map[string]string{"foo": "bar"})
	d3 := newDeployment("d3", 1, nil, nil, nil, map[string]string{"foo": "bar"})
	d3.Spec.Selector.MatchLabels = map[string]string{"foo": "notbar"}

	// 将该 RS 变为孤儿，期望所有匹配的 Deployment 都会入队。
	rs := newReplicaSet(d1, "rs1", 1)
	rs.OwnerReferences = nil

	f.dLister = append(f.dLister, d1, d2, d3)
	f.objects = append(f.objects, d1, d2, d3)

	// 只创建夹具而不启动，避免后台产生额外动作。
	dc, _, err := f.newController(ctx)
	if err != nil {
		t.Fatalf("error creating Deployment controller: %v", err)
	}

	dc.addReplicaSet(logger, rs)
	if got, want := dc.queue.Len(), 2; got != want {
		t.Fatalf("queue.Len() = %v, want %v", got, want)
	}
}

// TestUpdateReplicaSet 验证 ReplicaSet 更新事件会把新归属的 Deployment 加入队列。
func TestUpdateReplicaSet(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)

	f := newFixture(t)

	d1 := newDeployment("d1", 1, nil, nil, nil, map[string]string{"foo": "bar"})
	d2 := newDeployment("d2", 1, nil, nil, nil, map[string]string{"foo": "bar"})

	// 两个 ReplicaSet 的标签同时匹配两个 Deployment，
	// 但通过 ControllerRef 明确区分归属。
	rs1 := newReplicaSet(d1, "rs1", 1)
	rs2 := newReplicaSet(d2, "rs2", 1)

	f.dLister = append(f.dLister, d1, d2)
	f.rsLister = append(f.rsLister, rs1, rs2)
	f.objects = append(f.objects, d1, d2, rs1, rs2)

	// 只创建夹具而不启动，避免后台产生额外动作。
	dc, _, err := f.newController(ctx)
	if err != nil {
		t.Fatalf("error creating Deployment controller: %v", err)
	}

	prev := *rs1
	next := *rs1
	bumpResourceVersion(&next)
	dc.updateReplicaSet(logger, &prev, &next)
	if got, want := dc.queue.Len(), 1; got != want {
		t.Fatalf("queue.Len() = %v, want %v", got, want)
	}
	key, done := dc.queue.Get()
	if key == "" || done {
		t.Fatalf("failed to enqueue controller for rs %v", rs1.Name)
	}
	expectedKey, _ := controller.KeyFunc(d1)
	if got, want := key, expectedKey; got != want {
		t.Errorf("queue.Get() = %v, want %v", got, want)
	}

	prev = *rs2
	next = *rs2
	bumpResourceVersion(&next)
	dc.updateReplicaSet(logger, &prev, &next)
	if got, want := dc.queue.Len(), 1; got != want {
		t.Fatalf("queue.Len() = %v, want %v", got, want)
	}
	key, done = dc.queue.Get()
	if key == "" || done {
		t.Fatalf("failed to enqueue controller for rs %v", rs2.Name)
	}
	expectedKey, _ = controller.KeyFunc(d2)
	if got, want := key, expectedKey; got != want {
		t.Errorf("queue.Get() = %v, want %v", got, want)
	}
}

// TestUpdateReplicaSetOrphanWithNewLabels 验证孤儿 ReplicaSet 标签变化时，
// 所有新匹配的 Deployment 都会入队，以便有机会认领它。
func TestUpdateReplicaSetOrphanWithNewLabels(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)

	f := newFixture(t)

	d1 := newDeployment("d1", 1, nil, nil, nil, map[string]string{"foo": "bar"})
	d2 := newDeployment("d2", 1, nil, nil, nil, map[string]string{"foo": "bar"})

	// 该 RS 同时匹配两个 Deployment，但处于无主状态。
	rs := newReplicaSet(d1, "rs1", 1)
	rs.OwnerReferences = nil

	f.dLister = append(f.dLister, d1, d2)
	f.rsLister = append(f.rsLister, rs)
	f.objects = append(f.objects, d1, d2, rs)

	// 只创建夹具而不启动，避免后台产生额外动作。
	dc, _, err := f.newController(ctx)
	if err != nil {
		t.Fatalf("error creating Deployment controller: %v", err)
	}

	// 修改标签，期望所有匹配的控制器都入队。
	prev := *rs
	prev.Labels = map[string]string{"foo": "notbar"}
	next := *rs
	bumpResourceVersion(&next)
	dc.updateReplicaSet(logger, &prev, &next)
	if got, want := dc.queue.Len(), 2; got != want {
		t.Fatalf("queue.Len() = %v, want %v", got, want)
	}
}

// TestUpdateReplicaSetChangeControllerRef 验证 ControllerRef 变更时新旧两个
// Deployment 都会入队，以便旧控制器释放、新控制器认领。
func TestUpdateReplicaSetChangeControllerRef(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)

	f := newFixture(t)

	d1 := newDeployment("d1", 1, nil, nil, nil, map[string]string{"foo": "bar"})
	d2 := newDeployment("d2", 1, nil, nil, nil, map[string]string{"foo": "bar"})

	rs := newReplicaSet(d1, "rs1", 1)

	f.dLister = append(f.dLister, d1, d2)
	f.rsLister = append(f.rsLister, rs)
	f.objects = append(f.objects, d1, d2, rs)

	// 只创建夹具而不启动，避免后台产生额外动作。
	dc, _, err := f.newController(ctx)
	if err != nil {
		t.Fatalf("error creating Deployment controller: %v", err)
	}

	// 修改 ControllerRef，期望新旧两个控制器都入队。
	prev := *rs
	prev.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(d2, controllerKind)}
	next := *rs
	bumpResourceVersion(&next)
	dc.updateReplicaSet(logger, &prev, &next)
	if got, want := dc.queue.Len(), 2; got != want {
		t.Fatalf("queue.Len() = %v, want %v", got, want)
	}
}

// TestUpdateReplicaSetRelease 验证移除 ControllerRef 后，所有匹配的 Deployment
// 都会入队以同步该孤儿 ReplicaSet。
func TestUpdateReplicaSetRelease(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)

	f := newFixture(t)

	d1 := newDeployment("d1", 1, nil, nil, nil, map[string]string{"foo": "bar"})
	d2 := newDeployment("d2", 1, nil, nil, nil, map[string]string{"foo": "bar"})

	rs := newReplicaSet(d1, "rs1", 1)

	f.dLister = append(f.dLister, d1, d2)
	f.rsLister = append(f.rsLister, rs)
	f.objects = append(f.objects, d1, d2, rs)

	// 只创建夹具而不启动，避免后台产生额外动作。
	dc, _, err := f.newController(ctx)
	if err != nil {
		t.Fatalf("error creating Deployment controller: %v", err)
	}

	// 移除 ControllerRef，期望所有匹配的控制器都同步该孤儿对象。
	prev := *rs
	next := *rs
	next.OwnerReferences = nil
	bumpResourceVersion(&next)
	dc.updateReplicaSet(logger, &prev, &next)
	if got, want := dc.queue.Len(), 2; got != want {
		t.Fatalf("queue.Len() = %v, want %v", got, want)
	}
}

// TestDeleteReplicaSet 验证 ReplicaSet 删除事件会把其所属 Deployment 加入队列。
func TestDeleteReplicaSet(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)

	f := newFixture(t)

	d1 := newDeployment("d1", 1, nil, nil, nil, map[string]string{"foo": "bar"})
	d2 := newDeployment("d2", 1, nil, nil, nil, map[string]string{"foo": "bar"})

	// 两个 ReplicaSet 的标签同时匹配两个 Deployment，
	// 但通过 ControllerRef 明确区分归属。
	rs1 := newReplicaSet(d1, "rs1", 1)
	rs2 := newReplicaSet(d2, "rs2", 1)

	f.dLister = append(f.dLister, d1, d2)
	f.rsLister = append(f.rsLister, rs1, rs2)
	f.objects = append(f.objects, d1, d2, rs1, rs2)

	// 只创建夹具而不启动，避免后台产生额外动作。
	dc, _, err := f.newController(ctx)
	if err != nil {
		t.Fatalf("error creating Deployment controller: %v", err)
	}

	dc.deleteReplicaSet(logger, rs1)
	if got, want := dc.queue.Len(), 1; got != want {
		t.Fatalf("queue.Len() = %v, want %v", got, want)
	}
	key, done := dc.queue.Get()
	if key == "" || done {
		t.Fatalf("failed to enqueue controller for rs %v", rs1.Name)
	}
	expectedKey, _ := controller.KeyFunc(d1)
	if got, want := key, expectedKey; got != want {
		t.Errorf("queue.Get() = %v, want %v", got, want)
	}

	dc.deleteReplicaSet(logger, rs2)
	if got, want := dc.queue.Len(), 1; got != want {
		t.Fatalf("queue.Len() = %v, want %v", got, want)
	}
	key, done = dc.queue.Get()
	if key == "" || done {
		t.Fatalf("failed to enqueue controller for rs %v", rs2.Name)
	}
	expectedKey, _ = controller.KeyFunc(d2)
	if got, want := key, expectedKey; got != want {
		t.Errorf("queue.Get() = %v, want %v", got, want)
	}
}

// TestDeleteReplicaSetOrphan 验证孤儿 ReplicaSet 被删除时不会让任何 Deployment 入队。
func TestDeleteReplicaSetOrphan(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)

	f := newFixture(t)

	d1 := newDeployment("d1", 1, nil, nil, nil, map[string]string{"foo": "bar"})
	d2 := newDeployment("d2", 1, nil, nil, nil, map[string]string{"foo": "bar"})

	// 将该 RS 变为孤儿，期望所有匹配的 Deployment 都会入队。
	rs := newReplicaSet(d1, "rs1", 1)
	rs.OwnerReferences = nil

	f.dLister = append(f.dLister, d1, d2)
	f.rsLister = append(f.rsLister, rs)
	f.objects = append(f.objects, d1, d2, rs)

	// 只创建夹具而不启动，避免后台产生额外动作。
	dc, _, err := f.newController(ctx)
	if err != nil {
		t.Fatalf("error creating Deployment controller: %v", err)
	}

	dc.deleteReplicaSet(logger, rs)
	if got, want := dc.queue.Len(), 0; got != want {
		t.Fatalf("queue.Len() = %v, want %v", got, want)
	}
}

// BenchmarkGetPodMapForDeployment 测量不同 Pod 规模与噪声量下，
// 构建 Deployment 的 Pod 索引映射的性能。
func BenchmarkGetPodMapForDeployment(b *testing.B) {
	cases := []struct {
		name      string
		numPods   int
		extraPods int // 同命名空间下不属于该 Deployment 的 Pod 数量
	}{
		{name: "10-Pods-No-Noise", numPods: 10, extraPods: 0},
		{name: "10-Pods-With-Noise", numPods: 10, extraPods: 100},
		{name: "10000-Pods-No-Noise", numPods: 10000, extraPods: 0},
		{name: "10000-Pods-With-Noise", numPods: 10000, extraPods: 100000},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			_, ctx := ktesting.NewTestContext(b)

			f := newFixture(b)

			d := newDeployment("foo", 1, nil, nil, nil, map[string]string{"foo": "bar"})

			rs1 := newReplicaSet(d, "rs1", 1)
			rs2 := newReplicaSet(d, "rs2", 1)

			var pods []*v1.Pod
			var objects []runtime.Object
			for i := 0; i < tc.numPods; i++ {
				p1, p2 := generatePodFromRS(rs1), generatePodFromRS(rs2)
				p1.Name, p2.Name = p1.Name+fmt.Sprintf("-%d", i), p2.Name+fmt.Sprintf("-%d", i)
				pods = append(pods, p1, p2)
				objects = append(objects, p1, p2)
			}

			// 添加不属于该 Deployment 的噪声 Pod。
			for i := 0; i < tc.extraPods; i++ {
				p := generatePodFromRS(rs1)
				p.Name = fmt.Sprintf("extra-pod-%d", i)
				p.OwnerReferences = nil // 无主/无关
				p.Labels = map[string]string{"foo": "notbar"}
				pods = append(pods, p)
				objects = append(objects, p)
			}

			f.dLister = append(f.dLister, d)
			f.rsLister = append(f.rsLister, rs1, rs2)
			f.podLister = append(f.podLister, pods...)
			f.objects = append(f.objects, d, rs1, rs2)
			f.objects = append(f.objects, objects...)

			// 启动夹具。
			c, informers, err := f.newController(ctx)
			if err != nil {
				b.Fatalf("error creating Deployment controller: %v", err)
			}
			stopCh := make(chan struct{})
			defer close(stopCh)
			informers.Start(stopCh)

			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				m, err := c.getPodMapForDeployment(d, f.rsLister)
				if err != nil {
					b.Fatalf("getPodMapForDeployment() error: %v", err)
				}
				if len(m) != 2 {
					b.Errorf("Invalid map size, expected 2, got: %d", len(m))
				}
			}
		})
	}
}

// bumpResourceVersion 把对象的 ResourceVersion 加一，
// 用于模拟一次真实更新，避免被 resync 的相同版本号判断忽略。
func bumpResourceVersion(obj metav1.Object) {
	ver, _ := strconv.ParseInt(obj.GetResourceVersion(), 10, 32)
	obj.SetResourceVersion(strconv.FormatInt(ver+1, 10))
}

// generatePodFromRS 根据给定 ReplicaSet 的选择器、模板和归属关系构造一个 Pod。
func generatePodFromRS(rs *apps.ReplicaSet) *v1.Pod {
	trueVar := true
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rs.Name + "-pod",
			Namespace: rs.Namespace,
			Labels:    rs.Spec.Selector.MatchLabels,
			OwnerReferences: []metav1.OwnerReference{
				{UID: rs.UID, APIVersion: "v1beta1", Kind: "ReplicaSet", Name: rs.Name, Controller: &trueVar},
			},
		},
		Spec: rs.Spec.Template.Spec,
	}
}
