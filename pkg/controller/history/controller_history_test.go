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

package history

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	clientscheme "k8s.io/client-go/kubernetes/scheme"
	core "k8s.io/client-go/testing"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/controller"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/utils/ptr"
)

// TestRealHistory_ListControllerRevisions 验证 History 真实实现按父对象与标签选择器列出修订，
// 且不会混入其他父对象的修订。
func TestRealHistory_ListControllerRevisions(t *testing.T) {
	// testcase 描述一次列表查询的输入与预期结果。
	type testcase struct {
		// name 用例名称。
		name string
		// parent 用于过滤的父对象。
		parent metav1.Object
		// selector 用于过滤修订的标签选择器。
		selector labels.Selector
		// revisions 预置到 informer 索引器中的修订。
		revisions []*apps.ControllerRevision
		// want 期望返回的修订名称集合（以 map 模拟集合）。
		want map[string]bool
	}
	// testFn 执行单个用例：启动 informer、灌入修订并校验列表结果。
	testFn := func(test *testcase, t *testing.T) {
		client := fake.NewSimpleClientset()
		informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())

		// 用 stop 通道控制 informer 生命周期，延迟关闭避免 goroutine 泄漏。
		stop := make(chan struct{})
		defer close(stop)
		informerFactory.Start(stop)
		informer := informerFactory.Apps().V1().ControllerRevisions()
		if err := AddControllerRevisionControllerIndexer(informer.Informer()); err != nil {
			t.Fatalf("failed to add indexer: %v", err)
		}
		informerFactory.WaitForCacheSync(stop)
		// 直接写入索引器绕过 API Server，使用例聚焦于列表过滤逻辑。
		for i := range test.revisions {
			informer.Informer().GetIndexer().Add(test.revisions[i])
		}

		history := NewHistory(client, informer.Lister(), informer.Informer().GetIndexer())
		revisions, err := history.ListControllerRevisions(test.parent, parentKind, test.selector)
		if err != nil {
			t.Errorf("%s: %s", test.name, err)
		}
		// 转为名称集合，避免受返回顺序影响。
		got := make(map[string]bool)
		for i := range revisions {
			got[revisions[i].Name] = true
		}
		if !reflect.DeepEqual(test.want, got) {
			t.Errorf("%s: want %v got %v", test.name, test.want, got)
		}
	}
	// 构造两个父对象及多个修订，覆盖同父对象、不同父对象与孤儿修订三种场景。
	ss1 := newStatefulSet(3, "ss1", types.UID("ss1"), map[string]string{"foo": "bar"})
	ss2 := newStatefulSet(3, "ss2", types.UID("ss2"), map[string]string{"goo": "car"})
	sel1, err := metav1.LabelSelectorAsSelector(ss1.Spec.Selector)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev1, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev1.Namespace = ss1.Namespace
	ss1Rev2, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev2.Namespace = ss1.Namespace
	ss2Rev1, err := NewControllerRevision(ss2, parentKind, ss2.Spec.Template.Labels, rawTemplate(&ss2.Spec.Template), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	ss2Rev1.Namespace = ss2.Namespace
	// 清空属主引用构造孤儿修订，用于验证孤儿也会被列出。
	ss1Orphan, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	ss1Orphan.Namespace = ss1.Namespace
	ss1Orphan.OwnerReferences = nil

	tests := []testcase{
		{
			name:      "selects none",
			parent:    &ss1.ObjectMeta,
			selector:  sel1,
			revisions: nil,
			want:      map[string]bool{},
		},
		{
			name:      "selects all",
			parent:    &ss1.ObjectMeta,
			selector:  sel1,
			revisions: []*apps.ControllerRevision{ss1Rev1, ss1Rev2},
			want:      map[string]bool{ss1Rev1.Name: true, ss1Rev2.Name: true},
		},
		{
			name:      "doesn't select another Objects history",
			parent:    &ss1.ObjectMeta,
			selector:  sel1,
			revisions: []*apps.ControllerRevision{ss1Rev1, ss1Rev2, ss2Rev1},
			want:      map[string]bool{ss1Rev1.Name: true, ss1Rev2.Name: true},
		},
		{
			name:      "selects orphans",
			parent:    &ss1.ObjectMeta,
			selector:  sel1,
			revisions: []*apps.ControllerRevision{ss1Rev1, ss1Rev2, ss1Orphan},
			want:      map[string]bool{ss1Rev1.Name: true, ss1Rev2.Name: true, ss1Orphan.Name: true},
		},
	}
	for i := range tests {
		testFn(&tests[i], t)
	}
}

// TestFakeHistory_ListControllerRevisions 验证 FakeHistory 按父对象与标签选择器列出修订，
// 覆盖空结果、同父对象全部、排除其他父对象与包含孤儿等场景。
func TestFakeHistory_ListControllerRevisions(t *testing.T) {
	// testcase 描述一次列表查询的输入与预期结果。
	type testcase struct {
		// name 用例名称。
		name string
		// parent 用于过滤的父对象。
		parent metav1.Object
		// selector 用于过滤修订的标签选择器。
		selector labels.Selector
		// revisions 预置到 informer 索引器中的修订。
		revisions []*apps.ControllerRevision
		// want 期望返回的修订名称集合（以 map 模拟集合）。
		want map[string]bool
	}
	// testFn 执行单个用例：启动 informer、灌入修订并校验列表结果。
	testFn := func(test *testcase, t *testing.T) {
		client := fake.NewSimpleClientset()
		informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())

		// 用 stop 通道控制 informer 生命周期，延迟关闭避免 goroutine 泄漏。
		stop := make(chan struct{})
		defer close(stop)
		informerFactory.Start(stop)
		informer := informerFactory.Apps().V1().ControllerRevisions()
		if err := AddControllerRevisionControllerIndexer(informer.Informer()); err != nil {
			t.Fatalf("failed to add indexer: %v", err)
		}
		informerFactory.WaitForCacheSync(stop)
		// 直接写入索引器绕过 API Server，使用例聚焦于列表过滤逻辑。
		for i := range test.revisions {
			informer.Informer().GetIndexer().Add(test.revisions[i])
		}

		history := NewFakeHistory(informer)
		revisions, err := history.ListControllerRevisions(test.parent, parentKind, test.selector)
		if err != nil {
			t.Errorf("%s: %s", test.name, err)
		}
		// 转为名称集合，避免受返回顺序影响。
		got := make(map[string]bool)
		for i := range revisions {
			got[revisions[i].Name] = true
		}
		if !reflect.DeepEqual(test.want, got) {
			t.Errorf("%s: want %v got %v", test.name, test.want, got)
		}
	}
	// 构造两个父对象及多个修订，覆盖不同过滤场景。
	ss1 := newStatefulSet(3, "ss1", types.UID("ss1"), map[string]string{"foo": "bar"})
	ss2 := newStatefulSet(3, "ss2", types.UID("ss2"), map[string]string{"goo": "car"})
	sel1, err := metav1.LabelSelectorAsSelector(ss1.Spec.Selector)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev1, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev1.Namespace = ss1.Namespace
	ss1Rev2, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev2.Namespace = ss1.Namespace
	ss2Rev1, err := NewControllerRevision(ss2, parentKind, ss2.Spec.Template.Labels, rawTemplate(&ss2.Spec.Template), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	ss2Rev1.Namespace = ss2.Namespace
	// 清空属主引用构造孤儿修订，用于验证孤儿也会被列出。
	ss1Orphan, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	ss1Orphan.Namespace = ss1.Namespace
	ss1Orphan.OwnerReferences = nil

	tests := []testcase{
		{
			name:      "selects none",
			parent:    &ss1.ObjectMeta,
			selector:  sel1,
			revisions: nil,
			want:      map[string]bool{},
		},
		{
			name:      "selects all",
			parent:    &ss1.ObjectMeta,
			selector:  sel1,
			revisions: []*apps.ControllerRevision{ss1Rev1, ss1Rev2},
			want:      map[string]bool{ss1Rev1.Name: true, ss1Rev2.Name: true},
		},
		{
			name:      "doesn't select another Objects history",
			parent:    &ss1.ObjectMeta,
			selector:  sel1,
			revisions: []*apps.ControllerRevision{ss1Rev1, ss1Rev2, ss2Rev1},
			want:      map[string]bool{ss1Rev1.Name: true, ss1Rev2.Name: true},
		},
		{
			name:      "selects orphans",
			parent:    &ss1.ObjectMeta,
			selector:  sel1,
			revisions: []*apps.ControllerRevision{ss1Rev1, ss1Rev2, ss1Orphan},
			want:      map[string]bool{ss1Rev1.Name: true, ss1Rev2.Name: true, ss1Orphan.Name: true},
		},
	}
	for i := range tests {
		testFn(&tests[i], t)
	}
}

// TestRealHistory_CreateControllerRevision 验证真实 History 创建修订：正常创建、不同父对象不冲突、
// 名称冲突时重命名并递增冲突计数。
func TestRealHistory_CreateControllerRevision(t *testing.T) {
	// testcase 描述一次创建操作的输入与预期行为。
	type testcase struct {
		// name 用例名称。
		name string
		// parent 待创建修订所属的父对象。
		parent metav1.Object
		// revision 待创建的修订。
		revision *apps.ControllerRevision
		// existing 预置的修订及其父对象，用于构造名称冲突场景。
		existing []struct {
			parent   metav1.Object
			revision *apps.ControllerRevision
		}
		// rename 是否期望因名称冲突而重命名。
		rename bool
	}
	// testFn 执行单个用例：预置修订、创建目标修订并校验名称与冲突计数。
	testFn := func(test *testcase, t *testing.T) {
		client := fake.NewSimpleClientset()
		informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
		stop := make(chan struct{})
		defer close(stop)
		informerFactory.Start(stop)
		informer := informerFactory.Apps().V1().ControllerRevisions()
		if err := AddControllerRevisionControllerIndexer(informer.Informer()); err != nil {
			t.Fatalf("failed to add indexer: %v", err)
		}
		informerFactory.WaitForCacheSync(stop)
		history := NewHistory(client, informer.Lister(), informer.Informer().GetIndexer())

		// 冲突计数在用例内共享，用于验证冲突处理逻辑。
		var collisionCount int32
		for _, item := range test.existing {
			_, err := client.AppsV1().ControllerRevisions(item.parent.GetNamespace()).Create(context.TODO(), item.revision, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
		}
		// 创建待测修订前清零冲突计数，避免前序用例状态干扰。
		collisionCount = 0
		created, err := history.CreateControllerRevision(test.parent, test.revision, &collisionCount)
		if err != nil {
			t.Errorf("%s: %s", test.name, err)
		}

		// 仅在预期冲突时校验重命名与冲突计数。
		if test.rename {
			if created.Name == test.revision.Name {
				t.Errorf("%s: wanted rename got %s %s", test.name, created.Name, test.revision.Name)
			}
			expectedName := ControllerRevisionName(test.parent.GetName(), HashControllerRevision(test.revision, &collisionCount))
			if created.Name != expectedName {
				t.Errorf("%s: on name collision wanted new name %s got %s", test.name, expectedName, created.Name)
			}

			// 第二次名称冲突由完全相同的修订引起，无需额外处理。
			_, err = history.CreateControllerRevision(test.parent, test.revision, &collisionCount)
			if err != nil {
				t.Errorf("%s: %s", test.name, err)
			}
			if collisionCount != 1 {
				t.Errorf("%s: on second name collision wanted collisionCount 1 got %d", test.name, collisionCount)
			}
		}
		if !test.rename && created.Name != test.revision.Name {
			t.Errorf("%s: wanted %s got %s", test.name, test.revision.Name, created.Name)
		}
	}
	ss1 := newStatefulSet(3, "ss1", types.UID("ss1"), map[string]string{"foo": "bar"})
	ss1.Status.CollisionCount = new(int32)
	ss2 := newStatefulSet(3, "ss2", types.UID("ss2"), map[string]string{"goo": "car"})
	ss2.Status.CollisionCount = new(int32)
	ss1Rev1, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 1, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev1.Namespace = ss1.Namespace

	// 构造一个与已有修订同名且哈希标签相同、但模板不同的新修订。
	// 真实场景中这可能由哈希碰撞引发；此处通过显式设置名称和哈希标签
	// 为已被占用的值来模拟该情况。
	modTemplate := ss1.Spec.Template.DeepCopy()
	modTemplate.Labels["foo"] = "not_bar"
	ss1Rev2, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(modTemplate), 2, ss1.Status.CollisionCount)
	ss1Rev2.Name = ss1Rev1.Name
	ss1Rev2.Labels[ControllerRevisionHashLabel] = ss1Rev1.Labels[ControllerRevisionHashLabel]
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev2.Namespace = ss1.Namespace
	ss2Rev1, err := NewControllerRevision(ss2, parentKind, ss2.Spec.Template.Labels, rawTemplate(&ss2.Spec.Template), 1, ss2.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss2Rev1.Namespace = ss2.Namespace
	tests := []testcase{
		{
			name:     "creates new",
			parent:   &ss1.ObjectMeta,
			revision: ss1Rev1,
			existing: nil,

			rename: false,
		},
		{
			name:     "create doesn't conflict when parents differ",
			parent:   &ss2.ObjectMeta,
			revision: ss2Rev1,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss1,
					revision: ss1Rev1,
				},
			},

			rename: false,
		},
		{
			name:     "create renames on conflict",
			parent:   &ss1.ObjectMeta,
			revision: ss1Rev1,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss1,
					revision: ss1Rev2,
				},
			},
			rename: true,
		},
	}
	for i := range tests {
		testFn(&tests[i], t)
	}
}

// TestFakeHistory_CreateControllerRevision 验证 FakeHistory 创建修订及冲突重命名逻辑与真实实现一致。
func TestFakeHistory_CreateControllerRevision(t *testing.T) {
	// testcase 描述一次创建操作的输入与预期行为。
	type testcase struct {
		// name 用例名称。
		name string
		// parent 待创建修订所属的父对象。
		parent metav1.Object
		// revision 待创建的修订。
		revision *apps.ControllerRevision
		// existing 预置的修订及其父对象，用于构造名称冲突场景。
		existing []struct {
			parent   metav1.Object
			revision *apps.ControllerRevision
		}
		// rename 是否期望因名称冲突而重命名。
		rename bool
	}
	// testFn 执行单个用例：预置修订、创建目标修订并校验名称与冲突计数。
	testFn := func(test *testcase, t *testing.T) {
		client := fake.NewSimpleClientset()
		informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())

		stop := make(chan struct{})
		defer close(stop)
		informerFactory.Start(stop)
		informer := informerFactory.Apps().V1().ControllerRevisions()
		if err := AddControllerRevisionControllerIndexer(informer.Informer()); err != nil {
			t.Fatalf("failed to add indexer: %v", err)
		}
		informerFactory.WaitForCacheSync(stop)
		history := NewFakeHistory(informer)

		// 冲突计数在用例内共享，用于验证冲突处理逻辑。
		var collisionCount int32
		for i := range test.existing {
			_, err := history.CreateControllerRevision(test.existing[i].parent, test.existing[i].revision, &collisionCount)
			if err != nil {
				t.Fatal(err)
			}
		}
		// 创建待测修订前清零冲突计数，避免前序用例状态干扰。
		collisionCount = 0
		created, err := history.CreateControllerRevision(test.parent, test.revision, &collisionCount)
		if err != nil {
			t.Errorf("%s: %s", test.name, err)
		}

		if test.rename {
			if created.Name == test.revision.Name {
				t.Errorf("%s: wanted rename got %s %s", test.name, created.Name, test.revision.Name)
			}
			expectedName := ControllerRevisionName(test.parent.GetName(), HashControllerRevision(test.revision, &collisionCount))
			if created.Name != expectedName {
				t.Errorf("%s: on name collision wanted new name %s got %s", test.name, expectedName, created.Name)
			}

			// 第二次名称冲突应将冲突计数递增到 2。
			_, err = history.CreateControllerRevision(test.parent, test.revision, &collisionCount)
			if err != nil {
				t.Errorf("%s: %s", test.name, err)
			}
			if collisionCount != 2 {
				t.Errorf("%s: on second name collision wanted collisionCount 1 got %d", test.name, collisionCount)
			}
		}
		if !test.rename && created.Name != test.revision.Name {
			t.Errorf("%s: wanted %s got %s", test.name, test.revision.Name, created.Name)
		}
	}
	ss1 := newStatefulSet(3, "ss1", types.UID("ss1"), map[string]string{"foo": "bar"})
	ss1.Status.CollisionCount = new(int32)
	ss2 := newStatefulSet(3, "ss2", types.UID("ss2"), map[string]string{"goo": "car"})
	ss2.Status.CollisionCount = new(int32)
	ss1Rev1, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 1, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev1.Namespace = ss1.Namespace
	ss1Rev2, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 2, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev2.Namespace = ss1.Namespace
	ss2Rev1, err := NewControllerRevision(ss2, parentKind, ss2.Spec.Template.Labels, rawTemplate(&ss2.Spec.Template), 1, ss2.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss2Rev1.Namespace = ss2.Namespace
	tests := []testcase{
		{
			name:     "creates new",
			parent:   &ss1.ObjectMeta,
			revision: ss1Rev1,
			existing: nil,

			rename: false,
		},
		{
			name:     "create doesn't conflict when parents differ",
			parent:   &ss2.ObjectMeta,
			revision: ss2Rev1,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss1,
					revision: ss1Rev1,
				},
			},

			rename: false,
		},
		{
			name:     "create renames on conflict",
			parent:   &ss1.ObjectMeta,
			revision: ss1Rev1,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss1,
					revision: ss1Rev1,
				},
			},
			rename: true,
		},
	}
	for i := range tests {
		testFn(&tests[i], t)
	}
}

// TestRealHistory_UpdateControllerRevision 验证真实 History 更新修订：成功更新、无变化时成功、
// 内部错误时失败、冲突时重试成功。
func TestRealHistory_UpdateControllerRevision(t *testing.T) {
	// 记录冲突重试次数，供反应器与用例间共享。
	conflictAttempts := 0
	// testcase 描述一次更新操作的输入与预期结果。
	type testcase struct {
		// name 用例名称。
		name string
		// revision 待更新的修订。
		revision *apps.ControllerRevision
		// newRevision 目标修订号。
		newRevision int64
		// existing 预置的修订及其父对象。
		existing []struct {
			parent   metav1.Object
			revision *apps.ControllerRevision
		}
		// reactor 注入到 fake client 的反应器，用于模拟冲突或内部错误。
		reactor core.ReactionFunc
		// err 是否期望返回错误。
		err bool
	}
	// conflictSuccess 在前两次更新时返回冲突、第三次放行，用于验证重试逻辑。
	conflictSuccess := func(action core.Action) (bool, runtime.Object, error) {
		defer func() {
			conflictAttempts++
		}()
		switch action.(type) {

		case core.UpdateActionImpl:
			update := action.(core.UpdateAction)
			if conflictAttempts < 2 {
				return true, update.GetObject(), errors.NewConflict(update.GetResource().GroupResource(), "", fmt.Errorf("conflict"))
			}
			return true, update.GetObject(), nil
		default:
			return false, nil, nil
		}
	}
	// internalError 对更新操作返回内部错误，用于验证错误传播。
	internalError := func(action core.Action) (bool, runtime.Object, error) {
		switch action.(type) {
		case core.UpdateActionImpl:
			return true, nil, errors.NewInternalError(fmt.Errorf("internal error"))
		default:
			return false, nil, nil
		}
	}

	// testFn 执行单个用例：预置修订、注入反应器并校验更新结果。
	testFn := func(test *testcase, t *testing.T) {
		client := fake.NewSimpleClientset()

		informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
		stop := make(chan struct{})
		defer close(stop)
		informerFactory.Start(stop)
		informer := informerFactory.Apps().V1().ControllerRevisions()
		if err := AddControllerRevisionControllerIndexer(informer.Informer()); err != nil {
			t.Fatalf("failed to add indexer: %v", err)
		}
		informerFactory.WaitForCacheSync(stop)
		history := NewHistory(client, informer.Lister(), informer.Informer().GetIndexer())
		var collisionCount int32
		for i := range test.existing {
			_, err := history.CreateControllerRevision(test.existing[i].parent, test.existing[i].revision, &collisionCount)
			if err != nil {
				t.Fatal(err)
			}
		}
		// 在预置修订后再注入反应器，避免影响初始化过程。
		if test.reactor != nil {
			client.PrependReactor("*", "*", test.reactor)
		}
		updated, err := history.UpdateControllerRevision(test.revision, test.newRevision)
		if !test.err && err != nil {
			t.Errorf("%s: %s", test.name, err)
		}
		if !test.err && updated.Revision != test.newRevision {
			t.Errorf("%s: got %d want %d", test.name, updated.Revision, test.newRevision)
		}
		if test.err && err == nil {
			t.Errorf("%s: expected error", test.name)
		}
	}
	ss1 := newStatefulSet(3, "ss1", types.UID("ss1"), map[string]string{"foo": "bar"})
	ss1.Status.CollisionCount = new(int32)
	ss1Rev1, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 1, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev1.Namespace = ss1.Namespace
	ss1Rev2, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 2, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev2.Namespace = ss1.Namespace

	tests := []testcase{
		{
			name:        "update succeeds",
			revision:    ss1Rev1,
			newRevision: ss1Rev1.Revision + 1,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss1,
					revision: ss1Rev1,
				},
			},
			reactor: nil,
			err:     false,
		},
		{
			name:        "update succeeds no noop",
			revision:    ss1Rev1,
			newRevision: ss1Rev1.Revision,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss1,
					revision: ss1Rev1,
				},
			},
			reactor: nil,
			err:     false,
		}, {
			name:        "update fails on error",
			revision:    ss1Rev1,
			newRevision: ss1Rev1.Revision + 10,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss1,
					revision: ss1Rev1,
				},
			},
			reactor: internalError,
			err:     true,
		},
		{
			name:        "update on succeeds on conflict",
			revision:    ss1Rev1,
			newRevision: ss1Rev1.Revision + 1,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss1,
					revision: ss1Rev1,
				},
			},
			reactor: conflictSuccess,
			err:     false,
		},
	}
	for i := range tests {
		// 每个用例前重置重试计数，确保反应器计数相互独立。
		conflictAttempts = 0
		testFn(&tests[i], t)
	}
}

// TestFakeHistory_UpdateControllerRevision 验证 FakeHistory 更新修订的成功路径，包括修订号无变化的场景。
func TestFakeHistory_UpdateControllerRevision(t *testing.T) {
	// testcase 描述一次更新操作的输入与预期结果。
	type testcase struct {
		// name 用例名称。
		name string
		// revision 待更新的修订。
		revision *apps.ControllerRevision
		// newRevision 目标修订号。
		newRevision int64
		// existing 预置的修订及其父对象。
		existing []struct {
			parent   metav1.Object
			revision *apps.ControllerRevision
		}
		// err 是否期望返回错误。
		err bool
	}

	// testFn 执行单个用例：预置修订并校验更新结果。
	testFn := func(test *testcase, t *testing.T) {
		client := fake.NewSimpleClientset()

		informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
		stop := make(chan struct{})
		defer close(stop)
		informerFactory.Start(stop)
		informer := informerFactory.Apps().V1().ControllerRevisions()
		if err := AddControllerRevisionControllerIndexer(informer.Informer()); err != nil {
			t.Fatalf("failed to add indexer: %v", err)
		}
		informerFactory.WaitForCacheSync(stop)
		history := NewFakeHistory(informer)
		var collisionCount int32
		for i := range test.existing {
			_, err := history.CreateControllerRevision(test.existing[i].parent, test.existing[i].revision, &collisionCount)
			if err != nil {
				t.Fatal(err)
			}
		}
		updated, err := history.UpdateControllerRevision(test.revision, test.newRevision)
		if !test.err && err != nil {
			t.Errorf("%s: %s", test.name, err)
		}
		if !test.err && updated.Revision != test.newRevision {
			t.Errorf("%s: got %d want %d", test.name, updated.Revision, test.newRevision)
		}
		if test.err && err == nil {
			t.Errorf("%s: expected error", test.name)
		}
	}
	ss1 := newStatefulSet(3, "ss1", types.UID("ss1"), map[string]string{"foo": "bar"})
	ss1.Status.CollisionCount = new(int32)

	ss1Rev1, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 1, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev1.Namespace = ss1.Namespace
	ss1Rev2, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 2, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev2.Namespace = ss1.Namespace
	tests := []testcase{
		{
			name:        "update succeeds",
			revision:    ss1Rev1,
			newRevision: ss1Rev1.Revision + 1,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss1,
					revision: ss1Rev1,
				},
			},
			err: false,
		},
		{
			name:        "update succeeds no noop",
			revision:    ss1Rev1,
			newRevision: ss1Rev1.Revision,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss1,
					revision: ss1Rev1,
				},
			},
			err: false,
		},
	}
	for i := range tests {
		testFn(&tests[i], t)
	}
}

// TestRealHistory_DeleteControllerRevision 验证真实 History 删除修订：空集群删除失败、存在则成功、不存在则失败。
func TestRealHistory_DeleteControllerRevision(t *testing.T) {
	// testcase 描述一次删除操作的输入与预期结果。
	type testcase struct {
		// name 用例名称。
		name string
		// revision 待删除的修订。
		revision *apps.ControllerRevision
		// existing 预置的修订及其父对象。
		existing []struct {
			parent   metav1.Object
			revision *apps.ControllerRevision
		}
		// err 是否期望返回错误。
		err bool
	}
	// testFn 执行单个用例：预置修订、删除目标修订并校验错误。
	testFn := func(test *testcase, t *testing.T) {
		client := fake.NewSimpleClientset()
		informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())

		stop := make(chan struct{})
		defer close(stop)
		informerFactory.Start(stop)
		informer := informerFactory.Apps().V1().ControllerRevisions()
		if err := AddControllerRevisionControllerIndexer(informer.Informer()); err != nil {
			t.Fatalf("failed to add indexer: %v", err)
		}
		informerFactory.WaitForCacheSync(stop)
		history := NewHistory(client, informer.Lister(), informer.Informer().GetIndexer())
		var collisionCount int32
		for i := range test.existing {
			_, err := history.CreateControllerRevision(test.existing[i].parent, test.existing[i].revision, &collisionCount)
			if err != nil {
				t.Fatal(err)
			}
		}
		err := history.DeleteControllerRevision(test.revision)
		if !test.err && err != nil {
			t.Errorf("%s: %s", test.name, err)
		}
		if test.err && err == nil {
			t.Errorf("%s: expected error", test.name)
		}
	}
	ss1 := newStatefulSet(3, "ss1", types.UID("ss1"), map[string]string{"foo": "bar"})
	ss1.Status.CollisionCount = new(int32)
	ss2 := newStatefulSet(3, "ss2", types.UID("ss2"), map[string]string{"goo": "car"})
	ss2.Status.CollisionCount = new(int32)
	ss1Rev1, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 1, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev1.Namespace = ss1.Namespace
	ss1Rev2, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 2, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev2.Namespace = ss1.Namespace
	ss2Rev1, err := NewControllerRevision(ss2, parentKind, ss2.Spec.Template.Labels, rawTemplate(&ss2.Spec.Template), 1, ss2.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss2Rev1.Namespace = ss2.Namespace
	ss2Rev2, err := NewControllerRevision(ss2, parentKind, ss2.Spec.Template.Labels, rawTemplate(&ss2.Spec.Template), 2, ss2.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss2Rev2.Namespace = ss2.Namespace
	tests := []testcase{
		{
			name:     "delete empty fails",
			revision: ss1Rev1,
			existing: nil,
			err:      true,
		},
		{
			name:     "delete existing succeeds",
			revision: ss1Rev1,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss1,
					revision: ss1Rev1,
				},
			},
			err: false,
		}, {
			name:     "delete non-existing fails",
			revision: ss1Rev1,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss2,
					revision: ss2Rev1,
				},
				{
					parent:   ss2,
					revision: ss2Rev2,
				},
			},
			err: true,
		},
	}
	for i := range tests {
		testFn(&tests[i], t)
	}
}

// TestFakeHistory_DeleteControllerRevision 验证 FakeHistory 删除修订的成功与失败路径。
func TestFakeHistory_DeleteControllerRevision(t *testing.T) {
	// testcase 描述一次删除操作的输入与预期结果。
	type testcase struct {
		// name 用例名称。
		name string
		// revision 待删除的修订。
		revision *apps.ControllerRevision
		// existing 预置的修订及其父对象。
		existing []struct {
			parent   metav1.Object
			revision *apps.ControllerRevision
		}
		// err 是否期望返回错误。
		err bool
	}
	// testFn 执行单个用例：预置修订、删除目标修订并校验错误。
	testFn := func(test *testcase, t *testing.T) {
		client := fake.NewSimpleClientset()
		informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())

		stop := make(chan struct{})
		defer close(stop)
		informerFactory.Start(stop)
		informer := informerFactory.Apps().V1().ControllerRevisions()
		if err := AddControllerRevisionControllerIndexer(informer.Informer()); err != nil {
			t.Fatalf("failed to add indexer: %v", err)
		}
		informerFactory.WaitForCacheSync(stop)
		history := NewFakeHistory(informer)
		var collisionCount int32
		for i := range test.existing {
			_, err := history.CreateControllerRevision(test.existing[i].parent, test.existing[i].revision, &collisionCount)
			if err != nil {
				t.Fatal(err)
			}
		}
		err := history.DeleteControllerRevision(test.revision)
		if !test.err && err != nil {
			t.Errorf("%s: %s", test.name, err)
		}
		if test.err && err == nil {
			t.Errorf("%s: expected error", test.name)
		}
	}
	ss1 := newStatefulSet(3, "ss1", types.UID("ss1"), map[string]string{"foo": "bar"})
	ss1.Status.CollisionCount = new(int32)
	ss2 := newStatefulSet(3, "ss2", types.UID("ss2"), map[string]string{"goo": "car"})
	ss2.Status.CollisionCount = new(int32)
	ss1Rev1, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 1, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev1.Namespace = ss1.Namespace
	ss1Rev2, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 2, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev2.Namespace = ss1.Namespace
	ss2Rev1, err := NewControllerRevision(ss2, parentKind, ss2.Spec.Template.Labels, rawTemplate(&ss2.Spec.Template), 1, ss2.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss2Rev1.Namespace = ss2.Namespace
	ss2Rev2, err := NewControllerRevision(ss2, parentKind, ss2.Spec.Template.Labels, rawTemplate(&ss2.Spec.Template), 2, ss2.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss2Rev2.Namespace = ss2.Namespace
	tests := []testcase{
		{
			name:     "delete empty fails",
			revision: ss1Rev1,
			existing: nil,
			err:      true,
		},
		{
			name:     "delete existing succeeds",
			revision: ss1Rev1,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss1,
					revision: ss1Rev1,
				},
			},
			err: false,
		}, {
			name:     "delete non-existing fails",
			revision: ss1Rev1,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss2,
					revision: ss2Rev1,
				},
				{
					parent:   ss2,
					revision: ss2Rev2,
				},
			},
			err: true,
		},
	}
	for i := range tests {
		testFn(&tests[i], t)
	}
}

// TestRealHistory_AdoptControllerRevision 验证真实 History 收养修订：孤儿可收养、已被其他对象拥有的修订拒绝、
// 不存在的修订失败。
func TestRealHistory_AdoptControllerRevision(t *testing.T) {
	// testcase 描述一次收养操作的输入与预期结果。
	type testcase struct {
		// name 用例名称。
		name string
		// parent 期望收养修订的父对象。
		parent metav1.Object
		// revision 待收养的修订。
		revision *apps.ControllerRevision
		// existing 预置的修订及其父对象。
		existing []struct {
			parent   metav1.Object
			revision *apps.ControllerRevision
		}
		// err 是否期望返回错误。
		err bool
	}
	// testFn 执行单个用例：预置修订、注入 Patch 反应器并校验收养结果。
	testFn := func(test *testcase, t *testing.T) {
		client := fake.NewSimpleClientset()
		// 注入 Patch 反应器，在内存中应用 strategic merge patch，模拟服务端合并行为。
		client.AddReactor("*", "*", func(action core.Action) (bool, runtime.Object, error) {
			switch action := action.(type) {
			case core.PatchActionImpl:
				var found *apps.ControllerRevision
				for i := range test.existing {
					if test.revision.Name == test.existing[i].revision.Name &&
						test.revision.Namespace == test.existing[i].revision.Namespace {
						found = test.existing[i].revision
						break
					}
				}
				if found == nil {
					return true, nil, errors.NewNotFound(apps.Resource("controllerrevisions"), test.revision.Name)
				}
				// 用 strategic merge patch 将补丁应用到修订，模拟 API Server 的合并语义。
				b, err := strategicpatch.StrategicMergePatch(
					[]byte(runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(apps.SchemeGroupVersion), test.revision)),
					action.GetPatch(), test.revision)
				if err != nil {
					return true, nil, err
				}
				obj, err := runtime.Decode(clientscheme.Codecs.LegacyCodec(apps.SchemeGroupVersion), b)
				if err != nil {
					return true, nil, err
				}
				patched, err := legacyscheme.Scheme.ConvertToVersion(obj, apps.SchemeGroupVersion)
				if err != nil {
					return true, nil, err
				}
				return true, patched, err
			default:
				return false, nil, nil
			}

		})
		informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
		stop := make(chan struct{})
		defer close(stop)
		informerFactory.Start(stop)
		informer := informerFactory.Apps().V1().ControllerRevisions()
		if err := AddControllerRevisionControllerIndexer(informer.Informer()); err != nil {
			t.Fatalf("failed to add indexer: %v", err)
		}
		informerFactory.WaitForCacheSync(stop)

		history := NewHistory(client, informer.Lister(), informer.Informer().GetIndexer())
		var collisionCount int32
		for i := range test.existing {
			_, err := history.CreateControllerRevision(test.existing[i].parent, test.existing[i].revision, &collisionCount)
			if err != nil {
				t.Fatal(err)
			}
		}
		adopted, err := history.AdoptControllerRevision(test.parent, parentKind, test.revision)
		if !test.err && err != nil {
			t.Errorf("%s: %s", test.name, err)
		}
		if !test.err && !metav1.IsControlledBy(adopted, test.parent) {
			t.Errorf("%s: adoption failed", test.name)
		}
		if test.err && err == nil {
			t.Errorf("%s: expected error", test.name)
		}
	}

	ss1 := newStatefulSet(3, "ss1", types.UID("ss1"), map[string]string{"foo": "bar"})
	ss1.Status.CollisionCount = new(int32)
	ss2 := newStatefulSet(3, "ss2", types.UID("ss2"), map[string]string{"goo": "car"})
	ss2.Status.CollisionCount = new(int32)
	ss1Rev1, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 1, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev1.Namespace = ss1.Namespace
	ss1Rev2, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 2, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev2.Namespace = ss1.Namespace
	// 清空属主引用，构造可被收养的孤儿修订。
	ss1Rev2.OwnerReferences = []metav1.OwnerReference{}
	ss2Rev1, err := NewControllerRevision(ss2, parentKind, ss2.Spec.Template.Labels, rawTemplate(&ss2.Spec.Template), 1, ss2.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss2Rev1.Namespace = ss2.Namespace
	tests := []testcase{
		{
			name:     "adopting an orphan succeeds",
			parent:   ss1,
			revision: ss1Rev2,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss1,
					revision: ss1Rev2,
				},
			},
			err: false,
		},
		{
			name:     "adopting an owned revision fails",
			parent:   ss1,
			revision: ss2Rev1,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss2,
					revision: ss2Rev1,
				},
			},
			err: true,
		},
		{
			name:     "adopting a non-existent revision fails",
			parent:   ss1,
			revision: ss1Rev2,
			existing: nil,
			err:      true,
		},
	}
	for i := range tests {
		testFn(&tests[i], t)
	}
}

// TestFakeHistory_AdoptControllerRevision 验证 FakeHistory 收养修订的成功与失败路径。
func TestFakeHistory_AdoptControllerRevision(t *testing.T) {
	// testcase 描述一次收养操作的输入与预期结果。
	type testcase struct {
		// name 用例名称。
		name string
		// parent 期望收养修订的父对象。
		parent metav1.Object
		// parentType 父对象的类型元信息。
		parentType *metav1.TypeMeta
		// revision 待收养的修订。
		revision *apps.ControllerRevision
		// existing 预置的修订及其父对象。
		existing []struct {
			parent   metav1.Object
			revision *apps.ControllerRevision
		}
		// err 是否期望返回错误。
		err bool
	}
	// testFn 执行单个用例：预置修订并校验收养结果。
	testFn := func(test *testcase, t *testing.T) {
		client := fake.NewSimpleClientset()

		informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
		stop := make(chan struct{})
		defer close(stop)
		informerFactory.Start(stop)
		informer := informerFactory.Apps().V1().ControllerRevisions()
		if err := AddControllerRevisionControllerIndexer(informer.Informer()); err != nil {
			t.Fatalf("failed to add indexer: %v", err)
		}
		informerFactory.WaitForCacheSync(stop)

		history := NewFakeHistory(informer)
		var collisionCount int32
		for i := range test.existing {
			_, err := history.CreateControllerRevision(test.existing[i].parent, test.existing[i].revision, &collisionCount)
			if err != nil {
				t.Fatal(err)
			}
		}
		adopted, err := history.AdoptControllerRevision(test.parent, parentKind, test.revision)
		if !test.err && err != nil {
			t.Errorf("%s: %s", test.name, err)
		}
		if !test.err && !metav1.IsControlledBy(adopted, test.parent) {
			t.Errorf("%s: adoption failed", test.name)
		}
		if test.err && err == nil {
			t.Errorf("%s: expected error", test.name)
		}
	}

	ss1 := newStatefulSet(3, "ss1", types.UID("ss1"), map[string]string{"foo": "bar"})
	ss1.Status.CollisionCount = new(int32)
	ss2 := newStatefulSet(3, "ss2", types.UID("ss2"), map[string]string{"goo": "car"})
	ss2.Status.CollisionCount = new(int32)
	ss1Rev1, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 1, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev1.Namespace = ss1.Namespace
	ss1Rev2, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 2, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev2.Namespace = ss1.Namespace
	// 清空属主引用，构造可被收养的孤儿修订。
	ss1Rev2.OwnerReferences = []metav1.OwnerReference{}
	ss2Rev1, err := NewControllerRevision(ss2, parentKind, ss2.Spec.Template.Labels, rawTemplate(&ss2.Spec.Template), 1, ss2.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss2Rev1.Namespace = ss2.Namespace
	tests := []testcase{
		{
			name:       "adopting an orphan succeeds",
			parent:     ss1,
			parentType: &ss1.TypeMeta,
			revision:   ss1Rev2,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss1,
					revision: ss1Rev2,
				},
			},
			err: false,
		},
		{
			name:       "adopting an owned revision fails",
			parent:     ss1,
			parentType: &ss1.TypeMeta,
			revision:   ss2Rev1,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss2,
					revision: ss2Rev1,
				},
			},
			err: true,
		},
		{
			name:       "adopting a non-existent revision fails",
			parent:     ss1,
			parentType: &ss1.TypeMeta,
			revision:   ss1Rev2,
			existing:   nil,
			err:        true,
		},
	}
	for i := range tests {
		testFn(&tests[i], t)
	}
}

// TestRealHistory_ReleaseControllerRevision 验证真实 History 释放修订：释放自身拥有、孤儿、
// 他人拥有及不存在的修订均可成功。
func TestRealHistory_ReleaseControllerRevision(t *testing.T) {
	// testcase 描述一次释放操作的输入与预期结果。
	type testcase struct {
		// name 用例名称。
		name string
		// parent 期望释放修订的父对象。
		parent metav1.Object
		// revision 待释放的修订。
		revision *apps.ControllerRevision
		// existing 预置的修订及其父对象。
		existing []struct {
			parent   metav1.Object
			revision *apps.ControllerRevision
		}
		// err 是否期望返回错误。
		err bool
	}
	// testFn 执行单个用例：预置修订、注入 Patch 反应器并校验释放结果。
	testFn := func(test *testcase, t *testing.T) {
		client := fake.NewSimpleClientset()
		// 注入 Patch 反应器模拟服务端应用 strategic merge patch。
		client.AddReactor("*", "*", func(action core.Action) (bool, runtime.Object, error) {
			switch action := action.(type) {
			case core.PatchActionImpl:
				var found *apps.ControllerRevision
				for i := range test.existing {
					if test.revision.Name == test.existing[i].revision.Name &&
						test.revision.Namespace == test.existing[i].revision.Namespace {
						found = test.existing[i].revision
						break
					}
				}
				if found == nil {
					return true, nil, errors.NewNotFound(apps.Resource("controllerrevisions"), test.revision.Name)
				}
				if !metav1.IsControlledBy(test.revision, test.parent) {
					return true, nil, errors.NewInvalid(
						test.revision.GroupVersionKind().GroupKind(), test.revision.Name, nil)
				}
				b, err := strategicpatch.StrategicMergePatch(
					[]byte(runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(apps.SchemeGroupVersion), test.revision)),
					action.GetPatch(), test.revision)
				if err != nil {
					return true, nil, err
				}
				obj, err := runtime.Decode(clientscheme.Codecs.LegacyCodec(apps.SchemeGroupVersion), b)
				if err != nil {
					return true, nil, err
				}
				patched, err := legacyscheme.Scheme.ConvertToVersion(obj, apps.SchemeGroupVersion)
				if err != nil {
					return true, nil, err
				}
				return true, patched, err
			default:
				return false, nil, nil
			}

		})
		informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
		stop := make(chan struct{})
		defer close(stop)
		informerFactory.Start(stop)
		informer := informerFactory.Apps().V1().ControllerRevisions()
		if err := AddControllerRevisionControllerIndexer(informer.Informer()); err != nil {
			t.Fatalf("failed to add indexer: %v", err)
		}
		informerFactory.WaitForCacheSync(stop)

		history := NewHistory(client, informer.Lister(), informer.Informer().GetIndexer())
		var collisionCount int32
		for i := range test.existing {
			_, err := history.CreateControllerRevision(test.existing[i].parent, test.existing[i].revision, &collisionCount)
			if err != nil {
				t.Fatal(err)
			}
		}
		adopted, err := history.ReleaseControllerRevision(test.parent, test.revision)
		if !test.err {
			if err != nil {
				t.Errorf("%s: %s", test.name, err)
			}
			// 释放不存在的修订时返回 nil 对象，属正常情况。
			if adopted == nil {
				return
			}
			if metav1.IsControlledBy(adopted, test.parent) {
				t.Errorf("%s: release failed", test.name)
			}
		}
		if test.err && err == nil {
			t.Errorf("%s: expected error", test.name)
		}
	}

	ss1 := newStatefulSet(3, "ss1", types.UID("ss1"), map[string]string{"foo": "bar"})
	ss2 := newStatefulSet(3, "ss2", types.UID("ss2"), map[string]string{"goo": "car"})
	ss1Rev1, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev1.Namespace = ss1.Namespace
	ss1Rev2, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev2.Namespace = ss1.Namespace
	// 清空属主引用，构造孤儿修订。
	ss1Rev2.OwnerReferences = []metav1.OwnerReference{}
	ss2Rev1, err := NewControllerRevision(ss2, parentKind, ss2.Spec.Template.Labels, rawTemplate(&ss2.Spec.Template), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	ss2Rev1.Namespace = ss2.Namespace
	tests := []testcase{
		{
			name:     "releasing an owned revision succeeds",
			parent:   ss1,
			revision: ss1Rev1,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss1,
					revision: ss1Rev1,
				},
			},
			err: false,
		},
		{
			name:     "releasing an orphan succeeds",
			parent:   ss1,
			revision: ss1Rev2,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss1,
					revision: ss1Rev2,
				},
			},
			err: false,
		},
		{
			name:     "releasing a revision owned by another controller succeeds",
			parent:   ss1,
			revision: ss2Rev1,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss2,
					revision: ss2Rev1,
				},
			},
			err: false,
		},
		{
			name:     "releasing a non-existent revision succeeds",
			parent:   ss1,
			revision: ss1Rev1,
			existing: nil,
			err:      false,
		},
	}
	for i := range tests {
		testFn(&tests[i], t)
	}
}

// TestFakeHistory_ReleaseControllerRevision 验证 FakeHistory 释放修订的各类场景均成功。
func TestFakeHistory_ReleaseControllerRevision(t *testing.T) {
	// testcase 描述一次释放操作的输入与预期结果。
	type testcase struct {
		// name 用例名称。
		name string
		// parent 期望释放修订的父对象。
		parent metav1.Object
		// revision 待释放的修订。
		revision *apps.ControllerRevision
		// existing 预置的修订及其父对象。
		existing []struct {
			parent   metav1.Object
			revision *apps.ControllerRevision
		}
		// err 是否期望返回错误。
		err bool
	}
	// testFn 执行单个用例：预置修订并校验释放结果。
	testFn := func(test *testcase, t *testing.T) {
		client := fake.NewSimpleClientset()
		informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
		stop := make(chan struct{})
		defer close(stop)
		informerFactory.Start(stop)
		informer := informerFactory.Apps().V1().ControllerRevisions()
		if err := AddControllerRevisionControllerIndexer(informer.Informer()); err != nil {
			t.Fatalf("failed to add indexer: %v", err)
		}
		informerFactory.WaitForCacheSync(stop)
		history := NewFakeHistory(informer)
		var collisionCount int32
		for i := range test.existing {
			_, err := history.CreateControllerRevision(test.existing[i].parent, test.existing[i].revision, &collisionCount)
			if err != nil {
				t.Fatal(err)
			}
		}
		adopted, err := history.ReleaseControllerRevision(test.parent, test.revision)
		if !test.err {
			if err != nil {
				t.Errorf("%s: %s", test.name, err)
			}
			if adopted == nil {
				return
			}
			if metav1.IsControlledBy(adopted, test.parent) {
				t.Errorf("%s: release failed", test.name)
			}
		}
		if test.err && err == nil {
			t.Errorf("%s: expected error", test.name)
		}
	}

	ss1 := newStatefulSet(3, "ss1", types.UID("ss1"), map[string]string{"foo": "bar"})
	ss1.Status.CollisionCount = new(int32)
	ss2 := newStatefulSet(3, "ss2", types.UID("ss2"), map[string]string{"goo": "car"})
	ss2.Status.CollisionCount = new(int32)
	ss1Rev1, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 1, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev1.Namespace = ss1.Namespace
	ss1Rev2, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 2, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev2.Namespace = ss1.Namespace
	// 清空属主引用，构造孤儿修订。
	ss1Rev2.OwnerReferences = []metav1.OwnerReference{}
	ss2Rev1, err := NewControllerRevision(ss2, parentKind, ss2.Spec.Template.Labels, rawTemplate(&ss2.Spec.Template), 1, ss2.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss2Rev1.Namespace = ss2.Namespace
	tests := []testcase{
		{
			name:     "releasing an owned revision succeeds",
			parent:   ss1,
			revision: ss1Rev1,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss1,
					revision: ss1Rev1,
				},
			},
			err: false,
		},
		{
			name:     "releasing an orphan succeeds",
			parent:   ss1,
			revision: ss1Rev2,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss1,
					revision: ss1Rev2,
				},
			},
			err: false,
		},
		{
			name:     "releasing a revision owned by another controller succeeds",
			parent:   ss1,
			revision: ss2Rev1,
			existing: []struct {
				parent   metav1.Object
				revision *apps.ControllerRevision
			}{
				{
					parent:   ss2,
					revision: ss2Rev1,
				},
			},
			err: false,
		},
		{
			name:     "releasing a non-existent revision succeeds",
			parent:   ss1,
			revision: ss1Rev1,
			existing: nil,
			err:      false,
		},
	}
	for i := range tests {
		testFn(&tests[i], t)
	}
}

// TestFindEqualRevisions 验证 FindEqualRevisions 找出与目标修订内容等价的修订，
// 空列表与无匹配时返回空集合。
func TestFindEqualRevisions(t *testing.T) {
	// testcase 描述一次等价查找的输入与预期结果。
	type testcase struct {
		// name 用例名称。
		name string
		// revision 作为比较基准的修订。
		revision *apps.ControllerRevision
		// revisions 待比较的修订列表。
		revisions []*apps.ControllerRevision
		// want 期望找到的修订名称集合。
		want map[string]bool
	}
	// testFn 执行单个用例，校验找到的等价修订集合。
	testFn := func(test *testcase, t *testing.T) {
		found := FindEqualRevisions(test.revisions, test.revision)
		if len(found) != len(test.want) {
			t.Errorf("%s: want %d revisions found %d", test.name, len(test.want), len(found))
		}
		for i := range found {
			if !test.want[found[i].Name] {
				t.Errorf("%s: wanted %s not found", test.name, found[i].Name)
			}

		}
	}
	ss1 := newStatefulSet(3, "ss1", types.UID("ss1"), map[string]string{"foo": "bar"})
	ss1.Status.CollisionCount = new(int32)
	ss2 := newStatefulSet(3, "ss2", types.UID("ss2"), map[string]string{"goo": "car"})
	ss2.Status.CollisionCount = new(int32)
	ss1Rev1, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 1, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev1.Namespace = ss1.Namespace
	ss1Rev2, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 2, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev2.Namespace = ss1.Namespace
	ss1Rev2.OwnerReferences = []metav1.OwnerReference{}
	ss2Rev1, err := NewControllerRevision(ss2, parentKind, ss2.Spec.Template.Labels, rawTemplate(&ss2.Spec.Template), 1, ss2.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss2Rev1.Namespace = ss2.Namespace
	ss2Rev2, err := NewControllerRevision(ss2, parentKind, ss2.Spec.Template.Labels, rawTemplate(&ss2.Spec.Template), 2, ss2.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss2Rev2.Namespace = ss2.Namespace
	tests := []testcase{
		{
			name:      "finds equivalent",
			revision:  ss1Rev1,
			revisions: []*apps.ControllerRevision{ss1Rev1, ss2Rev1, ss2Rev2},
			want:      map[string]bool{ss1Rev1.Name: true},
		},
		{
			name:      "finds nothing when empty",
			revision:  ss1Rev1,
			revisions: nil,
			want:      map[string]bool{},
		},
		{
			name:      "finds nothing with no matches",
			revision:  ss1Rev1,
			revisions: []*apps.ControllerRevision{ss2Rev2, ss2Rev1},
			want:      map[string]bool{},
		},
	}
	for i := range tests {
		testFn(&tests[i], t)
	}
}

// TestSortControllerRevisions 验证 SortControllerRevisions 按修订号与创建时间排序，
// 覆盖乱序、已排序、逆序、相同时间戳及空列表。
func TestSortControllerRevisions(t *testing.T) {
	// testcase 描述一次排序操作的输入与预期顺序。
	type testcase struct {
		// name 用例名称。
		name string
		// revisions 待排序的修订。
		revisions []*apps.ControllerRevision
		// want 期望的修订名称顺序。
		want []string
	}
	// testFn 执行单个用例：排序后逐项校验名称顺序。
	testFn := func(test *testcase, t *testing.T) {
		// 使用子测试便于定位失败用例。
		t.Run(test.name, func(t *testing.T) {
			SortControllerRevisions(test.revisions)
			for i := range test.revisions {
				if test.revisions[i].Name != test.want[i] {
					t.Errorf("%s: want %s at %d got %s", test.name, test.want[i], i, test.revisions[i].Name)
				}
			}
		})
	}
	ss1 := newStatefulSet(3, "ss1", types.UID("ss1"), map[string]string{"foo": "bar"})
	ss1.Status.CollisionCount = new(int32)

	ss1Rev1, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 1, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}

	ss1Rev1.Namespace = ss1.Namespace
	ss1Rev2, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 2, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}

	ss1Rev2.Namespace = ss1.Namespace
	ss1Rev3, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 3, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}

	// 构造两个修订号相同但创建时间不同的副本，验证排序对“平局”的处理。
	ss1Rev3Time2, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 3, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev3Time2.Namespace = ss1.Namespace
	// 将创建时间后移一秒，制造排序所需的差异。
	ss1Rev3Time2.CreationTimestamp = metav1.Time{Time: ss1Rev3.CreationTimestamp.Add(time.Second)}

	ss1Rev3Time2Name2, err := NewControllerRevision(ss1, parentKind, ss1.Spec.Template.Labels, rawTemplate(&ss1.Spec.Template), 3, ss1.Status.CollisionCount)
	if err != nil {
		t.Fatal(err)
	}
	ss1Rev3Time2Name2.Namespace = ss1.Namespace
	ss1Rev3Time2Name2.CreationTimestamp = metav1.Time{Time: ss1Rev3.CreationTimestamp.Add(time.Second)}

	tests := []testcase{
		{
			name:      "out of order",
			revisions: []*apps.ControllerRevision{ss1Rev2, ss1Rev1, ss1Rev3},
			want:      []string{ss1Rev1.Name, ss1Rev2.Name, ss1Rev3.Name},
		},
		{
			name:      "sorted",
			revisions: []*apps.ControllerRevision{ss1Rev1, ss1Rev2, ss1Rev3},
			want:      []string{ss1Rev1.Name, ss1Rev2.Name, ss1Rev3.Name},
		},
		{
			name:      "reversed",
			revisions: []*apps.ControllerRevision{ss1Rev3, ss1Rev2, ss1Rev1},
			want:      []string{ss1Rev1.Name, ss1Rev2.Name, ss1Rev3.Name},
		},
		{
			name:      "with ties",
			revisions: []*apps.ControllerRevision{ss1Rev3, ss1Rev3Time2, ss1Rev2, ss1Rev1},
			want:      []string{ss1Rev1.Name, ss1Rev2.Name, ss1Rev3.Name, ss1Rev3Time2.Name, ss1Rev3Time2Name2.Name},
		},
		{
			name:      "empty",
			revisions: nil,
			want:      nil,
		},
	}
	for i := range tests {
		testFn(&tests[i], t)
	}
}

// newStatefulSet 构造测试用 StatefulSet，其选择器完全基于 MatchExpressions。
func newStatefulSet(replicas int, name string, uid types.UID, labels map[string]string) *apps.StatefulSet {
	// 将仅基于 map 的选择器转换为基于集合（set-based）的选择器。
	// 为每个标签生成一条 In 表达式。
	var testMatchExpressions []metav1.LabelSelectorRequirement
	for key, value := range labels {
		sel := metav1.LabelSelectorRequirement{
			Key:      key,
			Operator: metav1.LabelSelectorOpIn,
			Values:   []string{value},
		}
		testMatchExpressions = append(testMatchExpressions, sel)
	}
	return &apps.StatefulSet{
		TypeMeta: metav1.TypeMeta{
			Kind:       "StatefulSet",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: v1.NamespaceDefault,
			UID:       uid,
		},
		Spec: apps.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				// 刻意将 MatchLabels 留空，确保调用链中任何环节若忽略基于集合的
				// MatchExpressions 都会导致测试失败。
				MatchLabels:      nil,
				MatchExpressions: testMatchExpressions,
			},
			Replicas: ptr.To[int32](int32(replicas)),
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "nginx",
							Image: "nginx",
							VolumeMounts: []v1.VolumeMount{
								{Name: "datadir", MountPath: "/tmp/"},
								{Name: "home", MountPath: "/home"},
							},
						},
					},
					Volumes: []v1.Volume{{
						Name: "home",
						VolumeSource: v1.VolumeSource{
							HostPath: &v1.HostPathVolumeSource{
								Path: fmt.Sprintf("/tmp/%v", "home"),
							},
						}}},
				},
			},
			VolumeClaimTemplates: []v1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "datadir"},
					Spec: v1.PersistentVolumeClaimSpec{
						Resources: v1.VolumeResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceStorage: *resource.NewQuantity(1, resource.BinarySI),
							},
						},
					},
				},
			},
			ServiceName: "governingsvc",
		},
	}
}

// parentKind 测试中修订所属父对象的 Kind（StatefulSet）。
var parentKind = apps.SchemeGroupVersion.WithKind("StatefulSet")

// rawTemplate 将 Pod 模板编码为 JSON RawExtension，作为修订的数据负载。
func rawTemplate(template *v1.PodTemplateSpec) runtime.RawExtension {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	if err := enc.Encode(template); err != nil {
		panic(err)
	}
	return runtime.RawExtension{Raw: buf.Bytes()}
}
