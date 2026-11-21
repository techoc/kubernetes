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

package serviceaccount

import (
	"context"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestServiceAccountCreation 以表驱动方式验证 ServiceAccount 控制器在命名空间
// 新增/更新以及 ServiceAccount 被删除时，是否会按需补齐托管账号。
func TestServiceAccountCreation(t *testing.T) {
	ns := metav1.NamespaceDefault

	defaultName := "default"
	managedName := "managed"

	// 活跃命名空间需要补齐账号；终止中的命名空间不应再创建新账号。
	activeNS := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
		Status: v1.NamespaceStatus{
			Phase: v1.NamespaceActive,
		},
	}
	terminatingNS := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
		Status: v1.NamespaceStatus{
			Phase: v1.NamespaceTerminating,
		},
	}
	defaultServiceAccount := &v1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:            defaultName,
			Namespace:       ns,
			ResourceVersion: "1",
		},
	}
	managedServiceAccount := &v1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:            managedName,
			Namespace:       ns,
			ResourceVersion: "1",
		},
	}
	// 非托管账号不参与控制器的补齐逻辑，删除它不应触发任何创建。
	unmanagedServiceAccount := &v1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "other-unmanaged",
			Namespace:       ns,
			ResourceVersion: "1",
		},
	}

	// 每个用例描述初始状态、触发事件以及期望创建的账号列表。
	testcases := map[string]struct {
		ExistingNamespace       *v1.Namespace
		ExistingServiceAccounts []*v1.ServiceAccount

		AddedNamespace        *v1.Namespace
		UpdatedNamespace      *v1.Namespace
		DeletedServiceAccount *v1.ServiceAccount

		ExpectCreatedServiceAccounts []string
	}{
		// 新增活跃命名空间且没有任何账号时，两个托管账号都要创建。
		"new active namespace missing serviceaccounts": {
			ExistingServiceAccounts:      []*v1.ServiceAccount{},
			AddedNamespace:               activeNS,
			ExpectCreatedServiceAccounts: sets.NewString(defaultName, managedName).List(),
		},
		// 已有一个托管账号时，只补齐缺失的那个。
		"new active namespace missing serviceaccount": {
			ExistingServiceAccounts:      []*v1.ServiceAccount{managedServiceAccount},
			AddedNamespace:               activeNS,
			ExpectCreatedServiceAccounts: []string{defaultName},
		},
		// 账号齐全时不应产生任何创建动作。
		"new active namespace with serviceaccounts": {
			ExistingServiceAccounts:      []*v1.ServiceAccount{defaultServiceAccount, managedServiceAccount},
			AddedNamespace:               activeNS,
			ExpectCreatedServiceAccounts: []string{},
		},

		// 终止中的命名空间即使缺少账号也不再创建。
		"new terminating namespace": {
			ExistingServiceAccounts:      []*v1.ServiceAccount{},
			AddedNamespace:               terminatingNS,
			ExpectCreatedServiceAccounts: []string{},
		},

		// 命名空间更新事件同样会触发账号补齐。
		"updated active namespace missing serviceaccounts": {
			ExistingServiceAccounts:      []*v1.ServiceAccount{},
			UpdatedNamespace:             activeNS,
			ExpectCreatedServiceAccounts: sets.NewString(defaultName, managedName).List(),
		},
		"updated active namespace missing serviceaccount": {
			ExistingServiceAccounts:      []*v1.ServiceAccount{defaultServiceAccount},
			UpdatedNamespace:             activeNS,
			ExpectCreatedServiceAccounts: []string{managedName},
		},
		"updated active namespace with serviceaccounts": {
			ExistingServiceAccounts:      []*v1.ServiceAccount{defaultServiceAccount, managedServiceAccount},
			UpdatedNamespace:             activeNS,
			ExpectCreatedServiceAccounts: []string{},
		},
		"updated terminating namespace": {
			ExistingServiceAccounts:      []*v1.ServiceAccount{},
			UpdatedNamespace:             terminatingNS,
			ExpectCreatedServiceAccounts: []string{},
		},

		// 删除账号时若命名空间信息不存在，无法确定其状态，不会创建账号。
		"deleted serviceaccount without namespace": {
			DeletedServiceAccount:        defaultServiceAccount,
			ExpectCreatedServiceAccounts: []string{},
		},
		// 活跃命名空间中的托管账号被删除后应自动重建。
		"deleted serviceaccount with active namespace": {
			ExistingServiceAccounts:      []*v1.ServiceAccount{managedServiceAccount},
			ExistingNamespace:            activeNS,
			DeletedServiceAccount:        defaultServiceAccount,
			ExpectCreatedServiceAccounts: []string{defaultName},
		},
		// 终止中的命名空间里被删除的账号不重建，否则会阻碍命名空间退出。
		"deleted serviceaccount with terminating namespace": {
			ExistingNamespace:            terminatingNS,
			DeletedServiceAccount:        defaultServiceAccount,
			ExpectCreatedServiceAccounts: []string{},
		},
		// 删除非托管账号不属于控制器职责，不应触发创建。
		"deleted unmanaged serviceaccount with active namespace": {
			ExistingServiceAccounts:      []*v1.ServiceAccount{defaultServiceAccount, managedServiceAccount},
			ExistingNamespace:            activeNS,
			DeletedServiceAccount:        unmanagedServiceAccount,
			ExpectCreatedServiceAccounts: []string{},
		},
		"deleted unmanaged serviceaccount with terminating namespace": {
			ExistingNamespace:            terminatingNS,
			DeletedServiceAccount:        unmanagedServiceAccount,
			ExpectCreatedServiceAccounts: []string{},
		},
	}

	for k, tc := range testcases {
		t.Run(k, func(t *testing.T) {
			// fake 客户端用于记录 API 调用，informer 则手动填充初始对象以模拟缓存。
			client := fake.NewClientset(defaultServiceAccount, managedServiceAccount)
			informers := informers.NewSharedInformerFactory(fake.NewClientset(), controller.NoResyncPeriodFunc())
			options := DefaultServiceAccountsControllerOptions()
			options.ServiceAccounts = []v1.ServiceAccount{
				{ObjectMeta: metav1.ObjectMeta{Name: defaultName}},
				{ObjectMeta: metav1.ObjectMeta{Name: managedName}},
			}
			saInformer := informers.Core().V1().ServiceAccounts()
			nsInformer := informers.Core().V1().Namespaces()

			var wg sync.WaitGroup
			// 等待控制器 goroutine 退出，避免测试结束后仍有后台操作污染状态。
			defer wg.Wait()

			tCtx := ktesting.Init(t)
			logger := tCtx.Logger()
			defer tCtx.Cancel("test case terminating")

			controller, err := NewServiceAccountsController(
				logger,
				saInformer,
				nsInformer,
				client,
				options,
			)
			if err != nil {
				t.Fatalf("error creating ServiceAccounts controller: %v", err)
			}
			// 测试直接操作 informer 存储而不启动其 Reflector，故用始终就绪的同步函数跳过等待。
			controller.saListerSynced = alwaysReady
			controller.nsListerSynced = alwaysReady

			saStore := saInformer.Informer().GetStore()
			nsStore := nsInformer.Informer().GetStore()

			// 通过 channel 感知同步被调用，避免依赖固定 sleep 造成测试不稳定。
			syncCalls := make(chan struct{}, 1)
			controller.syncHandler = func(ctx context.Context, key string) error {
				err := controller.syncNamespace(ctx, key)
				if err != nil {
					t.Logf("%s: %v", k, err)
				}

				syncCalls <- struct{}{}
				return err
			}
			stopCh := make(chan struct{})
			defer close(stopCh)
			wg.Go(func() {
				controller.Run(tCtx, 1)
			})

			// 预置初始对象后再触发事件，使控制器看到的是用例指定的现场。
			if tc.ExistingNamespace != nil {
				nsStore.Add(tc.ExistingNamespace) //nolint:errcheck
			}
			for _, s := range tc.ExistingServiceAccounts {
				saStore.Add(s) //nolint:errcheck
			}

			if tc.AddedNamespace != nil {
				nsStore.Add(tc.AddedNamespace) //nolint:errcheck
				controller.namespaceAdded(tc.AddedNamespace)
			}
			if tc.UpdatedNamespace != nil {
				nsStore.Add(tc.UpdatedNamespace) //nolint:errcheck
				controller.namespaceUpdated(nil, tc.UpdatedNamespace)
			}
			if tc.DeletedServiceAccount != nil {
				controller.serviceAccountDeleted(logger, tc.DeletedServiceAccount)
			}

			// 等待事件处理路径调用一次 syncHandler。
			select {
			case <-syncCalls:
			case <-time.After(10 * time.Second):
				t.Errorf("%s: took too long", k)
			}

			// 用 fake 客户端记录的动作直接核对创建行为。
			actions := client.Actions()
			if len(tc.ExpectCreatedServiceAccounts) != len(actions) {
				t.Errorf("%s: Expected to create accounts %#v. Actual actions were: %#v", k, tc.ExpectCreatedServiceAccounts, actions)
				return
			}
			for i, expectedName := range tc.ExpectCreatedServiceAccounts {
				action := actions[i]
				if !action.Matches("create", "serviceaccounts") {
					t.Errorf("%s: Unexpected action %s", k, action)
					break
				}
				createdAccount := action.(core.CreateAction).GetObject().(*v1.ServiceAccount)
				if createdAccount.Name != expectedName {
					t.Errorf("%s: Expected %s to be created, got %s", k, expectedName, createdAccount.Name)
				}
			}
		})
	}
}

// alwaysReady 在测试中代替 informer 的同步检查函数，跳过缓存同步等待。
var alwaysReady = func() bool { return true }
