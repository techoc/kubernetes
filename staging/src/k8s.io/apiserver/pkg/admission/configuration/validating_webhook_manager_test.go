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

package configuration

import (
	"context"
	"reflect"
	"testing"
	"testing/synctest"

	v1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/admission/plugin/webhook"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
)

// TestGetValidatingWebhookConfig 验证管理器能从 informer 缓存中读出
// ValidatingWebhookConfiguration 并转换为访问器。
// 使用 synctest 让 informer 的异步通知在虚拟时间下确定性地完成，
// 避免 sleep 造成的偶发失败。创建前应为空，创建后应出现且内容一致。
func TestGetValidatingWebhookConfig(t *testing.T) { synctest.Test(t, testGetValidatingWebhookConfig) }
func testGetValidatingWebhookConfig(t *testing.T) {
	// 构造测试客户端，供准入插件查询 ValidatingWebhookConfiguration。
	client := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	stop := make(chan struct{})
	defer close(stop)

	manager := NewValidatingWebhookConfigurationManager(informerFactory, nil)
	informerFactory.Start(stop)
	informerFactory.WaitForCacheSync(stop)

	// 尚未创建任何配置
	synctest.Wait()
	if configurations := manager.Webhooks(); len(configurations) != 0 {
		t.Errorf("expected empty webhooks, but got %v", configurations)
	}

	webhookConfiguration := &v1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "webhook1"},
		Webhooks:   []v1.ValidatingWebhook{{Name: "webhook1.1"}},
	}

	client.
		AdmissionregistrationV1().
		ValidatingWebhookConfigurations().
		Create(context.TODO(), webhookConfiguration, metav1.CreateOptions{})

	// 等待通知送达 informer。
	synctest.Wait()
	configurations := manager.Webhooks()

	// 校验 webhook 已存在
	if len(configurations) == 0 {
		t.Errorf("expected non empty webhooks")
	}
	for i := range configurations {
		h, ok := configurations[i].GetValidatingWebhook()
		if !ok {
			t.Errorf("Expected validating webhook")
			continue
		}
		if !reflect.DeepEqual(h, &webhookConfiguration.Webhooks[i]) {
			t.Errorf("Expected\n%#v\ngot\n%#v", &webhookConfiguration.Webhooks[i], h)
		}
	}
}

// mockCreateValidatingWebhookAccessor 用于统计刷新 webhookAccessors 时
// webhook.NewValidatingWebhookAccessor 被调用了多少次。
//
// 注意：也许有现成的测试辅助工具可以直接复用，而不必自己写。
type mockCreateValidatingWebhookAccessor struct {
	numberOfCalls int
}

func (mock *mockCreateValidatingWebhookAccessor) calledNTimes() int { return mock.numberOfCalls }
func (mock *mockCreateValidatingWebhookAccessor) resetCounter()     { mock.numberOfCalls = 0 }
func (mock *mockCreateValidatingWebhookAccessor) incrementCounter() { mock.numberOfCalls++ }

// fn 是可注入的构造器，计数每次创建；用于验证「智能重载」只在必要时重建访问器。
func (mock *mockCreateValidatingWebhookAccessor) fn(uid string, configurationName string, h *v1.ValidatingWebhook) webhook.WebhookAccessor {
	mock.incrementCounter()
	return webhook.NewValidatingWebhookAccessor(uid, configurationName, h)
}

// configurationTotalWebhooks 统计所有配置中的 webhook 总数，作为期望访问器数量。
func configurationTotalWebhooks(configurations []*v1.ValidatingWebhookConfiguration) int {
	total := 0
	for _, configuration := range configurations {
		total += len(configuration.Webhooks)
	}
	return total
}

// TestGetValidatingWebhookConfigSmartReload 验证配置更新时只重建受影响的访问器。
//
// 「智能重载」的意义在于：未变化的 webhook 复用已有访问器，
// 避免每次配置变更都重建全部对象（重建会丢弃内部缓存与状态）。
// 用例通过统计构造器调用次数来量化：numberOfCreations 是首次创建数，
// numberOfRefreshes 是更新阶段的重建数，后者应只覆盖真正变化的条目。
func TestGetValidatingWebhookConfigSmartReload(t *testing.T) {
	type args struct {
		createWebhookConfigurations []*v1.ValidatingWebhookConfiguration
		updateWebhookConfigurations []*v1.ValidatingWebhookConfiguration
	}
	tests := []struct {
		name              string
		args              args
		numberOfCreations int
		// 刷新次数指没有复用缓存、而是重新创建 webhook 访问器的次数。
		numberOfRefreshes             int
		finalNumberOfWebhookAccessors int
	}{
		{
			name: "no creations and no updates",
			args: args{
				nil,
				nil,
			},
			numberOfCreations:             0,
			numberOfRefreshes:             0,
			finalNumberOfWebhookAccessors: 0,
		},
		{
			name: "create configurations and no updates",
			args: args{
				[]*v1.ValidatingWebhookConfiguration{
					{
						ObjectMeta: metav1.ObjectMeta{Name: "webhook1"},
						Webhooks:   []v1.ValidatingWebhook{{Name: "webhook1.1"}},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Name: "webhook2"},
						Webhooks:   []v1.ValidatingWebhook{{Name: "webhook2.1"}},
					},
				},
				nil,
			},
			numberOfCreations:             2,
			numberOfRefreshes:             0,
			finalNumberOfWebhookAccessors: 2,
		},
		{
			name: "create configurations and update some of them",
			args: args{
				[]*v1.ValidatingWebhookConfiguration{
					{
						ObjectMeta: metav1.ObjectMeta{Name: "webhook3"},
						Webhooks:   []v1.ValidatingWebhook{{Name: "webhook3.1"}},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Name: "webhook4"},
						Webhooks:   []v1.ValidatingWebhook{{Name: "webhook4.1"}},
					},
				},
				[]*v1.ValidatingWebhookConfiguration{
					{
						ObjectMeta: metav1.ObjectMeta{Name: "webhook3"},
						Webhooks:   []v1.ValidatingWebhook{{Name: "webhook3.1-updated"}},
					},
				},
			},
			numberOfCreations:             2,
			numberOfRefreshes:             1,
			finalNumberOfWebhookAccessors: 2,
		},
		{
			name: "create configuration and update moar of them",
			args: args{
				[]*v1.ValidatingWebhookConfiguration{
					{
						ObjectMeta: metav1.ObjectMeta{Name: "webhook5"},
						Webhooks:   []v1.ValidatingWebhook{{Name: "webhook5.1"}, {Name: "webhook5.2"}},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Name: "webhook6"},
						Webhooks:   []v1.ValidatingWebhook{{Name: "webhook6.1"}},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Name: "webhook7"},
						Webhooks:   []v1.ValidatingWebhook{{Name: "webhook7.1"}, {Name: "webhook7.1"}},
					},
				},
				[]*v1.ValidatingWebhookConfiguration{
					{
						ObjectMeta: metav1.ObjectMeta{Name: "webhook5"},
						Webhooks:   []v1.ValidatingWebhook{{Name: "webhook5.1-updated"}},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Name: "webhook7"},
						Webhooks:   []v1.ValidatingWebhook{{Name: "webhook7.1-updated"}, {Name: "webhook7.2-updated"}, {Name: "webhook7.3"}},
					},
				},
			},
			numberOfCreations:             5,
			numberOfRefreshes:             4,
			finalNumberOfWebhookAccessors: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := fake.NewSimpleClientset()
				informerFactory := informers.NewSharedInformerFactory(client, 0)
				stop := make(chan struct{})
				defer close(stop)
				manager := NewValidatingWebhookConfigurationManager(informerFactory, nil)
				managerStructPtr := manager.(*validatingWebhookConfigurationManager)
				fakeWebhookAccessorCreator := &mockCreateValidatingWebhookAccessor{}
				// 注入计数构造器，之后所有创建都会经过它。
				managerStructPtr.createValidatingWebhookAccessor = fakeWebhookAccessorCreator.fn
				informerFactory.Start(stop)
				informerFactory.WaitForCacheSync(stop)

				// 创建 webhook 配置
				for _, configurations := range tt.args.createWebhookConfigurations {
					client.
						AdmissionregistrationV1().
						ValidatingWebhookConfigurations().
						Create(context.TODO(), configurations, metav1.CreateOptions{})
				}

				synctest.Wait()
				webhooks := manager.Webhooks()
				if configurationTotalWebhooks(tt.args.createWebhookConfigurations) != len(webhooks) {
					t.Errorf("Expected number of webhooks %d received %d",
						configurationTotalWebhooks(tt.args.createWebhookConfigurations),
						len(webhooks),
					)
				}
				// 校验首次创建的访问器数量
				if tt.numberOfCreations != fakeWebhookAccessorCreator.calledNTimes() {
					t.Errorf(
						"Expected number of creations %d received %d",
						tt.numberOfCreations, fakeWebhookAccessorCreator.calledNTimes(),
					)
				}

				// 重置 mock 计数器
				fakeWebhookAccessorCreator.resetCounter()

				// 更新 webhook 配置
				for _, configurations := range tt.args.updateWebhookConfigurations {
					client.
						AdmissionregistrationV1().
						ValidatingWebhookConfigurations().
						Update(context.TODO(), configurations, metav1.UpdateOptions{})
				}
				synctest.Wait()
				webhooks = manager.Webhooks()
				if tt.finalNumberOfWebhookAccessors != len(webhooks) {
					t.Errorf("Expected final number of webhooks %d received %d",
						tt.finalNumberOfWebhookAccessors,
						len(webhooks),
					)
				}

				// 校验更新阶段重建的访问器数量
				if tt.numberOfRefreshes != fakeWebhookAccessorCreator.calledNTimes() {
					t.Errorf(
						"Expected number of refreshes %d received %d",
						tt.numberOfRefreshes, fakeWebhookAccessorCreator.calledNTimes(),
					)
				}
				// 为后续用例重置 mock 计数器
				fakeWebhookAccessorCreator.resetCounter()
			})
		})
	}
}
