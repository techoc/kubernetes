/*
Copyright 2025 The Kubernetes Authors.

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

package apidispatcher

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
)

// registerAndResetMetrics 在测试中开启异步 API 特性门并清空相关指标，
// 避免上一个用例的计数干扰断言。
func registerAndResetMetrics(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SchedulerAsyncAPICalls, true)
	metrics.Register()

	metrics.AsyncAPICallsTotal.Reset()
	metrics.AsyncAPICallDuration.Reset()
	metrics.AsyncAPIPendingCalls.Reset()
}

const (
	mockCallTypeLow  fwk.APICallType = "low"
	mockCallTypeHigh fwk.APICallType = "high"
)

// mockRelevances 定义测试用的标准关联度集合。
var mockRelevances = fwk.APICallRelevances{
	mockCallTypeLow:  1,
	mockCallTypeHigh: 2,
}

// mockAPICall 是 fwk.APICall 的模拟实现，各方法可通过注入的函数定制行为。
type mockAPICall struct {
	callType  fwk.APICallType
	uid       types.UID
	executeFn func(ctx context.Context, client clientset.Interface) error
	mergeFn   func(oldCall fwk.APICall) error
	syncFn    func(obj metav1.Object) (metav1.Object, error)
	isNoOpFn  func() bool
}

// CallType 返回模拟调用的调用类型。
func (mac *mockAPICall) CallType() fwk.APICallType {
	return mac.callType
}

// UID 返回模拟调用关联对象的 UID。
func (mac *mockAPICall) UID() types.UID {
	return mac.uid
}

// Execute 执行模拟调用；未注入函数时默认成功。
func (mac *mockAPICall) Execute(ctx context.Context, client clientset.Interface) error {
	if mac.executeFn != nil {
		return mac.executeFn(ctx, client)
	}
	return nil
}

// Merge 合并同 UID 的旧调用；未注入函数时默认成功。
func (mac *mockAPICall) Merge(oldCall fwk.APICall) error {
	if mac.mergeFn != nil {
		return mac.mergeFn(oldCall)
	}
	return nil
}

// Sync 用调用的最新状态同步缓存对象；未注入函数时原样返回。
func (mac *mockAPICall) Sync(obj metav1.Object) (metav1.Object, error) {
	if mac.syncFn != nil {
		return mac.syncFn(obj)
	}
	return obj, nil
}

// IsNoOp 判断调用是否已无需执行；默认返回仍需执行。
func (mac *mockAPICall) IsNoOp() bool {
	if mac.isNoOpFn != nil {
		return mac.isNoOpFn()
	}
	// 默认认为调用仍需执行。
	return false
}

// TestAPIDispatcherLifecycle 验证 dispatcher 的完整生命周期：加入、合并、同步、执行与指标上报。
func TestAPIDispatcherLifecycle(t *testing.T) {
	// 清空异步 API 指标，保证后续断言只统计本用例的行为。
	registerAndResetMetrics(t)

	logger, _ := ktesting.NewTestContext(t)

	uid := types.UID("uid")
	dispatcher := New(fake.NewClientset(), 1, mockRelevances)

	call1 := &mockAPICall{
		uid:      uid,
		callType: mockCallTypeLow,
	}
	onFinishCh1 := make(chan error, 1)
	opts1 := fwk.APICallOptions{
		OnFinish: onFinishCh1,
	}
	var executeCalls, mergeCalls, syncCalls, isNoOpCalls int
	call2 := &mockAPICall{
		uid:      uid,
		callType: mockCallTypeLow,
		executeFn: func(_ context.Context, _ clientset.Interface) error {
			executeCalls++
			return nil
		},
		mergeFn: func(oldCall fwk.APICall) error {
			mergeCalls++
			return nil
		},
		syncFn: func(obj metav1.Object) (metav1.Object, error) {
			syncCalls++
			return obj, nil
		},
		isNoOpFn: func() bool {
			isNoOpCalls++
			return false
		},
	}
	onFinishCh2 := make(chan error, 1)
	opts2 := fwk.APICallOptions{
		OnFinish: onFinishCh2,
	}

	if err := dispatcher.Add(call1, opts1); err != nil {
		t.Fatalf("Unexpected error while adding a call1: %v", err)
	}
	if err := dispatcher.Add(call2, opts2); err != nil {
		t.Fatalf("Unexpected error while adding a call2: %v", err)
	}
	if mergeCalls != 1 {
		t.Errorf("Expected call2's Merge() to be called once, but was %v times", mergeCalls)
	}
	expectOnFinish(t, onFinishCh1, fwk.ErrCallOverwritten)

	obj := &metav1.ObjectMeta{
		UID: uid,
	}
	_, err := dispatcher.SyncObject(obj)
	if err != nil {
		t.Fatalf("Unexpected error while syncing an object: %v", err)
	}
	if syncCalls != 1 {
		t.Errorf("Expected call2's sync to be called once, but was %v times", mergeCalls)
	}

	// 此处故意推迟 Run，以验证运行前的合并与同步逻辑已经生效。
	dispatcher.Run(logger)
	defer dispatcher.Close()

	expectOnFinish(t, onFinishCh2, nil)
	if executeCalls != 1 {
		t.Errorf("Expected call2's Execute() to be called once, but was %v times", executeCalls)
	}
	if isNoOpCalls != 2 {
		t.Errorf("Expected call2's IsNoOp() to be called two times, but was %v times", executeCalls)
	}

	// 校验执行指标：一次成功的调用应被准确计数。
	testutil.AssertVectorCount(t, "scheduler_async_api_call_execution_total", map[string]string{"call_type": "low", "result": "success"}, 1)
	testutil.AssertHistogramTotalCount(t, "scheduler_async_api_call_execution_duration_seconds", map[string]string{"call_type": "low", "result": "success"}, 1)
}
