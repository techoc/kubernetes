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

package resourcelock

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

// mockRecorder implements record.EventRecorder for testing
// 只记录消息文本，足以断言 RecordEvent 的格式与去重行为。
type mockRecorder struct {
	events []string
}

func (r *mockRecorder) Event(object runtime.Object, eventtype, reason, message string) {
	r.events = append(r.events, message)
}

func (r *mockRecorder) Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
	r.events = append(r.events, fmt.Sprintf(messageFmt, args...))
}

func (r *mockRecorder) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	r.events = append(r.events, fmt.Sprintf(messageFmt, args...))
}

const (
	testNamespace = "test-namespace"
	testName      = "test-name"
	testIdentity  = "test-identity"
)

var (
	// 测试共享状态由 setup() 在每个用例前重置，避免用例间相互影响。
	testTime   = time.Now()
	client     *fake.Clientset
	leaseLock  *LeaseLock
	recorder   *mockRecorder
	testRecord LeaderElectionRecord
)

// setup initializes the test variables before each test
func setup() {
	client = fake.NewClientset()
	recorder = &mockRecorder{}

	leaseLock = &LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testName,
		},
		Client: client.CoordinationV1(),
		LockConfig: ResourceLockConfig{
			Identity: testIdentity,
		},
	}

	testRecord = LeaderElectionRecord{
		HolderIdentity:       testIdentity,
		LeaseDurationSeconds: 15,
		AcquireTime:          metav1.Time{Time: testTime},
		RenewTime:            metav1.Time{Time: testTime},
		LeaderTransitions:    1,
		PreferredHolder:      "preferred-holder",
		Strategy:             "strategy",
	}
}

// TestGetError 校验读取不存在的 Lease 返回 NotFound，调用方可据此判断首次获取。
func TestGetError(t *testing.T) {
	setup()
	// Test Get on non-existent lease
	_, _, err := leaseLock.Get(context.Background())
	if err == nil {
		t.Error("Expected error getting non-existent lease, got nil")
	}
	if !apierrors.IsNotFound(err) {
		t.Errorf("Expected NotFound error, got %v", err)
	}
}

// TestGet 校验写入的 LeaderElectionRecord 能完整读回，且原始 JSON bytes 可再次反序列化。
func TestGet(t *testing.T) {
	setup()

	// Create a lease first
	if err := leaseLock.Create(context.Background(), testRecord); err != nil {
		t.Fatalf("Failed to create lease: %v", err)
	}

	// Test Get
	record, recordBytes, err := leaseLock.Get(context.Background())
	if err != nil {
		t.Fatalf("Failed to get lease: %v", err)
	}

	// Verify record matches what we created
	if record.HolderIdentity != testRecord.HolderIdentity {
		t.Errorf("HolderIdentity mismatch, got %q want %q", record.HolderIdentity, testRecord.HolderIdentity)
	}
	if record.LeaseDurationSeconds != testRecord.LeaseDurationSeconds {
		t.Errorf("LeaseDurationSeconds mismatch, got %d want %d", record.LeaseDurationSeconds, testRecord.LeaseDurationSeconds)
	}
	if record.LeaderTransitions != testRecord.LeaderTransitions {
		t.Errorf("LeaderTransitions mismatch, got %d want %d", record.LeaderTransitions, testRecord.LeaderTransitions)
	}
	if record.PreferredHolder != testRecord.PreferredHolder {
		t.Errorf("PreferredHolder mismatch, got %q want %q", record.PreferredHolder, testRecord.PreferredHolder)
	}
	if record.Strategy != testRecord.Strategy {
		t.Errorf("Strategy mismatch, got %q want %q", record.Strategy, testRecord.Strategy)
	}

	// Verify we got valid JSON bytes
	var unmarshalled LeaderElectionRecord
	if err := json.Unmarshal(recordBytes, &unmarshalled); err != nil {
		t.Errorf("Failed to unmarshal record bytes: %v", err)
	}
}

// TestRecordEventSuccess 校验事件消息前缀为 identity，便于在事件流中识别持有者。
func TestRecordEventSuccess(t *testing.T) {
	setup()

	leaseLock.LockConfig.EventRecorder = recorder

	// Create a lease first
	if err := leaseLock.Create(context.Background(), testRecord); err != nil {
		t.Fatalf("Failed to create lease: %v", err)
	}

	// Test recording an event
	leaseLock.RecordEvent("test event")

	// Verify event was recorded
	if len(recorder.events) != 1 {
		t.Errorf("Expected 1 event, got %d", len(recorder.events))
	}
	expectedEvent := fmt.Sprintf("%v %v", testIdentity, "test event")
	if recorder.events[0] != expectedEvent {
		t.Errorf("Event mismatch, got %q want %q", recorder.events[0], expectedEvent)
	}
}

// TestRecordEventNilRecorder 校验未配置 recorder 时记录事件不会 panic。
func TestRecordEventNilRecorder(t *testing.T) {
	setup()

	// Create a lease first
	if err := leaseLock.Create(context.Background(), testRecord); err != nil {
		t.Fatalf("Failed to create lease: %v", err)
	}

	// Verify nil recorder doesn't panic
	leaseLock.LockConfig.EventRecorder = nil
	leaseLock.RecordEvent("should not panic")
}

// TestCreateError 校验重复创建 Lease 返回 AlreadyExists。
func TestCreateError(t *testing.T) {
	setup()

	// Create initial lease
	if err := leaseLock.Create(context.Background(), testRecord); err != nil {
		t.Fatalf("Failed to create lease: %v", err)
	}

	// Attempt to create same lease again
	err := leaseLock.Create(context.Background(), testRecord)
	if err == nil {
		t.Error("Expected error creating duplicate lease, got nil")
	}
	if !apierrors.IsAlreadyExists(err) {
		t.Errorf("Expected AlreadyExists error, got %v", err)
	}
}

// TestUpdateError 校验在未 Get 初始化 lease 前直接 Update 会失败。
func TestUpdateError(t *testing.T) {
	setup()

	// Attempt to update without initializing lease
	err := leaseLock.Update(context.Background(), testRecord)
	if err == nil {
		t.Error("Expected error updating uninitialized lease, got nil")
	}
}

// TestDescribe 校验 Describe 输出 namespace/name，用于日志与事件定位。
func TestDescribe(t *testing.T) {
	setup()

	expected := fmt.Sprintf("%v/%v", testNamespace, testName)
	if got := leaseLock.Describe(); got != expected {
		t.Errorf("Describe() = %q, want %q", got, expected)
	}
}

// TestLeaseConversion 校验 LeaderElectionRecord 与 LeaseSpec 双向转换不丢字段。
func TestLeaseConversion(t *testing.T) {
	setup()

	// Convert LeaderElectionRecord to LeaseSpec
	leaseSpec := LeaderElectionRecordToLeaseSpec(&testRecord)

	// Verify all fields were converted correctly
	if *leaseSpec.HolderIdentity != testRecord.HolderIdentity {
		t.Errorf("HolderIdentity mismatch, got %q want %q", *leaseSpec.HolderIdentity, testRecord.HolderIdentity)
	}
	if *leaseSpec.LeaseDurationSeconds != int32(testRecord.LeaseDurationSeconds) {
		t.Errorf("LeaseDurationSeconds mismatch, got %d want %d", *leaseSpec.LeaseDurationSeconds, testRecord.LeaseDurationSeconds)
	}
	if leaseSpec.AcquireTime.Time != testRecord.AcquireTime.Time {
		t.Errorf("AcquireTime mismatch, got %v want %v", leaseSpec.AcquireTime.Time, testRecord.AcquireTime.Time)
	}
	if leaseSpec.RenewTime.Time != testRecord.RenewTime.Time {
		t.Errorf("RenewTime mismatch, got %v want %v", leaseSpec.RenewTime.Time, testRecord.RenewTime.Time)
	}
	if *leaseSpec.LeaseTransitions != int32(testRecord.LeaderTransitions) {
		t.Errorf("LeaseTransitions mismatch, got %d want %d", *leaseSpec.LeaseTransitions, testRecord.LeaderTransitions)
	}
	if *leaseSpec.PreferredHolder != testRecord.PreferredHolder {
		t.Errorf("PreferredHolder mismatch, got %q want %q", *leaseSpec.PreferredHolder, testRecord.PreferredHolder)
	}
	if *leaseSpec.Strategy != testRecord.Strategy {
		t.Errorf("Strategy mismatch, got %q want %q", *leaseSpec.Strategy, testRecord.Strategy)
	}

	// Convert back to LeaderElectionRecord
	convertedRecord := LeaseSpecToLeaderElectionRecord(&leaseSpec)

	// Verify round-trip conversion preserved all fields
	if convertedRecord.HolderIdentity != testRecord.HolderIdentity {
		t.Errorf("HolderIdentity lost in conversion, got %q want %q", convertedRecord.HolderIdentity, testRecord.HolderIdentity)
	}
	if convertedRecord.LeaseDurationSeconds != testRecord.LeaseDurationSeconds {
		t.Errorf("LeaseDurationSeconds lost in conversion, got %d want %d", convertedRecord.LeaseDurationSeconds, testRecord.LeaseDurationSeconds)
	}
	if !convertedRecord.AcquireTime.Equal(&testRecord.AcquireTime) {
		t.Errorf("AcquireTime lost in conversion, got %v want %v", convertedRecord.AcquireTime, testRecord.AcquireTime)
	}
	if !convertedRecord.RenewTime.Equal(&testRecord.RenewTime) {
		t.Errorf("RenewTime lost in conversion, got %v want %v", convertedRecord.RenewTime, testRecord.RenewTime)
	}
	if convertedRecord.LeaderTransitions != testRecord.LeaderTransitions {
		t.Errorf("LeaderTransitions lost in conversion, got %d want %d", convertedRecord.LeaderTransitions, testRecord.LeaderTransitions)
	}
	if convertedRecord.PreferredHolder != testRecord.PreferredHolder {
		t.Errorf("PreferredHolder lost in conversion, got %q want %q", convertedRecord.PreferredHolder, testRecord.PreferredHolder)
	}
	if convertedRecord.Strategy != testRecord.Strategy {
		t.Errorf("Strategy lost in conversion, got %q want %q", convertedRecord.Strategy, testRecord.Strategy)
	}
}

// TestUpdateWithNilLabelsOnLease 校验 Lease 上 Labels 为 nil 时 Update 仍能补齐锁配置中的标签。
func TestUpdateWithNilLabelsOnLease(t *testing.T) {
	setup()

	// Create initial lease
	if err := leaseLock.Create(context.Background(), testRecord); err != nil {
		t.Fatalf("Failed to create lease: %v", err)
	}
	// Get the lease to initialize leaseLock.lease
	if _, _, err := leaseLock.Get(context.Background()); err != nil {
		t.Fatalf("Failed to get lease: %v", err)
	}

	leaseLock.lease.Labels = nil

	leaseLock.Labels = map[string]string{"custom-key": "custom-val"}

	// Update should succeed even with nil Labels on the lease itself
	if err := leaseLock.Update(context.Background(), testRecord); err != nil {
		t.Errorf("Update failed with nil Labels: %v", err)
	}
}

// TestUpdateWithNilLabelsOnLeaseLock 校验 LeaseLock.Labels 为 nil 时不会触发 nil map 写入 panic。
func TestUpdateWithNilLabelsOnLeaseLock(t *testing.T) {
	setup()

	// Create initial lease
	if err := leaseLock.Create(context.Background(), testRecord); err != nil {
		t.Fatalf("Failed to create lease: %v", err)
	}
	// Get the lease to initialize leaseLock.lease
	if _, _, err := leaseLock.Get(context.Background()); err != nil {
		t.Fatalf("Failed to get lease: %v", err)
	}

	leaseLock.Labels = nil

	leaseLock.lease.Labels = map[string]string{"custom-key": "custom-val"}

	// Update should succeed even with nil Labels on the leaselock
	if err := leaseLock.Update(context.Background(), testRecord); err != nil {
		t.Errorf("Update failed with nil Labels: %v", err)
	}
}

// TestLabelUpdate 校验锁配置中的标签会覆盖 Lease 上的同名标签，同时保留其他既有标签。
func TestLabelUpdate(t *testing.T) {
	setup()

	labels := map[string]string{"custom-key": "custom-val"}
	leaseLock.Labels = labels

	// Create initial lease
	if err := leaseLock.Create(context.Background(), testRecord); err != nil {
		t.Fatalf("Failed to create lease: %v", err)
	}

	// Update label on actual lease with new value
	leaseLock.lease.Labels["custom-key"] = "new-custom-val"

	// Update extra label on lease
	leaseLock.lease.Labels["custom-key-2"] = "custom-val-2"

	// Update labels
	lease, err := leaseLock.Client.Leases(testNamespace).Update(context.Background(), leaseLock.lease, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update lease labels: %v", err)
	}
	// verify the labels were updated
	val, exists := lease.Labels["custom-key"]
	if !exists {
		t.Error("Label was not set on the lease")
	}
	if val != "new-custom-val" {
		t.Errorf("Label value mismatch, got %q want %q", val, "custom-val")
	}

	val, exists = lease.Labels["custom-key-2"]
	if !exists {
		t.Error("Label was not set on the lease")
	}
	if val != "custom-val-2" {
		t.Errorf("Label value mismatch, got %q want %q", val, "custom-val")
	}

	// Update the lease
	if err := leaseLock.Update(context.Background(), testRecord); err != nil {
		t.Fatalf("Failed to update lease: %v", err)
	}

	// Verify labels are preserved
	lease, err = leaseLock.Client.Leases(testNamespace).Get(context.Background(), testName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to update lease labels: %v", err)
	}

	val, exists = lease.Labels["custom-key"]
	if !exists {
		t.Error("Label was not set on the lease")
	}
	if val != "custom-val" {
		t.Errorf("Label value mismatch, got %q want %q", val, "custom-val")
	}
	val, exists = lease.Labels["custom-key-2"]
	if !exists {
		t.Error("Label was not set on the lease")
	}
	if val != "custom-val-2" {
		t.Errorf("Label value mismatch, got %q want %q", val, "custom-val-2")
	}
}
