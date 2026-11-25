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

package leaderelection

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/fake"
	fakeclient "k8s.io/client-go/testing"
	rl "k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/utils/clock"
)

// createLockObject 按对象类型构造锁对象；record 非 nil 时以注解/LeaseSpec 形式写入。
func createLockObject(t *testing.T, objectType, namespace, name string, record *rl.LeaderElectionRecord) (obj runtime.Object) {
	objectMeta := metav1.ObjectMeta{
		Namespace: namespace,
		Name:      name,
	}
	if record != nil {
		recordBytes, _ := json.Marshal(record)
		objectMeta.Annotations = map[string]string{
			rl.LeaderElectionRecordAnnotationKey: string(recordBytes),
		}
	}
	switch objectType {
	case "endpoints":
		obj = &corev1.Endpoints{ObjectMeta: objectMeta}
	case "configmaps":
		obj = &corev1.ConfigMap{ObjectMeta: objectMeta}
	case "leases":
		// Lease 使用结构化字段而非注解保存记录，需做一次转换。
		var spec coordinationv1.LeaseSpec
		if record != nil {
			spec = rl.LeaderElectionRecordToLeaseSpec(record)
		}
		obj = &coordinationv1.Lease{ObjectMeta: objectMeta, Spec: spec}
	default:
		t.Fatal("unexpected objType:" + objectType)
	}
	return
}

// Reactor 描述 fake clientset 上注册的单条反应器：verb + 对象类型 + 处理函数。
type Reactor struct {
	verb       string
	objectType string
	reaction   fakeclient.ReactionFunc
}

// testTryAcquireOrRenew 表驱动验证 tryAcquireOrRenew 的获取/续约决策：
// 在“无对象、无注解、他持有且过期/未过期、自己持有”等场景下是否成功，
// 以及 HolderIdentity 与 LeaderTransitions 的期望值。
func testTryAcquireOrRenew(t *testing.T, objectType string) {
	clock := clock.RealClock{}
	future := clock.Now().Add(1000 * time.Hour)
	past := clock.Now().Add(-1000 * time.Hour)

	tests := []struct {
		name           string
		observedRecord rl.LeaderElectionRecord
		observedTime   time.Time
		retryAfter     time.Duration
		reactors       []Reactor
		expectedEvents []string

		expectSuccess    bool
		transitionLeader bool
		outHolder        string
	}{
		{
			name: "acquire from no object",
			reactors: []Reactor{
				{
					verb: "get",
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, nil, errors.NewNotFound(action.(fakeclient.GetAction).GetResource().GroupResource(), action.(fakeclient.GetAction).GetName())
					},
				},
				{
					verb: "create",
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, action.(fakeclient.CreateAction).GetObject(), nil
					},
				},
			},
			expectSuccess: true,
			outHolder:     "baz",
		},
		{
			name: "acquire from object without annotations",
			reactors: []Reactor{
				{
					verb: "get",
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, createLockObject(t, objectType, action.GetNamespace(), action.(fakeclient.GetAction).GetName(), nil), nil
					},
				},
				{
					verb: "update",
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, action.(fakeclient.CreateAction).GetObject(), nil
					},
				},
			},
			expectSuccess:    true,
			transitionLeader: true,
			outHolder:        "baz",
		},
		{
			name: "acquire from led object with the lease duration seconds",
			reactors: []Reactor{
				{
					verb: "get",
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, createLockObject(t, objectType, action.GetNamespace(), action.(fakeclient.GetAction).GetName(), &rl.LeaderElectionRecord{HolderIdentity: "bing", LeaseDurationSeconds: 3}), nil
					},
				},
				{
					verb: "get",
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, createLockObject(t, objectType, action.GetNamespace(), action.(fakeclient.GetAction).GetName(), &rl.LeaderElectionRecord{HolderIdentity: "bing", LeaseDurationSeconds: 3}), nil
					},
				},
				{
					verb: "update",
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, action.(fakeclient.CreateAction).GetObject(), nil
					},
				},
			},
			retryAfter:       3 * time.Second,
			expectSuccess:    true,
			transitionLeader: true,
			outHolder:        "baz",
		},
		{
			name: "acquire from unled object",
			reactors: []Reactor{
				{
					verb: "get",
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, createLockObject(t, objectType, action.GetNamespace(), action.(fakeclient.GetAction).GetName(), &rl.LeaderElectionRecord{}), nil
					},
				},
				{
					verb: "update",
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, action.(fakeclient.CreateAction).GetObject(), nil
					},
				},
			},

			expectSuccess:    true,
			transitionLeader: true,
			outHolder:        "baz",
		},
		{
			name: "acquire from led, unacked object",
			reactors: []Reactor{
				{
					verb: "get",
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, createLockObject(t, objectType, action.GetNamespace(), action.(fakeclient.GetAction).GetName(), &rl.LeaderElectionRecord{HolderIdentity: "bing"}), nil
					},
				},
				{
					verb: "update",
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, action.(fakeclient.CreateAction).GetObject(), nil
					},
				},
			},
			observedRecord: rl.LeaderElectionRecord{HolderIdentity: "bing"},
			observedTime:   past,

			expectSuccess:    true,
			transitionLeader: true,
			outHolder:        "baz",
		},
		{
			name: "acquire from empty led, acked object",
			reactors: []Reactor{
				{
					verb: "get",
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, createLockObject(t, objectType, action.GetNamespace(), action.(fakeclient.GetAction).GetName(), &rl.LeaderElectionRecord{HolderIdentity: ""}), nil
					},
				},
				{
					verb: "update",
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, action.(fakeclient.CreateAction).GetObject(), nil
					},
				},
			},
			observedTime: future,

			expectSuccess:    true,
			transitionLeader: true,
			outHolder:        "baz",
		},
		{
			name: "don't acquire from led, acked object",
			reactors: []Reactor{
				{
					verb: "get",
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, createLockObject(t, objectType, action.GetNamespace(), action.(fakeclient.GetAction).GetName(), &rl.LeaderElectionRecord{HolderIdentity: "bing"}), nil
					},
				},
			},
			observedTime: future,

			expectSuccess: false,
			outHolder:     "bing",
		},
		{
			name: "renew already acquired object",
			reactors: []Reactor{
				{
					verb: "get",
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, createLockObject(t, objectType, action.GetNamespace(), action.(fakeclient.GetAction).GetName(), &rl.LeaderElectionRecord{HolderIdentity: "baz"}), nil
					},
				},
				{
					verb: "update",
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, action.(fakeclient.CreateAction).GetObject(), nil
					},
				},
			},
			observedTime:   future,
			observedRecord: rl.LeaderElectionRecord{HolderIdentity: "baz"},

			expectSuccess: true,
			outHolder:     "baz",
		},
	}

	for i := range tests {
		test := &tests[i]
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)

			// OnNewLeader 是异步调用的，因此需要等待它。
			var wg sync.WaitGroup
			wg.Add(1)
			var reportedLeader string
			var lock rl.Interface

			objectMeta := metav1.ObjectMeta{Namespace: "foo", Name: "bar"}
			recorder := record.NewFakeRecorder(100)
			resourceLockConfig := rl.ResourceLockConfig{
				Identity:      "baz",
				EventRecorder: recorder,
			}
			c := &fake.Clientset{}
			for _, reactor := range test.reactors {
				c.AddReactor(reactor.verb, objectType, reactor.reaction)
			}
			c.AddReactor("*", "*", func(action fakeclient.Action) (bool, runtime.Object, error) {
				t.Errorf("unreachable action. testclient called too many times: %+v", action)
				return true, nil, fmt.Errorf("unreachable action")
			})

			switch objectType {
			case "leases":
				lock = &rl.LeaseLock{
					LeaseMeta:  objectMeta,
					LockConfig: resourceLockConfig,
					Client:     c.CoordinationV1(),
				}
			default:
				t.Fatalf("Unknown objectType: %v", objectType)
			}

			lec := LeaderElectionConfig{
				Lock:          lock,
				LeaseDuration: 10 * time.Second,
				Callbacks: LeaderCallbacks{
					OnNewLeader: func(l string) {
						defer wg.Done()
						reportedLeader = l
					},
				},
			}
			observedRawRecord := GetRawRecordOrDie(t, objectType, test.observedRecord)
			le := &LeaderElector{
				config:            lec,
				observedRecord:    test.observedRecord,
				observedRawRecord: observedRawRecord,
				observedTime:      test.observedTime,
				clock:             clock,
				metrics:           globalMetricsFactory.newLeaderMetrics(),
			}
			if test.expectSuccess != le.tryAcquireOrRenew(ctx) {
				if test.retryAfter != 0 {
					time.Sleep(test.retryAfter)
					if test.expectSuccess != le.tryAcquireOrRenew(ctx) {
						t.Errorf("unexpected result of tryAcquireOrRenew: [succeeded=%v]", !test.expectSuccess)
					}
				} else {
					t.Errorf("unexpected result of tryAcquireOrRenew: [succeeded=%v]", !test.expectSuccess)
				}
			}

			le.observedRecord.AcquireTime = metav1.Time{}
			le.observedRecord.RenewTime = metav1.Time{}
			if le.observedRecord.HolderIdentity != test.outHolder {
				t.Errorf("expected holder:\n\t%+v\ngot:\n\t%+v", test.outHolder, le.observedRecord.HolderIdentity)
			}
			if len(test.reactors) != len(c.Actions()) {
				t.Errorf("wrong number of api interactions")
			}
			if test.transitionLeader && le.observedRecord.LeaderTransitions != 1 {
				t.Errorf("leader should have transitioned but did not")
			}
			if !test.transitionLeader && le.observedRecord.LeaderTransitions != 0 {
				t.Errorf("leader should not have transitioned but did")
			}

			le.maybeReportTransition()
			wg.Wait()
			if reportedLeader != test.outHolder {
				t.Errorf("reported leader was not the new leader. expected %q, got %q", test.outHolder, reportedLeader)
			}
			assertEqualEvents(t, test.expectedEvents, recorder.Events)
		})
	}
}

// TestTryCoordinatedRenew 验证 Coordinated 模式下的续约路径：
// 仅当他人的租约已过期时才可接管，否则必须保持现状。
func TestTryCoordinatedRenew(t *testing.T) {
	objectType := "leases"
	clock := clock.RealClock{}
	future := clock.Now().Add(1000 * time.Hour)

	tests := []struct {
		name           string
		observedRecord rl.LeaderElectionRecord
		observedTime   time.Time
		retryAfter     time.Duration
		reactors       []Reactor
		expectedEvents []string

		expectSuccess    bool
		transitionLeader bool
		outHolder        string
	}{
		{
			name: "don't acquire from led, acked object",
			reactors: []Reactor{
				{
					verb: "get",
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, createLockObject(t, objectType, action.GetNamespace(), action.(fakeclient.GetAction).GetName(), &rl.LeaderElectionRecord{HolderIdentity: "bing"}), nil
					},
				},
			},
			observedTime: future,

			expectSuccess: false,
			outHolder:     "bing",
		},
		{
			name: "renew already acquired object",
			reactors: []Reactor{
				{
					verb: "get",
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, createLockObject(t, objectType, action.GetNamespace(), action.(fakeclient.GetAction).GetName(), &rl.LeaderElectionRecord{HolderIdentity: "baz"}), nil
					},
				},
				{
					verb: "update",
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, action.(fakeclient.CreateAction).GetObject(), nil
					},
				},
			},
			observedTime:   future,
			observedRecord: rl.LeaderElectionRecord{HolderIdentity: "baz"},

			expectSuccess: true,
			outHolder:     "baz",
		},
	}

	for i := range tests {
		test := &tests[i]
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)

			// OnNewLeader 是异步调用的，因此需要等待它。
			var wg sync.WaitGroup
			wg.Add(1)
			var reportedLeader string
			var lock rl.Interface

			objectMeta := metav1.ObjectMeta{Namespace: "foo", Name: "bar"}
			recorder := record.NewFakeRecorder(100)
			resourceLockConfig := rl.ResourceLockConfig{
				Identity:      "baz",
				EventRecorder: recorder,
			}
			c := &fake.Clientset{}
			for _, reactor := range test.reactors {
				c.AddReactor(reactor.verb, objectType, reactor.reaction)
			}
			c.AddReactor("*", "*", func(action fakeclient.Action) (bool, runtime.Object, error) {
				t.Errorf("unreachable action. testclient called too many times: %+v", action)
				return true, nil, fmt.Errorf("unreachable action")
			})

			lock = &rl.LeaseLock{
				LeaseMeta:  objectMeta,
				LockConfig: resourceLockConfig,
				Client:     c.CoordinationV1(),
			}
			lec := LeaderElectionConfig{
				Lock:          lock,
				LeaseDuration: 10 * time.Second,
				Callbacks: LeaderCallbacks{
					OnNewLeader: func(l string) {
						defer wg.Done()
						reportedLeader = l
					},
				},
				Coordinated: true,
			}
			observedRawRecord := GetRawRecordOrDie(t, objectType, test.observedRecord)
			le := &LeaderElector{
				config:            lec,
				observedRecord:    test.observedRecord,
				observedRawRecord: observedRawRecord,
				observedTime:      test.observedTime,
				clock:             clock,
				metrics:           globalMetricsFactory.newLeaderMetrics(),
			}
			if test.expectSuccess != le.tryCoordinatedRenew(ctx) {
				if test.retryAfter != 0 {
					time.Sleep(test.retryAfter)
					if test.expectSuccess != le.tryCoordinatedRenew(ctx) {
						t.Errorf("unexpected result of tryCoordinatedRenew: [succeeded=%v]", !test.expectSuccess)
					}
				} else {
					t.Errorf("unexpected result of gryCoordinatedRenew: [succeeded=%v]", !test.expectSuccess)
				}
			}

			le.observedRecord.AcquireTime = metav1.Time{}
			le.observedRecord.RenewTime = metav1.Time{}
			if le.observedRecord.HolderIdentity != test.outHolder {
				t.Errorf("expected holder:\n\t%+v\ngot:\n\t%+v", test.outHolder, le.observedRecord.HolderIdentity)
			}
			if len(test.reactors) != len(c.Actions()) {
				t.Errorf("wrong number of api interactions")
			}
			if test.transitionLeader && le.observedRecord.LeaderTransitions != 1 {
				t.Errorf("leader should have transitioned but did not")
			}
			if !test.transitionLeader && le.observedRecord.LeaderTransitions != 0 {
				t.Errorf("leader should not have transitioned but did")
			}

			le.maybeReportTransition()
			wg.Wait()
			if reportedLeader != test.outHolder {
				t.Errorf("reported leader was not the new leader. expected %q, got %q", test.outHolder, reportedLeader)
			}
			assertEqualEvents(t, test.expectedEvents, recorder.Events)
		})
	}
}

// 以 Lease 为资源运行 tryAcquireOrRenew 的全部用例，测试 leader 选举。
func TestTryAcquireOrRenewLeases(t *testing.T) {
	testTryAcquireOrRenew(t, "leases")
}

// TestLeaseSpecToLeaderElectionRecordRoundTrip 验证 LeaseSpec 与 LeaderElectionRecord
// 双向转换后语义不变（字段一一对应且无精度损失）。
func TestLeaseSpecToLeaderElectionRecordRoundTrip(t *testing.T) {
	holderIdentity := "foo"
	leaseDurationSeconds := int32(10)
	leaseTransitions := int32(1)
	oldSpec := coordinationv1.LeaseSpec{
		HolderIdentity:       &holderIdentity,
		LeaseDurationSeconds: &leaseDurationSeconds,
		AcquireTime:          &metav1.MicroTime{Time: time.Now()},
		RenewTime:            &metav1.MicroTime{Time: time.Now()},
		LeaseTransitions:     &leaseTransitions,
	}

	oldRecord := rl.LeaseSpecToLeaderElectionRecord(&oldSpec)
	newSpec := rl.LeaderElectionRecordToLeaseSpec(oldRecord)

	if !equality.Semantic.DeepEqual(oldSpec, newSpec) {
		t.Errorf("diff: %v", cmp.Diff(oldSpec, newSpec))
	}

	newRecord := rl.LeaseSpecToLeaderElectionRecord(&newSpec)

	if !equality.Semantic.DeepEqual(oldRecord, newRecord) {
		t.Errorf("diff: %v", cmp.Diff(oldRecord, newRecord))
	}
}

// GetRawRecordOrDie 序列化记录为 raw bytes，模拟从 API 读到的原始注解内容。
func GetRawRecordOrDie(t *testing.T, objectType string, ler rl.LeaderElectionRecord) (ret []byte) {
	var err error
	switch objectType {
	case "leases":
		ret, err = json.Marshal(ler)
		if err != nil {
			t.Fatalf("lock %s get raw record %v failed: %v", objectType, ler, err)
		}
	default:
		t.Fatal("unexpected objType:" + objectType)
	}
	return
}

// testReleaseLease 验证主动释放锁：获取成功后 release 会把 HolderIdentity 清空。
func testReleaseLease(t *testing.T, objectType string) {
	tests := []struct {
		name           string
		observedRecord rl.LeaderElectionRecord
		observedTime   time.Time
		reactors       []Reactor
		expectedEvents []string

		expectSuccess    bool
		transitionLeader bool
		outHolder        string
	}{
		{
			name: "release acquired lock from no object",
			reactors: []Reactor{
				{
					verb:       "get",
					objectType: objectType,
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, createLockObject(t, objectType, action.GetNamespace(), action.(fakeclient.GetAction).GetName(), &rl.LeaderElectionRecord{HolderIdentity: "baz"}), nil
					},
				},
				{
					verb:       "create",
					objectType: objectType,
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, action.(fakeclient.CreateAction).GetObject(), nil
					},
				},
				{
					verb:       "update",
					objectType: objectType,
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, action.(fakeclient.UpdateAction).GetObject(), nil
					},
				},
				{
					verb:       "get",
					objectType: objectType,
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						return true, nil, errors.NewNotFound(action.(fakeclient.GetAction).GetResource().GroupResource(), action.(fakeclient.GetAction).GetName())
					},
				},
			},
			expectSuccess: true,
			outHolder:     "",
		},
	}

	for i := range tests {
		test := &tests[i]
		t.Run(test.name, func(t *testing.T) {
			logger, ctx := ktesting.NewTestContext(t)

			// OnNewLeader 是异步调用的，因此需要等待它。
			var wg sync.WaitGroup
			wg.Add(1)
			var reportedLeader string
			var lock rl.Interface

			objectMeta := metav1.ObjectMeta{Namespace: "foo", Name: "bar"}
			recorder := record.NewFakeRecorder(100)
			resourceLockConfig := rl.ResourceLockConfig{
				Identity:      "baz",
				EventRecorder: recorder,
			}
			c := &fake.Clientset{}
			for _, reactor := range test.reactors {
				c.AddReactor(reactor.verb, objectType, reactor.reaction)
			}
			c.AddReactor("*", "*", func(action fakeclient.Action) (bool, runtime.Object, error) {
				t.Errorf("unreachable action. testclient called too many times: %+v", action)
				return true, nil, fmt.Errorf("unreachable action")
			})

			switch objectType {
			case "leases":
				lock = &rl.LeaseLock{
					LeaseMeta:  objectMeta,
					LockConfig: resourceLockConfig,
					Client:     c.CoordinationV1(),
				}
			default:
				t.Fatalf("Unknown objectType: %v", objectType)
			}

			lec := LeaderElectionConfig{
				Lock:          lock,
				LeaseDuration: 10 * time.Second,
				Callbacks: LeaderCallbacks{
					OnNewLeader: func(l string) {
						defer wg.Done()
						reportedLeader = l
					},
				},
			}
			observedRawRecord := GetRawRecordOrDie(t, objectType, test.observedRecord)
			le := &LeaderElector{
				config:            lec,
				observedRecord:    test.observedRecord,
				observedRawRecord: observedRawRecord,
				observedTime:      test.observedTime,
				clock:             clock.RealClock{},
				metrics:           globalMetricsFactory.newLeaderMetrics(),
			}
			if !le.tryAcquireOrRenew(ctx) {
				t.Errorf("unexpected result of tryAcquireOrRenew: [succeeded=%v]", true)
			}

			le.maybeReportTransition()

			// 等待对 leader 切换的响应，并加 1 以便跟踪最后一次切换。
			wg.Wait()
			wg.Add(1)

			if test.expectSuccess != le.release(logger) {
				t.Errorf("unexpected result of release: [succeeded=%v]", !test.expectSuccess)
			}

			le.observedRecord.AcquireTime = metav1.Time{}
			le.observedRecord.RenewTime = metav1.Time{}
			if le.observedRecord.HolderIdentity != test.outHolder {
				t.Errorf("expected holder:\n\t%+v\ngot:\n\t%+v", test.outHolder, le.observedRecord.HolderIdentity)
			}
			if len(test.reactors) != len(c.Actions()) {
				t.Errorf("wrong number of api interactions")
			}
			if test.transitionLeader && le.observedRecord.LeaderTransitions != 1 {
				t.Errorf("leader should have transitioned but did not")
			}
			if !test.transitionLeader && le.observedRecord.LeaderTransitions != 0 {
				t.Errorf("leader should not have transitioned but did")
			}
			le.maybeReportTransition()
			wg.Wait()
			if reportedLeader != test.outHolder {
				t.Errorf("reported leader was not the new leader. expected %q, got %q", test.outHolder, reportedLeader)
			}
			assertEqualEvents(t, test.expectedEvents, recorder.Events)
		})
	}
}

// 历史上以 endpoints 资源测试 leader 选举，实际现在只测试 Lease。
func TestReleaseLeaseLeases(t *testing.T) {
	testReleaseLease(t, "leases")
}

// TestReleaseMethodCallsGet 验证 release 会先 Get 最新锁内容再 Update，避免用过期的 record 覆盖。
func TestReleaseMethodCallsGet(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	objectType := "leases"
	getCalled := false

	lockMeta := metav1.ObjectMeta{Namespace: "foo", Name: "bar"}
	recorder := record.NewFakeRecorder(100)
	resourceLockConfig := rl.ResourceLockConfig{
		Identity:      "baz",
		EventRecorder: recorder,
	}
	c := &fake.Clientset{}
	c.AddReactor("get", objectType, func(action fakeclient.Action) (bool, runtime.Object, error) {
		// 用于检查 Get 是否被调用的标志
		getCalled = true
		return true, createLockObject(t, objectType, action.GetNamespace(), action.(fakeclient.GetAction).GetName(), &rl.LeaderElectionRecord{
			HolderIdentity:       "baz",
			LeaseDurationSeconds: 10,
		}), nil
	})
	c.AddReactor("update", objectType, func(action fakeclient.Action) (bool, runtime.Object, error) {
		return true, action.(fakeclient.UpdateAction).GetObject(), nil
	})

	lock := &rl.LeaseLock{
		LeaseMeta:  lockMeta,
		LockConfig: resourceLockConfig,
		Client:     c.CoordinationV1(),
	}
	lec := LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: 10 * time.Second,
		Callbacks: LeaderCallbacks{
			OnNewLeader: func(l string) {},
		},
	}
	observedRawRecord := GetRawRecordOrDie(t, objectType, rl.LeaderElectionRecord{HolderIdentity: "baz"})
	le := &LeaderElector{
		config:            lec,
		observedRecord:    rl.LeaderElectionRecord{HolderIdentity: "baz"},
		observedRawRecord: observedRawRecord,
		observedTime:      time.Now(),
		clock:             clock.RealClock{},
		metrics:           globalMetricsFactory.newLeaderMetrics(),
	}

	le.release(logger)

	if !getCalled {
		t.Errorf("release method does not call Get")
	}
}

// TestReleaseRetriesOnConflict 验证当 Update 调用返回冲突错误
// （例如来自进行中的 renew）时 release 会重试。
//
// 首次 Update 返回 Conflict，release 应重新 Get 并重试直至成功。
func TestReleaseRetriesOnConflict(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	objectType := "leases"
	var conflicted bool

	lockMeta := metav1.ObjectMeta{Namespace: "foo", Name: "bar"}
	recorder := record.NewFakeRecorder(100)
	resourceLockConfig := rl.ResourceLockConfig{
		Identity:      "baz",
		EventRecorder: recorder,
	}
	c := &fake.Clientset{}
	c.AddReactor("get", objectType, func(action fakeclient.Action) (bool, runtime.Object, error) {
		return true, createLockObject(t, objectType, action.GetNamespace(), action.(fakeclient.GetAction).GetName(), &rl.LeaderElectionRecord{
			HolderIdentity:       "baz",
			LeaseDurationSeconds: 10,
		}), nil
	})
	c.AddReactor("update", objectType, func(action fakeclient.Action) (bool, runtime.Object, error) {
		if !conflicted {
			conflicted = true
			return true, nil, errors.NewConflict(coordinationv1.Resource("leases"), "bar", fmt.Errorf("the object has been modified"))
		}
		return true, action.(fakeclient.UpdateAction).GetObject(), nil
	})

	lock := &rl.LeaseLock{
		LeaseMeta:  lockMeta,
		LockConfig: resourceLockConfig,
		Client:     c.CoordinationV1(),
	}
	lec := LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: 10 * time.Second,
		RenewDeadline: 5 * time.Second,
		Callbacks: LeaderCallbacks{
			OnNewLeader: func(l string) {},
		},
	}
	observedRawRecord := GetRawRecordOrDie(t, objectType, rl.LeaderElectionRecord{HolderIdentity: "baz"})
	le := &LeaderElector{
		config:            lec,
		observedRecord:    rl.LeaderElectionRecord{HolderIdentity: "baz"},
		observedRawRecord: observedRawRecord,
		observedTime:      time.Now(),
		clock:             clock.RealClock{},
		metrics:           globalMetricsFactory.newLeaderMetrics(),
	}

	if !le.release(logger) {
		t.Error("release should have succeeded after retrying on conflict")
	}
	if !conflicted {
		t.Error("expected conflict to have occurred")
	}
}

// TestReleaseOnCancellation_Leases 验证 ReleaseOnCancel=true 时，ctx 取消后（无论
// 取消发生在 Update 还是 Get）都会尝试释放锁并记录 stopped leading 事件。
func TestReleaseOnCancellation_Leases(t *testing.T) {
	testReleaseOnCancellation(t, "leases")
}

// testReleaseOnCancellation 是两个取消场景的公共驱动器：
// 用 channel 精确控制 renew 的进行与恢复，从而断言“取消后仍会执行 release”。
func testReleaseOnCancellation(t *testing.T, objectType string) {
	var (
		onNewLeader   = make(chan struct{})
		onRenewCalled = make(chan struct{})
		onRenewResume = make(chan struct{})
		onRelease     = make(chan struct{})

		lockObj runtime.Object
		gets    int
		updates int
		wg      sync.WaitGroup
	)
	resetVars := func() {
		onNewLeader = make(chan struct{})
		onRenewCalled = make(chan struct{})
		onRenewResume = make(chan struct{})
		onRelease = make(chan struct{})

		lockObj = nil
		gets = 0
		updates = 0
	}
	lec := LeaderElectionConfig{
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 2 * time.Second,
		RetryPeriod:   1 * time.Second,

		// 关键配置（本用例的测试点）：取消时主动释放锁。
		ReleaseOnCancel: true,

		Callbacks: LeaderCallbacks{
			OnNewLeader:      func(identity string) {},
			OnStoppedLeading: func() {},
			OnStartedLeading: func(context.Context) {
				close(onNewLeader)
			},
		},
	}

	tests := []struct {
		name           string
		reactors       []Reactor
		expectedEvents []string
	}{
		{
			name: "release acquired lock on cancellation of update",
			reactors: []Reactor{
				{
					verb:       "get",
					objectType: objectType,
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						gets++
						if lockObj != nil {
							return true, lockObj, nil
						}
						return true, nil, errors.NewNotFound(action.(fakeclient.GetAction).GetResource().GroupResource(), action.(fakeclient.GetAction).GetName())
					},
				},
				{
					verb:       "create",
					objectType: objectType,
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						lockObj = action.(fakeclient.CreateAction).GetObject()
						return true, lockObj, nil
					},
				},
				{
					verb:       "update",
					objectType: objectType,
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						updates++
						// 跳过最初两次 fast path 续约
						if updates%2 == 1 && updates < 5 {
							return true, nil, context.Canceled
						}

						// 第二次 update（首次续约）应返回我们构造的 canceled 错误
						// FakeClient 不会处理 context，所以这里由我们自己模拟
						if updates == 4 {
							close(onRenewCalled)
							<-onRenewResume
							return true, nil, context.Canceled
						} else if updates == 5 {
							// 我们在取消之后再更新锁以释放它
							// 这个 wg 用于避免 lockObj 上的数据竞争
							defer wg.Done()
							close(onRelease)
						}

						lockObj = action.(fakeclient.UpdateAction).GetObject()
						return true, lockObj, nil
					},
				},
			},
			expectedEvents: []string{
				"Normal LeaderElection baz became leader",
				"Normal LeaderElection baz stopped leading",
			},
		},
		{
			name: "release acquired lock on cancellation of get",
			reactors: []Reactor{
				{
					verb:       "get",
					objectType: objectType,
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						gets++
						if lockObj != nil {
							// 第三次及以后的 get（第一次 create、第二次 renew）应返回我们构造的 canceled 错误
							// FakeClient 不会处理 context，所以这里由我们自己模拟
							if gets >= 4 {
								if gets == 4 {
									close(onRenewCalled)
									<-onRenewResume
								}
								return true, nil, context.Canceled
							}
							return true, lockObj, nil
						}
						return true, nil, errors.NewNotFound(action.(fakeclient.GetAction).GetResource().GroupResource(), action.(fakeclient.GetAction).GetName())
					},
				},
				{
					verb:       "create",
					objectType: objectType,
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						lockObj = action.(fakeclient.CreateAction).GetObject()
						return true, lockObj, nil
					},
				},
				{
					verb:       "update",
					objectType: objectType,
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						updates++
						// 总是跳过 fast path 续约
						if updates%2 == 1 {
							return true, nil, context.Canceled
						}
						// 第二次 update（首次续约）应释放锁
						if updates == 4 {
							// 我们在取消之后再更新锁以释放它
							// 这个 wg 用于避免 lockObj 上的数据竞争
							defer wg.Done()
							close(onRelease)
						}

						lockObj = action.(fakeclient.UpdateAction).GetObject()
						return true, lockObj, nil
					},
				},
			},
			expectedEvents: []string{
				"Normal LeaderElection baz became leader",
				"Normal LeaderElection baz stopped leading",
			},
		},
	}

	for i := range tests {
		test := &tests[i]
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)

			wg.Add(1)
			resetVars()

			recorder := record.NewFakeRecorder(100)
			resourceLockConfig := rl.ResourceLockConfig{
				Identity:      "baz",
				EventRecorder: recorder,
			}
			c := &fake.Clientset{}
			for _, reactor := range test.reactors {
				c.AddReactor(reactor.verb, objectType, reactor.reaction)
			}
			c.AddReactor("*", "*", func(action fakeclient.Action) (bool, runtime.Object, error) {
				t.Errorf("unreachable action. testclient called too many times: %+v", action)
				return true, nil, fmt.Errorf("unreachable action")
			})
			lock, err := rl.New(objectType, "foo", "bar", c.CoreV1(), c.CoordinationV1(), resourceLockConfig)
			if err != nil {
				t.Fatal("resourcelock.New() = ", err)
			}

			lec.Lock = lock
			elector, err := NewLeaderElector(lec)
			if err != nil {
				t.Fatal("Failed to create leader elector: ", err)
			}

			ctx, cancel := context.WithCancel(ctx)

			go elector.Run(ctx)

			// 等待本实例成为 leader
			select {
			case <-onNewLeader:
			case <-time.After(10 * time.Second):
				t.Fatal("failed to become the leader")
			}

			// 等待 renew（update）被调用
			select {
			case <-onRenewCalled:
			case <-time.After(10 * time.Second):
				t.Fatal("the elector failed to renew the lock")
			}

			// 取消 context —— 在 elector 运行过程中将其停止
			cancel()

			// 恢复 tryAcquireOrRenew 调用使其返回取消错误，
			// 从而触发 release 流程
			close(onRenewResume)

			select {
			case <-onRelease:
			case <-time.After(10 * time.Second):
				t.Fatal("the lock was not released")
			}
			wg.Wait()
			assertEqualEvents(t, test.expectedEvents, recorder.Events)
		})
	}
}

// TestLeaderElectionConfigValidation 验证 NewLeaderElector 的配置校验，
// 特别是空的 Lock identity 必须被拒绝。
func TestLeaderElectionConfigValidation(t *testing.T) {
	resourceLockConfig := rl.ResourceLockConfig{
		Identity: "baz",
	}

	lock := &rl.LeaseLock{
		LockConfig: resourceLockConfig,
	}

	lec := LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 2 * time.Second,
		RetryPeriod:   1 * time.Second,

		ReleaseOnCancel: true,

		Callbacks: LeaderCallbacks{
			OnNewLeader:      func(identity string) {},
			OnStoppedLeading: func() {},
			OnStartedLeading: func(context.Context) {},
		},
	}

	_, err := NewLeaderElector(lec)
	assert.NoError(t, err)

	// 非法的锁身份
	resourceLockConfig.Identity = ""
	lock.LockConfig = resourceLockConfig
	lec.Lock = lock
	_, err = NewLeaderElector(lec)
	assert.EqualError(t, err, "Lock identity is empty")
}

// assertEqualEvents 按顺序校验事件通道内容，并确认没有多余事件。
func assertEqualEvents(t *testing.T, expected []string, actual <-chan string) {
	c := time.After(wait.ForeverTestTimeout)
	for _, e := range expected {
		select {
		case a := <-actual:
			if e != a {
				t.Errorf("Expected event %q, got %q", e, a)
				return
			}
		case <-c:
			t.Errorf("Expected event %q, got nothing", e)
			// 继续迭代以便打印所有期望的事件
		}
	}
	for {
		select {
		case a := <-actual:
			t.Errorf("Unexpected event: %q", a)
		default:
			return // No more events, as expected.
		}
	}
}

// TestFastPathLeaderElection 验证快路径：已持有锁且未过期时，续约只需一次 Update，
// 不进行 Get；若 Update 冲突则回退到慢路径（Get + 重试）。
func TestFastPathLeaderElection(t *testing.T) {
	objectType := "leases"
	var (
		lockObj    runtime.Object
		updates    int
		lockOps    []string
		cancelFunc func()
	)
	resetVars := func() {
		lockObj = nil
		updates = 0
		lockOps = []string{}
		cancelFunc = nil
	}
	lec := LeaderElectionConfig{
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 2 * time.Second,
		RetryPeriod:   1 * time.Second,

		Callbacks: LeaderCallbacks{
			OnNewLeader:      func(identity string) {},
			OnStoppedLeading: func() {},
			OnStartedLeading: func(context.Context) {
			},
		},
	}

	tests := []struct {
		name            string
		reactors        []Reactor
		expectedLockOps []string
	}{
		{
			name: "Exercise fast path after lock acquired",
			reactors: []Reactor{
				{
					verb:       "get",
					objectType: objectType,
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						lockOps = append(lockOps, "get")
						if lockObj != nil {
							return true, lockObj, nil
						}
						return true, nil, errors.NewNotFound(action.(fakeclient.GetAction).GetResource().GroupResource(), action.(fakeclient.GetAction).GetName())
					},
				},
				{
					verb:       "create",
					objectType: objectType,
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						lockOps = append(lockOps, "create")
						lockObj = action.(fakeclient.CreateAction).GetObject()
						return true, lockObj, nil
					},
				},
				{
					verb:       "update",
					objectType: objectType,
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						updates++
						lockOps = append(lockOps, "update")
						if updates == 2 {
							cancelFunc()
						}
						lockObj = action.(fakeclient.UpdateAction).GetObject()
						return true, lockObj, nil
					},
				},
			},
			expectedLockOps: []string{"get", "create", "update", "update"},
		},
		{
			name: "Fallback to slow path after fast path fails",
			reactors: []Reactor{
				{
					verb:       "get",
					objectType: objectType,
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						lockOps = append(lockOps, "get")
						if lockObj != nil {
							return true, lockObj, nil
						}
						return true, nil, errors.NewNotFound(action.(fakeclient.GetAction).GetResource().GroupResource(), action.(fakeclient.GetAction).GetName())
					},
				},
				{
					verb:       "create",
					objectType: objectType,
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						lockOps = append(lockOps, "create")
						lockObj = action.(fakeclient.CreateAction).GetObject()
						return true, lockObj, nil
					},
				},
				{
					verb:       "update",
					objectType: objectType,
					reaction: func(action fakeclient.Action) (handled bool, ret runtime.Object, err error) {
						updates++
						lockOps = append(lockOps, "update")
						switch updates {
						case 2:
							return true, nil, errors.NewConflict(action.(fakeclient.UpdateAction).GetResource().GroupResource(), "fake conflict", nil)
						case 4:
							cancelFunc()
						}
						lockObj = action.(fakeclient.UpdateAction).GetObject()
						return true, lockObj, nil
					},
				},
			},
			expectedLockOps: []string{"get", "create", "update", "update", "get", "update", "update"},
		},
	}

	for i := range tests {
		test := &tests[i]
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)

			resetVars()

			recorder := record.NewFakeRecorder(100)
			resourceLockConfig := rl.ResourceLockConfig{
				Identity:      "baz",
				EventRecorder: recorder,
			}
			c := &fake.Clientset{}
			for _, reactor := range test.reactors {
				c.AddReactor(reactor.verb, objectType, reactor.reaction)
			}
			c.AddReactor("*", "*", func(action fakeclient.Action) (bool, runtime.Object, error) {
				t.Errorf("unreachable action. testclient called too many times: %+v", action)
				return true, nil, fmt.Errorf("unreachable action")
			})
			lock, err := rl.New("leases", "foo", "bar", c.CoreV1(), c.CoordinationV1(), resourceLockConfig)
			if err != nil {
				t.Fatal("resourcelock.New() = ", err)
			}

			lec.Lock = lock
			elector, err := NewLeaderElector(lec)
			if err != nil {
				t.Fatal("Failed to create leader elector: ", err)
			}

			ctx, cancel := context.WithCancel(ctx)
			cancelFunc = cancel

			elector.Run(ctx)
			assert.Equal(t, test.expectedLockOps, lockOps, "Expected lock ops %q, got %q", test.expectedLockOps, lockOps)
		})
	}
}

// TestCheckMethodRace 以激进的时间参数并发调用 Check 与 Run，
// 配合 -race 验证 observedRecord 的锁保护（读路径不产生数据竞争）。
func TestCheckMethodRace(t *testing.T) {
	objectType := "leases"
	objectMeta := metav1.ObjectMeta{Namespace: "foo", Name: "bar"}
	recorder := record.NewFakeRecorder(100)
	resourceLockConfig := rl.ResourceLockConfig{
		Identity:      "baz",
		EventRecorder: recorder,
	}

	var lockMu sync.Mutex
	var lockObj runtime.Object

	c := &fake.Clientset{}

	c.AddReactor("get", objectType, func(action fakeclient.Action) (bool, runtime.Object, error) {
		lockMu.Lock()
		defer lockMu.Unlock()
		if lockObj != nil {
			return true, lockObj, nil
		}
		return true, nil, errors.NewNotFound(action.(fakeclient.GetAction).GetResource().GroupResource(), action.(fakeclient.GetAction).GetName())
	})

	c.AddReactor("create", objectType, func(action fakeclient.Action) (bool, runtime.Object, error) {
		lockMu.Lock()
		defer lockMu.Unlock()
		lockObj = action.(fakeclient.CreateAction).GetObject()
		return true, lockObj, nil
	})

	c.AddReactor("update", objectType, func(action fakeclient.Action) (bool, runtime.Object, error) {
		lockMu.Lock()
		defer lockMu.Unlock()
		lockObj = action.(fakeclient.UpdateAction).GetObject()
		return true, lockObj, nil
	})

	lock := &rl.LeaseLock{
		LeaseMeta:  objectMeta,
		LockConfig: resourceLockConfig,
		Client:     c.CoordinationV1(),
	}

	// 激进的时序设置以诱发竞争：
	// 极短 RetryPeriod 让续约与 Check 高频交错，放大竞争窗口。
	lec := LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: 2 * time.Second,
		RenewDeadline: 1 * time.Second,
		RetryPeriod:   5 * time.Millisecond,
		Callbacks: LeaderCallbacks{
			OnNewLeader:      func(identity string) {},
			OnStartedLeading: func(context.Context) {},
			OnStoppedLeading: func() {},
		},
	}

	le, err := NewLeaderElector(lec)
	if err != nil {
		t.Fatalf("Failed to create leader elector: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go le.Run(ctx)

	start := time.Now()
	for time.Since(start) < 2*time.Second {
		// 如果 Check() 不加 observedRecordLock，这里的读取会与
		// le.Run() 中 setObservedRecord 的写入发生竞争。
		_ = le.Check(time.Second)

		time.Sleep(10 * time.Millisecond)
	}
}
