/*
Copyright 2023 The Kubernetes Authors.

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

package flowcontrol

import (
	"context"
	"testing"
	"time"

	flowcontrol "k8s.io/api/flowcontrol/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/endpoints/request"
	fq "k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing"
	fqs "k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing/queueset"
	"k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing/testing/eventclock"
	"k8s.io/apiserver/pkg/util/flowcontrol/metrics"
	fcrequest "k8s.io/apiserver/pkg/util/flowcontrol/request"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

// TestQueueWaitTimeLatencyTracker 验证调用 Handle 时 P&F 延迟追踪器记录的队列等待时间。
// 在调用 Handle 时记录。
//
// 通过 QueueNoteFn 推进 fake clock 来制造确定的排队时长，从而避免依赖
// 真实时间，稳定断言 APFQueueWaitTracker 的记录值。
func TestQueueWaitTimeLatencyTracker(t *testing.T) {
	// 指标是全局注册的，多个测试共用，必须先注册再构造控制器。
	metrics.Register()

	var fsObj *flowcontrol.FlowSchema
	var plcObj *flowcontrol.PriorityLevelConfiguration
	cfgObjs := []runtime.Object{}

	plName := "test-pl"
	username := "test-user"
	fsName := "test-fs"
	lendable := int32(0)
	borrowingLimit := int32(0)
	fsObj = &flowcontrol.FlowSchema{
		ObjectMeta: metav1.ObjectMeta{
			Name: fsName,
		},
		Spec: flowcontrol.FlowSchemaSpec{
			MatchingPrecedence: 100,
			PriorityLevelConfiguration: flowcontrol.PriorityLevelConfigurationReference{
				Name: plName,
			},
			DistinguisherMethod: &flowcontrol.FlowDistinguisherMethod{
				Type: flowcontrol.FlowDistinguisherMethodByUserType,
			},
			Rules: []flowcontrol.PolicyRulesWithSubjects{{
				Subjects: []flowcontrol.Subject{{
					Kind: flowcontrol.SubjectKindUser,
					User: &flowcontrol.UserSubject{Name: username},
				}},
				NonResourceRules: []flowcontrol.NonResourcePolicyRule{{
					Verbs:           []string{"*"},
					NonResourceURLs: []string{"*"},
				}},
			}},
		},
	}
	plcObj = &flowcontrol.PriorityLevelConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: plName,
		},
		Spec: flowcontrol.PriorityLevelConfigurationSpec{
			Type: flowcontrol.PriorityLevelEnablementLimited,
			Limited: &flowcontrol.LimitedPriorityLevelConfiguration{
				NominalConcurrencyShares: ptr.To(int32(100)),
				LendablePercent:          &lendable,
				BorrowingLimitPercent:    &borrowingLimit,
				LimitResponse: flowcontrol.LimitResponse{
					Type: flowcontrol.LimitResponseTypeQueue,
					Queuing: &flowcontrol.QueuingConfiguration{
						Queues:           10,
						HandSize:         2,
						QueueLengthLimit: 10,
					},
				},
			},
		},
	}
	cfgObjs = append(cfgObjs, fsObj, plcObj)

	clientset := clientsetfake.NewSimpleClientset(cfgObjs...)
	informerFactory := informers.NewSharedInformerFactory(clientset, time.Second)
	flowcontrolClient := clientset.FlowcontrolV1()
	startTime := time.Now()
	// 使用 fake clock 让排队耗时可控；QueueSetFactory 必须与该 clock
	// 绑定，否则队列内部计时与测试推进的时间不一致。
	clk, _ := eventclock.NewFake(startTime, 0, nil)
	controller := newTestableController(TestableConfig{
		Name:                   "Controller",
		Clock:                  clk,
		AsFieldManager:         ConfigConsumerAsFieldManager,
		FoundToDangling:        func(found bool) bool { return !found },
		InformerFactory:        informerFactory,
		FlowcontrolClient:      flowcontrolClient,
		ServerConcurrencyLimit: 24,
		ReqsGaugeVec:           metrics.PriorityLevelConcurrencyGaugeVec,
		ExecSeatsGaugeVec:      metrics.PriorityLevelExecutionSeatsGaugeVec,
		QueueSetFactory:        fqs.NewQueueSetFactory(clk),
	})

	stopCh := make(chan struct{})
	defer close(stopCh)

	informerFactory.Start(stopCh)
	status := informerFactory.WaitForCacheSync(stopCh)
	if names := unsynced(status); len(names) > 0 {
		t.Fatalf("WaitForCacheSync did not successfully complete, resources=%#v", names)
	}

	go func() {
		controller.Run(stopCh)
	}()

	// 确保控制器已跑完第一轮循环。
	err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		return controller.hasPriorityLevelState(plcObj.Name), nil
	})
	if err != nil {
		t.Errorf("expected the controller to reconcile the priority level configuration object: %s, error: %s", plcObj.Name, err)
	}

	reqInfo := &request.RequestInfo{
		IsResourceRequest: false,
		Path:              "/foobar",
		Verb:              "GET",
	}
	noteFn := func(fs *flowcontrol.FlowSchema, plc *flowcontrol.PriorityLevelConfiguration, fd string) {}
	workEstr := func() fcrequest.WorkEstimate { return fcrequest.WorkEstimate{InitialSeats: 1} }

	flowUser := testUser{name: "test-user"}
	rd := RequestDigest{
		RequestInfo: reqInfo,
		User:        flowUser,
	}

	// 在 QueueNoteFn 期间给假时钟加 1 秒。
	// QueueNoteFn 在请求入队/出队时被调用，借此把时钟推进 1s，模拟
	// 请求在队列中等待了 1 秒。
	newTime := startTime.Add(time.Second)
	qnf := fq.QueueNoteFn(func(bool) { clk.FakePassiveClock.SetTime(newTime) })
	ctx := request.WithLatencyTrackers(context.Background())
	controller.Handle(ctx, rd, noteFn, workEstr, qnf, func() {})

	latencyTracker, ok := request.LatencyTrackersFrom(ctx)
	if !ok {
		t.Fatalf("error getting latency tracker: %v", err)
	}

	expectedLatency := time.Second // newTime - startTime
	latency := latencyTracker.APFQueueWaitTracker.GetLatency()
	if latency != expectedLatency {
		t.Errorf("unexpected latency, got %s, expected %s", latency, expectedLatency)
	}
}
