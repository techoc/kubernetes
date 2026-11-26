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

package instrumentation

import (
	"context"
	"encoding/json"
	"time"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/instrumentation/common"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
)

const (
	eventRetryPeriod  = 1 * time.Second
	eventRetryTimeout = 1 * time.Minute
)

var _ = common.SIGDescribe("Events", func() {
	f := framework.NewDefaultFramework("events")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	/*
		Release: v1.25
		Testname: Event, manage lifecycle of an Event
		Description: 尝试创建 event，必须成功。
		尝试用 label selector 列出所有命名空间，必须
		成功，且必须找到至少一个列表项。给 event 打补丁换成新 message，
		检查时必须看到更新后的 message。用一系列新事件更新 event，检查必须确认
		这次更新。删除 event 后，列出所有事件时不得再出现。


	*/
	framework.ConformanceIt("should manage the lifecycle of an event", func(ctx context.Context) {
		// 根据 2022 年 7 月 14 日 SIG-Arch 会议，本 e2e 测试现取代
		// 已删除的 e2e 测试 "Event resource lifecycle"。

		eventTestName := "event-test"

		ginkgo.By("creating a test event")
		// 在测试命名空间中创建一个测试 event
		_, err := f.ClientSet.CoreV1().Events(f.Namespace.Name).Create(ctx, &v1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Name: eventTestName,
				Labels: map[string]string{
					"testevent-constant": "true",
				},
			},
			Message: "This is a test event",
			Reason:  "Test",
			Type:    "Normal",
			Count:   1,
			InvolvedObject: v1.ObjectReference{
				Namespace: f.Namespace.Name,
			},
		}, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create test event")

		ginkgo.By("listing all events in all namespaces")
		// 获取所有命名空间中的 Event 列表，确保端点覆盖
		eventsList, err := f.ClientSet.CoreV1().Events("").List(ctx, metav1.ListOptions{
			LabelSelector: "testevent-constant=true",
		})
		framework.ExpectNoError(err, "failed list all events")

		foundCreatedEvent := false
		var eventCreatedName string
		for _, val := range eventsList.Items {
			if val.ObjectMeta.Name == eventTestName && val.ObjectMeta.Namespace == f.Namespace.Name {
				foundCreatedEvent = true
				eventCreatedName = val.ObjectMeta.Name
				break
			}
		}
		if !foundCreatedEvent {
			framework.Failf("unable to find test event %s in namespace %s, full list of events is %+v", eventTestName, f.Namespace.Name, eventsList.Items)
		}

		ginkgo.By("patching the test event")
		// 给 event 的 message 打补丁
		eventPatchMessage := "This is a test event - patched"
		eventPatch, err := json.Marshal(map[string]interface{}{
			"message": eventPatchMessage,
		})
		framework.ExpectNoError(err, "failed to marshal the patch JSON payload")

		_, err = f.ClientSet.CoreV1().Events(f.Namespace.Name).Patch(ctx, eventTestName, types.StrategicMergePatchType, []byte(eventPatch), metav1.PatchOptions{})
		framework.ExpectNoError(err, "failed to patch the test event")

		ginkgo.By("fetching the test event")
		// 按名称获取 event
		event, err := f.ClientSet.CoreV1().Events(f.Namespace.Name).Get(ctx, eventCreatedName, metav1.GetOptions{})
		framework.ExpectNoError(err, "failed to fetch the test event")
		gomega.Expect(event.Message).To(gomega.Equal(eventPatchMessage), "test event message does not match patch message")

		ginkgo.By("updating the test event")

		testEvent, err := f.ClientSet.CoreV1().Events(f.Namespace.Name).Get(ctx, event.Name, metav1.GetOptions{})
		framework.ExpectNoError(err, "failed to get test event")

		testEvent.Series = &v1.EventSeries{
			Count:            100,
			LastObservedTime: metav1.MicroTime{Time: time.Unix(1505828956, 0)},
		}

		// 清掉由 control-plane 设置的 ResourceVersion 和 ManagedFields
		testEvent.ObjectMeta.ResourceVersion = ""
		testEvent.ObjectMeta.ManagedFields = nil

		_, err = f.ClientSet.CoreV1().Events(f.Namespace.Name).Update(ctx, testEvent, metav1.UpdateOptions{})
		framework.ExpectNoError(err, "failed to update the test event")

		ginkgo.By("getting the test event")
		event, err = f.ClientSet.CoreV1().Events(f.Namespace.Name).Get(ctx, testEvent.Name, metav1.GetOptions{})
		framework.ExpectNoError(err, "failed to get test event")
		// 清掉由 control-plane 设置的 ResourceVersion 和 ManagedFields
		event.ObjectMeta.ResourceVersion = ""
		event.ObjectMeta.ManagedFields = nil
		if !apiequality.Semantic.DeepEqual(testEvent, event) {
			framework.Failf("test event wasn't properly updated: %v", cmp.Diff(testEvent, event))
		}

		ginkgo.By("deleting the test event")
		// 删除原始 event
		err = f.ClientSet.CoreV1().Events(f.Namespace.Name).Delete(ctx, eventCreatedName, metav1.DeleteOptions{})
		framework.ExpectNoError(err, "failed to delete the test event")

		ginkgo.By("listing all events in all namespaces")
		// 获取各命名空间中的 Event 列表
		eventsList, err = f.ClientSet.CoreV1().Events("").List(ctx, metav1.ListOptions{
			LabelSelector: "testevent-constant=true",
		})
		framework.ExpectNoError(err, "fail to list all events")
		foundCreatedEvent = false
		for _, val := range eventsList.Items {
			if val.ObjectMeta.Name == eventTestName && val.ObjectMeta.Namespace == f.Namespace.Name {
				foundCreatedEvent = true
				break
			}
		}
		if foundCreatedEvent {
			framework.Failf("Should not have found test event %s in namespace %s, full list of events %+v", eventTestName, f.Namespace.Name, eventsList.Items)
		}
	})

	/*
	   Release: v1.20
	   Testname: Event, delete a collection
	   Description: 创建一组带 label selector 的 event，列出时必须能找到。
	   删除这组 event 后，按 label selector 列出时不得再出现。
	*/
	framework.ConformanceIt("should delete a collection of events", func(ctx context.Context) {
		eventTestNames := []string{"test-event-1", "test-event-2", "test-event-3"}

		ginkgo.By("Create set of events")
		// 在测试命名空间中创建一个测试 event
		for _, eventTestName := range eventTestNames {
			eventMessage := "This is " + eventTestName
			_, err := f.ClientSet.CoreV1().Events(f.Namespace.Name).Create(ctx, &v1.Event{

				ObjectMeta: metav1.ObjectMeta{
					Name:   eventTestName,
					Labels: map[string]string{"testevent-set": "true"},
				},
				Message: eventMessage,
				Reason:  "Test",
				Type:    "Normal",
				Count:   1,
				InvolvedObject: v1.ObjectReference{
					Namespace: f.Namespace.Name,
				},
			}, metav1.CreateOptions{})
			framework.ExpectNoError(err, "failed to create event")
			framework.Logf("created %v", eventTestName)
		}

		ginkgo.By("get a list of Events with a label in the current namespace")
		// 获取 event 列表
		eventList, err := f.ClientSet.CoreV1().Events(f.Namespace.Name).List(ctx, metav1.ListOptions{
			LabelSelector: "testevent-set=true",
		})
		framework.ExpectNoError(err, "failed to get a list of events")

		gomega.Expect(eventList.Items).To(gomega.HaveLen(len(eventTestNames)), "looking for expected number of pod templates events")

		ginkgo.By("delete collection of events")
		// 删除 collection

		framework.Logf("requesting DeleteCollection of events")
		err = f.ClientSet.CoreV1().Events(f.Namespace.Name).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
			LabelSelector: "testevent-set=true"})
		framework.ExpectNoError(err, "failed to delete the test event")

		ginkgo.By("check that the list of events matches the requested quantity")

		err = wait.PollUntilContextTimeout(ctx, eventRetryPeriod, eventRetryTimeout, true, checkEventListQuantity(f, "testevent-set=true", 0))
		framework.ExpectNoError(err, "failed to count required events")
	})

})

func checkEventListQuantity(f *framework.Framework, label string, quantity int) func(ctx context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		var err error

		framework.Logf("requesting list of events to confirm quantity")

		eventList, err := f.ClientSet.CoreV1().Events(f.Namespace.Name).List(ctx, metav1.ListOptions{
			LabelSelector: label})

		if err != nil {
			return false, err
		}

		if len(eventList.Items) != quantity {
			return false, err
		}
		return true, nil
	}
}
