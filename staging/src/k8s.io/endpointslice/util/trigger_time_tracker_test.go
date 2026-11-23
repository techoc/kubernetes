/*
Copyright 2019 The Kubernetes Authors.

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

package util

import (
	"runtime"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

var (
	// 测试用的时间基准点（t0~t5 每次递增 1 秒）与固定的命名空间/Service 名称。
	// t0 为 2019-01-01 00:00:00 UTC，后续时间依次加 1 秒；
	// ttNamespace 与 ttServiceName 为测试用例共用的 Service 标识。
	t0 = time.Date(2019, 01, 01, 0, 0, 0, 0, time.UTC)
	t1 = t0.Add(time.Second)
	t2 = t1.Add(time.Second)
	t3 = t2.Add(time.Second)
	t4 = t3.Add(time.Second)
	t5 = t4.Add(time.Second)

	ttNamespace   = "ttNamespace1"
	ttServiceName = "my-service"
)

// TestNewServiceNoPods 验证新 Service 在无 Pod 时首次同步返回其创建时间。
func TestNewServiceNoPods(t *testing.T) {
	tester := newTester(t)

	service := createService(ttNamespace, ttServiceName, t2)
	// 创建时间为 t2，期望返回 t2。
	tester.whenComputeEndpointLastChangeTriggerTime(ttNamespace, service).expect(t2)
}

// TestNewServiceExistingPods 验证已有 Pod 的新 Service 仍返回 Service 创建时间，
// 而不是 Pod 的就绪时间。
func TestNewServiceExistingPods(t *testing.T) {
	tester := newTester(t)

	service := createService(ttNamespace, ttServiceName, t3)
	// Service 创建于 t3，Pod 就绪时间分布在 t0/t1/t5。
	pod1 := createPod(ttNamespace, "pod1", t0)
	pod2 := createPod(ttNamespace, "pod2", t1)
	pod3 := createPod(ttNamespace, "pod3", t5)
	tester.whenComputeEndpointLastChangeTriggerTime(ttNamespace, service, pod1, pod2, pod3).
		// Pods were created before service, but trigger time is the time when service was created.
		// 首次同步始终以 Service 创建时间为准。
		expect(t3)
}

// TestPodsAdded 验证已有 Service 首次加入 Pod 时，
// 返回最早变化的 Pod 触发时间。
func TestPodsAdded(t *testing.T) {
	tester := newTester(t)

	service := createService(ttNamespace, ttServiceName, t0)
	// Service 创建于 t0，首次无 Pod 同步返回 t0。
	tester.whenComputeEndpointLastChangeTriggerTime(ttNamespace, service).expect(t0)

	pod1 := createPod(ttNamespace, "pod1", t2)
	// 新增两个 Pod，就绪时间分别为 t2、t1，期望返回 t1。
	pod2 := createPod(ttNamespace, "pod2", t1)
	tester.whenComputeEndpointLastChangeTriggerTime(ttNamespace, service, pod1, pod2).expect(t1)
}

// TestPodsUpdated 验证部分 Pod 触发时间更新后返回变化中的最小值，
// 未变化的 Pod 不参与计算。
func TestPodsUpdated(t *testing.T) {
	tester := newTester(t)

	service := createService(ttNamespace, ttServiceName, t0)
	// 初始三个 Pod 就绪于 t1/t2/t3，首次同步返回 Service 创建时间 t0。
	pod1 := createPod(ttNamespace, "pod1", t1)
	pod2 := createPod(ttNamespace, "pod2", t2)
	pod3 := createPod(ttNamespace, "pod3", t3)
	tester.whenComputeEndpointLastChangeTriggerTime(ttNamespace, service, pod1, pod2, pod3).expect(t0)

	pod1 = createPod(ttNamespace, "pod1", t5)
	// pod1、pod2 更新到 t5、t4，pod3 不变，期望返回 t4。
	pod2 = createPod(ttNamespace, "pod2", t4)
	// pod3 doesn't change.
	tester.whenComputeEndpointLastChangeTriggerTime(ttNamespace, service, pod1, pod2, pod3).expect(t4)
}

// TestPodsUpdatedNoOp 验证没有任何变化时返回零值时间（不输出注解）。
func TestPodsUpdatedNoOp(t *testing.T) {
	tester := newTester(t)

	service := createService(ttNamespace, ttServiceName, t0)
	// 首次同步记录三个 Pod。
	pod1 := createPod(ttNamespace, "pod1", t1)
	pod2 := createPod(ttNamespace, "pod2", t2)
	pod3 := createPod(ttNamespace, "pod3", t3)
	tester.whenComputeEndpointLastChangeTriggerTime(ttNamespace, service, pod1, pod2, pod3).expect(t0)

	// Nothing has changed.
	// 再次以完全相同的对象同步，期望零值。
	tester.whenComputeEndpointLastChangeTriggerTime(ttNamespace, service, pod1, pod2, pod3).expectNil()
}

// TestPodDeletedThenAdded 验证 Pod 被移除后重新加入（带新触发时间）时，
// 能正确返回新的触发时间。
func TestPodDeletedThenAdded(t *testing.T) {
	tester := newTester(t)

	service := createService(ttNamespace, ttServiceName, t0)
	// 首次两个 Pod 同步返回 t0。
	pod1 := createPod(ttNamespace, "pod1", t1)
	pod2 := createPod(ttNamespace, "pod2", t2)
	tester.whenComputeEndpointLastChangeTriggerTime(ttNamespace, service, pod1, pod2).expect(t0)

	tester.whenComputeEndpointLastChangeTriggerTime(ttNamespace, service, pod1).expectNil()
	// 移除 pod2 后无变化，返回零值。

	pod2 = createPod(ttNamespace, "pod2", t4)
	// pod2 以 t4 重新加入，期望返回 t4。
	tester.whenComputeEndpointLastChangeTriggerTime(ttNamespace, service, pod1, pod2).expect(t4)
}

// TestServiceDeletedThenAdded 验证删除 Service 状态后重新创建 Service 会按新 Service 处理。
func TestServiceDeletedThenAdded(t *testing.T) {
	tester := newTester(t)

	service := createService(ttNamespace, ttServiceName, t0)
	// 首次同步返回 t0。
	pod1 := createPod(ttNamespace, "pod1", t1)
	pod2 := createPod(ttNamespace, "pod2", t2)
	tester.whenComputeEndpointLastChangeTriggerTime(ttNamespace, service, pod1, pod2).expect(t0)

	tester.DeleteService(ttNamespace, ttServiceName)
	// 清除 tracker 中该 Service 的状态。

	service = createService(ttNamespace, ttServiceName, t3)
	// 以 t3 重建 Service，期望返回 t3。
	tester.whenComputeEndpointLastChangeTriggerTime(ttNamespace, service, pod1, pod2).expect(t3)
}

// TestServiceUpdatedNoPodChange 验证仅修改 Service 端口（触发时间未变）时返回零值，
// 因为当前实现无法计算 Service 更新的触发时间。
func TestServiceUpdatedNoPodChange(t *testing.T) {
	tester := newTester(t)

	service := createService(ttNamespace, ttServiceName, t0)
	// 首次同步返回 t0。
	pod1 := createPod(ttNamespace, "pod1", t1)
	pod2 := createPod(ttNamespace, "pod2", t2)
	tester.whenComputeEndpointLastChangeTriggerTime(ttNamespace, service, pod1, pod2).expect(t0)

	// service's ports have changed.
	// 仅修改 Service 的端口配置。
	service.Spec = v1.ServiceSpec{
		Selector: map[string]string{},
		Ports:    []v1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(8080), Protocol: "TCP"}},
	}

	// Currently we're not able to calculate trigger time for service updates, hence the returned
	// value is a nil time.
	// 当前实现不计算 Service 更新的触发时间，期望零值。
	tester.whenComputeEndpointLastChangeTriggerTime(ttNamespace, service, pod1, pod2).expectNil()
}

// ------- Test Utils -------
// ------- 测试辅助工具 -------

// tester 封装 TriggerTimeTracker 与 testing.T，提供链式断言能力。
type tester struct {
	*TriggerTimeTracker
	t *testing.T
}

// newTester 为给定测试创建一个新的 tester 实例。
func newTester(t *testing.T) *tester {
	return &tester{NewTriggerTimeTracker(), t}
}

// whenComputeEndpointLastChangeTriggerTime 调用被测方法并返回可链式断言的 subject。
func (t *tester) whenComputeEndpointLastChangeTriggerTime(
	namespace string, service *v1.Service, pods ...*v1.Pod) subject {
	return subject{t.ComputeEndpointLastChangeTriggerTime(namespace, service, pods), t.t}
}

// subject 保存一次计算的结果与测试上下文。
type subject struct {
	got time.Time
	t   *testing.T
}

// expect 断言结果与期望时间一致。
func (s subject) expect(expected time.Time) {
	s.doExpect(expected)
}

// expectNil 断言结果为零值时间（不输出注解）。
func (s subject) expectNil() {
	s.doExpect(time.Time{})
}

// doExpect 执行实际断言，并在失败时报告调用处的文件与行号。
func (s subject) doExpect(expected time.Time) {
	if s.got != expected {
		// 不一致时定位到断言调用处并报错。
		_, fn, line, _ := runtime.Caller(2)
		s.t.Errorf("Wrong trigger time in %s:%d expected %s, got %s", fn, line, expected, s.got)
	}
}

// createPod 创建一个带 Ready=True 条件且转换时间为 readyTime 的 Pod。
func createPod(namespace, ttServiceName string, readyTime time.Time) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: ttServiceName},
		Status: v1.PodStatus{Conditions: []v1.PodCondition{
			{
				Type:               v1.PodReady,
				Status:             v1.ConditionTrue,
				LastTransitionTime: metav1.NewTime(readyTime),
			},
		},
		},
	}
}

// createService 创建一个创建时间为 creationTime 的 Service。
func createService(namespace, ttServiceName string, creationTime time.Time) *v1.Service {
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              ttServiceName,
			CreationTimestamp: metav1.NewTime(creationTime),
		},
	}
}
