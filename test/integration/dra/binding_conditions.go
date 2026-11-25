/*
Copyright The Kubernetes Authors.

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

package dra

import (
	"fmt"
	"strings"
	"time"

	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gstruct"
	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
	"k8s.io/kubernetes/test/utils/ktesting/format"
)

var (
	nodeName           = "worker-0"
	poolWithBinding    = nodeName + "-with-binding"
	poolWithoutBinding = nodeName + "-without-binding"
	bindingCondition   = "attached"
	failureCondition   = "failed"
)

// testDeviceBindingConditions 是各 DeviceBindingConditions 集成测试的入口。
// 其中部分测试使用 device taint，假设 DRADeviceTaints 已启用。
//
// 此外，部分测试使用自定义调度器配置，因此不能并行运行。
// 使用默认配置的测试之间可以并行，但不能与非 DeviceBindingConditions 测试并行。
func testDeviceBindingConditions(tCtx ktesting.TContext, enabled bool) {
	tCtx.Run("BasicFlow", func(tCtx ktesting.TContext) { testDeviceBindingConditionsBasicFlow(tCtx, enabled) })
	if enabled {
		tCtx.Run("FailureTaints", func(tCtx ktesting.TContext) { testDeviceBindingFailureConditionsReschedule(tCtx, true) })
		tCtx.Run("FailureRemove", func(tCtx ktesting.TContext) { testDeviceBindingFailureConditionsReschedule(tCtx, false) })
		tCtx.Run("TimeoutReached", func(tCtx ktesting.TContext) { testDeviceBindingConditionsTimeoutReached(tCtx) })
		tCtx.Run("TimeoutRecover", func(tCtx ktesting.TContext) { testDeviceBindingConditionsTimeoutRecovery(tCtx) })
	}
}

// testDeviceBindingConditionsBasicFlow 验证混合设备场景：一个设备有 BindingConditions，另一个没有。
// 调度器应优先为第一个 Pod 选择无 BindingConditions 的设备，第二个 Pod 则使用带条件的设备。
// 测试还验证第二个 Pod 初次绑定失败后调度器会重试，重新调度后最终成功。
func testDeviceBindingConditionsBasicFlow(tCtx ktesting.TContext, enabled bool) {
	tCtx.Parallel()

	namespace := createTestNamespace(tCtx, nil)
	class, driverName := createTestClass(tCtx, namespace)

	slice := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: namespace + "-",
		},
		Spec: resourceapi.ResourceSliceSpec{
			NodeName: &nodeName,
			Pool: resourceapi.ResourcePool{
				Name:               poolWithBinding,
				ResourceSliceCount: 1,
			},
			Driver: driverName,
			Devices: []resourceapi.Device{
				{
					Name:                     "with-binding",
					BindingConditions:        []string{bindingCondition},
					BindingFailureConditions: []string{failureCondition},
				},
			},
		},
	}
	slice, err := tCtx.Client().ResourceV1().ResourceSlices().Create(tCtx, slice, metav1.CreateOptions{FieldValidation: "Strict"})
	tCtx.ExpectNoError(err, "create slice")

	haveBindingConditionFields := len(slice.Spec.Devices[0].BindingConditions) > 0 || len(slice.Spec.Devices[0].BindingFailureConditions) > 0
	if !enabled {
		if haveBindingConditionFields {
			tCtx.Fatalf("Expected device binding condition fields to get dropped, got instead:\n%s", format.Object(slice, 1))
		}
		return
	}
	if !haveBindingConditionFields {
		tCtx.Fatalf("Expected device binding condition fields to be stored, got instead:\n%s", format.Object(slice, 1))
	}

	sliceWithoutBinding := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: namespace + "-without-binding-",
		},
		Spec: resourceapi.ResourceSliceSpec{
			NodeName: &nodeName,
			Pool: resourceapi.ResourcePool{
				Name:               poolWithoutBinding,
				ResourceSliceCount: 1,
			},
			Driver: driverName,
			Devices: []resourceapi.Device{
				{
					Name: "without-binding",
				},
			},
		},
	}
	_, err = tCtx.Client().ResourceV1().ResourceSlices().Create(tCtx, sliceWithoutBinding, metav1.CreateOptions{FieldValidation: "Strict"})
	tCtx.ExpectNoError(err, "create slice without binding conditions")

	startScheduler(tCtx)
	// 调度第一个 Pod，等待调度器进入 binding 阶段（此时 claim 被标记为已分配）。
	start := time.Now()
	claim1 := createClaim(tCtx, namespace, "-a", class, claim)
	pod := createPod(tCtx, namespace, "-a", podWithClaimName, claim1)
	claim1 = waitForClaimAllocatedToDevice(tCtx, namespace, claim1.Name, schedulingTimeout)
	end := time.Now()
	gomega.NewWithT(tCtx).Expect(claim1).To(gomega.HaveField("Status.Allocation", gstruct.PointTo(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
		"Devices": gomega.Equal(resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{
				Request: claim1.Spec.Devices.Requests[0].Name,
				Driver:  driverName,
				Pool:    poolWithoutBinding,
				Device:  "without-binding",
			}}}),
		// 有意不检查 NodeSelector，这部分由其他测试覆盖。
		"AllocationTimestamp": gomega.HaveField("Time", gomega.And(
			gomega.BeTemporally(">=", start.Truncate(time.Second) /* 序列化往返时可能被向下取整 */),
			gomega.BeTemporally("<=", end),
		)),
	}))), "first allocated claim")

	waitForPodScheduled(tCtx, namespace, pod.Name)

	// 第二个 Pod 应使用带 BindingConditions 的设备。
	claim2 := createClaim(tCtx, namespace, "-b", class, claim)
	pod = createPod(tCtx, namespace, "-b", podWithClaimName, claim2)
	claim2 = waitForClaimAllocatedToDevice(tCtx, namespace, claim2.Name, schedulingTimeout)
	end = time.Now()
	gomega.NewWithT(tCtx).Expect(claim2).To(gomega.HaveField("Status.Allocation", gstruct.PointTo(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
		"Devices": gomega.Equal(resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{
				Request:                  claim2.Spec.Devices.Requests[0].Name,
				Driver:                   driverName,
				Pool:                     poolWithBinding,
				Device:                   "with-binding",
				BindingConditions:        []string{bindingCondition},
				BindingFailureConditions: []string{failureCondition},
			}}}),
		// 有意不检查 NodeSelector，这部分由其他测试覆盖。
		"AllocationTimestamp": gomega.HaveField("Time", gomega.And(
			gomega.BeTemporally(">=", start.Truncate(time.Second) /* 序列化往返时可能被向下取整 */),
			gomega.BeTemporally("<=", end),
		)),
	}))), "second allocated claim")

	// 让第二个 claim 的绑定条件失败，从而触发后续重新调度。
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest, err := tCtx.Client().ResourceV1().ResourceClaims(namespace).Get(tCtx, claim2.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		latest.Status.Devices = []resourceapi.AllocatedDeviceStatus{{
			Driver: driverName,
			Pool:   poolWithBinding,
			Device: "with-binding",
			Conditions: []metav1.Condition{{
				Type:               failureCondition,
				Status:             metav1.ConditionTrue,
				ObservedGeneration: latest.Generation,
				LastTransitionTime: metav1.Now(),
				Reason:             "Testing",
				Message:            "The test has seen the allocation and is failing the binding.",
			}},
		}}
		_, err = tCtx.Client().ResourceV1().ResourceClaims(namespace).UpdateStatus(tCtx, latest, metav1.UpdateOptions{})
		return err
	})
	tCtx.ExpectNoError(err, "add binding failure condition to second claim")

	// 等待调度器再次清除设备状态。
	waitForClaim(tCtx, namespace, claim2.Name, schedulingTimeout,
		gomega.HaveField("Status.Devices", gomega.HaveLen(0)),
		"claim should have cleared device conditions after rescheduling",
	)

	// 分配是否恢复？
	claim2 = waitForClaimAllocatedToDevice(tCtx, namespace, claim2.Name, schedulingTimeout)

	// 此时设置最终的绑定条件是安全的，可以放行调度器继续。
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest, err := tCtx.Client().ResourceV1().ResourceClaims(namespace).Get(tCtx, claim2.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		// 为当前分配的设备写入 status.devices。
		latest.Status.Devices = []resourceapi.AllocatedDeviceStatus{{
			Driver: driverName,
			Pool:   poolWithBinding,
			Device: "with-binding",
			Conditions: []metav1.Condition{{
				Type:               bindingCondition,
				Status:             metav1.ConditionTrue,
				ObservedGeneration: latest.Generation,
				LastTransitionTime: metav1.Now(),
				Reason:             "Testing",
				Message:            "The test has seen the allocation.",
			}},
		}}
		_, err = tCtx.Client().ResourceV1().ResourceClaims(namespace).UpdateStatus(tCtx, latest, metav1.UpdateOptions{})
		return err
	})
	tCtx.ExpectNoError(err, "add binding condition to second claim")
	waitForPodScheduled(tCtx, namespace, pod.Name)
}

// testDeviceBindingFailureConditionsReschedule 验证节点上设备准备失败时的调度行为：
// 写入 BindingFailure 后，调度器应能将 Pod 重新调度到绑定成功的其他节点。
// 失败通过 DeviceTaint 或从 ResourceSlice 移除设备两种方式模拟，由 useTaints 参数控制。
func testDeviceBindingFailureConditionsReschedule(tCtx ktesting.TContext, useTaints bool) {
	tCtx.Parallel()

	namespace := createTestNamespace(tCtx, nil)
	class, driverName := createTestClass(tCtx, namespace)
	startScheduler(tCtx)

	anotherNodeName := "worker-1"
	anotherPoolWithoutBinding := anotherNodeName + "-without-binding"

	slice := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: namespace + "-",
		},
		Spec: resourceapi.ResourceSliceSpec{
			NodeName: &nodeName,
			Pool: resourceapi.ResourcePool{
				Name:               poolWithBinding,
				ResourceSliceCount: 1,
			},
			Driver: driverName,
			Devices: []resourceapi.Device{
				{
					Name:                     "with-binding",
					BindingConditions:        []string{bindingCondition},
					BindingFailureConditions: []string{failureCondition},
				},
			},
		},
	}
	slice = createSlice(tCtx, slice)

	// 将第一个 Pod 调度到带 BindingConditions 的设备，
	// 确保初始分配落在预期的节点上。
	claim1 := createClaim(tCtx, namespace, "-a", class, claim)
	pod := createPod(tCtx, namespace, "-a", podWithClaimName, claim1)
	claim1 = waitForClaimAllocatedToDevice(tCtx, namespace, claim1.Name, schedulingTimeout)
	gomega.NewWithT(tCtx).Expect(claim1).To(gomega.HaveField("Status.Allocation", gstruct.PointTo(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
		"Devices": gomega.Equal(resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{
				Request:                  claim1.Spec.Devices.Requests[0].Name,
				Driver:                   driverName,
				Pool:                     poolWithBinding,
				Device:                   "with-binding",
				BindingConditions:        []string{bindingCondition},
				BindingFailureConditions: []string{failureCondition},
			}}}),
	}))), "third allocated claim to the device with binding conditions")

	if useTaints {
		// 给带 BindingConditions 的设备打 taint，阻止后续再调度到该设备。
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest, err := tCtx.Client().ResourceV1().ResourceSlices().Get(tCtx, slice.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			slice = latest.DeepCopy()
			slice.Spec.Devices[0].Taints = []resourceapi.DeviceTaint{
				{
					Key:    "dra-test.k8s.io/preparation-failed",
					Value:  "true",
					Effect: resourceapi.DeviceTaintEffectNoSchedule,
				},
			}
			_, err = tCtx.Client().ResourceV1().ResourceSlices().Update(tCtx, slice, metav1.UpdateOptions{})
			return err
		})
		tCtx.ExpectNoError(err, "add taint to second slice")
	} else {
		// 从 slice 中移除设备，模拟准备失败导致设备不可用。
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest, err := tCtx.Client().ResourceV1().ResourceSlices().Get(tCtx, slice.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			slice = latest.DeepCopy()
			slice.Spec.Devices = nil
			_, err = tCtx.Client().ResourceV1().ResourceSlices().Update(tCtx, slice, metav1.UpdateOptions{})
			return err
		})
		tCtx.ExpectNoError(err, "remove devices in slice")
	}

	// 在其他节点上创建一个含无 BindingConditions 设备的新 slice，
	// 使调度器重试后能成功分配 claim。
	sliceWithoutBinding := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: namespace + "-without-binding-",
		},
		Spec: resourceapi.ResourceSliceSpec{
			NodeName: &anotherNodeName,
			Pool: resourceapi.ResourcePool{
				Name:               anotherPoolWithoutBinding,
				ResourceSliceCount: 1,
			},
			Driver: driverName,
			Devices: []resourceapi.Device{
				{
					Name: "without-binding",
				},
			},
		},
	}
	createSlice(tCtx, sliceWithoutBinding)

	// 显式让第三个 claim 的绑定条件失败，以触发重新调度逻辑。
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest, err := tCtx.Client().ResourceV1().ResourceClaims(namespace).Get(tCtx, claim1.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		claim1 = latest.DeepCopy()
		claim1.Status.Devices = []resourceapi.AllocatedDeviceStatus{{
			Driver: driverName,
			Pool:   poolWithBinding,
			Device: "with-binding",
			Conditions: []metav1.Condition{{
				Type:               failureCondition,
				Status:             metav1.ConditionTrue,
				ObservedGeneration: claim1.Generation,
				LastTransitionTime: metav1.Now(),
				Reason:             "Testing",
				Message:            "The test has seen the allocation and is failing the binding.",
			}},
		}}
		_, err = tCtx.Client().ResourceV1().ResourceClaims(namespace).UpdateStatus(tCtx, claim1, metav1.UpdateOptions{})
		return err
	})
	tCtx.ExpectNoError(err, "add binding failure condition to claim")

	// 等待调度器再次清除设备状态。
	waitForClaim(tCtx, namespace, claim1.Name, schedulingTimeout,
		gomega.HaveField("Status.Devices", gomega.HaveLen(0)),
		"claim should have cleared device conditions after rescheduling",
	)

	// 分配是否恢复？
	claim1 = waitForClaimAllocatedToDevice(tCtx, namespace, claim1.Name, schedulingTimeout)

	gomega.NewWithT(tCtx).Expect(claim1).To(gomega.HaveField("Status.Allocation", gstruct.PointTo(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
		"Devices": gomega.Equal(resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{
				Request: claim1.Spec.Devices.Requests[0].Name,
				Driver:  driverName,
				Pool:    anotherPoolWithoutBinding,
				Device:  "without-binding",
			}}}),
	}))), "third allocated claim to the device without binding conditions")

	waitForPodScheduled(tCtx, namespace, pod.Name)
}

// testDeviceBindingConditionsTimeoutReached 验证当所需 BindingConditions 一直未满足时，
// 较短的 bindingTimeout 会触发 PreBind 超时。
//
// 该测试使用非标准调度器配置，因此不能并行运行。
func testDeviceBindingConditionsTimeoutReached(tCtx ktesting.TContext) {
	namespace := createTestNamespace(tCtx, nil)
	class, driver := createTestClass(tCtx, namespace)

	// 一个必须满足绑定条件的设备。
	slice := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{GenerateName: namespace + "-timeout-"},
		Spec: resourceapi.ResourceSliceSpec{
			NodeName: &nodeName,
			Pool: resourceapi.ResourcePool{
				Name:               poolWithBinding,
				ResourceSliceCount: 1,
			},
			Driver: driver,
			Devices: []resourceapi.Device{{
				Name:                     "with-binding",
				BindingConditions:        []string{bindingCondition},
				BindingFailureConditions: []string{failureCondition},
			}},
		},
	}
	createSlice(tCtx, slice)

	wantTO := 6 * time.Second // bindingTimeout
	maxTO := wantTO * 19 / 10 // 1.9 * wantTO

	// 以较短的绑定超时启动调度器。
	cfg := fmt.Sprintf(`
profiles:
- schedulerName: default-scheduler
  pluginConfig:
  - name: DynamicResources
    args:
      bindingTimeout: %s
`, wantTO)

	startSchedulerWithConfig(tCtx, cfg)

	// 创建 claim 和 Pod：先完成分配，然后调度器在 PreBind 阶段等待。
	claim1 := createClaim(tCtx, namespace, "-timeout-enforced", class, claim)
	pod := createPod(tCtx, namespace, "-timeout-enforced", podWithClaimName, claim1)

	// 等待 claim 分配完成。
	allocatedClaim := waitForClaimAllocatedToDevice(tCtx, namespace, claim1.Name, schedulingTimeout)

	gomega.NewWithT(tCtx).Expect(allocatedClaim).To(gomega.HaveField(
		"Status.Allocation",
		gstruct.PointTo(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
			"Devices": gomega.Equal(resourceapi.DeviceAllocationResult{
				Results: []resourceapi.DeviceRequestAllocationResult{{
					Request:                  allocatedClaim.Spec.Devices.Requests[0].Name,
					Driver:                   driver,
					Pool:                     poolWithBinding,
					Device:                   "with-binding",
					BindingConditions:        []string{bindingCondition},
					BindingFailureConditions: []string{failureCondition},
				}},
			}),
		}),
		)), "Claim must be allocated to the condition-gated device")

	tStart := time.Now()

	// 调度器应触发绑定超时，并在 Pod 上体现。
	// 轮询 Pod 的 conditions 直到出现包含 "binding timeout" 的消息。
	tCtx.Eventually(func(tCtx ktesting.TContext) (*v1.Pod, error) {
		return tCtx.Client().CoreV1().Pods(namespace).Get(tCtx, pod.Name, metav1.GetOptions{})
	}).WithTimeout(maxTO).WithPolling(300*time.Millisecond).Should(
		gomega.HaveField("Status.Conditions",
			gomega.ContainElement(
				gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
					"Message": gomega.ContainSubstring("binding timeout"),
				}),
			),
		),
		"pod should report binding timeout in a condition message",
	)

	elapsed := time.Since(tStart)
	gomega.NewWithT(tCtx).Expect(elapsed).To(
		gomega.BeNumerically("<=", maxTO),
		"bindingTimeout should trigger roughly near %s (observed %v)", wantTO, elapsed,
	)
	// 验证绑定超时后 Pod 仍处于未调度状态。
	tCtx.Eventually(func(tCtx ktesting.TContext) (*v1.Pod, error) {
		return tCtx.Client().CoreV1().Pods(namespace).Get(tCtx, pod.Name, metav1.GetOptions{})
	}).WithTimeout(wantTO).WithPolling(200 * time.Millisecond).Should(gomega.SatisfyAll(
		gomega.HaveField("Spec.NodeName", gomega.BeEmpty()),

		gomega.HaveField("Status.Conditions",
			gomega.Not(gomega.ContainElement(v1.PodCondition{
				Type:   v1.PodScheduled,
				Status: v1.ConditionTrue,
			}))),
	))
}

// testDeviceBindingConditionsTimeoutRecovery 验证：带 BindingConditions 的设备在超时内未就绪时，
// 若加入一个无绑定条件的新设备，调度器应将 claim 重新调度到该可用设备。
//
// 该测试使用非标准调度器配置，因此不能并行运行。
func testDeviceBindingConditionsTimeoutRecovery(tCtx ktesting.TContext) {
	namespace := createTestNamespace(tCtx, nil)
	class, driverName := createTestClass(tCtx, namespace)

	// 以较短的绑定超时启动调度器。
	const cfg = `
profiles:
- schedulerName: default-scheduler
  pluginConfig:
  - name: DynamicResources
    args:
      bindingTimeout: 5s
`
	startSchedulerWithConfig(tCtx, cfg)

	// 初始 slice 中只有一个设备，且其必需的绑定条件永远不会变为 true。
	slice := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: namespace + "-",
		},
		Spec: resourceapi.ResourceSliceSpec{
			NodeName: &nodeName,
			Pool: resourceapi.ResourcePool{
				Name:               poolWithBinding,
				ResourceSliceCount: 1,
			},
			Driver: driverName,
			Devices: []resourceapi.Device{{
				Name:                     "with-binding",
				BindingConditions:        []string{bindingCondition},
				BindingFailureConditions: []string{failureCondition},
			}},
		},
	}
	createSlice(tCtx, slice)
	claim1 := createClaim(tCtx, namespace, "-timeout", class, claim)
	pod := createPod(tCtx, namespace, "-timeout", podWithClaimName, claim1)

	claim1 = waitForClaimAllocatedToDevice(tCtx, namespace, claim1.Name, schedulingTimeout)
	gomega.NewWithT(tCtx).Expect(claim1).To(gomega.HaveField(
		"Status.Allocation",
		gstruct.PointTo(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
			"Devices": gomega.Equal(resourceapi.DeviceAllocationResult{
				Results: []resourceapi.DeviceRequestAllocationResult{{
					Request:                  claim.Spec.Devices.Requests[0].Name,
					Driver:                   driverName,
					Pool:                     poolWithBinding,
					Device:                   "with-binding",
					BindingConditions:        []string{bindingCondition},
					BindingFailureConditions: []string{failureCondition},
				}},
			}),
		}),
		)), "Expected allocation to the condition-gated device")

	sliceWithoutBinding := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{GenerateName: namespace + "-recovery-"},
		Spec: resourceapi.ResourceSliceSpec{
			NodeName: &nodeName,
			Pool: resourceapi.ResourcePool{
				Name:               poolWithoutBinding,
				ResourceSliceCount: 1,
			},
			Driver: driverName,
			Devices: []resourceapi.Device{{
				Name: "without-binding",
			}},
		},
	}
	sliceWithoutBinding = createSlice(tCtx, sliceWithoutBinding)

	// 确保 ResourceSlice 在绑定超时之前创建完成。
	tCtx.Eventually(func(tCtx ktesting.TContext) error {
		p, err := tCtx.Client().CoreV1().Pods(namespace).Get(tCtx, pod.Name, metav1.GetOptions{})
		if err == nil {
			for _, c := range p.Status.Conditions {
				if strings.Contains(strings.ToLower(c.Message), "binding timeout") {
					return gomega.StopTrying("binding timeout occurred before slice is created")
				}
			}
		}
		_, err = tCtx.Client().ResourceV1().ResourceSlices().Get(tCtx, sliceWithoutBinding.Name, metav1.GetOptions{})
		return err
	}).WithTimeout(schedulingTimeout).WithPolling(300*time.Millisecond).Should(
		gomega.Succeed(), "slice must be created before binding timeout")

	// 等待绑定超时发生。
	tCtx.Eventually(func(tCtx ktesting.TContext) (*v1.Pod, error) {
		return tCtx.Client().CoreV1().Pods(namespace).Get(tCtx, pod.Name, metav1.GetOptions{})
	}).WithTimeout(20*time.Second).WithPolling(300*time.Millisecond).Should(
		gomega.HaveField("Status.Conditions",
			gomega.ContainElement(
				gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
					"Message": gomega.ContainSubstring("binding timeout"),
				}),
			),
		),
		"pod should report binding timeout before reallocation",
	)

	// 验证绑定超时触发重新调度后，claim 能恢复分配到新加入的无 BindingConditions 设备。
	tCtx.Eventually(func(tCtx ktesting.TContext) (*resourceapi.ResourceClaim, error) {
		return tCtx.Client().ResourceV1().ResourceClaims(namespace).Get(tCtx, claim1.Name, metav1.GetOptions{})
	}).WithTimeout(schedulingTimeout).WithPolling(1*time.Second).Should(gomega.HaveField(
		"Status.Allocation",
		gstruct.PointTo(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
			"Devices": gomega.Equal(resourceapi.DeviceAllocationResult{
				Results: []resourceapi.DeviceRequestAllocationResult{{
					Request: claim.Spec.Devices.Requests[0].Name,
					Driver:  driverName,
					Pool:    poolWithoutBinding,
					Device:  "without-binding",
				}},
			}),
		}),
		)), "Expected allocation to the device without binding conditions")

	waitForPodScheduled(tCtx, namespace, pod.Name)
}
