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
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gstruct"
	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	resourcealpha "k8s.io/api/resource/v1alpha3"
	resourcebeta "k8s.io/api/resource/v1beta2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	resourceinformers "k8s.io/client-go/informers/resource/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/controller/devicetainteviction"
	"k8s.io/kubernetes/pkg/features"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
	"k8s.io/utils/ptr"
)

type useRuleMode int

const (
	useNoRule = useRuleMode(iota)
	useV1alpha3Rule
	useV1beta2Rule
	useV1Rule
)

// testEvictCluster 模拟一个包含大量已调度 Pod 的集群，每个 Pod 各自使用一个带一个 device 的 ResourceClaim。
// 随后通过单条 DeviceTaintRule 给所有 device 打 taint（一次性驱逐所有 Pod），
// 或逐个更新 ResourceSlice 来逐步驱逐。
func testEvictCluster(tCtx ktesting.TContext, useRule useRuleMode) {
	// 不并行执行：该测试会创建 controller。

	var wg sync.WaitGroup
	defer func() {
		tCtx.Cancel("time to shut down")
		wg.Wait()
	}()

	numPods := 1000
	devicesPerSlice := 50
	numSlices := (numPods + devicesPerSlice - 1) / devicesPerSlice
	namespace := createTestNamespace(tCtx, nil)
	nodeName := "node-" + namespace
	driverName := "driver-" + namespace
	poolName := "cluster"

	tCtx.Logf("testing with driver name %s", driverName)

	var slices []*resourceapi.ResourceSlice
	for i := range numSlices {
		slice := &resourceapi.ResourceSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("slice-%s-%d", namespace, i),
			},
			Spec: resourceapi.ResourceSliceSpec{
				Driver: driverName,
				Pool: resourceapi.ResourcePool{
					Name:               poolName,
					ResourceSliceCount: int64(numSlices),
				},
				AllNodes: ptr.To(true),
			},
		}
		for e := range devicesPerSlice {
			slice.Spec.Devices = append(slice.Spec.Devices, resourceapi.Device{
				Name: fmt.Sprintf("device-%d", i*devicesPerSlice+e),
			})
		}
		slices = append(slices, createSlice(tCtx, slice))
	}

	for i := range numPods {
		suffix := fmt.Sprintf("-%d", i)

		pod := podWithClaimName.DeepCopy()
		pod.Name += suffix
		pod.Namespace = namespace
		*pod.Spec.ResourceClaims[0].ResourceClaimName += suffix
		pod.Spec.NodeName = nodeName // Must be scheduled to be evicted.
		pod = must(tCtx, tCtx.Client().CoreV1().Pods(namespace).Create, pod, metav1.CreateOptions{})

		// 已调度的 Pod 没有 kubelet 确认不会被删除，测试环境里没有 kubelet，只能强制删除来清理。
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			err := tCtx.Client().CoreV1().Pods(namespace).Delete(tCtx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: ptr.To(int64(0))})
			if !apierrors.IsNotFound(err) {
				tCtx.ExpectNoError(err)
			}
		})

		// claim 没有 finalizer，删除 Namespace 时可以一并清理。
		claim := claim.DeepCopy()
		claim.Name += suffix
		claim.Namespace = namespace
		claim = must(tCtx, tCtx.Client().ResourceV1().ResourceClaims(namespace).Create, claim, metav1.CreateOptions{})
		claim.Status.Allocation = &resourceapi.AllocationResult{
			Devices: resourceapi.DeviceAllocationResult{
				Results: []resourceapi.DeviceRequestAllocationResult{{
					Request: claim.Spec.Devices.Requests[0].Name,
					Driver:  driverName,
					Pool:    poolName,
					Device:  "device" + suffix,
				}},
			},
		}
		claim.Status.ReservedFor = []resourceapi.ResourceClaimConsumerReference{{
			Resource: "pods",
			Name:     pod.Name,
			UID:      pod.UID,
		}}
		must(tCtx, tCtx.Client().ResourceV1().ResourceClaims(namespace).UpdateStatus, claim, metav1.UpdateOptions{})
	}

	// 新建 factory 并先同步缓存，确保 controller 启动时看到的是最新状态。
	// 仅当本测试是唯一使用者时该方法才成立。
	informerFactory := informers.NewSharedInformerFactory(tCtx.Client(), 0)
	var ruleInformer resourceinformers.DeviceTaintRuleInformer
	if utilfeature.DefaultFeatureGate.Enabled(features.DRADeviceTaintRules) {
		ruleInformer = informerFactory.Resource().V1().DeviceTaintRules()
	}
	controller := devicetainteviction.New(tCtx.Client(),
		informerFactory.Core().V1().Pods(),
		informerFactory.Resource().V1().ResourceClaims(),
		informerFactory.Resource().V1().ResourceSlices(),
		ruleInformer,
		informerFactory.Resource().V1().DeviceClasses(),
		"device-taint-eviction",
	)

	var numExistingPods atomic.Int64
	podsDeleted := make(chan struct{})
	_, _ = informerFactory.Core().V1().Pods().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			pod := obj.(*v1.Pod)
			if pod.Namespace == namespace {
				numExistingPods.Add(1)
			}
		},

		UpdateFunc: func(oldObj, newObj any) {
			oldPod, newPod := oldObj.(*v1.Pod), newObj.(*v1.Pod)
			if newPod.Namespace == namespace &&
				oldPod.DeletionTimestamp == nil &&
				newPod.DeletionTimestamp != nil &&
				numExistingPods.Add(-1) == 0 {
				// 所有 Pod 都已标记删除。
				close(podsDeleted)
			}
		},
	})

	informerFactory.Start(tCtx.Done())
	for t, synced := range informerFactory.WaitForCacheSync(tCtx.Done()) {
		tCtx.Logf("informer %s synced: %v", t, synced)
		if !synced {
			tCtx.Errorf("informer for %s failed to sync", t)
		}
	}
	if tCtx.Failed() {
		tCtx.FailNow()
	}
	wg.Go(func() {
		if err := controller.Run(tCtx, 10 /* workers */); err != nil {
			tCtx.Errorf("Unexpected Run error: %v", err)
		}
	})

	ruleName := "rule-" + namespace
	getV1alpha3Rule := func(tCtx ktesting.TContext) *resourcealpha.DeviceTaintRule {
		return must(tCtx, tCtx.Client().ResourceV1alpha3().DeviceTaintRules().Get, ruleName, metav1.GetOptions{})
	}
	getV1beta2Rule := func(tCtx ktesting.TContext) *resourcebeta.DeviceTaintRule {
		return must(tCtx, tCtx.Client().ResourceV1beta2().DeviceTaintRules().Get, ruleName, metav1.GetOptions{})
	}
	getV1Rule := func(tCtx ktesting.TContext) *resourceapi.DeviceTaintRule {
		return must(tCtx, tCtx.Client().ResourceV1().DeviceTaintRules().Get, ruleName, metav1.GetOptions{})
	}
	var getRule any

	switch useRule {
	case useV1alpha3Rule:
		getRule = getV1alpha3Rule
		// 通过 DeviceTaintRule 触发驱逐。
		// 同时验证其部分语义：从 NoEffect 开始，检查上报结果，手动修改 TimeAdded，
		// 再开启驱逐。
		rule := &resourcealpha.DeviceTaintRule{
			ObjectMeta: metav1.ObjectMeta{
				Name: ruleName,
			},
			Spec: resourcealpha.DeviceTaintRuleSpec{
				DeviceSelector: &resourcealpha.DeviceTaintSelector{
					Driver: &driverName,
				},
				Taint: resourcealpha.DeviceTaint{
					Key:    "testing",
					Effect: resourcealpha.DeviceTaintEffectNone,
				},
			},
		}
		rule = must(tCtx, tCtx.Client().ResourceV1alpha3().DeviceTaintRules().Create, rule, metav1.CreateOptions{})
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			err := tCtx.Client().ResourceV1alpha3().DeviceTaintRules().Delete(tCtx, ruleName, metav1.DeleteOptions{})
			if apierrors.IsNotFound(err) {
				return
			}
			tCtx.ExpectNoError(err)
		})
		tCtx.Expect(rule).To(gomega.HaveField("ObjectMeta.Generation", gomega.Equal(int64(1))))

		tCtx.Eventually(getV1alpha3Rule).
			WithPolling(10*time.Second).
			Should(gomega.HaveField("Status.Conditions", gomega.ConsistOf(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
				"ObservedGeneration": gomega.Equal(rule.Generation),
			}))), "waiting for simulation result")
		rule = getV1alpha3Rule(tCtx)
		tCtx.Expect(rule).To(gomega.HaveField("Status.Conditions", gomega.ConsistOf(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
			"ObservedGeneration": gomega.Equal(rule.Generation),
			"Type":               gomega.Equal(resourcealpha.DeviceTaintConditionEvictionInProgress),
			"Status":             gomega.Equal(metav1.ConditionFalse),
			"Message":            gomega.Equal(fmt.Sprintf("%[1]d published devices selected. %[1]d allocated devices selected. %[1]d pods would be evicted in 1 namespace if the effect was NoExecute. This information will not be updated again. Recreate the DeviceTaintRule to trigger an update.", numPods)),
		}))))

		var newTime *metav1.Time
		for {
			newTime = &metav1.Time{Time: time.Now().Truncate(time.Second)}
			if !newTime.Equal(rule.Spec.Taint.TimeAdded) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		rule.Spec.Taint.TimeAdded = newTime
		rule = must(tCtx, tCtx.Client().ResourceV1alpha3().DeviceTaintRules().Update, rule, metav1.UpdateOptions{})
		if !newTime.Equal(rule.Spec.Taint.TimeAdded) {
			tCtx.Error("our TimeAdded should have been stored")
		}
		tCtx.Expect(rule).To(gomega.HaveField("ObjectMeta.Generation", gomega.Equal(int64(2))))
		tCtx.Eventually(getV1alpha3Rule).
			WithPolling(10*time.Second).
			Should(gomega.HaveField("Status.Conditions", gomega.ConsistOf(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
				"ObservedGeneration": gomega.Equal(rule.Generation),
			}))), "waiting for reaction to TimeAdded bump")
		rule = getV1alpha3Rule(tCtx)
		tCtx.Expect(rule).To(gomega.HaveField("Status.Conditions", gomega.ConsistOf(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
			"ObservedGeneration": gomega.Equal(rule.Generation),
			"Type":               gomega.Equal(resourcealpha.DeviceTaintConditionEvictionInProgress),
			"Status":             gomega.Equal(metav1.ConditionFalse),
			"Message":            gomega.Equal(fmt.Sprintf("%[1]d published devices selected. %[1]d allocated devices selected. %[1]d pods would be evicted in 1 namespace if the effect was NoExecute. This information will not be updated again. Recreate the DeviceTaintRule to trigger an update.", numPods)),
		}))))

		rule.Spec.Taint.Effect = resourcealpha.DeviceTaintEffectNoExecute
		// 必须跨到下一秒：TimeAdded 的精度为秒。不能依赖状态更新的延迟，那个时间可能变化。
		time.Sleep(time.Second)
		rule = must(tCtx, tCtx.Client().ResourceV1alpha3().DeviceTaintRules().Update, rule, metav1.UpdateOptions{})
		if newTime.Equal(rule.Spec.Taint.TimeAdded) {
			tCtx.Error("TimeAdded should have been updated automatically")
		}
	case useV1beta2Rule:
		getRule = getV1beta2Rule
		// 通过 DeviceTaintRule 触发驱逐。
		// 同时验证其部分语义：从 NoEffect 开始，检查上报结果，手动修改 TimeAdded，
		// 再开启驱逐。
		rule := &resourcebeta.DeviceTaintRule{
			ObjectMeta: metav1.ObjectMeta{
				Name: ruleName,
			},
			Spec: resourcebeta.DeviceTaintRuleSpec{
				DeviceSelector: &resourcebeta.DeviceTaintSelector{
					Driver: &driverName,
				},
				Taint: resourcebeta.DeviceTaint{
					Key:    "testing",
					Effect: resourcebeta.DeviceTaintEffectNone,
				},
			},
		}
		rule = must(tCtx, tCtx.Client().ResourceV1beta2().DeviceTaintRules().Create, rule, metav1.CreateOptions{})
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			err := tCtx.Client().ResourceV1beta2().DeviceTaintRules().Delete(tCtx, ruleName, metav1.DeleteOptions{})
			if apierrors.IsNotFound(err) {
				return
			}
			tCtx.ExpectNoError(err)
		})
		tCtx.Expect(rule).To(gomega.HaveField("ObjectMeta.Generation", gomega.Equal(int64(1))))

		tCtx.Eventually(getV1beta2Rule).
			WithPolling(10*time.Second).
			Should(gomega.HaveField("Status.Conditions", gomega.ConsistOf(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
				"ObservedGeneration": gomega.Equal(rule.Generation),
			}))), "waiting for simulation result")
		rule = getV1beta2Rule(tCtx)
		tCtx.Expect(rule).To(gomega.HaveField("Status.Conditions", gomega.ConsistOf(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
			"ObservedGeneration": gomega.Equal(rule.Generation),
			"Type":               gomega.Equal(resourcebeta.DeviceTaintConditionEvictionInProgress),
			"Status":             gomega.Equal(metav1.ConditionFalse),
			"Message":            gomega.Equal(fmt.Sprintf("%[1]d published devices selected. %[1]d allocated devices selected. %[1]d pods would be evicted in 1 namespace if the effect was NoExecute. This information will not be updated again. Recreate the DeviceTaintRule to trigger an update.", numPods)),
		}))))

		var newTime *metav1.Time
		for {
			newTime = &metav1.Time{Time: time.Now().Truncate(time.Second)}
			if !newTime.Equal(rule.Spec.Taint.TimeAdded) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		rule.Spec.Taint.TimeAdded = newTime
		rule = must(tCtx, tCtx.Client().ResourceV1beta2().DeviceTaintRules().Update, rule, metav1.UpdateOptions{})
		if !newTime.Equal(rule.Spec.Taint.TimeAdded) {
			tCtx.Error("our TimeAdded should have been stored")
		}
		tCtx.Expect(rule).To(gomega.HaveField("ObjectMeta.Generation", gomega.Equal(int64(2))))
		tCtx.Eventually(getV1beta2Rule).
			WithPolling(10*time.Second).
			Should(gomega.HaveField("Status.Conditions", gomega.ConsistOf(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
				"ObservedGeneration": gomega.Equal(rule.Generation),
			}))), "waiting for reaction to TimeAdded bump")
		rule = getV1beta2Rule(tCtx)
		tCtx.Expect(rule).To(gomega.HaveField("Status.Conditions", gomega.ConsistOf(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
			"ObservedGeneration": gomega.Equal(rule.Generation),
			"Type":               gomega.Equal(resourcealpha.DeviceTaintConditionEvictionInProgress),
			"Status":             gomega.Equal(metav1.ConditionFalse),
			"Message":            gomega.Equal(fmt.Sprintf("%[1]d published devices selected. %[1]d allocated devices selected. %[1]d pods would be evicted in 1 namespace if the effect was NoExecute. This information will not be updated again. Recreate the DeviceTaintRule to trigger an update.", numPods)),
		}))))

		rule.Spec.Taint.Effect = resourcebeta.DeviceTaintEffectNoExecute
		// 必须跨到下一秒：TimeAdded 的精度为秒。不能依赖状态更新的延迟，那个时间可能变化。
		time.Sleep(time.Second)
		rule = must(tCtx, tCtx.Client().ResourceV1beta2().DeviceTaintRules().Update, rule, metav1.UpdateOptions{})
		if newTime.Equal(rule.Spec.Taint.TimeAdded) {
			tCtx.Error("TimeAdded should have been updated automatically")
		}
	case useV1Rule:
		getRule = getV1Rule
		// 通过 DeviceTaintRule 触发驱逐。
		// 同时验证其部分语义：从 NoEffect 开始，检查上报结果，手动修改 TimeAdded，
		// 再开启驱逐。
		rule := &resourceapi.DeviceTaintRule{
			ObjectMeta: metav1.ObjectMeta{
				Name: ruleName,
			},
			Spec: resourceapi.DeviceTaintRuleSpec{
				DeviceSelector: &resourceapi.DeviceTaintSelector{
					Driver: &driverName,
				},
				Taint: resourceapi.DeviceTaint{
					Key:    "testing",
					Effect: resourceapi.DeviceTaintEffectNone,
				},
			},
		}
		rule = must(tCtx, tCtx.Client().ResourceV1().DeviceTaintRules().Create, rule, metav1.CreateOptions{})
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			err := tCtx.Client().ResourceV1().DeviceTaintRules().Delete(tCtx, ruleName, metav1.DeleteOptions{})
			if apierrors.IsNotFound(err) {
				return
			}
			tCtx.ExpectNoError(err)
		})
		tCtx.Expect(rule).To(gomega.HaveField("ObjectMeta.Generation", gomega.Equal(int64(1))))

		tCtx.Eventually(getV1Rule).
			WithPolling(10*time.Second).
			Should(gomega.HaveField("Status.Conditions", gomega.ConsistOf(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
				"ObservedGeneration": gomega.Equal(rule.Generation),
			}))), "waiting for simulation result")
		rule = getV1Rule(tCtx)
		tCtx.Expect(rule).To(gomega.HaveField("Status.Conditions", gomega.ConsistOf(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
			"ObservedGeneration": gomega.Equal(rule.Generation),
			"Type":               gomega.Equal(resourceapi.DeviceTaintConditionEvictionInProgress),
			"Status":             gomega.Equal(metav1.ConditionFalse),
			"Message":            gomega.Equal(fmt.Sprintf("%[1]d published devices selected. %[1]d allocated devices selected. %[1]d pods would be evicted in 1 namespace if the effect was NoExecute. This information will not be updated again. Recreate the DeviceTaintRule to trigger an update.", numPods)),
		}))))

		var newTime *metav1.Time
		for {
			newTime = &metav1.Time{Time: time.Now().Truncate(time.Second)}
			if !newTime.Equal(rule.Spec.Taint.TimeAdded) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		rule.Spec.Taint.TimeAdded = newTime
		rule = must(tCtx, tCtx.Client().ResourceV1().DeviceTaintRules().Update, rule, metav1.UpdateOptions{})
		if !newTime.Equal(rule.Spec.Taint.TimeAdded) {
			tCtx.Error("our TimeAdded should have been stored")
		}
		tCtx.Expect(rule).To(gomega.HaveField("ObjectMeta.Generation", gomega.Equal(int64(2))))
		tCtx.Eventually(getV1Rule).
			WithPolling(10*time.Second).
			Should(gomega.HaveField("Status.Conditions", gomega.ConsistOf(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
				"ObservedGeneration": gomega.Equal(rule.Generation),
			}))), "waiting for reaction to TimeAdded bump")
		rule = getV1Rule(tCtx)
		tCtx.Expect(rule).To(gomega.HaveField("Status.Conditions", gomega.ConsistOf(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
			"ObservedGeneration": gomega.Equal(rule.Generation),
			"Type":               gomega.Equal(resourcealpha.DeviceTaintConditionEvictionInProgress),
			"Status":             gomega.Equal(metav1.ConditionFalse),
			"Message":            gomega.Equal(fmt.Sprintf("%[1]d published devices selected. %[1]d allocated devices selected. %[1]d pods would be evicted in 1 namespace if the effect was NoExecute. This information will not be updated again. Recreate the DeviceTaintRule to trigger an update.", numPods)),
		}))))

		rule.Spec.Taint.Effect = resourceapi.DeviceTaintEffectNoExecute
		// 必须跨到下一秒：TimeAdded 的精度为秒。不能依赖状态更新的延迟，那个时间可能变化。
		time.Sleep(time.Second)
		rule = must(tCtx, tCtx.Client().ResourceV1().DeviceTaintRules().Update, rule, metav1.UpdateOptions{})
		if newTime.Equal(rule.Spec.Taint.TimeAdded) {
			tCtx.Error("TimeAdded should have been updated automatically")
		}
	case useNoRule:
		// 通过逐个给 device 打 taint 来驱逐。
		for i, slice := range slices {
			slice = slice.DeepCopy()
			slice.Spec.Pool.Generation++
			for i := range slice.Spec.Devices {
				slice.Spec.Devices[i].Taints = []resourceapi.DeviceTaint{{
					Key:    "testing",
					Effect: resourceapi.DeviceTaintEffectNoExecute,
				}}
			}
			slices[i] = must(tCtx, tCtx.Client().ResourceV1().ResourceSlices().Update, slice, metav1.UpdateOptions{})
		}
	default:
		tCtx.Fatalf("unsupported useRule %d", useRule)
	}

	// 触发驱逐并等待所有 Pod 消失。
	start := time.Now()
	select {
	case <-podsDeleted:
		// 符合预期。
	case <-tCtx.Done():
		tCtx.Fatalf("Waiting for pod deletion was canceled: %v", context.Cause(tCtx))
	case <-time.After(30 * time.Second):
		tCtx.Fatal("Timed out waiting for pod deletion")
	}
	duration := time.Since(start)
	tCtx.Logf("Evicted %d pods in %s.", numPods, duration)

	if useRule != useNoRule {
		// 检查上报的 condition。
		tCtx.Eventually(getRule).
			WithPolling(10 * time.Second).
			Should(gomega.HaveField("Status.Conditions", gomega.ConsistOf(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
				"Type":    gomega.Equal(resourcealpha.DeviceTaintConditionEvictionInProgress),
				"Status":  gomega.Equal(metav1.ConditionFalse),
				"Message": gomega.Equal(fmt.Sprintf("%d pods evicted since starting the controller.", numPods)),
			}))))
	}
}

func testNoScheduleRule(tCtx ktesting.TContext, useRule useRuleMode) {
	tCtx.Parallel()

	startScheduler(tCtx)

	namespace := createTestNamespace(tCtx, nil)
	class, driverName := createTestClass(tCtx, namespace)
	slice := st.MakeResourceSlice("worker-0", driverName).Devices(device1)

	taintKey := "testing"
	ruleName := "rule-" + namespace
	switch useRule {
	case useV1alpha3Rule:
		rule := &resourcealpha.DeviceTaintRule{
			ObjectMeta: metav1.ObjectMeta{
				Name: ruleName,
			},
			Spec: resourcealpha.DeviceTaintRuleSpec{
				DeviceSelector: &resourcealpha.DeviceTaintSelector{
					Driver: &driverName,
				},
				Taint: resourcealpha.DeviceTaint{
					Key:    taintKey,
					Effect: resourcealpha.DeviceTaintEffectNoSchedule,
				},
			},
		}
		_ = must(tCtx, tCtx.Client().ResourceV1alpha3().DeviceTaintRules().Create, rule, metav1.CreateOptions{})
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			err := tCtx.Client().ResourceV1alpha3().DeviceTaintRules().Delete(tCtx, ruleName, metav1.DeleteOptions{})
			if apierrors.IsNotFound(err) {
				return
			}
			tCtx.ExpectNoError(err)
		})
	case useV1beta2Rule:
		rule := &resourcebeta.DeviceTaintRule{
			ObjectMeta: metav1.ObjectMeta{
				Name: ruleName,
			},
			Spec: resourcebeta.DeviceTaintRuleSpec{
				DeviceSelector: &resourcebeta.DeviceTaintSelector{
					Driver: &driverName,
				},
				Taint: resourcebeta.DeviceTaint{
					Key:    taintKey,
					Effect: resourcebeta.DeviceTaintEffectNoSchedule,
				},
			},
		}
		_ = must(tCtx, tCtx.Client().ResourceV1beta2().DeviceTaintRules().Create, rule, metav1.CreateOptions{})
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			err := tCtx.Client().ResourceV1beta2().DeviceTaintRules().Delete(tCtx, ruleName, metav1.DeleteOptions{})
			if apierrors.IsNotFound(err) {
				return
			}
			tCtx.ExpectNoError(err)
		})
	case useV1Rule:
		rule := &resourceapi.DeviceTaintRule{
			ObjectMeta: metav1.ObjectMeta{
				Name: ruleName,
			},
			Spec: resourceapi.DeviceTaintRuleSpec{
				DeviceSelector: &resourceapi.DeviceTaintSelector{
					Driver: &driverName,
				},
				Taint: resourceapi.DeviceTaint{
					Key:    taintKey,
					Effect: resourceapi.DeviceTaintEffectNoSchedule,
				},
			},
		}
		_ = must(tCtx, tCtx.Client().ResourceV1().DeviceTaintRules().Create, rule, metav1.CreateOptions{})
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			err := tCtx.Client().ResourceV1().DeviceTaintRules().Delete(tCtx, ruleName, metav1.DeleteOptions{})
			if apierrors.IsNotFound(err) {
				return
			}
			tCtx.ExpectNoError(err)
		})
	case useNoRule:
		slice.Spec.Devices[0].Taints = []resourceapi.DeviceTaint{
			{
				Key:    taintKey,
				Effect: resourceapi.DeviceTaintEffectNoSchedule,
			},
		}
	default:
		tCtx.Fatalf("unsupported useRule %d", useRule)
	}

	// 在 DeviceTaintRule 之后创建 ResourceSlice，以覆盖 ResourceSlice tracker 的额外代码路径。
	_ = createSlice(tCtx, slice.Obj())

	pod := st.MakePod().Name(podName).Namespace(namespace).
		Container("my-container").
		Obj()

	untoleratingClaim := createClaim(tCtx, namespace, "-untolerating", class, claim)
	untoleratingPod := createPod(tCtx, namespace, "-untolerating", pod, untoleratingClaim)
	expectPodUnschedulable(tCtx, untoleratingPod, "cannot allocate all claims")

	toleratingClaim := claim.DeepCopy()
	toleratingClaim.Spec.Devices.Requests[0].Exactly.Tolerations = []resourceapi.DeviceToleration{
		{
			Key:    taintKey,
			Effect: resourceapi.DeviceTaintEffectNoSchedule,
		},
	}
	_ = createClaim(tCtx, namespace, "-tolerating", class, toleratingClaim)
	toleratingPod := createPod(tCtx, namespace, "-tolerating", pod)
	waitForPodScheduled(tCtx, namespace, toleratingPod.Name)
}
