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

package dra

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gstruct"
	gtypes "github.com/onsi/gomega/types"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
	"k8s.io/utils/ptr"
)

const (
	driverNameSuffix = ".driver"
	classNameSuffix  = ".class"
)

// must 包装 Create/Update/Patch/Get/Delete 调用，省去每次重复的错误检查：
//
//	pod = must(tCtx, tCtx.Client().CoreV1().Pods(namespace).Create, pod, metav1.CreateOptions{})
func must[R, P, O any](tCtx ktesting.TContext, call func(context.Context, P, O) (*R, error), p P, o O) *R {
	tCtx.Helper()
	r, err := call(tCtx, p, o)
	tCtx.ExpectNoError(err)
	return r
}

func listObjectNames(list runtime.Object) ([]string, error) {
	items, err := meta.ExtractList(list)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(items))
	for _, item := range items {
		object, err := meta.Accessor(item)
		if err != nil {
			return nil, err
		}
		names = append(names, object.GetName())
	}
	return names, nil
}

// createTestNamespace 根据当前测试名生成 namespace 名称：
// - 去掉 TestDRA 前缀。
// - 非字母数字字符替换为连字符。
// - 从中间截断，保证长度足够供 GenerateName 追加后缀。
// - 由 apiserver 追加连字符和随机后缀。
func createTestNamespace(tCtx ktesting.TContext, labels map[string]string) string {
	tCtx.Helper()
	name := regexp.MustCompile(`[^[:alnum:]_-]`).ReplaceAllString(tCtx.Name(), "-")
	name = strings.ToLower(name)
	name = strings.TrimPrefix(name, "testdra-")
	// 预留足够长度，使生成的名称可作为 driver 名的前缀
	if len(name) > (56 - len(driverNameSuffix)) {
		name = name[:24] + "--" + name[len(name)-24:]
	}
	ns := &v1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: name + "-"}}
	ns.Labels = labels
	ns, err := tCtx.Client().CoreV1().Namespaces().Create(tCtx, ns, metav1.CreateOptions{})
	tCtx.ExpectNoError(err, "create test namespace")
	tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
		tCtx.ExpectNoError(tCtx.Client().CoreV1().Namespaces().Delete(tCtx, ns.Name, metav1.DeleteOptions{}))
		// 此处不等待删除完成，namespace 删除通常较慢
	})
	return ns.Name
}

// createSlice 创建给定的 ResourceSlice，并在测试结束时删除
func createSlice(tCtx ktesting.TContext, slice *resourceapi.ResourceSlice) *resourceapi.ResourceSlice {
	tCtx.Helper()
	slice, err := tCtx.Client().ResourceV1().ResourceSlices().Create(tCtx, slice, metav1.CreateOptions{})
	tCtx.ExpectNoError(err, "create ResourceSlice")
	tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
		tCtx.Log("Cleaning up ResourceSlice...")
		deleteAndWait(tCtx, tCtx.Client().ResourceV1().ResourceSlices().Delete, tCtx.Client().ResourceV1().ResourceSlices().Get, slice.Name)
	})
	return slice
}

// createTestClass 创建 DeviceClass，driver 名由测试 namespace 派生
func createTestClass(tCtx ktesting.TContext, namespace string) (*resourceapi.DeviceClass, string) {
	return createTestClassWithSpec(tCtx, namespace, nil)
}

func createTestClassWithSpec(tCtx ktesting.TContext, namespace string, spec *resourceapi.DeviceClassSpec) (*resourceapi.DeviceClass, string) {
	tCtx.Helper()
	driverName := namespace + driverNameSuffix
	class := class.DeepCopy()
	class.Name = namespace + classNameSuffix
	if spec != nil {
		class.Spec = *spec
	}
	// 调用方未提供 selector 时，默认按 driver 名筛选
	if len(class.Spec.Selectors) == 0 {
		class.Spec.Selectors = []resourceapi.DeviceSelector{{
			CEL: &resourceapi.CELDeviceSelector{
				Expression: fmt.Sprintf("device.driver == %q", driverName),
			},
		}}
	}
	class, err := tCtx.Client().ResourceV1().DeviceClasses().Create(tCtx, class, metav1.CreateOptions{})
	tCtx.ExpectNoError(err, "create class")
	tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
		tCtx.Log("Cleaning up DeviceClass...")
		deleteAndWait(tCtx, tCtx.Client().ResourceV1().DeviceClasses().Delete, tCtx.Client().ResourceV1().DeviceClasses().Get, class.Name)
	})

	return class, driverName
}

// createClaim 在指定 namespace 中创建 claim。
// class 必须已存在，并用于所有 request。
func createClaim(tCtx ktesting.TContext, namespace string, suffix string, class *resourceapi.DeviceClass, claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
	tCtx.Helper()
	claim = claim.DeepCopy()
	claim.Namespace = namespace
	claim.Name += suffix
	claimName := claim.Name
	for i := range claim.Spec.Devices.Requests {
		request := &claim.Spec.Devices.Requests[i]
		if request.Exactly != nil && request.Exactly.DeviceClassName != "" {
			request.Exactly.DeviceClassName = class.Name
			continue
		}
		for e := range request.FirstAvailable {
			subRequest := &request.FirstAvailable[e]
			subRequest.DeviceClassName = class.Name
		}
	}
	claim, err := tCtx.Client().ResourceV1().ResourceClaims(namespace).Create(tCtx, claim, metav1.CreateOptions{})
	tCtx.ExpectNoError(err, "create claim "+claimName)
	// TODO: 部分测试会泄漏 claim，后续需要修复。
	// tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
	//		// 用于观察清理过程何时卡住
	//	deleteAndWait(tCtx, tCtx.Client().ResourceV1().ResourceClaims(namespace).Delete, tCtx.Client().ResourceV1().ResourceClaims(namespace).Get, claim.Name)
	// })
	return claim
}

// createPod 在指定 namespace 中创建引用给定 claim 的 Pod
func createPod(tCtx ktesting.TContext, namespace string, suffix string, pod *v1.Pod, claims ...*resourceapi.ResourceClaim) *v1.Pod {
	return createPodInternal(tCtx, namespace, suffix, pod, nil, claims...)
}

// createPodWithExtendedResource 在指定 namespace 中创建请求给定扩展资源的 Pod
func createPodWithExtendedResource(tCtx ktesting.TContext, namespace, resourceName, resourceQuantity string, pod *v1.Pod) *v1.Pod {
	return createPodInternal(tCtx, namespace, "", pod, map[v1.ResourceName]string{
		v1.ResourceName(resourceName): resourceQuantity,
	})
}

// createPodInternal 提取 createPod 与 createPodWithExtendedResource 的公共逻辑，
// 测试中不应直接调用。
var createPodInternal = func(tCtx ktesting.TContext, namespace string, suffix string, pod *v1.Pod, resources map[v1.ResourceName]string, claims ...*resourceapi.ResourceClaim) *v1.Pod {
	tCtx.Helper()
	pod = pod.DeepCopy()
	pod.Name += suffix
	podName := pod.Name
	pod.Namespace = namespace
	var resourceClaims []v1.PodResourceClaim
	for _, claim := range claims {
		resourceClaims = append(resourceClaims, v1.PodResourceClaim{
			Name:              claim.Name,
			ResourceClaimName: &claim.Name,
		})
	}
	for res, qty := range resources {
		for i := range pod.Spec.Containers {
			container := &pod.Spec.Containers[i]
			if container.Resources.Requests == nil {
				container.Resources.Requests = v1.ResourceList{}
			}
			if container.Resources.Limits == nil {
				container.Resources.Limits = v1.ResourceList{}
			}
			container.Resources.Requests[res] = resource.MustParse(qty)
			container.Resources.Limits[res] = resource.MustParse(qty)
		}
	}
	if len(claims) > 0 {
		// 仅在传入 claim 时才设置该字段
		pod.Spec.ResourceClaims = resourceClaims
	}
	pod, err := tCtx.Client().CoreV1().Pods(namespace).Create(tCtx, pod, metav1.CreateOptions{})
	tCtx.ExpectNoError(err, "create pod "+podName)
	tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
		tCtx.Log("Cleaning up Pod...")
		// 必须在卸载 driver 前删除 Pod，并观察删除过程是否卡住
		deleteAndWait(tCtx, tCtx.Client().CoreV1().Pods(namespace).Delete, tCtx.Client().CoreV1().Pods(namespace).Get, pod.Name)
	})
	return pod
}

func waitForPodScheduled(tCtx ktesting.TContext, namespace, podName string) *v1.Pod {
	tCtx.Helper()

	var pod *v1.Pod
	tCtx.Eventually(func(tCtx ktesting.TContext) (*v1.Pod, error) {
		p, err := tCtx.Client().CoreV1().Pods(namespace).Get(tCtx, podName, metav1.GetOptions{})
		pod = p
		return p, err
	}).WithTimeout(60*time.Second).Should(
		gomega.HaveField("Status.Conditions", gomega.ContainElement(
			gomega.And(
				gomega.HaveField("Type", gomega.Equal(v1.PodScheduled)),
				gomega.HaveField("Status", gomega.Equal(v1.ConditionTrue)),
			),
		)),
		"Pod %s should have been scheduled.", podName,
	)
	return pod
}

func deleteAndWait[T any](tCtx ktesting.TContext, del func(context.Context, string, metav1.DeleteOptions) error, get func(context.Context, string, metav1.GetOptions) (T, error), name string) {
	tCtx.Helper()

	var t T
	var anyT any = t
	var options metav1.DeleteOptions
	if _, ok := anyT.(*v1.Pod); ok {
		// Pod 需要特殊处理：没有 kubelet 确认已调度 Pod 的关闭，只能强制删除
		options.GracePeriodSeconds = ptr.To(int64(0))
	}

	err := del(tCtx, name, options)
	if !apierrors.IsNotFound(err) {
		tCtx.ExpectNoError(err, fmt.Sprintf("delete %T %s", t, name))
	}
	waitForNotFound(tCtx, get, name)
}

func waitForNotFound[T any](tCtx ktesting.TContext, get func(context.Context, string, metav1.GetOptions) (T, error), name string) {
	tCtx.Helper()

	var t T
	tCtx.Eventually(func(tCtx ktesting.TContext) error {
		_, err := get(tCtx, name, metav1.GetOptions{})
		return err
	}).WithTimeout(60*time.Second).Should(gomega.MatchError(apierrors.IsNotFound, "IsNotFound"), "Object %T %s should have been removed.", t, name)
}

func waitForClaim(tCtx ktesting.TContext, namespace, claimName string, timeout time.Duration, match gtypes.GomegaMatcher, description ...any) *resourceapi.ResourceClaim {
	tCtx.Helper()
	var latestClaim *resourceapi.ResourceClaim
	tCtx.Eventually(func(tCtx ktesting.TContext) (*resourceapi.ResourceClaim, error) {
		c, err := tCtx.Client().ResourceV1().ResourceClaims(namespace).Get(tCtx, claimName, metav1.GetOptions{})
		latestClaim = c
		return c, err
	}).WithTimeout(timeout).WithPolling(time.Second).Should(match, description...)
	return latestClaim
}

func waitForClaimAllocatedToDevice(tCtx ktesting.TContext, namespace, claimName string, timeout time.Duration) *resourceapi.ResourceClaim {
	tCtx.Helper()
	return waitForClaim(
		tCtx,
		namespace,
		claimName,
		timeout,
		gomega.HaveField("Status.Allocation", gomega.Not(gomega.BeNil())),
		"Claim should have been allocated.",
	)
}

func expectPodSchedulerError(tCtx ktesting.TContext, pod *v1.Pod, reason string) {
	tCtx.Helper()
	tCtx.ExpectNoError(e2epod.WaitForPodCondition(tCtx, tCtx.Client(), pod.Namespace, pod.Name, v1.PodReasonSchedulerError, time.Minute, func(pod *v1.Pod) (bool, error) {
		if pod.Status.Phase == v1.PodPending {
			for _, cond := range pod.Status.Conditions {
				if cond.Type == v1.PodScheduled && cond.Status == v1.ConditionFalse && cond.Reason == v1.PodReasonSchedulerError && strings.Contains(cond.Message, reason) {
					return true, nil
				}
			}
		}
		return false, nil
	}), fmt.Sprintf("expected pod to have scheduler error because %q", reason))
}

type nodeInfo struct {
	name       string
	driverName string
	class      *resourceapi.DeviceClass
	pool       string
}

func expectedAllocatedClaim(request string, nodeInfo nodeInfo) gtypes.GomegaMatcher {
	return gomega.HaveField("Status.Allocation", gstruct.PointTo(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
		"Devices": gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
			"Results": gomega.HaveExactElements(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
				"Request": gomega.Equal(request),
				"Driver":  gomega.Equal(nodeInfo.driverName),
				"Pool":    gomega.Equal(nodeInfo.pool),
			})),
		}),
		"NodeSelector": gstruct.PointTo(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
			"NodeSelectorTerms": gomega.HaveExactElements(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
				"MatchFields": gomega.HaveExactElements(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
					"Key":      gomega.Equal("metadata.name"),
					"Operator": gomega.Equal(v1.NodeSelectorOpIn),
					"Values":   gomega.HaveExactElements(gomega.Equal(nodeInfo.name)),
				})),
			})),
		})),
	})))
}
