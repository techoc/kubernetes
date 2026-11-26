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

package e2edra

import (
	"fmt"
	"slices"
	"time"

	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	"k8s.io/kubernetes/test/e2e/dra/test-driver/app"
	drautils "k8s.io/kubernetes/test/e2e/dra/utils"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
)

const (
	resourceTypeExplicit = "explicit"
	resourceTypeImplicit = "implicit"
)

func extendedResourcesDriverResources(nodes *drautils.Nodes) map[string]resourceslice.DriverResources {
	return drautils.DriverResourcesNow(nodes, 8)
}

// extendedResourceUpgradeDowngrade 在升级/降级场景中测试 DRAExtendedResources 特性，
// 针对特定资源类型（显式或隐式）。
// 本测试验证：
// 1. 可以用指定的 extendedResourceName 类型创建 DeviceClass
// 2. 请求扩展资源的 Pod 在升级前被调度并分配设备
// 3. 升级后已有 Pod 继续正常运行
// 4. 升级后带扩展资源的新 Pod 正常工作
// 5. 降级后所有 Pod 继续正常运行
// 6. Pod 删除时 ResourceClaim 被 GC
func extendedResourceUpgradeDowngrade(resourceType string) initialTestFunc {
	return func(tCtx ktesting.TContext, b *drautils.Builder) upgradedTestFunc {
		return testExtendedResource(tCtx, b, resourceType)
	}
}

// testExtendedResource 测试单一扩展资源类型的升级/降级周期。
// 它创建 DeviceClass，在升级前创建 Pod，升级后校验，升级后创建新 Pod，
// 降级后校验并清理所有 Pod。
func testExtendedResource(tCtx ktesting.TContext, b *drautils.Builder, resourceType string) upgradedTestFunc {
	namespace := tCtx.Namespace()

	// 创建 DeviceClass 并获取资源名
	resourceName := createDeviceClassForExtendedResource(tCtx, b, resourceType)

	// 升级前：创建并校验 Pod
	podBefore := createPodWithExtendedResource(tCtx, b, fmt.Sprintf("%s-before-upgrade", resourceType), resourceName)
	verifyPodRunningWithClaim(tCtx, b, podBefore, resourceName)

	return func(tCtx ktesting.TContext) downgradedTestFunc {
		// 升级后：校验已有 Pod 仍正常运行
		verifyPodRunningWithClaim(tCtx, b, podBefore, resourceName)

		// 再创建并校验一个使用同一扩展资源的 Pod
		podAfter := createPodWithExtendedResource(tCtx, b, fmt.Sprintf("%s-after-upgrade", resourceType), resourceName)
		verifyPodRunningWithClaim(tCtx, b, podAfter, resourceName)

		return func(tCtx ktesting.TContext) {
			// 降级后：校验所有 Pod 及其 claim 仍正常运行
			pods := []*v1.Pod{podBefore, podAfter}
			for _, pod := range pods {
				verifyPodRunningWithClaim(tCtx, b, pod, resourceName)
			}

			// 删除 Pod 前先收集 claim 名，以便校验 GC。
			claimNames := make([]string, 0, len(pods))
			for _, pod := range pods {
				claimNames = append(claimNames, claimNameFromPod(tCtx, pod))
			}

			// 清理 Pod
			tCtx.Logf("Cleaning up %s resource pods after downgrade", resourceType)
			for _, pod := range pods {
				tCtx.ExpectNoError(e2epod.DeletePodWithWaitByName(tCtx, tCtx.Client(), pod.Name, namespace))
			}
			tCtx.Logf("Successfully cleaned up %s resource pods", resourceType)

			eventuallyClaimsGone(tCtx, claimNames, 3*time.Minute)
		}
	}
}

// extendedResourceGateCycle 测试 DRAExtendedResource 特性 gate 的 ON→OFF→ON 周期：
//
//   - 阶段 0（gate ON）：创建带扩展资源 claim 的 survivor Pod 和待删除 Pod，
//     校验它们运行且已分配。
//   - 阶段 1（gate OFF）：断言 survivor 的 claim 保留；校验 apiserver 会剥离新
//     DeviceClass.ExtendedResourceName 字段；校验 probe Pod 在没有特殊 claim 时保持
//     Pending；删除待删除 Pod 并校验其 claim 被 GC。
//   - 阶段 2（再次 gate ON）：等待 kubelet 的 reconcile 循环 unprepare 阶段 1 中
//     删除的 claim；校验 survivor 仍在运行；端到端分配一个新 Pod；
//     清理。

func extendedResourceGateCycle(tCtx ktesting.TContext, b *drautils.Builder) gateOffFunc {
	var podSurvivor, podToDelete *v1.Pod
	var resourceName v1.ResourceName
	tCtx.Run("pods-with-extended-resource-running", func(tCtx ktesting.TContext) {
		resourceName = createDeviceClassForExtendedResource(tCtx, b, resourceTypeExplicit)
		podSurvivor = createPodWithExtendedResource(tCtx, b, "survivor", resourceName)
		verifyPodRunningWithClaim(tCtx, b, podSurvivor, resourceName)
		podToDelete = createPodWithExtendedResource(tCtx, b, "to-delete", resourceName)
		verifyPodRunningWithClaim(tCtx, b, podToDelete, resourceName)
	})

	return func(tCtx ktesting.TContext) gateOnAgainFunc {
		tCtx.Run("survivor-pods-still-running", func(tCtx ktesting.TContext) {
			tCtx.ExpectNoError(e2epod.WaitForPodRunningInNamespace(tCtx, tCtx.Client(), podSurvivor))
			tCtx.ExpectNoError(e2epod.WaitForPodRunningInNamespace(tCtx, tCtx.Client(), podToDelete))
		})

		// 校验每个 Pod 的特殊 ResourceClaim 仍存在且已分配。
		tCtx.Run("survivor-claim-preserved", func(tCtx ktesting.TContext) {
			for _, pod := range []*v1.Pod{podSurvivor, podToDelete} {
				claimName := claimNameFromPod(tCtx, pod)
				claim, err := tCtx.Client().ResourceV1().ResourceClaims(pod.Namespace).Get(tCtx, claimName, metav1.GetOptions{})
				tCtx.ExpectNoError(err, "get ResourceClaim %s", claimName)
				tCtx.Expect(claim.Status.Allocation).ToNot(gomega.BeNil(),
					"ResourceClaim %s must still be allocated", claimName)
			}
		})

		tCtx.Run("new-deviceclass-field-stripped", func(tCtx ktesting.TContext) {
			dc := b.ClassWithExtendedResource(string(resourceName))
			dc.Name += "-post-off"
			created := b.Create(tCtx, dc)[0].(*resourceapi.DeviceClass)
			tCtx.Expect(created.Spec.ExtendedResourceName).To(gomega.BeNil(),
				"DeviceClass.Spec.ExtendedResourceName must be stripped when gate is off")
		})

		// 检查 probe 保持 Pending 且从未拿到特殊 claim。
		tCtx.Run("probe-pod-pending-no-special-claim", func(tCtx ktesting.TContext) {
			probePod := createPodWithExtendedResource(tCtx, b, "probe-off", resourceName)
			tCtx.WithStep("probe pod stays Pending").
				Consistently(func(tCtx ktesting.TContext) v1.PodPhase {
					p, err := tCtx.Client().CoreV1().Pods(probePod.Namespace).Get(tCtx, probePod.Name, metav1.GetOptions{})
					tCtx.ExpectNoError(err)
					tCtx.Expect(p.Status.ExtendedResourceClaimStatus).To(gomega.BeNil(),
						"probe pod must not get a special claim when gate is off")
					return p.Status.Phase
				}).WithTimeout(30 * time.Second).Should(gomega.Equal(v1.PodPending))

			// 清理 probe Pod。
			tCtx.ExpectNoError(e2epod.DeletePodWithWaitByName(tCtx, tCtx.Client(), probePod.Name, probePod.Namespace))
		})

		var toDeleteClaimName string
		tCtx.Run("delete-pod-triggers-gc-with-gate-off", func(tCtx ktesting.TContext) {
			// KEP 要求：gate 关闭时删除 Pod，必须由 GC（而不是
			// apiserver）移除 claim，且节点记账必须释放
			// 设备。
			toDeleteClaimName = claimNameFromPod(tCtx, podToDelete)
			tCtx.ExpectNoError(e2epod.DeletePodWithWaitByName(tCtx, tCtx.Client(), podToDelete.Name, podToDelete.Namespace))
			eventuallyClaimsGone(tCtx, []string{toDeleteClaimName}, 3*time.Minute)
			tCtx.ExpectNoError(e2epod.WaitForPodRunningInNamespace(tCtx, tCtx.Client(), podSurvivor))
		})

		return func(tCtx ktesting.TContext) {
			// 等待 kubelet 的 reconcile 循环（周期 60s）为 to-delete 调用 NodeUnprepareResources，
			// 它是在 gate 关闭时被删除的。reconcile 循环只在 gate 开启时调用 unprepare，
			// 因此这必须发生在翻转之后。
			tCtx.Run("wait-for-claim-unprepared", func(tCtx ktesting.TContext) {
				for host, plugin := range b.Driver.Nodes {
					tCtx.WithStep(fmt.Sprintf("wait for claim %s on %s to be unprepared", toDeleteClaimName, host)).
						Eventually(func(ktesting.TContext) []app.ClaimID {
							return plugin.GetPreparedResources()
						}).WithTimeout(3*time.Minute).ShouldNot(gomega.ContainElement(gomega.HaveField("Name", gomega.HavePrefix(toDeleteClaimName))),
						// 用 HavePrefix，因为 app.ClaimID.Name 是 "<claimName>/<device>"，不是裸 claim 名。
						"claim %s on host %s should be unprepared after gate re-enable", toDeleteClaimName, host)
				}
			})

			tCtx.Run("survivor-pod-survives-reenable", func(tCtx ktesting.TContext) {
				verifyPodRunningWithClaim(tCtx, b, podSurvivor, resourceName)
			})

			var freshPod *v1.Pod
			tCtx.Run("fresh-pod-allocates-end-to-end", func(tCtx ktesting.TContext) {
				freshPod = createPodWithExtendedResource(tCtx, b, "fresh", resourceName)
				verifyPodRunningWithClaim(tCtx, b, freshPod, resourceName)
			})

			claimNames := []string{
				claimNameFromPod(tCtx, podSurvivor),
				claimNameFromPod(tCtx, freshPod),
			}

			tCtx.Run("cleanup-pods", func(tCtx ktesting.TContext) {
				tCtx.ExpectNoError(e2epod.DeletePodWithWaitByName(tCtx, tCtx.Client(), podSurvivor.Name, podSurvivor.Namespace))
				tCtx.ExpectNoError(e2epod.DeletePodWithWaitByName(tCtx, tCtx.Client(), freshPod.Name, freshPod.Namespace))
			})

			tCtx.Run("claims-GC", func(tCtx ktesting.TContext) {
				eventuallyClaimsGone(tCtx, claimNames, 3*time.Minute)
			})
		}
	}
}

// createDeviceClassForExtendedResource 获取或创建用于测试扩展资源的 DeviceClass。
// 显式资源类型时，创建一个带唯一 extendedResourceName 的 DeviceClass 并返回该名称。
// 隐式资源类型时，返回 builder 创建的已有 DeviceClass 推导出的资源名。
func createDeviceClassForExtendedResource(tCtx ktesting.TContext, b *drautils.Builder, resourceType string) v1.ResourceName {
	switch resourceType {
	case resourceTypeExplicit:
		resourceName := v1.ResourceName(b.ExtendedResourceName(resourceTypeExplicit))
		deviceClass := b.ClassWithExtendedResource(string(resourceName))
		// 创建 DeviceClass
		createdObjs := b.Create(tCtx, deviceClass)
		// 校验 spec 中存在 ExtendedResourceName
		deviceClass = createdObjs[0].(*resourceapi.DeviceClass)
		tCtx.Expect(deviceClass.Spec.ExtendedResourceName).ToNot(gomega.BeNil(), "DeviceClass.Spec.ExtendedResourceName should be set for explicit resource name")
		tCtx.Logf("Created DeviceClass %q with extendedResourceName: %s", deviceClass.Name, resourceName)
		return resourceName
	case resourceTypeImplicit:
		// DeviceClass 已由 builder 创建，只需拿到名称。
		return v1.ResourceName(fmt.Sprintf("%s%s", resourceapi.ResourceDeviceClassPrefix, b.ClassName()))
	default:
		tCtx.Fatalf("invalid resource type %q", resourceType)
		return ""
	}
}

// claimNameFromPod 重新获取 Pod，并从 pod.Status.ExtendedResourceClaimStatus 返回 ResourceClaim 名。

func claimNameFromPod(tCtx ktesting.TContext, pod *v1.Pod) string {
	tCtx.Helper()
	p, err := tCtx.Client().CoreV1().Pods(pod.Namespace).Get(tCtx, pod.Name, metav1.GetOptions{})
	tCtx.ExpectNoError(err, "get pod %s", pod.Name)
	tCtx.Expect(p.Status.ExtendedResourceClaimStatus).ToNot(gomega.BeNil(),
		"pod %s must have ExtendedResourceClaimStatus", pod.Name)
	return p.Status.ExtendedResourceClaimStatus.ResourceClaimName
}

// createPodWithExtendedResource 创建带给定名称、请求扩展资源的 Pod。
func createPodWithExtendedResource(tCtx ktesting.TContext, b *drautils.Builder, podName string, resourceName v1.ResourceName) *v1.Pod {
	pod := b.Pod()
	pod.Name = podName
	resources := v1.ResourceList{resourceName: resource.MustParse("1")}
	pod.Spec.Containers[0].Resources.Requests = resources
	pod.Spec.Containers[0].Resources.Limits = resources
	pod = b.Create(tCtx, pod)[0].(*v1.Pod)
	tCtx.Logf("Created pod %q requesting extended resource %q", pod.Name, resourceName)
	return pod
}

// verifyPodRunningWithClaim 校验 Pod 正在运行且有有效的 ResourceClaim。
func verifyPodRunningWithClaim(tCtx ktesting.TContext, b *drautils.Builder, pod *v1.Pod, resourceName v1.ResourceName) {
	namespace := tCtx.Namespace()
	// 用 Eventually 重试完整的运行+exec 检查。集群升级和
	// 降级期间节点会临时重启，即使 API server 报告 Pod 为 Running，
	// 也可能出现瞬时 exec 失败
	//（"unable to upgrade connection: pod does not exist"）。
	tCtx.Eventually(func(tCtx ktesting.TContext) {
		b.TestPod(tCtx, pod, "container_0_request_0", "true")
	}).WithTimeout(framework.PodStartTimeout).Should(gomega.Succeed(), "pod %s should be running with expected env", pod.Name)
	tCtx.Logf("Pod %q is running", pod.Name)

	// 获取更新后的 Pod 检查状态
	pod, err := tCtx.Client().CoreV1().Pods(namespace).Get(tCtx, pod.Name, metav1.GetOptions{})
	tCtx.ExpectNoError(err, "get pod to check status")

	// 校验 pod.status.extendedResourceClaimStatus 包含该映射
	tCtx.Expect(pod.Status.ExtendedResourceClaimStatus).ToNot(gomega.BeNil(), "pod.status.extendedResourceClaimStatus should not be nil for pod %s", pod.Name)

	// 从 Pod 状态获取 claim 名
	claimName := pod.Status.ExtendedResourceClaimStatus.ResourceClaimName
	tCtx.Logf("Verified extendedResourceClaimStatus.ResourceClaimName: %s for pod %q", claimName, pod.Name)

	// 用 Pod 状态中的名字获取 ResourceClaim
	claim, err := tCtx.Client().ResourceV1().ResourceClaims(namespace).Get(tCtx, claimName, metav1.GetOptions{})
	tCtx.ExpectNoError(err, "getting ResourceClaim %s", claimName)

	// 校验 claim 已分配
	tCtx.Expect(claim.Status.Allocation).ToNot(gomega.BeNil(), "ResourceClaim %s should be allocated", claim.Name)
	tCtx.Logf("ResourceClaim %q is allocated for pod %q", claim.Name, pod.Name)

	// 校验 request mappings 包含该扩展资源
	foundMapping := slices.ContainsFunc(pod.Status.ExtendedResourceClaimStatus.RequestMappings, func(reqMapping v1.ContainerExtendedResourceRequest) bool {
		if reqMapping.ResourceName == string(resourceName) {
			tCtx.Logf("Found request mapping for pod %q: container=%s, resource=%s, request=%s",
				pod.Name, reqMapping.ContainerName, reqMapping.ResourceName, reqMapping.RequestName)
			return true
		}
		return false
	})
	tCtx.Expect(foundMapping).To(gomega.BeTrueBecause("extendedResourceClaimStatus.RequestMappings should contain %s for pod %s", resourceName, pod.Name))
}
