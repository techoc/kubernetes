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
	"time"

	"github.com/onsi/gomega"
	resourceapi "k8s.io/api/resource/v1"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	drautils "k8s.io/kubernetes/test/e2e/dra/utils"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
)

func workloadResourceClaimsDriverResources(nodes *drautils.Nodes) map[string]resourceslice.DriverResources {
	return drautils.DriverResourcesNow(nodes, 1)
}

// workloadResourceClaimsGateCycle 实现 DRAWorkloadResourceClaims feature gate 的 ON→OFF→ON 循环测试：
//
//   - 阶段 0（gate 开启）：创建一个 PodGroup，其中 2 个成员 Pod 共享为该
//     PodGroup 预留的 ResourceClaim。
//   - 阶段 1（gate 关闭）：ResourceClaim 仍为该 PodGroup 预留，且不会创建新的
//     ResourceClaim。使用 PodGroup 预留 claim 的 Pod 仍可被删除。
//   - 阶段 2（gate 重新开启）：新加入组的 Pod 可以共享阶段 0 初次分配的 ResourceClaim。
func workloadResourceClaimsGateCycle(tCtx ktesting.TContext, b *drautils.Builder) gateOffFunc {
	namespace := tCtx.Namespace()
	workload, template := b.WorkloadInline()
	podGroup := b.PodGroup(workload, workload.Spec.PodGroupTemplates[0])
	pod := b.GroupedPodWithClaims(podGroup)
	podToDelete := b.GroupedPodWithClaims(podGroup)
	b.Create(tCtx, workload, podGroup, template, pod, podToDelete)
	b.TestPod(tCtx, pod)
	b.TestPod(tCtx, podToDelete)

	podGroup, err := tCtx.Client().SchedulingV1beta1().PodGroups(namespace).Get(tCtx, podGroup.Name, metav1.GetOptions{})
	tCtx.ExpectNoError(err, "get PodGroup")
	tCtx.Expect(podGroup.Status.ResourceClaimStatuses).To(gomega.HaveExactElements(
		gomega.SatisfyAll(
			gomega.HaveField("Name", podGroup.Spec.ResourceClaims[0].Name),
			gomega.HaveField("ResourceClaimName", gomega.HaveValue(gomega.Not(gomega.BeEmpty()))),
		),
	), "PodGroup status is missing generated ResourceClaim name")

	claimName := *podGroup.Status.ResourceClaimStatuses[0].ResourceClaimName
	claim, err := tCtx.Client().ResourceV1().ResourceClaims(namespace).Get(tCtx, claimName, metav1.GetOptions{})
	tCtx.ExpectNoError(err, "get generated ResourceClaim")
	reservedFor := claim.Status.ReservedFor
	tCtx.Expect(reservedFor).To(gomega.HaveExactElements(
		resourceapi.ResourceClaimConsumerReference{
			APIGroup: schedulingapi.GroupName,
			Resource: "podgroups",
			Name:     podGroup.Name,
			UID:      podGroup.UID,
		},
	))

	checkClaimsStable := func(tCtx ktesting.TContext) {
		tCtx.Helper()
		tCtx.Consistently(func(tCtx ktesting.TContext) (*resourceapi.ResourceClaimList, error) {
			return tCtx.Client().ResourceV1().ResourceClaims(namespace).List(tCtx, metav1.ListOptions{})
		}).
			WithPolling(100 * time.Millisecond).
			WithTimeout(15 * time.Second).
			Should(gomega.HaveField("Items", gomega.HaveExactElements(
				gomega.SatisfyAll(
					gomega.HaveField("ObjectMeta.UID", claim.UID),
					gomega.HaveField("Status.ReservedFor", reservedFor),
				),
			)))
	}

	return func(tCtx ktesting.TContext) gateOnAgainFunc {
		// 功能禁用时，PodGroup 中新加入的 Pod 即使请求与其他 Pod 相同的 claim
		// 也无法调度：ResourceClaim controller 不会把 PodGroup 的 claim 与该 Pod 关联。
		// Kubernetes 1.36 会为该 Pod 创建新的 ResourceClaim，这里要验证这种行为*不会*再出现。
		stuckPod := b.GroupedPodWithClaims(podGroup)
		b.Create(tCtx, stuckPod)
		checkClaimsStable(tCtx)

		// 无论 Pod 是在功能禁用前还是禁用后创建，都应能被删除。
		b.DeletePodAndWaitForNotFound(tCtx, stuckPod)
		b.DeletePodAndWaitForNotFound(tCtx, podToDelete)

		return func(tCtx ktesting.TContext) {
			newPod := b.GroupedPodWithClaims(podGroup)
			b.Create(tCtx, newPod)
			b.TestPod(tCtx, newPod)
			checkClaimsStable(tCtx)
		}
	}
}
