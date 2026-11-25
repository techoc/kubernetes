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
	"math/rand/v2"
	"time"

	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gstruct"
	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
)

func testPrioritizedList(tCtx ktesting.TContext, enabled bool) {
	tCtx.Parallel()
	namespace := createTestNamespace(tCtx, nil)
	class, _ := createTestClass(tCtx, namespace)
	// 功能关闭时允许创建失败。createClaim 通常不返回错误，这里通过 WithError 间接获取。
	claim, err := func() (claim *resourceapi.ResourceClaim, finalError error) {
		tCtx, finalize := tCtx.WithError(&finalError)
		defer finalize()
		return createClaim(tCtx, namespace, "", class, claimPrioritizedList), nil
	}()

	if !enabled {
		require.Error(tCtx, err, "claim should have become invalid after dropping FirstAvailable")
		return
	}

	require.NoError(tCtx, err, "create ResourceClaim")
	require.NotEmpty(tCtx, claim.Spec.Devices.Requests[0].FirstAvailable, "should store FirstAvailable")
	startScheduler(tCtx)

	// 不必为 Node 创建匹配的 ResourceSlice：只要失败发生在 Filter 阶段，
	// 就足以说明没有因为 FirstAvailable 在 PreFilter 阶段失败。
	pod := createPod(tCtx, namespace, "", podWithClaimName, claim)
	schedulingAttempted := gomega.HaveField("Status.Conditions", gomega.ContainElement(
		gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
			"Type":    gomega.Equal(v1.PodScheduled),
			"Status":  gomega.Equal(v1.ConditionFalse),
			"Reason":  gomega.Equal("Unschedulable"),
			"Message": gomega.Equal("0/8 nodes are available: 8 cannot allocate all claims. preemption: 0/8 nodes are available: 8 Preemption is not helpful for scheduling."),
		}),
	))
	tCtx.Eventually(func(tCtx ktesting.TContext) (*v1.Pod, error) {
		return tCtx.Client().CoreV1().Pods(namespace).Get(tCtx, pod.Name, metav1.GetOptions{})
	}).WithTimeout(schedulingTimeout).WithPolling(time.Second).Should(schedulingAttempted)
}

func testPrioritizedListScoring(tCtx ktesting.TContext) {
	tCtx.Parallel()
	namespace := createTestNamespace(tCtx, nil)
	nodes, err := tCtx.Client().CoreV1().Nodes().List(tCtx, metav1.ListOptions{})
	tCtx.ExpectNoError(err, "list nodes")

	// 子请求最多 8 个，因此测试用的 Node 也不超过 8 个。
	var nodesForTest []v1.Node
	if len(nodes.Items) > 8 {
		nodesForTest = nodes.Items[:8]
	} else {
		nodesForTest = nodes.Items
	}

	// 为每个 Node 分别创建 device class 和 driver，便于构造只能由特定 Node 满足的子请求。
	var nodeInfos []nodeInfo
	for _, node := range nodesForTest {
		driverName := fmt.Sprintf("%s-%s", namespace, node.Name)
		class, driverName := createTestClass(tCtx, driverName)
		slice := st.MakeResourceSlice(node.Name, driverName).Devices(device1, device2)
		createSlice(tCtx, slice.Obj())
		nodeInfos = append(nodeInfos, nodeInfo{
			name:       node.Name,
			driverName: driverName,
			class:      class,
			pool:       slice.Spec.Pool.Name,
		})
	}

	// 打乱 Node 顺序，避免总是选中第一个。
	rand.Shuffle(len(nodeInfos), func(i, j int) {
		nodeInfos[i], nodeInfos[j] = nodeInfos[j], nodeInfos[i]
	})

	startScheduler(tCtx)

	runSubTest(tCtx, "single-claim", func(tCtx ktesting.TContext) {
		var firstAvailable []resourceapi.DeviceSubRequest
		for i := range nodeInfos {
			firstAvailable = append(firstAvailable, resourceapi.DeviceSubRequest{
				Name:            fmt.Sprintf("subreq-%d", i),
				DeviceClassName: nodeInfos[i].class.Name,
			})
		}
		claimPrioritizedList := st.MakeResourceClaim().
			Name(claimName + "-pl-single-claim").
			Namespace(namespace).
			RequestWithPrioritizedList(firstAvailable...).
			Obj()
		claim, err := tCtx.Client().ResourceV1().ResourceClaims(namespace).Create(tCtx, claimPrioritizedList, metav1.CreateOptions{})
		tCtx.ExpectNoError(err, "create claim "+claimName)

		_ = createPod(tCtx, namespace, "-pl-single-claim", podWithClaimName, claim)
		expectedSelectedRequest := fmt.Sprintf("%s/%s", claim.Spec.Devices.Requests[0].Name, claim.Spec.Devices.Requests[0].FirstAvailable[0].Name)
		tCtx.Eventually(func(tCtx ktesting.TContext) (*resourceapi.ResourceClaim, error) {
			return tCtx.Client().ResourceV1().ResourceClaims(namespace).Get(tCtx, claim.Name, metav1.GetOptions{})
		}).WithTimeout(schedulingTimeout).WithPolling(time.Second).Should(expectedAllocatedClaim(expectedSelectedRequest, nodeInfos[0]))
	})

	runSubTest(tCtx, "multi-claim", func(tCtx ktesting.TContext) {
		// 构造两个 claim，使 nodeInfos[2] 得分最高：
		// nodeInfos[1]：在 claim1 中排第 1，在 claim2 中排第 3，得分为 14
		// nodeInfos[2]：在 claim1 中排第 2，在 claim2 中排第 1，得分为 15
		// nodeInfos[3]：在 claim1 中排第 3，在 claim2 中排第 2，得分为 13
		claimPrioritizedList1 := st.MakeResourceClaim().
			Name(claimName + "-pl-multiclaim-1").
			Namespace(namespace).
			RequestWithPrioritizedList([]resourceapi.DeviceSubRequest{
				{
					Name:            "subreq-0",
					DeviceClassName: nodeInfos[1].class.Name,
				},
				{
					Name:            "subreq-1",
					DeviceClassName: nodeInfos[2].class.Name,
				},
				{
					Name:            "subreq-2",
					DeviceClassName: nodeInfos[3].class.Name,
				},
			}...).
			Obj()
		claim1, err := tCtx.Client().ResourceV1().ResourceClaims(namespace).Create(tCtx, claimPrioritizedList1, metav1.CreateOptions{})
		tCtx.ExpectNoError(err, "create claim "+claimName)

		claimPrioritizedList2 := st.MakeResourceClaim().
			Name(claimName + "-pl-multiclaim-2").
			Namespace(namespace).
			RequestWithPrioritizedList([]resourceapi.DeviceSubRequest{
				{
					Name:            "subreq-0",
					DeviceClassName: nodeInfos[2].class.Name,
				},
				{
					Name:            "subreq-1",
					DeviceClassName: nodeInfos[3].class.Name,
				},
				{
					Name:            "subreq-2",
					DeviceClassName: nodeInfos[1].class.Name,
				},
			}...).
			Obj()
		claim2, err := tCtx.Client().ResourceV1().ResourceClaims(namespace).Create(tCtx, claimPrioritizedList2, metav1.CreateOptions{})
		tCtx.ExpectNoError(err, "create claim "+claimName)

		pod := st.MakePod().Name(podName).Namespace(namespace).
			Container("my-container").
			Obj()
		_ = createPod(tCtx, namespace, "-pl-multiclaim", pod, claim1, claim2)

		// claim1 的第二个子请求对应 nodeInfos[2]，应被选中。
		expectedSelectedRequest := fmt.Sprintf("%s/%s", claim1.Spec.Devices.Requests[0].Name, claim1.Spec.Devices.Requests[0].FirstAvailable[1].Name)
		tCtx.Eventually(func(tCtx ktesting.TContext) (*resourceapi.ResourceClaim, error) {
			return tCtx.Client().ResourceV1().ResourceClaims(namespace).Get(tCtx, claimPrioritizedList1.Name, metav1.GetOptions{})
		}).WithTimeout(schedulingTimeout).WithPolling(time.Second).Should(expectedAllocatedClaim(expectedSelectedRequest, nodeInfos[2]))

		// claim2 的第一个子请求对应 nodeInfos[2]，应被选中。
		expectedSelectedRequest = fmt.Sprintf("%s/%s", claim2.Spec.Devices.Requests[0].Name, claim2.Spec.Devices.Requests[0].FirstAvailable[0].Name)
		tCtx.Eventually(func(tCtx ktesting.TContext) (*resourceapi.ResourceClaim, error) {
			return tCtx.Client().ResourceV1().ResourceClaims(namespace).Get(tCtx, claimPrioritizedList2.Name, metav1.GetOptions{})
		}).WithTimeout(schedulingTimeout).WithPolling(time.Second).Should(expectedAllocatedClaim(expectedSelectedRequest, nodeInfos[2]))
	})
}
