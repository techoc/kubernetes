/*
Copyright 2018 The Kubernetes Authors.

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

package node

import (
	"context"
	"fmt"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	admissionapi "k8s.io/pod-security-admission/api"

	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	testutils "k8s.io/kubernetes/test/utils"

	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = SIGDescribe("NodeLease", framework.WithNodeConformance(), func() {
	var nodeName string
	f := framework.NewDefaultFramework("node-lease-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.BeforeEach(func(ctx context.Context) {
		node, err := e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
		framework.ExpectNoError(err)
		nodeName = node.Name
	})

	ginkgo.Context("NodeLease", func() {
		ginkgo.It("the kubelet should create and update a lease in the kube-node-lease namespace", func(ctx context.Context) {
			leaseClient := f.ClientSet.CoordinationV1().Leases(v1.NamespaceNodeLease)
			var (
				err   error
				lease *coordinationv1.Lease
			)
			ginkgo.By("check that lease for this Kubelet exists in the kube-node-lease namespace")
			gomega.Eventually(ctx, func() error {
				lease, err = leaseClient.Get(ctx, nodeName, metav1.GetOptions{})
				if err != nil {
					return err
				}
				return nil
			}, 5*time.Minute, 5*time.Second).Should(gomega.BeNil())
			// 检查 lease 的基本预期
			framework.ExpectNoError(expectLease(lease, nodeName))

			ginkgo.By("check that node lease is updated at least once within the lease duration")
			gomega.Eventually(ctx, func() error {
				newLease, err := leaseClient.Get(ctx, nodeName, metav1.GetOptions{})
				if err != nil {
					return err
				}
				// 检查最新 lease 的基本预期
				if err := expectLease(newLease, nodeName); err != nil {
					return err
				}
				// 检查最新 lease 的 RenewTime 是否已更新
				newTime := (*newLease.Spec.RenewTime).Time
				oldTime := (*lease.Spec.RenewTime).Time
				if !newTime.After(oldTime) {
					return fmt.Errorf("new lease has time %v, which is not after old lease time %v", newTime, oldTime)
				}
				return nil
			}, time.Duration(*lease.Spec.LeaseDurationSeconds)*time.Second,
				time.Duration(*lease.Spec.LeaseDurationSeconds/4)*time.Second).Should(gomega.Succeed())
		})

		ginkgo.It("should have OwnerReferences set", func(ctx context.Context) {
			leaseClient := f.ClientSet.CoordinationV1().Leases(v1.NamespaceNodeLease)
			var (
				err       error
				leaseList *coordinationv1.LeaseList
			)
			gomega.Eventually(ctx, func() error {
				leaseList, err = leaseClient.List(ctx, metav1.ListOptions{})
				if err != nil {
					return err
				}
				return nil
			}, 5*time.Minute, 5*time.Second).Should(gomega.BeNil())
			// 所有 lease 的 OwnerReferences 都应指向各自对应的
			// Node 对象。
			for i := range leaseList.Items {
				lease := &leaseList.Items[i]
				ownerRefs := lease.ObjectMeta.OwnerReferences
				gomega.Expect(ownerRefs).To(gomega.HaveLen(1))
				gomega.Expect(ownerRefs[0].Kind).To(gomega.Equal(v1.SchemeGroupVersion.WithKind("Node").Kind))
				gomega.Expect(ownerRefs[0].APIVersion).To(gomega.Equal(v1.SchemeGroupVersion.WithKind("Node").Version))
			}
		})

		ginkgo.It("the kubelet should report node status infrequently", func(ctx context.Context) {
			ginkgo.By("wait until node is ready")
			e2enode.WaitForNodeToBeReady(ctx, f.ClientSet, nodeName, 5*time.Minute)

			ginkgo.By("wait until there is node lease")
			var err error
			var lease *coordinationv1.Lease
			gomega.Eventually(ctx, func() error {
				lease, err = f.ClientSet.CoordinationV1().Leases(v1.NamespaceNodeLease).Get(ctx, nodeName, metav1.GetOptions{})
				if err != nil {
					return err
				}
				return nil
			}, 5*time.Minute, 5*time.Second).Should(gomega.BeNil())
			// 检查 lease 的基本预期
			framework.ExpectNoError(expectLease(lease, nodeName))
			leaseDuration := time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second

			ginkgo.By("verify NodeStatus report period is longer than lease duration")
			// 节点状态在有变化或经过足够时间后才会从 node 上报给 master。
			// 因此这里持续检查两次 NodeStatus 上报之间的时间差，
			// 直到它超过 lease duration（与 nodeMonitorGracePeriod 相同），
			// 或至少在 leaseDuration 时间内没有变化。
			lastHeartbeatTime, lastStatus := getHeartbeatTimeAndStatus(ctx, f.ClientSet, nodeName)
			lastObserved := time.Now()
			err = wait.Poll(time.Second, 5*time.Minute, func() (bool, error) {
				currentHeartbeatTime, currentStatus := getHeartbeatTimeAndStatus(ctx, f.ClientSet, nodeName)
				currentObserved := time.Now()

				if currentHeartbeatTime == lastHeartbeatTime {
					if currentObserved.Sub(lastObserved) > 2*leaseDuration {
						// 观察至少 2*leaseDuration 心跳都没有变化，成功！
						framework.Logf("node status heartbeat is unchanged for %s, was waiting for at least %s, success!", currentObserved.Sub(lastObserved), 2*leaseDuration)
						return true, nil
					}
					framework.Logf("node status heartbeat is unchanged for %s, waiting for %s", currentObserved.Sub(lastObserved), 2*leaseDuration)
					return false, nil
				}

				if currentHeartbeatTime.Sub(lastHeartbeatTime) >= leaseDuration {
					// 心跳时间变了，但间隔大于 leaseDuration，成功！
					framework.Logf("node status heartbeat changed in %s, was waiting for at least %s, success!", currentHeartbeatTime.Sub(lastHeartbeatTime), leaseDuration)
					return true, nil
				}

				if !apiequality.Semantic.DeepEqual(lastStatus, currentStatus) {
					// 心跳时间变了，但状态中有相关变化，继续等待
					framework.Logf("node status heartbeat changed in %s (with other status changes), waiting for %s", currentHeartbeatTime.Sub(lastHeartbeatTime), leaseDuration)
					framework.Logf("%s", cmp.Diff(lastStatus, currentStatus))
					lastHeartbeatTime = currentHeartbeatTime
					lastObserved = currentObserved
					lastStatus = currentStatus
					return false, nil
				}

				// 心跳时间变了，且没有其他状态变化，但间隔短于预期，失败。
				return false, fmt.Errorf("node status heartbeat changed in %s (with no other status changes), was waiting for %s", currentHeartbeatTime.Sub(lastHeartbeatTime), leaseDuration)
			})
			// 超时是可接受的，因为这意味着等待了 5 分钟仍未出现异常的节点状态更新
			if !wait.Interrupted(err) {
				framework.ExpectNoError(err, "error waiting for infrequent nodestatus update")
			}

			ginkgo.By("verify node is still in ready status even though node status report is infrequent")
			// 对节点状态的这项检查仅在 e2e 测试作为集群 e2e 运行时有意义，
			// 因为 node e2e 不会创建并运行 controller manager，
			// 也就没有 node lifecycle controller。
			node, err := f.ClientSet.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
			framework.ExpectNoError(err)
			_, readyCondition := testutils.GetNodeCondition(&node.Status, v1.NodeReady)
			gomega.Expect(readyCondition.Status).To(gomega.Equal(v1.ConditionTrue))
		})
	})
})

func getHeartbeatTimeAndStatus(ctx context.Context, clientSet clientset.Interface, nodeName string) (time.Time, v1.NodeStatus) {
	node, err := clientSet.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	framework.ExpectNoError(err)
	_, readyCondition := testutils.GetNodeCondition(&node.Status, v1.NodeReady)
	gomega.Expect(readyCondition.Status).To(gomega.Equal(v1.ConditionTrue))
	heartbeatTime := readyCondition.LastHeartbeatTime.Time
	readyCondition.LastHeartbeatTime = metav1.Time{}
	return heartbeatTime, node.Status
}

func expectLease(lease *coordinationv1.Lease, nodeName string) error {
	// 预期 HolderIdentity、LeaseDurationSeconds 和 RenewTime 的值
	if lease.Spec.HolderIdentity == nil {
		return fmt.Errorf("Spec.HolderIdentity should not be nil")
	}
	if lease.Spec.LeaseDurationSeconds == nil {
		return fmt.Errorf("Spec.LeaseDurationSeconds should not be nil")
	}
	if lease.Spec.RenewTime == nil {
		return fmt.Errorf("Spec.RenewTime should not be nil")
	}
	// 确保 HolderIdentity 与节点名匹配
	if *lease.Spec.HolderIdentity != nodeName {
		return fmt.Errorf("Spec.HolderIdentity (%v) should match the node name (%v)", *lease.Spec.HolderIdentity, nodeName)
	}
	return nil
}
