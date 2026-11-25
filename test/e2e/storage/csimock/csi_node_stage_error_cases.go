/*
Copyright 2022 The Kubernetes Authors.

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

package csimock

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/onsi/ginkgo/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2epv "k8s.io/kubernetes/test/e2e/framework/pv"
	"k8s.io/kubernetes/test/e2e/storage/drivers"
	"k8s.io/kubernetes/test/e2e/storage/utils"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = utils.SIGDescribe("CSI Mock volume node stage", func() {
	f := framework.NewDefaultFramework("csi-mock-volumes-node-stage")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	m := newMockDriverSetup(f)

	f.Context("CSI NodeStage error cases", f.WithSlow(), func() {
		trackedCalls := []string{
			"NodeStageVolume",
			"NodeUnstageVolume",
		}

		tests := []struct {
			name             string
			expectPodRunning bool
			expectedCalls    []csiCall

			// 每次调用 NodeStageVolume 时触发，调用前计数器已原子递增（即首次值为 1）。
			nodeStageHook func(counter int64) error
		}{
			{
				// 别处已覆盖该场景，这里加一个简单正常用例以验证测试框架本身。
				name:             "should call NodeUnstage after NodeStage success",
				expectPodRunning: true,
				expectedCalls: []csiCall{
					{expectedMethod: "NodeStageVolume", expectedError: codes.OK, deletePod: true},
					{expectedMethod: "NodeUnstageVolume", expectedError: codes.OK},
				},
			},
			{
				// 只要 Pod 存在，kubelet 就应重复调用 NodeStage
				name:             "should retry NodeStage after NodeStage final error",
				expectPodRunning: true,
				expectedCalls: []csiCall{
					// 匹配全部 3 次返回 InvalidArgument 错误的 NodeStage 调用
					{expectedMethod: "NodeStageVolume", expectedError: codes.InvalidArgument},
					{expectedMethod: "NodeStageVolume", expectedError: codes.OK, deletePod: true},
					{expectedMethod: "NodeUnstageVolume", expectedError: codes.OK},
				},
				// Fail first 3 NodeStage requests, 4th succeeds
				nodeStageHook: func(counter int64) error {
					if counter < 4 {
						return status.Error(codes.InvalidArgument, "fake error")
					}
					return nil
				},
			},
			{
				// 只要 Pod 存在，kubelet 就应重复调用 NodeStage
				name:             "should retry NodeStage after NodeStage ephemeral error",
				expectPodRunning: true,
				expectedCalls: []csiCall{
					// 匹配全部 3 次返回 DeadlineExceeded 错误的 NodeStage 调用
					{expectedMethod: "NodeStageVolume", expectedError: codes.DeadlineExceeded},
					{expectedMethod: "NodeStageVolume", expectedError: codes.OK, deletePod: true},
					{expectedMethod: "NodeUnstageVolume", expectedError: codes.OK},
				},
				// Fail first 3 NodeStage requests, 4th succeeds
				nodeStageHook: func(counter int64) error {
					if counter < 4 {
						return status.Error(codes.DeadlineExceeded, "fake error")
					}
					return nil
				},
			},
			{
				// NodeUnstage 返回临时错误后，驱动可能仍在继续暂存卷；
				// Pod 删除后 kubelet 应再次调用 NodeUnstage，确保卷确实已取消暂存。
				name:             "should call NodeUnstage after NodeStage ephemeral error",
				expectPodRunning: false,
				expectedCalls: []csiCall{
					// 在 NodeStage 成功前删除 Pod —— 因临时错误，结果应为 "uncertain"。
					// 匹配所有因指数退避而重复、返回 DeadlineExceeded 的 NodeStage 调用。
					{expectedMethod: "NodeStageVolume", expectedError: codes.DeadlineExceeded, deletePod: true},
					{expectedMethod: "NodeUnstageVolume", expectedError: codes.OK},
				},
				nodeStageHook: func(counter int64) error {
					return status.Error(codes.DeadlineExceeded, "fake error")
				},
			},
			{
				// NodeUnstage 返回最终错误后，kubelet 可确定卷未被暂存；
				// 测试校验 NodeUnstage 不会被调用。
				name:             "should not call NodeUnstage after NodeStage final error",
				expectPodRunning: false,
				expectedCalls: []csiCall{
					// 在 NodeStage 成功前删除 Pod —— 因最终错误，结果应为 "globally unmounted"。
					// 匹配所有因指数退避而重复、返回 InvalidArgument 的 NodeStage 调用。
					{expectedMethod: "NodeStageVolume", expectedError: codes.InvalidArgument, deletePod: true},
				},
				// nodeStageScript: `INVALIDARGUMENT;`,
				nodeStageHook: func(counter int64) error {
					return status.Error(codes.InvalidArgument, "fake error")
				},
			},
		}
		for _, test := range tests {
			ginkgo.It(test.name, func(ctx context.Context) {
				var hooks *drivers.Hooks
				if test.nodeStageHook != nil {
					hooks = createPreHook("NodeStageVolume", test.nodeStageHook)
				}
				m.init(ctx, testParameters{
					disableAttach:  true,
					registerDriver: true,
					hooks:          hooks,
				})
				ginkgo.DeferCleanup(m.cleanup)

				_, claim, pod := m.createPod(ctx, pvcReference)
				if pod == nil {
					return
				}
				// 等待 PVC 绑定，确保 CSI 驱动已完全启动。
				err := e2epv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, f.ClientSet, f.Namespace.Name, claim.Name, time.Second, framework.ClaimProvisionTimeout)
				framework.ExpectNoError(err, "while waiting for PVC to get provisioned")

				ginkgo.By("Waiting for expected CSI calls")
				// 监听 deletePod = true 之前的所有调用
				timeoutCtx, cancel := context.WithTimeout(ctx, csiPodRunningTimeout)
				defer cancel()
				for {
					if timeoutCtx.Err() != nil {
						framework.Failf("timed out waiting for the CSI call that indicates that the pod can be deleted: %v", test.expectedCalls)
					}
					time.Sleep(1 * time.Second)
					_, index, err := compareCSICalls(timeoutCtx, trackedCalls, test.expectedCalls, m.driver.GetCalls)
					framework.ExpectNoError(err, "while waiting for initial CSI calls")
					if index == 0 {
						// No CSI call received yet
						continue
					}
					// 检查最后*收到*的调用是否要求删除 Pod
					if test.expectedCalls[index-1].deletePod {
						break
					}
				}

				if test.expectPodRunning {
					ginkgo.By("Waiting for pod to be running")
					err := e2epod.WaitForPodNameRunningInNamespace(ctx, m.cs, pod.Name, pod.Namespace)
					framework.ExpectNoError(err, "Failed to start pod: %v", err)
				}

				ginkgo.By("Deleting the previously created pod")
				err = e2epod.DeletePodWithWait(ctx, m.cs, pod)
				framework.ExpectNoError(err, "while deleting")

				ginkgo.By("Waiting for all remaining expected CSI calls")
				err = wait.Poll(time.Second, csiUnstageWaitTimeout, func() (done bool, err error) {
					_, index, err := compareCSICalls(ctx, trackedCalls, test.expectedCalls, m.driver.GetCalls)
					if err != nil {
						return true, err
					}
					if index == 0 {
						// No CSI call received yet
						return false, nil
					}
					if len(test.expectedCalls) == index {
						// all calls received
						return true, nil
					}
					return false, nil
				})
				framework.ExpectNoError(err, "while waiting for all CSI calls")
			})
		}
	})

	f.Context("CSI NodeUnstage error cases", f.WithSlow(), func() {
		trackedCalls := []string{
			"NodeStageVolume",
			"NodeUnstageVolume",
		}

		// 每个测试依次启动两个 Pod。第一个 Pod 总是成功运行，但 NodeUnstage hook 可注入各种错误；
		// 随后测试检查第二个 Pod 的 NodeStage 是如何被调用的。
		tests := []struct {
			name          string
			expectedCalls []csiCall

			// 每次 NodeStageVolume 调用时触发；计数器在调用前原子递增（即首个值为 1），
			// 参数还包括被删除 Pod（第一个 Pod）的索引
			// has index 1)
			nodeUnstageHook func(counter, pod int64) error
		}{
			{
				// 别处已覆盖该场景，这里加一个简单正常用例以验证测试框架本身。
				name: "should call NodeStage after NodeUnstage success",
				expectedCalls: []csiCall{
					{expectedMethod: "NodeStageVolume", expectedError: codes.OK},
					{expectedMethod: "NodeUnstageVolume", expectedError: codes.OK},
					{expectedMethod: "NodeStageVolume", expectedError: codes.OK},
					{expectedMethod: "NodeUnstageVolume", expectedError: codes.OK},
				},
			},
			{
				name: "two pods: should call NodeStage after previous NodeUnstage final error",
				expectedCalls: []csiCall{
					{expectedMethod: "NodeStageVolume", expectedError: codes.OK},
					{expectedMethod: "NodeUnstageVolume", expectedError: codes.InvalidArgument},
					{expectedMethod: "NodeStageVolume", expectedError: codes.OK},
					{expectedMethod: "NodeUnstageVolume", expectedError: codes.OK},
				},
				nodeUnstageHook: func(counter, pod int64) error {
					if pod == 1 {
						return status.Error(codes.InvalidArgument, "fake final error")
					}
					return nil
				},
			},
			{
				name: "two pods: should call NodeStage after previous NodeUnstage transient error",
				expectedCalls: []csiCall{
					{expectedMethod: "NodeStageVolume", expectedError: codes.OK},
					{expectedMethod: "NodeUnstageVolume", expectedError: codes.DeadlineExceeded},
					{expectedMethod: "NodeStageVolume", expectedError: codes.OK},
					{expectedMethod: "NodeUnstageVolume", expectedError: codes.OK},
				},
				nodeUnstageHook: func(counter, pod int64) error {
					if pod == 1 {
						return status.Error(codes.DeadlineExceeded, "fake transient error")
					}
					return nil
				},
			},
		}
		for _, test := range tests {
			ginkgo.It(test.name, func(ctx context.Context) {
				// 最近删除的 Pod 的索引；后续的 NodeUnstage 调用都与它相关。
				var deletedPodNumber atomic.Int64
				deletedPodNumber.Store(1)
				var hooks *drivers.Hooks
				if test.nodeUnstageHook != nil {
					hooks = createPreHook("NodeUnstageVolume", func(counter int64) error {
						pod := deletedPodNumber.Load()
						return test.nodeUnstageHook(counter, pod)
					})
				}
				m.init(ctx, testParameters{
					disableAttach:  true,
					registerDriver: true,
					hooks:          hooks,
				})
				ginkgo.DeferCleanup(m.cleanup)

				_, claim, pod := m.createPod(ctx, pvcReference)
				if pod == nil {
					return
				}
				// 等待 PVC 绑定，确保 CSI 驱动已完全启动。
				err := e2epv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, f.ClientSet, f.Namespace.Name, claim.Name, time.Second, framework.ClaimProvisionTimeout)
				framework.ExpectNoError(err, "while waiting for PVC to get provisioned")
				err = e2epod.WaitForPodNameRunningInNamespace(ctx, m.cs, pod.Name, pod.Namespace)
				framework.ExpectNoError(err, "while waiting for the first pod to start")
				err = e2epod.DeletePodWithWait(ctx, m.cs, pod)
				framework.ExpectNoError(err, "while deleting the first pod")

				// 创建第二个 Pod
				pod, err = m.createPodWithPVC(claim)
				framework.ExpectNoError(err, "while creating the second pod")
				err = e2epod.WaitForPodNameRunningInNamespace(ctx, m.cs, pod.Name, pod.Namespace)
				framework.ExpectNoError(err, "while waiting for the second pod to start")
				// 第二个 Pod 正在运行，kubelet 无法再调用第一个 Pod 的 NodeUnstage，
				// 因此此处递增 Pod 计数器是安全的。
				deletedPodNumber.Add(1)
				err = e2epod.DeletePodWithWait(ctx, m.cs, pod)
				framework.ExpectNoError(err, "while deleting the second pod")

				ginkgo.By("Waiting for all remaining expected CSI calls")
				err = wait.Poll(time.Second, csiUnstageWaitTimeout, func() (done bool, err error) {
					_, index, err := compareCSICalls(ctx, trackedCalls, test.expectedCalls, m.driver.GetCalls)
					if err != nil {
						return true, err
					}
					if index == 0 {
						// No CSI call received yet
						return false, nil
					}
					if len(test.expectedCalls) == index {
						// all calls received
						return true, nil
					}
					return false, nil
				})
				framework.ExpectNoError(err, "while waiting for all CSI calls")
			})
		}
	})

})
