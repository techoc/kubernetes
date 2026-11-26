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
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	cachetools "k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	"k8s.io/kubernetes/test/e2e/storage/drivers"
	"k8s.io/kubernetes/test/e2e/storage/utils"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = utils.SIGDescribe("CSI Mock volume storage capacity", func() {
	f := framework.NewDefaultFramework("csi-mock-volumes-capacity")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	m := newMockDriverSetup(f)

	ginkgo.Context("storage capacity", func() {
		tests := []struct {
			name              string
			resourceExhausted bool
			lateBinding       bool
			topology          bool
		}{
			{
				name: "unlimited",
			},
			{
				name:              "exhausted, immediate binding",
				resourceExhausted: true,
			},
			{
				name:              "exhausted, late binding, no topology",
				resourceExhausted: true,
				lateBinding:       true,
			},
			{
				name:              "exhausted, late binding, with topology",
				resourceExhausted: true,
				lateBinding:       true,
				topology:          true,
			},
		}

		createVolume := "CreateVolume"
		deleteVolume := "DeleteVolume"
		// publishVolume := "NodePublishVolume"
		// unpublishVolume := "NodeUnpublishVolume"
		// stageVolume := "NodeStageVolume"
		// unstageVolume := "NodeUnstageVolume"

		// 每次测试运行都假定这些调用按此顺序发生。NodeStageVolume 和
		// NodePublishVolume 本应确定性地只调用一次，但有时 kubelet 会
		// 多次调用两者，导致本测试失败
		//（https://github.com/kubernetes/kubernetes/issues/90250）。
		// 因此在问题 resolved 前暂时把它们注释掉。
		//
		//
		//
		//
		// NodeUnpublishVolume 和 NodeUnstageVolume 与 DeleteVolume 存在竞态，
		// 无法假定顺序确定，只能忽略它们
		//（https://github.com/kubernetes/kubernetes/issues/94108）。
		//
		deterministicCalls := []string{
			createVolume,
			// stageVolume,
			// publishVolume,
			// unpublishVolume,
			// unstageVolume,
			deleteVolume,
		}

		for _, t := range tests {
			test := t
			ginkgo.It(test.name, ginkgo.NodeTimeout(csiPodRunningTimeout), func(ctx context.Context) {
				var err error
				params := testParameters{
					lateBinding:    test.lateBinding,
					enableTopology: test.topology,

					// 并非必需，但这样会稍快一些，而且有一段时间
					// 曾因 bug 导致两分钟延迟
					//（https://github.com/kubernetes-csi/csi-test/pull/250）。
					disableAttach:  true,
					registerDriver: true,
				}

				var nodeAnnotationSet atomic.Bool
				var nodeAnnotationReset atomic.Bool

				if test.resourceExhausted {
					params.hooks = createPreHook("CreateVolume", func(counter int64) error {
						if test.topology && test.lateBinding && !nodeAnnotationReset.Load() {
							return status.Error(codes.ResourceExhausted, "fake error")
						}
						if counter == 1 {
							return status.Error(codes.ResourceExhausted, "fake error")
						}
						return nil
					})
				}

				m.init(ctx, params)
				ginkgo.DeferCleanup(m.cleanup)

				// 与原始 watch 不同，RetryWatcher 预期能交付所有事件，
				// 即使底层原始 watch 提前关闭
				//（https://github.com/kubernetes/kubernetes/pull/93777#discussion_r467932080）。
				// 这一点很重要，因为下面的测试要对
				// PVC 状态变化做断言。
				initResource, err := f.ClientSet.CoreV1().PersistentVolumeClaims(f.Namespace.Name).List(ctx, metav1.ListOptions{})
				framework.ExpectNoError(err, "Failed to fetch initial PVC resource")
				listWatcher := &cachetools.ListWatch{
					WatchFunc: func(listOptions metav1.ListOptions) (watch.Interface, error) {
						return f.ClientSet.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Watch(ctx, listOptions)
					},
				}
				pvcWatch, err := watchtools.NewRetryWatcher(initResource.GetResourceVersion(), listWatcher)
				framework.ExpectNoError(err, "create PVC watch")
				defer pvcWatch.Stop()

				var watchFailed atomic.Bool
				watchDone := make(chan struct{})
				go func() {
					defer close(watchDone)
					for {
						select {
						case event, ok := <-pvcWatch.ResultChan():
							if !ok {
								watchFailed.Store(true)
								return
							}

							framework.Logf("PVC event %s: %#v", event.Type, event.Object)
							switch event.Type {
							case watch.Modified:
								pvc, ok := event.Object.(*v1.PersistentVolumeClaim)
								if !ok {
									continue
								}
								_, set := pvc.Annotations["volume.kubernetes.io/selected-node"]
								if set {
									nodeAnnotationSet.Store(true)
								} else if nodeAnnotationSet.Load() {
									nodeAnnotationReset.Store(true)
								}
							case watch.Deleted:
								return
							case watch.Error:
								watchFailed.Store(true)
								return
							}
						case <-ctx.Done():
							return
						}
					}
				}()

				sc, claim, pod := m.createPod(ctx, pvcReference)
				gomega.Expect(pod).NotTo(gomega.BeNil(), "while creating pod")
				bindingMode := storagev1.VolumeBindingImmediate
				if test.lateBinding {
					bindingMode = storagev1.VolumeBindingWaitForFirstConsumer
				}
				gomega.Expect(*sc.VolumeBindingMode).To(gomega.Equal(bindingMode), "volume binding mode")

				err = e2epod.WaitForPodNameRunningInNamespace(ctx, m.cs, pod.Name, pod.Namespace)
				framework.ExpectNoError(err, "failed to start pod")
				err = e2epod.DeletePodWithWait(ctx, m.cs, pod)
				framework.ExpectNoError(err, "failed to delete pod")
				err = m.cs.CoreV1().PersistentVolumeClaims(claim.Namespace).Delete(ctx, claim.Name, metav1.DeleteOptions{})
				framework.ExpectNoError(err, "failed to delete claim")

				normal := []csiCall{}
				for _, method := range deterministicCalls {
					normal = append(normal, csiCall{expectedMethod: method})
				}
				expected := normal
				// 模拟容量受限时，预期恰好调用两次 CreateVolume，
				// 因为第一次调用应该
				// 失败。
				//
				if test.resourceExhausted {
					expected = []csiCall{
						{expectedMethod: createVolume, expectedError: codes.ResourceExhausted},
					}
					expected = append(expected, normal...)
				}

				var calls []drivers.MockCSICall
				err = wait.PollUntilContextCancel(ctx, time.Second, true, func(ctx context.Context) (done bool, err error) {
					c, index, err := compareCSICalls(ctx, deterministicCalls, expected, m.driver.GetCalls)
					if err != nil {
						return true, fmt.Errorf("error waiting for expected CSI calls: %w", err)
					}
					calls = c
					if index == 0 {
						// 尚未收到 CSI 调用
						return false, nil
					}
					if len(expected) == index {
						// 已收到所有调用
						return true, nil
					}
					return false, nil
				})
				framework.ExpectNoError(err, "while waiting for all CSI calls")

				// 容量错误有两种处理方式。
				//
				// 延迟绑定时，external-provisioner 应清除节点 annotation，
				// 让 scheduler 有机会把 Pod 重新调度到其他
				// 节点。
				//
				// 立即绑定时，external-scheduler 必须持续重试。
				//
				// 遗憾的是两种情况下调用日志相同。必须收集
				// 额外证据证明确实发生了重调度。上面观察到的
				// 是 PVC 随时间的变化，现在可以分析了。
				ginkgo.By("Checking PVC events")
				<-watchDone

				// 容量受限时的更多测试。
				if test.resourceExhausted {
					for _, call := range calls {
						if call.Method == createVolume {
							gomega.Expect(call.Error).To(gomega.ContainSubstring("code = ResourceExhausted"), "first CreateVolume error in\n%s", calls)
							break
						}
					}

					switch {
					case watchFailed.Load():
						// 如果 watch 失败或提前停止（随时可能发生），则无法
						// 校验 annotation 是否按预期设置。这种情况仍视为测试成功。
						//
						framework.Logf("PVC watch delivered incomplete data, cannot check annotation")
					case test.lateBinding:
						gomega.Expect(nodeAnnotationSet.Load()).To(gomega.BeTrueBecause("selected-node should have been set"))
						// 是否重置取决于是否启用 topology。未启用时
						// 无需重调度。
						if test.topology {
							gomega.Expect(nodeAnnotationReset.Load()).To(gomega.BeTrueBecause("selected-node should have been set"))
						} else {
							gomega.Expect(nodeAnnotationReset.Load()).To(gomega.BeFalseBecause("selected-node should not have been reset"))
						}
					default:
						gomega.Expect(nodeAnnotationSet.Load()).To(gomega.BeFalseBecause("selected-node should not have been set"))
						gomega.Expect(nodeAnnotationReset.Load()).To(gomega.BeFalseBecause("selected-node should not have been reset"))
					}
				}
			})
		}
	})

	// 这些测试*仅*适用于启用 CSIStorageCapacity 特性的集群。
	ginkgo.Context("CSIStorageCapacity", func() {
		var (
			err error
			yes = true
			no  = false
		)
		// 预期失败的测试很慢，因为要等一段时间
		// 才能确认卷没有被创建。
		tests := []struct {
			name            string
			storageCapacity *bool
			capacities      []string
			expectFailure   bool
		}{
			{
				name: "CSIStorageCapacity unused",
			},
			{
				name:            "CSIStorageCapacity disabled",
				storageCapacity: &no,
			},
			{
				name:            "CSIStorageCapacity used, no capacity",
				storageCapacity: &yes,
				expectFailure:   true,
			},
			{
				name:            "CSIStorageCapacity used, insufficient capacity",
				storageCapacity: &yes,
				expectFailure:   true,
				capacities:      []string{"1Mi"},
			},
			{
				name:            "CSIStorageCapacity used, have capacity",
				storageCapacity: &yes,
				capacities:      []string{"100Gi"},
			},
			// 这里可以针对各种情况补充更多测试用例，
			// 但通过 scheduler binder 单元测试覆盖它们更快。
			//
		}
		for _, t := range tests {
			test := t
			ginkgo.It(t.name, ginkgo.NodeTimeout(f.Timeouts.PodStart), func(ctx context.Context) {
				scName := "mock-csi-storage-capacity-" + f.UniqueName
				m.init(ctx, testParameters{
					registerDriver:  true,
					scName:          scName,
					storageCapacity: test.storageCapacity,
					lateBinding:     true,
				})
				ginkgo.DeferCleanup(m.cleanup)

				// storage class 使用随机名称，必须先创建它，
				// 再为它添加 CSIStorageCapacity 对象。
				for _, capacityStr := range test.capacities {
					capacityQuantity := resource.MustParse(capacityStr)
					capacity := &storagev1.CSIStorageCapacity{
						ObjectMeta: metav1.ObjectMeta{
							GenerateName: "fake-capacity-",
						},
						// 空 topology，任何节点都可使用。
						StorageClassName: scName,
						NodeTopology:     &metav1.LabelSelector{},
						Capacity:         &capacityQuantity,
					}
					createdCapacity, err := f.ClientSet.StorageV1().CSIStorageCapacities(f.Namespace.Name).Create(ctx, capacity, metav1.CreateOptions{})
					framework.ExpectNoError(err, "create CSIStorageCapacity %+v", *capacity)
					ginkgo.DeferCleanup(framework.IgnoreNotFound(f.ClientSet.StorageV1().CSIStorageCapacities(f.Namespace.Name).Delete), createdCapacity.Name, metav1.DeleteOptions{})
				}

				// kube-scheduler 可能需要一些时间才能拿到 CSIDriver 和 CSIStorageCapacity 对象。
				// 没有它们时，调度不会按测试预期进行。
				syncDelay := 5 * time.Second
				time.Sleep(syncDelay)

				sc, _, pod := m.createPod(ctx, pvcReference) // 按上面的说明使用延迟绑定
				gomega.Expect(sc.Name).To(gomega.Equal(scName), "pre-selected storage class name not used")

				condition := anyOf(
					podRunning(ctx, f.ClientSet, pod.Name, pod.Namespace),
					// CSIStorageCapacity 对象刚创建，因此
					// 必须忽略所有更早的事件，并加上 syncDelay 作为
					// 安全余量。
					podHasStorage(ctx, f.ClientSet, pod.Name, pod.Namespace, time.Now().Add(syncDelay)),
				)
				err = wait.PollImmediateUntil(poll, condition, ctx.Done())
				if test.expectFailure {
					switch {
					case errors.Is(err, context.DeadlineExceeded),
						errors.Is(err, wait.ErrorInterrupted(errors.New("timed out waiting for the condition"))),
						errors.Is(err, errNotEnoughSpace):
						// 符合预期。
					case err == nil:
						framework.Fail("pod unexpectedly started to run")
					default:
						framework.Failf("unexpected error while waiting for pod: %v", err)
					}
				} else {
					framework.ExpectNoError(err, "failed to start pod")
				}

				ginkgo.By("Deleting the previously created pod")
				err = e2epod.DeletePodWithWait(ctx, m.cs, pod)
				framework.ExpectNoError(err, "while deleting")
			})
		}
	})
})
