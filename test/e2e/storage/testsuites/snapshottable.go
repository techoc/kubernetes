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

package testsuites

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/component-helpers/storage/ephemeral"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	e2epv "k8s.io/kubernetes/test/e2e/framework/pv"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	storageutils "k8s.io/kubernetes/test/e2e/storage/utils"
	admissionapi "k8s.io/pod-security-admission/api"
)

// data 文件名
const datapath = "/mnt/test/data"

type snapshottableTestSuite struct {
	tsInfo storageframework.TestSuiteInfo
}

var (
	sDriver storageframework.SnapshottableTestDriver
	dDriver storageframework.DynamicPVTestDriver
)

// InitCustomSnapshottableTestSuite 返回使用自定义测试模式、实现 TestSuite 接口的 snapshottableTestSuite

func InitCustomSnapshottableTestSuite(patterns []storageframework.TestPattern) storageframework.TestSuite {
	return &snapshottableTestSuite{
		tsInfo: storageframework.TestSuiteInfo{
			Name:         "snapshottable",
			TestPatterns: patterns,
			SupportedSizeRange: e2evolume.SizeRange{
				Min: "1Mi",
			},
			TestTags: []interface{}{feature.VolumeSnapshotDataSource},
		},
	}
}

// InitSnapshottableTestSuite 返回使用测试套件默认模式、实现 TestSuite 接口的 snapshottableTestSuite

func InitSnapshottableTestSuite() storageframework.TestSuite {
	patterns := []storageframework.TestPattern{
		storageframework.DynamicSnapshotDelete,
		storageframework.DynamicSnapshotRetain,
		storageframework.EphemeralSnapshotDelete,
		storageframework.EphemeralSnapshotRetain,
		storageframework.PreprovisionedSnapshotDelete,
		storageframework.PreprovisionedSnapshotRetain,
	}
	return InitCustomSnapshottableTestSuite(patterns)
}

func (s *snapshottableTestSuite) GetTestSuiteInfo() storageframework.TestSuiteInfo {
	return s.tsInfo
}

func (s *snapshottableTestSuite) SkipUnsupportedTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) string {
	// 检查前置条件。
	dInfo := driver.GetDriverInfo()
	ok := false
	_, ok = driver.(storageframework.SnapshottableTestDriver)
	if !dInfo.Capabilities[storageframework.CapSnapshotDataSource] || !ok {
		return fmt.Sprintf("Driver %q does not support snapshots", dInfo.Name)
	}
	_, ok = driver.(storageframework.DynamicPVTestDriver)
	if !ok {
		return fmt.Sprintf("Driver %q does not support dynamic provisioning", driver.GetDriverInfo().Name)
	}
	return ""
}

func (s *snapshottableTestSuite) DefineTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) {

	// 注意它还会注册一个 AfterEach，使 f 不可用。任何使用
	// f 的代码都必须在 It 或 Context 回调中运行。
	f := framework.NewFrameworkWithCustomTimeouts("snapshotting", storageframework.GetDriverTimeouts(driver))
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.Describe("volume snapshot controller", func() {
		var (
			err    error
			config *storageframework.PerTestConfig

			cs                  clientset.Interface
			dc                  dynamic.Interface
			pvc                 *v1.PersistentVolumeClaim
			sc                  *storagev1.StorageClass
			volumeResource      *storageframework.VolumeResource
			pod                 *v1.Pod
			claimSize           string
			originalMntTestData string
		)
		init := func(ctx context.Context) {
			sDriver, _ = driver.(storageframework.SnapshottableTestDriver)
			dDriver, _ = driver.(storageframework.DynamicPVTestDriver)
			// 初始化 snap class，创建源 PV、PVC、Pod
			cs = f.ClientSet
			dc = f.DynamicClient

			// 现在做开销更大的测试初始化。
			config = driver.PrepareTest(ctx, f)

			volumeResource = storageframework.CreateVolumeResource(ctx, dDriver, config, pattern, s.GetTestSuiteInfo().SupportedSizeRange)
			ginkgo.DeferCleanup(volumeResource.CleanupResource)

			ginkgo.By("[init] starting a pod to use the claim")
			originalMntTestData = fmt.Sprintf("hello from %s namespace", f.Namespace.Name)
			// 向文件写入数据后，`sync` 会把数据从内存刷到磁盘。
			// Linux 和 Windows 版 agnhost 都提供 sync。
			command := fmt.Sprintf("echo '%s' > %s; sync", originalMntTestData, datapath)

			pod = StartInPodWithVolumeSource(ctx, cs, *volumeResource.VolSource, f.Namespace.Name, "pvc-snapshottable-tester", command, config.ClientNodeSelection)

			// 此时已创建带 PVC 的 Pod，下一步取决于正在运行哪个测试。
		}

		ginkgo.Context("", func() {
			ginkgo.It("should check snapshot fields, check restore correctly works, check deletion (ephemeral)", func(ctx context.Context) {
				if pattern.VolType != storageframework.GenericEphemeralVolume {
					e2eskipper.Skipf("volume type %q is not ephemeral", pattern.VolType)
				}
				init(ctx)

				// 测试结束时删除 Pod
				ginkgo.DeferCleanup(e2epod.DeletePodWithWait, cs, pod)

				// 可以通过在 Pod 运行时（在线）创建快照来测试 generic
				// ephemeral volume 的快照。Pod 删除后无法这样做，
				// 因为那时 PVC 也会被标记，快照不再可用
				//（即使 finalizer 阻止 PVC 被真正删除）。
				//
				//
				// 因为无法保证数据一致性，这种测试变体
				// 不检查快照内容。
				//

				framework.ExpectNoError(e2epod.WaitForPodSuccessInNamespaceTimeout(ctx, cs, pod.Name, pod.Namespace, f.Timeouts.PodStartSlow))
				pod, err = cs.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
				framework.ExpectNoError(err, "check pod after it terminated")

				// 获取 claim 的新副本
				ginkgo.By("[init] checking the claim")
				pvcName := ephemeral.VolumeClaimName(pod, &pod.Spec.Volumes[0])
				pvcNamespace := pod.Namespace

				parameters := map[string]string{}
				sr := storageframework.CreateSnapshotResource(ctx, sDriver, config, pattern, pvcName, pvcNamespace, f.Timeouts, parameters)
				ginkgo.DeferCleanup(sr.CleanupResource, f.Timeouts)

				err = e2epv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, cs, pvcNamespace, pvcName, framework.Poll, f.Timeouts.ClaimProvision)
				framework.ExpectNoError(err)

				pvc, err = cs.CoreV1().PersistentVolumeClaims(pvcNamespace).Get(ctx, pvcName, metav1.GetOptions{})
				framework.ExpectNoError(err, "get PVC")
				claimSize = pvc.Spec.Resources.Requests.Storage().String()
				sc = volumeResource.Sc

				// 获取绑定的 PV
				ginkgo.By("[init] checking the PV")
				_, err := cs.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
				framework.ExpectNoError(err)

				vs := sr.Vs
				// 获取快照并检查 SnapshotContent 属性
				vscontent := checkSnapshot(ctx, dc, sr, pattern)

				var restoredPVC *v1.PersistentVolumeClaim
				var restoredPod *v1.Pod

				ginkgo.By("creating a pvc from the snapshot")
				restoredPVC = e2epv.MakePersistentVolumeClaim(e2epv.PersistentVolumeClaimConfig{
					ClaimSize:        claimSize,
					StorageClassName: &(sc.Name),
				}, config.Framework.Namespace.Name)

				group := "snapshot.storage.k8s.io"

				restoredPVC.Spec.DataSource = &v1.TypedLocalObjectReference{
					APIGroup: &group,
					Kind:     "VolumeSnapshot",
					Name:     vs.GetName(),
				}

				ginkgo.By("starting a pod to use the snapshot")
				volSrc := v1.VolumeSource{
					Ephemeral: &v1.EphemeralVolumeSource{
						VolumeClaimTemplate: &v1.PersistentVolumeClaimTemplate{
							Spec: restoredPVC.Spec,
						},
					},
				}

				restoredPod = StartInPodWithVolumeSource(ctx, cs, volSrc, restoredPVC.Namespace, "restored-pvc-tester", "sleep 300", config.ClientNodeSelection)
				ginkgo.DeferCleanup(e2epod.DeletePodWithWait, cs, restoredPod)

				framework.ExpectNoError(e2epod.WaitTimeoutForPodRunningInNamespace(ctx, cs, restoredPod.Name, restoredPod.Namespace, f.Timeouts.PodStartSlow))
				if pattern.VolType != storageframework.GenericEphemeralVolume {
					commands := e2evolume.GenerateReadFileCmd(datapath)
					_, err = e2eoutput.LookForStringInPodExec(restoredPod.Namespace, restoredPod.Name, commands, originalMntTestData, time.Minute)
					framework.ExpectNoError(err)
				}

				ginkgo.By("should delete the VolumeSnapshotContent according to its deletion policy")
				// 同时删除 Snapshot 和恢复出的 Pod/PVC，因为不同存储系统
				// 的删除顺序不同。有的要求先删恢复出的 PVC 再删 Snapshot，
				// 有的正好相反。
				err = storageutils.DeleteSnapshotWithoutWaiting(ctx, dc, vs.GetNamespace(), vs.GetName())
				framework.ExpectNoError(err)
				framework.Logf("deleting restored pod %q/%q", restoredPod.Namespace, restoredPod.Name)
				err = cs.CoreV1().Pods(restoredPod.Namespace).Delete(context.TODO(), restoredPod.Name, metav1.DeleteOptions{})
				framework.ExpectNoError(err)
				deleteVolumeSnapshot(ctx, f, dc, sr, pattern, vscontent)
			})

			ginkgo.It("should check snapshot fields, check restore correctly works after modifying source data, check deletion (persistent)", func(ctx context.Context) {
				if pattern.VolType == storageframework.GenericEphemeralVolume {
					e2eskipper.Skipf("volume type %q is ephemeral", pattern.VolType)
				}
				init(ctx)

				pvc = volumeResource.Pvc
				sc = volumeResource.Sc

				// Pod 应处于 Success 状态。
				ginkgo.By("[init] check pod success")
				pod, err = cs.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
				framework.ExpectNoError(err, "Failed to fetch pod: %v", err)
				framework.ExpectNoError(e2epod.WaitForPodSuccessInNamespaceTimeout(ctx, cs, pod.Name, pod.Namespace, f.Timeouts.PodStartSlow))
				// 同步 Pod 以获取额外字段。
				pod, err = cs.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
				framework.ExpectNoError(err, "Failed to fetch pod: %v", err)

				ginkgo.By("[init] checking the claim")
				err = e2epv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, cs, pvc.Namespace, pvc.Name, framework.Poll, f.Timeouts.ClaimProvision)
				framework.ExpectNoError(err)
				// 获取 claim 的新副本。
				pvc, err = cs.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
				framework.ExpectNoError(err)

				// 获取绑定的 PV。
				ginkgo.By("[init] checking the PV")
				pv, err := cs.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
				framework.ExpectNoError(err)

				// 删除 Pod 以强制 NodeUnpublishVolume（不同于 ephemeral 场景在测试结束时才删 Pod）。
				ginkgo.By("[init] deleting the pod")
				StopPod(ctx, cs, pod)

				// 此时我们知道：
				// - 创建了一个带 PV 的 Pod，该 PV 本应有数据
				//
				// 但 @jinxu97 解释过存在缓存问题，与 Pod 和卷
				// 生命周期相关。要理解它，先分析 volumemanager 的行为：
				// - 删除 Pod 时 volumemanager 会尝试清理卷挂载
				//   - NodeUnpublishVolume：解除容器上的 bind mount
				//     - Linux：移除 bind mount，不会刷任何缓存
				//     - Windows：删除 symlink，数据还没刷到磁盘
				//   - NodeUnstageVolume：卸载 global mount
				//     - Linux：卸载磁盘并刷所有缓存。
				//     - Windows：数据刷到磁盘并分离磁盘
				//
				// 删除 Pod 不保证数据刷盘，但 NodeUnstageVolume 增加了
				// 刷盘逻辑（详见 #81690）。必须等待 NodeUnstageVolume，因为
				// NodeUnpublishVolume 只移除 bind mount，不会强制刷缓存。
				// 不等待的话可能创建出空快照（详见 #101279）。
				//
				// 下面的代码通过检查 PV 不在 node.Status.VolumesInUse 字段中，
				// 确保节点不再使用该卷（表明 NodeUnstageVolume 已经
				// 完成）。
				nodeName := pod.Spec.NodeName
				gomega.Expect(nodeName).NotTo(gomega.BeEmpty(), "pod.Spec.NodeName must not be empty")

				// 快照测试只对 CSI driver 执行。CSI driver
				// 挂载到节点时使用 VolumeHandle 而不是 pv.Name。
				volumeName := pv.Spec.PersistentVolumeSource.CSI.VolumeHandle

				ginkgo.By(fmt.Sprintf("[init] waiting until the node=%s is not using the volume=%s", nodeName, volumeName))
				success := storageutils.WaitUntil(framework.Poll, f.Timeouts.PVDelete, func() bool {
					node, err := cs.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
					framework.ExpectNoError(err)
					volumesInUse := node.Status.VolumesInUse
					framework.Logf("current volumes in use: %+v", volumesInUse)
					for i := range volumesInUse {
						if strings.HasSuffix(string(volumesInUse[i]), volumeName) {
							return false
						}
					}
					return true
				})
				if !success {
					framework.Failf("timed out waiting for node=%s to not use the volume=%s", nodeName, volumeName)
				}

				// 创建快照。
				parameters := map[string]string{}
				sr := storageframework.CreateSnapshotResource(ctx, sDriver, config, pattern, pvc.Name, pvc.Namespace, f.Timeouts, parameters)
				ginkgo.DeferCleanup(sr.CleanupResource, f.Timeouts)
				vs := sr.Vs
				// 获取快照并检查 SnapshotContent 属性
				vscontent := checkSnapshot(ctx, dc, sr, pattern)

				ginkgo.By("Modifying source data test")
				var restoredPVC *v1.PersistentVolumeClaim
				var restoredPod *v1.Pod
				modifiedMntTestData := fmt.Sprintf("modified data from %s namespace", pvc.GetNamespace())

				ginkgo.By("modifying the data in the source PVC")

				// 向文件写入数据后，`sync` 会把数据从内存刷到磁盘。
				// Linux 和 Windows 版 agnhost 都提供 sync。
				command := fmt.Sprintf("echo '%s' > %s; sync", modifiedMntTestData, datapath)
				RunInPodWithVolume(ctx, cs, f.Timeouts, pvc.Namespace, pvc.Name, "pvc-snapshottable-data-tester", command, config.ClientNodeSelection)

				ginkgo.By("creating a pvc from the snapshot")
				claimSize = pvc.Spec.Resources.Requests.Storage().String()
				restoredPVC = e2epv.MakePersistentVolumeClaim(e2epv.PersistentVolumeClaimConfig{
					ClaimSize:        claimSize,
					StorageClassName: &(sc.Name),
				}, config.Framework.Namespace.Name)

				group := "snapshot.storage.k8s.io"

				restoredPVC.Spec.DataSource = &v1.TypedLocalObjectReference{
					APIGroup: &group,
					Kind:     "VolumeSnapshot",
					Name:     vs.GetName(),
				}

				restoredPVC, err = cs.CoreV1().PersistentVolumeClaims(restoredPVC.Namespace).Create(ctx, restoredPVC, metav1.CreateOptions{})
				framework.ExpectNoError(err)
				ginkgo.DeferCleanup(func(ctx context.Context) {
					framework.Logf("deleting claim %q/%q", restoredPVC.Namespace, restoredPVC.Name)
					// 通常此时 claim 已被删除
					err = cs.CoreV1().PersistentVolumeClaims(restoredPVC.Namespace).Delete(ctx, restoredPVC.Name, metav1.DeleteOptions{})
					if err != nil && !apierrors.IsNotFound(err) {
						framework.Failf("Error deleting claim %q. Error: %v", restoredPVC.Name, err)
					}
				})

				ginkgo.By("starting a pod to use the snapshot")
				restoredPod = StartInPodWithVolume(ctx, cs, restoredPVC.Namespace, restoredPVC.Name, "restored-pvc-tester", "sleep 300", config.ClientNodeSelection)
				ginkgo.DeferCleanup(StopPod, cs, restoredPod)
				framework.ExpectNoError(e2epod.WaitTimeoutForPodRunningInNamespace(ctx, cs, restoredPod.Name, restoredPod.Namespace, f.Timeouts.PodStartSlow))
				commands := e2evolume.GenerateReadFileCmd(datapath)
				_, err = e2eoutput.LookForStringInPodExec(restoredPod.Namespace, restoredPod.Name, commands, originalMntTestData, time.Minute)
				framework.ExpectNoError(err)

				ginkgo.By("should delete the VolumeSnapshotContent according to its deletion policy")

				// 同时删除 Snapshot 和恢复出的 Pod/PVC，因为不同存储系统
				// 的删除顺序不同。有的要求先删恢复出的 PVC 再删 Snapshot，
				// 有的正好相反。
				err = storageutils.DeleteSnapshotWithoutWaiting(ctx, dc, vs.GetNamespace(), vs.GetName())
				framework.ExpectNoError(err)
				framework.Logf("deleting restored pod %q/%q", restoredPod.Namespace, restoredPod.Name)
				err = cs.CoreV1().Pods(restoredPod.Namespace).Delete(ctx, restoredPod.Name, metav1.DeleteOptions{})
				framework.ExpectNoError(err)
				framework.Logf("deleting restored PVC %q/%q", restoredPVC.Namespace, restoredPVC.Name)
				err = cs.CoreV1().PersistentVolumeClaims(restoredPVC.Namespace).Delete(ctx, restoredPVC.Name, metav1.DeleteOptions{})
				framework.ExpectNoError(err)

				deleteVolumeSnapshot(ctx, f, dc, sr, pattern, vscontent)
			})
		})
	})
}

func deleteVolumeSnapshot(ctx context.Context, f *framework.Framework, dc dynamic.Interface, sr *storageframework.SnapshotResource, pattern storageframework.TestPattern, vscontent *unstructured.Unstructured) {
	vs := sr.Vs

	// 等待 Snapshot 真正从 API server 删除
	err := storageutils.WaitForNamespacedGVRDeletion(ctx, dc, storageutils.SnapshotGVR, vs.GetNamespace(), vs.GetName(), framework.Poll, f.Timeouts.SnapshotDelete)
	framework.ExpectNoError(err)

	switch pattern.SnapshotDeletionPolicy {
	case storageframework.DeleteSnapshot:
		ginkgo.By("checking the SnapshotContent has been deleted")
		err = storageutils.EnsureGVRDeletion(ctx, dc, storageutils.SnapshotContentGVR, vscontent.GetName(), framework.Poll, f.Timeouts.SnapshotDelete, "")
		framework.ExpectNoError(err)
	case storageframework.RetainSnapshot:
		ginkgo.By("checking the SnapshotContent has not been deleted")
		err = storageutils.EnsureNoGVRDeletion(ctx, dc, storageutils.SnapshotContentGVR, vscontent.GetName(), 1*time.Second /* poll */, 30*time.Second /* timeout */, "")
		framework.ExpectNoError(err)
	}
}

func checkSnapshot(ctx context.Context, dc dynamic.Interface, sr *storageframework.SnapshotResource, pattern storageframework.TestPattern) *unstructured.Unstructured {
	vs := sr.Vs
	vsc := sr.Vsclass

	// 获取快照的新副本
	ginkgo.By("checking the snapshot")
	vs, err := dc.Resource(storageutils.SnapshotGVR).Namespace(vs.GetNamespace()).Get(ctx, vs.GetName(), metav1.GetOptions{})
	framework.ExpectNoError(err)

	// 获取绑定的 snapshotContent
	snapshotStatus := vs.Object["status"].(map[string]interface{})
	snapshotContentName := snapshotStatus["boundVolumeSnapshotContentName"].(string)
	vscontent, err := dc.Resource(storageutils.SnapshotContentGVR).Get(ctx, snapshotContentName, metav1.GetOptions{})
	framework.ExpectNoError(err)

	snapshotContentSpec := vscontent.Object["spec"].(map[string]interface{})
	volumeSnapshotRef := snapshotContentSpec["volumeSnapshotRef"].(map[string]interface{})

	// 检查 SnapshotContent 属性
	ginkgo.By("checking the SnapshotContent")
	// PreprovisionedCreatedSnapshot 不需要设置 volume snapshot class 名
	if pattern.SnapshotType != storageframework.PreprovisionedCreatedSnapshot {
		gomega.Expect(snapshotContentSpec["volumeSnapshotClassName"]).To(gomega.Equal(vsc.GetName()))
	}
	gomega.Expect(volumeSnapshotRef).To(gomega.HaveKeyWithValue("name", vs.GetName()))
	gomega.Expect(volumeSnapshotRef).To(gomega.HaveKeyWithValue("namespace", vs.GetNamespace()))
	return vscontent
}
