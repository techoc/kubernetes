/*
Copyright 2015 The Kubernetes Authors.

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

package storage

import (
	"context"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/apimachinery/pkg/util/uuid"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	apimachineryutils "k8s.io/kubernetes/test/e2e/common/apimachinery"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2epv "k8s.io/kubernetes/test/e2e/framework/pv"
	e2estatefulset "k8s.io/kubernetes/test/e2e/framework/statefulset"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	"k8s.io/kubernetes/test/e2e/storage/utils"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gstruct"
)

// 校验 PV/PVC，创建并验证 writer pod，删除 PVC，并校验 PV 的
// phase。注意：PV 在 AfterEach 中删除，不在这里。
func completeTest(ctx context.Context, f *framework.Framework, c clientset.Interface, ns string, pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim) {
	// 1. 验证 PV 和 PVC 正确绑定
	ginkgo.By("Validating the PV-PVC binding")
	framework.ExpectNoError(e2epv.WaitOnPVandPVC(ctx, c, f.Timeouts, ns, pv, pvc))

	// 2. 创建 nfs writer pod，测试写入是否成功，
	//    然后删除 pod 并验证它已被删除
	ginkgo.By("Checking pod has write access to PersistentVolume")
	framework.ExpectNoError(createWaitAndDeletePod(ctx, c, f.Timeouts, ns, pvc, "touch /mnt/volume1/SUCCESS && (id -G | grep -E '\\b777\\b')"))

	// 3. 删除 PVC，等待 PV 变为 "Released"
	ginkgo.By("Deleting the PVC to invoke the reclaim policy.")
	framework.ExpectNoError(e2epv.DeletePVCandValidatePV(ctx, c, f.Timeouts, ns, pvc, pv, v1.VolumeReleased))
}

// 校验多对 PV 和 PVC，创建并验证 writer pod，删除 PVC 并校验
// PV。确保每一步都成功。
// 注意：PV 在 AfterEach 中删除，不在这里。
// 注意：这个函数是串行的，创建下一个 pod 前会等待
//
// 前一个 pod 被删除。增加并发是待办事项。
func completeMultiTest(ctx context.Context, f *framework.Framework, c clientset.Interface, ns string, pvols e2epv.PVMap, claims e2epv.PVCMap, expectPhase v1.PersistentVolumePhase) error {
	var err error

	// 1. 验证每个 PV 都允许 client pod 写入
	ginkgo.By("Checking pod has write access to PersistentVolumes")
	for pvcKey := range claims {
		pvc, err := c.CoreV1().PersistentVolumeClaims(pvcKey.Namespace).Get(ctx, pvcKey.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("error getting pvc %q: %w", pvcKey.Name, err)
		}
		if len(pvc.Spec.VolumeName) == 0 {
			continue // claim is not bound
		}
		// 健全性检查，确保我们的 map 同步
		_, found := pvols[pvc.Spec.VolumeName]
		if !found {
			return fmt.Errorf("internal: pvols map is missing volume %q", pvc.Spec.VolumeName)
		}
		// TODO: 目前对每个 PV 都是串行测试
		if err = createWaitAndDeletePod(ctx, c, f.Timeouts, pvcKey.Namespace, pvc, "touch /mnt/volume1/SUCCESS && (id -G | grep -E '\\b777\\b')"); err != nil {
			return err
		}
	}

	// 2. 删除每个 PVC，等待其绑定的 PV 达到 `expectedPhase`
	ginkgo.By("Deleting PVCs to invoke reclaim policy")
	if err = e2epv.DeletePVCandValidatePVGroup(ctx, c, f.Timeouts, ns, pvols, claims, expectPhase); err != nil {
		return err
	}
	return nil
}

var _ = utils.SIGDescribe("PersistentVolumes", func() {

	// 下面 ginkgo.Context() 和 ginkgo.It() 的全局变量
	f := framework.NewDefaultFramework("pv")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	var (
		c         clientset.Interface
		ns        string
		pvConfig  e2epv.PersistentVolumeConfig
		pvcConfig e2epv.PersistentVolumeClaimConfig
		volLabel  labels.Set
		selector  *metav1.LabelSelector
		pv        *v1.PersistentVolume
		pvc       *v1.PersistentVolumeClaim
		err       error
	)
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.BeforeEach(func() {
		c = f.ClientSet
		ns = f.Namespace.Name
		// 通过 selector label 强制只在测试空间内绑定
		volLabel = labels.Set{e2epv.VolumeSelectorKey: ns}
		selector = metav1.SetAsLabelSelector(volLabel)
	})

	// 测试单个 PV/PVC 对、多个均匀配对的 PV/PVC，
	// 以及多个非均匀配对的 PV/PVC 的配置
	ginkgo.Describe("NFS", func() {

		var (
			nfsServerPod *v1.Pod
			serverHost   string
		)

		ginkgo.BeforeEach(func(ctx context.Context) {
			_, nfsServerPod, serverHost = e2evolume.NewNFSServer(ctx, c, ns, []string{"-G", "777", "/exports"})
			pvConfig = e2epv.PersistentVolumeConfig{
				NamePrefix: "nfs-",
				Labels:     volLabel,
				PVSource: v1.PersistentVolumeSource{
					NFS: &v1.NFSVolumeSource{
						Server:   serverHost,
						Path:     "/exports",
						ReadOnly: false,
					},
				},
			}
			emptyStorageClass := ""
			pvcConfig = e2epv.PersistentVolumeClaimConfig{
				Selector:         selector,
				StorageClassName: &emptyStorageClass,
			}
		})

		ginkgo.AfterEach(func(ctx context.Context) {
			framework.ExpectNoError(e2epod.DeletePodWithWait(ctx, c, nfsServerPod), "AfterEach: Failed to delete pod ", nfsServerPod.Name)
			pv, pvc = nil, nil
			pvConfig, pvcConfig = e2epv.PersistentVolumeConfig{}, e2epv.PersistentVolumeClaimConfig{}
		})

		ginkgo.Context("with Single PV - PVC pairs", func() {
			// 注意：这是唯一删除 pv 的代码。
			ginkgo.AfterEach(func(ctx context.Context) {
				framework.Logf("AfterEach: Cleaning up test resources.")
				if errs := e2epv.PVPVCCleanup(ctx, c, ns, pv, pvc); len(errs) > 0 {
					framework.Failf("AfterEach: Failed to delete PVC and/or PV. Errors: %v", utilerrors.NewAggregate(errs))
				}
			})

			// 各个测试如下：
			//
			// 创建一个 nfs PV，再创建一个匹配该 PV 的 claim，以及一个
			// 包含该 claim 的 pod。验证 PV 和 PVC 正确绑定，且
			// pod 能写入 nfs volume。
			ginkgo.It("should create a non-pre-bound PV and PVC: test write access", func(ctx context.Context) {
				pv, pvc, err = e2epv.CreatePVPVC(ctx, c, f.Timeouts, pvConfig, pvcConfig, ns, false)
				framework.ExpectNoError(err)
				completeTest(ctx, f, c, ns, pv, pvc)
			})

			// 先创建一个 claim，再创建匹配该 claim 的 nfs PV，以及一个
			// 包含该 claim 的 pod。验证 PV 和 PVC 正确
			// 绑定，且 pod 能写入 nfs volume。
			ginkgo.It("create a PVC and non-pre-bound PV: test write access", func(ctx context.Context) {
				pv, pvc, err = e2epv.CreatePVCPV(ctx, c, f.Timeouts, pvConfig, pvcConfig, ns, false)
				framework.ExpectNoError(err)
				completeTest(ctx, f, c, ns, pv, pvc)
			})

			// 先创建一个 claim，再创建匹配该 claim 的预绑定 nfs PV，
			// 以及一个包含该 claim 的 pod。验证 PV 和 PVC 正确
			// 绑定，且 pod 能写入 nfs volume。
			ginkgo.It("create a PVC and a pre-bound PV: test write access", func(ctx context.Context) {
				pv, pvc, err = e2epv.CreatePVCPV(ctx, c, f.Timeouts, pvConfig, pvcConfig, ns, true)
				framework.ExpectNoError(err)
				completeTest(ctx, f, c, ns, pv, pvc)
			})

			// 先创建一个 nfs PV，再创建匹配该 PV 的预绑定 PVC，
			// 以及一个包含该 claim 的 pod。验证 PV 和 PVC 正确
			// 绑定，且 pod 能写入 nfs volume。
			ginkgo.It("create a PV and a pre-bound PVC: test write access", func(ctx context.Context) {
				pv, pvc, err = e2epv.CreatePVPVC(ctx, c, f.Timeouts, pvConfig, pvcConfig, ns, true)
				framework.ExpectNoError(err)
				completeTest(ctx, f, c, ns, pv, pvc)
			})

			// 与上面相同，但 pod 中有多个 volume 引用同一个 PVC。
			ginkgo.It("create a PVC and use it multiple times in a single pod", func(ctx context.Context) {
				pv, pvc, err = e2epv.CreatePVPVC(ctx, c, f.Timeouts, pvConfig, pvcConfig, ns, true)
				framework.ExpectNoError(err)

				framework.Logf("Creating nfs test pod")
				pod := e2epod.MakePod(ns, nil, []*v1.PersistentVolumeClaim{pvc, pvc}, admissionapi.LevelPrivileged,
					"touch /mnt/volume1/SUCCESS && cat /mnt/volume2/SUCCESS")
				runPod, err := c.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
				framework.ExpectNoError(err)
				defer func() {
					err := e2epod.DeletePodWithWait(ctx, c, runPod)
					framework.ExpectNoError(err)
				}()

				err = testPodSuccessOrFail(ctx, c, f.Timeouts, ns, runPod)
				framework.ExpectNoError(err)
			})

			// 创建无 claim 的新 PV，验证其处于 Available 状态且设置了 LastPhaseTransitionTime。
			f.It("create a PV: test phase transition timestamp is set and phase is Available", func(ctx context.Context) {
				pvObj := e2epv.MakePersistentVolume(pvConfig)
				pv, err = e2epv.CreatePV(ctx, c, f.Timeouts, pvObj)
				framework.ExpectNoError(err)

				// 新 PV 应转换 phase 到：Available
				err = e2epv.WaitForPersistentVolumePhase(ctx, v1.VolumeAvailable, c, pv.Name, 2*time.Second, framework.ClaimProvisionShortTimeout)
				framework.ExpectNoError(err)

				// 验证新 PV 设置了 phase 转换时间戳。
				pv, err = c.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
				framework.ExpectNoError(err)
				if pv.Status.LastPhaseTransitionTime == nil {
					framework.Failf("New persistent volume %v should have LastPhaseTransitionTime value set, but it's nil.", pv.GetName())
				}
			})

			// 创建 PV 和匹配该 PV 的预绑定 PVC，验证 PV 和 PVC 绑定时
			// PV 的 LastPhaseTransitionTime 字段被更新。
			f.It("create a PV and a pre-bound PVC: test phase transition timestamp is set", func(ctx context.Context) {
				pv, pvc, err = e2epv.CreatePVPVC(ctx, c, f.Timeouts, pvConfig, pvcConfig, ns, true)
				framework.ExpectNoError(err)

				// claim 应转换 phase 到：Bound
				err = e2epv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, c, ns, pvc.Name, 2*time.Second, framework.ClaimProvisionShortTimeout)
				framework.ExpectNoError(err)
				pvc, err = c.CoreV1().PersistentVolumeClaims(ns).Get(ctx, pvc.Name, metav1.GetOptions{})
				framework.ExpectNoError(err)
				pv, err = c.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
				framework.ExpectNoError(err)
				if pv.Status.LastPhaseTransitionTime == nil {
					framework.Failf("Persistent volume %v should have LastPhaseTransitionTime value set after transitioning phase, but it's nil.", pv.GetName())
				}
				completeTest(ctx, f, c, ns, pv, pvc)
			})

			// 创建 PV 和匹配该 PV 的预绑定 PVC，验证 PV 和 PVC 绑定时
			// PV 的 LastPhaseTransitionTime 字段被设置，然后删除 PVC 使 PV phase 变为
			// released，并验证 PV LastPhaseTransitionTime 正确更新时间戳。
			f.It("create a PV and a pre-bound PVC: test phase transition timestamp multiple updates", func(ctx context.Context) {
				pv, pvc, err = e2epv.CreatePVPVC(ctx, c, f.Timeouts, pvConfig, pvcConfig, ns, true)
				framework.ExpectNoError(err)

				// claim 应转换 phase 到：Bound。
				err = e2epv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, c, ns, pvc.Name, 2*time.Second, framework.ClaimProvisionShortTimeout)
				framework.ExpectNoError(err)
				pvc, err = c.CoreV1().PersistentVolumeClaims(ns).Get(ctx, pvc.Name, metav1.GetOptions{})
				framework.ExpectNoError(err)
				pv, err = c.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
				framework.ExpectNoError(err)

				// 保存第一次 phase 转换时间。
				firstPhaseTransition := pv.Status.LastPhaseTransitionTime

				// 让测试完成并删除 PVC。
				completeTest(ctx, f, c, ns, pv, pvc)

				// claim 应转换 phase 到：Released。
				err = e2epv.WaitForPersistentVolumePhase(ctx, v1.VolumeReleased, c, pv.Name, 2*time.Second, framework.ClaimProvisionShortTimeout)
				framework.ExpectNoError(err)

				// 验证 phase 转换时间戳在第一次转换*之后*按时间顺序更新。
				pv, err = c.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
				if !firstPhaseTransition.Before(pv.Status.LastPhaseTransitionTime) {
					framework.Failf("Persistent volume %v should have LastPhaseTransitionTime value updated to be chronologically after previous phase change: %v, but it's %v.", pv.GetName(), firstPhaseTransition, pv.Status.LastPhaseTransitionTime)
				}
			})
		})

		// 创建多个 pv 和 pvc，都在同一 namespace。验证 PV-PVC
		// 能绑定，但事先不知道哪个 PV 会绑定到
		// 哪个 claim。为每对 pv-pvc 创建一个写入 nfs mount 的 pod。
		// 注意：当 PV 数量超过 PVC 数量时，每个多余的 PV
		//   都会产生最大绑定等待时间。这是预期的，但 delta
		//   应保持较小，以免测试不必要的缓慢。
		// 注意：未来的测试可能希望加入以下内容：
		//   a) 预绑定，b) 先创建 pvc 再创建 pv，c) 在不同 namespace
		//   中创建 pvc 和 pod。
		ginkgo.Context("with multiple PVs and PVCs all in same ns", func() {

			// 把 pv 和 pvc 的 map 作用域限定为 AfterEach 可用
			// 注意：这些 map 在 CreatePVsPVCs() 中重新创建
			var pvols e2epv.PVMap
			var claims e2epv.PVCMap

			ginkgo.AfterEach(func(ctx context.Context) {
				framework.Logf("AfterEach: deleting %v PVCs and %v PVs...", len(claims), len(pvols))
				errs := e2epv.PVPVCMapCleanup(ctx, c, ns, pvols, claims)
				if len(errs) > 0 {
					errmsg := []string{}
					for _, e := range errs {
						errmsg = append(errmsg, e.Error())
					}
					framework.Failf("AfterEach: Failed to delete 1 or more PVs/PVCs. Errors: %v", strings.Join(errmsg, "; "))
				}
			})

			// 创建 2 个 PV 和 4 个 PVC。
			// 注意：PV 在 claim 之前创建，且无预绑定
			ginkgo.It("should create 2 PVs and 4 PVCs: test write access", func(ctx context.Context) {
				numPVs, numPVCs := 2, 4
				pvols, claims, err = e2epv.CreatePVsPVCs(ctx, numPVs, numPVCs, c, f.Timeouts, ns, pvConfig, pvcConfig)
				framework.ExpectNoError(err)
				framework.ExpectNoError(e2epv.WaitAndVerifyBinds(ctx, c, f.Timeouts, ns, pvols, claims, true))
				framework.ExpectNoError(completeMultiTest(ctx, f, c, ns, pvols, claims, v1.VolumeReleased))
			})

			// 创建 3 个 PV 和 3 个 PVC。
			// 注意：PV 在 claim 之前创建，且无预绑定
			ginkgo.It("should create 3 PVs and 3 PVCs: test write access", func(ctx context.Context) {
				numPVs, numPVCs := 3, 3
				pvols, claims, err = e2epv.CreatePVsPVCs(ctx, numPVs, numPVCs, c, f.Timeouts, ns, pvConfig, pvcConfig)
				framework.ExpectNoError(err)
				framework.ExpectNoError(e2epv.WaitAndVerifyBinds(ctx, c, f.Timeouts, ns, pvols, claims, true))
				framework.ExpectNoError(completeMultiTest(ctx, f, c, ns, pvols, claims, v1.VolumeReleased))
			})

			// 创建 4 个 PV 和 2 个 PVC。
			// 注意：PV 在 claim 之前创建，且无预绑定。
			f.It("should create 4 PVs and 2 PVCs: test write access", f.WithSlow(), func(ctx context.Context) {
				numPVs, numPVCs := 4, 2
				pvols, claims, err = e2epv.CreatePVsPVCs(ctx, numPVs, numPVCs, c, f.Timeouts, ns, pvConfig, pvcConfig)
				framework.ExpectNoError(err)
				framework.ExpectNoError(e2epv.WaitAndVerifyBinds(ctx, c, f.Timeouts, ns, pvols, claims, true))
				framework.ExpectNoError(completeMultiTest(ctx, f, c, ns, pvols, claims, v1.VolumeReleased))
			})
		})

		// 这个 Context 专门隔离并测试 "Recycle" reclaim 行为。Recycler
		// 被弃用后，整个 context 可以删除，不会影响测试套件，也不会留下
		// 死代码。
		ginkgo.Context("when invoking the Recycle reclaim policy", func() {
			ginkgo.BeforeEach(func(ctx context.Context) {
				pvConfig.ReclaimPolicy = v1.PersistentVolumeReclaimRecycle
				pv, pvc, err = e2epv.CreatePVPVC(ctx, c, f.Timeouts, pvConfig, pvcConfig, ns, false)
				framework.ExpectNoError(err, "BeforeEach: Failed to create PV/PVC")
				framework.ExpectNoError(e2epv.WaitOnPVandPVC(ctx, c, f.Timeouts, ns, pv, pvc), "BeforeEach: WaitOnPVandPVC failed")
			})

			ginkgo.AfterEach(func(ctx context.Context) {
				framework.Logf("AfterEach: Cleaning up test resources.")
				if errs := e2epv.PVPVCCleanup(ctx, c, ns, pv, pvc); len(errs) > 0 {
					framework.Failf("AfterEach: Failed to delete PVC and/or PV. Errors: %v", utilerrors.NewAggregate(errs))
				}
			})

			// 这个 ginkgo.It() 测试如下场景：PV 被 Pod 写入、回收，然后检查
			// volume 中是否有文件。发现文件则检查 Pod 失败，测试失败。否则 pod
			// （和测试）成功。
			ginkgo.It("should test that a PV becomes Available and is clean after the PVC is deleted.", func(ctx context.Context) {
				ginkgo.By("Writing to the volume.")
				pod := e2epod.MakePod(ns, nil, []*v1.PersistentVolumeClaim{pvc}, f.NamespacePodSecurityLevel, "touch /mnt/volume1/SUCCESS && (id -G | grep -E '\\b777\\b')")
				pod, err = c.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
				framework.ExpectNoError(err)
				framework.ExpectNoError(e2epod.WaitForPodSuccessInNamespaceTimeout(ctx, c, pod.Name, ns, f.Timeouts.PodStart))

				ginkgo.By("Deleting the claim")
				framework.ExpectNoError(e2epod.DeletePodWithWait(ctx, c, pod))
				framework.ExpectNoError(e2epv.DeletePVCandValidatePV(ctx, c, f.Timeouts, ns, pvc, pv, v1.VolumeAvailable))

				ginkgo.By("Re-mounting the volume.")
				pvc = e2epv.MakePersistentVolumeClaim(pvcConfig, ns)
				pvc, err = e2epv.CreatePVC(ctx, c, ns, pvc)
				framework.ExpectNoError(err)
				framework.ExpectNoError(e2epv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, c, ns, pvc.Name, 2*time.Second, 60*time.Second), "Failed to reach 'Bound' for PVC ", pvc.Name)

				// 如果在 /mnt 中发现文件，让 pod 失败且不重启它。
				ginkgo.By("Verifying the mount has been cleaned.")
				mount := pod.Spec.Containers[0].VolumeMounts[0].MountPath
				pod = e2epod.MakePod(ns, nil, []*v1.PersistentVolumeClaim{pvc}, f.NamespacePodSecurityLevel, fmt.Sprintf("[ $(ls -A %s | wc -l) -eq 0 ] && exit 0 || exit 1", mount))
				pod, err = c.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
				framework.ExpectNoError(err)
				framework.ExpectNoError(e2epod.WaitForPodSuccessInNamespaceTimeout(ctx, c, pod.Name, ns, f.Timeouts.PodStart))

				framework.ExpectNoError(e2epod.DeletePodWithWait(ctx, c, pod))
				framework.Logf("Pod exited without failure; the volume has been recycled.")

				// 删除 PVC，等待 recycler 完成后再在清理时关闭 NFS server。
				framework.Logf("Removing second PVC, waiting for the recycler to finish before cleanup.")
				framework.ExpectNoError(e2epv.DeletePVCandValidatePV(ctx, c, f.Timeouts, ns, pvc, pv, v1.VolumeAvailable))
				pvc = nil
			})
		})
	})

	ginkgo.Describe("CSI Conformance", func() {

		var pvols e2epv.PVMap
		var claims e2epv.PVCMap

		ginkgo.AfterEach(func(ctx context.Context) {
			framework.Logf("AfterEach: deleting %v PVCs and %v PVs...", len(claims), len(pvols))
			errs := e2epv.PVPVCMapCleanup(ctx, c, ns, pvols, claims)
			if len(errs) > 0 {
				errmsg := []string{}
				for _, e := range errs {
					errmsg = append(errmsg, e.Error())
				}
				framework.Failf("AfterEach: Failed to delete 1 or more PVs/PVCs. Errors: %v", strings.Join(errmsg, "; "))
			}
		})

		/*
			发布版本：v1.29
			测试名称：PersistentVolumes(Claims), lifecycle
			描述：创建 PV 和 PVC 必须成功。用 labelSelector 列举 PV
			必须成功。列举 namespace 中的 PVC 必须成功。patch PV 必须成功
			且能找到新标签。patch PVC 必须成功且能找到新标签。
			读取 PV 和 PVC 必须成功且能取回所需 UID。删除 PVC
			和 PV 必须成功且必须被确认。必须创建替代的 PV 和 PVC。
			更新 PV 必须成功且能找到新标签。更新 PVC 必须成功
			且能找到新标签。通过 deleteCollection 删除 PVC 和 PV 必须成功
			且必须被确认。
		*/
		framework.ConformanceIt("should run through the lifecycle of a PV and a PVC", func(ctx context.Context) {

			pvClient := c.CoreV1().PersistentVolumes()
			pvcClient := c.CoreV1().PersistentVolumeClaims(ns)

			ginkgo.By("Creating initial PV and PVC")

			// 配置 csiDriver
			defaultFSGroupPolicy := storagev1.ReadWriteOnceWithFSTypeFSGroupPolicy
			csiDriverLabel := map[string]string{"e2e-test": f.UniqueName}
			csiDriver := &storagev1.CSIDriver{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "inline-driver-" + string(uuid.NewUUID()),
					Labels: csiDriverLabel,
				},

				Spec: storagev1.CSIDriverSpec{
					VolumeLifecycleModes: []storagev1.VolumeLifecycleMode{
						storagev1.VolumeLifecyclePersistent,
					},
					FSGroupPolicy: &defaultFSGroupPolicy,
				},
			}

			pvNamePrefix := ns + "-"
			pvHostPathConfig := e2epv.PersistentVolumeConfig{
				NamePrefix:       pvNamePrefix,
				Labels:           volLabel,
				StorageClassName: ns,
				PVSource: v1.PersistentVolumeSource{
					CSI: &v1.CSIPersistentVolumeSource{
						Driver:       csiDriver.Name,
						VolumeHandle: "e2e-conformance",
					},
				},
			}
			pvcConfig := e2epv.PersistentVolumeClaimConfig{
				StorageClassName: &ns,
			}

			numPVs, numPVCs := 1, 1
			pvols, claims, err = e2epv.CreatePVsPVCs(ctx, numPVs, numPVCs, c, f.Timeouts, ns, pvHostPathConfig, pvcConfig)
			framework.ExpectNoError(err, "Failed to create the requested storage resources")

			ginkgo.By(fmt.Sprintf("Listing all PVs with the labelSelector: %q", volLabel.AsSelector().String()))
			pvList, err := pvClient.List(ctx, metav1.ListOptions{LabelSelector: volLabel.AsSelector().String()})
			framework.ExpectNoError(err, "Failed to list PVs with the labelSelector: %q", volLabel.AsSelector().String())
			gomega.Expect(pvList.Items).To(gomega.HaveLen(1))
			initialPV := pvList.Items[0]
			gomega.Expect(&initialPV).To(apimachineryutils.HaveValidResourceVersion())

			ginkgo.By(fmt.Sprintf("Listing PVCs in namespace %q", ns))
			pvcList, err := pvcClient.List(ctx, metav1.ListOptions{})
			framework.ExpectNoError(err, "Failed to list PVCs with the labelSelector: %q", volLabel.AsSelector().String())
			gomega.Expect(pvcList.Items).To(gomega.HaveLen(1))
			initialPVC := pvcList.Items[0]
			gomega.Expect(&initialPVC).To(apimachineryutils.HaveValidResourceVersion())

			ginkgo.By(fmt.Sprintf("Patching the PV %q", initialPV.Name))
			payload := "{\"metadata\":{\"labels\":{\"" + initialPV.Name + "\":\"patched\"}}}"
			patchedPV, err := pvClient.Patch(ctx, initialPV.Name, types.StrategicMergePatchType, []byte(payload), metav1.PatchOptions{})
			framework.ExpectNoError(err, "Failed to patch PV %q", initialPV.Name)
			gomega.Expect(patchedPV.Labels).To(gomega.HaveKeyWithValue(patchedPV.Name, "patched"), "Checking that patched label has been applied")
			gomega.Expect(resourceversion.CompareResourceVersion(initialPV.ResourceVersion, patchedPV.ResourceVersion)).To(gomega.BeNumerically("==", -1), "patched object should have a larger resource version")

			ginkgo.By(fmt.Sprintf("Patching the PVC %q", initialPVC.Name))
			payload = "{\"metadata\":{\"labels\":{\"" + initialPVC.Name + "\":\"patched\"}}}"
			patchedPVC, err := pvcClient.Patch(ctx, initialPVC.Name, types.StrategicMergePatchType, []byte(payload), metav1.PatchOptions{})
			framework.ExpectNoError(err, "Failed to patch PVC %q", initialPVC.Name)
			gomega.Expect(patchedPVC.Labels).To(gomega.HaveKeyWithValue(patchedPVC.Name, "patched"), "Checking that patched label has been applied")
			gomega.Expect(resourceversion.CompareResourceVersion(initialPVC.ResourceVersion, patchedPVC.ResourceVersion)).To(gomega.BeNumerically("==", -1), "patched object should have a larger resource version")

			ginkgo.By(fmt.Sprintf("Getting PV %q", patchedPV.Name))
			retrievedPV, err := pvClient.Get(ctx, patchedPV.Name, metav1.GetOptions{})
			framework.ExpectNoError(err, "Failed to get PV %q", patchedPV.Name)
			gomega.Expect(retrievedPV.UID).To(gomega.Equal(patchedPV.UID))

			ginkgo.By(fmt.Sprintf("Getting PVC %q", patchedPVC.Name))
			retrievedPVC, err := pvcClient.Get(ctx, patchedPVC.Name, metav1.GetOptions{})
			framework.ExpectNoError(err, "Failed to get PVC %q", patchedPVC.Name)
			gomega.Expect(retrievedPVC.UID).To(gomega.Equal(patchedPVC.UID))

			ginkgo.By(fmt.Sprintf("Deleting PVC %q", retrievedPVC.Name))
			err = pvcClient.Delete(ctx, retrievedPVC.Name, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "Failed to delete PVC %q", retrievedPVC.Name)

			ginkgo.By(fmt.Sprintf("Confirm deletion of PVC %q", retrievedPVC.Name))

			type state struct {
				PersistentVolumes      []v1.PersistentVolume
				PersistentVolumeClaims []v1.PersistentVolumeClaim
			}

			err = framework.Gomega().Eventually(ctx, framework.HandleRetry(func(ctx context.Context) (*state, error) {
				pvcList, err := pvcClient.List(ctx, metav1.ListOptions{})
				if err != nil {
					return nil, fmt.Errorf("failed to list pvc: %w", err)
				}
				return &state{
					PersistentVolumeClaims: pvcList.Items,
				}, nil
			})).WithTimeout(30 * time.Second).Should(framework.MakeMatcher(func(s *state) (func() string, error) {
				if len(s.PersistentVolumeClaims) == 0 {
					return nil, nil
				}
				return func() string {
					return fmt.Sprintf("Expected pvc to be deleted, found %q", s.PersistentVolumeClaims[0].Name)
				}, nil
			}))
			framework.ExpectNoError(err, "Timeout while waiting to confirm PVC %q deletion", retrievedPVC.Name)

			ginkgo.By(fmt.Sprintf("Deleting PV %q", retrievedPV.Name))
			err = pvClient.Delete(ctx, retrievedPV.Name, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "Failed to delete PV %q", retrievedPV.Name)

			ginkgo.By(fmt.Sprintf("Confirm deletion of PV %q", retrievedPV.Name))
			err = framework.Gomega().Eventually(ctx, framework.HandleRetry(func(ctx context.Context) (*state, error) {
				pvList, err := pvClient.List(ctx, metav1.ListOptions{LabelSelector: volLabel.AsSelector().String()})
				if err != nil {
					return nil, fmt.Errorf("failed to list pv: %w", err)
				}
				return &state{
					PersistentVolumes: pvList.Items,
				}, nil
			})).WithTimeout(30 * time.Second).Should(framework.MakeMatcher(func(s *state) (func() string, error) {
				if len(s.PersistentVolumes) == 0 {
					return nil, nil
				}
				return func() string {
					return fmt.Sprintf("Expected pv to be deleted, found %q", s.PersistentVolumes[0].Name)
				}, nil
			}))
			framework.ExpectNoError(err, "Timeout while waiting to confirm PV %q deletion", retrievedPV.Name)

			ginkgo.By("Recreating another PV & PVC")
			pvols, claims, err = e2epv.CreatePVsPVCs(ctx, numPVs, numPVCs, c, f.Timeouts, ns, pvHostPathConfig, pvcConfig)
			framework.ExpectNoError(err, "Failed to create the requested storage resources")

			var pvName string
			for key := range pvols {
				pvName = key
			}

			var pvcName string
			for key := range claims {
				pvcName = key.Name
			}

			ginkgo.By(fmt.Sprintf("Updating the PV %q", pvName))
			var updatedPV *v1.PersistentVolume
			pvSelector := labels.Set{pvName: "updated"}.AsSelector().String()

			err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
				pv, err := pvClient.Get(ctx, pvName, metav1.GetOptions{})
				framework.ExpectNoError(err, "Unable to get PV %q", pvName)
				pv.Labels[pvName] = "updated"
				updatedPV, err = pvClient.Update(ctx, pv, metav1.UpdateOptions{})

				return err
			})
			framework.ExpectNoError(err, "failed to update PV %q", pvName)
			gomega.Expect(updatedPV.Labels).To(gomega.HaveKeyWithValue(updatedPV.Name, "updated"), "Checking that updated label has been applied")

			ginkgo.By(fmt.Sprintf("Updating the PVC %q", pvcName))
			var updatedPVC *v1.PersistentVolumeClaim
			pvcSelector := labels.Set{pvcName: "updated"}.AsSelector().String()

			err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
				pvc, err := pvcClient.Get(ctx, pvcName, metav1.GetOptions{})
				framework.ExpectNoError(err, "Unable to get PVC %q", pvcName)
				pvc.Labels = map[string]string{
					pvcName: "updated",
				}
				updatedPVC, err = pvcClient.Update(ctx, pvc, metav1.UpdateOptions{})

				return err
			})
			framework.ExpectNoError(err, "failed to update PVC %q", pvcName)
			gomega.Expect(updatedPVC.Labels).To(gomega.HaveKeyWithValue(updatedPVC.Name, "updated"), "Checking that updated label has been applied")

			ginkgo.By(fmt.Sprintf("Listing PVCs in all namespaces with the labelSelector: %q", pvcSelector))
			pvcList, err = c.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{LabelSelector: pvcSelector})
			framework.ExpectNoError(err, "Failed to list PVCs in all namespaces with the labelSelector: %q", pvcSelector)
			gomega.Expect(pvcList.Items).To(gomega.HaveLen(1))

			ginkgo.By(fmt.Sprintf("Deleting PVC %q via DeleteCollection", pvcName))
			err = pvcClient.DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: pvcSelector})
			framework.ExpectNoError(err, "Failed to delete PVC %q", retrievedPVC.Name)

			ginkgo.By(fmt.Sprintf("Confirm deletion of PVC %q", pvcName))
			err = framework.Gomega().Eventually(ctx, framework.HandleRetry(func(ctx context.Context) (*state, error) {
				pvcList, err := pvcClient.List(ctx, metav1.ListOptions{LabelSelector: pvcSelector})
				if err != nil {
					return nil, fmt.Errorf("failed to list pvc: %w", err)
				}
				return &state{
					PersistentVolumeClaims: pvcList.Items,
				}, nil
			})).WithTimeout(30 * time.Second).Should(framework.MakeMatcher(func(s *state) (func() string, error) {
				if len(s.PersistentVolumeClaims) == 0 {
					return nil, nil
				}
				return func() string {
					return fmt.Sprintf("Expected pvc to be deleted, found %q", s.PersistentVolumeClaims[0].Name)
				}, nil
			}))
			framework.ExpectNoError(err, "Timeout while waiting to confirm PVC %q deletion", pvcName)

			ginkgo.By(fmt.Sprintf("Deleting PV %q via DeleteCollection", pvName))
			err = pvClient.DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: pvSelector})
			framework.ExpectNoError(err, "Failed to delete PV %q", retrievedPVC.Name)

			ginkgo.By(fmt.Sprintf("Confirm deletion of PV %q", pvName))
			err = framework.Gomega().Eventually(ctx, framework.HandleRetry(func(ctx context.Context) (*state, error) {
				pvList, err := pvClient.List(ctx, metav1.ListOptions{LabelSelector: pvSelector})
				if err != nil {
					return nil, fmt.Errorf("failed to list pv: %w", err)
				}
				return &state{
					PersistentVolumes: pvList.Items,
				}, nil
			})).WithTimeout(30 * time.Second).Should(framework.MakeMatcher(func(s *state) (func() string, error) {
				if len(s.PersistentVolumes) == 0 {
					return nil, nil
				}
				return func() string {
					return fmt.Sprintf("Expected pv to be deleted, found %q", s.PersistentVolumes[0].Name)
				}, nil
			}))
			framework.ExpectNoError(err, "Timeout while waiting to confirm PV %q deletion", retrievedPV.Name)
		})

		/*
			发布版本：v1.29
			测试名称：PersistentVolumes(Claims), apply changes to a pv/pvc status
			描述：创建 PV 和 PVC 必须成功。用 labelSelector 列举 PV
			 必须成功。列举 namespace 中的 PVC 必须成功。读取 PVC status 必须
			 成功且找到有效 phase。读取 PV status 必须成功且找到有效
			 phase。patch PVC status 必须成功且找到新 condition。
			 patch PV status 必须成功且找到新的 reason/message。更新
			 PVC status 必须成功且找到新 condition。更新 PV status
			 必须成功且找到新的 reason/message。
		*/
		framework.ConformanceIt("should apply changes to a pv/pvc status", func(ctx context.Context) {

			pvClient := c.CoreV1().PersistentVolumes()
			pvcClient := c.CoreV1().PersistentVolumeClaims(ns)

			ginkgo.By("Creating initial PV and PVC")

			pvHostPathConfig := e2epv.PersistentVolumeConfig{
				NamePrefix:       ns + "-",
				Labels:           volLabel,
				StorageClassName: ns,
				PVSource: v1.PersistentVolumeSource{
					CSI: &v1.CSIPersistentVolumeSource{
						Driver:       "e2e-driver-" + string(uuid.NewUUID()),
						VolumeHandle: "e2e-status-conformance",
					},
				},
			}

			pvcConfig := e2epv.PersistentVolumeClaimConfig{
				StorageClassName: &ns,
			}

			numPVs, numPVCs := 1, 1
			pvols, claims, err = e2epv.CreatePVsPVCs(ctx, numPVs, numPVCs, c, f.Timeouts, ns, pvHostPathConfig, pvcConfig)
			framework.ExpectNoError(err, "Failed to create the requested storage resources")

			ginkgo.By(fmt.Sprintf("Listing all PVs with the labelSelector: %q", volLabel.AsSelector().String()))
			pvList, err := pvClient.List(ctx, metav1.ListOptions{LabelSelector: volLabel.AsSelector().String()})
			framework.ExpectNoError(err, "Failed to list PVs with the labelSelector: %q", volLabel.AsSelector().String())
			gomega.Expect(pvList.Items).To(gomega.HaveLen(1))
			initialPV := pvList.Items[0]

			ginkgo.By(fmt.Sprintf("Listing PVCs in namespace %q", ns))
			pvcList, err := pvcClient.List(ctx, metav1.ListOptions{})
			framework.ExpectNoError(err, "Failed to list PVCs with the labelSelector: %q", volLabel.AsSelector().String())
			gomega.Expect(pvcList.Items).To(gomega.HaveLen(1))
			initialPVC := pvcList.Items[0]

			ginkgo.By(fmt.Sprintf("Reading %q Status", initialPVC.Name))
			pvcResource := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "persistentvolumeclaims"}
			pvcUnstructured, err := f.DynamicClient.Resource(pvcResource).Namespace(ns).Get(ctx, initialPVC.Name, metav1.GetOptions{}, "status")
			framework.ExpectNoError(err, "Failed to fetch the status of PVC %s in namespace %s", initialPVC.Name, ns)
			retrievedPVC := &v1.PersistentVolumeClaim{}
			err = runtime.DefaultUnstructuredConverter.FromUnstructured(pvcUnstructured.UnstructuredContent(), &retrievedPVC)
			framework.ExpectNoError(err, "Failed to retrieve %q status.", initialPV.Name)
			gomega.Expect(string(retrievedPVC.Status.Phase)).To(gomega.Or(gomega.Equal("Pending"), gomega.Equal("Bound")), "Checking that the PVC status has been read")

			ginkgo.By(fmt.Sprintf("Reading %q Status", initialPV.Name))
			pvResource := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "persistentvolumes"}
			pvUnstructured, err := f.DynamicClient.Resource(pvResource).Get(ctx, initialPV.Name, metav1.GetOptions{}, "status")
			framework.ExpectNoError(err, "Failed to fetch the status of PV %s in namespace %s", initialPV.Name, ns)
			retrievedPV := &v1.PersistentVolume{}
			err = runtime.DefaultUnstructuredConverter.FromUnstructured(pvUnstructured.UnstructuredContent(), &retrievedPV)
			framework.ExpectNoError(err, "Failed to retrieve %q status.", initialPV.Name)
			gomega.Expect(string(retrievedPV.Status.Phase)).To(gomega.Or(gomega.Equal("Available"), gomega.Equal("Bound"), gomega.Equal("Pending")), "Checking that the PV status has been read")

			ginkgo.By(fmt.Sprintf("Patching %q Status", initialPVC.Name))
			payload := []byte(`{"status":{"conditions":[{"type":"StatusPatched","status":"True", "reason":"E2E patchedStatus", "message":"Set from e2e test"}]}}`)

			patchedPVC, err := pvcClient.Patch(ctx, initialPVC.Name, types.MergePatchType, payload, metav1.PatchOptions{}, "status")
			framework.ExpectNoError(err, "Failed to patch status.")

			gomega.Expect(patchedPVC.Status.Conditions).To(gstruct.MatchElements(conditionType, gstruct.IgnoreExtras, gstruct.Elements{
				"StatusPatched": gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
					"Message": gomega.ContainSubstring("Set from e2e test"),
					"Reason":  gomega.ContainSubstring("E2E patchedStatus"),
				}),
			}), "Checking that patched status has been applied")

			ginkgo.By(fmt.Sprintf("Patching %q Status", retrievedPV.Name))
			payload = []byte(`{"status":{"message": "StatusPatched", "reason": "E2E patchStatus"}}`)

			patchedPV, err := pvClient.Patch(ctx, retrievedPV.Name, types.MergePatchType, payload, metav1.PatchOptions{}, "status")
			framework.ExpectNoError(err, "Failed to patch %q status.", retrievedPV.Name)
			gomega.Expect(patchedPV.Status.Reason).To(gomega.Equal("E2E patchStatus"), "Checking that patched status has been applied")
			gomega.Expect(patchedPV.Status.Message).To(gomega.Equal("StatusPatched"), "Checking that patched status has been applied")

			ginkgo.By(fmt.Sprintf("Updating %q Status", patchedPVC.Name))
			var statusToUpdate, updatedPVC *v1.PersistentVolumeClaim

			err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
				statusToUpdate, err = pvcClient.Get(ctx, patchedPVC.Name, metav1.GetOptions{})
				framework.ExpectNoError(err, "Unable to retrieve pvc %s", patchedPVC.Name)

				statusToUpdate.Status.Conditions = append(statusToUpdate.Status.Conditions, v1.PersistentVolumeClaimCondition{
					Type:    "StatusUpdated",
					Status:  "True",
					Reason:  "E2E updateStatus",
					Message: "Set from e2e test",
				})

				updatedPVC, err = pvcClient.UpdateStatus(ctx, statusToUpdate, metav1.UpdateOptions{})
				return err
			})
			framework.ExpectNoError(err, "Failed to update status.")
			gomega.Expect(updatedPVC.Status.Conditions).To(gstruct.MatchElements(conditionType, gstruct.IgnoreExtras, gstruct.Elements{
				"StatusUpdated": gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
					"Message": gomega.ContainSubstring("Set from e2e test"),
					"Reason":  gomega.ContainSubstring("E2E updateStatus"),
				}),
			}), "Checking that updated status has been applied")

			ginkgo.By(fmt.Sprintf("Updating %q Status", patchedPV.Name))
			var pvToUpdate, updatedPV *v1.PersistentVolume

			err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
				pvToUpdate, err = pvClient.Get(ctx, patchedPV.Name, metav1.GetOptions{})
				framework.ExpectNoError(err, "Unable to retrieve pv %s", patchedPV.Name)

				pvToUpdate.Status.Reason = "E2E updateStatus"
				pvToUpdate.Status.Message = "StatusUpdated"
				updatedPV, err = pvClient.UpdateStatus(ctx, pvToUpdate, metav1.UpdateOptions{})
				return err
			})
			framework.ExpectNoError(err, "Failed to update status.")
			gomega.Expect(updatedPV.Status.Reason).To(gomega.Equal("E2E updateStatus"), "Checking that updated status has been applied")
			gomega.Expect(updatedPV.Status.Message).To(gomega.Equal("StatusUpdated"), "Checking that updated status has been applied")
		})
	})

	// testsuites/multivolume 测试现在可以在 windows 节点上运行
	// 这个测试与 windows 不兼容，因为默认 StorageClass
	// 没有 ntfs 参数，我们不能改变集群状态
	// 来添加一个兼容 windows 且同时是
	// 默认 StorageClass 的 StorageClass
	ginkgo.Describe("Default StorageClass [LinuxOnly]", func() {
		ginkgo.Context("pods that use multiple volumes", func() {

			ginkgo.AfterEach(func(ctx context.Context) {
				e2estatefulset.DeleteAllStatefulSets(ctx, c, ns)
			})

			f.It("should be reschedulable", f.WithSlow(), func(ctx context.Context) {
				// 只在有默认 storageclass 的 provider 上运行
				e2epv.SkipIfNoDefaultStorageClass(ctx, c)

				numVols := 4

				ginkgo.By("Creating a StatefulSet pod to initialize data")
				writeCmd := "true"
				for i := range numVols {
					writeCmd += fmt.Sprintf("&& touch %v", getVolumeFile(i))
				}
				writeCmd += "&& sleep 10000"

				probe := &v1.Probe{
					ProbeHandler: v1.ProbeHandler{
						Exec: &v1.ExecAction{
							// 检查最后一个文件已创建
							Command: []string{"test", "-f", getVolumeFile(numVols - 1)},
						},
					},
					InitialDelaySeconds: 1,
					PeriodSeconds:       1,
				}

				mounts := []v1.VolumeMount{}
				claims := []v1.PersistentVolumeClaim{}

				for i := range numVols {
					pvc := e2epv.MakePersistentVolumeClaim(e2epv.PersistentVolumeClaimConfig{}, ns)
					pvc.Name = getVolName(i)
					mounts = append(mounts, v1.VolumeMount{Name: pvc.Name, MountPath: getMountPath(i)})
					claims = append(claims, *pvc)
				}

				spec := makeStatefulSetWithPVCs(ns, writeCmd, mounts, claims, probe)
				ss, err := c.AppsV1().StatefulSets(ns).Create(ctx, spec, metav1.CreateOptions{})
				framework.ExpectNoError(err)
				e2estatefulset.WaitForRunningAndReady(ctx, c, 1, ss)

				ginkgo.By("Deleting the StatefulSet but not the volumes")
				// 先缩容到 0，让 Delete 更快
				ss, err = e2estatefulset.Scale(ctx, c, ss, 0)
				framework.ExpectNoError(err)
				e2estatefulset.WaitForStatusReplicas(ctx, c, ss, 0)
				err = c.AppsV1().StatefulSets(ns).Delete(ctx, ss.Name, metav1.DeleteOptions{})
				framework.ExpectNoError(err)

				ginkgo.By("Creating a new Statefulset and validating the data")
				validateCmd := "true"
				for i := range numVols {
					validateCmd += fmt.Sprintf("&& test -f %v", getVolumeFile(i))
				}
				validateCmd += "&& sleep 10000"

				spec = makeStatefulSetWithPVCs(ns, validateCmd, mounts, claims, probe)
				ss, err = c.AppsV1().StatefulSets(ns).Create(ctx, spec, metav1.CreateOptions{})
				framework.ExpectNoError(err)
				e2estatefulset.WaitForRunningAndReady(ctx, c, 1, ss)
			})
		})
	})
})

func getVolName(i int) string {
	return fmt.Sprintf("vol%v", i)
}

func getMountPath(i int) string {
	return fmt.Sprintf("/mnt/%v", getVolName(i))
}

func getVolumeFile(i int) string {
	return fmt.Sprintf("%v/data%v", getMountPath(i), i)
}

func makeStatefulSetWithPVCs(ns, cmd string, mounts []v1.VolumeMount, claims []v1.PersistentVolumeClaim, readyProbe *v1.Probe) *appsv1.StatefulSet {
	ssReplicas := int32(1)

	labels := map[string]string{"app": "many-volumes-test"}
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "many-volumes-test",
			Namespace: ns,
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "many-volumes-test"},
			},
			Replicas: &ssReplicas,
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:           "nginx",
							Image:          e2epod.GetTestImage(imageutils.Nginx),
							Command:        e2epod.GenerateScriptCmd(cmd),
							VolumeMounts:   mounts,
							ReadinessProbe: readyProbe,
						},
					},
				},
			},
			VolumeClaimTemplates: claims,
		},
	}
}

// createWaitAndDeletePod 创建测试 pod，等待（希望）成功，然后删除 pod。
// 注意：需要命名返回值，这样 defer 中的 err 赋值才能设置返回的错误。
//
// 已证明在 Go 1.7 中有必要这样做。
func createWaitAndDeletePod(ctx context.Context, c clientset.Interface, t *framework.TimeoutContext, ns string, pvc *v1.PersistentVolumeClaim, command string) (err error) {
	framework.Logf("Creating nfs test pod")
	pod := e2epod.MakePod(ns, nil, []*v1.PersistentVolumeClaim{pvc}, admissionapi.LevelPrivileged, command)
	runPod, err := c.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("pod Create API error: %w", err)
	}
	defer func() {
		delErr := e2epod.DeletePodWithWait(ctx, c, runPod)
		if err == nil { // don't override previous err value
			err = delErr // assign to returned err, can be nil
		}
	}()

	err = testPodSuccessOrFail(ctx, c, t, ns, runPod)
	if err != nil {
		return fmt.Errorf("pod %q did not exit with Success: %w", runPod.Name, err)
	}
	return // note: named return value
}

// testPodSuccessOrFail 测试 pod 的退出码是否为 0。
func testPodSuccessOrFail(ctx context.Context, c clientset.Interface, t *framework.TimeoutContext, ns string, pod *v1.Pod) error {
	framework.Logf("Pod should terminate with exitcode 0 (success)")
	if err := e2epod.WaitForPodSuccessInNamespaceTimeout(ctx, c, pod.Name, ns, t.PodStart); err != nil {
		return fmt.Errorf("pod %q failed to reach Success: %w", pod.Name, err)
	}
	framework.Logf("Pod %v succeeded ", pod.Name)
	return nil
}

func conditionType(condition interface{}) string {
	return string(condition.(v1.PersistentVolumeClaimCondition).Type)
}
