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

package pv

import (
	"context"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	"k8s.io/kubernetes/test/e2e/storage/utils"

	"github.com/onsi/ginkgo/v2"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
)

const (
	pdRetryTimeout  = 5 * time.Minute
	pdRetryPollTime = 5 * time.Second

	// VolumeSelectorKey 是 volume selector 的 key。
	VolumeSelectorKey = "e2e-pv-pool"

	// volumeGidAnnotationKey 是 PersistentVolume 对象上指定 supplemental GID 的 annotation key。
	// 复制自 k8s.io/kubernetes/pkg/volume/util 的 VolumeGidAnnotationKey
	volumeGidAnnotationKey = "pv.beta.kubernetes.io/gid"
)

var (
	// SELinuxLabel 是通用的 selinux label。
	SELinuxLabel = &v1.SELinuxOptions{
		Level: "s0:c0,c1"}
)

type pvval struct{}

// PVMap 保存 multi pv-pvc 测试中用到的所有 PV。key 是 PV 名称，保证唯一。
// value 是 {}（空结构体），因为我们只关心 PV 名称及其是否存在。
// 引用 PV 的任何字段（如 ClaimRef）之前，都必须先 Get 该 PV 对象。
type PVMap map[string]pvval

type pvcval struct{}

// PVCMap 保存 multi pv-pvc 测试中用到的所有 PVC。key 是 "namespace/pvc.Name"。
// value 是 {}（空结构体），因为我们只关心 PVC 名称及其是否存在。
// 引用 PVC 的任何字段（如 VolumeName）之前，都必须先 Get 该 PVC 对象。
// 注意：在循环中向 map 添加 key 是不安全的。map 中的插入顺序不可预测，
// 可能导致同一个 key 被重复遍历。
type PVCMap map[types.NamespacedName]pvcval

// MakePersistentVolume() 使用 PersistentVolumeConfig 为不同的存储选项（NFS、ceph 等）生成 PV 对象。
// (+optional) prebind 保存一个预先绑定的 PVC
// pvSource 示例：
//
//	pvSource: api.PersistentVolumeSource{
//		NFS: &api.NFSVolumeSource{
//	 		...
//	 	},
//	 }
type PersistentVolumeConfig struct {
	// [Optional] NamePrefix 未设置时默认为 "pv-"
	NamePrefix string
	// [Optional] Labels 包含用于组织和分类对象的信息
	Labels labels.Set
	// [Optional] Annotations 包含用于组织和分类对象的信息
	Annotations map[string]string
	// PVSource 包含底层 volume 的详细信息，必须设置
	PVSource v1.PersistentVolumeSource
	// [Optional] Prebind 用于指定在创建前要与此 PV 绑定的 PVC
	Prebind *v1.PersistentVolumeClaim
	// [Optiona] ReclaimPolicy 未设置时默认为 "Reclaim"
	ReclaimPolicy    v1.PersistentVolumeReclaimPolicy
	StorageClassName string
	// [Optional] NodeAffinity 定义约束，限制可以从哪些节点访问该 volume。
	NodeAffinity *v1.VolumeNodeAffinity
	// [Optional] VolumeMode 未设置时默认为 "Filesystem"
	VolumeMode *v1.PersistentVolumeMode
	// [Optional] AccessModes 未设置时默认为 RWO
	AccessModes []v1.PersistentVolumeAccessMode
	// [Optional] Capacity 是 Quantity 格式的存储容量。未设置时默认为 "2Gi"
	Capacity string
}

// MakePersistentVolumeClaim() 使用 PersistentVolumeClaimConfig 生成 PVC 对象。
type PersistentVolumeClaimConfig struct {
	// PVC 名称。设置后会覆盖 NamePrefix
	Name string
	// NamePrefix 未指定时默认为 "pvc-"
	NamePrefix string
	// ClaimSize 必须使用 Quantity 格式。未指定时默认为 2Gi
	ClaimSize string
	// AccessModes 未指定时默认为 RWO
	AccessModes               []v1.PersistentVolumeAccessMode
	Annotations               map[string]string
	Selector                  *metav1.LabelSelector
	StorageClassName          *string
	VolumeAttributesClassName *string
	// VolumeMode 未指定或指定为空字符串时默认为 nil
	VolumeMode *v1.PersistentVolumeMode
}

// PVPVCCleanup 清理单个 pv/pvc 测试用例中的 pv 和 pvc。
// 注意：删除错误会追加到 []error，以便我们同时尝试删除 pvc 和 pv。
func PVPVCCleanup(ctx context.Context, c clientset.Interface, ns string, pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim) []error {
	var errs []error

	if pvc != nil {
		err := DeletePersistentVolumeClaim(ctx, c, pvc.Name, ns)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to delete PVC %q: %w", pvc.Name, err))
		}
	} else {
		framework.Logf("pvc is nil")
	}
	if pv != nil {
		err := DeletePersistentVolume(ctx, c, pv.Name)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to delete PV %q: %w", pv.Name, err))
		}
	} else {
		framework.Logf("pv is nil")
	}
	return errs
}

// PVPVCMapCleanup 清理 multi-pv-pvc 测试用例中的 pv 和 pvc。只要 Delete API 调用成功，
// pv 和 pvc map 中的对应条目就会被删除。
// 注意：删除错误会追加到 []error，以便尽可能多地删除 pvc 和 pv。
func PVPVCMapCleanup(ctx context.Context, c clientset.Interface, ns string, pvols PVMap, claims PVCMap) []error {
	var errs []error

	for pvcKey := range claims {
		err := DeletePersistentVolumeClaim(ctx, c, pvcKey.Name, ns)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to delete PVC %q: %w", pvcKey.Name, err))
		} else {
			delete(claims, pvcKey)
		}
	}

	for pvKey := range pvols {
		err := DeletePersistentVolume(ctx, c, pvKey)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to delete PV %q: %w", pvKey, err))
		} else {
			delete(pvols, pvKey)
		}
	}
	return errs
}

// DeletePersistentVolume 删除 PV。
func DeletePersistentVolume(ctx context.Context, c clientset.Interface, pvName string) error {
	if c != nil && len(pvName) > 0 {
		framework.Logf("Deleting PersistentVolume %q", pvName)
		err := c.CoreV1().PersistentVolumes().Delete(ctx, pvName, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("PV Delete API error: %w", err)
		}
	}
	return nil
}

// DeletePersistentVolumeClaim 删除 Claim。
func DeletePersistentVolumeClaim(ctx context.Context, c clientset.Interface, pvcName string, ns string) error {
	if c != nil && len(pvcName) > 0 {
		framework.Logf("Deleting PersistentVolumeClaim %q", pvcName)
		err := c.CoreV1().PersistentVolumeClaims(ns).Delete(ctx, pvcName, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("PVC Delete API error: %w", err)
		}
	}
	return nil
}

// DeletePVCandValidatePV 删除 PVC，等待 PV 进入预期阶段，并校验 PV 已被回收
// （这里假设了 reclaimPolicy）。调用方指定与待删除 PVC 绑定的 PV 应进入哪个阶段。
func DeletePVCandValidatePV(ctx context.Context, c clientset.Interface, timeouts *framework.TimeoutContext, ns string, pvc *v1.PersistentVolumeClaim, pv *v1.PersistentVolume, expectPVPhase v1.PersistentVolumePhase) error {
	pvname := pvc.Spec.VolumeName
	framework.Logf("Deleting PVC %v to trigger reclamation of PV %v", pvc.Name, pvname)
	err := DeletePersistentVolumeClaim(ctx, c, pvc.Name, ns)
	if err != nil {
		return err
	}

	// 等待 PV 的阶段回到 `expectPVPhase`
	framework.Logf("Waiting for reclaim process to complete.")
	err = WaitForPersistentVolumePhase(ctx, expectPVPhase, c, pv.Name, framework.Poll, timeouts.PVReclaim)
	if err != nil {
		return fmt.Errorf("pv %q phase did not become %v: %w", pv.Name, expectPVPhase, err)
	}

	// 检查 PV 的 ClaimRef 和 UID，与预期值比较
	pv, err = c.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("PV Get API error: %w", err)
	}
	cr := pv.Spec.ClaimRef
	if expectPVPhase == v1.VolumeAvailable {
		if cr != nil && len(cr.UID) > 0 {
			return fmt.Errorf("PV is 'Available' but ClaimRef.UID is not empty")
		}
	} else if expectPVPhase == v1.VolumeBound {
		if cr == nil {
			return fmt.Errorf("PV is 'Bound' but ClaimRef is nil")
		}
		if len(cr.UID) == 0 {
			return fmt.Errorf("PV is 'Bound' but ClaimRef.UID is empty")
		}
	}

	framework.Logf("PV %v now in %q phase", pv.Name, expectPVPhase)
	return nil
}

// DeletePVCandValidatePVGroup 封装 deletePVCandValidatePV()，在 PV map 上循环调用。只删除已绑定的 PV。
// 校验 PVC 已被删除，且 PV 处于预期阶段（Released、Available、Bound）。
// 注意：如果 PVC 多于 PV，剩余的部分 PVC 可能会绑定到刚变为 Available 的 PV。
func DeletePVCandValidatePVGroup(ctx context.Context, c clientset.Interface, timeouts *framework.TimeoutContext, ns string, pvols PVMap, claims PVCMap, expectPVPhase v1.PersistentVolumePhase) error {
	var boundPVs, deletedPVCs int

	for pvName := range pvols {
		pv, err := c.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("PV Get API error: %w", err)
		}
		cr := pv.Spec.ClaimRef
		// 如果 PV 已绑定，就删除它绑定的 PVC
		if cr != nil && len(cr.Name) > 0 {
			boundPVs++
			// 断言已绑定的 PVC 在本测试中有记录。断言失败可能意味着
			// 有外部 PVC 干扰了测试。
			pvcKey := makePvcKey(ns, cr.Name)
			if _, found := claims[pvcKey]; !found {
				return fmt.Errorf("internal: claims map is missing pvc %q", pvcKey)
			}
			// 获取 PVC，供下面的删除调用使用
			pvc, err := c.CoreV1().PersistentVolumeClaims(ns).Get(ctx, cr.Name, metav1.GetOptions{})
			if err == nil {
				if err = DeletePVCandValidatePV(ctx, c, timeouts, ns, pvc, pv, expectPVPhase); err != nil {
					return err
				}
			} else if !apierrors.IsNotFound(err) {
				return fmt.Errorf("PVC Get API error: %w", err)
			}
			// 即使上面的 apierrors.IsNotFound 为 true（即 PVC 实际上并非在此处删除），
			// 也从 map 中删除 pvckey
			delete(claims, pvcKey)
			deletedPVCs++
		}
	}
	if boundPVs != deletedPVCs {
		return fmt.Errorf("expect number of bound PVs (%v) to equal number of deleted PVCs (%v)", boundPVs, deletedPVCs)
	}
	return nil
}

// 创建 PV 资源。出错时测试失败。
func createPV(ctx context.Context, c clientset.Interface, timeouts *framework.TimeoutContext, pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	var resultPV *v1.PersistentVolume
	var lastCreateErr error
	err := wait.PollUntilContextTimeout(ctx, 29*time.Second, timeouts.PVCreate, true, func(ctx context.Context) (done bool, err error) {
		resultPV, lastCreateErr = c.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
		if lastCreateErr != nil {
			// 如果遇到配额问题，说明还没完成，应该重试。这是 GCP 的配额失败字符串。
			// 如果找到其他平台的配额失败字符串，可以加进来，以提高在同一云账号下
			// 运行大量并行测试任务时的可靠性。这类似于 controller 的行为，也是我们
			// 推荐一般客户端采用的作法。
			if strings.Contains(lastCreateErr.Error(), `googleapi: Error 403: Quota exceeded for quota group`) {
				return false, nil
			}

			// 如果不是配额失败，就立即失败
			return false, lastCreateErr
		}

		return true, nil
	})
	// 如果创建 PV 时出错，优先使用该错误而不是超时错误
	if lastCreateErr != nil {
		return nil, fmt.Errorf("PV Create API error: %w", err)
	}
	if err != nil {
		return nil, fmt.Errorf("PV Create API error: %w", err)
	}

	return resultPV, nil
}

// CreatePV 创建 PV 资源。出错时测试失败。
func CreatePV(ctx context.Context, c clientset.Interface, timeouts *framework.TimeoutContext, pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	return createPV(ctx, c, timeouts, pv)
}

// CreatePVC 创建 PVC 资源。出错时测试失败。
func CreatePVC(ctx context.Context, c clientset.Interface, ns string, pvc *v1.PersistentVolumeClaim) (*v1.PersistentVolumeClaim, error) {
	pvc, err := c.CoreV1().PersistentVolumeClaims(ns).Create(ctx, pvc, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("PVC Create API error: %w", err)
	}
	return pvc, nil
}

// CreatePVCPV 根据传入的 nfs-server IP 和 namespace，先创建 PVC 再创建 PV。
// 如果 "preBind" 为 true，则通过 PV 的 ClaimRef 将 PV 预先绑定到 PVC。
// 返回创建出的 pv 和 pvc。
// 注意：在预绑定场景中，PVC 的真实名称是在实例化后才生成的，所以要先创建 PVC。
func CreatePVCPV(ctx context.Context, c clientset.Interface, timeouts *framework.TimeoutContext, pvConfig PersistentVolumeConfig, pvcConfig PersistentVolumeClaimConfig, ns string, preBind bool) (*v1.PersistentVolume, *v1.PersistentVolumeClaim, error) {
	// 构造 PVC spec
	pvc := MakePersistentVolumeClaim(pvcConfig, ns)
	preBindMsg := ""
	if preBind {
		preBindMsg = " pre-bound"
		pvConfig.Prebind = pvc
	}
	// 构造 PV spec
	pv := MakePersistentVolume(pvConfig)

	ginkgo.By(fmt.Sprintf("Creating a PVC followed by a%s PV", preBindMsg))
	pvc, err := CreatePVC(ctx, c, ns, pvc)
	if err != nil {
		return nil, nil, err
	}

	// 实例化 PV，必要时通过 ClaimRef 处理预绑定
	if preBind {
		pv.Spec.ClaimRef.Name = pvc.Name
	}
	pv, err = createPV(ctx, c, timeouts, pv)
	if err != nil {
		return nil, pvc, err
	}
	return pv, pvc, nil
}

// CreatePVPVC 根据传入的 nfs-server IP 和 namespace，先创建 PV 再创建 PVC。
// 如果 "preBind" 为 true，则通过 PVC 的 VolumeName 将 PVC 预先绑定到 PV。
// 返回创建出的 pv 和 pvc。
// 注意：在预绑定场景中，PV 的真实名称是在实例化后才生成的，所以要先创建 PV。
func CreatePVPVC(ctx context.Context, c clientset.Interface, timeouts *framework.TimeoutContext, pvConfig PersistentVolumeConfig, pvcConfig PersistentVolumeClaimConfig, ns string, preBind bool) (*v1.PersistentVolume, *v1.PersistentVolumeClaim, error) {
	preBindMsg := ""
	if preBind {
		preBindMsg = " pre-bound"
	}
	framework.Logf("Creating a PV followed by a%s PVC", preBindMsg)

	// 构造 PV 和 PVC 定义
	pv := MakePersistentVolume(pvConfig)
	pvc := MakePersistentVolumeClaim(pvcConfig, ns)

	// 实例化 PV
	pv, err := createPV(ctx, c, timeouts, pv)
	if err != nil {
		return nil, nil, err
	}
	// 实例化 PVC，必要时通过 VolumeName 处理预绑定
	if preBind {
		pvc.Spec.VolumeName = pv.Name
	}
	pvc, err = CreatePVC(ctx, c, ns, pvc)
	if err != nil {
		return pv, nil, err
	}
	return pv, pvc, nil
}

// CreatePVsPVCs 创建指定数量的 PV 和 PVC，并分别放到两个 map 中返回。如果 PV 数与 PVC 数不相等，
// 预计绑定的 PV 数为两者的较小值。如果创建过程中出错，返回的 map 中仍可能包含已成功创建的资源条目。
// 也就是说，调用方看到返回的错误时，需要自行决定如何处理 map 中的条目。
// 注意：测试套件删除 namespace 时，孤立的 PVC 和 pod 会被删除，但孤立的 PV 不会被删除，
// 在测试套件结束后仍会残留。
func CreatePVsPVCs(ctx context.Context, numpvs, numpvcs int, c clientset.Interface, timeouts *framework.TimeoutContext, ns string, pvConfig PersistentVolumeConfig, pvcConfig PersistentVolumeClaimConfig) (PVMap, PVCMap, error) {
	pvMap := make(PVMap, numpvs)
	pvcMap := make(PVCMap, numpvcs)
	extraPVCs := 0
	extraPVs := numpvs - numpvcs
	if extraPVs < 0 {
		extraPVCs = -extraPVs
		extraPVs = 0
	}
	pvsToCreate := numpvs - extraPVs // 取 min(numpvs, numpvcs)

	// 创建 PV 和 PVC
	for range pvsToCreate {
		pv, pvc, err := CreatePVPVC(ctx, c, timeouts, pvConfig, pvcConfig, ns, false)
		if err != nil {
			return pvMap, pvcMap, err
		}
		pvMap[pv.Name] = pvval{}
		pvcMap[makePvcKey(ns, pvc.Name)] = pvcval{}
	}

	// 按需创建额外的 PV 或 PVC
	for i := 0; i < extraPVs; i++ {
		pv := MakePersistentVolume(pvConfig)
		pv, err := createPV(ctx, c, timeouts, pv)
		if err != nil {
			return pvMap, pvcMap, err
		}
		pvMap[pv.Name] = pvval{}
	}
	for i := 0; i < extraPVCs; i++ {
		pvc := MakePersistentVolumeClaim(pvcConfig, ns)
		pvc, err := CreatePVC(ctx, c, ns, pvc)
		if err != nil {
			return pvMap, pvcMap, err
		}
		pvcMap[makePvcKey(ns, pvc.Name)] = pvcval{}
	}
	return pvMap, pvcMap, nil
}

// WaitOnPVandPVC 等待 PV 和 PVC 相互绑定。
func WaitOnPVandPVC(ctx context.Context, c clientset.Interface, timeouts *framework.TimeoutContext, ns string, pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim) error {
	// 等待新建的 PVC 与 PV 绑定
	framework.Logf("Waiting for PV %v to bind to PVC %v", pv.Name, pvc.Name)
	err := WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, c, ns, pvc.Name, framework.Poll, timeouts.ClaimBound)
	if err != nil {
		return fmt.Errorf("PVC %q did not become Bound: %w", pvc.Name, err)
	}

	// 等待 PersistentVolume.Status.Phase 变为 Bound。既然 PVC 已经绑定，
	// PV 理应如此。
	err = WaitForPersistentVolumePhase(ctx, v1.VolumeBound, c, pv.Name, framework.Poll, timeouts.PVBound)
	if err != nil {
		return fmt.Errorf("PV %q did not become Bound: %w", pv.Name, err)
	}

	// 重新获取 PV 和 PVC 对象
	pv, err = c.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("PV Get API error: %w", err)
	}
	pvc, err = c.CoreV1().PersistentVolumeClaims(ns).Get(ctx, pvc.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("PVC Get API error: %w", err)
	}

	// PV 和 PVC 都已绑定，但是否互相绑定？
	// 检查 PersistentVolume.ClaimRef 是否与 PVC 匹配
	if pv.Spec.ClaimRef == nil {
		return fmt.Errorf("PV %q ClaimRef is nil", pv.Name)
	}
	if pv.Spec.ClaimRef.Name != pvc.Name {
		return fmt.Errorf("PV %q ClaimRef's name (%q) should be %q", pv.Name, pv.Spec.ClaimRef.Name, pvc.Name)
	}
	if pvc.Spec.VolumeName != pv.Name {
		return fmt.Errorf("PVC %q VolumeName (%q) should be %q", pvc.Name, pvc.Spec.VolumeName, pv.Name)
	}
	if pv.Spec.ClaimRef.UID != pvc.UID {
		return fmt.Errorf("PV %q ClaimRef's UID (%q) should be %q", pv.Name, pv.Spec.ClaimRef.UID, pvc.UID)
	}
	return nil
}

// WaitAndVerifyBinds 通过检查 pvols 中非 nil 的 claimRef 来查找已绑定的 PV 和 PVC。
// NOTE: 每次迭代中，每个 PV 最多等待 3 分钟；如果 PV 已绑定，PVC 最多再等待 3 分钟。
// 当 PV 数与 PVC 数不相等时，可能出现连续多次达到最大等待时间的情况，
// 从而延长测试时长。因此建议让 PV 与 PVC 的数量差尽量小。
func WaitAndVerifyBinds(ctx context.Context, c clientset.Interface, timeouts *framework.TimeoutContext, ns string, pvols PVMap, claims PVCMap, testExpected bool) error {
	var actualBinds int
	expectedBinds := len(pvols)
	if expectedBinds > len(claims) { // 取 PV 数与 PVC 数的较小值
		expectedBinds = len(claims)
	}

	for pvName := range pvols {
		err := WaitForPersistentVolumePhase(ctx, v1.VolumeBound, c, pvName, framework.Poll, timeouts.PVBound)
		if err != nil && len(pvols) > len(claims) {
			framework.Logf("WARN: pv %v is not bound after max wait", pvName)
			framework.Logf("      This may be ok since there are more pvs than pvcs")
			continue
		}
		if err != nil {
			return fmt.Errorf("PV %q did not become Bound: %w", pvName, err)
		}

		pv, err := c.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("PV Get API error: %w", err)
		}
		cr := pv.Spec.ClaimRef
		if cr != nil && len(cr.Name) > 0 {
			// 断言已绑定的 PVC 是测试资源。断言失败可能意味着
			// 有非测试用的 PVC 干扰，或测试本身存在 bug
			pvcKey := makePvcKey(ns, cr.Name)
			if _, found := claims[pvcKey]; !found {
				return fmt.Errorf("internal: claims map is missing pvc %q", pvcKey)
			}

			err := WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, c, ns, cr.Name, framework.Poll, timeouts.ClaimBound)
			if err != nil {
				return fmt.Errorf("PVC %q did not become Bound: %w", cr.Name, err)
			}
			actualBinds++
		}
	}

	if testExpected && actualBinds != expectedBinds {
		return fmt.Errorf("expect number of bound PVs (%v) to equal number of claims (%v)", actualBinds, expectedBinds)
	}
	return nil
}

// 返回 pvckey 结构体。
func makePvcKey(ns, name string) types.NamespacedName {
	return types.NamespacedName{Namespace: ns, Name: name}
}

// MakePersistentVolume 根据 nfs server IP 返回 PV 定义。如果 PVC 非 nil，
// 则 PV 会带上一个包含 PVC namespace 的 ClaimRef；如果 PVC 为 nil，则不设置 ClaimRef。
// 如果未指定 reclaimPolicy，则默认 "Retain"。Spec 应与测试的 PVC 匹配。
// 注意：传入的 claim 在创建前没有名称，因此本函数无法完整填充 PV 的 ClaimRef。
// ClaimRef 的名称会在 CreatePVCPV 中后续补上。
func MakePersistentVolume(pvConfig PersistentVolumeConfig) *v1.PersistentVolume {
	var claimRef *v1.ObjectReference

	if len(pvConfig.AccessModes) == 0 {
		pvConfig.AccessModes = append(pvConfig.AccessModes, v1.ReadWriteOnce)
	}

	if len(pvConfig.NamePrefix) == 0 {
		pvConfig.NamePrefix = "pv-"
	}

	if pvConfig.ReclaimPolicy == "" {
		pvConfig.ReclaimPolicy = v1.PersistentVolumeReclaimRetain
	}

	if len(pvConfig.Capacity) == 0 {
		pvConfig.Capacity = "2Gi"
	}

	if pvConfig.Prebind != nil {
		claimRef = &v1.ObjectReference{
			Kind:       "PersistentVolumeClaim",
			APIVersion: "v1",
			Name:       pvConfig.Prebind.Name,
			Namespace:  pvConfig.Prebind.Namespace,
			UID:        pvConfig.Prebind.UID,
		}
	}

	annotations := map[string]string{
		volumeGidAnnotationKey: "777",
	}
	for k, v := range pvConfig.Annotations {
		annotations[k] = v
	}

	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: pvConfig.NamePrefix,
			Labels:       pvConfig.Labels,
			Annotations:  annotations,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: pvConfig.ReclaimPolicy,
			Capacity: v1.ResourceList{
				v1.ResourceStorage: resource.MustParse(pvConfig.Capacity),
			},
			PersistentVolumeSource: pvConfig.PVSource,
			AccessModes:            pvConfig.AccessModes,
			ClaimRef:               claimRef,
			StorageClassName:       pvConfig.StorageClassName,
			NodeAffinity:           pvConfig.NodeAffinity,
			VolumeMode:             pvConfig.VolumeMode,
		},
	}
}

// MakePersistentVolumeClaim 根据 PersistentVolumeClaimConfig 返回 PVC API 对象。
func MakePersistentVolumeClaim(cfg PersistentVolumeClaimConfig, ns string) *v1.PersistentVolumeClaim {

	if len(cfg.AccessModes) == 0 {
		cfg.AccessModes = append(cfg.AccessModes, v1.ReadWriteOnce)
	}

	if len(cfg.ClaimSize) == 0 {
		cfg.ClaimSize = "2Gi"
	}

	if len(cfg.NamePrefix) == 0 {
		cfg.NamePrefix = "pvc-"
	}

	if cfg.VolumeMode != nil && *cfg.VolumeMode == "" {
		framework.Logf("Warning: Making PVC: VolumeMode specified as invalid empty string, treating as nil")
		cfg.VolumeMode = nil
	}

	return &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:         cfg.Name,
			GenerateName: cfg.NamePrefix,
			Namespace:    ns,
			Annotations:  cfg.Annotations,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			Selector:    cfg.Selector,
			AccessModes: cfg.AccessModes,
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: resource.MustParse(cfg.ClaimSize),
				},
			},
			StorageClassName:          cfg.StorageClassName,
			VolumeAttributesClassName: cfg.VolumeAttributesClassName,
			VolumeMode:                cfg.VolumeMode,
		},
	}
}

func createPDWithRetry(ctx context.Context, zone string) (string, error) {
	var err error
	var newDiskName string
	for start := time.Now(); ; time.Sleep(pdRetryPollTime) {
		if time.Since(start) >= pdRetryTimeout ||
			ctx.Err() != nil {
			return "", fmt.Errorf("timed out while trying to create PD in zone %q, last error: %w", zone, err)
		}

		newDiskName, err = createPD(zone)
		if err != nil {
			framework.Logf("Couldn't create a new PD in zone %q, sleeping 5 seconds: %v", zone, err)
			continue
		}
		framework.Logf("Successfully created a new PD in zone %q: %q.", zone, newDiskName)
		return newDiskName, nil
	}
}

func CreateShare() (string, string, string, error) {
	return framework.TestContext.CloudConfig.Provider.CreateShare()
}

func DeleteShare(accountName, shareName string) error {
	return framework.TestContext.CloudConfig.Provider.DeleteShare(accountName, shareName)
}

// CreatePDWithRetry 带重试地创建 PD。
func CreatePDWithRetry(ctx context.Context) (string, error) {
	return createPDWithRetry(ctx, "")
}

// CreatePDWithRetryAndZone 带重试地在指定 zone 创建 PD。
func CreatePDWithRetryAndZone(ctx context.Context, zone string) (string, error) {
	return createPDWithRetry(ctx, zone)
}

// DeletePDWithRetry 带重试地删除 PD。
func DeletePDWithRetry(ctx context.Context, diskName string) error {
	var err error
	for start := time.Now(); ; time.Sleep(pdRetryPollTime) {
		if time.Since(start) >= pdRetryTimeout ||
			ctx.Err() != nil {
			return fmt.Errorf("timed out while trying to delete PD %q, last error: %w", diskName, err)
		}
		err = deletePD(diskName)
		if err != nil {
			framework.Logf("Couldn't delete PD %q, sleeping %v: %v", diskName, pdRetryPollTime, err)
			continue
		}
		framework.Logf("Successfully deleted PD %q.", diskName)
		return nil
	}
}

func createPD(zone string) (string, error) {
	if zone == "" {
		zone = framework.TestContext.CloudConfig.Zone
	}
	return framework.TestContext.CloudConfig.Provider.CreatePD(zone)
}

func deletePD(pdName string) error {
	return framework.TestContext.CloudConfig.Provider.DeletePD(pdName)
}

// WaitForPVClaimBoundPhase 等待所有 PVC 的阶段变为 Bound
func WaitForPVClaimBoundPhase(ctx context.Context, client clientset.Interface, pvclaims []*v1.PersistentVolumeClaim, timeout time.Duration) ([]*v1.PersistentVolume, error) {
	persistentvolumes := make([]*v1.PersistentVolume, len(pvclaims))

	for index, claim := range pvclaims {
		err := WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, client, claim.Namespace, claim.Name, framework.Poll, timeout)
		if err != nil {
			return persistentvolumes, err
		}
		// 获取 PVC 的最新副本
		claim, err = client.CoreV1().PersistentVolumeClaims(claim.Namespace).Get(ctx, claim.Name, metav1.GetOptions{})
		if err != nil {
			return persistentvolumes, fmt.Errorf("PVC Get API error: %w", err)
		}
		// 获取已绑定的 PV
		persistentvolumes[index], err = client.CoreV1().PersistentVolumes().Get(ctx, claim.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			return persistentvolumes, fmt.Errorf("PV Get API error: %w", err)
		}
	}
	return persistentvolumes, nil
}

// WaitForPersistentVolumePhase 等待 PersistentVolume 进入指定阶段，或等待超时，以先发生者为准。
func WaitForPersistentVolumePhase(ctx context.Context, phase v1.PersistentVolumePhase, c clientset.Interface, pvName string, poll, timeout time.Duration) error {
	framework.Logf("Waiting up to %v for PersistentVolume %s to have phase %s", timeout, pvName, phase)
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(poll) {
		pv, err := c.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
		if err != nil {
			framework.Logf("Get persistent volume %s in failed, ignoring for %v: %v", pvName, poll, err)
			continue
		}
		if pv.Status.Phase == phase {
			framework.Logf("PersistentVolume %s found and phase=%s (%v)", pvName, phase, time.Since(start))
			return nil
		}
		framework.Logf("PersistentVolume %s found but phase is %s instead of %s.", pvName, pv.Status.Phase, phase)
	}
	return fmt.Errorf("PersistentVolume %s not in phase %s within %v", pvName, phase, timeout)
}

// WaitForPersistentVolumeClaimPhase 等待 PersistentVolumeClaim 进入指定阶段，或等待超时，以先发生者为准。
func WaitForPersistentVolumeClaimPhase(ctx context.Context, phase v1.PersistentVolumeClaimPhase, c clientset.Interface, ns string, pvcName string, poll, timeout time.Duration) error {
	return WaitForPersistentVolumeClaimsPhase(ctx, phase, c, ns, []string{pvcName}, poll, timeout, true)
}

// WaitForPersistentVolumeClaimsPhase 等待任意一个（matchAny 为 true）或全部（matchAny 为 false）
// PersistentVolumeClaim 进入指定阶段，或等待超时，以先发生者为准。
func WaitForPersistentVolumeClaimsPhase(ctx context.Context, phase v1.PersistentVolumeClaimPhase, c clientset.Interface, ns string, pvcNames []string, poll, timeout time.Duration, matchAny bool) error {
	if len(pvcNames) == 0 {
		return fmt.Errorf("Incorrect parameter: Need at least one PVC to track. Found 0")
	}
	framework.Logf("Waiting up to timeout=%v for PersistentVolumeClaims %v to have phase %s", timeout, pvcNames, phase)
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(poll) {
		phaseFoundInAllClaims := true
		for _, pvcName := range pvcNames {
			pvc, err := c.CoreV1().PersistentVolumeClaims(ns).Get(ctx, pvcName, metav1.GetOptions{})
			if err != nil {
				framework.Logf("Failed to get claim %q, retrying in %v. Error: %v", pvcName, poll, err)
				phaseFoundInAllClaims = false
				break
			}
			if pvc.Status.Phase == phase {
				framework.Logf("PersistentVolumeClaim %s found and phase=%s (%v)", pvcName, phase, time.Since(start))
				if matchAny {
					return nil
				}
			} else {
				framework.Logf("PersistentVolumeClaim %s found but phase is %s instead of %s.", pvcName, pvc.Status.Phase, phase)
				phaseFoundInAllClaims = false
			}
		}
		if phaseFoundInAllClaims {
			return nil
		}
	}
	return fmt.Errorf("PersistentVolumeClaims %v not all in phase %s within %v", pvcNames, phase, timeout)
}

// CreatePVSource 创建 PV source。
func CreatePVSource(ctx context.Context, zone string) (*v1.PersistentVolumeSource, error) {
	diskName, err := CreatePDWithRetryAndZone(ctx, zone)
	if err != nil {
		return nil, err
	}
	return framework.TestContext.CloudConfig.Provider.CreatePVSource(ctx, zone, diskName)
}

// DeletePVSource 删除 PV source。
func DeletePVSource(ctx context.Context, pvSource *v1.PersistentVolumeSource) error {
	return framework.TestContext.CloudConfig.Provider.DeletePVSource(ctx, pvSource)
}

// GetDefaultStorageClassName 返回默认 storageClass，或返回错误
func GetDefaultStorageClassName(ctx context.Context, c clientset.Interface) (string, error) {
	list, err := c.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("Error listing storage classes: %w", err)
	}
	var scName string
	for _, sc := range list.Items {
		if util.IsDefaultAnnotation(sc.ObjectMeta) {
			if len(scName) != 0 {
				return "", fmt.Errorf("Multiple default storage classes found: %q and %q", scName, sc.Name)
			}
			scName = sc.Name
		}
	}
	if len(scName) == 0 {
		return "", fmt.Errorf("No default storage class found")
	}
	framework.Logf("Default storage class: %q", scName)
	return scName, nil
}

// SkipIfNoDefaultStorageClass 在找不到默认 SC 时跳过测试。
func SkipIfNoDefaultStorageClass(ctx context.Context, c clientset.Interface) {
	_, err := GetDefaultStorageClassName(ctx, c)
	if err != nil {
		e2eskipper.Skipf("error finding default storageClass : %v", err)
	}
}

// WaitForPersistentVolumeDeleted 等待 PersistentVolume 被删除，或等待超时，以先发生者为准。
func WaitForPersistentVolumeDeleted(ctx context.Context, c clientset.Interface, pvName string, poll, timeout time.Duration) error {
	framework.Logf("Waiting up to %v for PersistentVolume %s to get deleted", timeout, pvName)
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(poll) {
		pv, err := c.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
		if err == nil {
			framework.Logf("PersistentVolume %s found and phase=%s (%v)", pvName, pv.Status.Phase, time.Since(start))
			continue
		}
		if apierrors.IsNotFound(err) {
			framework.Logf("PersistentVolume %s was removed", pvName)
			return nil
		}
		framework.Logf("Get persistent volume %s in failed, ignoring for %v: %v", pvName, poll, err)
	}
	return fmt.Errorf("PersistentVolume %s still exists within %v", pvName, timeout)
}

// WaitForPVCFinalizer 等待指定 namespace 中的 PVC 被添加 finalizer。
func WaitForPVCFinalizer(ctx context.Context, cs clientset.Interface, name, namespace, finalizer string, poll, timeout time.Duration) error {
	var (
		err error
		pvc *v1.PersistentVolumeClaim
	)
	framework.Logf("Waiting up to %v for PersistentVolumeClaim %s/%s to contain finalizer %s", timeout, namespace, name, finalizer)
	if successful := utils.WaitUntil(poll, timeout, func() bool {
		pvc, err = cs.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			framework.Logf("Failed to get PersistentVolumeClaim %s/%s with err: %v. Will retry in %v", name, namespace, err, timeout)
			return false
		}
		for _, f := range pvc.Finalizers {
			if f == finalizer {
				return true
			}
		}
		return false
	}); successful {
		return nil
	}
	if err == nil {
		err = fmt.Errorf("finalizer %s not added to pvc %s/%s", finalizer, namespace, name)
	}
	return err
}

// GetDefaultFSType 返回默认 fsType
func GetDefaultFSType() string {
	if framework.NodeOSDistroIs("windows") {
		return "ntfs"
	}
	return "ext4"
}
