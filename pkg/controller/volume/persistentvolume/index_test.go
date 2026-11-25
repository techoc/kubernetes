/*
Copyright 2014 The Kubernetes Authors.

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

package persistentvolume

import (
	"sort"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	ref "k8s.io/client-go/tools/reference"
	"k8s.io/component-helpers/storage/volume"
	"k8s.io/kubernetes/pkg/volume/util"
)

// makePVC 创建一个用于测试的 PVC，默认请求指定容量、以 Filesystem 模式使用 RWO/ROX，
// 并可通过 modfn 在返回前按需改写任意字段。
func makePVC(size string, modfn func(*v1.PersistentVolumeClaim)) *v1.PersistentVolumeClaim {
	fs := v1.PersistentVolumeFilesystem
	pvc := v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "claim01",
			Namespace: "myns",
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadOnlyMany, v1.ReadWriteOnce},
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse(size),
				},
			},
			VolumeMode: &fs,
		},
	}
	// 通过回调修改默认 PVC，使各测试场景能复用同一构造逻辑。
	if modfn != nil {
		modfn(&pvc)
	}
	return &pvc
}

// makeVolumeModePVC 与 makePVC 类似，但允许显式指定 VolumeMode（包括 nil），
// 默认只请求 RWO，用于卷模式匹配测试。
func makeVolumeModePVC(size string, mode *v1.PersistentVolumeMode, modfn func(*v1.PersistentVolumeClaim)) *v1.PersistentVolumeClaim {
	pvc := v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "claim01",
			Namespace: "myns",
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeMode:  mode,
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse(size),
				},
			},
		},
	}
	// 通过回调修改默认 PVC，以构造不同卷模式的测试场景。
	if modfn != nil {
		modfn(&pvc)
	}
	return &pvc
}

// TestMatchVolume 使用一组预置 PV 构建有序索引，以表驱动方式验证 findBestMatchForClaim 的匹配结果：
// 覆盖容量适配、绑定卷跳过、标签选择器、存储类、VolumeAttributesClassName 以及超大容量（1E/800E/950E）等场景。
func TestMatchVolume(t *testing.T) {
	// 用全部测试卷填充索引，容量和访问模式各不相同，用于模拟真实的候选集。
	volList := newPersistentVolumeOrderedIndex()
	for _, pv := range createTestVolumes() {
		volList.store.Add(pv)
	}

	// 每个场景声明期望匹配的 PV UID 和待匹配的 PVC；PVC 由 makePVC 配合可选修改函数构造。
	scenarios := map[string]struct {
		expectedMatch string
		claim         *v1.PersistentVolumeClaim
	}{
		"successful-match-gce-10": {
			expectedMatch: "gce-pd-10",
			claim:         makePVC("8G", nil),
		},
		"successful-match-nfs-5": {
			expectedMatch: "nfs-5",
			claim: makePVC("5G", func(pvc *v1.PersistentVolumeClaim) {
				pvc.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadOnlyMany, v1.ReadWriteOnce, v1.ReadWriteMany}
			}),
		},
		"successful-skip-1g-bound-volume": {
			expectedMatch: "gce-pd-5",
			claim:         makePVC("1G", nil),
		},
		"successful-no-match": {
			expectedMatch: "",
			claim:         makePVC("999G", nil),
		},
		"successful-no-match-due-to-label": {
			expectedMatch: "",
			claim: makePVC("999G", func(pvc *v1.PersistentVolumeClaim) {
				pvc.Spec.Selector = &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"should-not-exist": "true",
					},
				}
			}),
		},
		"successful-no-match-due-to-size-constraint-with-label-selector": {
			expectedMatch: "",
			claim: makePVC("20000G", func(pvc *v1.PersistentVolumeClaim) {
				pvc.Spec.Selector = &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"should-exist": "true",
					},
				}
				pvc.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadOnlyMany, v1.ReadWriteOnce}
			}),
		},
		"successful-match-due-with-constraint-and-label-selector": {
			expectedMatch: "gce-pd-2",
			claim: makePVC("20000G", func(pvc *v1.PersistentVolumeClaim) {
				pvc.Spec.Selector = &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"should-exist": "true",
					},
				}
				pvc.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
			}),
		},
		"successful-match-with-empty-vac": {
			expectedMatch: "gce-pd-10",
			claim: makePVC("8G", func(pvc *v1.PersistentVolumeClaim) {
				pvc.Spec.VolumeAttributesClassName = &classEmpty
			}),
		},
		"successful-match-with-vac": {
			expectedMatch: "gce-pd-vac-silver1",
			claim: makePVC("1G", func(pvc *v1.PersistentVolumeClaim) {
				pvc.Spec.VolumeAttributesClassName = &classSilver
			}),
		},
		"successful-no-match-vac-nonexisting": {
			expectedMatch: "",
			claim: makePVC("1G", func(pvc *v1.PersistentVolumeClaim) {
				pvc.Spec.VolumeAttributesClassName = &classNonExisting
			}),
		},
		"successful-match-with-class": {
			expectedMatch: "gce-pd-silver1",
			claim: makePVC("1G", func(pvc *v1.PersistentVolumeClaim) {
				pvc.Spec.Selector = &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"should-exist": "true",
					},
				}
				pvc.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
				pvc.Spec.StorageClassName = &classSilver
			}),
		},
		"successful-match-with-class-and-labels": {
			expectedMatch: "gce-pd-silver2",
			claim: makePVC("1G", func(pvc *v1.PersistentVolumeClaim) {
				pvc.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
				pvc.Spec.StorageClassName = &classSilver
			}),
		},
		"successful-match-very-large": {
			expectedMatch: "local-pd-very-large",
			// 保持 PVC 容量小于 int64 上限：这样即使 PV 容量溢出，PVC 不会同样溢出而产生误匹配信号。
			claim: makePVC("1E", func(pvc *v1.PersistentVolumeClaim) {
				pvc.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
				pvc.Spec.StorageClassName = &classLarge
			}),
		},
		"successful-match-exact-extremely-large": {
			expectedMatch: "local-pd-extremely-large",
			claim: makePVC("800E", func(pvc *v1.PersistentVolumeClaim) {
				pvc.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
				pvc.Spec.StorageClassName = &classLarge
			}),
		},
		"successful-no-match-way-too-large": {
			expectedMatch: "",
			claim: makePVC("950E", func(pvc *v1.PersistentVolumeClaim) {
				pvc.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
				pvc.Spec.StorageClassName = &classLarge
			}),
		},
	}

	// 逐个场景执行匹配，检查错误、期望命中与未命中三种结果。
	for name, scenario := range scenarios {
		volume, err := volList.findBestMatchForClaim(scenario.claim, false)
		if err != nil {
			t.Errorf("Unexpected error matching volume by claim: %v", err)
		}
		if len(scenario.expectedMatch) != 0 && volume == nil {
			t.Errorf("Expected match but received nil volume for scenario: %s", name)
		}
		if len(scenario.expectedMatch) != 0 && volume != nil && string(volume.UID) != scenario.expectedMatch {
			t.Errorf("Expected %s but got volume %s in scenario %s", scenario.expectedMatch, volume.UID, name)
		}
		if len(scenario.expectedMatch) == 0 && volume != nil {
			t.Errorf("Unexpected match for scenario: %s, matched with %s instead", name, volume.UID)
		}
	}
}

// TestMatchingWithBoundVolumes 验证已绑定（ClaimRef 非空且 Phase=Bound）的 PV 不会被再次匹配：
// 两个容量与访问模式相同的 PV 中，只有一个已绑定，期望匹配到未绑定的那个。
func TestMatchingWithBoundVolumes(t *testing.T) {
	fs := v1.PersistentVolumeFilesystem
	volumeIndex := newPersistentVolumeOrderedIndex()
	// 构造两个相似的 PV，pv1 假装已被绑定。
	pv1 := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			UID:  "gce-pd-1",
			Name: "gce001",
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse("1G"),
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{},
			},
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce, v1.ReadOnlyMany},
			// 该卷假装已被绑定：设置 ClaimRef。
			ClaimRef:   &v1.ObjectReference{UID: "abc123"},
			VolumeMode: &fs,
		},
		Status: v1.PersistentVolumeStatus{
			Phase: v1.VolumeBound,
		},
	}

	pv2 := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			UID:  "gce-pd-2",
			Name: "gce002",
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse("1G"),
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{},
			},
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce, v1.ReadOnlyMany},
			VolumeMode:  &fs,
		},
		Status: v1.PersistentVolumeStatus{
			Phase: v1.VolumeAvailable,
		},
	}

	// 两个卷都加入索引，但只有 pv2 可被匹配。
	volumeIndex.store.Add(pv1)
	volumeIndex.store.Add(pv2)

	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "claim01",
			Namespace: "myns",
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadOnlyMany, v1.ReadWriteOnce},
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("1G"),
				},
			},
			VolumeMode: &fs,
		},
	}

	// 断言最终匹配到未绑定的 pv2。
	volume, err := volumeIndex.findBestMatchForClaim(claim, false)
	if err != nil {
		t.Fatalf("Unexpected error matching volume by claim: %v", err)
	}
	if volume == nil {
		t.Fatalf("Unexpected nil volume.  Expected %s", pv2.Name)
	}
	if pv2.Name != volume.Name {
		t.Errorf("Expected %s but got volume %s instead", pv2.Name, volume.Name)
	}
}

// TestListByAccessModes 验证 listByAccessModes 按访问模式返回 PV：
// RWO+ROX 模式应返回三个 GCE 卷且按容量升序，增加 RWX 后还应包含 NFS 与大容量本地卷。
func TestListByAccessModes(t *testing.T) {
	volList := newPersistentVolumeOrderedIndex()
	for _, pv := range createTestVolumes() {
		volList.store.Add(pv)
	}

	volumes, err := volList.listByAccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOnce, v1.ReadOnlyMany})
	if err != nil {
		t.Error("Unexpected error retrieving volumes by access modes:", err)
	}
	// 结果顺序不确定，显式按容量排序后再断言。
	sort.Sort(byCapacity{volumes})

	for i, expected := range []string{"gce-pd-1", "gce-pd-5", "gce-pd-10"} {
		if string(volumes[i].UID) != expected {
			t.Errorf("Incorrect ordering of persistent volumes.  Expected %s but got %s", expected, volumes[i].UID)
		}
	}

	// 加入 RWX 模式后，支持 RWX 的 NFS 卷也应出现在结果中。
	volumes, err = volList.listByAccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOnce, v1.ReadOnlyMany, v1.ReadWriteMany})
	if err != nil {
		t.Error("Unexpected error retrieving volumes by access modes:", err)
	}
	// 同样按容量排序后再断言。
	sort.Sort(byCapacity{volumes})

	for i, expected := range []string{"nfs-1", "nfs-5", "nfs-10", "local-pd-very-large", "local-pd-extremely-large"} {
		if string(volumes[i].UID) != expected {
			t.Errorf("Incorrect ordering of persistent volumes.  Expected %s but got %s", expected, volumes[i].UID)
		}
	}
}

// TestAllPossibleAccessModes 验证 allPossibleMatchingAccessModes 为给定访问模式生成所有可能的组合：
// 测试卷包含 RWO+ROX 与 RWO+ROW+RWX 两类模式，因此 RWO 有三种匹配组合，RWX 只有一种。
func TestAllPossibleAccessModes(t *testing.T) {
	index := newPersistentVolumeOrderedIndex()
	for _, pv := range createTestVolumes() {
		index.store.Add(pv)
	}

	// 模拟 PV 中仅存在两种访问模式组合：RWO+ROX 和 RWO+ROW+RWX。
	// 请求 RWO 时，期望生成三种包含 RWO 的模式组合。
	possibleModes := index.allPossibleMatchingAccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteOnce})
	if len(possibleModes) != 3 {
		t.Errorf("Expected 3 arrays of modes that match RWO, but got %v", len(possibleModes))
	}
	// 每种组合都必须包含请求的 RWO 模式。
	for _, m := range possibleModes {
		if !util.ContainsAccessMode(m, v1.ReadWriteOnce) {
			t.Errorf("AccessModes does not contain %s", v1.ReadWriteOnce)
		}
	}

	// 请求 RWX 时，只有一种模式组合能满足。
	possibleModes = index.allPossibleMatchingAccessModes([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany})
	if len(possibleModes) != 1 {
		t.Errorf("Expected 1 array of modes that match RWX, but got %v", len(possibleModes))
	}
	// 该组合必须包含 RWX。
	if !util.ContainsAccessMode(possibleModes[0], v1.ReadWriteMany) {
		t.Errorf("AccessModes does not contain %s", v1.ReadWriteOnce)
	}

}

// TestFindingVolumeWithDifferentAccessModes 验证不同访问模式请求下的最佳匹配：
// 请求 RWO 时选中最精确的 ebs，请求 RWO+ROX 时选中 gce，请求 RWX 或调整顺序时选中 nfs；
// 随后依次把精确匹配的卷标记为已绑定，验证匹配会逐级回退到包含更多模式的卷。
func TestFindingVolumeWithDifferentAccessModes(t *testing.T) {
	fs := v1.PersistentVolumeFilesystem
	gce := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{UID: "001", Name: "gce"},
		Spec: v1.PersistentVolumeSpec{
			Capacity:               v1.ResourceList{v1.ResourceName(v1.ResourceStorage): resource.MustParse("10G")},
			PersistentVolumeSource: v1.PersistentVolumeSource{GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{}},
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
				v1.ReadOnlyMany,
			},
			VolumeMode: &fs,
		},
		Status: v1.PersistentVolumeStatus{
			Phase: v1.VolumeAvailable,
		},
	}

	ebs := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{UID: "002", Name: "ebs"},
		Spec: v1.PersistentVolumeSpec{
			Capacity:               v1.ResourceList{v1.ResourceName(v1.ResourceStorage): resource.MustParse("10G")},
			PersistentVolumeSource: v1.PersistentVolumeSource{AWSElasticBlockStore: &v1.AWSElasticBlockStoreVolumeSource{}},
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			VolumeMode: &fs,
		},
		Status: v1.PersistentVolumeStatus{
			Phase: v1.VolumeAvailable,
		},
	}

	nfs := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{UID: "003", Name: "nfs"},
		Spec: v1.PersistentVolumeSpec{
			Capacity:               v1.ResourceList{v1.ResourceName(v1.ResourceStorage): resource.MustParse("10G")},
			PersistentVolumeSource: v1.PersistentVolumeSource{NFS: &v1.NFSVolumeSource{}},
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
				v1.ReadOnlyMany,
				v1.ReadWriteMany,
			},
			VolumeMode: &fs,
		},
		Status: v1.PersistentVolumeStatus{
			Phase: v1.VolumeAvailable,
		},
	}

	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "claim01",
			Namespace: "myns",
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Resources:   v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceName(v1.ResourceStorage): resource.MustParse("1G")}},
			VolumeMode:  &fs,
		},
	}

	index := newPersistentVolumeOrderedIndex()
	index.store.Add(gce)
	index.store.Add(ebs)
	index.store.Add(nfs)

	// 只请求 RWO：ebs 仅支持 RWO，是最精确的匹配。
	volume, _ := index.findBestMatchForClaim(claim, false)
	if volume.Name != ebs.Name {
		t.Errorf("Expected %s but got volume %s instead", ebs.Name, volume.Name)
	}

	// 请求 RWO+ROX：gce 的模式与之完全一致。
	claim.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce, v1.ReadOnlyMany}
	volume, _ = index.findBestMatchForClaim(claim, false)
	if volume.Name != gce.Name {
		t.Errorf("Expected %s but got volume %s instead", gce.Name, volume.Name)
	}

	// 请求模式的顺序不应影响匹配结果；包含 RWX 的请求应匹配到 nfs。
	claim.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteMany, v1.ReadWriteOnce, v1.ReadOnlyMany}
	volume, _ = index.findBestMatchForClaim(claim, false)
	if volume.Name != nfs.Name {
		t.Errorf("Expected %s but got volume %s instead", nfs.Name, volume.Name)
	}

	// 请求更少的模式仍应匹配到支持该模式的卷。
	claim.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteMany}
	volume, _ = index.findBestMatchForClaim(claim, false)
	if volume.Name != nfs.Name {
		t.Errorf("Expected %s but got volume %s instead", nfs.Name, volume.Name)
	}

	// 假装精确匹配的 ebs 已被绑定，应回退到下一级模式（gce）。
	ebs.Spec.ClaimRef = &v1.ObjectReference{}
	claim.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
	volume, _ = index.findBestMatchForClaim(claim, false)
	if volume.Name != gce.Name {
		t.Errorf("Expected %s but got volume %s instead", gce.Name, volume.Name)
	}

	// 继续回退：gce 也被绑定后，应匹配到 nfs。
	gce.Spec.ClaimRef = &v1.ObjectReference{}
	claim.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
	volume, _ = index.findBestMatchForClaim(claim, false)
	if volume.Name != nfs.Name {
		t.Errorf("Expected %s but got volume %s instead", nfs.Name, volume.Name)
	}

	// 部分模式请求：只请求 ROX 时，应匹配到仍可用的 gce。
	gce.Spec.ClaimRef = nil
	claim.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}
	volume, _ = index.findBestMatchForClaim(claim, false)
	if volume.Name != gce.Name {
		t.Errorf("Expected %s but got volume %s instead", gce.Name, volume.Name)
	}
}

// createNodeAffinity 为给定的 key 和 value 构造一个 VolumeNodeAffinity，
// 其中包含一个使用 In 操作符的 NodeSelectorRequirement。
func createNodeAffinity(key string, value string) *v1.VolumeNodeAffinity {
	return &v1.VolumeNodeAffinity{
		Required: &v1.NodeSelector{
			NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      key,
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{value},
						},
					},
				},
			},
		},
	}
}

// createTestVolumes 返回测试用的 PV 集合：容量、访问模式、绑定状态、存储类、节点亲和性等各不相同，
// 且故意乱序排列，以检验索引排序逻辑。
func createTestVolumes() []*v1.PersistentVolume {
	fs := v1.PersistentVolumeFilesystem
	// 这些卷故意乱序排列，用于测试索引的排序功能。
	return []*v1.PersistentVolume{
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "gce-pd-10",
				Name: "gce003",
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("10G"),
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
					v1.ReadOnlyMany,
				},
				VolumeMode: &fs,
			},
			Status: v1.PersistentVolumeStatus{
				Phase: v1.VolumeAvailable,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "gce-pd-20",
				Name: "gce004",
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("20G"),
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
					v1.ReadOnlyMany,
				},
				// 该卷假装已被绑定。
				ClaimRef:   &v1.ObjectReference{UID: "def456"},
				VolumeMode: &fs,
			},
			Status: v1.PersistentVolumeStatus{
				Phase: v1.VolumeBound,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "nfs-5",
				Name: "nfs002",
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("5G"),
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					Glusterfs: &v1.GlusterfsPersistentVolumeSource{},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
					v1.ReadOnlyMany,
					v1.ReadWriteMany,
				},
				VolumeMode: &fs,
			},
			Status: v1.PersistentVolumeStatus{
				Phase: v1.VolumeAvailable,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "gce-pd-1",
				Name: "gce001",
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("1G"),
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
					v1.ReadOnlyMany,
				},
				// 该卷假装已被绑定。
				ClaimRef:   &v1.ObjectReference{UID: "abc123"},
				VolumeMode: &fs,
			},
			Status: v1.PersistentVolumeStatus{
				Phase: v1.VolumeBound,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "nfs-10",
				Name: "nfs003",
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("10G"),
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					Glusterfs: &v1.GlusterfsPersistentVolumeSource{},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
					v1.ReadOnlyMany,
					v1.ReadWriteMany,
				},
				VolumeMode: &fs,
			},
			Status: v1.PersistentVolumeStatus{
				Phase: v1.VolumeAvailable,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "gce-pd-5",
				Name: "gce002",
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("5G"),
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
					v1.ReadOnlyMany,
				},
				VolumeMode: &fs,
			},
			Status: v1.PersistentVolumeStatus{
				Phase: v1.VolumeAvailable,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "nfs-1",
				Name: "nfs001",
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("1G"),
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					Glusterfs: &v1.GlusterfsPersistentVolumeSource{},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
					v1.ReadOnlyMany,
					v1.ReadWriteMany,
				},
				VolumeMode: &fs,
			},
			Status: v1.PersistentVolumeStatus{
				Phase: v1.VolumeAvailable,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "gce-pd-2",
				Name: "gce0022",
				Labels: map[string]string{
					"should-exist": "true",
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("20000G"),
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
				},
				VolumeMode: &fs,
			},
			Status: v1.PersistentVolumeStatus{
				Phase: v1.VolumeAvailable,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "gce-pd-silver1",
				Name: "gce0023",
				Labels: map[string]string{
					"should-exist": "true",
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("10000G"),
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
				},
				StorageClassName: classSilver,
				VolumeMode:       &fs,
			},
			Status: v1.PersistentVolumeStatus{
				Phase: v1.VolumeAvailable,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "gce-pd-silver2",
				Name: "gce0024",
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("100G"),
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
				},
				StorageClassName: classSilver,
				VolumeMode:       &fs,
			},
			Status: v1.PersistentVolumeStatus{
				Phase: v1.VolumeAvailable,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "gce-pd-gold",
				Name: "gce0025",
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("50G"),
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
				},
				StorageClassName: classGold,
				VolumeMode:       &fs,
			},
			Status: v1.PersistentVolumeStatus{
				Phase: v1.VolumeAvailable,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "local-pd-very-large",
				Name: "local001",
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("200E"),
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					Local: &v1.LocalVolumeSource{},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
					v1.ReadOnlyMany,
					v1.ReadWriteMany,
				},
				StorageClassName: classLarge,
				VolumeMode:       &fs,
			},
			Status: v1.PersistentVolumeStatus{
				Phase: v1.VolumeAvailable,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "local-pd-extremely-large",
				Name: "local002",
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("800E"),
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					Local: &v1.LocalVolumeSource{},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
					v1.ReadOnlyMany,
					v1.ReadWriteMany,
				},
				StorageClassName: classLarge,
				VolumeMode:       &fs,
			},
			Status: v1.PersistentVolumeStatus{
				Phase: v1.VolumeAvailable,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "affinity-pv",
				Name: "affinity001",
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("100G"),
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					Local: &v1.LocalVolumeSource{},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
					v1.ReadOnlyMany,
				},
				StorageClassName: classWait,
				NodeAffinity:     createNodeAffinity("key1", "value1"),
				VolumeMode:       &fs,
			},
			Status: v1.PersistentVolumeStatus{
				Phase: v1.VolumeAvailable,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "affinity-pv2",
				Name: "affinity002",
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("150G"),
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					Local: &v1.LocalVolumeSource{},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
					v1.ReadOnlyMany,
				},
				StorageClassName: classWait,
				NodeAffinity:     createNodeAffinity("key1", "value1"),
				VolumeMode:       &fs,
			},
			Status: v1.PersistentVolumeStatus{
				Phase: v1.VolumeAvailable,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "affinity-prebound",
				Name: "affinity003",
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("100G"),
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					Local: &v1.LocalVolumeSource{},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
					v1.ReadOnlyMany,
				},
				StorageClassName: classWait,
				ClaimRef:         &v1.ObjectReference{Name: "claim02", Namespace: "myns"},
				NodeAffinity:     createNodeAffinity("key1", "value1"),
				VolumeMode:       &fs,
			},
			Status: v1.PersistentVolumeStatus{
				Phase: v1.VolumeAvailable,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "affinity-pv3",
				Name: "affinity003",
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("200G"),
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					Local: &v1.LocalVolumeSource{},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
					v1.ReadOnlyMany,
				},
				StorageClassName: classWait,
				NodeAffinity:     createNodeAffinity("key1", "value3"),
				VolumeMode:       &fs,
			},
			Status: v1.PersistentVolumeStatus{
				Phase: v1.VolumeAvailable,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "affinity-pv4-pending",
				Name: "affinity004-pending",
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("200G"),
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					Local: &v1.LocalVolumeSource{},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
					v1.ReadOnlyMany,
				},
				StorageClassName: classWait,
				NodeAffinity:     createNodeAffinity("key1", "value4"),
				VolumeMode:       &fs,
			},
			Status: v1.PersistentVolumeStatus{
				Phase: v1.VolumePending,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "affinity-pv4-failed",
				Name: "affinity004-failed",
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("200G"),
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					Local: &v1.LocalVolumeSource{},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
					v1.ReadOnlyMany,
				},
				StorageClassName: classWait,
				NodeAffinity:     createNodeAffinity("key1", "value4"),
				VolumeMode:       &fs,
			},
			Status: v1.PersistentVolumeStatus{
				Phase: v1.VolumeFailed,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "affinity-pv4-released",
				Name: "affinity004-released",
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("200G"),
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					Local: &v1.LocalVolumeSource{},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
					v1.ReadOnlyMany,
				},
				StorageClassName: classWait,
				NodeAffinity:     createNodeAffinity("key1", "value4"),
				VolumeMode:       &fs,
			},
			Status: v1.PersistentVolumeStatus{
				Phase: v1.VolumeReleased,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "affinity-pv4-empty",
				Name: "affinity004-empty",
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("200G"),
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					Local: &v1.LocalVolumeSource{},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
					v1.ReadOnlyMany,
				},
				StorageClassName: classWait,
				NodeAffinity:     createNodeAffinity("key1", "value4"),
				VolumeMode:       &fs,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "gce-pd-vac-silver1",
				Name: "gce-pd-vac-silver1",
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("100G"),
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
					v1.ReadOnlyMany,
				},
				VolumeAttributesClassName: &classSilver,
				VolumeMode:                &fs,
			},
			Status: v1.PersistentVolumeStatus{
				Phase: v1.VolumeAvailable,
			},
		},
	}
}

// testVolume 构造一个 HostPath 类型的测试 PV，带指定名称与容量，默认为 Available、RWO、Filesystem。
func testVolume(name, size string) *v1.PersistentVolume {
	fs := v1.PersistentVolumeFilesystem
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Annotations: map[string]string{},
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity:               v1.ResourceList{v1.ResourceName(v1.ResourceStorage): resource.MustParse(size)},
			PersistentVolumeSource: v1.PersistentVolumeSource{HostPath: &v1.HostPathVolumeSource{}},
			AccessModes:            []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			VolumeMode:             &fs,
		},
		Status: v1.PersistentVolumeStatus{
			Phase: v1.VolumeAvailable,
		},
	}
}

// createVolumeModeBlockTestVolume 构造一个 VolumeMode=Block 的测试 PV。
func createVolumeModeBlockTestVolume() *v1.PersistentVolume {
	blockMode := v1.PersistentVolumeBlock

	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			UID:  "local-1",
			Name: "block",
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse("10G"),
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				Local: &v1.LocalVolumeSource{},
			},
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			VolumeMode: &blockMode,
		},
		Status: v1.PersistentVolumeStatus{
			Phase: v1.VolumeAvailable,
		},
	}
}

// createVolumeModeFilesystemTestVolume 构造一个 VolumeMode=Filesystem 的测试 PV。
func createVolumeModeFilesystemTestVolume() *v1.PersistentVolume {
	filesystemMode := v1.PersistentVolumeFilesystem

	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			UID:  "local-1",
			Name: "block",
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse("10G"),
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				Local: &v1.LocalVolumeSource{},
			},
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			VolumeMode: &filesystemMode,
		},
		Status: v1.PersistentVolumeStatus{
			Phase: v1.VolumeAvailable,
		},
	}
}

// createVolumeModeNilTestVolume 构造一个未设置 VolumeMode（nil）的测试 PV。
func createVolumeModeNilTestVolume() *v1.PersistentVolume {
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			UID:  "local-1",
			Name: "nil-mode",
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse("10G"),
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				Local: &v1.LocalVolumeSource{},
			},
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
		},
		Status: v1.PersistentVolumeStatus{
			Phase: v1.VolumeAvailable,
		},
	}
}

// createTestVolOrderedIndex 构造一个只包含指定 PV 的有序索引，供单例匹配测试使用。
func createTestVolOrderedIndex(pv *v1.PersistentVolume) persistentVolumeOrderedIndex {
	volFile := newPersistentVolumeOrderedIndex()
	volFile.store.Add(pv)
	return volFile
}

// TestVolumeModeCheck 以表驱动方式验证 CheckVolumeModeMismatches 对 PVC/PV 卷模式组合的判断：
// block 与 filesystem 互不匹配，nil 被视为默认 filesystem，只有 filesystem-filesystem、block-block 及 nil 相关组合按预期匹配。
func TestVolumeModeCheck(t *testing.T) {

	blockMode := v1.PersistentVolumeBlock
	filesystemMode := v1.PersistentVolumeFilesystem

	// 若 feature gate 启用，VolumeMode 总会被默认值化；若禁用，VolumeMode 会被 API 丢弃并忽略。
	// 每个场景声明是否期望模式不匹配，以及待检查的 PV 和 PVC。
	scenarios := map[string]struct {
		isExpectedMismatch bool
		vol                *v1.PersistentVolume
		pvc                *v1.PersistentVolumeClaim
	}{
		"pvc block and pv filesystem": {
			isExpectedMismatch: true,
			vol:                createVolumeModeFilesystemTestVolume(),
			pvc:                makeVolumeModePVC("8G", &blockMode, nil),
		},
		"pvc filesystem and pv block": {
			isExpectedMismatch: true,
			vol:                createVolumeModeBlockTestVolume(),
			pvc:                makeVolumeModePVC("8G", &filesystemMode, nil),
		},
		"pvc block and pv block": {
			isExpectedMismatch: false,
			vol:                createVolumeModeBlockTestVolume(),
			pvc:                makeVolumeModePVC("8G", &blockMode, nil),
		},
		"pvc filesystem and pv filesystem": {
			isExpectedMismatch: false,
			vol:                createVolumeModeFilesystemTestVolume(),
			pvc:                makeVolumeModePVC("8G", &filesystemMode, nil),
		},
		"pvc filesystem and pv nil": {
			isExpectedMismatch: false,
			vol:                createVolumeModeNilTestVolume(),
			pvc:                makeVolumeModePVC("8G", &filesystemMode, nil),
		},
		"pvc nil and pv filesystem": {
			isExpectedMismatch: false,
			vol:                createVolumeModeFilesystemTestVolume(),
			pvc:                makeVolumeModePVC("8G", nil, nil),
		},
		"pvc nil and pv nil": {
			isExpectedMismatch: false,
			vol:                createVolumeModeNilTestVolume(),
			pvc:                makeVolumeModePVC("8G", nil, nil),
		},
		"pvc nil and pv block": {
			isExpectedMismatch: true,
			vol:                createVolumeModeBlockTestVolume(),
			pvc:                makeVolumeModePVC("8G", nil, nil),
		},
		"pvc block and pv nil": {
			isExpectedMismatch: true,
			vol:                createVolumeModeNilTestVolume(),
			pvc:                makeVolumeModePVC("8G", &blockMode, nil),
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			expectedMismatch := volume.CheckVolumeModeMismatches(&scenario.pvc.Spec, &scenario.vol.Spec)
			// 期望匹配却检测到不匹配（或反之）都属于错误。
			if expectedMismatch && !scenario.isExpectedMismatch {
				t.Errorf("Unexpected failure for scenario, expected not to mismatch on modes but did: %s", name)
			}
			if !expectedMismatch && scenario.isExpectedMismatch {
				t.Errorf("Unexpected failure for scenario, did not mismatch on mode when expected to mismatch: %s", name)
			}
		})
	}
}

// TestFilteringVolumeModes 以表驱动方式验证 findBestMatchForClaim 在卷模式层面的过滤：
// block 与 filesystem 不应互相匹配，相同模式及默认 filesystem 的 nil 模式组合应匹配。
func TestFilteringVolumeModes(t *testing.T) {
	blockMode := v1.PersistentVolumeBlock
	filesystemMode := v1.PersistentVolumeFilesystem

	// 若 feature gate 启用，VolumeMode 总会被默认值化；若禁用，VolumeMode 会被 API 丢弃并忽略。
	// 每个场景声明是否期望匹配，以及待匹配的索引和 PVC。
	scenarios := map[string]struct {
		isExpectedMatch bool
		vol             persistentVolumeOrderedIndex
		pvc             *v1.PersistentVolumeClaim
	}{
		"pvc block and pv filesystem": {
			isExpectedMatch: false,
			vol:             createTestVolOrderedIndex(createVolumeModeFilesystemTestVolume()),
			pvc:             makeVolumeModePVC("8G", &blockMode, nil),
		},
		"pvc filesystem and pv block": {
			isExpectedMatch: false,
			vol:             createTestVolOrderedIndex(createVolumeModeBlockTestVolume()),
			pvc:             makeVolumeModePVC("8G", &filesystemMode, nil),
		},
		"pvc block and pv no mode with default filesystem": {
			isExpectedMatch: false,
			vol:             createTestVolOrderedIndex(createVolumeModeFilesystemTestVolume()),
			pvc:             makeVolumeModePVC("8G", &blockMode, nil),
		},
		"pvc no mode defaulted to filesystem and pv block": {
			isExpectedMatch: false,
			vol:             createTestVolOrderedIndex(createVolumeModeBlockTestVolume()),
			pvc:             makeVolumeModePVC("8G", &filesystemMode, nil),
		},
		"pvc block and pv block": {
			isExpectedMatch: true,
			vol:             createTestVolOrderedIndex(createVolumeModeBlockTestVolume()),
			pvc:             makeVolumeModePVC("8G", &blockMode, nil),
		},
		"pvc filesystem and pv filesystem": {
			isExpectedMatch: true,
			vol:             createTestVolOrderedIndex(createVolumeModeFilesystemTestVolume()),
			pvc:             makeVolumeModePVC("8G", &filesystemMode, nil),
		},
		"pvc mode is nil and defaulted and pv mode is nil and defaulted": {
			isExpectedMatch: true,
			vol:             createTestVolOrderedIndex(createVolumeModeFilesystemTestVolume()),
			pvc:             makeVolumeModePVC("8G", &filesystemMode, nil),
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			pvmatch, err := scenario.vol.findBestMatchForClaim(scenario.pvc, false)
			// 期望匹配却没匹配到（或期望不匹配却匹配到）都应报错。
			if pvmatch == nil && scenario.isExpectedMatch {
				t.Errorf("Unexpected failure for scenario, no matching volume: %s", name)
			}
			if err != nil && scenario.isExpectedMatch {
				t.Errorf("Unexpected failure for scenario: %s - %+v", name, err)
			}
			// 期望不匹配时，若返回了匹配卷或出现错误，也属于异常。
			if pvmatch != nil && !scenario.isExpectedMatch {
				t.Errorf("Unexpected failure for scenario, expected no matching volume: %s", name)
			}
			if err != nil && !scenario.isExpectedMatch {
				t.Errorf("Unexpected failure for scenario: %s - %+v", name, err)
			}
		})
	}
}

// TestStorageObjectInUseProtectionFiltering 验证存储对象保护机制：设置了删除时间戳（DeletionTimestamp）的 PV
// 既不能满足 PVC（checkVolumeSatisfyClaim），也不会被索引匹配（findBestMatchForClaim）。
func TestStorageObjectInUseProtectionFiltering(t *testing.T) {
	fs := v1.PersistentVolumeFilesystem
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "pv1",
			Annotations: map[string]string{},
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity:               v1.ResourceList{v1.ResourceName(v1.ResourceStorage): resource.MustParse("1G")},
			PersistentVolumeSource: v1.PersistentVolumeSource{HostPath: &v1.HostPathVolumeSource{}},
			AccessModes:            []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			VolumeMode:             &fs,
		},
		Status: v1.PersistentVolumeStatus{
			Phase: v1.VolumeAvailable,
		},
	}

	// 复制一个 PV 并加上删除时间戳，模拟处于删除保护中的 PV。
	pvToDelete := pv.DeepCopy()
	now := metav1.Now()
	pvToDelete.ObjectMeta.DeletionTimestamp = &now

	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc1",
			Namespace: "myns",
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Resources:   v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceName(v1.ResourceStorage): resource.MustParse("1G")}},
			VolumeMode:  &fs,
		},
	}

	// 直接调用 checkVolumeSatisfyClaim 的场景：验证带删除时间戳的 PV 不满足 PVC。
	satisfyingTestCases := map[string]struct {
		isExpectedMatch bool
		vol             *v1.PersistentVolume
		pvc             *v1.PersistentVolumeClaim
	}{
		"pv deletionTimeStamp not set": {
			isExpectedMatch: true,
			vol:             pv,
			pvc:             pvc,
		},
		"pv deletionTimeStamp set": {
			isExpectedMatch: false,
			vol:             pvToDelete,
			pvc:             pvc,
		},
	}

	for name, testCase := range satisfyingTestCases {
		t.Run(name, func(t *testing.T) {
			err := checkVolumeSatisfyClaim(testCase.vol, testCase.pvc)
			// 期望匹配却返回错误，属于异常。
			if err != nil && testCase.isExpectedMatch {
				t.Errorf("%s: expected to match but got an error: %v", name, err)
			}
			// 期望不匹配却通过了检查，属于异常。
			if err == nil && !testCase.isExpectedMatch {
				t.Errorf("%s: not expected to match but did", name)
			}
		})
	}

	// 通过 findBestMatchForClaim 验证过滤效果：带删除时间戳的 PV 不会被选中。
	filteringTestCases := map[string]struct {
		isExpectedMatch bool
		vol             persistentVolumeOrderedIndex
		pvc             *v1.PersistentVolumeClaim
	}{
		"pv deletionTimeStamp not set": {
			isExpectedMatch: true,
			vol:             createTestVolOrderedIndex(pv),
			pvc:             pvc,
		},
		"pv deletionTimeStamp set": {
			isExpectedMatch: false,
			vol:             createTestVolOrderedIndex(pvToDelete),
			pvc:             pvc,
		},
	}
	for name, testCase := range filteringTestCases {
		t.Run(name, func(t *testing.T) {
			pvmatch, err := testCase.vol.findBestMatchForClaim(testCase.pvc, false)
			// 期望匹配却没匹配到（或期望不匹配却匹配到）都应报错。
			if pvmatch == nil && testCase.isExpectedMatch {
				t.Errorf("Unexpected failure for testcase, no matching volume: %s", name)
			}
			if err != nil && testCase.isExpectedMatch {
				t.Errorf("Unexpected failure for testcase: %s - %+v", name, err)
			}
			// 期望不匹配时，若返回了匹配卷或出现错误，也属于异常。
			if pvmatch != nil && !testCase.isExpectedMatch {
				t.Errorf("Unexpected failure for testcase, expected no matching volume: %s", name)
			}
			if err != nil && !testCase.isExpectedMatch {
				t.Errorf("Unexpected failure for testcase: %s - %+v", name, err)
			}
		})
	}
}

// TestFindingPreboundVolumes 验证预绑定（ClaimRef 指向该 PVC）在容量与访问模式满足要求时优先于常规匹配顺序：
// 精确容量匹配优先；精确匹配被别的 PVC 预绑定时选下一档容量；
// 满足要求的大卷预绑定给本 PVC 时直接返回；容量不足或访问模式不符的预绑定卷仍会被过滤。
func TestFindingPreboundVolumes(t *testing.T) {
	fs := v1.PersistentVolumeFilesystem
	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "claim01",
			Namespace: "myns",
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Resources:   v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceName(v1.ResourceStorage): resource.MustParse("1Gi")}},
			VolumeMode:  &fs,
		},
	}
	// 获取 PVC 引用，用于模拟某个 PV 被预绑定给该 PVC。
	claimRef, err := ref.GetReference(scheme.Scheme, claim)
	if err != nil {
		t.Errorf("error getting claimRef: %v", err)
	}

	// 构造容量分别为 1Gi/5Gi/8Gi 的候选卷，另有容量过小和访问模式不符的卷。
	pv1 := testVolume("pv1", "1Gi")
	pv5 := testVolume("pv5", "5Gi")
	pv8 := testVolume("pv8", "8Gi")
	pvBadSize := testVolume("pvBadSize", "1Mi")
	pvBadMode := testVolume("pvBadMode", "1Gi")
	pvBadMode.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}

	index := newPersistentVolumeOrderedIndex()
	index.store.Add(pv1)
	index.store.Add(pv5)
	index.store.Add(pv8)
	index.store.Add(pvBadSize)
	index.store.Add(pvBadMode)

	// 无预绑定时，按容量精确匹配到 pv1。
	volume, _ := index.findBestMatchForClaim(claim, false)
	if volume.Name != pv1.Name {
		t.Errorf("Expected %s but got volume %s instead", pv1.Name, volume.Name)
	}

	// 精确匹配的 pv1 被别的 PVC 预绑定，应退而选择容量更大的 pv5。
	pv1.Spec.ClaimRef = &v1.ObjectReference{Name: "foo", Namespace: "bar"}
	volume, _ = index.findBestMatchForClaim(claim, false)
	if volume.Name != pv5.Name {
		t.Errorf("Expected %s but got volume %s instead", pv5.Name, volume.Name)
	}

	// pv1 可用但最大的 pv8 预绑定给本 PVC：预绑定关系优先，应直接返回 pv8。
	pv1.Spec.ClaimRef = nil
	pv8.Spec.ClaimRef = claimRef
	volume, _ = index.findBestMatchForClaim(claim, false)
	if volume.Name != pv8.Name {
		t.Errorf("Expected %s but got volume %s instead", pv8.Name, volume.Name)
	}

	// 容量过小的 pvBadSize 虽预绑定给本 PVC，仍不满足容量要求，应选择精确匹配的 pv1。
	pv8.Spec.ClaimRef = nil
	pvBadSize.Spec.ClaimRef = claimRef
	volume, _ = index.findBestMatchForClaim(claim, false)
	if volume.Name != pv1.Name {
		t.Errorf("Expected %s but got volume %s instead", pv1.Name, volume.Name)
	}

	// 访问模式不符的 pvBadMode 虽预绑定给本 PVC，仍不满足访问模式要求，应选择精确匹配的 pv1。
	pvBadSize.Spec.ClaimRef = nil
	pvBadMode.Spec.ClaimRef = claimRef
	volume, _ = index.findBestMatchForClaim(claim, false)
	if volume.Name != pv1.Name {
		t.Errorf("Expected %s but got volume %s instead", pv1.Name, volume.Name)
	}
}

// TestBestMatchDelayed 验证 delayBinding=true 时 findBestMatchForClaim 不返回任何卷：
// 通过 PV controller 完成的绑定需要延迟，因此即使存在匹配卷也应返回 nil。
func TestBestMatchDelayed(t *testing.T) {
	volList := newPersistentVolumeOrderedIndex()
	for _, pv := range createTestVolumes() {
		volList.store.Add(pv)
	}

	// 通过 PV controller 完成的绑定应被延迟。
	claim := makePVC("8G", nil)
	volume, err := volList.findBestMatchForClaim(claim, true)
	if err != nil {
		t.Errorf("Unexpected error matching volume by claim: %v", err)
	}
	if volume != nil {
		t.Errorf("Unexpected match with %q", volume.UID)
	}
}

// TestCheckAccessModes 验证 CheckAccessModes 对访问模式兼容性的判断：
// PV 支持 RWO 与 RWX，请求单一 RWX 或多个模式（只要包含 PV 支持的模式）应通过，
// 请求 ROX 或包含 ROX 的组合则失败。
func TestCheckAccessModes(t *testing.T) {
	pv := &v1.PersistentVolume{
		Spec: v1.PersistentVolumeSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce, v1.ReadWriteMany},
		},
	}

	// 每个场景声明期望结果和待检查的 PVC。
	scenarios := map[string]struct {
		shouldSucceed bool
		claim         *v1.PersistentVolumeClaim
	}{
		"success-single-mode": {
			shouldSucceed: true,
			claim: makePVC("100G", func(pvc *v1.PersistentVolumeClaim) {
				pvc.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteMany}
			}),
		},
		"success-many-modes": {
			shouldSucceed: true,
			claim: makePVC("100G", func(pvc *v1.PersistentVolumeClaim) {
				pvc.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteMany, v1.ReadWriteOnce}
			}),
		},
		"fail-single-mode": {
			shouldSucceed: false,
			claim: makePVC("100G", func(pvc *v1.PersistentVolumeClaim) {
				pvc.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}
			}),
		},
		"fail-many-modes": {
			shouldSucceed: false,
			claim: makePVC("100G", func(pvc *v1.PersistentVolumeClaim) {
				pvc.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteMany, v1.ReadOnlyMany}
			}),
		},
	}

	for name, scenario := range scenarios {
		result := volume.CheckAccessModes(scenario.claim, pv)
		if result != scenario.shouldSucceed {
			t.Errorf("Test %q failed: Expected %v, got %v", name, scenario.shouldSucceed, result)
		}
	}
}

// byCapacity 实现 sort.Interface，按容量升序排列 PV 列表。
type byCapacity struct {
	volumes []*v1.PersistentVolume
}

// Less 按容量比较两个 PV。
func (c byCapacity) Less(i, j int) bool {
	return matchStorageCapacity(c.volumes[i], c.volumes[j])
}

// Swap 交换两个 PV 的位置。
func (c byCapacity) Swap(i, j int) {
	c.volumes[i], c.volumes[j] = c.volumes[j], c.volumes[i]
}

// Len 返回待排序 PV 的数量。
func (c byCapacity) Len() int {
	return len(c.volumes)
}

// matchStorageCapacity 比较两个 PV 的存储容量，pvA 小于等于 pvB 时返回 true。
func matchStorageCapacity(pvA, pvB *v1.PersistentVolume) bool {
	aQty := pvA.Spec.Capacity[v1.ResourceStorage]
	bQty := pvB.Spec.Capacity[v1.ResourceStorage]
	return aQty.Cmp(bQty) <= 0
}
