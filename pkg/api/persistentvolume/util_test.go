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

package persistentvolume

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"
)

// TestDropDisabledFields 验证 DropDisabledSpecFields 对 VolumeAttributesClassName
// 的处理：特性关闭时清除新 spec 中的字段，除非旧对象已经在使用它（避免更新时丢配置）；
// 特性开启时则无论新旧取值都原样保留。
func TestDropDisabledFields(t *testing.T) {
	vacName := ptr.To("vac")

	tests := map[string]struct {
		oldSpec       *api.PersistentVolumeSpec
		newSpec       *api.PersistentVolumeSpec
		expectOldSpec *api.PersistentVolumeSpec
		expectNewSpec *api.PersistentVolumeSpec
		vacEnabled    bool
	}{
		// 特性关闭、创建场景（旧 spec 为 nil）：新字段应被清除。
		"disabled vac clears volume attributes class name": {
			vacEnabled:    false,
			newSpec:       specWithVACName(vacName),
			expectNewSpec: specWithVACName(nil),
			oldSpec:       nil,
			expectOldSpec: nil,
		},
		// 特性开启：新字段正常保留。
		"enabled vac preserve volume attributes class name": {
			vacEnabled:    true,
			newSpec:       specWithVACName(vacName),
			expectNewSpec: specWithVACName(vacName),
			oldSpec:       nil,
			expectOldSpec: nil,
		},
		// 特性开启且新旧都使用该字段：两者都不变。
		"enabled vac preserve volume attributes class name when both old and new have it": {
			vacEnabled:    true,
			newSpec:       specWithVACName(vacName),
			expectNewSpec: specWithVACName(vacName),
			oldSpec:       specWithVACName(vacName),
			expectOldSpec: specWithVACName(vacName),
		},
		// 特性关闭但旧对象已在用该字段：必须保留，防止更新丢失已有配置。
		"disabled vac old pv had volume attributes class name": {
			vacEnabled:    false,
			newSpec:       specWithVACName(vacName),
			expectNewSpec: specWithVACName(vacName),
			oldSpec:       specWithVACName(vacName),
			expectOldSpec: specWithVACName(vacName),
		},
		// 特性开启、旧对象未使用该字段：新字段仍保留。
		"enabled vac preserves volume attributes class name when old pv did not had it": {
			vacEnabled:    true,
			newSpec:       specWithVACName(vacName),
			expectNewSpec: specWithVACName(vacName),
			oldSpec:       specWithVACName(nil),
			expectOldSpec: specWithVACName(nil),
		},
		// 新旧都没有该字段：清除是空操作，结果仍为 nil。
		"disabled vac neither new pv nor old pv had volume attributes class name": {
			vacEnabled:    false,
			newSpec:       specWithVACName(nil),
			expectNewSpec: specWithVACName(nil),
			oldSpec:       specWithVACName(nil),
			expectOldSpec: specWithVACName(nil),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if !tc.vacEnabled {
				// 关闭特性时同时把模拟版本降到 1.35，覆盖按版本判断的分支。
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.35"))
			}
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.VolumeAttributesClass, tc.vacEnabled)

			DropDisabledSpecFields(tc.newSpec, tc.oldSpec)
			// 新旧 spec 都要校验：函数只应改动新对象，旧对象必须保持不变。
			if !reflect.DeepEqual(tc.newSpec, tc.expectNewSpec) {
				t.Error(cmp.Diff(tc.newSpec, tc.expectNewSpec))
			}
			if !reflect.DeepEqual(tc.oldSpec, tc.expectOldSpec) {
				t.Error(cmp.Diff(tc.oldSpec, tc.expectOldSpec))
			}
		})
	}
}

// specWithVACName 构造一个带 CSI 数据源的 PV spec，
// vacName 非 nil 时顺带设置 VolumeAttributesClassName。
func specWithVACName(vacName *string) *api.PersistentVolumeSpec {
	pvSpec := &api.PersistentVolumeSpec{
		PersistentVolumeSource: api.PersistentVolumeSource{
			CSI: &api.CSIPersistentVolumeSource{
				Driver:       "com.google.gcepd",
				VolumeHandle: "foobar",
			},
		},
	}

	if vacName != nil {
		pvSpec.VolumeAttributesClassName = vacName
	}
	return pvSpec
}

// TestWarnings 验证 GetWarningsForPersistentVolume 能覆盖各类弃用点：
// 旧注解、nodeAffinity 中的 beta 标签、Recycle 回收策略，以及多种已弃用的
// 存储插件；nil 对象和无问题的对象都不产生警告。
func TestWarnings(t *testing.T) {
	testcases := []struct {
		name     string
		template *api.PersistentVolume
		expected []string
	}{
		// nil 对象应安全返回，不产生警告。
		{
			name:     "null",
			template: nil,
			expected: nil,
		},
		// 普通 Bound PV 没有弃用字段，不告警。
		{
			name: "no warning",
			template: &api.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
				Status: api.PersistentVolumeStatus{
					Phase: api.VolumeBound,
				},
			},
			expected: nil,
		},
		// 两个旧注解和一个 nodeAffinity 中的 beta 标签，合计三条警告。
		{
			name: "warning",
			template: &api.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
					Annotations: map[string]string{
						api.BetaStorageClassAnnotation: "",
						api.MountOptionAnnotation:      "",
					},
				},
				Spec: api.PersistentVolumeSpec{
					NodeAffinity: &api.VolumeNodeAffinity{
						Required: &api.NodeSelector{
							NodeSelectorTerms: []api.NodeSelectorTerm{
								{
									MatchExpressions: []api.NodeSelectorRequirement{
										{
											Key:      "beta.kubernetes.io/os",
											Operator: "Equal",
											Values:   []string{"windows"},
										},
									},
								},
							},
						},
					},
				},
				Status: api.PersistentVolumeStatus{
					Phase: api.VolumeBound,
				},
			},
			expected: []string{
				`metadata.annotations[volume.beta.kubernetes.io/storage-class]: deprecated since v1.8; use "storageClassName" attribute instead`,
				`metadata.annotations[volume.beta.kubernetes.io/mount-options]: deprecated since v1.31; use "mountOptions" attribute instead`,
				`spec.nodeAffinity.required.nodeSelectorTerms[0].matchExpressions[0].key: beta.kubernetes.io/os is deprecated since v1.14; use "kubernetes.io/os" instead`,
			},
		},
		// Recycle 回收策略已弃用，应提示改用动态供给。
		{
			name: "PersistentVolumeReclaimRecycle deprecation warning",
			template: &api.PersistentVolume{
				Spec: api.PersistentVolumeSpec{
					PersistentVolumeReclaimPolicy: api.PersistentVolumeReclaimRecycle,
				},
			},
			expected: []string{
				`spec.persistentVolumeReclaimPolicy: The Recycle reclaim policy is deprecated. Instead, the recommended approach is to use dynamic provisioning.`,
			},
		},
		// 以下用例分别覆盖各已弃用的存储插件，各应产生一条弃用警告。
		{
			name: "PV CephFS deprecation warning",
			template: &api.PersistentVolume{
				Spec: api.PersistentVolumeSpec{
					PersistentVolumeSource: api.PersistentVolumeSource{
						CephFS: &api.CephFSPersistentVolumeSource{
							Monitors:   nil,
							Path:       "",
							User:       "",
							SecretFile: "",
							SecretRef:  nil,
							ReadOnly:   false,
						},
					},
				},
			},
			expected: []string{
				`spec.cephfs: deprecated in v1.28, non-functional in v1.31+`,
			},
		},
		{
			name: "PV PhotonPersistentDisk deprecation warning",
			template: &api.PersistentVolume{
				Spec: api.PersistentVolumeSpec{
					PersistentVolumeSource: api.PersistentVolumeSource{
						PhotonPersistentDisk: &api.PhotonPersistentDiskVolumeSource{
							PdID:   "",
							FSType: "",
						},
					},
				},
			},
			expected: []string{
				`spec.photonPersistentDisk: deprecated in v1.11, non-functional in v1.16+`,
			},
		},
		{
			name: "PV RBD deprecation warning",
			template: &api.PersistentVolume{
				Spec: api.PersistentVolumeSpec{
					PersistentVolumeSource: api.PersistentVolumeSource{
						RBD: &api.RBDPersistentVolumeSource{
							CephMonitors: nil,
							RBDImage:     "",
							FSType:       "",
							RBDPool:      "",
							RadosUser:    "",
							Keyring:      "",
							SecretRef:    nil,
							ReadOnly:     false,
						},
					},
				},
			},
			expected: []string{
				`spec.rbd: deprecated in v1.28, non-functional in v1.31+`},
		},
		{
			name: "PV ScaleIO deprecation warning",
			template: &api.PersistentVolume{
				Spec: api.PersistentVolumeSpec{
					PersistentVolumeSource: api.PersistentVolumeSource{
						ScaleIO: &api.ScaleIOPersistentVolumeSource{
							Gateway:          "",
							System:           "",
							SecretRef:        nil,
							SSLEnabled:       false,
							ProtectionDomain: "",
							StoragePool:      "",
							StorageMode:      "",
							VolumeName:       "",
							FSType:           "",
							ReadOnly:         false,
						},
					},
				},
			},
			expected: []string{
				`spec.scaleIO: deprecated in v1.16, non-functional in v1.22+`,
			},
		},
		{
			name: "PV StorageOS deprecation warning",
			template: &api.PersistentVolume{
				Spec: api.PersistentVolumeSpec{
					PersistentVolumeSource: api.PersistentVolumeSource{
						StorageOS: &api.StorageOSPersistentVolumeSource{
							VolumeName:      "",
							VolumeNamespace: "",
							FSType:          "",
							ReadOnly:        false,
							SecretRef:       nil,
						},
					},
				},
			},
			expected: []string{
				`spec.storageOS: deprecated in v1.22, non-functional in v1.25+`,
			},
		},
		{
			name: "PV GlusterFS deprecation warning",
			template: &api.PersistentVolume{
				Spec: api.PersistentVolumeSpec{
					PersistentVolumeSource: api.PersistentVolumeSource{
						Glusterfs: &api.GlusterfsPersistentVolumeSource{
							EndpointsName:      "",
							Path:               "",
							ReadOnly:           false,
							EndpointsNamespace: nil,
						},
					},
				},
			},
			expected: []string{
				`spec.glusterfs: deprecated in v1.25, non-functional in v1.26+`,
			},
		},
	}

	for _, tc := range testcases {
		t.Run("podspec_"+tc.name, func(t *testing.T) {
			// 用集合比较：注解和 nodeSelector 来自 map，警告顺序不稳定。
			actual := sets.New[string](GetWarningsForPersistentVolume(tc.template)...)
			expected := sets.New[string](tc.expected...)
			for _, missing := range sets.List[string](expected.Difference(actual)) {
				t.Errorf("missing: %s", missing)
			}
			for _, extra := range sets.List[string](actual.Difference(expected)) {
				t.Errorf("extra: %s", extra)
			}
		})

	}
}
