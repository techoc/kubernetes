/*
Copyright 2020 The Kubernetes Authors.

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

package plugins

import (
	"fmt"
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2/ktesting"
	_ "k8s.io/klog/v2/ktesting/init"
)

// TestTranslatevSphereInTreeStorageClassToCSI 验证 StorageClass 的参数与拓扑翻译：
// 参数改名、未知参数丢弃、迁移标志注入以及 GA/Beta zone 键转换。
func TestTranslatevSphereInTreeStorageClassToCSI(t *testing.T) {
	translator := NewvSphereCSITranslator()
	logger, _ := ktesting.NewTestContext(t)
	// 构造 GA/Beta 两种 zone 标签选择器，以及期望的 CSI 拓扑选择器。
	topologySelectorTerm := v1.TopologySelectorTerm{MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
		{
			Key:    v1.LabelTopologyZone,
			Values: []string{"zone-a"},
		},
	}}
	topologySelectorTermWithBetaLabels := v1.TopologySelectorTerm{MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
		{
			Key:    v1.LabelFailureDomainBetaZone,
			Values: []string{"zone-a"},
		},
	}}
	expectedTopologySelectorTerm := v1.TopologySelectorTerm{MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
		{
			Key:    vSphereCSITopologyZoneKey,
			Values: []string{"zone-a"},
		},
	}}
	cases := []struct {
		name   string
		sc     *storage.StorageClass
		expSc  *storage.StorageClass
		expErr bool
	}{
		{
			// nil StorageClass 应报错。
			name:   "expect error when sc is nil",
			sc:     nil,
			expSc:  nil,
			expErr: true,
		},
		{
			// 即使没有参数，也要注入迁移标志。
			name:  "translate with no parameter",
			sc:    NewStorageClass(map[string]string{}, nil),
			expSc: NewStorageClass(map[string]string{paramcsiMigration: "true"}, nil),
		},
		{
			// 未知参数被丢弃，仅保留迁移标志。
			name:  "translate with unknown parameter",
			sc:    NewStorageClass(map[string]string{"unknownparam": "value"}, nil),
			expSc: NewStorageClass(map[string]string{paramcsiMigration: "true"}, nil),
		},
		{
			// datastore 应改名为 datastore-migrationparam。
			name:  "translate with storagepolicyname and datastore",
			sc:    NewStorageClass(map[string]string{"storagepolicyname": "test-policy-name", "datastore": "vsanDatastore"}, nil),
			expSc: NewStorageClass(map[string]string{"storagepolicyname": "test-policy-name", "datastore-migrationparam": "vsanDatastore", paramcsiMigration: "true"}, nil),
		},
		{
			// fstype 改名为 CSI 的 fstype 参数键。
			name:  "translate with fstype",
			sc:    NewStorageClass(map[string]string{"fstype": "ext4"}, nil),
			expSc: NewStorageClass(map[string]string{"csi.storage.k8s.io/fstype": "ext4", paramcsiMigration: "true"}, nil),
		},
		{
			// 两项同时出现时应都保留。
			name:  "translate with storagepolicyname and fstype",
			sc:    NewStorageClass(map[string]string{"storagepolicyname": "test-policy-name", "fstype": "ext4"}, nil),
			expSc: NewStorageClass(map[string]string{"csi.storage.k8s.io/fstype": "ext4", "storagepolicyname": "test-policy-name", paramcsiMigration: "true"}, nil),
		},
		{
			// GA zone 拓扑选择器应改为 vSphere CSI 拓扑键。
			name:  "translate with no parameter and allowedTopology",
			sc:    NewStorageClass(map[string]string{}, []v1.TopologySelectorTerm{topologySelectorTerm}),
			expSc: NewStorageClass(map[string]string{paramcsiMigration: "true"}, []v1.TopologySelectorTerm{expectedTopologySelectorTerm}),
		},
		{
			// 参数与拓扑需要同时翻译。
			name:  "translate with storagepolicyname and allowedTopology",
			sc:    NewStorageClass(map[string]string{"storagepolicyname": "test-policy-name"}, []v1.TopologySelectorTerm{topologySelectorTerm}),
			expSc: NewStorageClass(map[string]string{"storagepolicyname": "test-policy-name", paramcsiMigration: "true"}, []v1.TopologySelectorTerm{expectedTopologySelectorTerm}),
		},
		{
			// Beta zone 标签同样要改为 CSI 拓扑键。
			name:  "translate with storagepolicyname and allowedTopology beta labels",
			sc:    NewStorageClass(map[string]string{"storagepolicyname": "test-policy-name"}, []v1.TopologySelectorTerm{topologySelectorTermWithBetaLabels}),
			expSc: NewStorageClass(map[string]string{"storagepolicyname": "test-policy-name", paramcsiMigration: "true"}, []v1.TopologySelectorTerm{expectedTopologySelectorTerm}),
		},
		{
			// vSAN 原始参数改名，并叠加迁移标志与拓扑翻译。
			name:  "translate with raw vSAN policy parameters, datastore and diskformat",
			sc:    NewStorageClass(map[string]string{"hostfailurestotolerate": "2", "datastore": "vsanDatastore", "diskformat": "thin"}, []v1.TopologySelectorTerm{topologySelectorTerm}),
			expSc: NewStorageClass(map[string]string{"hostfailurestotolerate-migrationparam": "2", "datastore-migrationparam": "vsanDatastore", "diskformat-migrationparam": "thin", paramcsiMigration: "true"}, []v1.TopologySelectorTerm{expectedTopologySelectorTerm}),
		},
		{
			// 覆盖所有支持的参数，验证完整映射。
			name:  "translate with all parameters",
			sc:    NewStorageClass(map[string]string{"storagepolicyname": "test-policy-name", "datastore": "test-datastore-name", "fstype": "ext4", "diskformat": "thin", "hostfailurestotolerate": "1", "forceprovisioning": "yes", "cachereservation": "25", "diskstripes": "4", "objectspacereservation": "10", "iopslimit": "32"}, []v1.TopologySelectorTerm{topologySelectorTerm}),
			expSc: NewStorageClass(map[string]string{"storagepolicyname": "test-policy-name", "datastore-migrationparam": "test-datastore-name", "csi.storage.k8s.io/fstype": "ext4", "diskformat-migrationparam": "thin", "hostfailurestotolerate-migrationparam": "1", "forceprovisioning-migrationparam": "yes", "cachereservation-migrationparam": "25", "diskstripes-migrationparam": "4", "objectspacereservation-migrationparam": "10", "iopslimit-migrationparam": "32", paramcsiMigration: "true"}, []v1.TopologySelectorTerm{expectedTopologySelectorTerm}),
		},
	}
	for _, tc := range cases {
		t.Logf("Testing %v", tc.name)
		// 逐例执行转换并比较结果。
		got, err := translator.TranslateInTreeStorageClassToCSI(logger, tc.sc)
		if err != nil && !tc.expErr {
			t.Errorf("Did not expect error but got: %v", err)
		}

		if err == nil && tc.expErr {
			t.Errorf("Expected error, but did not get one.")
		}
		if !reflect.DeepEqual(got, tc.expSc) {
			t.Errorf("Got parameters: %v, expected :%v", got, tc.expSc)
		}
	}
}

// TestTranslateVSphereCSIPVToInTree 验证 CSI PV 到树内 PV 的反向转换，
// 覆盖卷路径还原、错误分支以及 CSI 拓扑到树内标签的转换。
func TestTranslateVSphereCSIPVToInTree(t *testing.T) {
	translator := NewvSphereCSITranslator()
	cases := []struct {
		name     string
		csiPV    *v1.PersistentVolume
		intreePV *v1.PersistentVolume
		expErr   bool
	}{
		{
			// nil PV 应报错。
			name:     "expect error when pv is nil",
			csiPV:    nil,
			intreePV: nil,
			expErr:   true,
		},
		{
			// 缺少 CSI 来源应报错。
			name: "expect error when pv.Spec.CSI is nil",
			csiPV: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pvc-d8b4475f-2c47-486e-9b57-43ae006f9b59",
				},
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						CSI: nil,
					},
					AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
				},
			},
			intreePV: nil,
			expErr:   true,
		},
		{
			// 正常情况：VolumePath 取自 initialvolumefilepath 属性，FSType 保留。
			name: "translate valid vSphere CSI PV to vSphere in-tree PV",
			csiPV: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pvc-d8b4475f-2c47-486e-9b57-43ae006f9b59",
				},
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						CSI: &v1.CSIPersistentVolumeSource{
							Driver:       VSphereDriverName,
							VolumeHandle: "e4073a6d-642e-4dff-8f4a-b4e3a47c4bbd",
							FSType:       "ext4",
							VolumeAttributes: map[string]string{
								paramStoragePolicyName:         "vSAN Default Storage Policy",
								AttributeInitialVolumeFilepath: "[vsanDatastore] 6785a85e-268e-6352-a2e8-02008b7afadd/kubernetes-dynamic-pvc-68734c9f-a679-42e6-a694-39632c51e31f.vmdk",
							},
						},
					},
					AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
				},
			},
			intreePV: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pvc-d8b4475f-2c47-486e-9b57-43ae006f9b59",
				},
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						VsphereVolume: &v1.VsphereVirtualDiskVolumeSource{
							VolumePath: "[vsanDatastore] 6785a85e-268e-6352-a2e8-02008b7afadd/kubernetes-dynamic-pvc-68734c9f-a679-42e6-a694-39632c51e31f.vmdk",
							FSType:     "ext4",
						},
					},
					AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
				},
			},
			expErr: false,
		},
		{
			// 带 CSI 拓扑的 PV 应转换为树内标签与 NodeAffinity。
			name: "translate valid vSphere CSI PV with topology Node Affinity rules to vSphere CSI PV with topology labels",
			csiPV: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pvc-d8b4475f-2c47-486e-9b57-43ae006f9b59",
				},
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						CSI: &v1.CSIPersistentVolumeSource{
							Driver:       VSphereDriverName,
							VolumeHandle: "e4073a6d-642e-4dff-8f4a-b4e3a47c4bbd",
							FSType:       "ext4",
							VolumeAttributes: map[string]string{
								paramStoragePolicyName:         "vSAN Default Storage Policy",
								AttributeInitialVolumeFilepath: "[vsanDatastore] 6785a85e-268e-6352-a2e8-02008b7afadd/kubernetes-dynamic-pvc-68734c9f-a679-42e6-a694-39632c51e31f.vmdk",
							},
						},
					},
					AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
					NodeAffinity: &v1.VolumeNodeAffinity{
						Required: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      "topology.csi.vmware.com/zone",
											Operator: v1.NodeSelectorOpIn,
											Values:   []string{"z1"},
										},
										{
											Key:      "topology.csi.vmware.com/region",
											Operator: v1.NodeSelectorOpIn,
											Values:   []string{"r1"},
										},
									},
								},
							},
						},
					},
				},
			},
			intreePV: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "pvc-d8b4475f-2c47-486e-9b57-43ae006f9b59",
					Labels: map[string]string{"topology.kubernetes.io/zone": "z1", "topology.kubernetes.io/region": "r1"},
				},
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						VsphereVolume: &v1.VsphereVirtualDiskVolumeSource{
							VolumePath: "[vsanDatastore] 6785a85e-268e-6352-a2e8-02008b7afadd/kubernetes-dynamic-pvc-68734c9f-a679-42e6-a694-39632c51e31f.vmdk",
							FSType:     "ext4",
						},
					},
					AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
					NodeAffinity: &v1.VolumeNodeAffinity{
						Required: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      "topology.kubernetes.io/zone",
											Operator: v1.NodeSelectorOpIn,
											Values:   []string{"z1"},
										},
										{
											Key:      "topology.kubernetes.io/region",
											Operator: v1.NodeSelectorOpIn,
											Values:   []string{"r1"},
										},
									},
								},
							},
						},
					},
				},
			},
			expErr: false,
		},
	}

	for _, tc := range cases {
		t.Logf("Testing %v", tc.name)
		// 逐例执行反向转换并比较结果。
		got, err := translator.TranslateCSIPVToInTree(tc.csiPV)
		if err != nil && !tc.expErr {
			t.Errorf("Did not expect error but got: %v", err)
		}

		if err == nil && tc.expErr {
			t.Errorf("Expected error, but did not get one.")
		}

		if !reflect.DeepEqual(got, tc.intreePV) {
			t.Errorf("Got PV: %v, expected :%v", got, tc.intreePV)
		}
	}
}

// TestTranslateVSphereInTreePVToCSI 验证树内 PV 到 CSI PV 的转换，
// 覆盖卷路径作为 VolumeHandle、策略名写入卷属性以及 zone/region 标签转换。
func TestTranslateVSphereInTreePVToCSI(t *testing.T) {
	translator := NewvSphereCSITranslator()
	logger, _ := ktesting.NewTestContext(t)
	cases := []struct {
		name     string
		intreePV *v1.PersistentVolume
		csiPV    *v1.PersistentVolume
		expErr   bool
	}{
		{
			// 缺少 VsphereVolume 来源应报错。
			name:     "expect error when in-tree vsphere PV is nil",
			intreePV: &v1.PersistentVolume{},
			csiPV:    nil,
			expErr:   true,
		},
		{
			// 正常情况：VolumePath 成为 VolumeHandle，策略名进入卷属性。
			name: "translate valid vSphere in-tree PV to vSphere CSI PV",
			intreePV: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pvc-d8b4475f-2c47-486e-9b57-43ae006f9b59",
				},
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						VsphereVolume: &v1.VsphereVirtualDiskVolumeSource{
							VolumePath:        "[vsanDatastore] 6785a85e-268e-6352-a2e8-02008b7afadd/kubernetes-dynamic-pvc-68734c9f-a679-42e6-a694-39632c51e31f.vmdk",
							FSType:            "ext4",
							StoragePolicyName: "vSAN Default Storage Policy",
						},
					},
					AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
				},
			},
			csiPV: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pvc-d8b4475f-2c47-486e-9b57-43ae006f9b59",
				},
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						CSI: &v1.CSIPersistentVolumeSource{
							Driver:       VSphereDriverName,
							VolumeHandle: "[vsanDatastore] 6785a85e-268e-6352-a2e8-02008b7afadd/kubernetes-dynamic-pvc-68734c9f-a679-42e6-a694-39632c51e31f.vmdk",
							FSType:       "ext4",
							VolumeAttributes: map[string]string{
								paramStoragePolicyName: "vSAN Default Storage Policy",
							},
						},
					},
					AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
				},
			},
			expErr: false,
		},
		{
			// Beta 标签应转换为 CSI 拓扑 NodeAffinity，原标签保留。
			name: "translate valid vSphere in-tree PV with beta topology labels to vSphere CSI PV",
			intreePV: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "pvc-d8b4475f-2c47-486e-9b57-43ae006f9b59",
					Labels: map[string]string{"failure-domain.beta.kubernetes.io/zone": "z1", "failure-domain.beta.kubernetes.io/region": "r1"},
				},
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						VsphereVolume: &v1.VsphereVirtualDiskVolumeSource{
							VolumePath:        "[vsanDatastore] 6785a85e-268e-6352-a2e8-02008b7afadd/kubernetes-dynamic-pvc-68734c9f-a679-42e6-a694-39632c51e31f.vmdk",
							FSType:            "ext4",
							StoragePolicyName: "vSAN Default Storage Policy",
						},
					},
					AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
				},
			},
			csiPV: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "pvc-d8b4475f-2c47-486e-9b57-43ae006f9b59",
					Labels: map[string]string{"failure-domain.beta.kubernetes.io/zone": "z1", "failure-domain.beta.kubernetes.io/region": "r1"},
				},
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						CSI: &v1.CSIPersistentVolumeSource{
							Driver:       VSphereDriverName,
							VolumeHandle: "[vsanDatastore] 6785a85e-268e-6352-a2e8-02008b7afadd/kubernetes-dynamic-pvc-68734c9f-a679-42e6-a694-39632c51e31f.vmdk",
							FSType:       "ext4",
							VolumeAttributes: map[string]string{
								paramStoragePolicyName: "vSAN Default Storage Policy",
							},
						},
					},
					AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
					NodeAffinity: &v1.VolumeNodeAffinity{
						Required: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      "topology.csi.vmware.com/zone",
											Operator: v1.NodeSelectorOpIn,
											Values:   []string{"z1"},
										},
										{
											Key:      "topology.csi.vmware.com/region",
											Operator: v1.NodeSelectorOpIn,
											Values:   []string{"r1"},
										},
									},
								},
							},
						},
					},
				},
			},
			expErr: false,
		},
		{
			// GA 标签同样转换为 CSI 拓扑。
			name: "translate valid vSphere in-tree PV with GA topology labels to vSphere CSI PV",
			intreePV: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "pvc-d8b4475f-2c47-486e-9b57-43ae006f9b59",
					Labels: map[string]string{"topology.kubernetes.io/zone": "z1", "topology.kubernetes.io/region": "r1"},
				},
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						VsphereVolume: &v1.VsphereVirtualDiskVolumeSource{
							VolumePath:        "[vsanDatastore] 6785a85e-268e-6352-a2e8-02008b7afadd/kubernetes-dynamic-pvc-68734c9f-a679-42e6-a694-39632c51e31f.vmdk",
							FSType:            "ext4",
							StoragePolicyName: "vSAN Default Storage Policy",
						},
					},
					AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
				},
			},
			csiPV: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "pvc-d8b4475f-2c47-486e-9b57-43ae006f9b59",
					Labels: map[string]string{"topology.kubernetes.io/zone": "z1", "topology.kubernetes.io/region": "r1"},
				},
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						CSI: &v1.CSIPersistentVolumeSource{
							Driver:       VSphereDriverName,
							VolumeHandle: "[vsanDatastore] 6785a85e-268e-6352-a2e8-02008b7afadd/kubernetes-dynamic-pvc-68734c9f-a679-42e6-a694-39632c51e31f.vmdk",
							FSType:       "ext4",
							VolumeAttributes: map[string]string{
								paramStoragePolicyName: "vSAN Default Storage Policy",
							},
						},
					},
					AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
					NodeAffinity: &v1.VolumeNodeAffinity{
						Required: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      "topology.csi.vmware.com/zone",
											Operator: v1.NodeSelectorOpIn,
											Values:   []string{"z1"},
										},
										{
											Key:      "topology.csi.vmware.com/region",
											Operator: v1.NodeSelectorOpIn,
											Values:   []string{"r1"},
										},
									},
								},
							},
						},
					},
				},
			},
			expErr: false,
		},
	}

	for _, tc := range cases {
		t.Logf("Testing %v", tc.name)
		// 逐例执行转换并比较结果。
		got, err := translator.TranslateInTreePVToCSI(logger, tc.intreePV)
		if err != nil && !tc.expErr {
			t.Errorf("Did not expect error but got: %v", err)
		}

		if err == nil && tc.expErr {
			t.Errorf("Expected error, but did not get one.")
		}

		if !reflect.DeepEqual(got, tc.csiPV) {
			t.Errorf("Got PV: %v, expected :%v", got, tc.csiPV)
		}
	}
}

// TestTranslatevSphereInTreeInlineVolumeToCSI 验证 vSphere 内联卷转换，
// 包括缺少来源时的错误分支和 PV 名称/来源字段的生成。
func TestTranslatevSphereInTreeInlineVolumeToCSI(t *testing.T) {
	translator := NewvSphereCSITranslator()
	logger, _ := ktesting.NewTestContext(t)
	cases := []struct {
		name         string
		inlinevolume *v1.Volume
		csiPV        *v1.PersistentVolume
		expErr       bool
	}{
		{
			// 没有 VsphereVolume 的内联卷应报错。
			name:         "expect error when inline vsphere volume is nil",
			inlinevolume: &v1.Volume{},
			csiPV:        nil,
			expErr:       true,
		},
		{
			// 正常情况：生成 CSI PV，名称由驱动名与卷路径拼接。
			name: "translate valid in-tree vsphere volume to vSphere CSI PV",
			inlinevolume: &v1.Volume{
				Name: "inlinevolume",
				VolumeSource: v1.VolumeSource{
					VsphereVolume: &v1.VsphereVirtualDiskVolumeSource{
						VolumePath: "[vsanDatastore] volume/inlinevolume.vmdk",
						FSType:     "ext4",
					},
				}},
			csiPV: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: fmt.Sprintf("%s-%s", VSphereDriverName, "[vsanDatastore] volume/inlinevolume.vmdk"),
				},
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						CSI: &v1.CSIPersistentVolumeSource{
							Driver:           VSphereDriverName,
							VolumeHandle:     "[vsanDatastore] volume/inlinevolume.vmdk",
							FSType:           "ext4",
							VolumeAttributes: make(map[string]string),
						},
					},
					AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
				},
			},
			expErr: false,
		},
	}

	for _, tc := range cases {
		t.Logf("Testing %v", tc.name)
		// 逐例执行转换；该测试单独处理错误分支，随后才做结果比较。
		got, err := translator.TranslateInTreeInlineVolumeToCSI(logger, tc.inlinevolume, "")
		if err == nil && tc.expErr {
			t.Errorf("Expected error, but did not get one.")
			continue
		}
		if err != nil {
			if tc.expErr {
				t.Logf("expected error received")
				continue
			} else {
				t.Errorf("Did not expect error but got: %v", err)
				continue
			}
		}
		if !reflect.DeepEqual(got, tc.csiPV) {
			t.Errorf("Got PV: %v, expected :%v", got, tc.csiPV)
		}
	}
}
