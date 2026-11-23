/*
Copyright 2019 The Kubernetes Authors.

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

// NewStorageClass 是测试辅助函数，用给定参数和拓扑约束快速构造 StorageClass。
func NewStorageClass(params map[string]string, allowedTopologies []v1.TopologySelectorTerm) *storage.StorageClass {
	return &storage.StorageClass{
		// 只填充与翻译相关的两个字段，其余留零值。
		Parameters:        params,
		AllowedTopologies: allowedTopologies,
	}
}

// TestTranslatePDInTreeStorageClassToCSI 验证 StorageClass 参数翻译：普通参数透传、fstype 改名、zone/zones 转拓扑以及旧拓扑键转换。
func TestTranslatePDInTreeStorageClassToCSI(t *testing.T) {
	g := NewGCEPersistentDiskCSITranslator()
	logger, _ := ktesting.NewTestContext(t)

	// tcs 覆盖参数翻译与拓扑转换的各种组合。
	tcs := []struct {
		name       string
		options    *storage.StorageClass
		expOptions *storage.StorageClass
		expErr     bool
	}{
		// 普通参数原样保留。
		{
			name:       "nothing special",
			options:    NewStorageClass(map[string]string{"foo": "bar"}, nil),
			expOptions: NewStorageClass(map[string]string{"foo": "bar"}, nil),
		},
		// fstype 应被改写成 CSI 驱动识别的完整键名。
		{
			name:       "fstype",
			options:    NewStorageClass(map[string]string{"fstype": "myfs"}, nil),
			expOptions: NewStorageClass(map[string]string{"csi.storage.k8s.io/fstype": "myfs"}, nil),
		},
		// 空参数集应保持为空。
		{
			name:       "empty params",
			options:    NewStorageClass(map[string]string{}, nil),
			expOptions: NewStorageClass(map[string]string{}, nil),
		},
		// zone 参数应转换为单值拓扑选择器，并从参数中移除。
		{
			name:       "zone",
			options:    NewStorageClass(map[string]string{"zone": "foo"}, nil),
			expOptions: NewStorageClass(map[string]string{}, generateToplogySelectors(GCEPDTopologyKey, []string{"foo"})),
		},
		// zones 参数按逗号拆分成多个值后转换为拓扑选择器。
		{
			name:       "zones",
			options:    NewStorageClass(map[string]string{"zones": "foo,bar,baz"}, nil),
			expOptions: NewStorageClass(map[string]string{}, generateToplogySelectors(GCEPDTopologyKey, []string{"foo", "bar", "baz"})),
		},
		// 已经是 CSI 拓扑键的 allowedTopologies 应保持不变。
		{
			name:       "some normal topology",
			options:    NewStorageClass(map[string]string{}, generateToplogySelectors(GCEPDTopologyKey, []string{"foo"})),
			expOptions: NewStorageClass(map[string]string{}, generateToplogySelectors(GCEPDTopologyKey, []string{"foo"})),
		},
		// 旧 beta 拓扑键应被翻译成 CSI 拓扑键。
		{
			name:       "some translated topology",
			options:    NewStorageClass(map[string]string{}, generateToplogySelectors(v1.LabelFailureDomainBetaZone, []string{"foo"})),
			expOptions: NewStorageClass(map[string]string{}, generateToplogySelectors(GCEPDTopologyKey, []string{"foo"})),
		},
		// 同时设置 zone 与 allowedTopologies 属于冲突输入，应报错。
		{
			name:    "zone and topology",
			options: NewStorageClass(map[string]string{"zone": "foo"}, generateToplogySelectors(GCEPDTopologyKey, []string{"foo"})),
			expErr:  true,
		},
	}

	// 逐用例执行翻译并比较结果。
	for _, tc := range tcs {
		t.Logf("Testing %v", tc.name)
		gotOptions, err := g.TranslateInTreeStorageClassToCSI(logger, tc.options)
		if err != nil && !tc.expErr {
			t.Errorf("Did not expect error but got: %v", err)
		}
		if err == nil && tc.expErr {
			t.Errorf("Expected error, but did not get one.")
		}
		if !reflect.DeepEqual(gotOptions, tc.expOptions) {
			t.Errorf("Got parameters: %v, expected :%v", gotOptions, tc.expOptions)
		}
	}
}

// TestRepairVolumeHandle 验证用节点 ID 补全 volume handle 中 UNSPECIFIED 字段的逻辑，覆盖区域级与地区级 handle 以及非法输入。
func TestRepairVolumeHandle(t *testing.T) {
	// testCases 中 nodeID 提供项目与可用区，expectedVolumeHandle 是补全后的期望结果。
	testCases := []struct {
		name                 string
		volumeHandle         string
		nodeID               string
		expectedVolumeHandle string
		expectedErr          bool
	}{
		// handle 已经完整：应原样返回。
		{
			name:                 "fully specified",
			volumeHandle:         fmt.Sprintf(volIDZonalFmt, "foo", "bar", "baz"),
			nodeID:               fmt.Sprintf(nodeIDFmt, "bing", "bada", "boom"),
			expectedVolumeHandle: fmt.Sprintf(volIDZonalFmt, "foo", "bar", "baz"),
		},
		// 地区级 handle 已经完整：同样原样返回。
		{
			name:                 "fully specified (regional)",
			volumeHandle:         fmt.Sprintf(volIDRegionalFmt, "foo", "us-central1-c", "baz"),
			nodeID:               fmt.Sprintf(nodeIDFmt, "bing", "bada", "boom"),
			expectedVolumeHandle: fmt.Sprintf(volIDRegionalFmt, "foo", "us-central1-c", "baz"),
		},
		// 只有项目未指定：从节点 ID 取项目，可用区沿用 handle 中的值。
		{
			name:                 "no project",
			volumeHandle:         fmt.Sprintf(volIDZonalFmt, UnspecifiedValue, "bar", "baz"),
			nodeID:               fmt.Sprintf(nodeIDFmt, "bing", "bada", "boom"),
			expectedVolumeHandle: fmt.Sprintf(volIDZonalFmt, "bing", "bar", "baz"),
		},
		// 项目和可用区都未指定：两者都从节点 ID 补全。
		{
			name:                 "no project or zone",
			volumeHandle:         fmt.Sprintf(volIDZonalFmt, UnspecifiedValue, UnspecifiedValue, "baz"),
			nodeID:               fmt.Sprintf(nodeIDFmt, "bing", "bada", "boom"),
			expectedVolumeHandle: fmt.Sprintf(volIDZonalFmt, "bing", "bada", "baz"),
		},
		// 地区级 handle 项目和地区都未指定：地区从节点可用区 us-central1-c 推导为 us-central1。
		{
			name:                 "no project or region",
			volumeHandle:         fmt.Sprintf(volIDRegionalFmt, UnspecifiedValue, UnspecifiedValue, "baz"),
			nodeID:               fmt.Sprintf(nodeIDFmt, "bing", "us-central1-c", "boom"),
			expectedVolumeHandle: fmt.Sprintf(volIDRegionalFmt, "bing", "us-central1", "baz"),
		},
		// 地区级只缺项目：地区沿用 handle 中已有的 us-west1。
		{
			name:                 "no project (regional)",
			volumeHandle:         fmt.Sprintf(volIDRegionalFmt, UnspecifiedValue, "us-west1", "baz"),
			nodeID:               fmt.Sprintf(nodeIDFmt, "bing", "us-central1-c", "boom"),
			expectedVolumeHandle: fmt.Sprintf(volIDRegionalFmt, "bing", "us-west1", "baz"),
		},
		// handle 分段数不足：应报错。
		{
			name:         "invalid handle",
			volumeHandle: "foo",
			nodeID:       fmt.Sprintf(nodeIDFmt, "bing", "us-central1-c", "boom"),
			expectedErr:  true,
		},
		// 节点 ID 分段数不足：应报错。
		{
			name:         "invalid node ID",
			volumeHandle: fmt.Sprintf(volIDRegionalFmt, UnspecifiedValue, "us-west1", "baz"),
			nodeID:       "foo",
			expectedErr:  true,
		},
	}
	g := NewGCEPersistentDiskCSITranslator()
	// 逐用例运行，并分别断言错误与补全后的 handle。
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotVolumeHandle, err := g.RepairVolumeHandle(tc.volumeHandle, tc.nodeID)
			if err != nil && !tc.expectedErr {
				if !tc.expectedErr {
					t.Fatalf("Got error: %v, but expected none", err)
				}
				return
			}
			if err == nil && tc.expectedErr {
				t.Fatal("Got no error, but expected one")
			}

			if gotVolumeHandle != tc.expectedVolumeHandle {
				t.Fatalf("Got volume handle %s, but expected %s", gotVolumeHandle, tc.expectedVolumeHandle)
			}
		})
	}
}

// TestBackwardCompatibleAccessModes 验证遗留访问模式到 CSI 访问模式的映射：RWX 降级为 RWO，RWO+ROX 组合折叠为 RWO。
func TestBackwardCompatibleAccessModes(t *testing.T) {
	// testCases 中 accessModes 是树内遗留值，expAccessModes 是 CSI 可接受的期望值。
	testCases := []struct {
		name           string
		accessModes    []v1.PersistentVolumeAccessMode
		expAccessModes []v1.PersistentVolumeAccessMode
	}{
		// 单独 ROX 合法，保持不变。
		{
			name: "ROX",
			accessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadOnlyMany,
			},
			expAccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadOnlyMany,
			},
		},
		// 单独 RWO 合法，保持不变。
		{
			name: "RWO",
			accessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			expAccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
		},
		// RWX 在 CSI 中不被 GCE PD 支持，降级为 RWO。
		{
			name: "RWX",
			accessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteMany,
			},
			expAccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
		},
		// RWO+ROX 组合在 CSI 中不可支持，折叠为 RWO。
		{
			name: "RWO, ROX",
			accessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadOnlyMany,
				v1.ReadWriteOnce,
			},
			expAccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
		},
		// RWO+RWX 中 RWX 降级后去重，结果为单个 RWO。
		{
			name: "RWO, RWX",
			accessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
				v1.ReadWriteMany,
			},
			expAccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
		},
		// RWX+ROX：RWX 降级为 RWO 后与 ROX 组合，仍折叠为 RWO。
		{
			name: "RWX, ROX",
			accessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteMany,
				v1.ReadOnlyMany,
			},
			expAccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
		},
		// 三种模式同时出现时同样折叠为 RWO。
		{
			name: "RWX, ROX, RWO",
			accessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteMany,
				v1.ReadWriteOnce,
				v1.ReadOnlyMany,
			},
			expAccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
		},
	}

	// 逐用例调用函数并比较结果。
	for _, tc := range testCases {
		t.Logf("running test: %v", tc.name)

		got := backwardCompatibleAccessModes(tc.accessModes)

		if !reflect.DeepEqual(tc.expAccessModes, got) {
			t.Fatalf("Expected access modes: %v, instead got: %v", tc.expAccessModes, got)
		}
	}
}

// TestInlineReadOnly 验证只读的内联 GCE PD 卷翻译后仍保留只读标志，并映射为 ReadOnlyMany 访问模式。
func TestInlineReadOnly(t *testing.T) {
	g := NewGCEPersistentDiskCSITranslator()
	logger, _ := ktesting.NewTestContext(t)
	// 构造一个 ReadOnly=true 的内联卷；Pod 命名空间为空。
	pv, err := g.TranslateInTreeInlineVolumeToCSI(logger, &v1.Volume{
		VolumeSource: v1.VolumeSource{
			GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
				PDName:   "foo",
				ReadOnly: true,
			},
		},
	}, "")
	if err != nil {
		t.Fatalf("Failed to translate in tree inline volume to CSI: %v", err)
	}

	// 首先确认翻译结果非空且确实是 CSI 来源。
	if pv == nil || pv.Spec.PersistentVolumeSource.CSI == nil {
		t.Fatal("PV or volume source unexpectedly nil")
	}

	// CSI 来源的 ReadOnly 必须继承输入值。
	if !pv.Spec.PersistentVolumeSource.CSI.ReadOnly {
		t.Error("PV readonly value not true")
	}

	// 只读卷只应有一个访问模式。
	ams := pv.Spec.AccessModes
	if len(ams) != 1 {
		t.Errorf("got am %v, expected length of 1", ams)
	}

	// 该访问模式应为 ReadOnlyMany。
	if ams[0] != v1.ReadOnlyMany {
		t.Errorf("got am %v, expected access mode of ReadOnlyMany", ams[0])
	}
}

// TestTranslateInTreePVToCSIVolIDFmt 验证不同拓扑标签（beta/GA）与不同取值（单区/多区）下生成的 volume handle 格式。
func TestTranslateInTreePVToCSIVolIDFmt(t *testing.T) {
	g := NewGCEPersistentDiskCSITranslator()
	logger, _ := ktesting.NewTestContext(t)
	pdName := "pd-name"
	// tests 用不同标签键和标签值覆盖区域级与地区级 handle 的生成路径。
	tests := []struct {
		desc               string
		topologyLabelKey   string
		topologyLabelValue string
		wantVolId          string
	}{
		// 旧 beta 标签 + 单个可用区：生成区域级 handle。
		{
			desc:               "beta topology key zonal",
			topologyLabelKey:   v1.LabelFailureDomainBetaZone,
			topologyLabelValue: "us-east1-a",
			wantVolId:          "projects/UNSPECIFIED/zones/us-east1-a/disks/pd-name",
		},
		// GA 标签 + 单个可用区：结果应与 beta 标签一致。
		{
			desc:               "v1 topology key zonal",
			topologyLabelKey:   v1.LabelTopologyZone,
			topologyLabelValue: "us-east1-a",
			wantVolId:          "projects/UNSPECIFIED/zones/us-east1-a/disks/pd-name",
		},
		// 旧 beta 标签 + "__" 分隔的多个可用区：推导出地区 us-central1。
		{
			desc:               "beta topology key regional",
			topologyLabelKey:   v1.LabelFailureDomainBetaZone,
			topologyLabelValue: "us-central1-a__us-central1-c",
			wantVolId:          "projects/UNSPECIFIED/regions/us-central1/disks/pd-name",
		},
		// GA 标签 + 多个可用区：同样生成地区级 handle。
		{
			desc:               "v1 topology key regional",
			topologyLabelKey:   v1.LabelTopologyZone,
			topologyLabelValue: "us-central1-a__us-central1-c",
			wantVolId:          "projects/UNSPECIFIED/regions/us-central1/disks/pd-name",
		},
	}
	// 逐用例构造带指定标签的 PV，翻译后只断言 volume handle。
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			translatedPV, err := g.TranslateInTreePVToCSI(logger, &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					// 把用例指定的拓扑标签放到 PV 的 Labels 上。
					Labels: map[string]string{tc.topologyLabelKey: tc.topologyLabelValue},
				},
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName: pdName,
						},
					},
				},
			})
			if err != nil {
				t.Errorf("got error translating in-tree PV to CSI: %v", err)
			}
			if got := translatedPV.Spec.PersistentVolumeSource.CSI.VolumeHandle; got != tc.wantVolId {
				t.Errorf("got translated volume handle: %q, want %q", got, tc.wantVolId)
			}
		})
	}
}
