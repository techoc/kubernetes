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

	corev1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2/ktesting"
	_ "k8s.io/klog/v2/ktesting/init"
)

// TestIsManagedDisk 验证托管/非托管磁盘判定逻辑：
// URI 以 http 开头（不区分大小写）即视为非托管磁盘。
func TestIsManagedDisk(t *testing.T) {
	// options 为磁盘 URI，expected 为期望的判定结果。
	tests := []struct {
		options  string
		expected bool
	}{
		// 资源 ID 形式：不是 http 开头，视为托管磁盘。
		{
			options:  "testurl/subscriptions/12/resourceGroups/23/providers/Microsoft.Compute/disks/name",
			expected: true,
		},
		// 普通字符串同样视为托管磁盘。
		{
			options:  "test.com",
			expected: true,
		},
		// 大写 HTTP 前缀也应被识别为非托管。
		{
			options:  "HTTP://test.com",
			expected: false,
		},
		// 标准 VHD URL：非托管磁盘。
		{
			options:  "http://test.com/vhds/name",
			expected: false,
		},
	}

	// 用 reflect.DeepEqual 对比判定结果。
	for _, test := range tests {
		result := isManagedDisk(test.options)
		if !reflect.DeepEqual(result, test.expected) {
			t.Errorf("input: %q, isManagedDisk result: %t, expected: %t", test.options, result, test.expected)
		}
	}
}

// TestGetDiskName 验证从磁盘 URI 提取磁盘名的逻辑：
// 托管与非托管的合法 URI 都能解析出名称，格式不符时返回错误。
func TestGetDiskName(t *testing.T) {
	mDiskPathRE := managedDiskPathRE
	uDiskPathRE := unmanagedDiskPathRE
	// options 为输入 URI，expected1 为期望磁盘名，expected2 为期望错误。
	tests := []struct {
		options   string
		expected1 string
		expected2 error
	}{
		// 合法托管磁盘资源 ID，提取出 name。
		{
			options:   "testurl/subscriptions/12/resourceGroups/23/providers/Microsoft.Compute/disks/name",
			expected1: "name",
			expected2: nil,
		},
		// 托管磁盘 ID 缺少 resourceGroups 段，应返回格式错误。
		{
			options:   "testurl/subscriptions/23/providers/Microsoft.Compute/disks/name",
			expected1: "",
			expected2: fmt.Errorf("could not get disk name from testurl/subscriptions/23/providers/Microsoft.Compute/disks/name, correct format: %s", mDiskPathRE),
		},
		// 合法非托管 VHD URL，提取出 name。
		{
			options:   "http://test.com/vhds/name",
			expected1: "name",
			expected2: nil,
		},
		// URL 缺少 /vhds/ 路径，应返回格式错误。
		{
			options:   "http://test.io/name",
			expected1: "",
			expected2: fmt.Errorf("could not get disk name from http://test.io/name, correct format: %s", uDiskPathRE),
		},
	}

	// 同时对比磁盘名和错误是否符合预期。
	for _, test := range tests {
		result1, result2 := getDiskName(test.options)
		if !reflect.DeepEqual(result1, test.expected1) || !reflect.DeepEqual(result2, test.expected2) {
			t.Errorf("input: %q, getDiskName result1: %q, expected1: %q, result2: %q, expected2: %q", test.options, result1, test.expected1,
				result2, test.expected2)
		}
	}
}

// TestTranslateAzureDiskInTreeInlineVolumeToCSI 验证 inline Azure Disk 卷的转换：
// 空输入与非托管类型应报错，普通托管磁盘应生成预期的 CSI PV。
func TestTranslateAzureDiskInTreeInlineVolumeToCSI(t *testing.T) {
	// 非托管（Shared Blob）类型，用于验证不支持的场景。
	sharedBlobDiskKind := corev1.AzureDedicatedBlobDisk
	translator := NewAzureDiskCSITranslator()
	logger, _ := ktesting.NewTestContext(t)

	// volume 为输入，expVol 为期望生成的 PV。
	cases := []struct {
		name   string
		volume *corev1.Volume
		expVol *corev1.PersistentVolume
		expErr bool
	}{
		// nil Volume 直接报错。
		{
			name:   "empty volume",
			expErr: true,
		},
		// 空 Volume 没有 AzureDisk 源，也应报错。
		{
			name:   "no azure disk volume",
			volume: &corev1.Volume{},
			expErr: true,
		},
		// 普通托管磁盘：期望 PV 名为 DataDiskURI，CSI 源的 kind 为 Managed。
		{
			name: "azure disk volume",
			volume: &corev1.Volume{
				VolumeSource: corev1.VolumeSource{
					AzureDisk: &corev1.AzureDiskVolumeSource{
						DiskName:    "diskname",
						DataDiskURI: "datadiskuri",
					},
				},
			},
			expVol: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "datadiskuri",
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							Driver:           "disk.csi.azure.com",
							VolumeHandle:     "datadiskuri",
							VolumeAttributes: map[string]string{azureDiskKind: "Managed"},
						},
					},
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				},
			},
		},
		// 非托管类型（Shared Blob Disk）不被 CSI 迁移支持，应报错。
		{
			name: "azure disk volume with non-managed kind",
			volume: &corev1.Volume{
				VolumeSource: corev1.VolumeSource{
					AzureDisk: &corev1.AzureDiskVolumeSource{
						DiskName:    "diskname",
						DataDiskURI: "datadiskuri",
						Kind:        &sharedBlobDiskKind,
					},
				},
			},
			expErr: true,
		},
	}

	// 逐个执行用例，核对错误预期及结果与期望 PV 是否一致。
	for _, tc := range cases {
		t.Logf("Testing %v", tc.name)
		got, err := translator.TranslateInTreeInlineVolumeToCSI(logger, tc.volume, "")
		if err != nil && !tc.expErr {
			t.Errorf("Did not expect error but got: %v", err)
		}

		if err == nil && tc.expErr {
			t.Errorf("Expected error, but did not get one.")
		}

		if !reflect.DeepEqual(got, tc.expVol) {
			t.Errorf("Got parameters: %v, expected :%v", got, tc.expVol)
		}
	}
}

// TestTranslateAzureDiskInTreePVToCSI 验证 in-tree PV 到 CSI PV 的字段映射：
// 缓存模式与文件系统类型写入 VolumeAttributes，FSType/ReadOnly 写入 CSI 字段。
func TestTranslateAzureDiskInTreePVToCSI(t *testing.T) {
	translator := NewAzureDiskCSITranslator()
	logger, _ := ktesting.NewTestContext(t)

	// 构造输入 PV 所需的指针字段值。
	sharedBlobDiskKind := corev1.AzureDedicatedBlobDisk
	cachingMode := corev1.AzureDataDiskCachingMode("cachingmode")
	fsType := "fstype"
	readOnly := true
	diskURI := "/subscriptions/12/resourceGroups/23/providers/Microsoft.Compute/disks/name"

	// volume 为输入 PV，expVol 为期望结果。
	cases := []struct {
		name   string
		volume *corev1.PersistentVolume
		expVol *corev1.PersistentVolume
		expErr bool
	}{
		// nil PV 报错。
		{
			name:   "empty volume",
			expErr: true,
		},
		// PV 没有 AzureDisk 源，报错。
		{
			name:   "no azure disk volume",
			volume: &corev1.PersistentVolume{},
			expErr: true,
		},
		// 完整字段的托管磁盘：验证所有字段都被正确映射。
		{
			name: "azure disk volume",
			volume: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						AzureDisk: &corev1.AzureDiskVolumeSource{
							CachingMode: &cachingMode,
							DataDiskURI: diskURI,
							FSType:      &fsType,
							ReadOnly:    &readOnly,
						},
					},
				},
			},
			expVol: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							Driver:   "disk.csi.azure.com",
							FSType:   "fstype",
							ReadOnly: true,
							VolumeAttributes: map[string]string{
								"cachingmode":   "cachingmode",
								azureDiskFSType: fsType,
								azureDiskKind:   "Managed",
							},
							VolumeHandle: diskURI,
						},
					},
				},
			},
		},
		// 非托管类型同样应被拒绝。
		{
			name: "azure disk volume with non-managed kind",
			volume: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						AzureDisk: &corev1.AzureDiskVolumeSource{
							CachingMode: &cachingMode,
							DataDiskURI: diskURI,
							FSType:      &fsType,
							ReadOnly:    &readOnly,
							Kind:        &sharedBlobDiskKind,
						},
					},
				},
			},
			expErr: true,
		},
	}

	// 执行翻译并对比结果。
	for _, tc := range cases {
		t.Logf("Testing %v", tc.name)
		got, err := translator.TranslateInTreePVToCSI(logger, tc.volume)
		if err != nil && !tc.expErr {
			t.Errorf("Did not expect error but got: %v", err)
		}

		if err == nil && tc.expErr {
			t.Errorf("Expected error, but did not get one.")
		}

		if !reflect.DeepEqual(got, tc.expVol) {
			t.Errorf("Got parameters: %v, expected :%v", got, tc.expVol)
		}
	}
}

// TestTranslateTranslateCSIPVToInTree 验证 CSI PV 反向翻译为 in-tree AzureDisk PV：
// 覆盖不同缓存模式以及 VolumeAttributes 键名大小写/命名形式的兼容性。
func TestTranslateTranslateCSIPVToInTree(t *testing.T) {
	// 准备三种缓存模式及通用字段值。
	cachingModeNone := corev1.AzureDataDiskCachingNone
	cachingModeReadOnly := corev1.AzureDataDiskCachingReadOnly
	cachingModeReadWrite := corev1.AzureDataDiskCachingReadWrite
	fsType := "fstype"
	readOnly := true
	diskURI := "/subscriptions/12/resourceGroups/23/providers/Microsoft.Compute/disks/name"
	managed := corev1.AzureManagedDisk

	translator := NewAzureDiskCSITranslator()
	// volume 为输入 CSI PV，expVol 为期望还原的 in-tree PV。
	cases := []struct {
		name        string
		cachingMode corev1.AzureDataDiskCachingMode
		volume      *corev1.PersistentVolume
		expVol      *corev1.PersistentVolume
		expErr      bool
	}{
		// 标准属性键：cachingmode=ReadOnly 应还原为 ReadOnly 缓存模式。
		{
			name:        "azure disk volume with ReadOnly cachingMode",
			cachingMode: corev1.AzureDataDiskCachingReadOnly,
			volume: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							Driver:   "disk.csi.azure.com",
							FSType:   "fstype",
							ReadOnly: true,
							VolumeAttributes: map[string]string{
								"cachingmode":   "ReadOnly",
								azureDiskFSType: fsType,
								azureDiskKind:   "managed",
							},
							VolumeHandle: diskURI,
						},
					},
				},
			},
			expVol: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						AzureDisk: &corev1.AzureDiskVolumeSource{
							CachingMode: &cachingModeReadOnly,
							DataDiskURI: diskURI,
							FSType:      &fsType,
							ReadOnly:    &readOnly,
							Kind:        &managed,
							DiskName:    "name",
						},
					},
				},
			},
			expErr: false,
		},
		// 用字面量 "fstype" 作为键，验证与 azureDiskFSType 常量等价。
		{
			name:        "azure disk volume with ReadOnly cachingMode",
			cachingMode: corev1.AzureDataDiskCachingReadOnly,
			volume: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							Driver:   "disk.csi.azure.com",
							FSType:   "fstype",
							ReadOnly: true,
							VolumeAttributes: map[string]string{
								"cachingmode": "ReadOnly",
								"fstype":      fsType,
								azureDiskKind: "managed",
							},
							VolumeHandle: diskURI,
						},
					},
				},
			},
			expVol: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						AzureDisk: &corev1.AzureDiskVolumeSource{
							CachingMode: &cachingModeReadOnly,
							DataDiskURI: diskURI,
							FSType:      &fsType,
							ReadOnly:    &readOnly,
							Kind:        &managed,
							DiskName:    "name",
						},
					},
				},
			},
			expErr: false,
		},
		// 驼峰式键名 cachingMode/fsType 应被大小写不敏感地识别，还原为 None 模式。
		{
			name:        "azure disk volume with None cachingMode",
			cachingMode: corev1.AzureDataDiskCachingReadOnly,
			volume: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							Driver:   "disk.csi.azure.com",
							FSType:   "fstype",
							ReadOnly: true,
							VolumeAttributes: map[string]string{
								"cachingMode": "None",
								"fsType":      fsType,
								azureDiskKind: "managed",
							},
							VolumeHandle: diskURI,
						},
					},
				},
			},
			expVol: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						AzureDisk: &corev1.AzureDiskVolumeSource{
							CachingMode: &cachingModeNone,
							DataDiskURI: diskURI,
							FSType:      &fsType,
							ReadOnly:    &readOnly,
							Kind:        &managed,
							DiskName:    "name",
						},
					},
				},
			},
			expErr: false,
		},
		// 同样使用驼峰式键名，验证还原为 ReadWrite 缓存模式。
		{
			name:        "azure disk volume with ReadWrite cachingMode",
			cachingMode: corev1.AzureDataDiskCachingReadOnly,
			volume: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							Driver:   "disk.csi.azure.com",
							FSType:   "fstype",
							ReadOnly: true,
							VolumeAttributes: map[string]string{
								"cachingMode": "ReadWrite",
								"fsType":      fsType,
								azureDiskKind: "managed",
							},
							VolumeHandle: diskURI,
						},
					},
				},
			},
			expVol: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						AzureDisk: &corev1.AzureDiskVolumeSource{
							CachingMode: &cachingModeReadWrite,
							DataDiskURI: diskURI,
							FSType:      &fsType,
							ReadOnly:    &readOnly,
							Kind:        &managed,
							DiskName:    "name",
						},
					},
				},
			},
			expErr: false,
		},
	}

	// 执行反向翻译，核对错误预期与还原后的 PV 内容。
	for _, tc := range cases {
		t.Logf("Testing %v", tc.name)
		got, err := translator.TranslateCSIPVToInTree(tc.volume)
		if err != nil && !tc.expErr {
			t.Errorf("Did not expect error but got: %v", err)
		}

		if err == nil && tc.expErr {
			t.Errorf("Expected error, but did not get one.")
		}

		if !reflect.DeepEqual(got, tc.expVol) {
			t.Errorf("Got parameters: %v, expected :%v", got, tc.expVol)
		}
	}
}

// TestTranslateInTreeStorageClassToCSI 验证 Azure Disk StorageClass 翻译：
// zone/zones 转拓扑、已有拓扑键的转换，以及数字型拓扑值到空字符串的清洗。
func TestTranslateInTreeStorageClassToCSI(t *testing.T) {
	translator := NewAzureDiskCSITranslator()
	logger, _ := ktesting.NewTestContext(t)

	// options 为输入 StorageClass，expOptions 为期望结果。
	tcs := []struct {
		name       string
		options    *storage.StorageClass
		expOptions *storage.StorageClass
		expErr     bool
	}{
		// 普通参数应原样透传。
		{
			name:       "nothing special",
			options:    NewStorageClass(map[string]string{"foo": "bar"}, nil),
			expOptions: NewStorageClass(map[string]string{"foo": "bar"}, nil),
		},
		// 空参数映射保持不变。
		{
			name:       "empty params",
			options:    NewStorageClass(map[string]string{}, nil),
			expOptions: NewStorageClass(map[string]string{}, nil),
		},
		// zone 参数应转换为 CSI 拓扑选择器。
		{
			name:       "zone",
			options:    NewStorageClass(map[string]string{"zone": "foo"}, nil),
			expOptions: NewStorageClass(map[string]string{}, generateToplogySelectors(AzureDiskTopologyKey, []string{"foo"})),
		},
		// 逗号分隔的 zones 应生成一个多值拓扑选择器。
		{
			name:       "zones",
			options:    NewStorageClass(map[string]string{"zones": "foo,bar,baz"}, nil),
			expOptions: NewStorageClass(map[string]string{}, generateToplogySelectors(AzureDiskTopologyKey, []string{"foo", "bar", "baz"})),
		},
		// 已经是 CSI 拓扑键的 AllowedTopologies 应保持不变。
		{
			name:       "some normal topology",
			options:    NewStorageClass(map[string]string{}, generateToplogySelectors(AzureDiskTopologyKey, []string{"foo"})),
			expOptions: NewStorageClass(map[string]string{}, generateToplogySelectors(AzureDiskTopologyKey, []string{"foo"})),
		},
		// 稳定版 in-tree 拓扑键应翻译为 CSI 拓扑键。
		{
			name:       "some translated topology",
			options:    NewStorageClass(map[string]string{}, generateToplogySelectors(corev1.LabelTopologyZone, []string{"foo"})),
			expOptions: NewStorageClass(map[string]string{}, generateToplogySelectors(AzureDiskTopologyKey, []string{"foo"})),
		},
		// Beta 版拓扑键同样应翻译为 CSI 拓扑键。
		{
			name:       "some translated topology with beta labels",
			options:    NewStorageClass(map[string]string{}, generateToplogySelectors(corev1.LabelFailureDomainBetaZone, []string{"foo"})),
			expOptions: NewStorageClass(map[string]string{}, generateToplogySelectors(AzureDiskTopologyKey, []string{"foo"})),
		},
		// zone 参数与 AllowedTopologies 同时出现属于冲突，应报错。
		{
			name:    "zone and topology",
			options: NewStorageClass(map[string]string{"zone": "foo"}, generateToplogySelectors(AzureDiskTopologyKey, []string{"foo"})),
			expErr:  true,
		},
		// 无可用区 region 的数字值 "0" 应被清洗为空字符串。
		{
			name:       "topology in regions without zones",
			options:    NewStorageClass(map[string]string{}, generateToplogySelectors(corev1.LabelTopologyZone, []string{"0"})),
			expOptions: NewStorageClass(map[string]string{}, generateToplogySelectors(AzureDiskTopologyKey, []string{""})),
		},
		// 多位数数字同样属于无可用区表示，也应清空。
		{
			name:       "longer topology in regions without zones",
			options:    NewStorageClass(map[string]string{}, generateToplogySelectors(corev1.LabelTopologyZone, []string{"1234"})),
			expOptions: NewStorageClass(map[string]string{}, generateToplogySelectors(AzureDiskTopologyKey, []string{""})),
		},
		// 普通区域名不是纯数字，应原样保留。
		{
			name:       "topology in regions with zones",
			options:    NewStorageClass(map[string]string{}, generateToplogySelectors(corev1.LabelTopologyZone, []string{"centralus-1"})),
			expOptions: NewStorageClass(map[string]string{}, generateToplogySelectors(AzureDiskTopologyKey, []string{"centralus-1"})),
		},
	}

	// 执行翻译并核对错误预期与结果内容。
	for _, tc := range tcs {
		t.Logf("Testing %v", tc.name)
		gotOptions, err := translator.TranslateInTreeStorageClassToCSI(logger, tc.options)
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
