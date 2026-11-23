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
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"

	storage "k8s.io/api/storage/v1"

	"k8s.io/klog/v2/ktesting"
	_ "k8s.io/klog/v2/ktesting/init"
)

// 测试用卷 ID：normalVolumeID 是裸 EBS 卷 ID，awsVolumeID 与 awsZoneVolumeID 是
// 两种 URL 形式，invalidVolumeID 缺少 vol- 前缀，用于触发格式校验失败。
const (
	normalVolumeID  = "vol-02399794d890f9375"
	awsVolumeID     = "aws:///vol-02399794d890f9375"
	awsZoneVolumeID = "aws://us-west-2a/vol-02399794d890f9375"
	invalidVolumeID = "aws://us-west-2a/02399794d890f9375"
)

// TestKubernetesVolumeIDToEBSVolumeID 验证三种合法卷 ID 都能转换为裸 EBS ID，
// 并且非法 ID 会返回错误。
func TestKubernetesVolumeIDToEBSVolumeID(t *testing.T) {
	// 表驱动：kubernetesID 为输入，ebsVolumeID 为期望输出，expErr 标记是否应报错。
	testCases := []struct {
		name         string
		kubernetesID string
		ebsVolumeID  string
		expErr       bool
	}{
		{
			name:         "Normal ID format",
			kubernetesID: normalVolumeID,
			ebsVolumeID:  normalVolumeID,
		},
		{
			name:         "aws:///{volumeId} format",
			kubernetesID: awsVolumeID,
			ebsVolumeID:  normalVolumeID,
		},
		{
			name:         "aws://{zone}/{volumeId} format",
			kubernetesID: awsZoneVolumeID,
			ebsVolumeID:  normalVolumeID,
		},
		{
			name:         "fails on invalid volume ID",
			kubernetesID: invalidVolumeID,
			expErr:       true,
		},
	}

	// 逐个执行用例：期望报错时允许 err 非空，否则断言转换结果等于期望值。
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := KubernetesVolumeIDToEBSVolumeID(tc.kubernetesID)
			if err != nil {
				if !tc.expErr {
					t.Errorf("KubernetesVolumeIDToEBSVolumeID failed %v", err)
				}
			} else {
				if actual != tc.ebsVolumeID {
					t.Errorf("Wrong EBS Volume ID. actual: %s expected: %s", actual, tc.ebsVolumeID)
				}
			}
		})
	}
}

// TestTranslateEBSInTreeStorageClassToCSI 验证 StorageClass 参数翻译：
// 普通参数透传、fstype 重命名，以及 iopsPerGB 会附带自动提升 IOPS 的参数。
func TestTranslateEBSInTreeStorageClassToCSI(t *testing.T) {
	translator := NewAWSElasticBlockStoreCSITranslator()
	logger, _ := ktesting.NewTestContext(t)

	// sc 为输入 StorageClass，expSc 为期望翻译后的结果。
	cases := []struct {
		name   string
		sc     *storage.StorageClass
		expSc  *storage.StorageClass
		expErr bool
	}{
		// 普通参数应原样保留。
		{
			name:  "translate normal",
			sc:    NewStorageClass(map[string]string{"foo": "bar"}, nil),
			expSc: NewStorageClass(map[string]string{"foo": "bar"}, nil),
		},
		// 空参数映射保持不变。
		{
			name:  "translate empty map",
			sc:    NewStorageClass(map[string]string{}, nil),
			expSc: NewStorageClass(map[string]string{}, nil),
		},

		// fstype 应被重命名为 CSI 标准参数键。
		{
			name:  "translate with fstype",
			sc:    NewStorageClass(map[string]string{"fstype": "ext3"}, nil),
			expSc: NewStorageClass(map[string]string{"csi.storage.k8s.io/fstype": "ext3"}, nil),
		},
		// iopsPerGB 保留的同时，应补充 allowautoiopspergbincrease=true。
		{
			name:  "translate with iops",
			sc:    NewStorageClass(map[string]string{"iopsPerGB": "100"}, nil),
			expSc: NewStorageClass(map[string]string{"iopsPerGB": "100", "allowautoiopspergbincrease": "true"}, nil),
		},
	}

	// 调用翻译函数并核对错误预期与结果内容。
	for _, tc := range cases {
		t.Logf("Testing %v", tc.name)
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

// TestTranslateInTreeInlineVolumeToCSI 验证 inline EBS 卷转换：
// 三种合法卷 ID 都能生成期望的 PV 名称与 volumeHandle，非法或空源应报错。
func TestTranslateInTreeInlineVolumeToCSI(t *testing.T) {
	translator := NewAWSElasticBlockStoreCSITranslator()
	logger, _ := ktesting.NewTestContext(t)

	// volumeSource 为 inline 卷来源，expPVName 为期望生成的 PV 名称。
	cases := []struct {
		name         string
		volumeSource v1.VolumeSource
		expPVName    string
		expErr       bool
	}{
		// 裸卷 ID：PV 名称由驱动名加卷句柄组成。
		{
			name: "Normal ID format",
			volumeSource: v1.VolumeSource{
				AWSElasticBlockStore: &v1.AWSElasticBlockStoreVolumeSource{
					VolumeID: normalVolumeID,
				},
			},
			expPVName: "ebs.csi.aws.com-" + normalVolumeID,
		},
		// aws:///vol-id 形式：应归一化为同一个卷句柄。
		{
			name: "aws:///{volumeId} format",
			volumeSource: v1.VolumeSource{
				AWSElasticBlockStore: &v1.AWSElasticBlockStoreVolumeSource{
					VolumeID: awsVolumeID,
				},
			},
			expPVName: "ebs.csi.aws.com-" + normalVolumeID,
		},
		// aws://zone/vol-id 形式：zone 信息被丢弃，结果同上。
		{
			name: "aws://{zone}/{volumeId} format",
			volumeSource: v1.VolumeSource{
				AWSElasticBlockStore: &v1.AWSElasticBlockStoreVolumeSource{
					VolumeID: awsZoneVolumeID,
				},
			},
			expPVName: "ebs.csi.aws.com-" + normalVolumeID,
		},
		// 非法格式的卷 ID 应导致转换失败。
		{
			name: "fails on invalid volume ID",
			volumeSource: v1.VolumeSource{
				AWSElasticBlockStore: &v1.AWSElasticBlockStoreVolumeSource{
					VolumeID: invalidVolumeID,
				},
			},
			expErr: true,
		},
		// 空的 VolumeSource 没有 EBS 源，应报错。
		{
			name:         "fails on empty volume source",
			volumeSource: v1.VolumeSource{},
			expErr:       true,
		},
	}

	// 构造 Volume 后调用翻译函数，断言错误预期以及成功时的 PV 字段。
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Testing %v", tc.name)
			got, err := translator.TranslateInTreeInlineVolumeToCSI(logger, &v1.Volume{Name: "volume", VolumeSource: tc.volumeSource}, "")
			if err != nil && !tc.expErr {
				t.Fatalf("Did not expect error but got: %v", err)
			}

			if err == nil && tc.expErr {
				t.Fatalf("Expected error, but did not get one.")
			}

			if err == nil {
				if !reflect.DeepEqual(got.Name, tc.expPVName) {
					t.Errorf("Got PV name: %v, expected :%v", got.Name, tc.expPVName)
				}

				if !reflect.DeepEqual(got.Spec.CSI.VolumeHandle, normalVolumeID) {
					t.Errorf("Got PV volumeHandle: %v, expected :%v", got.Spec.CSI.VolumeHandle, normalVolumeID)
				}
			}

		})
	}
}

// TestGetAwsRegionFromZones 验证从 zone 列表推导 region 的逻辑：
// 覆盖商业区、GovCloud、Wavelength、Local Zone 以及多种非法输入。
func TestGetAwsRegionFromZones(t *testing.T) {

	// zones 为输入可用区列表，expRegion 为期望推导出的 region。
	cases := []struct {
		name      string
		zones     []string
		expRegion string
		expErr    bool
	}{
		// 普通商业区：去掉末尾字母得到 region。
		{
			name:      "Commercial zone",
			zones:     []string{"us-west-2a", "us-west-2b"},
			expRegion: "us-west-2",
		},
		// GovCloud 区：四段形式，同样去掉末尾字母。
		{
			name:      "Govcloud zone",
			zones:     []string{"us-gov-east-1a"},
			expRegion: "us-gov-east-1",
		},
		// Wavelength 区：取前三段作为 region。
		{
			name:      "Wavelength zone",
			zones:     []string{"us-east-1-wl1-bos-wlz-1"},
			expRegion: "us-east-1",
		},
		// Local Zone：同样取前三段。
		{
			name:      "Local zone",
			zones:     []string{"us-west-2-lax-1a"},
			expRegion: "us-west-2",
		},
		// 空列表无法推导 region。
		{
			name:   "Invalid: empty zones",
			zones:  []string{},
			expErr: true,
		},
		// 多个不同 region 无法得到唯一结果。
		{
			name:   "Invalid: multiple regions",
			zones:  []string{"us-west-2a", "us-east-1a"},
			expErr: true,
		},
		// 只有 region 名、没有可用区后缀。
		{
			name:   "Invalid: region name only",
			zones:  []string{"us-west-2"},
			expErr: true,
		},
		// 可用区后缀长度非法。
		{
			name:   "Invalid: invalid suffix",
			zones:  []string{"us-west-2ab"},
			expErr: true,
		},
		// 分段数量不足，无法识别 zone 格式。
		{
			name:   "Invalid: not enough fields",
			zones:  []string{"us-west"},
			expErr: true,
		},
	}

	// 逐个执行用例，校验错误预期与返回的 region。
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Testing %v", tc.name)
			got, err := getAwsRegionFromZones(tc.zones)
			if err != nil && !tc.expErr {
				t.Fatalf("Did not expect error but got: %v", err)
			}

			if err == nil && tc.expErr {
				t.Fatalf("Expected error, but did not get one.")
			}

			if err == nil && !reflect.DeepEqual(got, tc.expRegion) {
				t.Errorf("Got PV name: %v, expected :%v", got, tc.expRegion)
			}
		})
	}
}
