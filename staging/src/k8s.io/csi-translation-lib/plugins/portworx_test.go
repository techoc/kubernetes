/*
Copyright 2021 The Kubernetes Authors.

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2/ktesting"
	_ "k8s.io/klog/v2/ktesting/init"
)

// TestTranslatePortworxInTreeStorageClassToCSI 验证 StorageClass 翻译：普通参数原样保留，
// 认证 Secret 扇出为全部 CSI 阶段参数，nil 输入报错。
func TestTranslatePortworxInTreeStorageClassToCSI(t *testing.T) {
	translator := NewPortworxCSITranslator()
	logger, _ := ktesting.NewTestContext(t)
	testCases := []struct {
		name     string
		inTreeSC *storage.StorageClass
		csiSC    *storage.StorageClass
		errorExp bool
	}{
		{
			// 普通参数应原样复制，同时 provisioner 被替换为 CSI 驱动名。
			name: "correct",
			inTreeSC: &storage.StorageClass{
				Provisioner: PortworxVolumePluginName,
				Parameters: map[string]string{
					"repl":        "1",
					"fs":          "ext4",
					"shared":      "true",
					"priority_io": "high",
				},
			},
			csiSC: &storage.StorageClass{
				Provisioner: PortworxDriverName,
				Parameters: map[string]string{
					"repl":        "1",
					"fs":          "ext4",
					"shared":      "true",
					"priority_io": "high",
				},
			},
			errorExp: false,
		},
		{
			// 输入 nil 应返回错误。
			name:     "nil, err expected",
			inTreeSC: nil,
			csiSC:    nil,
			errorExp: true,
		},
		{
			// 空 StorageClass 不报错，只设置 provisioner。
			name:     "empty",
			inTreeSC: &storage.StorageClass{},
			csiSC: &storage.StorageClass{
				Provisioner: PortworxDriverName,
			},
			errorExp: false,
		},
		{
			// 树内认证 Secret 键应展开为全部 CSI 阶段的带前缀参数。
			name: "with secret params",
			inTreeSC: &storage.StorageClass{
				Parameters: map[string]string{
					"repl":                                 "1",
					"openstorage.io/auth-secret-name":      "test-secret",
					"openstorage.io/auth-secret-namespace": "test-namespace",
				},
			},
			csiSC: &storage.StorageClass{
				Parameters: map[string]string{
					"repl": "1",
					"csi.storage.k8s.io/provisioner-secret-name":             "test-secret",
					"csi.storage.k8s.io/provisioner-secret-namespace":        "test-namespace",
					"csi.storage.k8s.io/controller-publish-secret-name":      "test-secret",
					"csi.storage.k8s.io/controller-publish-secret-namespace": "test-namespace",
					"csi.storage.k8s.io/node-stage-secret-name":              "test-secret",
					"csi.storage.k8s.io/node-stage-secret-namespace":         "test-namespace",
					"csi.storage.k8s.io/node-publish-secret-name":            "test-secret",
					"csi.storage.k8s.io/node-publish-secret-namespace":       "test-namespace",
					"csi.storage.k8s.io/controller-expand-secret-name":       "test-secret",
					"csi.storage.k8s.io/controller-expand-secret-namespace":  "test-namespace",
					"csi.storage.k8s.io/node-expand-secret-name":             "test-secret",
					"csi.storage.k8s.io/node-expand-secret-namespace":        "test-namespace",
				},
				Provisioner: PortworxDriverName,
			},
			errorExp: false,
		},
	}
	for _, tc := range testCases {
		t.Logf("Testing %v", tc.name)
		// 逐例执行转换，并校验错误预期与深比较结果。
		result, err := translator.TranslateInTreeStorageClassToCSI(logger, tc.inTreeSC)
		if err != nil && !tc.errorExp {
			t.Errorf("Did not expect error but got: %v", err)
		}
		if err == nil && tc.errorExp {
			t.Errorf("Expected error, but did not get one.")
		}
		if !reflect.DeepEqual(result, tc.csiSC) {
			t.Errorf("Got parameters: %v\n, expected :%v", result, tc.csiSC)
		}
	}
}

// TestTranslatePortworxInTreeInlineVolumeToCSI 验证内联卷转换：PV 名称、CSI 来源字段与访问模式的生成，
// 以及 nil 输入时的错误分支。
func TestTranslatePortworxInTreeInlineVolumeToCSI(t *testing.T) {
	translator := NewPortworxCSITranslator()
	logger, _ := ktesting.NewTestContext(t)

	testCases := []struct {
		name        string
		inLine      *v1.Volume
		csiVol      *v1.PersistentVolume
		errExpected bool
	}{
		{
			// 可读写内联卷应转换为 CSI PV：名称为 驱动名-VolumeID，访问模式为 ReadWriteOnce。
			name: "normal",
			inLine: &v1.Volume{
				Name: "PortworxVol",
				VolumeSource: v1.VolumeSource{
					PortworxVolume: &v1.PortworxVolumeSource{
						VolumeID: "ID",
						FSType:   "type",
						ReadOnly: false,
					},
				},
			},
			csiVol: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					// Must be unique per disk as it is used as the unique part of the
					// staging path
					Name: "pxd.portworx.com-ID",
				},
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						CSI: &v1.CSIPersistentVolumeSource{
							Driver:           PortworxDriverName,
							VolumeHandle:     "ID",
							FSType:           "type",
							VolumeAttributes: make(map[string]string),
						},
					},
					AccessModes: []v1.PersistentVolumeAccessMode{
						v1.ReadWriteOnce,
					},
				},
			},
			errExpected: false,
		},
		{
			// nil 输入应报错。
			name:        "nil",
			inLine:      nil,
			csiVol:      nil,
			errExpected: true,
		},
	}

	for _, tc := range testCases {
		t.Logf("Testing %v", tc.name)
		// podNamespace 不参与 Portworx 内联卷的转换，仅按接口要求透传。
		result, err := translator.TranslateInTreeInlineVolumeToCSI(logger, tc.inLine, "ns")
		if err != nil && !tc.errExpected {
			t.Errorf("Did not expect error but got: %v", err)
		}
		if err == nil && tc.errExpected {
			t.Errorf("Expected error, but did not get one.")
		}
		if !reflect.DeepEqual(result, tc.csiVol) {
			t.Errorf("Got parameters: %v\n, expected :%v", result, tc.csiVol)
		}
	}
}

// TestTranslatePortworxInTreePVToCSI 验证树内 PV 到 CSI PV 的转换，
// 重点覆盖 Secret 注解到各阶段 SecretRef 的映射。
func TestTranslatePortworxInTreePVToCSI(t *testing.T) {
	translator := NewPortworxCSITranslator()
	logger, _ := ktesting.NewTestContext(t)

	testCases := []struct {
		name        string
		inTree      *v1.PersistentVolume
		csi         *v1.PersistentVolume
		errExpected bool
	}{
		{
			// 缺少 PortworxVolume 来源的 PV 应报错。
			name: "no Portworx volume",
			inTree: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pxd.portworx.com",
				},
				Spec: v1.PersistentVolumeSpec{
					AccessModes: []v1.PersistentVolumeAccessMode{
						v1.ReadWriteOnce,
					},
					ClaimRef: &v1.ObjectReference{
						Name:      "test-pvc",
						Namespace: "default",
					},
				},
			},
			csi:         nil,
			errExpected: true,
		},
		{
			// 正常转换：CSI 来源由 VolumeID 与 FSType 填充，其余 PV 元数据保持不变。
			name: "normal",
			inTree: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pxd.portworx.com",
				},
				Spec: v1.PersistentVolumeSpec{
					AccessModes: []v1.PersistentVolumeAccessMode{
						v1.ReadWriteOnce,
					},
					ClaimRef: &v1.ObjectReference{
						Name:      "test-pvc",
						Namespace: "default",
					},
					PersistentVolumeSource: v1.PersistentVolumeSource{
						PortworxVolume: &v1.PortworxVolumeSource{
							VolumeID: "ID1111",
							FSType:   "type",
							ReadOnly: false,
						},
					},
				},
			},
			csi: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pxd.portworx.com",
				},
				Spec: v1.PersistentVolumeSpec{
					AccessModes: []v1.PersistentVolumeAccessMode{
						v1.ReadWriteOnce,
					},
					ClaimRef: &v1.ObjectReference{
						Name:      "test-pvc",
						Namespace: "default",
					},
					PersistentVolumeSource: v1.PersistentVolumeSource{
						CSI: &v1.CSIPersistentVolumeSource{
							Driver:           PortworxDriverName,
							VolumeHandle:     "ID1111",
							FSType:           "type",
							VolumeAttributes: make(map[string]string),
						},
					},
				},
			},
			errExpected: false,
		},
		{
			// 两个认证注解同时存在时，应生成全部五个阶段的 Secret 引用。
			name: "with secret annotations",
			inTree: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pxd.portworx.com",
					Annotations: map[string]string{
						"openstorage.io/auth-secret-name":      "test-secret",
						"openstorage.io/auth-secret-namespace": "test-namespace",
					},
				},
				Spec: v1.PersistentVolumeSpec{
					AccessModes: []v1.PersistentVolumeAccessMode{
						v1.ReadWriteOnce,
					},
					ClaimRef: &v1.ObjectReference{
						Name:      "test-pvc",
						Namespace: "default",
					},
					PersistentVolumeSource: v1.PersistentVolumeSource{
						PortworxVolume: &v1.PortworxVolumeSource{
							VolumeID: "ID1111",
							FSType:   "type",
							ReadOnly: false,
						},
					},
				},
			},
			csi: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pxd.portworx.com",
					Annotations: map[string]string{
						"openstorage.io/auth-secret-name":      "test-secret",
						"openstorage.io/auth-secret-namespace": "test-namespace",
					},
				},
				Spec: v1.PersistentVolumeSpec{
					AccessModes: []v1.PersistentVolumeAccessMode{
						v1.ReadWriteOnce,
					},
					ClaimRef: &v1.ObjectReference{
						Name:      "test-pvc",
						Namespace: "default",
					},
					PersistentVolumeSource: v1.PersistentVolumeSource{
						CSI: &v1.CSIPersistentVolumeSource{
							Driver:           PortworxDriverName,
							VolumeHandle:     "ID1111",
							FSType:           "type",
							VolumeAttributes: make(map[string]string),
							ControllerPublishSecretRef: &v1.SecretReference{
								Name:      "test-secret",
								Namespace: "test-namespace",
							},
							NodeStageSecretRef: &v1.SecretReference{
								Name:      "test-secret",
								Namespace: "test-namespace",
							},
							NodePublishSecretRef: &v1.SecretReference{
								Name:      "test-secret",
								Namespace: "test-namespace",
							},
							ControllerExpandSecretRef: &v1.SecretReference{
								Name:      "test-secret",
								Namespace: "test-namespace",
							},
							NodeExpandSecretRef: &v1.SecretReference{
								Name:      "test-secret",
								Namespace: "test-namespace",
							},
						},
					},
				},
			},
			errExpected: false,
		},
		{
			name:        "nil PV",
			inTree:      nil,
			csi:         nil,
			errExpected: true,
		},
	}

	for _, tc := range testCases {
		t.Logf("Testing %v", tc.name)
		// 逐例执行转换，并校验 SecretRef 是否被正确写入 CSI 来源。
		result, err := translator.TranslateInTreePVToCSI(logger, tc.inTree)
		if err != nil && !tc.errExpected {
			t.Errorf("Did not expect error but got: %v", err)
		}
		if err == nil && tc.errExpected {
			t.Errorf("Expected error, but did not get one.")
		}
		if !reflect.DeepEqual(result, tc.csi) {
			t.Errorf("Got parameters: %v\n, expected :%v", result, tc.csi)
		}
	}
}

// TestTranslatePortworxCSIPvToInTree 验证 CSI PV 到树内 PV 的反向转换，
// 以及缺少 CSI 来源与 nil 输入时的错误分支。
func TestTranslatePortworxCSIPvToInTree(t *testing.T) {
	translator := NewPortworxCSITranslator()

	testCases := []struct {
		name        string
		csi         *v1.PersistentVolume
		inTree      *v1.PersistentVolume
		errExpected bool
	}{
		{
			// 缺少 CSI 来源的 PV 应报错。
			name: "no CSI section",
			csi: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					// Must be unique per disk as it is used as the unique part of the
					// staging path
					Name: "pxd.portworx.com",
				},
				Spec: v1.PersistentVolumeSpec{
					AccessModes: []v1.PersistentVolumeAccessMode{
						v1.ReadWriteOnce,
					},
					ClaimRef: &v1.ObjectReference{
						Name:      "test-pvc",
						Namespace: "default",
					},
				},
			},
			inTree:      nil,
			errExpected: true,
		},
		{
			// 正常反向转换：VolumeHandle 回填为 VolumeID，FSType 保留。
			name: "normal",
			csi: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pxd.portworx.com",
				},
				Spec: v1.PersistentVolumeSpec{
					AccessModes: []v1.PersistentVolumeAccessMode{
						v1.ReadWriteOnce,
					},
					ClaimRef: &v1.ObjectReference{
						Name:      "test-pvc",
						Namespace: "default",
					},
					PersistentVolumeSource: v1.PersistentVolumeSource{
						CSI: &v1.CSIPersistentVolumeSource{
							Driver:           PortworxDriverName,
							VolumeHandle:     "ID1111",
							FSType:           "type",
							VolumeAttributes: make(map[string]string),
						},
					},
				},
			},
			inTree: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pxd.portworx.com",
				},
				Spec: v1.PersistentVolumeSpec{
					AccessModes: []v1.PersistentVolumeAccessMode{
						v1.ReadWriteOnce,
					},
					ClaimRef: &v1.ObjectReference{
						Name:      "test-pvc",
						Namespace: "default",
					},
					PersistentVolumeSource: v1.PersistentVolumeSource{
						PortworxVolume: &v1.PortworxVolumeSource{
							VolumeID: "ID1111",
							FSType:   "type",
							ReadOnly: false,
						},
					},
				},
			},
			errExpected: false,
		},
		{
			name:        "nil PV",
			inTree:      nil,
			csi:         nil,
			errExpected: true,
		},
	}

	for _, tc := range testCases {
		t.Logf("Testing %v", tc.name)
		// 逐例执行反向转换，并校验错误预期与结果。
		result, err := translator.TranslateCSIPVToInTree(tc.csi)
		if err != nil && !tc.errExpected {
			t.Errorf("Did not expect error but got: %v", err)
		}
		if err == nil && tc.errExpected {
			t.Errorf("Expected error, but did not get one.")
		}
		if !reflect.DeepEqual(result, tc.inTree) {
			t.Errorf("Got parameters: %v\n, expected :%v", result, tc.inTree)
		}
	}
}
