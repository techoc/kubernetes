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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2/ktesting"
	_ "k8s.io/klog/v2/ktesting/init"

	"github.com/stretchr/testify/assert"
)

// TestGetFileShareInfo 验证按 # 拆分 volume handle 的解析逻辑：正常四段、缺省磁盘名以及各种非法输入。
func TestGetFileShareInfo(t *testing.T) {
	// 表驱动测试：id 是输入，其余字段是期望得到的四个分段与错误。
	tests := []struct {
		id                string
		resourceGroupName string
		accountName       string
		fileShareName     string
		diskName          string
		expectedError     error
	}{
		// 完整四段：资源组、账户、共享名、磁盘名都应正确解析。
		{
			id:                "rg#f5713de20cde511e8ba4900#pvc-file-dynamic-17e43f84-f474-11e8-acd0-000d3a00df41#diskname.vhd",
			resourceGroupName: "rg",
			accountName:       "f5713de20cde511e8ba4900",
			fileShareName:     "pvc-file-dynamic-17e43f84-f474-11e8-acd0-000d3a00df41",
			diskName:          "diskname.vhd",
			expectedError:     nil,
		},
		// 只有三段：磁盘名应为空，且不报错。
		{
			id:                "rg#f5713de20cde511e8ba4900#pvc-file-dynamic-17e43f84-f474-11e8-acd0-000d3a00df41",
			resourceGroupName: "rg",
			accountName:       "f5713de20cde511e8ba4900",
			fileShareName:     "pvc-file-dynamic-17e43f84-f474-11e8-acd0-000d3a00df41",
			diskName:          "",
			expectedError:     nil,
		},
		// 只有两段：段数不足，期望返回解析错误。
		{
			id:                "rg#f5713de20cde511e8ba4900",
			resourceGroupName: "",
			accountName:       "",
			fileShareName:     "",
			diskName:          "",
			expectedError:     fmt.Errorf("error parsing volume id: \"rg#f5713de20cde511e8ba4900\", should at least contain two #"),
		},
		// 单段与空串：同样应报错。
		{
			id:                "rg",
			resourceGroupName: "",
			accountName:       "",
			fileShareName:     "",
			diskName:          "",
			expectedError:     fmt.Errorf("error parsing volume id: \"rg\", should at least contain two #"),
		},
		{
			id:                "",
			resourceGroupName: "",
			accountName:       "",
			fileShareName:     "",
			diskName:          "",
			expectedError:     fmt.Errorf("error parsing volume id: \"\", should at least contain two #"),
		},
	}

	// 逐个用例调用被测函数，并逐字段断言返回值与错误。
	for _, test := range tests {
		resourceGroupName, accountName, fileShareName, diskName, expectedError := getFileShareInfo(test.id)
		if resourceGroupName != test.resourceGroupName {
			t.Errorf("getFileShareInfo(%q) returned with: %q, expected: %q", test.id, resourceGroupName, test.resourceGroupName)
		}
		if accountName != test.accountName {
			t.Errorf("getFileShareInfo(%q) returned with: %q, expected: %q", test.id, accountName, test.accountName)
		}
		if fileShareName != test.fileShareName {
			t.Errorf("getFileShareInfo(%q) returned with: %q, expected: %q", test.id, fileShareName, test.fileShareName)
		}
		if diskName != test.diskName {
			t.Errorf("getFileShareInfo(%q) returned with: %q, expected: %q", test.id, diskName, test.diskName)
		}
		if !reflect.DeepEqual(expectedError, test.expectedError) {
			t.Errorf("getFileShareInfo(%q) returned with: %v, expected: %v", test.id, expectedError, test.expectedError)
		}
	}
}

// TestTranslateAzureFileInTreeStorageClassToCSI 验证内联 AzureFile 卷到 CSI PV 的翻译（函数名沿用历史命名，实际测试的是内联卷转换）。
func TestTranslateAzureFileInTreeStorageClassToCSI(t *testing.T) {
	translator := NewAzureFileCSITranslator()
	logger, _ := ktesting.NewTestContext(t)

	// cases 覆盖非法输入、使用默认命名空间以及显式 Pod 命名空间三种场景。
	cases := []struct {
		name         string
		volume       *corev1.Volume
		podNamespace string
		expVol       *corev1.PersistentVolume
		expErr       bool
	}{
		// 空卷：应返回错误。
		{
			name:   "empty volume",
			expErr: true,
		},
		// 没有 AzureFile 来源的卷：应返回错误。
		{
			name:   "no azure file volume",
			volume: &corev1.Volume{},
			expErr: true,
		},
		// 正常内联卷：未传 podNamespace 时 handle 与 Secret 命名空间都应为 default。
		{
			name: "azure file volume",
			volume: &corev1.Volume{
				Name: "name",
				VolumeSource: corev1.VolumeSource{
					// 输入来源包含共享名、Secret 名和只读标志。
					AzureFile: &corev1.AzureFileVolumeSource{
						ReadOnly:   true,
						SecretName: "secretname",
						ShareName:  "sharename",
					},
				},
			},
			expVol: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					// PV 名与 handle 一致，资源组段为空。
					Name: "#secretname#sharename#name#default",
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							// 期望驱动名为 Azure File CSI 驱动。
							Driver: "file.csi.azure.com",
							// Secret 引用使用原 Secret 名和默认命名空间。
							NodeStageSecretRef: &corev1.SecretReference{
								Name:      "secretname",
								Namespace: "default",
							},
							// 其余字段由 in-tree 来源直接映射。
							ReadOnly:         true,
							VolumeAttributes: map[string]string{shareNameField: "sharename"},
							VolumeHandle:     "#secretname#sharename#name#default",
						},
					},
					// Azure File 固定映射为 ReadWriteMany。
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
				},
			},
		},
		// 传入 podNamespace 时，handle 和 NodeStageSecretRef 的命名空间都应随之改变。
		{
			name: "azure file volume with a pod namespace",
			volume: &corev1.Volume{
				Name: "name",
				VolumeSource: corev1.VolumeSource{
					// 输入与上一用例相同，仅命名空间不同。
					AzureFile: &corev1.AzureFileVolumeSource{
						ReadOnly:   true,
						SecretName: "secretname",
						ShareName:  "sharename",
					},
				},
			},
			podNamespace: "test",
			expVol: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					// handle 末段变为 test。
					Name: "#secretname#sharename#name#test",
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							Driver: "file.csi.azure.com",
							// Secret 引用命名空间同步变为 test。
							NodeStageSecretRef: &corev1.SecretReference{
								Name:      "secretname",
								Namespace: "test",
							},
							ReadOnly:         true,
							VolumeAttributes: map[string]string{shareNameField: "sharename"},
							VolumeHandle:     "#secretname#sharename#name#test",
						},
					},
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
				},
			},
		},
	}

	// 对每个用例执行翻译，并断言错误与结果均符合预期。
	for _, tc := range cases {
		t.Logf("Testing %v", tc.name)
		got, err := translator.TranslateInTreeInlineVolumeToCSI(logger, tc.volume, tc.podNamespace)
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

// TestTranslateAzureFileInTreePVToCSI 验证树内 Azure File PV 到 CSI PV 的翻译，重点检查命名空间来源与资源组 annotation。
func TestTranslateAzureFileInTreePVToCSI(t *testing.T) {
	translator := NewAzureFileCSITranslator()
	logger, _ := ktesting.NewTestContext(t)

	// 供各用例复用的 Secret 命名空间值。
	secretNamespace := "secretnamespace"

	// cases 覆盖非法 PV、命名空间缺失报错、显式命名空间、资源组注解以及从 ClaimRef 回退等场景。
	cases := []struct {
		name   string
		volume *corev1.PersistentVolume
		expVol *corev1.PersistentVolume
		expErr bool
	}{
		// 空 PV：应报错。
		{
			name:   "empty volume",
			expErr: true,
		},
		// 非 AzureFile 来源：应报错。
		{
			name:   "no azure file volume",
			volume: &corev1.PersistentVolume{},
			expErr: true,
		},
		// 既无 SecretNamespace 又无 ClaimRef：无法确定命名空间，应报错。
		{
			name: "return error if secret namespace could not be found",
			volume: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "uuid",
					Annotations: map[string]string{resourceGroupAnnotation: "rg"},
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						// 输入来源缺少 SecretNamespace 且 PV 没有 ClaimRef。
						AzureFile: &corev1.AzureFilePersistentVolumeSource{
							ShareName:  "sharename",
							SecretName: "secretname",
							ReadOnly:   true,
						},
					},
				},
			},
			expErr: true,
		},
		// 显式指定 SecretNamespace：handle 首段为空，末段使用该命名空间。
		{
			name: "azure file volume",
			volume: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "uuid",
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						// 来源声明了 SecretNamespace，应优先于其他途径。
						AzureFile: &corev1.AzureFilePersistentVolumeSource{
							ShareName:       "sharename",
							SecretName:      "secretname",
							SecretNamespace: &secretNamespace,
							ReadOnly:        true,
						},
					},
				},
			},
			expVol: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "uuid",
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							// 驱动名与只读标志直接映射。
							Driver:   "file.csi.azure.com",
							ReadOnly: true,
							// Secret 引用使用来源中的名称与命名空间。
							NodeStageSecretRef: &corev1.SecretReference{
								Name:      "secretname",
								Namespace: secretNamespace,
							},
							VolumeAttributes: map[string]string{shareNameField: "sharename"},
							VolumeHandle:     "#secretname#sharename#uuid#secretnamespace",
						},
					},
				},
			},
		},
		// 带资源组 annotation：handle 首段应填入资源组名。
		{
			name: "azure file volume with rg annotation",
			volume: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "uuid",
					Annotations: map[string]string{resourceGroupAnnotation: "rg"},
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						// 来源字段与上一用例一致。
						AzureFile: &corev1.AzureFilePersistentVolumeSource{
							ShareName:       "sharename",
							SecretName:      "secretname",
							SecretNamespace: &secretNamespace,
							ReadOnly:        true,
						},
					},
				},
			},
			expVol: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					// annotation 在翻译后应被保留。
					Name:        "uuid",
					Annotations: map[string]string{resourceGroupAnnotation: "rg"},
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							Driver:   "file.csi.azure.com",
							ReadOnly: true,
							NodeStageSecretRef: &corev1.SecretReference{
								Name:      "secretname",
								Namespace: secretNamespace,
							},
							VolumeAttributes: map[string]string{shareNameField: "sharename"},
							VolumeHandle:     "rg#secretname#sharename#uuid#secretnamespace",
						},
					},
				},
			},
		},
		// SecretNamespace 缺失但有 ClaimRef：应从 ClaimRef 的命名空间回退。
		{
			name: "get secret namespace from ClaimRef when it's missing in pv spec source",
			volume: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "uuid",
					Annotations: map[string]string{resourceGroupAnnotation: "rg"},
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						// 这里刻意不设置 SecretNamespace，以验证回退逻辑。
						AzureFile: &corev1.AzureFilePersistentVolumeSource{
							ShareName:  "sharename",
							SecretName: "secretname",
							ReadOnly:   true,
						},
					},
					// 命名空间信息改由 ClaimRef 提供。
					ClaimRef: &corev1.ObjectReference{
						Namespace: secretNamespace,
					},
				},
			},
			expVol: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "uuid",
					Annotations: map[string]string{resourceGroupAnnotation: "rg"},
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							Driver:   "file.csi.azure.com",
							ReadOnly: true,
							// Secret 引用的命名空间来自 ClaimRef。
							NodeStageSecretRef: &corev1.SecretReference{
								Name:      "secretname",
								Namespace: secretNamespace,
							},
							VolumeAttributes: map[string]string{shareNameField: "sharename"},
							VolumeHandle:     "rg#secretname#sharename#uuid#secretnamespace",
						},
					},
					// ClaimRef 在翻译后应保持原样。
					ClaimRef: &corev1.ObjectReference{
						Namespace: secretNamespace,
					},
				},
			},
		},
	}

	// 逐用例执行翻译并断言结果。
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

// TestTranslateCSIPVToInTree 验证 CSI Azure File PV 回滚为树内 AzureFile PV 的各种信息来源：VolumeAttributes、NodeStageSecretRef 和 VolumeHandle。
func TestTranslateCSIPVToInTree(t *testing.T) {
	translator := NewAzureFileCSITranslator()

	// 各用例复用的期望值。
	secretName := "secretname"
	secretNamespace := "secretnamespace"
	shareName := "sharename"
	defaultNS := "default"
	// mp 只包含共享名，模拟不含 Secret 信息的 VolumeAttributes。
	mp := make(map[string]string)
	mp["shareName"] = shareName

	// secretMap 同时包含共享名和 Secret 信息，模拟完整的 VolumeAttributes。
	secretMap := make(map[string]string)
	secretMap["shareName"] = shareName
	secretMap["secretName"] = secretName
	secretMap["secretNamespace"] = secretNamespace

	// cases 逐一验证不同字段来源以及错误处理。
	cases := []struct {
		name   string
		volume *corev1.PersistentVolume
		expVol *corev1.PersistentVolume
		expErr bool
	}{
		// 空 PV：应报错。
		{
			name:   "empty volume",
			expErr: true,
		},
		// 只有 NodeStageSecretRef、没有 VolumeHandle：共享名从 VolumeAttributes 取，命名空间由 Secret 引用提供。
		{
			name: "resource group empty",
			volume: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							// Secret 引用提供名称与命名空间。
							NodeStageSecretRef: &corev1.SecretReference{
								Name:      "ut",
								Namespace: secretNamespace,
							},
							ReadOnly:         true,
							VolumeAttributes: mp,
						},
					},
				},
			},
			expVol: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					// 没有资源组信息时 annotation 为空 map。
					Annotations: map[string]string{},
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						// 还原出的树内来源字段。
						AzureFile: &corev1.AzureFilePersistentVolumeSource{
							SecretName:      "ut",
							SecretNamespace: &secretNamespace,
							ReadOnly:        true,
							ShareName:       shareName,
						},
					},
				},
			},
			expErr: false,
		},
		// VolumeHandle 非法且 VolumeAttributes 缺少 Secret：解析 handle 失败应返回错误。
		{
			name: "translate from volume handle error",
			volume: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							VolumeHandle:     shareName,
							ReadOnly:         true,
							VolumeAttributes: mp,
						},
					},
				},
			},
			expErr: true,
		},
		// 共享名来自 VolumeAttributes，Secret 名和共享名从 handle 反解，资源组写入 annotation。
		{
			name: "translate from VolumeAttributes",
			volume: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "file.csi.azure.com-sharename",
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							// handle 形如 rg#账户#共享#磁盘，可反解出资源组与账户。
							VolumeHandle:     "rg#st#pvc-file-dynamic#diskname.vhd",
							ReadOnly:         true,
							VolumeAttributes: mp,
						},
					},
				},
			},
			expVol: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "file.csi.azure.com-sharename",
					Annotations: map[string]string{resourceGroupAnnotation: "rg"},
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						// Secret 名由账户名套用默认模板生成，命名空间缺失时用 default。
						AzureFile: &corev1.AzureFilePersistentVolumeSource{
							SecretName:      "azure-storage-account-st-secret",
							ShareName:       shareName,
							SecretNamespace: &defaultNS,
							ReadOnly:        true,
						},
					},
				},
			},
			expErr: false,
		},
		// VolumeAttributes 中同时带有 Secret 信息：这些值优先于从 handle 反解的结果。
		{
			name: "translate from SecretMap VolumeAttributes",
			volume: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "file.csi.azure.com-sharename",
					Annotations: map[string]string{},
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							VolumeHandle:     "rg#st#pvc-file-dynamic#diskname.vhd",
							ReadOnly:         true,
							VolumeAttributes: secretMap,
						},
					},
				},
			},
			expVol: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "file.csi.azure.com-sharename",
					Annotations: map[string]string{},
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						// 名称与命名空间直接取自 VolumeAttributes。
						AzureFile: &corev1.AzureFilePersistentVolumeSource{
							SecretName:      secretName,
							SecretNamespace: &secretNamespace,
							ShareName:       shareName,
							ReadOnly:        true,
						},
					},
				},
			},
			expErr: false,
		},
		// 同时存在 NodeStageSecretRef 和 VolumeAttributes：Secret 引用应覆盖属性中的 Secret 信息。
		{
			name: "translate from NodeStageSecretRef",
			volume: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "file.csi.azure.com-sharename",
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							VolumeHandle:     "rg#st#pvc-file-dynamic#diskname.vhd",
							ReadOnly:         true,
							VolumeAttributes: mp,
							NodeStageSecretRef: &corev1.SecretReference{
								Name:      secretName,
								Namespace: secretNamespace,
							},
						},
					},
				},
			},
			expVol: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "file.csi.azure.com-sharename",
					Annotations: map[string]string{},
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						// 名称与命名空间均来自 NodeStageSecretRef。
						AzureFile: &corev1.AzureFilePersistentVolumeSource{
							SecretName:      secretName,
							ShareName:       shareName,
							SecretNamespace: &secretNamespace,
							ReadOnly:        true,
						},
					},
				},
			},
			expErr: false,
		},
		// 完全没有 VolumeAttributes 和 Secret 引用：所有信息都只能从 VolumeHandle 反解。
		{
			name: "translate from VolumeHandle",
			volume: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "file.csi.azure.com-sharename",
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							VolumeHandle: "rg#st#pvc-file-dynamic#diskname.vhd",
							ReadOnly:     true,
						},
					},
				},
			},
			expVol: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "file.csi.azure.com-sharename",
					Annotations: map[string]string{resourceGroupAnnotation: "rg"},
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						// 共享名、账户名、资源组均来自 handle 的对应分段。
						AzureFile: &corev1.AzureFilePersistentVolumeSource{
							SecretName:      "azure-storage-account-st-secret",
							ShareName:       "pvc-file-dynamic",
							SecretNamespace: &defaultNS,
							ReadOnly:        true,
						},
					},
				},
			},
			expErr: false,
		},
	}

	// 逐用例执行回滚翻译并断言错误与结果。
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

// TestGetStorageAccount 验证从 Secret 名称中解析存储账户名的正则匹配行为，包括名称中带连字符的情况和非法格式。
func TestGetStorageAccount(t *testing.T) {
	// tests 中的 expectedError 表示是否应当解析失败，expectedResult 是期望的账户名。
	tests := []struct {
		secretName     string
		expectedError  bool
		expectedResult string
	}{
		// 标准命名：成功提取账户名。
		{
			secretName:     "azure-storage-account-accountname-secret",
			expectedError:  false,
			expectedResult: "accountname",
		},
		// 账户名本身包含连字符时也应完整提取。
		{
			secretName:     "azure-storage-account-accountname-dup-secret",
			expectedError:  false,
			expectedResult: "accountname-dup",
		},
		// 不符合模板的名称应报错，且返回空账户名。
		{
			secretName:     "invalid",
			expectedError:  true,
			expectedResult: "",
		},
	}

	// 用 testify 断言错误标志和解析结果。
	for i, test := range tests {
		accountName, err := getStorageAccountName(test.secretName)
		assert.Equal(t, test.expectedError, err != nil, "TestCase[%d]", i)
		assert.Equal(t, test.expectedResult, accountName, "TestCase[%d]", i)
	}
}
