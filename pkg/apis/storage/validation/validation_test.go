/*
Copyright 2016 The Kubernetes Authors.

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

package validation

import (
	"fmt"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/storage"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"
)

// 本文件共享的测试夹具：回收策略、卷绑定模式与内联卷规格，多个用例复用这些值。
var (
	deleteReclaimPolicy = api.PersistentVolumeReclaimDelete
	immediateMode1      = storage.VolumeBindingImmediate
	immediateMode2      = storage.VolumeBindingImmediate
	waitingMode         = storage.VolumeBindingWaitForFirstConsumer
	invalidMode         = storage.VolumeBindingMode("foo")
	inlineSpec          = api.PersistentVolumeSpec{
		AccessModes: []api.PersistentVolumeAccessMode{api.ReadWriteOnce},
		PersistentVolumeSource: api.PersistentVolumeSource{
			CSI: &api.CSIPersistentVolumeSource{
				Driver:       "com.test.foo",
				VolumeHandle: "foobar",
			},
		},
	}
)

// TestValidateStorageClass 验证 StorageClass 的创建校验：合法参数组合应通过，
// 命名空间、provisioner 名称、回收策略以及参数大小等非法规格应报错。
func TestValidateStorageClass(t *testing.T) {
	deleteReclaimPolicy := api.PersistentVolumeReclaimPolicy("Delete")
	retainReclaimPolicy := api.PersistentVolumeReclaimPolicy("Retain")
	recycleReclaimPolicy := api.PersistentVolumeReclaimPolicy("Recycle")
	successCases := []storage.StorageClass{{
		// 空参数 map：合法。
		ObjectMeta:        metav1.ObjectMeta{Name: "foo"},
		Provisioner:       "kubernetes.io/foo-provisioner",
		Parameters:        map[string]string{},
		ReclaimPolicy:     &deleteReclaimPolicy,
		VolumeBindingMode: &immediateMode1,
	}, {
		// 参数为 nil：合法，等价于未显式设置。
		ObjectMeta:        metav1.ObjectMeta{Name: "foo"},
		Provisioner:       "kubernetes.io/foo-provisioner",
		ReclaimPolicy:     &deleteReclaimPolicy,
		VolumeBindingMode: &immediateMode1,
	}, {
		// 携带若干参数：合法，参数值允许任意格式（含内嵌 JSON）。
		ObjectMeta:  metav1.ObjectMeta{Name: "foo"},
		Provisioner: "kubernetes.io/foo-provisioner",
		Parameters: map[string]string{
			"kubernetes.io/foo-parameter": "free/form/string",
			"foo-parameter":               "free-form-string",
			"foo-parameter2":              "{\"embedded\": \"json\", \"with\": {\"structures\":\"inside\"}}",
		},
		ReclaimPolicy:     &deleteReclaimPolicy,
		VolumeBindingMode: &immediateMode1,
	}, {
		// 使用 Retain 回收策略：合法。
		ObjectMeta:        metav1.ObjectMeta{Name: "foo"},
		Provisioner:       "kubernetes.io/foo-provisioner",
		ReclaimPolicy:     &retainReclaimPolicy,
		VolumeBindingMode: &immediateMode1,
	}}

	// 合法用例应当通过校验。
	for k, v := range successCases {
		if errs := ValidateStorageClass(&v); len(errs) != 0 {
			t.Errorf("Expected success for %d, got %v", k, errs)
		}
	}

	// 构造一个总大小超过 maxProvisionerParameterSize 的参数 map，用于触发“参数过大”错误。
	longParameters := make(map[string]string)
	totalSize := 0
	for totalSize < maxProvisionerParameterSize {
		k := fmt.Sprintf("param/%d", totalSize)
		v := fmt.Sprintf("value-%d", totalSize)
		longParameters[k] = v
		totalSize = totalSize + len(k) + len(v)
	}

	errorCases := map[string]storage.StorageClass{
		// StorageClass 是集群级资源，不允许指定 namespace。
		"namespace is present": {
			ObjectMeta:    metav1.ObjectMeta{Name: "foo", Namespace: "bar"},
			Provisioner:   "kubernetes.io/foo-provisioner",
			ReclaimPolicy: &deleteReclaimPolicy,
		},
		// provisioner 名称中不允许出现多余的斜杠分段。
		"invalid provisioner": {
			ObjectMeta:    metav1.ObjectMeta{Name: "foo"},
			Provisioner:   "kubernetes.io/invalid/provisioner",
			ReclaimPolicy: &deleteReclaimPolicy,
		},
		// 参数名不能为空字符串。
		"invalid empty parameter name": {
			ObjectMeta:  metav1.ObjectMeta{Name: "foo"},
			Provisioner: "kubernetes.io/foo",
			Parameters: map[string]string{
				"": "value",
			},
			ReclaimPolicy: &deleteReclaimPolicy,
		},
		// provisioner 是必填字段，不能为空。
		"provisioner: Required value": {
			ObjectMeta:    metav1.ObjectMeta{Name: "foo"},
			Provisioner:   "",
			ReclaimPolicy: &deleteReclaimPolicy,
		},
		// 参数总大小超过上限会触发错误。
		"too long parameters": {
			ObjectMeta:    metav1.ObjectMeta{Name: "foo"},
			Provisioner:   "kubernetes.io/foo",
			Parameters:    longParameters,
			ReclaimPolicy: &deleteReclaimPolicy,
		},
		// Recycle 回收策略已废弃，属于非法取值。
		"invalid reclaimpolicy": {
			ObjectMeta:    metav1.ObjectMeta{Name: "foo"},
			Provisioner:   "kubernetes.io/foo",
			ReclaimPolicy: &recycleReclaimPolicy,
		},
	}

	// 非法用例应当至少产生一个校验错误。
	for testName, storageClass := range errorCases {
		if errs := ValidateStorageClass(&storageClass); len(errs) == 0 {
			t.Errorf("Expected failure for test: %s", testName)
		}
	}
}

// TestVolumeAttachmentValidation 验证 VolumeAttachment 的创建校验：attacher、节点名、卷来源必填且格式合法，
// status 中的错误码、错误信息与附加元数据也受约束。
func TestVolumeAttachmentValidation(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.MutableCSINodeAllocatableCount, true)
	volumeName := "pv-name"
	empty := ""
	// 以下是合法用例：来源可以是 PV 名称或内联卷规格，并可附带格式正确的 status。
	migrationEnabledSuccessCases := []storage.VolumeAttachment{{
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Spec: storage.VolumeAttachmentSpec{
			Attacher: "myattacher",
			Source: storage.VolumeAttachmentSource{
				PersistentVolumeName: &volumeName,
			},
			NodeName: "mynode",
		},
	}, {
		// 使用内联卷规格（InlineVolumeSpec）作为卷来源。
		ObjectMeta: metav1.ObjectMeta{Name: "foo-with-inlinespec"},
		Spec: storage.VolumeAttachmentSpec{
			Attacher: "myattacher",
			Source: storage.VolumeAttachmentSource{
				InlineVolumeSpec: &inlineSpec,
			},
			NodeName: "mynode",
		},
	}, {
		// 携带完整 status：附加成功后的元数据以及 attach/detach 错误信息。
		ObjectMeta: metav1.ObjectMeta{Name: "foo-with-status"},
		Spec: storage.VolumeAttachmentSpec{
			Attacher: "myattacher",
			Source: storage.VolumeAttachmentSource{
				PersistentVolumeName: &volumeName,
			},
			NodeName: "mynode",
		},
		Status: storage.VolumeAttachmentStatus{
			Attached: true,
			AttachmentMetadata: map[string]string{
				"foo": "bar",
			},
			AttachError: &storage.VolumeError{
				Time:    metav1.Time{},
				Message: "hello world",
			},
			DetachError: &storage.VolumeError{
				Time:    metav1.Time{},
				Message: "hello world",
			},
		},
	}, {
		// 内联卷规格与 status 同时存在。
		ObjectMeta: metav1.ObjectMeta{Name: "foo-with-inlinespec-and-status"},
		Spec: storage.VolumeAttachmentSpec{
			Attacher: "myattacher",
			Source: storage.VolumeAttachmentSource{
				InlineVolumeSpec: &inlineSpec,
			},
			NodeName: "mynode",
		},
		Status: storage.VolumeAttachmentStatus{
			Attached: true,
			AttachmentMetadata: map[string]string{
				"foo": "bar",
			},
			AttachError: &storage.VolumeError{
				Time:    metav1.Time{},
				Message: "hello world",
			},
			DetachError: &storage.VolumeError{
				Time:    metav1.Time{},
				Message: "hello world",
			},
		},
	}, {
		// 错误码在允许的取值范围内（例如 7）。
		ObjectMeta: metav1.ObjectMeta{Name: "foo-with-valid-error-code"},
		Spec: storage.VolumeAttachmentSpec{
			Attacher: "myattacher",
			Source: storage.VolumeAttachmentSource{
				PersistentVolumeName: &volumeName,
			},
			NodeName: "mynode",
		},
		Status: storage.VolumeAttachmentStatus{
			Attached: true,
			AttachmentMetadata: map[string]string{
				"foo": "bar",
			},
			AttachError: &storage.VolumeError{
				Time:      metav1.Time{},
				Message:   "hello world",
				ErrorCode: ptr.To[int32](7),
			},
			DetachError: &storage.VolumeError{
				Time:    metav1.Time{},
				Message: "hello world",
			},
		},
	}}

	for _, volumeAttachment := range migrationEnabledSuccessCases {
		if errs := ValidateVolumeAttachment(&volumeAttachment); len(errs) != 0 {
			t.Errorf("expected success: %v %v", volumeAttachment, errs)
		}
	}
	// 以下是非法用例。
	migrationEnabledErrorCases := []storage.VolumeAttachment{{
		// attacher 名称为空：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Spec: storage.VolumeAttachmentSpec{
			Attacher: "",
			NodeName: "mynode",
			Source: storage.VolumeAttachmentSource{
				PersistentVolumeName: &volumeName,
			},
		},
	}, {
		// 节点名为空：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Spec: storage.VolumeAttachmentSpec{
			Attacher: "myattacher",
			NodeName: "",
			Source: storage.VolumeAttachmentSource{
				PersistentVolumeName: &volumeName,
			},
		},
	}, {
		// 未提供卷名称（PersistentVolumeName 为 nil）：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Spec: storage.VolumeAttachmentSpec{
			Attacher: "myattacher",
			NodeName: "node",
			Source: storage.VolumeAttachmentSource{
				PersistentVolumeName: nil,
			},
		},
	}, {
		// 卷名称为空字符串：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Spec: storage.VolumeAttachmentSpec{
			Attacher: "myattacher",
			NodeName: "node",
			Source: storage.VolumeAttachmentSource{
				PersistentVolumeName: &empty,
			},
		},
	}, {
		// 错误信息超过长度上限：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Spec: storage.VolumeAttachmentSpec{
			Attacher: "myattacher",
			NodeName: "node",
			Source: storage.VolumeAttachmentSource{
				PersistentVolumeName: &volumeName,
			},
		},
		Status: storage.VolumeAttachmentStatus{
			Attached: true,
			AttachmentMetadata: map[string]string{
				"foo": "bar",
			},
			AttachError: &storage.VolumeError{
				Time:    metav1.Time{},
				Message: "hello world",
			},
			DetachError: &storage.VolumeError{
				Time:    metav1.Time{},
				Message: strings.Repeat("a", maxVolumeErrorMessageSize+1),
			},
		},
	},
		{
			// 错误码超出允许范围（含边界检查）：非法。
			ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			Spec: storage.VolumeAttachmentSpec{
				Attacher: "myattacher",
				NodeName: "node",
				Source: storage.VolumeAttachmentSource{
					PersistentVolumeName: &volumeName,
				},
			},
			Status: storage.VolumeAttachmentStatus{
				Attached: true,
				AttachmentMetadata: map[string]string{
					"foo": "bar",
				},
				AttachError: &storage.VolumeError{
					Time:      metav1.Time{},
					Message:   "hello world",
					ErrorCode: ptr.To[int32](-1),
				},
				DetachError: &storage.VolumeError{
					Time:      metav1.Time{},
					Message:   "hello world",
					ErrorCode: ptr.To[int32](5),
				},
			},
		},
		{
			// 附加元数据超过长度上限：非法。
			ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			Spec: storage.VolumeAttachmentSpec{
				Attacher: "myattacher",
				NodeName: "node",
				Source: storage.VolumeAttachmentSource{
					PersistentVolumeName: &volumeName,
				},
			},
			Status: storage.VolumeAttachmentStatus{
				Attached: true,
				AttachmentMetadata: map[string]string{
					"foo": strings.Repeat("a", maxAttachedVolumeMetadataSize),
				},
				AttachError: &storage.VolumeError{
					Time:    metav1.Time{},
					Message: "hello world",
				},
				DetachError: &storage.VolumeError{
					Time:    metav1.Time{},
					Message: "hello world",
				},
			},
		}, {
			// 卷来源既没有 PV 名称也没有内联规格：非法。
			ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			Spec: storage.VolumeAttachmentSpec{
				Attacher: "myattacher",
				NodeName: "node",
				Source:   storage.VolumeAttachmentSource{},
			},
		}, {
			// 同时设置 PV 名称与内联规格，二者互斥：非法。
			ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			Spec: storage.VolumeAttachmentSpec{
				Attacher: "myattacher",
				NodeName: "node",
				Source: storage.VolumeAttachmentSource{
					PersistentVolumeName: &volumeName,
					InlineVolumeSpec:     &inlineSpec,
				},
			},
		}, {
			// 内联规格中没有 CSI 卷来源（仅支持 CSI）：非法。
			ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			Spec: storage.VolumeAttachmentSpec{
				Attacher: "myattacher",
				NodeName: "node",
				Source: storage.VolumeAttachmentSource{
					PersistentVolumeName: &volumeName,
					InlineVolumeSpec: &api.PersistentVolumeSpec{
						Capacity: api.ResourceList{
							api.ResourceName(api.ResourceStorage): resource.MustParse("10G"),
						},
						AccessModes: []api.PersistentVolumeAccessMode{api.ReadWriteOnce},
						PersistentVolumeSource: api.PersistentVolumeSource{
							FlexVolume: &api.FlexPersistentVolumeSource{
								Driver: "kubernetes.io/blue",
								FSType: "ext4",
							},
						},
						StorageClassName: "test-storage-class",
					},
				},
			},
		}}

	for _, volumeAttachment := range migrationEnabledErrorCases {
		if errs := ValidateVolumeAttachment(&volumeAttachment); len(errs) == 0 {
			t.Errorf("expected failure for test: %v", volumeAttachment)
		}
	}
}

// TestVolumeAttachmentUpdateValidation 验证 VolumeAttachment 的更新校验：spec 中的 attacher、来源、节点名均不可变，
// status 允许更新但仍需满足格式约束。
func TestVolumeAttachmentUpdateValidation(t *testing.T) {
	volumeName := "foo"
	newVolumeName := "bar"

	old := storage.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Spec: storage.VolumeAttachmentSpec{
			Attacher: "myattacher",
			Source:   storage.VolumeAttachmentSource{},
			NodeName: "mynode",
		},
	}

	// 合法更新：无变更或仅修改 status。
	successCases := []storage.VolumeAttachment{{
		// 无任何变更：合法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Spec: storage.VolumeAttachmentSpec{
			Attacher: "myattacher",
			Source:   storage.VolumeAttachmentSource{},
			NodeName: "mynode",
		},
	}, {
		// 仅修改 status：合法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Spec: storage.VolumeAttachmentSpec{
			Attacher: "myattacher",
			Source:   storage.VolumeAttachmentSource{},
			NodeName: "mynode",
		},
		Status: storage.VolumeAttachmentStatus{
			Attached: true,
			AttachmentMetadata: map[string]string{
				"foo": "bar",
			},
			AttachError: &storage.VolumeError{
				Time:    metav1.Time{},
				Message: "hello world",
			},
			DetachError: &storage.VolumeError{
				Time:    metav1.Time{},
				Message: "hello world",
			},
		},
	}}

	for _, volumeAttachment := range successCases {
		volumeAttachment.Spec.Source = storage.VolumeAttachmentSource{}
		old.Spec.Source = storage.VolumeAttachmentSource{}
		// 分别测试以 PersistentVolumeName 作为来源的场景。
		volumeAttachment.Spec.Source.PersistentVolumeName = &volumeName
		old.Spec.Source.PersistentVolumeName = &volumeName
		if errs := ValidateVolumeAttachmentUpdate(&volumeAttachment, &old); len(errs) != 0 {
			t.Errorf("expected success: %+v", errs)
		}

		volumeAttachment.Spec.Source = storage.VolumeAttachmentSource{}
		old.Spec.Source = storage.VolumeAttachmentSource{}
		// 分别测试以 InlineVolumeSpec 作为来源的场景。
		volumeAttachment.Spec.Source.InlineVolumeSpec = &inlineSpec
		old.Spec.Source.InlineVolumeSpec = &inlineSpec
		if errs := ValidateVolumeAttachmentUpdate(&volumeAttachment, &old); len(errs) != 0 {
			t.Errorf("expected success: %+v", errs)
		}
	}

	// 重置 old 的来源，避免上一轮循环残留的值污染后续断言。
	old.Spec.Source = storage.VolumeAttachmentSource{}
	old.Spec.Source.PersistentVolumeName = &volumeName

	// 非法更新：修改不可变字段或写入非法 status。
	errorCases := []storage.VolumeAttachment{{
		// 修改 attacher（不可变字段）：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Spec: storage.VolumeAttachmentSpec{
			Attacher: "another-attacher",
			Source: storage.VolumeAttachmentSource{
				PersistentVolumeName: &volumeName,
			},
			NodeName: "mynode",
		},
	}, {
		// 修改来源卷名（不可变字段）：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Spec: storage.VolumeAttachmentSpec{
			Attacher: "myattacher",
			Source: storage.VolumeAttachmentSource{
				PersistentVolumeName: &newVolumeName,
			},
			NodeName: "mynode",
		},
	}, {
		// 修改节点名（不可变字段）：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Spec: storage.VolumeAttachmentSpec{
			Attacher: "myattacher",
			Source: storage.VolumeAttachmentSource{
				PersistentVolumeName: &volumeName,
			},
			NodeName: "anothernode",
		},
	}, {
		// 修改来源类型（不可变字段）：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Spec: storage.VolumeAttachmentSpec{
			Attacher: "myattacher",
			Source: storage.VolumeAttachmentSource{
				InlineVolumeSpec: &inlineSpec,
			},
			NodeName: "mynode",
		},
	}, {
		// 添加非法 status：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Spec: storage.VolumeAttachmentSpec{
			Attacher: "myattacher",
			Source: storage.VolumeAttachmentSource{
				PersistentVolumeName: &volumeName,
			},
			NodeName: "mynode",
		},
		Status: storage.VolumeAttachmentStatus{
			Attached: true,
			AttachmentMetadata: map[string]string{
				"foo": "bar",
			},
			AttachError: &storage.VolumeError{
				Time:    metav1.Time{},
				Message: strings.Repeat("a", maxAttachedVolumeMetadataSize),
			},
			DetachError: &storage.VolumeError{
				Time:    metav1.Time{},
				Message: "hello world",
			},
		},
	}}

	for _, volumeAttachment := range errorCases {
		if errs := ValidateVolumeAttachmentUpdate(&volumeAttachment, &old); len(errs) == 0 {
			t.Errorf("Expected failure for test: %+v", volumeAttachment)
		}
	}
}

// TestVolumeAttachmentValidationV1 验证 v1 版本对 attacher 名称与 PV 名称的格式校验（需符合 DNS 子域规范）。
func TestVolumeAttachmentValidationV1(t *testing.T) {
	volumeName := "pv-name"
	invalidVolumeName := "-invalid-@#$%^&*()-"
	successCases := []storage.VolumeAttachment{{
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Spec: storage.VolumeAttachmentSpec{
			Attacher: "myattacher",
			Source: storage.VolumeAttachmentSource{
				PersistentVolumeName: &volumeName,
			},
			NodeName: "mynode",
		},
	}}

	for _, volumeAttachment := range successCases {
		if errs := ValidateVolumeAttachment(&volumeAttachment); len(errs) != 0 {
			t.Errorf("expected success: %+v", errs)
		}
	}

	// 以下是非法用例。
	errorCases := []storage.VolumeAttachment{{
		// attacher 名称包含非法字符：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Spec: storage.VolumeAttachmentSpec{
			Attacher: "invalid-@#$%^&*()",
			NodeName: "mynode",
			Source: storage.VolumeAttachmentSource{
				PersistentVolumeName: &volumeName,
			},
		},
	}, {
		// PV 名称不符合 DNS 子域格式：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Spec: storage.VolumeAttachmentSpec{
			Attacher: "myattacher",
			NodeName: "mynode",
			Source: storage.VolumeAttachmentSource{
				PersistentVolumeName: &invalidVolumeName,
			},
		},
	}}

	for _, volumeAttachment := range errorCases {
		if errs := ValidateVolumeAttachment(&volumeAttachment); len(errs) == 0 {
			t.Errorf("Expected failure for test: %+v", volumeAttachment)
		}
	}
}

// makeClass 构造一个带指定绑定模式与拓扑约束的 StorageClass，供绑定模式/拓扑相关测试复用。
func makeClass(mode *storage.VolumeBindingMode, topologies []api.TopologySelectorTerm) *storage.StorageClass {
	return &storage.StorageClass{
		ObjectMeta:        metav1.ObjectMeta{Name: "foo", ResourceVersion: "foo"},
		Provisioner:       "kubernetes.io/foo-provisioner",
		ReclaimPolicy:     &deleteReclaimPolicy,
		VolumeBindingMode: mode,
		AllowedTopologies: topologies,
	}
}

// bindingTest 描述一个 StorageClass 用例及其期望的校验结果。
type bindingTest struct {
	class         *storage.StorageClass
	shouldSucceed bool
}

// TestValidateVolumeBindingMode 验证 volumeBindingMode 的取值：未设置或非法值会失败，
// Immediate 与 WaitForFirstConsumer 为合法值。
func TestValidateVolumeBindingMode(t *testing.T) {
	cases := map[string]bindingTest{
		// 未设置绑定模式：非法。
		"no mode": {
			class:         makeClass(nil, nil),
			shouldSucceed: false,
		},
		// 立即绑定（Immediate）：合法。
		"immediate mode": {
			class:         makeClass(&immediateMode1, nil),
			shouldSucceed: true,
		},
		// 等待首个消费者（WaitForFirstConsumer）：合法。
		"waiting mode": {
			class:         makeClass(&waitingMode, nil),
			shouldSucceed: true,
		},
		// 任意字符串：非法。
		"invalid mode": {
			class:         makeClass(&invalidMode, nil),
			shouldSucceed: false,
		},
	}

	for testName, testCase := range cases {
		errs := ValidateStorageClass(testCase.class)
		if testCase.shouldSucceed && len(errs) != 0 {
			t.Errorf("Expected success for test %q, got %v", testName, errs)
		}
		if !testCase.shouldSucceed && len(errs) == 0 {
			t.Errorf("Expected failure for test %q, got success", testName)
		}
	}
}

// updateTest 描述一次 StorageClass 更新：旧对象、新对象以及期望的校验结果。
type updateTest struct {
	oldClass      *storage.StorageClass
	newClass      *storage.StorageClass
	shouldSucceed bool
}

// TestValidateUpdateVolumeBindingMode 验证绑定模式更新规则：一旦设置后不允许修改，
// 包括从无到有、从有到无以及不同模式之间切换。
func TestValidateUpdateVolumeBindingMode(t *testing.T) {
	noBinding := makeClass(nil, nil)
	immediateBinding1 := makeClass(&immediateMode1, nil)
	immediateBinding2 := makeClass(&immediateMode2, nil)
	waitBinding := makeClass(&waitingMode, nil)

	cases := map[string]updateTest{
		// 新旧都未设置：合法。
		"old and new no mode": {
			oldClass:      noBinding,
			newClass:      noBinding,
			shouldSucceed: true,
		},
		// 新旧指向同一个对象（值未变）：合法。
		"old and new same mode ptr": {
			oldClass:      immediateBinding1,
			newClass:      immediateBinding1,
			shouldSucceed: true,
		},
		// 不同指针但值相同：合法。
		"old and new same mode value": {
			oldClass:      immediateBinding1,
			newClass:      immediateBinding2,
			shouldSucceed: true,
		},
		// 从“未设置”改为“已设置”：非法。
		"old no mode, new mode": {
			oldClass:      noBinding,
			newClass:      waitBinding,
			shouldSucceed: false,
		},
		// 从“已设置”改为“未设置”：非法。
		"old mode, new no mode": {
			oldClass:      waitBinding,
			newClass:      noBinding,
			shouldSucceed: false,
		},
		// 在不同模式之间切换：非法。
		"old and new different modes": {
			oldClass:      waitBinding,
			newClass:      immediateBinding1,
			shouldSucceed: false,
		},
	}

	for testName, testCase := range cases {
		errs := ValidateStorageClassUpdate(testCase.newClass, testCase.oldClass)
		if testCase.shouldSucceed && len(errs) != 0 {
			t.Errorf("Expected success for %v, got %v", testName, errs)
		}
		if !testCase.shouldSucceed && len(errs) == 0 {
			t.Errorf("Expected failure for %v, got success", testName)
		}
	}
}

// TestValidateAllowedTopologies 验证 allowedTopologies 的校验：标签键名、值列表、
// 表达式重复以及拓扑项去重（忽略表达式/值的顺序）等规则。
func TestValidateAllowedTopologies(t *testing.T) {

	// 合法拓扑：两个拓扑项，每项包含 zone 与 hostname 两个表达式。
	validTopology := []api.TopologySelectorTerm{{
		MatchLabelExpressions: []api.TopologySelectorLabelRequirement{{
			Key:    "failure-domain.beta.kubernetes.io/zone",
			Values: []string{"zone1"},
		}, {
			Key:    "kubernetes.io/hostname",
			Values: []string{"node1"},
		}},
	}, {
		MatchLabelExpressions: []api.TopologySelectorLabelRequirement{{
			Key:    "failure-domain.beta.kubernetes.io/zone",
			Values: []string{"zone2"},
		}, {
			Key:    "kubernetes.io/hostname",
			Values: []string{"node2"},
		}},
	}}

	// 标签键以斜杠开头：非法。
	topologyInvalidKey := []api.TopologySelectorTerm{{
		MatchLabelExpressions: []api.TopologySelectorLabelRequirement{{
			Key:    "/invalidkey",
			Values: []string{"zone1"},
		}},
	}}

	// 表达式没有任何取值：非法。
	topologyLackOfValues := []api.TopologySelectorTerm{{
		MatchLabelExpressions: []api.TopologySelectorLabelRequirement{{
			Key:    "kubernetes.io/hostname",
			Values: []string{},
		}},
	}}

	// 同一表达式内出现重复值：非法。
	topologyDupValues := []api.TopologySelectorTerm{{
		MatchLabelExpressions: []api.TopologySelectorLabelRequirement{{
			Key:    "kubernetes.io/hostname",
			Values: []string{"node1", "node1"},
		}},
	}}

	// 同一表达式内有多个不同值：合法。
	topologyMultiValues := []api.TopologySelectorTerm{{
		MatchLabelExpressions: []api.TopologySelectorLabelRequirement{{
			Key:    "kubernetes.io/hostname",
			Values: []string{"node1", "node2"},
		}},
	}}

	// 拓扑项的表达式列表为空：非法。
	topologyEmptyMatchLabelExpressions := []api.TopologySelectorTerm{{
		MatchLabelExpressions: nil,
	}}

	// 同一拓扑项内出现重复键：非法。
	topologyDupKeys := []api.TopologySelectorTerm{{
		MatchLabelExpressions: []api.TopologySelectorLabelRequirement{{
			Key:    "kubernetes.io/hostname",
			Values: []string{"node1"},
		}, {
			Key:    "kubernetes.io/hostname",
			Values: []string{"node2"},
		}},
	}}

	// 不同拓扑项中使用相同键：合法（判重以单个拓扑项为范围）。
	topologyMultiTerm := []api.TopologySelectorTerm{{
		MatchLabelExpressions: []api.TopologySelectorLabelRequirement{{
			Key:    "kubernetes.io/hostname",
			Values: []string{"node1"},
		}},
	}, {
		MatchLabelExpressions: []api.TopologySelectorLabelRequirement{{
			Key:    "kubernetes.io/hostname",
			Values: []string{"node2"},
		}},
	}}

	// 两个完全相同的拓扑项：非法（重复拓扑）。
	topologyDupTermsIdentical := []api.TopologySelectorTerm{{
		MatchLabelExpressions: []api.TopologySelectorLabelRequirement{{
			Key:    "failure-domain.beta.kubernetes.io/zone",
			Values: []string{"zone1"},
		}, {
			Key:    "kubernetes.io/hostname",
			Values: []string{"node1"},
		}},
	}, {
		MatchLabelExpressions: []api.TopologySelectorLabelRequirement{{
			Key:    "failure-domain.beta.kubernetes.io/zone",
			Values: []string{"zone1"},
		}, {
			Key:    "kubernetes.io/hostname",
			Values: []string{"node1"},
		}},
	}}

	// 两个拓扑项中一个表达式相同、另一个不同：不算重复，合法。
	topologyExprsOneSameOneDiff := []api.TopologySelectorTerm{{
		MatchLabelExpressions: []api.TopologySelectorLabelRequirement{{
			Key:    "failure-domain.beta.kubernetes.io/zone",
			Values: []string{"zone1"},
		}, {
			Key:    "kubernetes.io/hostname",
			Values: []string{"node1"},
		}},
	}, {
		MatchLabelExpressions: []api.TopologySelectorLabelRequirement{{
			Key:    "failure-domain.beta.kubernetes.io/zone",
			Values: []string{"zone1"},
		}, {
			Key:    "kubernetes.io/hostname",
			Values: []string{"node2"},
		}},
	}}

	// 两个拓扑项的值列表部分重叠：不算重复，合法。
	topologyValuesOneSameOneDiff := []api.TopologySelectorTerm{{
		MatchLabelExpressions: []api.TopologySelectorLabelRequirement{{
			Key:    "kubernetes.io/hostname",
			Values: []string{"node1", "node2"},
		}},
	}, {
		MatchLabelExpressions: []api.TopologySelectorLabelRequirement{{
			Key:    "kubernetes.io/hostname",
			Values: []string{"node1", "node3"},
		}},
	}}

	// 表达式顺序不同但语义相同的两个拓扑项：仍视为重复，非法。
	topologyDupTermsDiffExprOrder := []api.TopologySelectorTerm{{
		MatchLabelExpressions: []api.TopologySelectorLabelRequirement{{
			Key:    "kubernetes.io/hostname",
			Values: []string{"node1"},
		}, {
			Key:    "failure-domain.beta.kubernetes.io/zone",
			Values: []string{"zone1"},
		}},
	}, {
		MatchLabelExpressions: []api.TopologySelectorLabelRequirement{{
			Key:    "failure-domain.beta.kubernetes.io/zone",
			Values: []string{"zone1"},
		}, {
			Key:    "kubernetes.io/hostname",
			Values: []string{"node1"},
		}},
	}}

	// 值列表顺序不同但语义相同的两个拓扑项：仍视为重复，非法。
	topologyDupTermsDiffValueOrder := []api.TopologySelectorTerm{{
		MatchLabelExpressions: []api.TopologySelectorLabelRequirement{{
			Key:    "failure-domain.beta.kubernetes.io/zone",
			Values: []string{"zone1", "zone2"},
		}},
	}, {
		MatchLabelExpressions: []api.TopologySelectorLabelRequirement{{
			Key:    "failure-domain.beta.kubernetes.io/zone",
			Values: []string{"zone2", "zone1"},
		}},
	}}

	cases := map[string]bindingTest{
		// 未设置拓扑：合法。
		"no topology": {
			class:         makeClass(&waitingMode, nil),
			shouldSucceed: true,
		},
		// 合法拓扑配置。
		"valid topology": {
			class:         makeClass(&waitingMode, validTopology),
			shouldSucceed: true,
		},
		// 非法标签键名。
		"topology invalid key": {
			class:         makeClass(&waitingMode, topologyInvalidKey),
			shouldSucceed: false,
		},
		// 表达式缺少取值。
		"topology lack of values": {
			class:         makeClass(&waitingMode, topologyLackOfValues),
			shouldSucceed: false,
		},
		// 表达式内存在重复值。
		"duplicate TopologySelectorRequirement values": {
			class:         makeClass(&waitingMode, topologyDupValues),
			shouldSucceed: false,
		},
		// 表达式内有多个不同值。
		"multiple TopologySelectorRequirement values": {
			class:         makeClass(&waitingMode, topologyMultiValues),
			shouldSucceed: true,
		},
		// 表达式列表为空。
		"empty MatchLabelExpressions": {
			class:         makeClass(&waitingMode, topologyEmptyMatchLabelExpressions),
			shouldSucceed: false,
		},
		// 同一拓扑项内键名重复。
		"duplicate MatchLabelExpression keys": {
			class:         makeClass(&waitingMode, topologyDupKeys),
			shouldSucceed: false,
		},
		// 跨拓扑项的重复键名。
		"duplicate MatchLabelExpression keys but across separate terms": {
			class:         makeClass(&waitingMode, topologyMultiTerm),
			shouldSucceed: true,
		},
		// 完全相同的重复拓扑项。
		"duplicate AllowedTopologies terms - identical": {
			class:         makeClass(&waitingMode, topologyDupTermsIdentical),
			shouldSucceed: false,
		},
		// 部分表达式相同的两个拓扑项。
		"two AllowedTopologies terms, with a pair of the same MatchLabelExpressions and a pair of different ones": {
			class:         makeClass(&waitingMode, topologyExprsOneSameOneDiff),
			shouldSucceed: true,
		},
		// 部分取值相同的两个拓扑项。
		"two AllowedTopologies terms, with a pair of the same Values and a pair of different ones": {
			class:         makeClass(&waitingMode, topologyValuesOneSameOneDiff),
			shouldSucceed: true,
		},
		// 表达式顺序不同的重复拓扑项。
		"duplicate AllowedTopologies terms - different MatchLabelExpressions order": {
			class:         makeClass(&waitingMode, topologyDupTermsDiffExprOrder),
			shouldSucceed: false,
		},
		// 值顺序不同的重复拓扑项。
		"duplicate AllowedTopologies terms - different TopologySelectorRequirement values order": {
			class:         makeClass(&waitingMode, topologyDupTermsDiffValueOrder),
			shouldSucceed: false,
		},
	}

	for testName, testCase := range cases {
		errs := ValidateStorageClass(testCase.class)
		if testCase.shouldSucceed && len(errs) != 0 {
			t.Errorf("Expected success for test %q, got %v", testName, errs)
		}
		if !testCase.shouldSucceed && len(errs) == 0 {
			t.Errorf("Expected failure for test %q, got success", testName)
		}
	}
}

// TestCSINodeValidation 验证 CSINode 创建校验：驱动名称格式、节点 ID 长度、拓扑键格式与重复、
// 可分配卷数范围等。
func TestCSINodeValidation(t *testing.T) {
	driverName := "driver-name"
	driverName2 := "1io.kubernetes-storage-2-csi-driver3"
	longName := strings.Repeat("a", 88)
	nodeID := "nodeA"
	longID := strings.Repeat("a", 257)
	// 以下是合法用例。
	successCases := []storage.CSINode{{
		// 驱动名仅含点号：合法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo1"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io.kubernetes.storage.csi.driver",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}},
		},
	}, {
		// 驱动名仅含连字符：合法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo2"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io-kubernetes-storage-csi-driver",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}},
		},
	}, {
		// 驱动名含数字：合法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo3"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "1io-kubernetes-storage-2-csi-driver3",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}},
		},
	}, {
		// 驱动名同时含点与连字符：合法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo4"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io.kubernetes.storage-csi-driver",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}},
		},
	}, {
		// 驱动名含点、连字符与数字：合法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo5"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         driverName2,
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}},
		},
	}, {
		// 驱动名长度为 1：合法（最小长度）。
		ObjectMeta: metav1.ObjectMeta{Name: "foo2"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "a",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}},
		},
	}, {
		// 多个驱动且节点 ID、拓扑键各不相同：合法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo6"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "driver1",
				NodeID:       "node1",
				TopologyKeys: []string{"key1", "key2"},
			}, {
				Name:         "driverB",
				NodeID:       "nodeA",
				TopologyKeys: []string{"keyA", "keyB"},
			}},
		},
	}, {
		// 多个驱动共享相同节点 ID 与拓扑键：合法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo7"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "driver1",
				NodeID:       "node1",
				TopologyKeys: []string{"key1"},
			}, {
				Name:         "driver2",
				NodeID:       "node1",
				TopologyKeys: []string{"key1"},
			}},
		},
	}, {
		// 可分配卷数为 0：合法下界。
		ObjectMeta: metav1.ObjectMeta{Name: "foo11"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io.kubernetes.storage.csi.driver",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
				Allocatable:  &storage.VolumeNodeResources{Count: ptr.To[int32](0)},
			}},
		},
	}, {
		// 可分配卷数为正数：合法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo11"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io.kubernetes.storage.csi.driver",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
				Allocatable:  &storage.VolumeNodeResources{Count: ptr.To[int32](1)},
			}},
		},
	}, {
		// 拓扑键名含 -、_ 与点：合法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo8"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "driver1",
				NodeID:       "node1",
				TopologyKeys: []string{"zone_1", "zone.2"},
			}, {
				Name:         "driver2",
				NodeID:       "node1",
				TopologyKeys: []string{"zone-3", "zone.4"},
			}},
		},
	}, {
		// 拓扑前缀含连字符与点：合法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo9"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "driver1",
				NodeID:       "node1",
				TopologyKeys: []string{"company-com/zone1", "company.com/zone2"},
			}},
		},
	}, {
		// 未设置拓扑键：合法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo10"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:   driverName,
				NodeID: nodeID,
			}},
		},
	}}

	for _, csiNode := range successCases {
		if errs := ValidateCSINode(&csiNode); len(errs) != 0 {
			t.Errorf("expected success: %v", errs)
		}
	}

	// 以下是非法用例。
	errorCases := []storage.CSINode{{
		// 驱动名为空：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo1"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}},
		},
	}, {
		// 驱动名首字符非法（以下划线开头）：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo3"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "_io.kubernetes.storage.csi.driver",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}},
		},
	}, {
		// 驱动名尾字符非法（以斜杠结尾）：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo4"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io.kubernetes.storage.csi.driver/",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}},
		},
	}, {
		// 驱动名含非法分隔符（斜杠与波浪号）：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo5"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io/kubernetes/storage/csi~driver",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}},
		},
	}, {
		// 驱动名仅含下划线：非法字符。
		ObjectMeta: metav1.ObjectMeta{Name: "foo6"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io_kubernetes_storage_csi_driver",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}},
		},
	}, {
		// 驱动名超过 63 字符：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo7"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         longName,
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}},
		},
	}, {
		// 未设置驱动名：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo8"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}},
		},
	}, {
		// 某个拓扑键为空字符串：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo9"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         driverName,
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", ""},
			}},
		},
	}, {
		// 驱动列表中出现重名驱动：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo10"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "driver1",
				NodeID:       "node1",
				TopologyKeys: []string{"key1", "key2"},
			}, {
				Name:         "driver1",
				NodeID:       "nodeX",
				TopologyKeys: []string{"keyA", "keyB"},
			}},
		},
	}, {
		// 单个驱动内出现重复拓扑键：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo11"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "driver1",
				NodeID:       "node1",
				TopologyKeys: []string{"key1", "key1"},
			}},
		},
	}, {
		// 多个驱动中某一驱动的拓扑键重复：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo12"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "driver1",
				NodeID:       "node1",
				TopologyKeys: []string{"key1"},
			}, {
				Name:         "driver2",
				NodeID:       "nodeX",
				TopologyKeys: []string{"keyA", "keyA"},
			}},
		},
	}, {
		// 节点 ID 为空：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo13"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         driverName,
				NodeID:       "",
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}},
		},
	}, {
		// 可分配卷数为负数：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo11"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io.kubernetes.storage.csi.driver",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
				Allocatable:  &storage.VolumeNodeResources{Count: ptr.To[int32](-1)},
			}},
		},
	}, {
		// 拓扑前缀必须为小写：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo14"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         driverName,
				NodeID:       "node1",
				TopologyKeys: []string{"Company.Com/zone1", "company.com/zone2"},
			}},
		},
	}, {
		// 节点 ID 超过长度上限：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo15"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         driverName,
				NodeID:       longID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}},
		},
	},
	}

	for _, csiNode := range errorCases {
		if errs := ValidateCSINode(&csiNode); len(errs) == 0 {
			t.Errorf("Expected failure for test: %v", csiNode)
		}
	}
}

// TestCSINodeUpdateValidation 验证 CSINode 更新校验：驱动列表可以增删，但已有驱动的 nodeID/拓扑键不可变；
// 可分配卷数是否可变由 MutableCSINodeAllocatableCount 特性开关控制。
func TestCSINodeUpdateValidation(t *testing.T) {
	nodeID := "nodeA"
	// 先关闭特性开关，验证此时 allocatable 字段不可随意修改。
	featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.35"))
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.MutableCSINodeAllocatableCount, false)
	// 基准对象：包含两个驱动，其中第二个设置了可分配卷数。
	old := storage.CSINode{
		ObjectMeta: metav1.ObjectMeta{Name: "foo1"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io.kubernetes.storage.csi.driver-1",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}, {
				Name:         "io.kubernetes.storage.csi.driver-2",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
				Allocatable:  &storage.VolumeNodeResources{Count: ptr.To[int32](20)},
			}},
		},
	}

	// 以下是合法更新用例。
	successCases := []storage.CSINode{{
		// 无任何变更：合法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo1"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io.kubernetes.storage.csi.driver-1",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}, {
				Name:         "io.kubernetes.storage.csi.driver-2",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
				Allocatable:  &storage.VolumeNodeResources{Count: ptr.To[int32](20)},
			}},
		},
	}, {
		// 删除一个驱动：合法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo1"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io.kubernetes.storage.csi.driver-1",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}},
		},
	}, {
		// 新增一个驱动：合法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo1"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io.kubernetes.storage.csi.driver-1",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}, {
				Name:         "io.kubernetes.storage.csi.driver-2",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
				Allocatable:  &storage.VolumeNodeResources{Count: ptr.To[int32](20)},
			}, {
				Name:         "io.kubernetes.storage.csi.driver-3",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
				Allocatable:  &storage.VolumeNodeResources{Count: ptr.To[int32](30)},
			}},
		},
	}, {
		// 同时删除与新增驱动：合法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo1"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io.kubernetes.storage.csi.driver-1",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}, {
				Name:         "io.kubernetes.storage.csi.new-driver",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
				Allocatable:  &storage.VolumeNodeResources{Count: ptr.To[int32](30)},
			}},
		},
	}}

	for _, csiNode := range successCases {
		if errs := ValidateCSINodeUpdate(&csiNode, &old); len(errs) != 0 {
			t.Errorf("expected success: %+v", errs)
		}
	}

	// 以下是非法更新用例。
	errorCases := []storage.CSINode{{
		// 非法修改已有驱动的节点 ID。
		ObjectMeta: metav1.ObjectMeta{Name: "foo1"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io.kubernetes.storage.csi.driver-1",
				NodeID:       "nodeB",
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}, {
				Name:         "io.kubernetes.storage.csi.driver-2",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
				Allocatable:  &storage.VolumeNodeResources{Count: ptr.To[int32](20)},
			}},
		},
	}, {
		// 非法修改已有驱动的拓扑键。
		ObjectMeta: metav1.ObjectMeta{Name: "foo1"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io.kubernetes.storage.csi.driver-1",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}, {
				Name:         "io.kubernetes.storage.csi.driver-2",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone2"},
				Allocatable:  &storage.VolumeNodeResources{Count: ptr.To[int32](20)},
			}},
		},
	}, {
		// 原本未设置 allocatable，尝试为其赋值：开关关闭时非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo1"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io.kubernetes.storage.csi.driver-1",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
				Allocatable:  &storage.VolumeNodeResources{Count: ptr.To[int32](10)},
			}, {
				Name:         "io.kubernetes.storage.csi.driver-2",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
				Allocatable:  &storage.VolumeNodeResources{Count: ptr.To[int32](20)},
			}},
		},
	}, {
		// 把可分配卷数改成另一个值：开关关闭时非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo1"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io.kubernetes.storage.csi.driver-1",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}, {
				Name:         "io.kubernetes.storage.csi.driver-2",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
				Allocatable:  &storage.VolumeNodeResources{Count: ptr.To[int32](21)},
			}},
		},
	}, {
		// 把可分配卷数置空：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo1"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io.kubernetes.storage.csi.driver-1",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}, {
				Name:         "io.kubernetes.storage.csi.driver-2",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
				Allocatable:  &storage.VolumeNodeResources{Count: nil},
			}},
		},
	}, {
		// 删除 allocatable 字段：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo1"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io.kubernetes.storage.csi.driver-1",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}, {
				Name:         "io.kubernetes.storage.csi.driver-2",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}},
		},
	}}

	for _, csiNode := range errorCases {
		if errs := ValidateCSINodeUpdate(&csiNode, &old); len(errs) == 0 {
			t.Errorf("Expected failure for test: %+v", csiNode)
		}
	}

	// 打开 MutableCSINodeAllocatableCount 特性开关，验证现在允许修改可分配卷数。
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.MutableCSINodeAllocatableCount, true)
	successCases = []storage.CSINode{{
		// 开关打开后修改可分配卷数：合法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo1"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io.kubernetes.storage.csi.driver-1",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}, {
				Name:         "io.kubernetes.storage.csi.driver-2",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
				Allocatable:  &storage.VolumeNodeResources{Count: ptr.To[int32](21)},
			}},
		},
	}}

	// 开关打开后仍有不可修改的字段，这些用例应报错。
	errorCases = []storage.CSINode{{
		// 节点 ID 仍不可修改：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo1"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io.kubernetes.storage.csi.driver-1",
				NodeID:       "nodeB",
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}, {
				Name:         "io.kubernetes.storage.csi.driver-2",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
				Allocatable:  &storage.VolumeNodeResources{Count: ptr.To[int32](20)},
			}},
		},
	}, {
		// 拓扑键仍不可修改：非法。
		ObjectMeta: metav1.ObjectMeta{Name: "foo1"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{{
				Name:         "io.kubernetes.storage.csi.driver-1",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone1", "company.com/zone2"},
			}, {
				Name:         "io.kubernetes.storage.csi.driver-2",
				NodeID:       nodeID,
				TopologyKeys: []string{"company.com/zone2"},
				Allocatable:  &storage.VolumeNodeResources{Count: ptr.To[int32](20)},
			}},
		},
	}}

	for _, csiNode := range errorCases {
		if errs := ValidateCSINodeUpdate(&csiNode, &old); len(errs) == 0 {
			t.Errorf("Expected failure for test: %+v", csiNode)
		}
	}

	// 开关打开后，此前非法的可分配卷数变更应通过校验。
	for _, csiNode := range successCases {
		if errs := ValidateCSINodeUpdate(&csiNode, &old); len(errs) != 0 {
			t.Errorf("expected success with feature gate enabled: %+v", errs)
		}
	}
}

// TestCSIDriverValidation 验证 CSIDriver 创建校验：驱动名格式、必填布尔字段、卷生命周期模式、
// FSGroupPolicy、SELinuxMount 以及 ServiceAccountToken 相关字段的约束。
func TestCSIDriverValidation(t *testing.T) {
	// 本测试默认开启相关特性开关；更细的开关组合见 TestMutableCSINodeAllocatableCountEnabledDisabled。
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.MutableCSINodeAllocatableCount, true)
	// 本测试默认开启相关特性开关；更细的开关组合见 TestCSIDriverValidationPreventPodSchedulingIfMissingEnabledDisabled。
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.VolumeLimitScaling, true)

	driverName := "test-driver"
	longName := "my-a-b-c-d-c-f-g-h-i-j-k-l-m-n-o-p-q-r-s-t-u-v-w-x-y-z-ABCDEFGHIJKLMNOPQRSTUVWXYZ-driver"
	invalidName := "-invalid-@#$%^&*()-"
	attachRequired := true
	attachNotRequired := false
	podInfoOnMount := true
	notPodInfoOnMount := false
	notRequiresRepublish := false
	storageCapacity := true
	notStorageCapacity := false
	seLinuxMount := true
	notSELinuxMount := false
	serviceAccountTokenInSecrets := true
	notServiceAccountTokenInSecrets := false
	preventPodSchedulingIfMissing := false
	supportedFSGroupPolicy := storage.FileFSGroupPolicy
	invalidFSGroupPolicy := storage.FSGroupPolicy("invalid-mode")
	validNodeAllocatableUpdatePeriodSeconds := int64(10)
	invalidNodeAllocatableUpdatePeriodSeconds := int64(9)
	tokenRequests := []storage.TokenRequest{{Audience: "test-audience"}}
	// 以下是合法用例。
	successCases := []storage.CSIDriver{{
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                &attachRequired,
			PodInfoOnMount:                &podInfoOnMount,
			RequiresRepublish:             &notRequiresRepublish,
			StorageCapacity:               &storageCapacity,
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
		},
	}, {
		// 驱动名仅含点号：合法。
		ObjectMeta: metav1.ObjectMeta{Name: "io.kubernetes.storage.csi.driver"},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                &attachRequired,
			PodInfoOnMount:                &podInfoOnMount,
			RequiresRepublish:             &notRequiresRepublish,
			StorageCapacity:               &notStorageCapacity,
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
		},
	}, {
		// 驱动名仅含连字符：合法。
		ObjectMeta: metav1.ObjectMeta{Name: "io-kubernetes-storage-csi-driver"},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                &attachRequired,
			PodInfoOnMount:                &notPodInfoOnMount,
			RequiresRepublish:             &notRequiresRepublish,
			StorageCapacity:               &storageCapacity,
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
		},
	}, {
		// 驱动名含数字：合法。
		ObjectMeta: metav1.ObjectMeta{Name: "1csi2driver3"},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                &attachRequired,
			PodInfoOnMount:                &podInfoOnMount,
			RequiresRepublish:             &notRequiresRepublish,
			StorageCapacity:               &storageCapacity,
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
		},
	}, {
		// 驱动名含点与连字符：合法。
		ObjectMeta: metav1.ObjectMeta{Name: "io.kubernetes.storage.csi-driver"},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                &attachRequired,
			PodInfoOnMount:                &podInfoOnMount,
			RequiresRepublish:             &notRequiresRepublish,
			StorageCapacity:               &storageCapacity,
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
		},
	}, {
		// PodInfoOnMount 取 false：合法。
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                &attachRequired,
			PodInfoOnMount:                &notPodInfoOnMount,
			RequiresRepublish:             &notRequiresRepublish,
			StorageCapacity:               &storageCapacity,
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
		},
	}, {
		// PodInfoOnMount 取 true：合法。
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                &attachRequired,
			PodInfoOnMount:                &podInfoOnMount,
			RequiresRepublish:             &notRequiresRepublish,
			StorageCapacity:               &storageCapacity,
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
		},
	}, {
		// AttachRequired 取 false：合法。
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                &attachNotRequired,
			PodInfoOnMount:                &notPodInfoOnMount,
			RequiresRepublish:             &notRequiresRepublish,
			StorageCapacity:               &storageCapacity,
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
		},
	}, {
		// 生命周期模式仅包含 Persistent：合法。
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:    &attachNotRequired,
			PodInfoOnMount:    &notPodInfoOnMount,
			RequiresRepublish: &notRequiresRepublish,
			StorageCapacity:   &storageCapacity,
			VolumeLifecycleModes: []storage.VolumeLifecycleMode{
				storage.VolumeLifecyclePersistent,
			},
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
		},
	}, {
		// 生命周期模式仅包含 Ephemeral：合法。
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:    &attachNotRequired,
			PodInfoOnMount:    &notPodInfoOnMount,
			RequiresRepublish: &notRequiresRepublish,
			StorageCapacity:   &storageCapacity,
			VolumeLifecycleModes: []storage.VolumeLifecycleMode{
				storage.VolumeLifecycleEphemeral,
			},
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
		},
	}, {
		// 同时声明 Persistent 与 Ephemeral：合法。
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:    &attachNotRequired,
			PodInfoOnMount:    &notPodInfoOnMount,
			RequiresRepublish: &notRequiresRepublish,
			StorageCapacity:   &storageCapacity,
			VolumeLifecycleModes: []storage.VolumeLifecycleMode{
				storage.VolumeLifecycleEphemeral,
				storage.VolumeLifecyclePersistent,
			},
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
		},
	}, {
		// 生命周期模式列表含重复项：合法（仅校验取值是否受支持）。
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:    &attachNotRequired,
			PodInfoOnMount:    &notPodInfoOnMount,
			RequiresRepublish: &notRequiresRepublish,
			StorageCapacity:   &storageCapacity,
			VolumeLifecycleModes: []storage.VolumeLifecycleMode{
				storage.VolumeLifecycleEphemeral,
				storage.VolumeLifecyclePersistent,
				storage.VolumeLifecycleEphemeral,
			},
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
		},
	}, {
		// 指定受支持的 FSGroupPolicy：合法。
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                &attachNotRequired,
			PodInfoOnMount:                &notPodInfoOnMount,
			RequiresRepublish:             &notRequiresRepublish,
			StorageCapacity:               &storageCapacity,
			FSGroupPolicy:                 &supportedFSGroupPolicy,
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
		},
	}, {
		// SELinuxMount 为 false：合法。
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                &attachNotRequired,
			PodInfoOnMount:                &notPodInfoOnMount,
			RequiresRepublish:             &notRequiresRepublish,
			StorageCapacity:               &storageCapacity,
			SELinuxMount:                  &notSELinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
		},
	}, {
		// NodeAllocatableUpdatePeriodSeconds 为 nil（未设置）：合法。
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                     &attachNotRequired,
			PodInfoOnMount:                     &notPodInfoOnMount,
			RequiresRepublish:                  &notRequiresRepublish,
			StorageCapacity:                    &storageCapacity,
			SELinuxMount:                       &seLinuxMount,
			PreventPodSchedulingIfMissing:      &preventPodSchedulingIfMissing,
			NodeAllocatableUpdatePeriodSeconds: nil,
		},
	}, {
		// NodeAllocatableUpdatePeriodSeconds 为合法值（10 秒）。
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                     &attachNotRequired,
			PodInfoOnMount:                     &notPodInfoOnMount,
			RequiresRepublish:                  &notRequiresRepublish,
			StorageCapacity:                    &storageCapacity,
			SELinuxMount:                       &seLinuxMount,
			PreventPodSchedulingIfMissing:      &preventPodSchedulingIfMissing,
			NodeAllocatableUpdatePeriodSeconds: &validNodeAllocatableUpdatePeriodSeconds,
		},
	}, {
		// ServiceAccountTokenInSecrets 为 true 且配置 TokenRequests：合法。
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                &attachNotRequired,
			PodInfoOnMount:                &notPodInfoOnMount,
			RequiresRepublish:             &notRequiresRepublish,
			StorageCapacity:               &storageCapacity,
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
			ServiceAccountTokenInSecrets:  &serviceAccountTokenInSecrets,
			TokenRequests:                 tokenRequests,
		},
	}, {
		// ServiceAccountTokenInSecrets 为 false 且配置 TokenRequests：合法。
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                &attachNotRequired,
			PodInfoOnMount:                &notPodInfoOnMount,
			RequiresRepublish:             &notRequiresRepublish,
			StorageCapacity:               &storageCapacity,
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
			ServiceAccountTokenInSecrets:  &notServiceAccountTokenInSecrets,
			TokenRequests:                 tokenRequests,
		},
	}, {
		// ServiceAccountTokenInSecrets 为 nil（未设置）：合法。
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                &attachNotRequired,
			PodInfoOnMount:                &notPodInfoOnMount,
			RequiresRepublish:             &notRequiresRepublish,
			StorageCapacity:               &storageCapacity,
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
			ServiceAccountTokenInSecrets:  nil,
		},
	}}

	for _, csiDriver := range successCases {
		if errs := ValidateCSIDriver(&csiDriver); len(errs) != 0 {
			t.Errorf("expected success: %v", errs)
		}
	}
	// 以下是非法用例。
	errorCases := []storage.CSIDriver{{
		// 驱动名含非法字符：非法。
		ObjectMeta: metav1.ObjectMeta{Name: invalidName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                &attachRequired,
			PodInfoOnMount:                &podInfoOnMount,
			StorageCapacity:               &storageCapacity,
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
		},
	}, {
		// 驱动名超过长度上限：非法。
		ObjectMeta: metav1.ObjectMeta{Name: longName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                &attachNotRequired,
			PodInfoOnMount:                &notPodInfoOnMount,
			StorageCapacity:               &storageCapacity,
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
		},
	}, {
		// 未设置 AttachRequired：非法（必填字段）。
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                nil,
			PodInfoOnMount:                &podInfoOnMount,
			StorageCapacity:               &storageCapacity,
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
		},
	}, {
		// 未设置 PodInfoOnMount：非法（必填字段）。
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                &attachNotRequired,
			PodInfoOnMount:                nil,
			StorageCapacity:               &storageCapacity,
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
		},
	}, {
		// 未设置 StorageCapacity：非法（必填字段）。
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                &attachNotRequired,
			PodInfoOnMount:                &podInfoOnMount,
			StorageCapacity:               nil,
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
		},
	}, {
		// 未知的卷生命周期模式：非法。
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:  &attachNotRequired,
			PodInfoOnMount:  &notPodInfoOnMount,
			StorageCapacity: &storageCapacity,
			VolumeLifecycleModes: []storage.VolumeLifecycleMode{
				"no-such-mode",
			},
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
		},
	}, {
		// 非法的 FSGroupPolicy 取值：非法。
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                &attachNotRequired,
			PodInfoOnMount:                &notPodInfoOnMount,
			FSGroupPolicy:                 &invalidFSGroupPolicy,
			StorageCapacity:               &storageCapacity,
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
		},
	}, {
		// 未设置 SELinuxMount：非法（必填字段）。
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:  &attachNotRequired,
			PodInfoOnMount:  &notPodInfoOnMount,
			StorageCapacity: &storageCapacity,
		},
	}, {
		// NodeAllocatableUpdatePeriodSeconds 小于 10：非法。
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                     &attachNotRequired,
			PodInfoOnMount:                     &notPodInfoOnMount,
			StorageCapacity:                    &storageCapacity,
			SELinuxMount:                       &seLinuxMount,
			PreventPodSchedulingIfMissing:      &preventPodSchedulingIfMissing,
			NodeAllocatableUpdatePeriodSeconds: &invalidNodeAllocatableUpdatePeriodSeconds,
		},
	}, {
		// 设置了 ServiceAccountTokenInSecrets 却未提供 TokenRequests：非法。
		ObjectMeta: metav1.ObjectMeta{Name: driverName},
		Spec: storage.CSIDriverSpec{
			AttachRequired:                &attachNotRequired,
			PodInfoOnMount:                &notPodInfoOnMount,
			StorageCapacity:               &storageCapacity,
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
			ServiceAccountTokenInSecrets:  &serviceAccountTokenInSecrets,
		},
	}}

	for _, csiDriver := range errorCases {
		if errs := ValidateCSIDriver(&csiDriver); len(errs) == 0 {
			t.Errorf("Expected failure for test: %v", csiDriver)
		}
	}
}

// TestCSIDriverValidationUpdate 验证 CSIDriver 更新校验：多数字段可变，但名称、生命周期模式等字段
// 以及特性开关相关的必填约束仍需校验。
func TestCSIDriverValidationUpdate(t *testing.T) {
	// 本测试默认开启相关特性开关；更细的开关组合见 TestMutableCSINodeAllocatableCountEnabledDisabled。
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.MutableCSINodeAllocatableCount, true)
	// 本测试默认开启相关特性开关；更细的开关组合见 TestCSIDriverValidationPreventPodSchedulingIfMissingEnabledDisabled。
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.VolumeLimitScaling, true)

	driverName := "test-driver"
	longName := "my-a-b-c-d-c-f-g-h-i-j-k-l-m-n-o-p-q-r-s-t-u-v-w-x-y-z-ABCDEFGHIJKLMNOPQRSTUVWXYZ-driver"
	invalidName := "-invalid-@#$%^&*()-"
	attachRequired := true
	attachNotRequired := false
	podInfoOnMount := true
	storageCapacity := true
	notPodInfoOnMount := false
	gcp := "gcp"
	requiresRepublish := true
	notRequiresRepublish := false
	notStorageCapacity := false
	seLinuxMount := true
	notSELinuxMount := false
	serviceAccountTokenInSecrets := true
	notServiceAccountTokenInSecrets := false
	preventPodSchedulingIfMissing := false
	validNodeAllocatableUpdatePeriodSeconds := int64(10)
	invalidNodeAllocatableUpdatePeriodSeconds := int64(9)
	tokenRequests := []storage.TokenRequest{{Audience: "test-audience"}}

	// 基准对象。
	old := storage.CSIDriver{
		ObjectMeta: metav1.ObjectMeta{Name: driverName, ResourceVersion: "1"},
		Spec: storage.CSIDriverSpec{
			AttachRequired:    &attachNotRequired,
			PodInfoOnMount:    &notPodInfoOnMount,
			RequiresRepublish: &notRequiresRepublish,
			VolumeLifecycleModes: []storage.VolumeLifecycleMode{
				storage.VolumeLifecycleEphemeral,
				storage.VolumeLifecyclePersistent,
			},
			StorageCapacity:               &storageCapacity,
			SELinuxMount:                  &seLinuxMount,
			PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
		},
	}

	// 合法更新用例：modify 函数基于旧对象构造新对象。
	successCases := []struct {
		name   string
		modify func(new *storage.CSIDriver)
	}{{
		// 无任何变更。
		name:   "no change",
		modify: func(new *storage.CSIDriver) {},
	}, {
		// 修改 TokenRequests。
		name: "change TokenRequests",
		modify: func(new *storage.CSIDriver) {
			new.Spec.TokenRequests = []storage.TokenRequest{{Audience: gcp}}
		},
	}, {
		// 修改 RequiresRepublish。
		name: "change RequiresRepublish",
		modify: func(new *storage.CSIDriver) {
			new.Spec.RequiresRepublish = &requiresRepublish
		},
	}, {
		// 修改 StorageCapacity。
		name: "StorageCapacity changed",
		modify: func(new *storage.CSIDriver) {
			new.Spec.StorageCapacity = &notStorageCapacity
		},
	}, {
		// 修改 SELinuxMount。
		name: "SELinuxMount changed",
		modify: func(new *storage.CSIDriver) {
			new.Spec.SELinuxMount = &notSELinuxMount
		},
	}, {
		// 修改 PodInfoOnMount。
		name: "change PodInfoOnMount",
		modify: func(new *storage.CSIDriver) {
			new.Spec.PodInfoOnMount = &podInfoOnMount
		},
	}, {
		// 修改 FSGroupPolicy。
		name: "change FSGroupPolicy",
		modify: func(new *storage.CSIDriver) {
			fileFSGroupPolicy := storage.FileFSGroupPolicy
			new.Spec.FSGroupPolicy = &fileFSGroupPolicy
		},
	}, {
		// 把 NodeAllocatableUpdatePeriodSeconds 从 nil 改为合法值。
		name: "Update NodeAllocatableUpdatePeriodSeconds from nil to valid value",
		modify: func(new *storage.CSIDriver) {
			new.Spec.NodeAllocatableUpdatePeriodSeconds = &validNodeAllocatableUpdatePeriodSeconds
		},
	}, {
		// 把 ServiceAccountTokenInSecrets 从 nil 改为 true 并配置 TokenRequests。
		name: "change ServiceAccountTokenInSecrets from nil to true with TokenRequests",
		modify: func(new *storage.CSIDriver) {
			new.Spec.ServiceAccountTokenInSecrets = &serviceAccountTokenInSecrets
			new.Spec.TokenRequests = tokenRequests
		},
	}, {
		// 把 ServiceAccountTokenInSecrets 从 nil 改为 false 并配置 TokenRequests。
		name: "change ServiceAccountTokenInSecrets from nil to false with TokenRequests",
		modify: func(new *storage.CSIDriver) {
			new.Spec.ServiceAccountTokenInSecrets = &notServiceAccountTokenInSecrets
			new.Spec.TokenRequests = tokenRequests
		},
	}, {
		// 在 ServiceAccountTokenInSecrets 的 true/false 之间切换。
		name: "change ServiceAccountTokenInSecrets from true to false",
		modify: func(new *storage.CSIDriver) {
			new.Spec.ServiceAccountTokenInSecrets = &serviceAccountTokenInSecrets
			new.Spec.TokenRequests = tokenRequests
			old := new.DeepCopy()
			old.Spec.ServiceAccountTokenInSecrets = &notServiceAccountTokenInSecrets
		},
	}}

	for _, test := range successCases {
		t.Run(test.name, func(t *testing.T) {
			new := old.DeepCopy()
			test.modify(new)
			if errs := ValidateCSIDriverUpdate(new, &old); len(errs) != 0 {
				t.Errorf("Expected success for %+v: %v", new, errs)
			}
		})
	}

	// 非法更新用例。
	errorCases := []struct {
		name   string
		modify func(new *storage.CSIDriver)
	}{{
		// 驱动名非法。
		name: "invalid name",
		modify: func(new *storage.CSIDriver) {
			new.Name = invalidName
		},
	}, {
		// 驱动名超长。
		name: "long name",
		modify: func(new *storage.CSIDriver) {
			new.Name = longName
		},
	}, {
		// 未设置 AttachRequired。
		name: "AttachRequired not set",
		modify: func(new *storage.CSIDriver) {
			new.Spec.AttachRequired = nil
		},
	}, {
		// 修改 AttachRequired（不可变字段）。
		name: "AttachRequired changed",
		modify: func(new *storage.CSIDriver) {
			new.Spec.AttachRequired = &attachRequired
		},
	}, {
		// 未设置 PodInfoOnMount。
		name: "PodInfoOnMount not set",
		modify: func(new *storage.CSIDriver) {
			new.Spec.PodInfoOnMount = nil
		},
	}, {
		// 卷生命周期模式取值非法。
		name: "invalid volume lifecycle mode",
		modify: func(new *storage.CSIDriver) {
			new.Spec.VolumeLifecycleModes = []storage.VolumeLifecycleMode{
				"no-such-mode",
			}
		},
	}, {
		// 未设置卷生命周期模式。
		name: "volume lifecycle modes not set",
		modify: func(new *storage.CSIDriver) {
			new.Spec.VolumeLifecycleModes = nil
		},
	}, {
		// 移除 Persistent 模式。
		name: "VolumeLifecyclePersistent removed",
		modify: func(new *storage.CSIDriver) {
			new.Spec.VolumeLifecycleModes = []storage.VolumeLifecycleMode{
				storage.VolumeLifecycleEphemeral,
			}
		},
	}, {
		// 移除 Ephemeral 模式。
		name: "VolumeLifecycleEphemeral removed",
		modify: func(new *storage.CSIDriver) {
			new.Spec.VolumeLifecycleModes = []storage.VolumeLifecycleMode{
				storage.VolumeLifecyclePersistent,
			}
		},
	}, {
		// FSGroupPolicy 取值非法。
		name: "FSGroupPolicy invalidated",
		modify: func(new *storage.CSIDriver) {
			invalidFSGroupPolicy := storage.FSGroupPolicy("invalid")
			new.Spec.FSGroupPolicy = &invalidFSGroupPolicy
		},
	}, {
		// TokenRequests 中存在重复的 audience。
		name: "TokenRequests invalidated",
		modify: func(new *storage.CSIDriver) {
			new.Spec.TokenRequests = []storage.TokenRequest{{Audience: gcp}, {Audience: gcp}}
		},
	}, {
		// 将 StorageCapacity 置为 nil。
		name: "invalid nil StorageCapacity",
		modify: func(new *storage.CSIDriver) {
			new.Spec.StorageCapacity = nil
		},
	}, {
		// 未设置 SELinuxMount。
		name: "SELinuxMount not set",
		modify: func(new *storage.CSIDriver) {
			new.Spec.SELinuxMount = nil
		},
	}, {
		// 把 NodeAllocatableUpdatePeriodSeconds 改为非法值。
		name: "Update NodeAllocatableUpdatePeriodSeconds to invalid value",
		modify: func(new *storage.CSIDriver) {
			new.Spec.NodeAllocatableUpdatePeriodSeconds = &invalidNodeAllocatableUpdatePeriodSeconds
		},
	}, {
		// 设置 ServiceAccountTokenInSecrets 却未配置 TokenRequests。
		name: "ServiceAccountTokenInSecrets set without TokenRequests",
		modify: func(new *storage.CSIDriver) {
			new.Spec.ServiceAccountTokenInSecrets = &serviceAccountTokenInSecrets
		},
	}}

	for _, test := range errorCases {
		t.Run(test.name, func(t *testing.T) {
			new := old.DeepCopy()
			test.modify(new)
			if errs := ValidateCSIDriverUpdate(new, &old); len(errs) == 0 {
				t.Errorf("Expected failure for test: %+v", new)
			}
		})
	}
}

// TestCSIDriverStorageCapacityEnablement 验证 StorageCapacity 字段必须显式设置：省略该字段会校验失败。
func TestCSIDriverStorageCapacityEnablement(t *testing.T) {
	run := func(t *testing.T, withField bool) {
		driverName := "test-driver"
		attachRequired := true
		podInfoOnMount := true
		requiresRepublish := true
		storageCapacity := true
		seLinuxMount := false
		preventPodSchedulingIfMissing := false
		csiDriver := storage.CSIDriver{
			ObjectMeta: metav1.ObjectMeta{Name: driverName},
			Spec: storage.CSIDriverSpec{
				AttachRequired:                &attachRequired,
				PodInfoOnMount:                &podInfoOnMount,
				RequiresRepublish:             &requiresRepublish,
				SELinuxMount:                  &seLinuxMount,
				PreventPodSchedulingIfMissing: &preventPodSchedulingIfMissing,
			},
		}
		// 按需设置 StorageCapacity 字段。
		if withField {
			csiDriver.Spec.StorageCapacity = &storageCapacity
		}
		// 期望结果应与该字段是否设置保持一致。
		errs := ValidateCSIDriver(&csiDriver)
		success := withField
		if success && len(errs) != 0 {
			t.Errorf("expected success, got: %v", errs)
		}
		if !success && len(errs) == 0 {
			t.Errorf("expected error, got success")
		}
	}

	// 分别覆盖 true / false 两种情形。
	yesNo := []bool{true, false}
	for _, withField := range yesNo {
		t.Run(fmt.Sprintf("with-field=%v", withField), func(t *testing.T) {
			run(t, withField)
		})
	}
}

// TestValidateCSIStorageCapacity 验证 CSIStorageCapacity 校验：存储类名、容量取值以及节点拓扑选择器的合法性。
func TestValidateCSIStorageCapacity(t *testing.T) {
	storageClassName := "test-sc"
	invalidName := "-invalid-@#$%^&*()-"

	goodCapacity := storage.CSIStorageCapacity{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "csc-329803da-fdd2-42e4-af6f-7b07e7ccc305",
			Namespace: metav1.NamespaceDefault,
		},
		StorageClassName: storageClassName,
	}
	goodTopology := metav1.LabelSelector{
		MatchLabels: map[string]string{"foo": "bar"},
	}

	// 各场景通过闭包基于 goodCapacity 构造变体，避免用例之间互相污染。
	scenarios := map[string]struct {
		isExpectedFailure bool
		capacity          *storage.CSIStorageCapacity
	}{
		// 合法对象。
		"good-capacity": {
			capacity: &goodCapacity,
		},
		// 缺少存储类名：非法。
		"missing-storage-class-name": {
			isExpectedFailure: true,
			capacity: func() *storage.CSIStorageCapacity {
				capacity := goodCapacity
				capacity.StorageClassName = ""
				return &capacity
			}(),
		},
		// 存储类名含非法字符：非法。
		"bad-storage-class-name": {
			isExpectedFailure: true,
			capacity: func() *storage.CSIStorageCapacity {
				capacity := goodCapacity
				capacity.StorageClassName = invalidName
				return &capacity
			}(),
		},
		// 容量为合法的正数。
		"good-capacity-value": {
			capacity: func() *storage.CSIStorageCapacity {
				capacity := goodCapacity
				capacity.Capacity = resource.NewQuantity(1, resource.BinarySI)
				return &capacity
			}(),
		},
		// 容量为负数：非法。
		"bad-capacity-value": {
			isExpectedFailure: true,
			capacity: func() *storage.CSIStorageCapacity {
				capacity := goodCapacity
				capacity.Capacity = resource.NewQuantity(-11, resource.BinarySI)
				return &capacity
			}(),
		},
		// 合法的节点拓扑选择器。
		"good-topology": {
			capacity: func() *storage.CSIStorageCapacity {
				capacity := goodCapacity
				capacity.NodeTopology = &goodTopology
				return &capacity
			}(),
		},
		// 空的拓扑选择器是允许的（表示不限制拓扑）。
		"empty-topology": {
			capacity: func() *storage.CSIStorageCapacity {
				capacity := goodCapacity
				capacity.NodeTopology = &metav1.LabelSelector{}
				return &capacity
			}(),
		},
		// 拓扑选择器中包含未知操作符：非法。
		"bad-topology-fields": {
			isExpectedFailure: true,
			capacity: func() *storage.CSIStorageCapacity {
				capacity := goodCapacity
				capacity.NodeTopology = &metav1.LabelSelector{
					MatchExpressions: []metav1.LabelSelectorRequirement{{
						Key:      "foo",
						Operator: metav1.LabelSelectorOperator("no-such-operator"),
						Values: []string{
							"bar",
						},
					}},
				}
				return &capacity
			}(),
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			errs := ValidateCSIStorageCapacity(scenario.capacity, CSIStorageCapacityValidateOptions{false})
			if len(errs) == 0 && scenario.isExpectedFailure {
				t.Errorf("Unexpected success")
			}
			if len(errs) > 0 && !scenario.isExpectedFailure {
				t.Errorf("Unexpected failure: %+v", errs)
			}
		})
	}

}

// TestCSIServiceAccountToken 验证 CSIDriver 中 TokenRequests 的校验：audience 不可重复，
// 过期时间需落在合法区间内。
func TestCSIServiceAccountToken(t *testing.T) {
	driverName := "test-driver"
	gcp := "gcp"
	aws := "aws"
	notRequiresRepublish := false
	tests := []struct {
		desc      string
		csiDriver *storage.CSIDriver
		wantErr   bool
	}{{
		// audience 重复：非法。
		desc: "invalid - TokenRequests has tokens with the same audience",
		csiDriver: &storage.CSIDriver{
			ObjectMeta: metav1.ObjectMeta{Name: driverName},
			Spec: storage.CSIDriverSpec{
				TokenRequests:     []storage.TokenRequest{{Audience: gcp}, {Audience: gcp}},
				RequiresRepublish: &notRequiresRepublish,
			},
		},
		wantErr: true,
	}, {
		// 过期时间小于 10 分钟：非法。
		desc: "invalid - TokenRequests has tokens with ExpirationSeconds less than 10min",
		csiDriver: &storage.CSIDriver{
			ObjectMeta: metav1.ObjectMeta{Name: driverName},
			Spec: storage.CSIDriverSpec{
				TokenRequests:     []storage.TokenRequest{{Audience: gcp, ExpirationSeconds: ptr.To[int64](10)}},
				RequiresRepublish: &notRequiresRepublish,
			},
		},
		wantErr: true,
	}, {
		// 过期时间超过上限：非法。
		desc: "invalid - TokenRequests has tokens with ExpirationSeconds longer than 1<<32 min",
		csiDriver: &storage.CSIDriver{
			ObjectMeta: metav1.ObjectMeta{Name: driverName},
			Spec: storage.CSIDriverSpec{
				TokenRequests:     []storage.TokenRequest{{Audience: gcp, ExpirationSeconds: ptr.To[int64](1<<32 + 1)}},
				RequiresRepublish: &notRequiresRepublish,
			},
		},
		wantErr: true,
	}, {
		// 最多一个空 audience：合法。
		desc: "valid - TokenRequests has at most one token with empty string audience",
		csiDriver: &storage.CSIDriver{
			ObjectMeta: metav1.ObjectMeta{Name: driverName},
			Spec: storage.CSIDriverSpec{
				TokenRequests:     []storage.TokenRequest{{Audience: ""}},
				RequiresRepublish: &notRequiresRepublish,
			},
		},
	}, {
		// 各 audience 互不相同：合法。
		desc: "valid - TokenRequests has tokens with different audience",
		csiDriver: &storage.CSIDriver{
			ObjectMeta: metav1.ObjectMeta{Name: driverName},
			Spec: storage.CSIDriverSpec{
				TokenRequests:     []storage.TokenRequest{{}, {Audience: gcp}, {Audience: aws}},
				RequiresRepublish: &notRequiresRepublish,
			},
		},
	}}

	for _, test := range tests {
		// 补齐其他必填布尔字段，让校验只聚焦于 TokenRequests。
		test.csiDriver.Spec.AttachRequired = new(bool)
		test.csiDriver.Spec.PodInfoOnMount = new(bool)
		test.csiDriver.Spec.StorageCapacity = new(bool)
		test.csiDriver.Spec.SELinuxMount = new(bool)
		test.csiDriver.Spec.PreventPodSchedulingIfMissing = new(bool)
		if errs := ValidateCSIDriver(test.csiDriver); test.wantErr != (len(errs) != 0) {
			t.Errorf("ValidateCSIDriver = %v, want err: %v", errs, test.wantErr)
		}
	}
}

// TestCSIDriverValidationSELinuxMountEnabledDisabled 验证 SELinuxMount 字段在特性开关开/关下的必填性
// 以及更新规则。
func TestCSIDriverValidationSELinuxMountEnabledDisabled(t *testing.T) {
	tests := []struct {
		name              string
		featureEnabled    bool
		seLinuxMountValue *bool
		expectError       bool
	}{{
		// 开关开启时该字段必填，nil 会报错。
		name:              "feature enabled, nil value",
		featureEnabled:    true,
		seLinuxMountValue: nil,
		expectError:       true,
	}, {
		// 开关开启且字段已设置：合法。
		name:              "feature enabled, non-nil value",
		featureEnabled:    true,
		seLinuxMountValue: new(true),
		expectError:       false,
	}, {
		// 开关关闭时可省略该字段：合法。
		name:              "feature disabled, nil value",
		featureEnabled:    false,
		seLinuxMountValue: nil,
		expectError:       false,
	}, {
		// 开关关闭时设置该字段也不报错。
		name:              "feature disabled, non-nil value",
		featureEnabled:    false,
		seLinuxMountValue: new(true),
		expectError:       false,
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 关闭开关时先回退模拟版本，否则特性默认处于开启状态。
			if !test.featureEnabled {
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.35"))
			}
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SELinuxMountReadWriteOncePod, test.featureEnabled)
			csiDriver := &storage.CSIDriver{
				ObjectMeta: metav1.ObjectMeta{Name: "foo"},
				Spec: storage.CSIDriverSpec{
					AttachRequired:                new(true),
					PodInfoOnMount:                new(true),
					RequiresRepublish:             new(true),
					StorageCapacity:               new(true),
					SELinuxMount:                  test.seLinuxMountValue,
					PreventPodSchedulingIfMissing: new(false),
				},
			}
			err := ValidateCSIDriver(csiDriver)
			if test.expectError && err == nil {
				t.Error("Expected validation error, got nil")
			}
			if !test.expectError && err != nil {
				t.Errorf("Validation returned error: %s", err)
			}
		})
	}

	updateTests := []struct {
		name           string
		featureEnabled bool
		oldValue       *bool
		newValue       *bool
		expectError    bool
	}{{
		// 开关开启时保持 nil，仍会因字段缺失而失败。
		name:           "feature enabled, nil->nil",
		featureEnabled: true,
		oldValue:       nil,
		newValue:       nil,
		expectError:    true, // populated by defaulting and required when feature is enabled
	}, {
		// 开关开启时设置该字段：合法。
		name:           "feature enabled, nil->set",
		featureEnabled: true,
		oldValue:       nil,
		newValue:       new(true),
		expectError:    false,
	}, {
		// 开关开启时保持已设置：合法。
		name:           "feature enabled, set->set",
		featureEnabled: true,
		oldValue:       new(true),
		newValue:       new(true),
		expectError:    false,
	}, {
		// 开关开启时清空必填字段：非法。
		name:           "feature enabled, set->nil",
		featureEnabled: true,
		oldValue:       new(true),
		newValue:       nil,
		expectError:    true, // populated by defaulting and required when feature is enabled
	}, {
		// 开关关闭时保持 nil：合法。
		name:           "feature disabled, nil->nil",
		featureEnabled: false,
		oldValue:       nil,
		newValue:       nil,
		expectError:    false,
	}, {
		// 开关关闭时设置该字段：合法。
		name:           "feature disabled, nil->set",
		featureEnabled: false,
		oldValue:       nil,
		newValue:       new(true),
		expectError:    false,
	}, {
		// 开关关闭时保持已设置：合法。
		name:           "feature disabled, set->set",
		featureEnabled: false,
		oldValue:       new(true),
		newValue:       new(true),
		expectError:    false,
	}, {
		// 开关关闭时清空该字段：合法。
		name:           "feature disabled, set->nil",
		featureEnabled: false,
		oldValue:       new(true),
		newValue:       nil,
		expectError:    false,
	}}
	for _, test := range updateTests {
		t.Run(test.name, func(t *testing.T) {
			// 关闭开关时先回退模拟版本，确保按关闭后的规则校验。
			if !test.featureEnabled {
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.35"))
			}
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SELinuxMountReadWriteOncePod, test.featureEnabled)
			oldCSIDriver := &storage.CSIDriver{
				ObjectMeta: metav1.ObjectMeta{Name: "foo", ResourceVersion: "1"},
				Spec: storage.CSIDriverSpec{
					AttachRequired:                new(true),
					PodInfoOnMount:                new(true),
					RequiresRepublish:             new(true),
					StorageCapacity:               new(true),
					SELinuxMount:                  test.oldValue,
					PreventPodSchedulingIfMissing: new(false),
				},
			}
			newCSIDriver := oldCSIDriver.DeepCopy()
			newCSIDriver.Spec.SELinuxMount = test.newValue
			err := ValidateCSIDriverUpdate(newCSIDriver, oldCSIDriver)
			if test.expectError && err == nil {
				t.Error("Expected validation error, got nil")
			}
			if !test.expectError && err != nil {
				t.Errorf("Validation returned error: %s", err)
			}
		})
	}
}

// TestCSIDriverValidationPreventPodSchedulingIfMissingEnabledDisabled 验证 PreventPodSchedulingIfMissing
// 字段在特性开关开/关下的必填性与更新规则。
func TestCSIDriverValidationPreventPodSchedulingIfMissingEnabledDisabled(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SELinuxMountReadWriteOncePod, true)
	tests := []struct {
		name                               string
		featureEnabled                     bool
		preventPodSchedulingIfMissingValue *bool
		expectError                        bool
	}{{
		// 开关开启时该字段必填，nil 会报错。
		name:                               "feature enabled, nil value",
		featureEnabled:                     true,
		preventPodSchedulingIfMissingValue: nil,
		expectError:                        true,
	}, {
		// 开关开启且字段已设置：合法。
		name:                               "feature enabled, non-nil value",
		featureEnabled:                     true,
		preventPodSchedulingIfMissingValue: new(true),
		expectError:                        false,
	}, {
		// 开关关闭时可省略该字段：合法。
		name:                               "feature disabled, nil value",
		featureEnabled:                     false,
		preventPodSchedulingIfMissingValue: nil,
		expectError:                        false,
	}, {
		// 开关关闭时设置该字段也不报错。
		name:                               "feature disabled, non-nil value",
		featureEnabled:                     false,
		preventPodSchedulingIfMissingValue: new(true),
		expectError:                        false,
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.VolumeLimitScaling, test.featureEnabled)
			csiDriver := &storage.CSIDriver{
				ObjectMeta: metav1.ObjectMeta{Name: "foo"},
				Spec: storage.CSIDriverSpec{
					AttachRequired:                new(true),
					PodInfoOnMount:                new(true),
					RequiresRepublish:             new(true),
					StorageCapacity:               new(true),
					SELinuxMount:                  new(true),
					PreventPodSchedulingIfMissing: test.preventPodSchedulingIfMissingValue,
				},
			}
			err := ValidateCSIDriver(csiDriver)
			if test.expectError && err == nil {
				t.Error("Expected validation error, got nil")
			}
			if !test.expectError && err != nil {
				t.Errorf("Validation returned error: %s", err)
			}
		})
	}

	updateTests := []struct {
		name           string
		featureEnabled bool
		oldValue       *bool
		newValue       *bool
		expectError    bool
	}{{
		// 开关开启时保持 nil，仍会因字段缺失而失败。
		name:           "feature enabled, nil->nil",
		featureEnabled: true,
		oldValue:       nil,
		newValue:       nil,
		expectError:    true, // populated by defaulting and required when feature is enabled
	}, {
		// 开关开启时设置该字段：合法。
		name:           "feature enabled, nil->set",
		featureEnabled: true,
		oldValue:       nil,
		newValue:       new(true),
		expectError:    false,
	}, {
		// 开关开启时保持已设置：合法。
		name:           "feature enabled, set->set",
		featureEnabled: true,
		oldValue:       new(true),
		newValue:       new(true),
		expectError:    false,
	}, {
		// 开关开启时清空必填字段：非法。
		name:           "feature enabled, set->nil",
		featureEnabled: true,
		oldValue:       new(true),
		newValue:       nil,
		expectError:    true, // populated by defaulting and required when feature is enabled
	}, {
		// 开关关闭时保持 nil：合法。
		name:           "feature disabled, nil->nil",
		featureEnabled: false,
		oldValue:       nil,
		newValue:       nil,
		expectError:    false,
	}, {
		// 开关关闭时设置该字段：合法。
		name:           "feature disabled, nil->set",
		featureEnabled: false,
		oldValue:       nil,
		newValue:       new(true),
		expectError:    false,
	}, {
		// 开关关闭时保持已设置：合法。
		name:           "feature disabled, set->set",
		featureEnabled: false,
		oldValue:       new(true),
		newValue:       new(true),
		expectError:    false,
	}, {
		// 开关关闭时清空该字段：合法。
		name:           "feature disabled, set->nil",
		featureEnabled: false,
		oldValue:       new(true),
		newValue:       nil,
		expectError:    false,
	}}
	for _, test := range updateTests {
		t.Run(test.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.VolumeLimitScaling, test.featureEnabled)
			oldCSIDriver := &storage.CSIDriver{
				ObjectMeta: metav1.ObjectMeta{Name: "foo", ResourceVersion: "1"},
				Spec: storage.CSIDriverSpec{
					AttachRequired:                new(true),
					PodInfoOnMount:                new(true),
					RequiresRepublish:             new(true),
					StorageCapacity:               new(true),
					SELinuxMount:                  new(true),
					PreventPodSchedulingIfMissing: test.oldValue,
				},
			}
			newCSIDriver := oldCSIDriver.DeepCopy()
			newCSIDriver.Spec.PreventPodSchedulingIfMissing = test.newValue
			err := ValidateCSIDriverUpdate(newCSIDriver, oldCSIDriver)
			if test.expectError && err == nil {
				t.Error("Expected validation error, got nil")
			}
			if !test.expectError && err != nil {
				t.Errorf("Validation returned error: %s", err)
			}
		})
	}
}

// TestValidateVolumeAttributesClass 验证 VolumeAttributesClass 创建校验：驱动名格式、参数 map 必填
// 以及参数总大小上限。
func TestValidateVolumeAttributesClass(t *testing.T) {
	successCases := []storage.VolumeAttributesClass{
		{
			// 驱动名不含斜杠：合法。
			ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			DriverName: "foo",
			Parameters: map[string]string{
				"foo-parameter": "free-form-string",
			},
		},
		{
			// 携带若干参数：合法。
			ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			DriverName: "kubernetes.io/foo",
			Parameters: map[string]string{
				"kubernetes.io/foo-parameter": "free/form/string",
				"foo-parameter":               "free-form-string",
				"foo-parameter2":              "{\"embedded\": \"json\", \"with\": {\"structures\":\"inside\"}}",
				"foo-parameter3":              "",
			},
		}}

	// 合法用例应当通过校验。
	for testName, v := range successCases {
		if errs := ValidateVolumeAttributesClass(&v); len(errs) != 0 {
			t.Errorf("Expected success for %d, got %v", testName, errs)
		}
	}

	// 构造一个总大小超过上限的参数 map，用于触发“参数过大”错误。
	longParameters := make(map[string]string)
	totalSize := 0
	for totalSize < maxProvisionerParameterSize {
		k := fmt.Sprintf("param/%d", totalSize)
		v := fmt.Sprintf("value-%d", totalSize)
		longParameters[k] = v
		totalSize = totalSize + len(k) + len(v)
	}

	// 以下是非法用例。
	errorCases := map[string]storage.VolumeAttributesClass{
		// VolumeAttributesClass 是集群级资源，不允许指定 namespace。
		"namespace is present": {
			ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "bar"},
			DriverName: "kubernetes.io/foo",
			Parameters: map[string]string{
				"foo-parameter": "free-form-string",
			},
		},
		// 驱动名中含多余的斜杠分段：非法。
		"invalid driverName": {
			ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			DriverName: "kubernetes.io/invalid/foo",
			Parameters: map[string]string{
				"foo-parameter": "free-form-string",
			},
		},
		// 驱动名含非法字符：非法。
		"invalid driverName with invalid chars": {
			ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			DriverName: "^/ ",
			Parameters: map[string]string{
				"foo-parameter": "free-form-string",
			},
		},
		// 参数 map 为空：非法（必须至少有一个参数）。
		"empty parameters": {
			ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			DriverName: "kubernetes.io/foo",
			Parameters: map[string]string{},
		},
		// 参数 map 为 nil：非法。
		"nil parameters": {
			ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			DriverName: "kubernetes.io/foo",
		},
		// 参数名为空字符串：非法。
		"invalid empty parameter name": {
			ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			DriverName: "kubernetes.io/foo",
			Parameters: map[string]string{
				"": "value",
			},
		},
		// 驱动名未设置：非法。
		"driverName: Required value": {
			ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			DriverName: "",
			Parameters: map[string]string{
				"foo-parameter": "free-form-string",
			},
		},
		// 驱动名为空白字符：非法。
		"driverName: whitespace": {
			ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			DriverName: " ",
			Parameters: map[string]string{
				"foo-parameter": "free-form-string",
			},
		},
		// 参数总大小超过上限：非法。
		"too long parameters": {
			ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			DriverName: "kubernetes.io/foo",
			Parameters: longParameters,
		},
	}

	// 非法用例应当至少产生一个校验错误。
	for testName, v := range errorCases {
		if errs := ValidateVolumeAttributesClass(&v); len(errs) == 0 {
			t.Errorf("Expected failure for test: %s", testName)
		}
	}
}

// TestValidateVolumeAttributesClassUpdate 验证 VolumeAttributesClass 更新校验：驱动名与参数 map 均不可修改。
func TestValidateVolumeAttributesClassUpdate(t *testing.T) {
	cases := map[string]struct {
		oldClass      *storage.VolumeAttributesClass
		newClass      *storage.VolumeAttributesClass
		shouldSucceed bool
	}{
		// 修改驱动名：非法。
		"invalid driverName update": {
			oldClass: &storage.VolumeAttributesClass{
				DriverName: "kubernetes.io/foo",
			},
			newClass: &storage.VolumeAttributesClass{
				DriverName: "kubernetes.io/bar",
			},
			shouldSucceed: false,
		},
		// 修改已有参数的值：非法。
		"invalid parameter update which changes values": {
			oldClass: &storage.VolumeAttributesClass{
				DriverName: "kubernetes.io/foo",
				Parameters: map[string]string{
					"foo": "bar1",
				},
			},
			newClass: &storage.VolumeAttributesClass{
				DriverName: "kubernetes.io/foo",
				Parameters: map[string]string{
					"foo": "bar2",
				},
			},
			shouldSucceed: false,
		},
		// 新增参数项：非法。
		"invalid parameter update which add new item": {
			oldClass: &storage.VolumeAttributesClass{
				DriverName: "kubernetes.io/foo",
				Parameters: map[string]string{},
			},
			newClass: &storage.VolumeAttributesClass{
				DriverName: "kubernetes.io/foo",
				Parameters: map[string]string{
					"foo": "bar",
				},
			},
			shouldSucceed: false,
		},
		// 删除参数项：非法。
		"invalid parameter update which remove a item": {
			oldClass: &storage.VolumeAttributesClass{
				DriverName: "kubernetes.io/foo",
				Parameters: map[string]string{
					"foo": "bar",
				},
			},
			newClass: &storage.VolumeAttributesClass{
				DriverName: "kubernetes.io/foo",
				Parameters: map[string]string{},
			},
			shouldSucceed: false,
		},
	}

	for testName, testCase := range cases {
		errs := ValidateVolumeAttributesClassUpdate(testCase.newClass, testCase.oldClass)
		if testCase.shouldSucceed && len(errs) != 0 {
			t.Errorf("Expected success for %v, got %v", testName, errs)
		}
		if !testCase.shouldSucceed && len(errs) == 0 {
			t.Errorf("Expected failure for %v, got success", testName)
		}
	}
}

// TestValidateCSINodeStatusUpdate 验证 CSINode status 更新校验：存储健康状态的类型、驱动名、
// 原因格式、重复项与数量上限，以及访问模式/卷模式的取值。
func TestValidateCSINodeStatusUpdate(t *testing.T) {
	// 基准对象：spec 合法且不含 status。
	old := storage.CSINode{
		ObjectMeta: metav1.ObjectMeta{Name: "node1", ResourceVersion: "1"},
		Spec: storage.CSINodeSpec{
			Drivers: []storage.CSINodeDriver{
				{Name: "driver1", NodeID: "node1"},
			},
		},
	}
	// 构造一个仅 status 与旧对象不同的新对象，健康状态由入参指定。
	newNode := func(health ...storage.StorageHealth) storage.CSINode {
		return storage.CSINode{
			ObjectMeta: metav1.ObjectMeta{Name: "node1", ResourceVersion: "2"},
			Spec:       old.Spec,
			Status:     storage.CSINodeStatus{StorageHealth: health},
		}
	}

	// 各场景通过 newNode 构造新对象，只改动 status。
	tests := []struct {
		name        string
		new         storage.CSINode
		isErr       bool
		expectedErr string
	}{
		// StorageUnreachable 是合法的状态类型。
		{
			name: "valid status with StorageUnreachable",
			new: newNode(storage.StorageHealth{Name: "driver1", HealthConditions: []storage.StorageHealthCondition{
				{Status: storage.StorageUnreachable, Reason: "BackendDown"},
			}}),
		},
		// StorageDegraded 是合法的状态类型。
		{
			name: "valid status with StorageDegraded",
			new: newNode(storage.StorageHealth{Name: "driver1", HealthConditions: []storage.StorageHealthCondition{
				{Status: storage.StorageDegraded, Reason: "HighLatency"},
			}}),
		},
		// 空状态列表合法。
		{
			name: "empty status is valid",
			new:  newNode(),
		},
		// 未知的状态类型：非法。
		{
			name: "invalid status type",
			new: newNode(storage.StorageHealth{Name: "driver1", HealthConditions: []storage.StorageHealthCondition{
				{Status: "InvalidType", Reason: "SomeReason"},
			}}),
			isErr:       true,
			expectedErr: "status.storageHealth[0].healthConditions[0].status",
		},
		// 驱动名为空：非法。
		{
			name: "empty driver name",
			new: newNode(storage.StorageHealth{HealthConditions: []storage.StorageHealthCondition{
				{Status: storage.StorageUnreachable, Reason: "BackendDown"},
			}}),
			isErr:       true,
			expectedErr: "status.storageHealth[0].name",
		},
		// 未填写原因（reason）：非法。
		{
			name: "empty reason",
			new: newNode(storage.StorageHealth{Name: "driver1", HealthConditions: []storage.StorageHealthCondition{
				{Status: storage.StorageUnreachable},
			}}),
			isErr:       true,
			expectedErr: "status.storageHealth[0].healthConditions[0].reason",
		},
		// 原因格式非法（包含分号）：非法。
		{
			name: "invalid reason format",
			new: newNode(storage.StorageHealth{Name: "driver1", HealthConditions: []storage.StorageHealthCondition{
				{Status: storage.StorageUnreachable, Reason: "invalid;val"},
			}}),
			isErr:       true,
			expectedErr: "status.storageHealth[0].healthConditions[0].reason",
		},
		// 同一驱动出现多条健康记录：非法。
		{
			name: "duplicate driver",
			new: newNode(
				storage.StorageHealth{Name: "driver1"},
				storage.StorageHealth{Name: "driver1"},
			),
			isErr:       true,
			expectedErr: "status.storageHealth[1]",
		},
		// 同一记录内的重复条件在数量上限内是允许的。
		{
			name: "duplicate conditions are allowed within the bound",
			new: newNode(storage.StorageHealth{Name: "driver1", HealthConditions: []storage.StorageHealthCondition{
				{Status: storage.StorageUnreachable, Reason: "BackendDown"},
				{Status: storage.StorageUnreachable, Reason: "BackendDown"},
			}}),
		},
		// 原因超过长度上限：非法。
		{
			name: "reason too long",
			new: newNode(storage.StorageHealth{Name: "driver1", HealthConditions: []storage.StorageHealthCondition{
				{Status: storage.StorageUnreachable, Reason: strings.Repeat("a", 257)},
			}}),
			isErr:       true,
			expectedErr: "status.storageHealth[0].healthConditions[0].reason",
		},
		// 健康条件数量超过上限：非法。
		{
			name: "too many conditions",
			new: newNode(storage.StorageHealth{
				Name:             "driver1",
				HealthConditions: make([]storage.StorageHealthCondition, maxStorageHealthConditions+1),
			}),
			isErr:       true,
			expectedErr: "status.storageHealth[0].healthConditions",
		},
		// 访问模式取值非法。
		{
			name: "invalid access mode",
			new: newNode(storage.StorageHealth{Name: "driver1", HealthConditions: []storage.StorageHealthCondition{{
				Status: storage.StorageUnreachable, Reason: "BackendDown", AccessMode: ptr.To(api.PersistentVolumeAccessMode("invalid")),
			}}}),
			isErr:       true,
			expectedErr: "status.storageHealth[0].healthConditions[0].accessMode",
		},
		// 卷模式取值非法。
		{
			name: "invalid volume mode",
			new: newNode(storage.StorageHealth{Name: "driver1", HealthConditions: []storage.StorageHealthCondition{{
				Status: storage.StorageUnreachable, Reason: "BackendDown", VolumeMode: ptr.To(api.PersistentVolumeMode("invalid")),
			}}}),
			isErr:       true,
			expectedErr: "status.storageHealth[0].healthConditions[0].volumeMode",
		},
	}

	// 除了判断是否报错，还校验错误字段路径是否指向预期位置。
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := ValidateCSINodeStatusUpdate(&tt.new, &old)
			if tt.isErr && len(errs) == 0 {
				t.Errorf("expected error but got none")
			}
			if !tt.isErr && len(errs) > 0 {
				t.Errorf("unexpected errors: %v", errs)
			}
			if tt.isErr && len(errs) > 0 && tt.expectedErr != "" {
				found := false
				for _, err := range errs {
					if strings.Contains(err.Field, tt.expectedErr) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected error containing %q but got: %v", tt.expectedErr, errs)
				}
			}
		})
	}
}
