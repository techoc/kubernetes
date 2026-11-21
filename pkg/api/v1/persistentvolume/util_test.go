/*
Copyright 2017 The Kubernetes Authors.

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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// TestPVSecrets 验证 VisitPVSecretNames 能覆盖 PV 中所有真正的 Secret 引用：
// 用反射枚举 api.PersistentVolume 里名字含 secret 的字段路径作为基准，
// 确保没有漏掉新字段、也没有把 SecretFile 这类非引用字段算进来；
// 同时检查提取出的（命名空间, 名字）组合，以及空名字不会被收集。
func TestPVSecrets(t *testing.T) {
	// 包含 PV 中所有可能 Secret 引用的桩数据。
	// 被引用 Secret 的名字与反射探测到的结构体路径一致，便于反查漏掉的字段。
	secretNamespace := "Spec.PersistentVolumeSource.AzureFile.SecretNamespace"
	pvs := []*corev1.PersistentVolume{
		// AzureFile 未显式指定 SecretNamespace 时，应回退到 ClaimRef 的命名空间。
		{Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "claimrefns", Name: "claimrefname"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				AzureFile: &corev1.AzureFilePersistentVolumeSource{
					SecretName: "Spec.PersistentVolumeSource.AzureFile.SecretName"}}}},
		// AzureFile 显式指定 SecretNamespace 时，应以该字段为准。
		{Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "claimrefns", Name: "claimrefname"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				AzureFile: &corev1.AzureFilePersistentVolumeSource{
					SecretName:      "Spec.PersistentVolumeSource.AzureFile.SecretName",
					SecretNamespace: &secretNamespace}}}},
		// CephFS 带显式命名空间。
		{Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "claimrefns", Name: "claimrefname"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CephFS: &corev1.CephFSPersistentVolumeSource{
					SecretRef: &corev1.SecretReference{
						Name:      "Spec.PersistentVolumeSource.CephFS.SecretRef",
						Namespace: "cephfs"}}}}},
		// CephFS 未指定命名空间时，应回退到 ClaimRef 的命名空间。
		{Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "claimrefns", Name: "claimrefname"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CephFS: &corev1.CephFSPersistentVolumeSource{
					SecretRef: &corev1.SecretReference{
						Name: "Spec.PersistentVolumeSource.CephFS.SecretRef"}}}}},
		// Cinder 没有 ClaimRef，只能用 SecretRef 自带的命名空间。
		{Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				Cinder: &corev1.CinderPersistentVolumeSource{
					SecretRef: &corev1.SecretReference{
						Name:      "Spec.PersistentVolumeSource.Cinder.SecretRef",
						Namespace: "cinder"}}}}},
		{Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "claimrefns", Name: "claimrefname"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				FlexVolume: &corev1.FlexPersistentVolumeSource{
					SecretRef: &corev1.SecretReference{
						Name:      "Spec.PersistentVolumeSource.FlexVolume.SecretRef",
						Namespace: "flexns"}}}}},
		{Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "claimrefns", Name: "claimrefname"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				FlexVolume: &corev1.FlexPersistentVolumeSource{
					SecretRef: &corev1.SecretReference{
						Name: "Spec.PersistentVolumeSource.FlexVolume.SecretRef"}}}}},
		{Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "claimrefns", Name: "claimrefname"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				RBD: &corev1.RBDPersistentVolumeSource{
					SecretRef: &corev1.SecretReference{
						Name: "Spec.PersistentVolumeSource.RBD.SecretRef"}}}}},
		{Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "claimrefns", Name: "claimrefname"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				RBD: &corev1.RBDPersistentVolumeSource{
					SecretRef: &corev1.SecretReference{
						Name:      "Spec.PersistentVolumeSource.RBD.SecretRef",
						Namespace: "rbdns"}}}}},
		{Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "claimrefns", Name: "claimrefname"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				ScaleIO: &corev1.ScaleIOPersistentVolumeSource{
					SecretRef: &corev1.SecretReference{
						Name: "Spec.PersistentVolumeSource.ScaleIO.SecretRef"}}}}},
		{Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "claimrefns", Name: "claimrefname"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				ScaleIO: &corev1.ScaleIOPersistentVolumeSource{
					SecretRef: &corev1.SecretReference{
						Name:      "Spec.PersistentVolumeSource.ScaleIO.SecretRef",
						Namespace: "scaleions"}}}}},
		{Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "claimrefns", Name: "claimrefname"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				ISCSI: &corev1.ISCSIPersistentVolumeSource{
					SecretRef: &corev1.SecretReference{
						Name:      "Spec.PersistentVolumeSource.ISCSI.SecretRef",
						Namespace: "iscsi"}}}}},
		{Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "claimrefns", Name: "claimrefname"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				ISCSI: &corev1.ISCSIPersistentVolumeSource{
					SecretRef: &corev1.SecretReference{
						Name: "Spec.PersistentVolumeSource.ISCSI.SecretRef"}}}}},
		{Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "claimrefns", Name: "claimrefname"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				StorageOS: &corev1.StorageOSPersistentVolumeSource{
					SecretRef: &corev1.ObjectReference{
						Name:      "Spec.PersistentVolumeSource.StorageOS.SecretRef",
						Namespace: "storageosns"}}}}},
		// CSI 有五个独立的 Secret 引用字段，必须逐个覆盖。
		{Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "claimrefns", Name: "claimrefname"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					ControllerPublishSecretRef: &corev1.SecretReference{
						Name:      "Spec.PersistentVolumeSource.CSI.ControllerPublishSecretRef",
						Namespace: "csi"}}}}},
		{Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "claimrefns", Name: "claimrefname"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					NodePublishSecretRef: &corev1.SecretReference{
						Name:      "Spec.PersistentVolumeSource.CSI.NodePublishSecretRef",
						Namespace: "csi"}}}}},
		{Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "claimrefns", Name: "claimrefname"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					NodeStageSecretRef: &corev1.SecretReference{
						Name:      "Spec.PersistentVolumeSource.CSI.NodeStageSecretRef",
						Namespace: "csi"}}}}},
		{Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "claimrefns", Name: "claimrefname"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					ControllerExpandSecretRef: &corev1.SecretReference{
						Name:      "Spec.PersistentVolumeSource.CSI.ControllerExpandSecretRef",
						Namespace: "csi"}}}}},
		{Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "claimrefns", Name: "claimrefname"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					NodeExpandSecretRef: &corev1.SecretReference{
						Name:      "Spec.PersistentVolumeSource.CSI.NodeExpandSecretRef",
						Namespace: "csi"}}}}},
	}
	extractedNames := sets.New[string]()
	extractedNamesWithNamespace := sets.New[string]()

	for _, pv := range pvs {
		// 返回 true 表示继续遍历，收集所有引用而不在第一个处停止。
		VisitPVSecretNames(pv, func(namespace, name string, kubeletVisible bool) bool {
			extractedNames.Insert(name)
			extractedNamesWithNamespace.Insert(namespace + "/" + name)
			return true
		})
	}

	// excludedSecretPaths 是名字含 "secret" 但并非 Secret API 对象引用的字段路径。
	excludedSecretPaths := sets.New[string](
		"Spec.PersistentVolumeSource.CephFS.SecretFile",
		"Spec.PersistentVolumeSource.AzureFile.SecretNamespace",
	)
	// expectedSecretPaths 是名字含 "secret" 且确实引用 Secret 对象的字段路径。
	// 上面每个路径都应在 PV 桩数据中有对应例子，且 Secret 名字设为该路径本身。
	expectedSecretPaths := sets.New[string](
		"Spec.PersistentVolumeSource.AzureFile.SecretName",
		"Spec.PersistentVolumeSource.CephFS.SecretRef",
		"Spec.PersistentVolumeSource.Cinder.SecretRef",
		"Spec.PersistentVolumeSource.FlexVolume.SecretRef",
		"Spec.PersistentVolumeSource.RBD.SecretRef",
		"Spec.PersistentVolumeSource.ScaleIO.SecretRef",
		"Spec.PersistentVolumeSource.ISCSI.SecretRef",
		"Spec.PersistentVolumeSource.StorageOS.SecretRef",
		"Spec.PersistentVolumeSource.CSI.ControllerPublishSecretRef",
		"Spec.PersistentVolumeSource.CSI.NodePublishSecretRef",
		"Spec.PersistentVolumeSource.CSI.NodeStageSecretRef",
		"Spec.PersistentVolumeSource.CSI.ControllerExpandSecretRef",
		"Spec.PersistentVolumeSource.CSI.NodeExpandSecretRef",
	)
	// 反射枚举内部 API 类型的结构体路径，作为核对 VisitPVSecretNames 覆盖度的独立基准。
	secretPaths := collectSecretPaths(t, nil, "", reflect.TypeOf(&api.PersistentVolume{}))
	secretPaths = secretPaths.Difference(excludedSecretPaths)
	if missingPaths := expectedSecretPaths.Difference(secretPaths); len(missingPaths) > 0 {
		t.Logf("Missing expected secret paths:\n%s", strings.Join(sets.List[string](missingPaths), "\n"))
		t.Error("Missing expected secret paths. Verify VisitPVSecretNames() is correctly finding the missing paths, then correct expectedSecretPaths")
	}
	if extraPaths := secretPaths.Difference(expectedSecretPaths); len(extraPaths) > 0 {
		t.Logf("Extra secret paths:\n%s", strings.Join(sets.List[string](extraPaths), "\n"))
		t.Error("Extra fields with 'secret' in the name found. Verify VisitPVSecretNames() is including these fields if appropriate, then correct expectedSecretPaths")
	}

	// 桩数据中的 Secret 名字应恰好覆盖全部预期路径：新增字段时这里会先失败，
	// 提醒补充桩数据，而不是误以为是 VisitPVSecretNames 漏了字段。
	if missingNames := expectedSecretPaths.Difference(extractedNames); len(missingNames) > 0 {
		t.Logf("Missing expected secret names:\n%s", strings.Join(sets.List[string](missingNames), "\n"))
		t.Error("Missing expected secret names. Verify the PV stub above includes these references, then verify VisitPVSecretNames() is correctly finding the missing names")
	}
	if extraNames := extractedNames.Difference(expectedSecretPaths); len(extraNames) > 0 {
		t.Logf("Extra secret names:\n%s", strings.Join(sets.List(extraNames), "\n"))
		t.Error("Extra secret names extracted. Verify VisitPVSecretNames() is correctly extracting secret names")
	}

	// 命名空间来源有两种：SecretRef 自带的值，或回退到 ClaimRef.Namespace。
	expectedNamespacedNames := sets.New[string](
		"claimrefns/Spec.PersistentVolumeSource.AzureFile.SecretName",
		"Spec.PersistentVolumeSource.AzureFile.SecretNamespace/Spec.PersistentVolumeSource.AzureFile.SecretName",

		"claimrefns/Spec.PersistentVolumeSource.CephFS.SecretRef",
		"cephfs/Spec.PersistentVolumeSource.CephFS.SecretRef",

		"cinder/Spec.PersistentVolumeSource.Cinder.SecretRef",

		"claimrefns/Spec.PersistentVolumeSource.FlexVolume.SecretRef",
		"flexns/Spec.PersistentVolumeSource.FlexVolume.SecretRef",

		"claimrefns/Spec.PersistentVolumeSource.RBD.SecretRef",
		"rbdns/Spec.PersistentVolumeSource.RBD.SecretRef",

		"claimrefns/Spec.PersistentVolumeSource.ScaleIO.SecretRef",
		"scaleions/Spec.PersistentVolumeSource.ScaleIO.SecretRef",

		"claimrefns/Spec.PersistentVolumeSource.ISCSI.SecretRef",
		"iscsi/Spec.PersistentVolumeSource.ISCSI.SecretRef",

		"storageosns/Spec.PersistentVolumeSource.StorageOS.SecretRef",

		"csi/Spec.PersistentVolumeSource.CSI.ControllerPublishSecretRef",
		"csi/Spec.PersistentVolumeSource.CSI.NodePublishSecretRef",
		"csi/Spec.PersistentVolumeSource.CSI.NodeStageSecretRef",
		"csi/Spec.PersistentVolumeSource.CSI.ControllerExpandSecretRef",
		"csi/Spec.PersistentVolumeSource.CSI.NodeExpandSecretRef",
	)
	if missingNames := expectedNamespacedNames.Difference(extractedNamesWithNamespace); len(missingNames) > 0 {
		t.Logf("Missing expected namespaced names:\n%s", strings.Join(sets.List[string](missingNames), "\n"))
		t.Error("Missing expected namespaced names. Verify the PV stub above includes these references, then verify VisitPVSecretNames() is correctly finding the missing names")
	}
	if extraNames := extractedNamesWithNamespace.Difference(expectedNamespacedNames); len(extraNames) > 0 {
		t.Logf("Extra namespaced names:\n%s", strings.Join(sets.List[string](extraNames), "\n"))
		t.Error("Extra namespaced names extracted. Verify VisitPVSecretNames() is correctly extracting secret names")
	}

	// 空 Secret 名字不构成有效引用，回调不应被调用。
	emptyPV := &corev1.PersistentVolume{
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "claimrefns", Name: "claimrefname"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CephFS: &corev1.CephFSPersistentVolumeSource{
					SecretRef: &corev1.SecretReference{
						Name:      "",
						Namespace: "cephfs"}}}}}
	VisitPVSecretNames(emptyPV, func(namespace, name string, kubeletVisible bool) bool {
		t.Fatalf("expected no empty names collected, got %q", name)
		return false
	})
}

// collectSecretPaths 递归遍历对象类型，收集所有名字中含 "secret" 的结构体字段路径，
// 用于独立核对 VisitPVSecretNames 是否覆盖了全部真实 Secret 引用字段。
func collectSecretPaths(t *testing.T, path *field.Path, name string, tp reflect.Type) sets.Set[string] {
	secretPaths := sets.New[string]()

	if tp.Kind() == reflect.Pointer {
		// 先解引用指针，避免把 *T 与 T 的字段重复统计。
		secretPaths.Insert(sets.List[string](collectSecretPaths(t, path, name, tp.Elem()))...)
		return secretPaths
	}

	if strings.Contains(strings.ToLower(name), "secret") {
		secretPaths.Insert(path.String())
	}

	switch tp.Kind() {
	case reflect.Pointer:
		secretPaths.Insert(sets.List[string](collectSecretPaths(t, path, name, tp.Elem()))...)
	case reflect.Struct:
		// ObjectMeta 里不应有含 "secret" 的字段；
		// 而且它包含循环引用，直接跳过最省事。
		if name == "ObjectMeta" {
			break
		}
		for i := 0; i < tp.NumField(); i++ {
			field := tp.Field(i)
			secretPaths.Insert(sets.List[string](collectSecretPaths(t, path.Child(field.Name), field.Name, field.Type))...)
		}
	case reflect.Interface:
		// 接口里无法静态判断具体类型，遍历到此处需人工确认。
		t.Errorf("cannot find secret fields in interface{} field %s", path.String())
	case reflect.Map:
		secretPaths.Insert(sets.List[string](collectSecretPaths(t, path.Key("*"), "", tp.Elem()))...)
	case reflect.Slice:
		secretPaths.Insert(sets.List[string](collectSecretPaths(t, path.Key("*"), "", tp.Elem()))...)
	default:
		// 基本类型，无子字段可遍历。
	}

	return secretPaths
}
