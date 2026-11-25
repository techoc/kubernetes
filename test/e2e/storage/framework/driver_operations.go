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

package framework

import (
	"context"

	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/test/e2e/framework"
)

// GetDriverNameWithFeatureTags 返回可传给 framework.Context 的参数。
// 例如：
//   - [Driver: nfs]
//   - [Driver: rbd], feature.Volumes
func GetDriverNameWithFeatureTags(driver TestDriver) []interface{} {
	dInfo := driver.GetDriverInfo()

	return append([]interface{}{framework.WithLabel("Driver: " + dInfo.Name)}, dInfo.TestTags...)
}

// CreateVolume 为测试创建 volume，dynamicPV 或 CSI ephemeral inline volume 测试除外
func CreateVolume(ctx context.Context, driver TestDriver, config *PerTestConfig, volType TestVolType) TestVolume {
	switch volType {
	case InlineVolume, PreprovisionedPV:
		if pDriver, ok := driver.(PreprovisionedVolumeTestDriver); ok {
			return pDriver.CreateVolume(ctx, config, volType)
		}
	case CSIInlineVolume, GenericEphemeralVolume, DynamicPV:
		// 无需创建 volume
	default:
		framework.Failf("Invalid volType specified: %v", volType)
	}
	return nil
}

// CopyStorageClass 使用参数中传入的 StorageClass 配置构造一个新的 StorageClass 实例，
// 其唯一名称由 namespace + suffix 生成
func CopyStorageClass(sc *storagev1.StorageClass, ns string, suffix string) *storagev1.StorageClass {
	copy := sc.DeepCopy()
	copy.ObjectMeta.Name = names.SimpleNameGenerator.GenerateName(ns + "-" + suffix)
	copy.ResourceVersion = ""

	// 如果存在，删除 StorageClass 上的默认注解。
	// 多个 StorageClass 带有该注解会导致失败。
	delete(copy.Annotations, util.BetaIsDefaultStorageClassAnnotation)
	delete(copy.Annotations, util.IsDefaultStorageClassAnnotation)
	return copy
}

// GetStorageClass 构造一个新的 StorageClass 实例，
// 其唯一名称由 namespace + suffix 生成。
func GetStorageClass(
	provisioner string,
	parameters map[string]string,
	bindingMode *storagev1.VolumeBindingMode,
	ns string,
) *storagev1.StorageClass {
	if bindingMode == nil {
		defaultBindingMode := storagev1.VolumeBindingImmediate
		bindingMode = &defaultBindingMode
	}
	return &storagev1.StorageClass{
		TypeMeta: metav1.TypeMeta{
			Kind: "StorageClass",
		},
		ObjectMeta: metav1.ObjectMeta{
			// 名称必须唯一，因此基于 namespace 名称使用 GenerateName 生成
			Name: names.SimpleNameGenerator.GenerateName(ns),
		},
		Provisioner:       provisioner,
		Parameters:        parameters,
		VolumeBindingMode: bindingMode,
	}
}

// CopyVolumeAttributesClass 使用传入的 VolumeAttributesClass 构造一个新的 VolumeAttributesClass 实例，
// 其唯一名称由 namespace + suffix 生成
func CopyVolumeAttributesClass(vac *storagev1.VolumeAttributesClass, ns string, suffix string) *storagev1.VolumeAttributesClass {
	copy := vac.DeepCopy()
	copy.ObjectMeta.Name = names.SimpleNameGenerator.GenerateName(ns + "-" + suffix)
	copy.ResourceVersion = ""
	return copy
}
