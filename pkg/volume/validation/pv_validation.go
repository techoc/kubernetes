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

package validation

import (
	"errors"
	"path/filepath"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation/field"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// ValidatePersistentVolume 对 PV 对象做插件相关的校验。
// 这里放置与具体卷类型相关的校验逻辑。
func ValidatePersistentVolume(pv *api.PersistentVolume) field.ErrorList {
	return checkMountOption(pv)
}

// checkMountOption 校验 PV 的挂载选项是否合法。
func checkMountOption(pv *api.PersistentVolume) field.ErrorList {
	allErrs := field.ErrorList{}
	// 这些卷类型支持挂载选项，无需报错。
	if pv.Spec.GCEPersistentDisk != nil ||
		pv.Spec.AWSElasticBlockStore != nil ||
		pv.Spec.Glusterfs != nil ||
		pv.Spec.NFS != nil ||
		pv.Spec.RBD != nil ||
		pv.Spec.Quobyte != nil ||
		pv.Spec.ISCSI != nil ||
		pv.Spec.Cinder != nil ||
		pv.Spec.CephFS != nil ||
		pv.Spec.AzureFile != nil ||
		pv.Spec.VsphereVolume != nil ||
		pv.Spec.AzureDisk != nil ||
		pv.Spec.PhotonPersistentDisk != nil {
		return allErrs
	}
	// 其他卷类型一旦指定挂载选项就报错。
	if _, ok := pv.Annotations[api.MountOptionAnnotation]; ok {
		metaField := field.NewPath("metadata")
		allErrs = append(allErrs, field.Forbidden(metaField.Child("annotations", api.MountOptionAnnotation), "may not specify mount options for this volume type"))
	}
	return allErrs
}

// ValidatePathNoBacksteps 确保 targetPath 中不含 ".." 路径段。
func ValidatePathNoBacksteps(targetPath string) error {
	parts := strings.Split(filepath.ToSlash(targetPath), "/")
	if slices.Contains(parts, "..") {
		return errors.New("must not contain '..'")
	}

	return nil
}
