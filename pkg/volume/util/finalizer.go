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

package util

const (
	// PVCProtectionFinalizer 是运行在用的 Pod 所关联 PVC 上的 finalizer 名称。
	PVCProtectionFinalizer = "kubernetes.io/pvc-protection"

	// PVProtectionFinalizer 是被 PVC 绑定的 PV 上的 finalizer 名称。
	PVProtectionFinalizer = "kubernetes.io/pv-protection"

	// VACProtectionFinalizer 是被 PV 或 PVC 使用的 VAC 上的 finalizer 名称。
	VACProtectionFinalizer = "kubernetes.io/vac-protection"
)
