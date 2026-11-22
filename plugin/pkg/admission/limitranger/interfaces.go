/*
Copyright 2014 The Kubernetes Authors.

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

package limitranger

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/admission"
)

// LimitRangerActions 定义了一组可插拔动作，用于遍历 LimitRange 并识别、处理其中的限制。
type LimitRangerActions interface {
	// MutateLimit 是可插拔函数，用于为对象设置限制。
	MutateLimit(limitRange *corev1.LimitRange, kind string, obj runtime.Object) error
	// ValidateLimit 是可插拔函数，用于校验对象是否满足限制。
	ValidateLimit(limitRange *corev1.LimitRange, kind string, obj runtime.Object) error
	// SupportsAttributes 是可插拔函数，允许覆盖 limitranger 所支持的资源范围。
	SupportsAttributes(attr admission.Attributes) bool
	// SupportsLimit 是可插拔函数，允许基于任何原因忽略那些不应应用的限制。
	SupportsLimit(limitRange *corev1.LimitRange) bool
}
