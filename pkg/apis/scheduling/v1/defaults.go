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

package v1

import (
	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// addDefaultingFuncs 向 scheme 注册 scheduling/v1 的默认值函数。
// 这样 API machinery 在解码或创建该版本对象时会自动套用下面的默认值逻辑。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_PriorityClass 设置相对于 extensions 版本对应实现而言额外的默认值。
// 关键场景：当 PreemptionPolicy 未指定时，填充为默认的 PreemptLowerPriority（抢占低优先级 Pod）。
// 这里选择显式设置该默认值而非留空，是为了让后续消费方拿到确定的语义，
// 不必各自处理 nil 情况。
func SetDefaults_PriorityClass(obj *v1.PriorityClass) {
	if obj.PreemptionPolicy == nil {
		preemptLowerPriority := apiv1.PreemptLowerPriority
		obj.PreemptionPolicy = &preemptLowerPriority
	}
}
