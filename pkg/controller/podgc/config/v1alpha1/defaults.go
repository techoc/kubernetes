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

package v1alpha1

import (
	kubectrlmgrconfigv1alpha1 "k8s.io/kube-controller-manager/config/v1alpha1"
)

// RecommendedDefaultPodGCControllerConfiguration 为 PodGCControllerConfiguration 指针设置推荐默认值，
// 默认值可能随 API 版本变化。该函数刻意没有以常规 `SetDefaults_Foo` 形式注册到 scheme，
// 以免强制使用者接受这套默认值：scheme 中的默认化是转换的一部分，无法便捷退出。
// 需要这套默认值时，请在外层包装结构体的 `SetDefaults_` 方法中调用它。
func RecommendedDefaultPodGCControllerConfiguration(obj *kubectrlmgrconfigv1alpha1.PodGCControllerConfiguration) {
	// 零值表示未配置；12500 是历史沿用的默认阈值，
	// 表示最多保留这么多已终止 Pod，超出后由 GC 清理。
	if obj.TerminatedPodGCThreshold == 0 {
		obj.TerminatedPodGCThreshold = 12500
	}
}
