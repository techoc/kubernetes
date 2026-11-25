/*
Copyright 2023 The Kubernetes Authors.

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

package internalversion

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// 中文补充：开启 watch-list 特性时，对使用 legacy resourceVersion（空或 "0"）的
// watch 请求默认启用 SendInitialEvents 与 NotOlderThan，从而实现“先列后 watch”的语义。
func SetListOptionsDefaults(obj *ListOptions, isWatchListFeatureEnabled bool) {
	// 特性未开启时保持旧行为，不注入任何默认值。
	if !isWatchListFeatureEnabled {
		return
	}
	// 用户已显式指定 SendInitialEvents 或 ResourceVersionMatch 时尊重其选择，不覆盖。
	if obj.SendInitialEvents != nil || len(obj.ResourceVersionMatch) != 0 {
		return
	}
	// 只有 legacy resourceVersion 才适用默认值；指定了具体版本时语义已由用户确定。
	legacy := obj.ResourceVersion == "" || obj.ResourceVersion == "0"
	if obj.Watch && legacy {
		turnOnInitialEvents := true
		obj.SendInitialEvents = &turnOnInitialEvents
		obj.ResourceVersionMatch = metav1.ResourceVersionMatchNotOlderThan
	}
}
