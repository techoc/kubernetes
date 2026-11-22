/*
Copyright 2020 The Kubernetes Authors.

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
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// APILifecycleIntroduced 返回该 API 结构引入的版本，以 major 和 minor 的整数形式表示，便于比较。
func (in *ComponentStatus) APILifecycleIntroduced() (major, minor int) {
	return 1, 0
}

// APILifecycleDeprecated 返回该 API 结构已废弃或将要废弃的版本，以 major 和 minor 的整数形式表示，便于比较。
func (in *ComponentStatus) APILifecycleDeprecated() (major, minor int) {
	return 1, 19
}

// APILifecycleIntroduced 返回该 API 结构引入的版本，以 major 和 minor 的整数形式表示，便于比较。
func (in *ComponentStatusList) APILifecycleIntroduced() (major, minor int) {
	return 1, 0
}

// APILifecycleDeprecated 返回该 API 结构已废弃或将要废弃的版本，以 major 和 minor 的整数形式表示，便于比较。
func (in *ComponentStatusList) APILifecycleDeprecated() (major, minor int) {
	return 1, 19
}

// APILifecycleDeprecated 返回该 API 结构已废弃或将要废弃的版本，以 major 和 minor 的整数形式表示，便于比较。
func (in *Endpoints) APILifecycleDeprecated() (major, minor int) {
	return 1, 33
}

// APILifecycleReplacement 返回该 API 替代者的 GVK。
func (in *Endpoints) APILifecycleReplacement() schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: "discovery.k8s.io", Version: "v1", Kind: "EndpointSlice"}
}

// APILifecycleDeprecated 返回该 API 结构已废弃或将要废弃的版本，以 major 和 minor 的整数形式表示，便于比较。
func (in *EndpointsList) APILifecycleDeprecated() (major, minor int) {
	return 1, 33
}

// APILifecycleReplacement 返回该 API 替代者的 GVK。
func (in *EndpointsList) APILifecycleReplacement() schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: "discovery.k8s.io", Version: "v1", Kind: "EndpointSliceList"}
}
