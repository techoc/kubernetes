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

// RecommendedDefaultServiceControllerConfiguration 为配置指针填入推荐默认值；
// 默认值可能随 API 版本变化。该函数有意不注册为 scheme 中的常规 `SetDefaults_Foo`，
// 以便嵌入该类型的上层自行决定默认值：scheme 的默认值在转换时执行，强制使用它
// 会让上层无法退出。需要这些默认值时，应在包装结构的 `SetDefaults_` 方法中调用。
// 换句话说，这里提供“推荐值”而非强制契约：嵌入方可以自行决定采用、覆盖或忽略。
// 默认 1 个 worker 是为了让 service 对账串行化，避免并发创建同一 LB 资源。
func RecommendedDefaultServiceControllerConfiguration(obj *ServiceControllerConfiguration) {
	if obj.ConcurrentServiceSyncs == 0 {
		obj.ConcurrentServiceSyncs = 1
	}
}
