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

package config

// JobControllerConfiguration 描述 Job 控制器的可配置项。
type JobControllerConfiguration struct {
	// ConcurrentJobSyncs 是允许同时同步的 Job 对象数量。
	// 数值越大，Job 的状态更新越及时，但会增加 CPU 与 apiserver 网络负载；
	// 需要在响应速度与控制平面压力之间权衡。
	ConcurrentJobSyncs int32
}
