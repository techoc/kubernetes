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

// ReplicationControllerConfiguration 包含 ReplicationController 的配置参数。
type ReplicationControllerConfiguration struct {
	// ConcurrentRCSyncs 是允许并发同步的 ReplicationController 数量。
	// 数值越大，副本管理响应越快，但会增加 CPU 与网络负载。
	ConcurrentRCSyncs int32
}
