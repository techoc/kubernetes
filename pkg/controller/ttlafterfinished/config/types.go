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

// TTLAfterFinishedControllerConfiguration 包含描述 TTLAfterFinishedController 的各项配置。
type TTLAfterFinishedControllerConfiguration struct {
	// ConcurrentTTLSyncs 是允许并发同步的 TTL-after-finished 回收 worker 数量。
	ConcurrentTTLSyncs int32
}
