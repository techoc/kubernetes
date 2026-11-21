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

// ReplicaSetControllerConfiguration 描述副本集控制器（ReplicaSetController）的配置参数。
type ReplicaSetControllerConfiguration struct {
	// ConcurrentRSSyncs 是允许并发同步的副本集数量。
	// 增大该值可以更快响应副本数变化，但会增加 CPU 与网络负载；
	// 这是吞吐与资源开销之间的权衡。
	ConcurrentRSSyncs int32
}
