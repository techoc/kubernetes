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

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NamespaceControllerConfiguration 描述 NamespaceController 的配置项。
type NamespaceControllerConfiguration struct {
	// NamespaceSyncPeriod 是同步命名空间生命周期更新的周期。
	// 生命周期终结需要反复检查资源是否清理完毕，因此按固定间隔重新入队。
	NamespaceSyncPeriod metav1.Duration
	// ConcurrentNamespaceSyncs 是允许并发同步的命名空间对象数量。
	// 增大该值可提升终结速度，但会增加 API 服务器负载。
	ConcurrentNamespaceSyncs int32
}
