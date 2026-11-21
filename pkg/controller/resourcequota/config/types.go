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

// ResourceQuotaControllerConfiguration 包含描述 ResourceQuotaController 的各项配置。
type ResourceQuotaControllerConfiguration struct {
	// resourceQuotaSyncPeriod 是同步系统中配额使用状态的周期。
	ResourceQuotaSyncPeriod metav1.Duration
	// concurrentResourceQuotaSyncs 是允许并发同步的资源配额数量。
	// 数值越大，配额管理响应越快，但 CPU（及网络）负载也越高。
	ConcurrentResourceQuotaSyncs int32
}
