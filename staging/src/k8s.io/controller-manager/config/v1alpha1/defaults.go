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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	componentbaseconfigv1alpha1 "k8s.io/component-base/config/v1alpha1"
)

// 为 v1alpha1 通用配置填充推荐默认值。
// 逐个字段只在“零值”时才赋默认值，避免覆盖用户显式配置。
func RecommendedDefaultGenericControllerManagerConfiguration(obj *GenericControllerManagerConfiguration) {
	zero := metav1.Duration{}
	// 用零值 Duration 作为“用户未设置”的判断基准。
	if obj.Address == "" {
		// 监听地址默认绑定所有网卡。
		obj.Address = "0.0.0.0"
	}
	if obj.MinResyncPeriod == zero {
		// 最小 resync 周期默认 12 小时。
		obj.MinResyncPeriod = metav1.Duration{Duration: 12 * time.Hour}
	}
	if obj.ControllerStartInterval == zero {
		// 控制器启动间隔默认 0，即不刻意延迟。
		obj.ControllerStartInterval = metav1.Duration{Duration: 0 * time.Second}
	}
	if len(obj.Controllers) == 0 {
		// 未指定控制器列表时默认启用所有默认开启的控制器。
		obj.Controllers = []string{"*"}
	}

	if len(obj.LeaderElection.ResourceLock) == 0 {
		// Use lease-based leader election to reduce cost.
		// 锁类型默认选 leases：相比 endpoints 成本更低（1.17 起从 EndpointsLease 迁移，1.20 起使用 Lease）。
		// We migrated for EndpointsLease lock in 1.17 and starting in 1.20 we
		// migrated to Lease lock.
		obj.LeaderElection.ResourceLock = "leases"
	}

	// Use the default ClientConnectionConfiguration and LeaderElectionConfiguration options
	// 连接与选举配置中的细粒度默认值交给 component-base 处理。
	componentbaseconfigv1alpha1.RecommendedDefaultClientConnectionConfiguration(&obj.ClientConnection)
	componentbaseconfigv1alpha1.RecommendedDefaultLeaderElectionConfiguration(&obj.LeaderElection)
}
