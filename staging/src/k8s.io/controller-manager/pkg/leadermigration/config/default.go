/*
Copyright 2021 The Kubernetes Authors.

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

import internal "k8s.io/controller-manager/config"

// 返回当前版本推荐的默认迁移配置：
// 锁名为 cloud-provider-extraction-migration，使用 leases 锁，
// 并把路由、Service、云节点生命周期三个控制器标记为可由任意组件运行（"*"）。
func DefaultLeaderMigrationConfiguration() *internal.LeaderMigrationConfiguration {
	return &internal.LeaderMigrationConfiguration{
		LeaderName: "cloud-provider-extraction-migration",
		// KCM 与 CCM 必须使用同一个锁名，才能在迁移期间互斥。
		ResourceLock: ResourceLockLeases,
		ControllerLeaders: []internal.ControllerLeaderConfiguration{
			{
				Name: "route-controller",
				// "*" 表示迁移过程中该控制器可由任一参与迁移的组件接管。
				Component: "*",
			}, {
				Name:      "service-controller",
				Component: "*",
			}, {
				Name:      "cloud-node-lifecycle-controller",
				Component: "*",
			},
		},
	}
}
