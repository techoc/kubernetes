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

package leadermigration

import (
	internal "k8s.io/controller-manager/config"
)

// LeaderMigrator holds information required by the leader migration process.
// 领导者迁移器持有迁移过程所需的两个东西：
// 一个“准备就绪”通道，以及一个决定控制器启动方式的过滤器函数。
type LeaderMigrator struct {
	// MigrationReady is closed after the controller manager finishes preparing for the migration lock.
	// After this point, the leader migration process will proceed to acquire the migration lock.
	// controller-manager 完成迁移锁的准备后关闭该通道，随后才去获取迁移锁。
	MigrationReady chan struct{}

	// FilterFunc returns a FilterResult telling the controller manager what to do with the controller.
	// 根据控制器名字返回启动策略：使用迁移锁、主锁，或根本不启动。
	FilterFunc FilterFunc
}

// NewLeaderMigrator creates a LeaderMigrator with given config for the given component. component
// indicates which controller manager is requesting this leader migration, and it should be consistent
// with the component field of ControllerLeaderConfiguration.
// 根据迁移配置和“当前组件名”构造迁移器：
// 1) 把参与迁移的控制器汇总成 map（值为是否应由本组件运行）；
// 2) 返回闭包形式的 FilterFunc 完成策略判断。
func NewLeaderMigrator(config *internal.LeaderMigrationConfiguration, component string) *LeaderMigrator {
	migratedControllers := make(map[string]bool)
	// key=控制器名，value=本组件是否应负责运行它。
	for _, leader := range config.ControllerLeaders {
		migratedControllers[leader.Name] = leader.Component == component || leader.Component == "*"
		// component 匹配本组件，或配置为通配符 "*"，都表示由本组件运行。
	}
	return &LeaderMigrator{
		MigrationReady: make(chan struct{}),
		FilterFunc: func(controllerName string) FilterResult {
			shouldRun, ok := migratedControllers[controllerName]
			// ok 表示控制器是否参与迁移；shouldRun 表示是否归本组件。
			if ok {
				// 参与迁移：按归属决定用迁移锁启动还是完全不启动。
				// The controller is included in the migration
				if shouldRun {
					// If the controller manager should run the controller,
					//  start it in the migration lock.
					// 归本组件：用迁移锁启动。
					return ControllerMigrated
				}
				// Otherwise, the controller should be started by
				//  some other controller manager.
				// 归其它组件：本组件不启动。
				return ControllerUnowned
			}
			// The controller is not included in the migration,
			//  and should be started in the main lock.
			// 不参与迁移：走常规主锁。
			return ControllerNonMigrated
		},
	}
}
