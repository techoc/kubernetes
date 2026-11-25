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

// LeaderMigrator 持有 leader 迁移过程所需的信息：
// 一个“准备就绪”通道，以及一个决定控制器启动方式的过滤器函数。
type LeaderMigrator struct {
	// MigrationReady 在 controller-manager 完成迁移锁准备后被关闭；
	// 此后 leader 迁移流程才会去获取迁移锁。
	MigrationReady chan struct{}

	// FilterFunc 返回 FilterResult，告知 controller-manager 该如何处理某个控制器。
	// 根据控制器名字返回启动策略：使用迁移锁、主锁，或根本不启动。
	FilterFunc FilterFunc
}

// NewLeaderMigrator 为指定 component 按给定配置创建 LeaderMigrator。component 指明
// 是哪个 controller manager 在请求这次 leader 迁移，应与 ControllerLeaderConfiguration
// 的 component 字段保持一致。
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
				if shouldRun {
					// 该控制器应由本 controller manager 运行：用迁移锁启动。
					return ControllerMigrated
				}
				// 否则该控制器应由其它 controller manager 启动：本组件不启动。
				return ControllerUnowned
			}
			// 该控制器不参与迁移，应在主锁下启动：走常规主锁。
			return ControllerNonMigrated
		},
	}
}
