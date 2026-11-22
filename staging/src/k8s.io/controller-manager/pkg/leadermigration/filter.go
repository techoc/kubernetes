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

// 领导者迁移用 FilterResult 告诉 controller-manager：某个控制器该由谁、以哪把锁启动。
// 迁移期间 KCM 与 CCM 会对同一批控制器做不同判断，从而实现不中断的交接。
// FilterResult indicates whether and how the controller manager should start the controller.
type FilterResult int32

const (
	// ControllerUnowned indicates that the controller is owned by another controller manager
	//  and thus should NOT be started by this controller manager.
	ControllerUnowned = iota
	// 该控制器归另一个组件所有，本组件不应启动它。
	// ControllerMigrated indicates that the controller manager should start this controller
	// 本组件应启动该控制器，并使用“迁移锁”。
	//  with the migration lock.
	ControllerMigrated
	// ControllerNonMigrated indicates that the controller manager should start this controller
	// 该控制器不参与迁移，使用常规的主锁启动。
	//  with the main lock.
	ControllerNonMigrated
)

// FilterFunc takes a name of controller, returning a FilterResult indicating how to start controller.
// 根据控制器名字返回启动策略的函数类型。
type FilterFunc func(controllerName string) FilterResult
