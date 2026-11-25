/*
Copyright 2016 The Kubernetes Authors.

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

package initsystem

// InitSystem 是描述 init 系统行为的接口。
type InitSystem interface {
	// EnableCommand 返回描述如何启用服务的字符串。
	EnableCommand(service string) string

	// ServiceStart 尝试启动指定服务。
	ServiceStart(service string) error

	// ServiceStop 尝试停止指定服务。
	ServiceStop(service string) error

	// ServiceRestart 尝试重新加载环境并重启指定服务。
	ServiceRestart(service string) error

	// ServiceExists 确认服务已在此 init 系统中定义。
	ServiceExists(service string) bool

	// ServiceIsEnabled 确认服务已配置为每次启动时自动运行。
	ServiceIsEnabled(service string) bool

	// ServiceIsActive 确认服务正在运行或尝试运行；kubelet 崩溃循环时也视为活动。
	ServiceIsActive(service string) bool
}
