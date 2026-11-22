/*
Copyright 2022 The Kubernetes Authors.

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

// Package setverbositylevel stores callbacks that will be invoked by logs.GlogLevel.
//
// This is a separate package to avoid a dependency from
// k8s.io/component-base/logs (uses the callbacks) to
// k8s.io/component-base/logs/api/v1 (adds them). Not all users of the logs
// package also use the API.
//
// Package setverbositylevel 存放由 logs.GlogLevel 触发的回调。单独成包是为了
// 避免 logs（消费方）反向依赖 logs/api/v1（注册方）造成循环依赖；
// 并非所有 logs 的使用者都同时使用配置 API。
package setverbositylevel

import (
	"sync"
)

var (
	// Mutex controls access to the callbacks.
	// 调用方在读写 Callbacks 前后都必须持有该锁：回调可能在运行期被注册，
	// 而 GlogLevel 读取时没有其他同步手段。
	Mutex sync.Mutex

	// Callbacks 是设置全局日志级别时需要依次调用的钩子；
	// 返回错误即停止后续调用并把错误上抛给 --v 的解析方。
	Callbacks []func(v uint32) error
)
