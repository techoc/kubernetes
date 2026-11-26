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

package controller

import (
	"net/http"

	"k8s.io/controller-manager/pkg/healthz"
)

// Interface 是受 controller-manager 管理的控制器必须实现的最小接口。
// 它只要求提供规范名称，名称用于日志、指标以及 /debug/controllers/{name}/ 路由。
type Interface interface {
	Name() string
	// 返回控制器的规范名称（canonical name），在整个管理器内应唯一。
}

// Debuggable 由愿意暴露调试端点的控制器实现。
// 管理器启动时如果发现该接口且返回的 handler 非 nil，
// 会把它挂载到 /debug/controllers/{controllerName}/ 路径下。
type Debuggable interface {
	//
	DebuggingHandler() http.Handler
	// 返回调试用 handler；不希望暴露调试端点时返回 nil。
}

// HealthCheckable 由希望参与健康检查的控制器实现。
// 管理器会把返回的健康检查挂到 /healthz 端点上；返回 nil 表示不注册自定义检查。
type HealthCheckable interface {
	HealthChecker() healthz.UnnamedHealthChecker
	// 返回未命名的健康检查器，具体检查名称由 controller-manager 统一添加。
}
