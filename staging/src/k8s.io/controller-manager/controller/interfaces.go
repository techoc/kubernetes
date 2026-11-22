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

// Interface defines the base of a controller managed by a controller manager
// Interface 是受 controller-manager 管理的控制器必须实现的最小接口。
// 它只要求提供规范名称，名称用于日志、指标以及 /debug/controllers/{name}/ 路由。
type Interface interface {
	// Name returns the canonical name of the controller.
	Name() string
	// 返回控制器的规范名称（canonical name），在整个管理器内应唯一。
}

// Debuggable defines a controller that allows the controller manager
// to expose a debugging handler for the controller
//
// If a controller implements Debuggable, and the returned handler is
// not nil, the controller manager can mount the handler during startup.
// Debuggable 由愿意暴露调试端点的控制器实现。
// 管理器启动时如果发现该接口且返回的 handler 非 nil，
// 会把它挂载到 /debug/controllers/{controllerName}/ 路径下。
type Debuggable interface {
	// DebuggingHandler returns a Handler that expose debugging information
	// for the controller, or nil if a debugging handler is not desired.
	//
	// The handler will be accessible at "/debug/controllers/{controllerName}/".
	DebuggingHandler() http.Handler
	// 返回调试用 handler；不希望暴露调试端点时返回 nil。
}

// HealthCheckable defines a controller that allows the controller manager
// to include it in the health checks.
//
// If a controller implements HealthCheckable, and the returned check
// is not nil, the controller manager can expose the check to the
// /healthz endpoint.
// HealthCheckable 由希望参与健康检查的控制器实现。
// 管理器会把返回的健康检查挂到 /healthz 端点上；返回 nil 表示不注册自定义检查。
type HealthCheckable interface {
	// HealthChecker returns a UnnamedHealthChecker that the controller manager
	// can choose to mount on the /healthz endpoint, or nil if no custom
	// health check is desired.
	HealthChecker() healthz.UnnamedHealthChecker
	// 返回未命名的健康检查器，具体检查名称由 controller-manager 统一添加。
}
