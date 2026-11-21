/*
Copyright 2024 The Kubernetes Authors.

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

package watchdog

import (
	"context"
	"net/http"

	"k8s.io/apiserver/pkg/server/healthz"
)

// HealthChecker 定义健康检查器的接口。
type HealthChecker interface {
	// Start 启动健康检查器。
	Start(ctx context.Context)
	// SetHealthCheckers 注入 syncLoop 与需要聚合的健康检查器列表。
	SetHealthCheckers(syncLoop syncLoopHealthChecker, checkers []healthz.HealthChecker)
}

// syncLoopHealthChecker 包含 syncLoop 的健康检查方法。
type syncLoopHealthChecker interface {
	// SyncLoopHealthCheck 对 syncLoop 的健康状态执行检查。
	SyncLoopHealthCheck(req *http.Request) error
}
