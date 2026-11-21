//go:build !linux

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

	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/klog/v2"
)

// healthCheckerUnsupported 是不支持看门狗的平台上的空实现。
type healthCheckerUnsupported struct{}

var _ HealthChecker = &healthCheckerUnsupported{}

// Option 配置空实现的健康检查器。
type Option func(*healthCheckerUnsupported)

// WithExtendedCheckers 在不支持的平台上忽略扩展检查器。
func WithExtendedCheckers([]healthz.HealthChecker) Option {
	return nil
}

// NewHealthChecker 在不支持看门狗的平台返回空实现。
func NewHealthChecker(klog.Logger, ...Option) (HealthChecker, error) {
	return &healthCheckerUnsupported{}, nil
}

// SetHealthCheckers 为空实现，不注册任何健康检查器。
func (hc *healthCheckerUnsupported) SetHealthCheckers(syncLoop syncLoopHealthChecker, checkers []healthz.HealthChecker) {
}

// Start 为空实现，不启动任何检查循环。
func (ow *healthCheckerUnsupported) Start(context.Context) {
	return
}
