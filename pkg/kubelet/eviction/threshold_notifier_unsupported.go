//go:build !linux

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

package eviction

import (
	"context"

	"k8s.io/klog/v2"
)

// NewCgroupNotifier 创建空操作的 cgroup 通知器：非 Linux 系统没有 cgroups。
func NewCgroupNotifier(logger klog.Logger, path, attribute string, threshold int64) (CgroupNotifier, error) {
	logger.V(5).Info("cgroup notifications not supported")
	return &unsupportedThresholdNotifier{}, nil
}

// unsupportedThresholdNotifier 是不支持 cgroup 通知的平台的空实现。
type unsupportedThresholdNotifier struct{}

// Start 为空实现，不监视任何阈值。
func (*unsupportedThresholdNotifier) Start(context.Context, chan<- struct{}) {}

// Stop 为空实现。
func (*unsupportedThresholdNotifier) Stop() {}
