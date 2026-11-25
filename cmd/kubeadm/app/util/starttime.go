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

// Package util 包含 kubeadm 工具。
package util

import (
	"time"
)

// startTime 保存 kubeadm 进程启动时间，供各处一致使用，
// 避免多次调用 time.Now() 导致细微时间偏差。
var startTime time.Time

func init() {
	startTime = time.Now()
}

// StartTimeUTC 返回时区设置为 UTC 的 startTime。
func StartTimeUTC() time.Time {
	return startTime.UTC()
}
