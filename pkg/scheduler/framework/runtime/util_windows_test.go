//go:build windows

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

package runtime

import (
	"testing"
)

// checkLatency 在 Windows 上不检查延迟值：
// Windows 的 time.Now() 精度较低，连续两次调用可能得到相同时间戳，
// 导致延迟指标为 0（SinceInSeconds 返回 0），因此此处不做断言。
// See: https://github.com/golang/go/issues/8687
func checkLatency(t *testing.T, value float64) {
}
