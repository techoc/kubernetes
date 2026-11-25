/*
Copyright 2019 The Kubernetes Authors.

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

package framework

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
)

// Logf 记录信息。
//
// 用它代替 `klog.Infof`，因为栈展开时会自动
// 跳过通过调用 [ginkgo.GinkgoHelper] 标记自己为 helper 的
// 辅助函数。
func Logf(format string, args ...interface{}) {
	log(1, fmt.Sprintf(format, args...))
}

// Failf 记录失败信息，包括从其直接调用方开始的栈回溯
// （例如调用链 f -> g -> Failf("foo", ...) 时，错误将记录为来自 "g"）。
func Failf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	skip := 1
	ginkgo.Fail(msg, skip)
	panic("unreachable")
}

// Fail 是 ginkgo.Fail 的别名。
var Fail = ginkgo.Fail
