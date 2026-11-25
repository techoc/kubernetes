/*
Copyright 2018 The Kubernetes Authors.

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

package windows

import (
	"k8s.io/kubernetes/test/e2e/framework"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"

	"github.com/onsi/ginkgo/v2"
)

// sigDescribe 为测试标注 SIG 标签。
// 与 skipUnlessWindows 配合使用，可定义仅在节点操作系统为 Windows 时运行的测试：
//
//	sigDescribe("foo", skipUnlessWindows(func() { ... }))
var sigDescribe = framework.SIGDescribe("windows")

// skipUnlessWindows 包装其他 Ginkgo 回调，在其定义的测试前插入 BeforeEach，
// 当节点操作系统不是 Windows 时跳过这些测试。
func skipUnlessWindows(cb func()) func() {
	return func() {
		ginkgo.BeforeEach(func() {
			// 本包所有测试均特定于 Windows
			e2eskipper.SkipUnlessNodeOSDistroIs("windows")
		})

		cb()
	}
}
