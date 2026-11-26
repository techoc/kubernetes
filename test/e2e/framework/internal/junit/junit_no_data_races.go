//go:build !race

/*
Copyright 2025 The Kubernetes Authors.

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

package junit

import (
	"github.com/onsi/ginkgo/v2"
)

func detectDataRaces(report ginkgo.Report) {
	// 这是该函数的 NOP 版本，用于测试二进制未启用竞态检测的场景。
	// 此时不可能出现数据竞争报告，因此
	// 无需检查它们。
}
