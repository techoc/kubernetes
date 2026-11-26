/*
Copyright 2023 The Kubernetes Authors.

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

// Package environment 包含 test/e2e 和/或 test/e2e_node 使用的预定义环境。
// and/or test/e2e_node.
package environment

import (
	"k8s.io/kubernetes/test/e2e/framework"
)

// 请按字母顺序、区分大小写
// （大写字符排在小写字符之前）维护该列表。
var (
	// 该测试在 UserNS 中无法工作（例如 `open /proc/sys/kernel/shm_rmid_forced: permission denied`）。
	NotInUserNS = framework.WithEnvironment(framework.ValidEnvironments.Add("NotInUserNS"))
)

func init() {
	// 这可以避免在测试中添加额外的临时环境。
	framework.ValidEnvironments.Freeze()
}
